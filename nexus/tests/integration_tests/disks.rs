// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic disk support in the API

use super::instances::instance_wait_for_state;
use super::metrics_querier::MetricsNotYet;
use super::metrics_querier::MetricsQuerier;
use chrono::Utc;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_config::RegionAllocationStrategy;
use nexus_db_lookup::LookupPath;
use nexus_db_model::PhysicalDiskPolicy;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use nexus_db_queries::db::datastore::RegionAllocationFor;
use nexus_db_queries::db::datastore::RegionAllocationParameters;
use nexus_db_queries::db::fixed_data::FLEET_ID;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::Collection;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::wait_for_producer;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::identity::Asset;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::{ByteCount, SimpleIdentityOrName as _};
use omicron_nexus::Nexus;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::app::{MAX_DISK_SIZE_BYTES, MIN_DISK_SIZE_BYTES};
use omicron_uuid_kinds::VolumeUuid;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use oximeter::types::Datum;
use oximeter::types::Measurement;
use sled_agent_client::TestInterfaces as _;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;
type DiskTestBuilder<'a> = nexus_test_utils::resource_helpers::DiskTestBuilder<
    'a,
    omicron_nexus::Server,
>;

const PROJECT_NAME: &str = "springfield-squidport-disks";
const PROJECT_NAME_2: &str = "bouncymeadow-octopusharbor-disks";
const DISK_NAME: &str = "just-rainsticks";
const INSTANCE_NAME: &str = "just-rainsticks";

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_disk_url(disk_name: &str) -> String {
    format!("/v1/disks/{disk_name}?project={}", PROJECT_NAME)
}

fn get_instance_disks_url(instance_name: &str) -> String {
    format!("/v1/instances/{instance_name}/disks?project={}", PROJECT_NAME)
}

fn get_disk_attach_url(instance: &NameOrId) -> String {
    match instance {
        NameOrId::Name(instance_name) => format!(
            "/v1/instances/{}/disks/attach?project={}",
            instance_name.as_str(),
            PROJECT_NAME
        ),
        NameOrId::Id(instance_id) => {
            format!(
                "/v1/instances/{}/disks/attach",
                instance_id.to_string().as_str()
            )
        }
    }
}

fn get_disk_detach_url(instance: &NameOrId) -> String {
    match instance {
        NameOrId::Name(instance_name) => format!(
            "/v1/instances/{}/disks/detach?project={}",
            instance_name.as_str(),
            PROJECT_NAME
        ),
        NameOrId::Id(instance_id) => {
            format!(
                "/v1/instances/{}/disks/detach",
                instance_id.to_string().as_str()
            )
        }
    }
}

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_disk_not_found_before_creation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // List disks.  There aren't any yet.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 0);

    // Make sure we get a 404 if we fetch one.
    let disk_url = get_disk_url(DISK_NAME);
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &disk_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: disk with name \"{}\"", DISK_NAME)
    );

    // We should also get a 404 if we delete one.
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: disk with name \"{}\"", DISK_NAME)
    );
}

async fn set_instance_state(
    client: &ClientTestContext,
    instance_name: &str,
    state: &str,
) -> Instance {
    let url = format!(
        "/v1/instances/{instance_name}/{state}?project={}",
        PROJECT_NAME
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

async fn instance_simulate(nexus: &Arc<Nexus>, id: &InstanceUuid) {
    let info = nexus
        .active_instance_info(id, None)
        .await
        .unwrap()
        .expect("instance must be on a sled to simulate a state change");

    info.sled_client.vmm_finish_transition(info.propolis_id).await;
}

#[nexus_test]
async fn test_disk_create_attach_detach_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let project_id = create_project_and_pool(client).await;
    let nexus = &cptestctx.server.server_context().nexus;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = get_disk_url(DISK_NAME);
    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    assert_eq!(disk.identity.name, DISK_NAME);
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, project_id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.image_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.block_size.to_bytes(), 512);
    assert_eq!(disk.state, DiskState::Creating);

    // Fetch the disk and expect it to match what we just created except that
    // the state will now be "Detached", as the server has simulated the create
    // process.
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.identity.name, DISK_NAME);
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, project_id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.image_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.block_size.to_bytes(), 512);
    assert_eq!(disk.state, DiskState::Detached);

    // List disks again and expect to find the one we just created.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 1);
    disks_eq(&disks[0], &disk);

    // Create an instance to attach the disk.
    let instance = create_instance(&client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(&instance.identity.id.into());
    let url_instance_detach_disk =
        get_disk_detach_url(&instance.identity.id.into());

    // Start attaching the disk to the instance.
    let attached_disk = disk_post(
        client,
        &url_instance_attach_disk,
        disk.identity.name.clone(),
    )
    .await;
    let instance_id = &instance.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(*instance_id));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    let disk =
        disk_post(client, &url_instance_attach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Attached(*instance_id));

    // Begin detaching the disk.
    let disk = disk_post(
        client,
        &url_instance_detach_disk,
        disk.identity.name.clone(),
    )
    .await;
    assert_eq!(disk.state, DiskState::Detached);

    // Since detach is idempotent, we can detach it again.
    let disk = disk_post(
        client,
        &url_instance_detach_disk,
        disk.identity.name.clone(),
    )
    .await;
    assert_eq!(disk.state, DiskState::Detached);

    // Delete the disk.
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // It should no longer be present in our list of disks.
    assert_eq!(disks_list(&client, &disks_url).await.len(), 0);

    // We shouldn't find it if we request it explicitly.
    let error = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &disk_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    assert_eq!(
        error.message,
        format!("not found: disk with name \"{}\"", DISK_NAME)
    );
}

#[nexus_test]
async fn test_disk_create_disk_that_already_exists_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(1),
    };
    let _ = create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    let disk_url = get_disk_url(DISK_NAME);
    let disk = disk_get(&client, &disk_url).await;

    // Attempt to create a second disk with a conflicting name.
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("already exists: disk \"{}\"", DISK_NAME)
    );

    // List disks again and expect to find the one we just created.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 1);
    disks_eq(&disks[0], &disk);
}

#[nexus_test]
async fn test_disk_slot_assignment(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let nexus = &cptestctx.server.server_context().nexus;

    let disk_names = ["a", "b", "c", "d"];
    let mut disks = Vec::new();
    for name in disk_names {
        let disk = create_disk(&client, PROJECT_NAME, name).await;
        disks.push(disk);
    }

    // Create an instance to which to attach the disks, then force it to stop
    // to allow disks to be attached. There should be no disks attached
    // initially.
    let instance = create_instance(&client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);
    set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let listed_disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(listed_disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(&instance.identity.id.into());

    async fn get_disk_slot(ctx: &ControlPlaneTestContext, disk_id: Uuid) -> u8 {
        let apictx = &ctx.server.server_context();
        let nexus = &apictx.nexus;
        let datastore = nexus.datastore();
        let opctx =
            OpContext::for_tests(ctx.logctx.log.new(o!()), datastore.clone());

        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk_id)
            .fetch()
            .await
            .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

        db_disk.slot.expect("test disk should be attached").0
    }

    // Slots are assigned serially as disks are attached.
    for (expected_slot, disk) in disks.iter().enumerate() {
        let attached_disk = disk_post(
            client,
            &url_instance_attach_disk,
            disk.identity.name.clone(),
        )
        .await;

        assert_eq!(attached_disk.identity.name, disk.identity.name);
        assert_eq!(attached_disk.identity.id, disk.identity.id);
        assert_eq!(
            attached_disk.state,
            DiskState::Attached(instance_id.into_untyped_uuid())
        );

        assert_eq!(
            get_disk_slot(cptestctx, attached_disk.identity.id).await,
            expected_slot as u8
        );
    }

    // Detach disks 1 and 2 and reattach them in reverse order. Verify that
    // this inverts their slots but leaves the other disks alone.
    let url_instance_detach_disk =
        get_disk_detach_url(&instance.identity.id.into());
    disk_post(
        client,
        &url_instance_detach_disk,
        disks[1].identity.name.clone(),
    )
    .await;
    disk_post(
        client,
        &url_instance_detach_disk,
        disks[2].identity.name.clone(),
    )
    .await;

    disk_post(
        client,
        &url_instance_attach_disk,
        disks[2].identity.name.clone(),
    )
    .await;
    disk_post(
        client,
        &url_instance_attach_disk,
        disks[1].identity.name.clone(),
    )
    .await;

    // The slice here is constructed so that slice[x] yields the index of the
    // disk that should be assigned to slot x.
    for (expected_slot, disk_index) in [0, 2, 1, 3].iter().enumerate() {
        assert_eq!(
            get_disk_slot(cptestctx, disks[*disk_index as usize].identity.id)
                .await,
            expected_slot as u8
        );
    }
}

#[nexus_test]
async fn test_disk_move_between_instances(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(&client).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = get_disk_url(DISK_NAME);
    let disk = create_disk(client, PROJECT_NAME, DISK_NAME).await;

    // Create an instance to attach the disk.
    let instance = create_instance(&client, PROJECT_NAME, INSTANCE_NAME).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(&client, instance_id, InstanceState::Stopped).await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(&instance.identity.name.clone().into());
    let url_instance_detach_disk =
        get_disk_detach_url(&instance.identity.name.into());

    // Start attaching the disk to the instance.
    let attached_disk = disk_post(
        client,
        &url_instance_attach_disk,
        disk.identity.name.clone(),
    )
    .await;
    let instance_id = &instance.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(*instance_id));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    let disk =
        disk_post(client, &url_instance_attach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Attached(*instance_id));

    // Create a second instance and try to attach the disk to that.  This should
    // fail and the disk should remain attached to the first instance.
    let instance2 = create_instance(&client, PROJECT_NAME, "instance2").await;
    let instance2_id = InstanceUuid::from_untyped_uuid(instance2.identity.id);
    set_instance_state(&client, "instance2", "stop").await;
    instance_simulate(nexus, &instance2_id).await;
    instance_wait_for_state(&client, instance2_id, InstanceState::Stopped)
        .await;

    let url_instance2_attach_disk =
        get_disk_attach_url(&instance2.identity.id.into());
    let url_instance2_detach_disk =
        get_disk_detach_url(&instance2.identity.id.into());

    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance2_attach_disk)
            .body(Some(&params::DiskPath {
                disk: disk.identity.name.clone().into(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another instance",
            DISK_NAME
        )
    );

    let attached_disk = disk_get(&client, &disk_url).await;
    assert_eq!(
        attached_disk.state,
        DiskState::Attached(instance_id.into_untyped_uuid())
    );

    // Begin detaching the disk.
    let disk =
        disk_post(client, &url_instance_detach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Detached);

    // There's no problem attempting to detach it again.
    let disk =
        disk_post(client, &url_instance_detach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Since delete is idempotent, we can detach it again -- from either one.
    let disk =
        disk_post(client, &url_instance_detach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Detached);
    let disk =
        disk_post(client, &url_instance2_detach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Now, start attaching it again to the second instance.
    let attached_disk = disk_post(
        client,
        &url_instance2_attach_disk,
        disk.identity.name.clone(),
    )
    .await;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(
        attached_disk.state,
        DiskState::Attached(instance2_id.into_untyped_uuid())
    );

    // At this point, it's not legal to attempt to attach it to a different
    // instance (the first one).
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance_attach_disk)
            .body(Some(&params::DiskPath {
                disk: disk.identity.name.clone().into(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another instance",
            DISK_NAME
        )
    );

    // It's fine to attempt another attachment to the same instance.
    let disk = disk_post(
        client,
        &url_instance2_attach_disk,
        disk.identity.name.clone(),
    )
    .await;
    assert_eq!(
        disk.state,
        DiskState::Attached(instance2_id.into_untyped_uuid())
    );

    // It's not allowed to delete a disk that's attached.
    let error = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &disk_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected request to fail")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("cannot parse");
    assert_eq!(error.message, "disk cannot be deleted in state \"attached\"");

    // Now, begin a detach.
    let disk = disk_post(
        client,
        &url_instance2_detach_disk,
        disk.identity.name.clone(),
    )
    .await;
    assert_eq!(disk.state, DiskState::Detached);

    // Now we can delete the disk.
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // It should no longer be present in our list of disks.
    assert_eq!(disks_list(&client, &disks_url).await.len(), 0);

    // We shouldn't find it if we request it explicitly.
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &disk_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: disk with name \"{}\"", DISK_NAME)
    );
}

#[nexus_test]
async fn test_disk_creation_region_requested_then_started(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Before we create a disk, set the response from the Crucible Agent:
    // no matter what regions get requested, they'll always *start* as
    // "Requested", and transition to "Created" on the second call.
    test.set_requested_then_created_callback().await;

    // The disk is created successfully, even when this "requested" -> "started"
    // transition occurs.
    create_disk(client, PROJECT_NAME, DISK_NAME).await;
}

// Tests that region allocation failure causes disk allocation to fail.
#[nexus_test]
async fn test_disk_region_creation_failure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Before we create a disk, set the response from the Crucible Agent:
    // no matter what regions get requested, they'll always fail.
    test.set_always_fail_callback().await;

    // Attempt to allocate the disk, observe a server error.
    let disk_size = ByteCount::from_gibibytes_u32(3);

    // Allocation should have room, it should fail due to the callback set
    // above.
    assert!(
        disk_size.to_whole_gibibytes()
            < DiskTest::DEFAULT_ZPOOL_SIZE_GIB.into()
    );

    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    // Unfortunately, the error message is only posted internally to the
    // logs, and it not returned to the client.
    //
    // TODO: Maybe consider making this a more informative error?
    // How should we propagate this to the client?
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::INTERNAL_SERVER_ERROR)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // After the failed allocation, the disk creation should have unwound
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 0);
}

// Tests that invalid block sizes are rejected
#[nexus_test]
async fn test_disk_invalid_block_size_rejected(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Attempt to allocate the disk, observe a server error.
    let disk_size = ByteCount::from_gibibytes_u32(3);

    // Allocation should have room, it should fail due to block size mismatch.
    assert!(
        disk_size.to_whole_gibibytes()
            < DiskTest::DEFAULT_ZPOOL_SIZE_GIB.into()
    );

    let disks_url = get_disks_url();

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize(1024),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

// Tests that a disk is rejected if the total size isn't divided by the
// block size
#[nexus_test]
async fn test_disk_reject_total_size_not_divisible_by_block_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Attempt to allocate the disk, observe a server error.
    let disk_size = ByteCount::from(3 * 1024 * 1024 * 1024 + 256);

    // Allocation should have room, it should fail due to disk_size not being
    // divisible by block size.
    assert!(
        disk_size.to_bytes()
            < u64::from(DiskTest::DEFAULT_ZPOOL_SIZE_GIB) * 1024 * 1024 * 1024
    );

    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

// Tests that a disk is rejected if the total size is less than MIN_DISK_SIZE
#[nexus_test]
async fn test_disk_reject_total_size_less_than_min_disk_size_bytes(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disk_size = ByteCount::from(MIN_DISK_SIZE_BYTES / 2);

    // Attempt to allocate the disk, observe a server error.
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "unsupported value for \"size\": total size must be at least {}",
            ByteCount::from(MIN_DISK_SIZE_BYTES)
        )
    );
}

#[nexus_test]
async fn test_disk_reject_total_size_greater_than_max_disk_size_bytes(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disk_size =
        ByteCount::try_from(MAX_DISK_SIZE_BYTES + (1 << 30)).unwrap();

    // Atempt to allocate the disk, observe a server error.
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "unsupported value for \"size\": total size must be less than {}",
            ByteCount::try_from(MAX_DISK_SIZE_BYTES).unwrap()
        )
    );
}

// Tests that a disk is rejected if the total size isn't divisible by
// MIN_DISK_SIZE_BYTES
#[nexus_test]
async fn test_disk_reject_total_size_not_divisible_by_min_disk_size(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disk_size = ByteCount::from(1024 * 1024 * 1024 + 512);

    // Attempt to allocate the disk, observe a server error.
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "unsupported value for \"size\": total size must be a multiple of {}",
            ByteCount::from(MIN_DISK_SIZE_BYTES)
        )
    );
}

// Test disks backed by multiple region sets (one region set being three regions)
#[nexus_test]
async fn test_disk_backed_by_multiple_region_sets(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create six zpools, each with one crucible dataset
    let _test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(6).build().await;

    // Assert default is still 16 GiB
    assert_eq!(16, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    create_project_and_pool(client).await;

    // Ask for a 20 gibibyte disk.
    let disk_size = ByteCount::from_gibibytes_u32(20);
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            // TODO: this fails! the current allocation algorithm does not split
            // across datasets
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_disk_too_big(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Assert default is still 16 GiB
    assert_eq!(16, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    // Ask for a 300 gibibyte disk (but only 16 is available)
    let disk_size = ByteCount::from_gibibytes_u32(300);
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_disk_virtual_provisioning_collection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let _test = DiskTest::new(&cptestctx).await;

    create_default_ip_pool(client).await;
    let project_id1 = create_project(client, PROJECT_NAME).await.identity.id;
    let project_id2 = create_project(client, PROJECT_NAME_2).await.identity.id;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // The project and organization should start as empty.
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id2)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, DEFAULT_SILO_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, *FLEET_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );

    // Ask for a 1 gibibyte disk in the first project.
    //
    // This disk should appear in the accounting information for the project
    // in which it was allocated
    let disk_size = ByteCount::from_gibibytes_u32(1);
    let disks_url = get_disks_url();
    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-one".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 1 GiB disk");
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id2)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, DEFAULT_SILO_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, *FLEET_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );

    // Ask for a 1 gibibyte disk in the second project.
    //
    // Each project should be using "one disk" of real storage, but the org
    // should be using both.
    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME_2);
    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-two".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 1 GiB disk");
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id2)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, DEFAULT_SILO_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        2 * disk_size.to_bytes()
    );

    // Delete the disk we just created, observe the utilization drop
    // accordingly.
    let disk_url =
        format!("/v1/disks/{}?project={}", "disk-two", PROJECT_NAME_2);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id2)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection
            .virtual_disk_bytes_provisioned
            .to_bytes(),
        0
    );
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, DEFAULT_SILO_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size,
    );
}

#[nexus_test]
async fn test_disk_virtual_provisioning_collection_failed_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    // Confirm that there's no panic deleting a project if a disk deletion fails
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let disk_test = DiskTest::new(&cptestctx).await;

    let project_id1 = create_project_and_pool(client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create a 1 GB disk
    let disk_size = ByteCount::from_gibibytes_u32(1);
    let disks_url = get_disks_url();
    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-one".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 1 GiB disk");

    // Get the disk
    let disk_url = format!("/v1/disks/{}?project={}", "disk-one", PROJECT_NAME);
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Assert correct virtual provisioning collection numbers
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );

    // Set the third agent to fail when deleting regions
    let zpool =
        &disk_test.zpools().nth(2).expect("Expected at least three zpools");
    let dataset = zpool.crucible_dataset();
    cptestctx
        .first_sled_agent()
        .get_crucible_dataset(zpool.id, dataset.id)
        .set_region_deletion_error(true);

    // Delete the disk - expect this to fail
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::INTERNAL_SERVER_ERROR)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success deleting 1 GiB disk");

    // The virtual provisioning collection numbers haven't changed
    let virtual_provisioning_collection = datastore
        .virtual_provisioning_collection_get(&opctx, project_id1)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size
    );

    // And the disk is now faulted. The name will have changed due to the
    // "undelete and fault" function.
    let disk_url = format!(
        "/v1/disks/deleted-{}?project={}",
        disk.identity.id, PROJECT_NAME
    );
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Faulted);

    // Set the third agent to respond normally
    cptestctx
        .first_sled_agent()
        .get_crucible_dataset(zpool.id, dataset.id)
        .set_region_deletion_error(false);

    // Request disk delete again
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting 1 GiB disk");

    // Delete the project's default VPC subnet and VPC
    let subnet_url =
        format!("/v1/vpc-subnets/default?project={}&vpc=default", PROJECT_NAME);
    NexusRequest::object_delete(&client, &subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    let vpc_url = format!("/v1/vpcs/default?project={}", PROJECT_NAME);
    NexusRequest::object_delete(&client, &vpc_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");

    // The project can be deleted now
    let url = format!("/v1/projects/{}", PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting project");
}

#[nexus_test]
async fn test_phantom_disk_rename(cptestctx: &ControlPlaneTestContext) {
    // Confirm that phantom disks are renamed when they are un-deleted and
    // faulted

    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let _disk_test = DiskTest::new(&cptestctx).await;

    let _project_id1 = create_project(client, PROJECT_NAME).await.identity.id;

    // Create a 1 GB disk
    let disk_size = ByteCount::from_gibibytes_u32(1);
    let disks_url = get_disks_url();
    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-one".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 1 GiB disk");

    let disk_url = format!("/v1/disks/{}?project={}", "disk-one", PROJECT_NAME);

    // Confirm it's there
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    let original_disk_id = disk.identity.id;

    // Now, request disk delete
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting 1 GiB disk");

    // It's gone!
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &disk_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success finding 1 GiB disk");

    // Create a new disk with the same name
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 1 GiB disk");

    // Confirm it's there
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Confirm it's not the same disk
    let new_disk_id = disk.identity.id;
    assert_ne!(original_disk_id, new_disk_id);

    // Un-delete the original and set it to faulted
    datastore
        .project_undelete_disk_set_faulted_no_auth(&original_disk_id)
        .await
        .unwrap();

    // The original disk is now faulted
    let disk_url = format!(
        "/v1/disks/deleted-{}?project={}",
        original_disk_id, PROJECT_NAME
    );
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Faulted);

    // Make sure original can still be deleted
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting 1 GiB disk");

    // Make sure new can be deleted too
    let disk_url = format!("/v1/disks/{}?project={}", "disk-one", PROJECT_NAME);
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting 1 GiB disk");
}

// Test disk size accounting
#[nexus_test]
async fn test_disk_size_accounting(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    // Create three zpools, each with one dataset.
    let test = DiskTest::new(&cptestctx).await;

    // Assert default is still 16 GiB
    assert_eq!(16, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    create_project_and_pool(client).await;

    // Total occupied size should start at 0
    for zpool in test.zpools() {
        let dataset = zpool.crucible_dataset();
        assert_eq!(
            datastore.regions_total_reserved_size(dataset.id).await.unwrap(),
            0
        );

        assert_eq!(
            datastore.crucible_dataset_get(dataset.id).await.unwrap().size_used,
            0,
        );
    }

    // Ask for a 7 gibibyte disk, this should succeed
    let disk_size = ByteCount::from_gibibytes_u32(7);
    let disks_url = get_disks_url();

    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-one".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 7 GiB disk");

    // Total occupied size is 7 GiB * 3 (each Crucible disk requires three
    // regions to make a region set for an Upstairs, one region per dataset)
    // plus reservation overhead
    for zpool in test.zpools() {
        let dataset = zpool.crucible_dataset();
        assert_eq!(
            datastore.regions_total_reserved_size(dataset.id).await.unwrap(),
            ByteCount::from_mebibytes_u32(8960).to_bytes(),
        );
    }

    // Ask for a 6 gibibyte disk, this should fail because there isn't space
    // available.
    let disk_size = ByteCount::from_gibibytes_u32(6);
    let disk_two = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-two".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_two))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success creating 6 GiB disk");

    // Total occupied size is still 7 GiB * 3 (plus overhead)
    for zpool in test.zpools() {
        let dataset = zpool.crucible_dataset();
        assert_eq!(
            datastore.regions_total_reserved_size(dataset.id).await.unwrap(),
            ByteCount::from_mebibytes_u32(8960).to_bytes(),
        );
    }

    // Delete the first disk, freeing up 7 gibibytes.
    let disk_url = get_disk_url("disk-one");
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure deleting 7 GiB disk");

    // Total occupied size should be 0
    for zpool in test.zpools() {
        let dataset = zpool.crucible_dataset();
        assert_eq!(
            datastore.regions_total_reserved_size(dataset.id).await.unwrap(),
            0,
        );
    }

    // Ask for a 10 gibibyte disk.
    let disk_size = ByteCount::from_gibibytes_u32(10);
    let disk_three = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-three".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_three))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected failure creating 10 GiB disk");

    // Total occupied size should be 10 GiB * 3 plus overhead
    for zpool in test.zpools() {
        let dataset = zpool.crucible_dataset();
        assert_eq!(
            datastore.regions_total_reserved_size(dataset.id).await.unwrap(),
            ByteCount::from_mebibytes_u32(12800).to_bytes(),
        );
    }
}

// Test creating two disks across six zpools
#[nexus_test]
async fn test_multiple_disks_multiple_zpools(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create six zpools, each with one dataset
    let _test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(6)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Ask for a 10 gibibyte disk, this should succeed
    let disk_size = ByteCount::from_gibibytes_u32(10);
    let disks_url = get_disks_url();

    let disk_one = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-one".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_one))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Ask for another 10 gibibyte disk
    let disk_size = ByteCount::from_gibibytes_u32(10);
    let disk_two = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk-two".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk_two))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn create_instance_with_disk(client: &ClientTestContext) {
    create_instance_with(
        &client,
        PROJECT_NAME,
        INSTANCE_NAME,
        &params::InstanceNetworkInterfaceAttachment::Default,
        vec![params::InstanceDiskAttachment::Attach(
            params::InstanceDiskAttach { name: DISK_NAME.parse().unwrap() },
        )],
        Vec::<params::ExternalIpCreate>::new(),
        true,
        Default::default(),
    )
    .await;
}

const ALL_METRICS: [&'static str; 6] =
    ["activated", "read", "write", "read_bytes", "write_bytes", "flush"];

#[nexus_test]
async fn test_disk_metrics(cptestctx: &ControlPlaneTestContext) {
    let metrics_querier = MetricsQuerier::new(cptestctx);
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let project_id = create_project_and_pool(client).await;
    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // When grabbing a metric, we look for data points going back to the
    // start of this test all the way up to the current time.
    let metric_url = |metric: &str| {
        format!(
            "/v1/disks/{}/metrics/{}?start_time={:?}&end_time={:?}&project={}",
            DISK_NAME,
            metric,
            cptestctx.start_time,
            Utc::now(),
            PROJECT_NAME,
        )
    };

    // Try accessing metrics before we attach the disk to an instance.
    //
    // Observe that no metrics exist yet; no "upstairs" should have been
    // instantiated on a sled.
    let measurements =
        objects_list_page_authz::<Measurement>(client, &metric_url("read"))
            .await;
    assert!(measurements.items.is_empty());

    metrics_querier
        .wait_for_latest_silo_metric(
            "virtual_disk_space_provisioned",
            Some(project_id),
            |measurement| {
                if measurement == i64::from(disk.size) {
                    Ok(())
                } else {
                    Err(MetricsNotYet::new(format!(
                        "waiting for virtual_disk_space_provisioned={} \
                         (currently {measurement})",
                        disk.size,
                    )))
                }
            },
        )
        .await;

    // Create an instance, attach the disk to it.
    create_instance_with_disk(client).await;
    wait_for_producer(&cptestctx.oximeter, disk.id()).await;

    for metric in &ALL_METRICS {
        metrics_querier
            .wait_for_disk_metric(PROJECT_NAME, DISK_NAME, metric, |items| {
                if items.is_empty() {
                    return Err(MetricsNotYet::new(format!(
                        "waiting for at least one item for metric={metric}"
                    )));
                }
                for item in &items {
                    let cumulative = match item.datum() {
                        Datum::CumulativeI64(c) => c,
                        _ => panic!("Unexpected datum type {:?}", item.datum()),
                    };
                    assert!(cumulative.start_time() <= item.timestamp());
                }
                Ok(())
            })
            .await;
    }

    // Check the utilization info for the whole project too.
    metrics_querier
        .wait_for_latest_silo_metric(
            "virtual_disk_space_provisioned",
            Some(project_id),
            |measurement| {
                if measurement == i64::from(disk.size) {
                    Ok(())
                } else {
                    Err(MetricsNotYet::new(format!(
                        "waiting for virtual_disk_space_provisioned={} \
                         (currently {measurement})",
                        disk.size,
                    )))
                }
            },
        )
        .await;
}

#[nexus_test]
async fn test_disk_metrics_paginated(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    create_instance_with_disk(client).await;
    wait_for_producer(&cptestctx.oximeter, disk.id()).await;

    let metrics_querier = MetricsQuerier::new(cptestctx);
    for metric in &ALL_METRICS {
        // Wait until we have at least two measurements.
        metrics_querier
            .wait_for_disk_metric(
                PROJECT_NAME,
                DISK_NAME,
                metric,
                |measurements| {
                    let num_measurements = measurements.len();
                    if num_measurements >= 2 {
                        Ok(())
                    } else {
                        Err(MetricsNotYet::new(format!(
                            "waiting for at least 2 measurements \
                             (currently {num_measurements})"
                        )))
                    }
                },
            )
            .await;

        let collection_url = format!(
            "/v1/disks/{}/metrics/{}?project={}",
            DISK_NAME, metric, PROJECT_NAME
        );
        let initial_params = format!(
            "start_time={:?}&end_time={:?}",
            cptestctx.start_time,
            Utc::now(),
        );

        let measurements_paginated: Collection<Measurement> =
            NexusRequest::iter_collection_authn(
                client,
                &collection_url,
                &initial_params,
                Some(10),
            )
            .await
            .expect("failed to iterate over metrics");
        assert!(!measurements_paginated.all_items.is_empty());

        let mut last_timestamp = None;
        let mut last_value = None;
        for item in &measurements_paginated.all_items {
            let cumulative = match item.datum() {
                Datum::CumulativeI64(c) => c,
                _ => panic!("Unexpected datum type {:?}", item.datum()),
            };
            assert!(cumulative.start_time() <= item.timestamp());

            // Validate that the timestamps are non-decreasing.
            if let Some(last_ts) = last_timestamp {
                assert!(last_ts <= item.timestamp());
            }
            // Validate that the values increase.
            if let Some(last_value) = last_value {
                assert!(last_value < cumulative.value());
            }

            last_timestamp = Some(item.timestamp());
            last_value = Some(cumulative.value());
        }
    }
}

#[nexus_test]
async fn test_disk_create_for_importing(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::ImportingBlocks {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(1),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Disk>()
    .unwrap();

    let disk_url = get_disk_url(DISK_NAME);
    let disk = disk_get(&client, &disk_url).await;

    assert_eq!(disk.state, DiskState::ImportReady);

    // List disks again and expect to find the one we just created.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 1);
    disks_eq(&disks[0], &disk);
}

#[nexus_test]
async fn test_project_delete_disk_no_auth_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    // Create a disk
    let disks_url = get_disks_url();

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(1),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<Disk>()
    .unwrap();

    let disk_url = get_disk_url(DISK_NAME);
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Call project_delete_disk_no_auth twice, ensuring that the disk is either
    // there before deleting and not afterwards.

    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    assert_eq!(db_disk.id(), disk.identity.id);

    datastore
        .project_delete_disk_no_auth(
            &disk.identity.id,
            &[DiskState::Detached, DiskState::Faulted],
        )
        .await
        .unwrap();

    let r = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await;

    assert!(r.is_err());

    datastore
        .project_delete_disk_no_auth(
            &disk.identity.id,
            &[DiskState::Detached, DiskState::Faulted],
        )
        .await
        .unwrap();
}

// Test allocating a single region
#[nexus_test]
async fn test_single_region_allocate(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create three zpools, each with one dataset.
    let disk_test = DiskTest::new(&cptestctx).await;

    // Allocate a single 1 GB region
    let volume_id = VolumeUuid::new_v4();

    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            1,
        )
        .await
        .unwrap();

    assert_eq!(datasets_and_regions.len(), 1);

    // Double check!
    let allocated_regions =
        datastore.get_allocated_regions(volume_id).await.unwrap();

    assert_eq!(allocated_regions.len(), 1);

    // Triple check!
    let allocated_region =
        datastore.get_region(datasets_and_regions[0].1.id()).await.unwrap();
    assert_eq!(allocated_region.block_size().to_bytes(), 512);
    assert_eq!(allocated_region.blocks_per_extent(), 131072); // based on EXTENT_SIZE const
    assert_eq!(allocated_region.extent_count(), 16);

    // Quadruple check! Only one Crucible agent should have received a region
    // request
    let mut number_of_matching_regions = 0;

    for zpool in disk_test.zpools() {
        let dataset = zpool.crucible_dataset();
        let total_size =
            datastore.regions_total_reserved_size(dataset.id).await.unwrap();

        if total_size == allocated_region.reserved_size() {
            number_of_matching_regions += 1;
        } else if total_size == 0 {
            // ok, unallocated
        } else {
            panic!("unexpected regions total size of {total_size}");
        }
    }

    assert_eq!(number_of_matching_regions, 1);
}

// Ensure that `disk_region_allocate` is idempotent.
#[nexus_test]
async fn test_region_allocation_strategy_random_is_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset.
    let _test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Assert disk has three allocated regions
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), REGION_REDUNDANCY_THRESHOLD);

    // Call `disk_region_allocate` again
    let region: &nexus_db_model::Region = &allocated_regions[0].1;

    let region_total_size: ByteCount = ByteCount::try_from(
        region.block_size().to_bytes()
            * region.blocks_per_extent()
            * region.extent_count(),
    )
    .unwrap();

    assert_eq!(region_total_size, ByteCount::from_gibibytes_u32(1));

    let datasets_and_regions = datastore
        .disk_region_allocate(
            &opctx,
            db_disk.volume_id(),
            &params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(
                    region.block_size().to_bytes() as u32,
                )
                .unwrap(),
            },
            region_total_size,
            &RegionAllocationStrategy::Random { seed: None },
        )
        .await
        .unwrap();

    // There should be the same amount
    assert_eq!(allocated_regions.len(), datasets_and_regions.len());
}

// Ensure that adjusting redundancy level with `arbitrary_region_allocate` works
#[nexus_test]
async fn test_region_allocation_strategy_random_is_idempotent_arbitrary(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset.
    let _test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    // Call region allocation in isolation
    let volume_id = VolumeUuid::new_v4();

    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await
        .unwrap();

    // There should be the same amount as we requested
    assert_eq!(REGION_REDUNDANCY_THRESHOLD, datasets_and_regions.len());

    // Bump up the number of required regions
    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            REGION_REDUNDANCY_THRESHOLD + 1,
        )
        .await
        .unwrap();

    // There should be the same amount as we requested
    assert_eq!(REGION_REDUNDANCY_THRESHOLD + 1, datasets_and_regions.len());
}

// Test allocating a single region to replace a disk's region
#[nexus_test]
async fn test_single_region_allocate_for_replace(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset.
    //
    // We add one more then the "three" default to meet `region_allocate`'s
    // redundancy requirements.
    let _test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Assert disk has three allocated regions
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), REGION_REDUNDANCY_THRESHOLD);

    // Allocate one more single 1 GB region to replace one of the disk's regions
    let region_to_replace: &nexus_db_model::Region = &allocated_regions[0].1;

    let one_more = allocated_regions.len() + 1;
    assert_eq!(one_more, REGION_REDUNDANCY_THRESHOLD + 1);

    let region_total_size: ByteCount = ByteCount::try_from(
        region_to_replace.block_size().to_bytes()
            * region_to_replace.blocks_per_extent()
            * region_to_replace.extent_count(),
    )
    .unwrap();

    assert_eq!(region_total_size, ByteCount::from_gibibytes_u32(1));

    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id: db_disk.volume_id() },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        region_to_replace.block_size().to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: region_total_size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            one_more,
        )
        .await
        .unwrap();

    eprintln!("{:?}", datasets_and_regions);

    assert_eq!(datasets_and_regions.len(), one_more);

    // There should be `one_more` regions for this disk's volume id.
    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), one_more);

    // Each region should be on a different pool
    let pools_used: HashSet<Uuid> = datasets_and_regions
        .iter()
        .map(|(dataset, _)| dataset.pool_id)
        .collect();

    assert_eq!(pools_used.len(), REGION_REDUNDANCY_THRESHOLD + 1);
}

// Confirm allocating a single region to replace a disk's region fails if
// there's not enough unique zpools
#[nexus_test]
async fn test_single_region_allocate_for_replace_not_enough_zpools(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create three zpools, each with one dataset.
    let _disk_test = DiskTest::new(&cptestctx).await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Assert disk has three allocated regions
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), REGION_REDUNDANCY_THRESHOLD);

    // Allocate one more single 1 GB region to replace one of the disk's regions
    let region_to_replace: &nexus_db_model::Region = &allocated_regions[0].1;

    let one_more = allocated_regions.len() + 1;
    assert_eq!(one_more, REGION_REDUNDANCY_THRESHOLD + 1);

    let region_total_size: ByteCount = ByteCount::try_from(
        region_to_replace.block_size().to_bytes()
            * region_to_replace.blocks_per_extent()
            * region_to_replace.extent_count(),
    )
    .unwrap();

    assert_eq!(region_total_size, ByteCount::from_gibibytes_u32(1));

    // Trying to allocate one more should fail
    let result = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id: db_disk.volume_id() },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        region_to_replace.block_size().to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: region_total_size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            one_more,
        )
        .await;

    assert!(result.is_err());

    // Confirm calling `arbitrary_region_allocate` still idempotently works
    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::DiskVolume { volume_id: db_disk.volume_id() },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        region_to_replace.block_size().to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: region_total_size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            allocated_regions.len(),
        )
        .await
        .unwrap();

    assert_eq!(datasets_and_regions.len(), REGION_REDUNDANCY_THRESHOLD);
}

#[nexus_test]
async fn test_no_halt_disk_delete_one_region_on_expunged_agent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create the regular three 10 GiB zpools, each with one dataset.
    let disk_test = DiskTest::new(&cptestctx).await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Grab the db record now, before the delete
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk.identity.id)
        .fetch()
        .await
        .unwrap();

    // Choose one of the datasets, and drop the simulated Crucible agent
    let zpool = disk_test.zpools().next().expect("Expected at least one zpool");
    let dataset = zpool.crucible_dataset();

    cptestctx.first_sled_agent().drop_dataset(zpool.id, dataset.id);

    // Spawn a task that tries to delete the disk
    let disk_url = get_disk_url(DISK_NAME);
    let client = client.clone();

    let (task_started_tx, task_started_rx) = oneshot::channel();

    let jh = tokio::spawn(async move {
        task_started_tx.send(()).unwrap();

        NexusRequest::object_delete(&client, &disk_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete disk");
    });

    // Wait until the task starts
    task_started_rx.await.unwrap();

    // It won't finish until the dataset is expunged.
    assert!(!jh.is_finished());

    // Expunge the physical disk
    let (_, db_zpool) = LookupPath::new(&opctx, datastore)
        .zpool_id(zpool.id.into_untyped_uuid())
        .fetch()
        .await
        .unwrap();

    datastore
        .physical_disk_update_policy(
            &opctx,
            db_zpool.physical_disk_id.into(),
            PhysicalDiskPolicy::Expunged,
        )
        .await
        .unwrap();

    // Now, the delete call will finish Ok
    jh.await.unwrap();

    // Ensure that the disk was properly deleted and all the regions are gone -
    // Nexus should hard delete the region records in this case.

    let datasets_and_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert!(datasets_and_regions.is_empty());
}

#[nexus_test]
async fn test_disk_expunge(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create three zpools, each with one dataset.
    let _disk_test = DiskTest::new(&cptestctx).await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Assert disk has three allocated regions
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), REGION_REDUNDANCY_THRESHOLD);

    // Expunge the sled
    let int_client = &cptestctx.internal_client;
    int_client
        .make_request(
            Method::POST,
            "/sleds/expunge",
            Some(params::SledSelector {
                sled: SLED_AGENT_UUID.parse().unwrap(),
            }),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // All three regions should be returned
    let expunged_regions = datastore
        .find_read_write_regions_on_expunged_physical_disks(&opctx)
        .await
        .unwrap();

    assert_eq!(expunged_regions.len(), 3);
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_do_not_provision_on_dataset(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool, each with one dataset, on all the sleds
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // For one of the datasets, mark it as not provisionable
    let dataset = disk_test.zpools().next().unwrap().crucible_dataset();

    datastore
        .mark_crucible_dataset_not_provisionable(&opctx, dataset.id)
        .await
        .unwrap();

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

    // Assert no region was allocated to the marked dataset
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    for (allocated_region_dataset, _) in allocated_regions {
        assert_ne!(allocated_region_dataset.id(), dataset.id);
    }
}

#[nexus_test(extra_sled_agents = 2)]
async fn test_do_not_provision_on_dataset_not_enough(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool, each with one dataset, on all the sleds
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // For one of the datasets, mark it as not provisionable
    let dataset = disk_test.zpools().next().unwrap().crucible_dataset();

    datastore
        .mark_crucible_dataset_not_provisionable(&opctx, dataset.id)
        .await
        .unwrap();

    // Because there's only 3 sled agents, each with one zpool with one dataset,
    // this shouldn't be enough to create a disk.

    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disks_url = get_disks_url();

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(1),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Marking that dataset as provisionable should allow the disk to be
    // created.

    datastore
        .mark_crucible_dataset_provisionable(&opctx, dataset.id)
        .await
        .unwrap();

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test(extra_sled_agents = 2)]
async fn test_zpool_control_plane_storage_buffer(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool, each with one dataset, on all the sleds
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Assert default is still 16 GiB
    assert_eq!(16, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disks_url = get_disks_url();

    // Creating a 8G disk will work (10G size used due to reservation overhead)
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk1".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(8),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Creating a 4G disk will also work (5G size used due to reservation
    // overhead plus the previous 10G size used is less than 16G)
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk2".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(4),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Delete the 4G disk
    let disk_url = get_disk_url("disk2");
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // For any of the zpools, set the control plane storage buffer to 2G. This
    // should prevent the disk's region allocation from succeeding (as the
    // reserved sizes of 10G + 5G plus the storage buffer of 2G is 1G over the
    // the backing pool's 16G).

    let zpool = &disk_test.zpools().next().unwrap();
    datastore
        .zpool_set_control_plane_storage_buffer(
            &opctx,
            zpool.id,
            ByteCount::from_gibibytes_u32(2).into(),
        )
        .await
        .unwrap();

    // Now creating the 4G disk should fail

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Setting the storage buffer to 1G should allow the disk creation to
    // succeed.

    datastore
        .zpool_set_control_plane_storage_buffer(
            &opctx,
            zpool.id,
            ByteCount::from_gibibytes_u32(1).into(),
        )
        .await
        .unwrap();

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn disk_get(client: &ClientTestContext, disk_url: &str) -> Disk {
    NexusRequest::object_get(client, disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn disks_list(client: &ClientTestContext, list_url: &str) -> Vec<Disk> {
    NexusRequest::iter_collection_authn(client, list_url, "", None)
        .await
        .expect("failed to list disks")
        .all_items
}

async fn disk_post(
    client: &ClientTestContext,
    url: &str,
    disk_name: Name,
) -> Disk {
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, url)
            .body(Some(&params::DiskPath { disk: disk_name.into() }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

fn disks_eq(disk1: &Disk, disk2: &Disk) {
    identity_eq(&disk1.identity, &disk2.identity);
    assert_eq!(disk1.project_id, disk2.project_id);
    assert_eq!(disk1.snapshot_id, disk2.snapshot_id);
    assert_eq!(disk1.image_id, disk2.image_id);
    assert_eq!(disk1.size.to_bytes(), disk2.size.to_bytes());
    assert_eq!(disk1.block_size.to_bytes(), disk2.block_size.to_bytes());
    assert_eq!(disk1.state, disk2.state);
    assert_eq!(disk1.device_path, disk2.device_path);
}
