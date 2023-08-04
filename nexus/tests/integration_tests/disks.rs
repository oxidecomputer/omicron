// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic disk support in the API

use super::metrics::{get_latest_silo_metric, query_for_metrics};
use chrono::Utc;
use crucible_agent_client::types::State as RegionState;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::Collection;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::populate_ip_pool;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use omicron_nexus::app::{MAX_DISK_SIZE_BYTES, MIN_DISK_SIZE_BYTES};
use omicron_nexus::db::fixed_data::{silo::SILO_ID, FLEET_ID};
use omicron_nexus::db::lookup::LookupPath;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::{external_api::params, Nexus};
use oximeter::types::Datum;
use oximeter::types::Measurement;
use sled_agent_client::TestInterfaces as _;
use std::sync::Arc;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    populate_ip_pool(&client, "default", None).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_disk_not_found_before_creation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_org_and_project(client).await;
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

async fn instance_simulate(nexus: &Arc<Nexus>, id: &Uuid) {
    let sa = nexus.instance_sled_by_id(id).await.unwrap();
    sa.instance_finish_transition(*id).await;
}

#[nexus_test]
async fn test_disk_create_attach_detach_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let project_id = create_org_and_project(client).await;
    let nexus = &cptestctx.server.apictx().nexus;
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

    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    let instance_next =
        set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

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
    create_org_and_project(client).await;
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
    create_org_and_project(client).await;
    let nexus = &cptestctx.server.apictx().nexus;

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
    let instance_id = &instance.identity.id;
    let instance_next =
        set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let listed_disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(listed_disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(&instance.identity.id.into());

    async fn get_disk_slot(ctx: &ControlPlaneTestContext, disk_id: Uuid) -> u8 {
        let apictx = &ctx.server.apictx();
        let nexus = &apictx.nexus;
        let datastore = nexus.datastore();
        let opctx =
            OpContext::for_tests(ctx.logctx.log.new(o!()), datastore.clone());

        let (.., db_disk) = LookupPath::new(&opctx, &datastore)
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
        assert_eq!(attached_disk.state, DiskState::Attached(*instance_id));

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
    let nexus = &cptestctx.server.apictx().nexus;
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = get_disk_url(DISK_NAME);
    let disk = create_disk(client, PROJECT_NAME, DISK_NAME).await;

    // Create an instance to attach the disk.
    let instance = create_instance(&client, PROJECT_NAME, INSTANCE_NAME).await;
    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    let instance_next =
        set_instance_state(&client, INSTANCE_NAME, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

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
    let instance_next = set_instance_state(&client, "instance2", "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

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
    assert_eq!(attached_disk.state, DiskState::Attached(*instance_id));

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
    let instance2_id = &instance2.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(*instance2_id));

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
    assert_eq!(disk.state, DiskState::Attached(*instance2_id));

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
    create_org_and_project(client).await;

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
    create_org_and_project(client).await;

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

    // After the failed allocation, the disk should not exist.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 0);

    // After the failed allocation, regions will exist, but be "Failed".
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            let crucible = test
                .sled_agent
                .get_crucible_dataset(zpool.id, dataset.id)
                .await;
            let regions = crucible.list().await;
            assert_eq!(regions.len(), 1);
            assert_eq!(regions[0].state, RegionState::Failed);
        }
    }

    // Validate that the underlying regions were released as a part of
    // unwinding the failed disk allocation, by performing another disk
    // allocation that should succeed.
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            let crucible = test
                .sled_agent
                .get_crucible_dataset(zpool.id, dataset.id)
                .await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;
        }
    }
    let _ = create_disk(client, PROJECT_NAME, DISK_NAME).await;
}

// Tests that invalid block sizes are rejected
#[nexus_test]
async fn test_disk_invalid_block_size_rejected(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _test = DiskTest::new(&cptestctx).await;
    create_org_and_project(client).await;

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
    create_org_and_project(client).await;

    // Attempt to allocate the disk, observe a server error.
    let disk_size = ByteCount::from(3 * 1024 * 1024 * 1024 + 256);

    // Allocation should have room, it should fail due to disk_size not being
    // divisible by block size.
    assert!(
        disk_size.to_bytes()
            < DiskTest::DEFAULT_ZPOOL_SIZE_GIB as u64 * 1024 * 1024 * 1024
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
    create_org_and_project(client).await;

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
    create_org_and_project(client).await;

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
    create_org_and_project(client).await;

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

    // Create three zpools, all 10 gibibytes, each with one dataset
    let mut test = DiskTest::new(&cptestctx).await;

    // Assert default is still 10 GiB
    assert_eq!(10, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    // Create another three zpools, all 10 gibibytes, each with one dataset
    test.add_zpool_with_dataset(cptestctx, 10).await;
    test.add_zpool_with_dataset(cptestctx, 10).await;
    test.add_zpool_with_dataset(cptestctx, 10).await;

    create_org_and_project(client).await;

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
            .expect_status(Some(StatusCode::SERVICE_UNAVAILABLE)),
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
    create_org_and_project(client).await;

    // Assert default is still 10 GiB
    assert_eq!(10, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    // Ask for a 300 gibibyte disk (but only 10 is available)
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
            .expect_status(Some(StatusCode::SERVICE_UNAVAILABLE)),
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let _test = DiskTest::new(&cptestctx).await;

    populate_ip_pool(&client, "default", None).await;
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
        .virtual_provisioning_collection_get(&opctx, *SILO_ID)
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
        .virtual_provisioning_collection_get(&opctx, *SILO_ID)
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
        .virtual_provisioning_collection_get(&opctx, *SILO_ID)
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
        .virtual_provisioning_collection_get(&opctx, *SILO_ID)
        .await
        .unwrap();
    assert_eq!(
        virtual_provisioning_collection.virtual_disk_bytes_provisioned.0,
        disk_size,
    );
}

// Test disk size accounting
#[nexus_test]
async fn test_disk_size_accounting(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    // Create three 10 GiB zpools, each with one dataset.
    let test = DiskTest::new(&cptestctx).await;

    // Assert default is still 10 GiB
    assert_eq!(10, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    create_org_and_project(client).await;

    // Total occupied size should start at 0
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            assert_eq!(
                datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap(),
                0
            );
        }
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
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            assert_eq!(
                datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap(),
                ByteCount::from_gibibytes_u32(7).to_bytes(),
            );
        }
    }

    // Ask for a 4 gibibyte disk, this should fail because there isn't space
    // available.
    let disk_size = ByteCount::from_gibibytes_u32(4);
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
            .expect_status(Some(StatusCode::SERVICE_UNAVAILABLE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success creating 4 GiB disk");

    // Total occupied size is still 7 GiB * 3
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            assert_eq!(
                datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap(),
                ByteCount::from_gibibytes_u32(7).to_bytes(),
            );
        }
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
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            assert_eq!(
                datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap(),
                0,
            );
        }
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

    // Total occupied size should be 10 GiB * 3
    for zpool in &test.zpools {
        for dataset in &zpool.datasets {
            assert_eq!(
                datastore
                    .regions_total_occupied_size(dataset.id)
                    .await
                    .unwrap(),
                ByteCount::from_gibibytes_u32(10).to_bytes(),
            );
        }
    }
}

// Test creating two disks across six zpools
#[nexus_test]
async fn test_multiple_disks_multiple_zpools(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create six 10 GB zpools, each with one dataset
    let mut test = DiskTest::new(&cptestctx).await;

    // Assert default is still 10 GiB
    assert_eq!(10, DiskTest::DEFAULT_ZPOOL_SIZE_GIB);

    test.add_zpool_with_dataset(cptestctx, 10).await;
    test.add_zpool_with_dataset(cptestctx, 10).await;
    test.add_zpool_with_dataset(cptestctx, 10).await;

    create_org_and_project(client).await;

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
    )
    .await;
}

const ALL_METRICS: [&'static str; 6] =
    ["activated", "read", "write", "read_bytes", "write_bytes", "flush"];

#[nexus_test]
async fn test_disk_metrics(cptestctx: &ControlPlaneTestContext) {
    // Normally, Nexus is not registered as a producer for tests.
    // Turn this bit on so we can also test some metrics from Nexus itself.
    cptestctx.server.register_as_producer().await;

    let oximeter = &cptestctx.oximeter;
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let project_id = create_org_and_project(client).await;
    let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    oximeter.force_collect().await;

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

    assert_eq!(
        get_latest_silo_metric(
            cptestctx,
            "virtual_disk_space_provisioned",
            Some(project_id),
        )
        .await,
        i64::from(disk.size)
    );

    // Create an instance, attach the disk to it.
    create_instance_with_disk(client).await;
    oximeter.force_collect().await;

    for metric in &ALL_METRICS {
        let measurements = query_for_metrics(client, &metric_url(metric)).await;

        assert!(!measurements.items.is_empty());
        for item in &measurements.items {
            let cumulative = match item.datum() {
                Datum::CumulativeI64(c) => c,
                _ => panic!("Unexpected datum type {:?}", item.datum()),
            };
            assert!(cumulative.start_time() <= item.timestamp());
        }
    }

    // Check the utilization info for the whole project too.
    assert_eq!(
        get_latest_silo_metric(
            cptestctx,
            "virtual_disk_space_provisioned",
            Some(project_id),
        )
        .await,
        i64::from(disk.size)
    );
}

#[nexus_test]
async fn test_disk_metrics_paginated(cptestctx: &ControlPlaneTestContext) {
    // Normally, Nexus is not registered as a producer for tests.
    // Turn this bit on so we can also test some metrics from Nexus itself.
    cptestctx.server.register_as_producer().await;

    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_org_and_project(client).await;
    create_disk(&client, PROJECT_NAME, DISK_NAME).await;
    create_instance_with_disk(client).await;

    let oximeter = &cptestctx.oximeter;
    oximeter.force_collect().await;
    for metric in &ALL_METRICS {
        let collection_url = format!(
            "/v1/disks/{}/metrics/{}?project={}",
            DISK_NAME, metric, PROJECT_NAME
        );
        let initial_params = format!(
            "start_time={:?}&end_time={:?}",
            cptestctx.start_time,
            Utc::now(),
        );

        objects_list_page_authz::<Measurement>(
            client,
            &format!("{collection_url}&{initial_params}"),
        )
        .await;

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
    create_org_and_project(client).await;
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
