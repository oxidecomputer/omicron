// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic disk support in the API

use crucible_agent_client::types::State as RegionState;
use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::{external_api::params, Nexus};
use omicron_sled_agent::sim::SledAgent;
use sled_agent_client::TestInterfaces as _;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::read_json;
use dropshot::test_util::ClientTestContext;

use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

const ORG_NAME: &str = "test-org";
const PROJECT_NAME: &str = "springfield-squidport-disks";
const DISK_NAME: &str = "just-rainsticks";

fn get_project_url() -> String {
    format!("/organizations/{}/projects/{}", ORG_NAME, PROJECT_NAME)
}

fn get_disks_url() -> String {
    format!("{}/disks", get_project_url())
}

fn get_instances_url() -> String {
    format!("{}/instances", get_project_url())
}

fn get_instance_disks_url(instance_name: &str) -> String {
    format!("{}/{}/disks", get_instances_url(), instance_name)
}

fn get_disk_attach_url(instance_name: &str) -> String {
    format!("{}/attach", get_instance_disks_url(instance_name))
}

fn get_disk_detach_url(instance_name: &str) -> String {
    format!("{}/detach", get_instance_disks_url(instance_name))
}

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    create_organization(&client, ORG_NAME).await;
    let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
    project.identity.id
}

struct DiskTest {
    sled_agent: Arc<SledAgent>,
    zpool_id: Uuid,
    zpool_size: ByteCount,
    dataset_ids: Vec<Uuid>,
    project_id: Uuid,
}

impl DiskTest {
    // Creates fake physical storage, an organization, and a project.
    async fn new(cptestctx: &ControlPlaneTestContext) -> Self {
        let client = &cptestctx.external_client;
        let sled_agent = cptestctx.sled_agent.sled_agent.clone();

        // Create a Zpool.
        let zpool_id = Uuid::new_v4();
        let zpool_size = ByteCount::from_gibibytes_u32(10);
        sled_agent.create_zpool(zpool_id, zpool_size.to_bytes()).await;

        // Create multiple Datasets within that Zpool.
        let dataset_count = 3;
        let dataset_ids: Vec<_> =
            (0..dataset_count).map(|_| Uuid::new_v4()).collect();
        for id in &dataset_ids {
            sled_agent.create_crucible_dataset(zpool_id, *id).await;

            // By default, regions are created immediately.
            let crucible = sled_agent.get_crucible_dataset(zpool_id, *id).await;
            crucible
                .set_create_callback(Box::new(|_| RegionState::Created))
                .await;
        }

        // Create a project for testing.
        let project_id = create_org_and_project(&client).await;

        Self { sled_agent, zpool_id, zpool_size, dataset_ids, project_id }
    }
}

#[nexus_test]
async fn test_disk_not_found_before_creation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();

    // List disks.  There aren't any yet.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 0);

    // Make sure we get a 404 if we fetch one.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
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

#[nexus_test]
async fn test_disk_create_attach_detach_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;
    let nexus = &cptestctx.server.apictx.nexus;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let disk: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;
    assert_eq!(disk.identity.name, DISK_NAME);
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, test.project_id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.state, DiskState::Creating);

    // Fetch the disk and expect it to match what we just created except that
    // the state will now be "Detached", as the server has simulated the create
    // process.
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.identity.name, DISK_NAME);
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, test.project_id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.state, DiskState::Detached);

    // List disks again and expect to find the one we just created.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 1);
    disks_eq(&disks[0], &disk);

    // Create an instance to attach the disk.
    let url_instances = get_instances_url();
    let instance: Instance = objects_post(
        &client,
        &url_instances,
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "instance1".parse().unwrap(),
                description: "instance1".to_string(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: "instance1".to_string(),
        },
    )
    .await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = objects_list_page::<Disk>(&client, &url_instance_disks).await;
    assert_eq!(disks.items.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(instance.identity.name.as_str());
    let url_instance_detach_disk =
        get_disk_detach_url(instance.identity.name.as_str());

    // Start attaching the disk to the instance.
    let mut response = client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let attached_disk: Disk = read_json(&mut response).await;
    let instance_id = &instance.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attaching(instance_id.clone()));

    // Finish simulation of the attachment and verify the new state, both on the
    // attachment and the disk itself.
    disk_simulate(nexus, &disk.identity.id).await;
    let attached_disk: Disk = disk_get(&client, &disk_url).await;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    // Begin detaching the disk.
    client
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk: Disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance_id.clone()));

    // Finish the detachment.
    disk_simulate(nexus, &disk.identity.id).await;
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Since detach is idempotent, we can detach it again.
    client
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();

    // A priveleged user should be able to delete the disk.
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // It should no longer be present in our list of disks.
    assert_eq!(disks_list(&client, &disks_url).await.len(), 0);

    // We shouldn't find it if we request it explicitly.
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
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
    let disks_url = get_disks_url();

    // Create a disk.
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let _: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let disk = disk_get(&client, &disk_url).await;

    // Attempt to create a second disk with a conflicting name.
    let error = client
        .make_request_error_body(
            Method::POST,
            &disks_url,
            new_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
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
async fn test_disk_move_between_instances(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;
    DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let disk: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;

    // Create an instance to attach the disk.
    let url_instances = get_instances_url();
    let instance: Instance = objects_post(
        &client,
        &url_instances,
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: DISK_NAME.parse().unwrap(),
                description: String::from("sells rainsticks"),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: String::from("rainsticks"),
        },
    )
    .await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = objects_list_page::<Disk>(&client, &url_instance_disks).await;
    assert_eq!(disks.items.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(instance.identity.name.as_str());
    let url_instance_detach_disk =
        get_disk_detach_url(instance.identity.name.as_str());

    // Start attaching the disk to the instance.
    let mut response = client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let attached_disk: Disk = read_json(&mut response).await;
    let instance_id = &instance.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attaching(instance_id.clone()));

    // Finish simulation of the attachment and verify the new state, both on the
    // attachment and the disk itself.
    disk_simulate(nexus, &disk.identity.id).await;
    let attached_disk: Disk = disk_get(&client, &disk_url).await;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    // Create a second instance and try to attach the disk to that.  This should
    // fail and the disk should remain attached to the first instance.
    let instance2: Instance = objects_post(
        &client,
        &url_instances,
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "instance2".parse().unwrap(),
                description: "instance2".to_string(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: "instance2".to_string(),
        },
    )
    .await;
    let url_instance2_attach_disk =
        get_disk_attach_url(instance2.identity.name.as_str());
    let url_instance2_detach_disk =
        get_disk_detach_url(instance2.identity.name.as_str());
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance2_attach_disk,
            params::DiskIdentifier { disk: disk.identity.name.clone() },
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another \
         instance",
            DISK_NAME
        )
    );

    let attached_disk = disk_get(&client, &disk_url).await;
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    // Begin detaching the disk.
    client
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk: Disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance_id.clone()));

    // It's still illegal to attach this disk elsewhere.
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance2_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another \
         instance",
            DISK_NAME
        )
    );

    // It's even illegal to attach this disk back to the same instance.
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::BAD_REQUEST,
        )
        .await;
    // TODO-debug the error message here is misleading.
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another \
         instance",
            DISK_NAME
        )
    );

    // However, there's no problem attempting to detach it again.
    client
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance_id.clone()));

    // Finish the detachment.
    disk_simulate(nexus, &disk.identity.id).await;
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    // Since delete is idempotent, we can detach it again -- from either one.
    client
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    client
        .make_request(
            Method::POST,
            &url_instance2_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();

    // Now, start attaching it again to the second instance.
    let mut response = client
        .make_request(
            Method::POST,
            &url_instance2_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let attached_disk: Disk = read_json(&mut response).await;
    let instance2_id = &instance2.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attaching(instance2_id.clone()));

    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attaching(instance2_id.clone()));

    // At this point, it's not legal to attempt to attach it to a different
    // instance (the first one).
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        format!(
            "cannot attach disk \"{}\": disk is attached to another \
         instance",
            DISK_NAME
        )
    );

    // It's fine to attempt another attachment to the same instance.
    client
        .make_request(
            Method::POST,
            &url_instance2_attach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attaching(instance2_id.clone()));

    // It's not allowed to delete a disk that's attaching.
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected request to fail")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("cannot parse");
    assert_eq!(error.message, "disk cannot be deleted in state \"attaching\"");

    // Now, begin a detach while the disk is still being attached.
    client
        .make_request(
            Method::POST,
            &url_instance2_detach_disk,
            Some(params::DiskIdentifier { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let disk: Disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance2_id.clone()));

    // It's not allowed to delete a disk that's detaching, either.
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected request to fail")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .expect("cannot parse");
    assert_eq!(error.message, "disk cannot be deleted in state \"detaching\"");

    // Finish detachment.
    disk_simulate(nexus, &disk.identity.id).await;
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // It should no longer be present in our list of disks.
    assert_eq!(disks_list(&client, &disks_url).await.len(), 0);

    // We shouldn't find it if we request it explicitly.
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(
        error.message,
        format!("not found: disk with name \"{}\"", DISK_NAME)
    );
}

#[nexus_test]
async fn test_disk_deletion_requires_authentication(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let _: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;

    const BAD_DISK_NAME: &str = "wonderful-knife";
    let bad_disk_url = format!("{}/{}", disks_url, BAD_DISK_NAME);

    // If we are not authenticated, we should not be able to delete the disk.
    //
    // We should see the same error regardless of the existence of the disk.
    let urls = [&disk_url, &bad_disk_url];
    for url in &urls {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            Method::DELETE,
            &url,
        )
        .execute()
        .await
        .expect("expected request to fail");
    }

    // If we are unprivileged, we should not be able to delete the disk.
    //
    // We should see the same error regardless of the existence of the disk.
    for url in &urls {
        NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            Method::DELETE,
            &url,
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .expect("expected request to fail");
    }

    // Privileged users can delete disks.
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");
}

#[nexus_test]
async fn test_disk_creation_region_requested_then_started(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();

    // Before we create a disk, set the response from the Crucible Agent:
    // no matter what regions get requested, they'll always *start* as
    // "Requested", and transition to "Created" on the second call.
    for id in &test.dataset_ids {
        let crucible =
            test.sled_agent.get_crucible_dataset(test.zpool_id, *id).await;
        let called = std::sync::atomic::AtomicBool::new(false);
        crucible
            .set_create_callback(Box::new(move |_| {
                if !called.load(std::sync::atomic::Ordering::SeqCst) {
                    called.store(true, std::sync::atomic::Ordering::SeqCst);
                    RegionState::Requested
                } else {
                    RegionState::Created
                }
            }))
            .await;
    }

    // The disk is created successfully, even when this "requested" -> "started"
    // transition occurs.
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let _: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;
}

// Tests that region allocation failure causes disk allocation to fail.
#[nexus_test]
async fn test_disk_region_creation_failure(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;

    // Before we create a disk, set the response from the Crucible Agent:
    // no matter what regions get requested, they'll always fail.
    for id in &test.dataset_ids {
        let crucible =
            test.sled_agent.get_crucible_dataset(test.zpool_id, *id).await;
        crucible.set_create_callback(Box::new(|_| RegionState::Failed)).await;
    }

    let disk_size = ByteCount::from_gibibytes_u32(3);
    let dataset_count = test.dataset_ids.len() as u64;
    assert!(
        disk_size.to_bytes() * dataset_count < test.zpool_size.to_bytes(),
        "Disk size too big for Zpool size"
    );
    assert!(
        2 * disk_size.to_bytes() * dataset_count > test.zpool_size.to_bytes(),
        "(test constraint) Zpool needs to be smaller (to store only one disk)",
    );

    // Attempt to allocate the disk, observe a server error.
    let disks_url = get_disks_url();
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: DISK_NAME.parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: disk_size,
    };

    // Unfortunately, the error message is only posted internally to the
    // logs, and it not returned to the client.
    //
    // TODO: Maybe consider making this a more informative error?
    // How should we propagate this to the client?
    client
        .make_request_error_body(
            Method::POST,
            &disks_url,
            new_disk.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;

    // After the failed allocation, the disk should not exist.
    let disks = disks_list(&client, &disks_url).await;
    assert_eq!(disks.len(), 0);

    // After the failed allocation, regions will exist, but be "Failed".
    for id in &test.dataset_ids {
        let crucible =
            test.sled_agent.get_crucible_dataset(test.zpool_id, *id).await;
        let regions = crucible.list().await;
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].state, RegionState::Failed);
    }

    // Validate that the underlying regions were released as a part of
    // unwinding the failed disk allocation, by performing another disk
    // allocation that should succeed.
    for id in &test.dataset_ids {
        let crucible =
            test.sled_agent.get_crucible_dataset(test.zpool_id, *id).await;
        crucible.set_create_callback(Box::new(|_| RegionState::Created)).await;
    }
    let _: Disk = objects_post(&client, &disks_url, new_disk.clone()).await;
}

async fn disk_get(client: &ClientTestContext, disk_url: &str) -> Disk {
    object_get::<Disk>(client, disk_url).await
}

async fn disks_list(client: &ClientTestContext, list_url: &str) -> Vec<Disk> {
    objects_list_page::<Disk>(client, list_url).await.items
}

fn disks_eq(disk1: &Disk, disk2: &Disk) {
    identity_eq(&disk1.identity, &disk2.identity);
    assert_eq!(disk1.project_id, disk2.project_id);
    assert_eq!(disk1.snapshot_id, disk2.snapshot_id);
    assert_eq!(disk1.size.to_bytes(), disk2.size.to_bytes());
    assert_eq!(disk1.state, disk2.state);
    assert_eq!(disk1.device_path, disk2.device_path);
}

/**
 * Simulate completion of an ongoing disk state transition.
 */
async fn disk_simulate(nexus: &Arc<Nexus>, id: &Uuid) {
    let sa = nexus.disk_sled_by_id(id).await.unwrap();
    sa.disk_finish_transition(id.clone()).await;
}
