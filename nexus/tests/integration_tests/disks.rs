// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic disk support in the API

use crucible_agent_client::types::State as RegionState;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::Name;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::{external_api::params, Nexus};
use sled_agent_client::TestInterfaces as _;
use std::sync::Arc;
use uuid::Uuid;

const ORG_NAME: &str = "test-org";
const PROJECT_NAME: &str = "springfield-squidport-disks";
const DISK_NAME: &str = "just-rainsticks";
const INSTANCE_NAME: &str = "just-rainsticks";

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
    create_ip_pool(&client, "p0", None).await;
    create_organization(&client, ORG_NAME).await;
    let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
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
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
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
    instance_url: &str,
    state: &str,
) -> Instance {
    let url = format!("{}/{}", instance_url, state);
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
    sa.instance_finish_transition(id.clone()).await;
}

#[nexus_test]
async fn test_disk_create_attach_detach_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    let project_id = create_org_and_project(client).await;
    let nexus = &cptestctx.server.apictx.nexus;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let disk = create_disk(&client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;
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
    let instance =
        create_instance(&client, ORG_NAME, PROJECT_NAME, INSTANCE_NAME).await;

    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    let instance1_url = format!(
        "/organizations/{}/projects/{}/instances/{}",
        ORG_NAME, PROJECT_NAME, INSTANCE_NAME
    );
    let instance_next =
        set_instance_state(&client, &instance1_url, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(instance.identity.name.as_str());
    let url_instance_detach_disk =
        get_disk_detach_url(instance.identity.name.as_str());

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
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    let disk =
        disk_post(client, &url_instance_attach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

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
    let _ = create_disk(&client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
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
async fn test_disk_move_between_instances(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;
    DiskTest::new(&cptestctx).await;
    create_org_and_project(&client).await;
    let disks_url = get_disks_url();

    // Create a disk.
    let disk_url = format!("{}/{}", disks_url, DISK_NAME);
    let disk = create_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;

    // Create an instance to attach the disk.
    let instance =
        create_instance(&client, ORG_NAME, PROJECT_NAME, INSTANCE_NAME).await;
    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    //
    // Instances must be stopped before disks can be attached - this
    // is an artificial limitation without hotplug support.
    let instance_url = format!(
        "/organizations/{}/projects/{}/instances/{}",
        ORG_NAME, PROJECT_NAME, INSTANCE_NAME
    );
    let instance_next =
        set_instance_state(&client, &instance_url, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

    // Verify that there are no disks attached to the instance, and specifically
    // that our disk is not attached to this instance.
    let url_instance_disks =
        get_instance_disks_url(instance.identity.name.as_str());
    let disks = disks_list(&client, &url_instance_disks).await;
    assert_eq!(disks.len(), 0);

    let url_instance_attach_disk =
        get_disk_attach_url(instance.identity.name.as_str());
    let url_instance_detach_disk =
        get_disk_detach_url(instance.identity.name.as_str());

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
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    // Attach the disk to the same instance.  This should complete immediately
    // with no state change.
    let disk =
        disk_post(client, &url_instance_attach_disk, disk.identity.name).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    // Create a second instance and try to attach the disk to that.  This should
    // fail and the disk should remain attached to the first instance.
    let instance2 =
        create_instance(&client, ORG_NAME, PROJECT_NAME, "instance2").await;
    let instance2_url = format!(
        "/organizations/{}/projects/{}/instances/{}",
        ORG_NAME, PROJECT_NAME, "instance2"
    );
    let instance_next =
        set_instance_state(&client, &instance2_url, "stop").await;
    instance_simulate(nexus, &instance_next.identity.id).await;

    let url_instance2_attach_disk =
        get_disk_attach_url(instance2.identity.name.as_str());
    let url_instance2_detach_disk =
        get_disk_detach_url(instance2.identity.name.as_str());

    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance2_attach_disk)
            .body(Some(&params::DiskIdentifier {
                name: disk.identity.name.clone(),
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
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

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
    assert_eq!(attached_disk.state, DiskState::Attached(instance2_id.clone()));

    // At this point, it's not legal to attempt to attach it to a different
    // instance (the first one).
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance_attach_disk)
            .body(Some(&params::DiskIdentifier {
                name: disk.identity.name.clone(),
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
    assert_eq!(disk.state, DiskState::Attached(instance2_id.clone()));

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
    create_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;
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
    let _ = create_disk(client, ORG_NAME, PROJECT_NAME, DISK_NAME).await;
}

// Tests that invalid block sizes are rejected
#[nexus_test]
async fn test_disk_invalid_block_size_rejected(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let test = DiskTest::new(&cptestctx).await;
    create_org_and_project(client).await;

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
    let test = DiskTest::new(&cptestctx).await;
    create_org_and_project(client).await;

    let disk_size = ByteCount::from(3 * 1024 * 1024 * 1024 + 256);
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
async fn test_disk_reject_total_size_less_than_one_gibibyte(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    create_org_and_project(client).await;

    let disk_size = ByteCount::from(params::MIN_DISK_SIZE_BYTES / 2);

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
            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
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
            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
        )
    );
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
            .body(Some(&params::DiskIdentifier { name: disk_name }))
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
