// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Nexus' interactions with Crucible's pantry

use crate::integration_tests::instances::instance_simulate;
use crate::integration_tests::instances::instance_wait_for_state;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views::Snapshot;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceState;
use omicron_nexus::Nexus;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use std::sync::Arc;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "springfield-squidport-disks";
const DISK_NAME: &str = "just-rainsticks";
const INSTANCE_NAME: &str = "just-rainsticks";

// Helper functions

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_disk_url(disk_name: &str) -> String {
    format!("/v1/disks/{disk_name}?project={}", PROJECT_NAME)
}

fn get_disk_attach_url(instance_name: &str) -> String {
    format!(
        "/v1/instances/{instance_name}/disks/attach?project={}",
        PROJECT_NAME
    )
}

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
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

async fn create_disk_with_state_importing_blocks(client: &ClientTestContext) {
    let url = format!("/v1/disks?project={}", PROJECT_NAME,);

    let _disk: Disk = object_create(
        client,
        &url,
        &params::DiskCreate::Crucible {
            identity: IdentityMetadataCreateParams {
                name: DISK_NAME.parse().unwrap(),
                description: String::from("sells rainsticks"),
            },
            disk_source: params::DiskSource::ImportingBlocks {
                block_size: params::BlockSize::try_from(512).unwrap(),
            },
            size: ByteCount::from_gibibytes_u32(1),
        },
    )
    .await;
}

async fn create_instance_and_attach_disk(
    client: &ClientTestContext,
    nexus: &Arc<Nexus>,
    expected_status: StatusCode,
) {
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

    let url_instance_attach_disk =
        get_disk_attach_url(instance.identity.name.as_str());

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance_attach_disk)
            .body(Some(&params::DiskPath {
                disk: DISK_NAME.to_string().try_into().unwrap(),
            }))
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn attach_disk_to_instance(client: &ClientTestContext) {
    let url_instance_attach_disk = get_disk_attach_url(INSTANCE_NAME);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &url_instance_attach_disk)
            .body(Some(&params::DiskPath {
                disk: DISK_NAME.to_string().try_into().unwrap(),
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn bulk_write_start(
    client: &ClientTestContext,
    expected_status: StatusCode,
) {
    let bulk_write_start_url = format!(
        "/v1/disks/{}/bulk-write-start?project={}",
        DISK_NAME, PROJECT_NAME,
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bulk_write_start_url)
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn bulk_write_bytes(client: &ClientTestContext) {
    let bulk_write_url =
        format!("/v1/disks/{}/bulk-write?project={}", DISK_NAME, PROJECT_NAME,);

    // Use 4 MiB chunk size so this test won't take a long time.
    const CHUNK_SIZE: u64 = 4096 * 1024;

    for block in 0..8 {
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &bulk_write_url)
                .body(Some(&params::ImportBlocksBulkWrite {
                    offset: block * CHUNK_SIZE,
                    base64_encoded_data: base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        vec![0; CHUNK_SIZE as usize],
                    ),
                }))
                .expect_status(Some(StatusCode::NO_CONTENT)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    }
}

async fn bulk_write_bytes_expect_failure(client: &ClientTestContext) {
    let bulk_write_url =
        format!("/v1/disks/{}/bulk-write?project={}", DISK_NAME, PROJECT_NAME,);

    const CHUNK_SIZE: u64 = 4096 * 1024;

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bulk_write_url)
            .body(Some(&params::ImportBlocksBulkWrite {
                offset: CHUNK_SIZE,
                base64_encoded_data: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    vec![0; CHUNK_SIZE as usize],
                ),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn bulk_write_bytes_manual(
    client: &ClientTestContext,
    offset: u64,
    data: Vec<u8>,
    expected_status: StatusCode,
) {
    let bulk_write_url =
        format!("/v1/disks/{}/bulk-write?project={}", DISK_NAME, PROJECT_NAME,);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bulk_write_url)
            .body(Some(&params::ImportBlocksBulkWrite {
                offset,
                base64_encoded_data: base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    data,
                ),
            }))
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn bulk_write_stop(
    client: &ClientTestContext,
    expected_status: StatusCode,
) {
    let bulk_write_stop_url = format!(
        "/v1/disks/{}/bulk-write-stop?project={}",
        DISK_NAME, PROJECT_NAME,
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bulk_write_stop_url)
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn finalize_import(
    client: &ClientTestContext,
    expected_status: StatusCode,
) {
    let finalize_url =
        format!("/v1/disks/{}/finalize?project={}", DISK_NAME, PROJECT_NAME,);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &finalize_url)
            .raw_body(Some("{}".to_string()))
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn finalize_import_take_snapshot(
    client: &ClientTestContext,
    expected_status: StatusCode,
) {
    let finalize_url =
        format!("/v1/disks/{}/finalize?project={}", DISK_NAME, PROJECT_NAME,);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &finalize_url)
            .raw_body(Some("{ \"snapshot_name\": \"a-snapshot\" }".to_string()))
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

async fn validate_disk_state(client: &ClientTestContext, state: DiskState) {
    let disk_url = get_disk_url(DISK_NAME);
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, state);
}

// Tests

#[nexus_test]
async fn test_disk_create_for_importing(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    let new_disk = params::DiskCreate::Crucible {
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

// Validate that a disk in state ImportReady cannot be attached to an instance
#[nexus_test]
async fn test_cannot_mount_import_ready_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // We shouldn't be able to attach a disk in state ImportReady
    create_instance_and_attach_disk(client, nexus, StatusCode::BAD_REQUEST)
        .await;

    // Finalize import
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state Detached
    validate_disk_state(client, DiskState::Detached).await;

    // Can now attach disk ok
    attach_disk_to_instance(client).await;
}

// Validate that a disk in state ImportingFromBulkWrites cannot be attached to
// an instance
#[nexus_test]
async fn test_cannot_mount_import_from_bulk_writes_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Set this disk as importing from bulk writes
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // We shouldn't be able to attach a disk in state ImportingFromBulkWrites
    create_instance_and_attach_disk(client, nexus, StatusCode::BAD_REQUEST)
        .await;
}

// Test the normal flow of importing from bulk writes
#[nexus_test]
async fn test_import_blocks_with_bulk_write(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // bulk write some bytes in
    bulk_write_bytes(client).await;

    // bulk write stop
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // finalize import
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;
}

// Test the normal flow of importing with bulk writes, taking a snapshot
#[nexus_test]
async fn test_import_blocks_with_bulk_write_with_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // bulk write some bytes in
    bulk_write_bytes(client).await;

    // bulk write stop
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // finalize import
    finalize_import_take_snapshot(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Validate snapshot was created
    let snapshot_url =
        format!("/v1/snapshots/a-snapshot?project={}", PROJECT_NAME);
    let _snapshot: Snapshot = NexusRequest::object_get(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;
}

// Test that users cannot finalize a disk without stopping the bulk writes
#[nexus_test]
async fn test_cannot_finalize_without_stopping_bulk_writes(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // bulk write some bytes in
    bulk_write_bytes(client).await;

    // try to finalize the disk - should see BAD_REQUEST
    finalize_import(client, StatusCode::BAD_REQUEST).await;
}

// Test that users cannot bulk write to an offset that is not a block size
// multiple
#[nexus_test]
async fn test_cannot_bulk_write_to_unaligned_offset(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // Block size is 512 - test that writes to an offset that is not a block
    // size multiple fails
    bulk_write_bytes_manual(
        client,
        1000,
        vec![0; 512],
        StatusCode::BAD_REQUEST,
    )
    .await;
}

// Test that users cannot bulk write a data size that is not a block size
// multiple
#[nexus_test]
async fn test_cannot_bulk_write_data_not_block_size_multiple(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // Block size is 512 - test that writes to an offset that is not a block
    // size multiple fails
    bulk_write_bytes_manual(
        client,
        512,
        vec![0; 1000],
        StatusCode::BAD_REQUEST,
    )
    .await;
}

// Test that users cannot bulk write past the end of the disk
#[nexus_test]
async fn test_cannot_bulk_write_data_past_end_of_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    // Block size is 512 - test that writes to an offset off the end of the disk
    // fails
    bulk_write_bytes_manual(
        client,
        ByteCount::from_gibibytes_u32(1).to_bytes(),
        vec![0; 512 * 10],
        StatusCode::BAD_REQUEST,
    )
    .await;
}

// Test that users cannot bulk write non-base64 data
#[nexus_test]
async fn test_cannot_bulk_write_data_non_base64(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    // bulk write start
    bulk_write_start(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state ImportingFromBulkWrites
    validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

    let bulk_write_url =
        format!("/v1/disks/{}/bulk-write?project={}", DISK_NAME, PROJECT_NAME,);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &bulk_write_url)
            .body(Some(&params::ImportBlocksBulkWrite {
                offset: 0,
                base64_encoded_data: "this is not base64!".to_string(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

// Test that users can stop and start importing using bulk writes multiple times
#[nexus_test]
async fn test_can_stop_start_import_from_bulk_write(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // Validate disk is in state ImportReady
    validate_disk_state(client, DiskState::ImportReady).await;

    for _ in 0..16 {
        bulk_write_start(client, StatusCode::NO_CONTENT).await;
        validate_disk_state(client, DiskState::ImportingFromBulkWrites).await;

        bulk_write_stop(client, StatusCode::NO_CONTENT).await;
        validate_disk_state(client, DiskState::ImportReady).await;
    }

    finalize_import(client, StatusCode::NO_CONTENT).await;
}

// Test that users cannot start a bulk write when a disk is attached to an
// instance
#[nexus_test]
async fn test_cannot_bulk_write_start_attached_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // bulk write bytes in and finalize
    bulk_write_start(client, StatusCode::NO_CONTENT).await;
    bulk_write_bytes(client).await;
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;

    // Validate that a user cannot start a bulk write
    bulk_write_start(client, StatusCode::BAD_REQUEST).await;
}

// Test that users cannot bulk write when a disk is attached to an
// instance
#[nexus_test]
async fn test_cannot_bulk_write_attached_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // bulk write bytes in and finalize
    bulk_write_start(client, StatusCode::NO_CONTENT).await;
    bulk_write_bytes(client).await;
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;

    // Validate that a user cannot bulk write
    bulk_write_bytes_expect_failure(client).await;
}

// Test that users cannot stop a bulk write when a disk is attached to an
// instance
#[nexus_test]
async fn test_cannot_bulk_write_stop_attached_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // bulk write bytes in and finalize
    bulk_write_start(client, StatusCode::NO_CONTENT).await;
    bulk_write_bytes(client).await;
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;

    // Validate that a user cannot stop a bulk write
    bulk_write_stop(client, StatusCode::BAD_REQUEST).await;
}

// Test that users cannot finalize a disk it is attached to an instance
#[nexus_test]
async fn test_cannot_finalize_attached_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;

    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    create_disk_with_state_importing_blocks(client).await;

    // bulk write bytes in and finalize
    bulk_write_start(client, StatusCode::NO_CONTENT).await;
    bulk_write_bytes(client).await;
    bulk_write_stop(client, StatusCode::NO_CONTENT).await;
    finalize_import(client, StatusCode::NO_CONTENT).await;

    // Validate disk is in state detached
    validate_disk_state(client, DiskState::Detached).await;

    // Create an instance to attach the disk.
    create_instance_and_attach_disk(client, nexus, StatusCode::ACCEPTED).await;

    // Validate that a user cannot finalize
    finalize_import(client, StatusCode::BAD_REQUEST).await;
}
