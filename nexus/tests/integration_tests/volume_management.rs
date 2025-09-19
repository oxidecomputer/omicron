// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that Nexus properly manages and cleans up Crucible resources
//! associated with Volumes

use crate::integration_tests::crucible_replacements::wait_for_all_replacements;
use crate::integration_tests::sleds::sleds_list;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_config::RegionAllocationStrategy;
use nexus_db_lookup::LookupPath;
use nexus_db_model::CrucibleDataset;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_model::RegionSnapshotReplacementState;
use nexus_db_model::Volume;
use nexus_db_model::VolumeResourceUsage;
use nexus_db_model::VolumeResourceUsageRecord;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::CrucibleResources;
use nexus_db_queries::db::datastore::DestVolume;
use nexus_db_queries::db::datastore::Disk;
use nexus_db_queries::db::datastore::ExistingTarget;
use nexus_db_queries::db::datastore::RegionAllocationFor;
use nexus_db_queries::db::datastore::RegionAllocationParameters;
use nexus_db_queries::db::datastore::ReplacementTarget;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::datastore::SourceVolume;
use nexus_db_queries::db::datastore::VolumeReplaceResult;
use nexus_db_queries::db::datastore::VolumeToDelete;
use nexus_db_queries::db::datastore::VolumeWithTarget;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::pagination::paginated;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_disk_from_snapshot;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_snapshot;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::internal;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::DownstairsRegionUuid;
use omicron_uuid_kinds::DownstairsUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::UpstairsRepairUuid;
use omicron_uuid_kinds::UpstairsSessionUuid;
use omicron_uuid_kinds::UpstairsUuid;
use omicron_uuid_kinds::VolumeUuid;
use rand::prelude::SliceRandom;
use rand::{SeedableRng, rngs::StdRng};
use sled_agent_client::{CrucibleOpts, VolumeConstructionRequest};
use std::collections::HashSet;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;
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

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_disk_url(disk: &str) -> String {
    format!("/v1/disks/{}?project={}", disk, PROJECT_NAME)
}

fn get_snapshots_url() -> String {
    format!("/v1/snapshots?project={}", PROJECT_NAME)
}

fn get_snapshot_url(snapshot: &str) -> String {
    format!("/v1/snapshots/{}?project={}", snapshot, PROJECT_NAME)
}

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

async fn create_image(client: &ClientTestContext) -> views::Image {
    create_project_and_pool(client).await;

    // Define a global image

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "alpine".to_string(),
        version: "edge".to_string(),
    };

    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    NexusRequest::objects_post(client, &images_url, &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn create_base_disk(
    client: &ClientTestContext,
    image: &views::Image,
    disks_url: &String,
    base_disk_name: &Name,
) -> external::Disk {
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap()
}

#[nexus_test]
async fn test_snapshot_then_delete_disk(cptestctx: &ControlPlaneTestContext) {
    // Test that Nexus does not delete a region if there's a snapshot of that
    // region:
    //
    // 1. Create a disk
    // 2. Create a snapshot of that disk (creating running snapshots)
    // 3. Delete the disk
    // 4. Delete the snapshot

    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    let image = create_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot request
    let snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshot still remains
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // The Crucible snapshot still remains
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("a-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_delete_snapshot_then_disk(cptestctx: &ControlPlaneTestContext) {
    // Test that Nexus cleans up resources properly:
    //
    // 1. Create a disk
    // 2. Create a snapshot of that disk (creating running snapshots)
    // 3. Delete the snapshot
    // 4. Delete the disk

    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    // Define an image
    let image = create_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot request
    let snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshot still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("a-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // The Crucible regions still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_multiple_snapshots(cptestctx: &ControlPlaneTestContext) {
    // Test that Nexus cleans up resources properly:
    //
    // 1. Create a disk
    // 2. Create multiple snapshots of that disk (creating running snapshots)
    // 3. Delete the disk
    // 4. Delete the snapshots

    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    let image = create_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot requests
    for i in 0..4 {
        let snapshot: views::Snapshot = object_create(
            client,
            &get_snapshots_url(),
            &params::SnapshotCreate {
                identity: IdentityMetadataCreateParams {
                    name: format!("a-snapshot-{}", i).parse().unwrap(),
                    description: "a snapshot!".to_string(),
                },
                disk: base_disk_name.clone().into(),
            },
        )
        .await;

        assert_eq!(snapshot.disk_id, base_disk.identity.id);
        assert_eq!(snapshot.size, base_disk.size);
    }

    // The Crucible regions and snapshots still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Delete the snapshots
    for i in 0..4 {
        // The Crucible snapshots still remain
        assert!(!disk_test.crucible_resources_deleted().await);

        let snapshot_url = get_snapshot_url(&format!("a-snapshot-{}", i));
        NexusRequest::object_delete(client, &snapshot_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete snapshot");
    }

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_snapshot_prevents_other_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test that region remains if there is a snapshot, preventing further
    // allocation.

    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    let image = create_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot request
    let snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshots still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // The Crucible snapshots still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Attempt disk allocation, which will fail - the presence of the snapshot
    // means the region wasn't deleted.
    let disk_size = ByteCount::from_gibibytes_u32(10);
    let next_disk_name: Name = "next-disk".parse().unwrap();
    let next_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: next_disk_name.clone(),
            description: String::from("will fail"),
        },
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&next_disk))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Delete the snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("a-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // All resources were deleted
    assert!(disk_test.crucible_resources_deleted().await);

    // Disk allocation will work now
    let _next_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&next_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Delete it
    NexusRequest::object_delete(client, &get_disk_url("next-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_multiple_disks_multiple_snapshots_order_1(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test multiple disks with multiple snapshots
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Create a blank disk
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let first_disk_name: Name = "first-disk".parse().unwrap();
    let first_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: first_disk_name.clone(),
            description: String::from("disk 1"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let first_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&first_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let first_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-snapshot".parse().unwrap(),
                description: "first snapshot!".to_string(),
            },
            disk: first_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(first_snapshot.disk_id, first_disk.identity.id);
    assert_eq!(first_snapshot.size, first_disk.size);

    // Create another blank disk
    let second_disk_name: Name = "second-disk".parse().unwrap();
    let second_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: second_disk_name.clone(),
            description: String::from("disk 1"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let second_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&second_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request for the second disk
    let second_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-snapshot".parse().unwrap(),
                description: "second snapshot!".to_string(),
            },
            disk: second_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(second_snapshot.disk_id, second_disk.identity.id);
    assert_eq!(second_snapshot.size, second_disk.size);

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first disk
    NexusRequest::object_delete(client, &get_disk_url("first-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("second-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second disk
    NexusRequest::object_delete(client, &get_disk_url("second-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("first-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_multiple_disks_multiple_snapshots_order_2(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test multiple disks with multiple snapshots, varying the delete order
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Create a blank disk
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let first_disk_name: Name = "first-disk".parse().unwrap();
    let first_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: first_disk_name.clone(),
            description: String::from("disk 1"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let first_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&first_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let first_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-snapshot".parse().unwrap(),
                description: "first snapshot!".to_string(),
            },
            disk: first_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(first_snapshot.disk_id, first_disk.identity.id);
    assert_eq!(first_snapshot.size, first_disk.size);

    // Create another blank disk
    let second_disk_name: Name = "second-disk".parse().unwrap();
    let second_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: second_disk_name.clone(),
            description: String::from("disk 1"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let second_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&second_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request for the second disk
    let second_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-snapshot".parse().unwrap(),
                description: "second snapshot!".to_string(),
            },
            disk: second_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(second_snapshot.disk_id, second_disk.identity.id);
    assert_eq!(second_snapshot.size, second_disk.size);

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first disk
    NexusRequest::object_delete(client, &get_disk_url("first-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second disk
    NexusRequest::object_delete(client, &get_disk_url("second-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("second-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first snapshot
    NexusRequest::object_delete(client, &get_snapshot_url("first-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

async fn prepare_for_test_multiple_layers_of_snapshots(
    client: &ClientTestContext,
) {
    let disks_url = get_disks_url();

    // Create a blank disk
    let disk_size = ByteCount::from_gibibytes_u32(1);
    let layer_1_disk_name: Name = "layer-1-disk".parse().unwrap();
    let layer_1_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: layer_1_disk_name.clone(),
            description: String::from("layer 1"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let layer_1_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&layer_1_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let layer_1_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-1-snapshot".parse().unwrap(),
                description: "layer 1 snapshot!".to_string(),
            },
            disk: layer_1_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(layer_1_snapshot.disk_id, layer_1_disk.identity.id);
    assert_eq!(layer_1_snapshot.size, layer_1_disk.size);

    // Create a layer 2 disk out of the layer 1 snapshot
    let layer_2_disk_name: Name = "layer-2-disk".parse().unwrap();
    let layer_2_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: layer_2_disk_name.clone(),
            description: String::from("layer 2"),
        },
        disk_source: params::DiskSource::Snapshot {
            snapshot_id: layer_1_snapshot.identity.id,
        },
        size: disk_size,
    };

    let layer_2_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&layer_2_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request for the second disk
    let layer_2_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-2-snapshot".parse().unwrap(),
                description: "layer 2 snapshot!".to_string(),
            },
            disk: layer_2_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(layer_2_snapshot.disk_id, layer_2_disk.identity.id);
    assert_eq!(layer_2_snapshot.size, layer_2_disk.size);

    // Create a layer 3 disk out of the layer 2 snapshot
    let layer_3_disk_name: Name = "layer-3-disk".parse().unwrap();
    let layer_3_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: layer_3_disk_name.clone(),
            description: String::from("layer 3"),
        },
        disk_source: params::DiskSource::Snapshot {
            snapshot_id: layer_2_snapshot.identity.id,
        },
        size: disk_size,
    };

    let layer_3_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&layer_3_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request for the third disk
    let layer_3_snapshot: views::Snapshot = object_create(
        client,
        &get_snapshots_url(),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-3-snapshot".parse().unwrap(),
                description: "layer 3 snapshot!".to_string(),
            },
            disk: layer_3_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(layer_3_snapshot.disk_id, layer_3_disk.identity.id);
    assert_eq!(layer_3_snapshot.size, layer_3_disk.size);
}

#[nexus_test]
async fn test_multiple_layers_of_snapshots_delete_all_disks_first(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test layering read-only snapshots through multiple disks:
    // delete all disks, then delete all snapshots
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    prepare_for_test_multiple_layers_of_snapshots(&client).await;

    // Delete the disks
    for name in ["layer-1-disk", "layer-2-disk", "layer-3-disk"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        NexusRequest::object_delete(client, &get_disk_url(name))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete disk");
    }

    // Delete the snapshots
    for name in ["layer-1-snapshot", "layer-2-snapshot", "layer-3-snapshot"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        NexusRequest::object_delete(client, &get_snapshot_url(name))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete snapshot");
    }

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_multiple_layers_of_snapshots_delete_all_snapshots_first(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test layering read-only snapshots through multiple disks:
    // delete all snapshots, then delete all disks
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    prepare_for_test_multiple_layers_of_snapshots(&client).await;

    // Delete the snapshots
    for name in ["layer-1-snapshot", "layer-2-snapshot", "layer-3-snapshot"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        NexusRequest::object_delete(client, &get_snapshot_url(name))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete snapshot");
    }

    // Delete the disks
    for name in ["layer-1-disk", "layer-2-disk", "layer-3-disk"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        NexusRequest::object_delete(client, &get_disk_url(name))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete disk");
    }

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_multiple_layers_of_snapshots_random_delete_order(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test layering read-only snapshots through multiple disks:
    // delete snapshots and disks in a random order
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;

    prepare_for_test_multiple_layers_of_snapshots(&client).await;

    #[derive(Debug)]
    enum DeleteObject {
        Disk(String),
        Snapshot(String),
    }

    let objects = {
        let mut objects = vec![
            DeleteObject::Disk("layer-1-disk".to_string()),
            DeleteObject::Disk("layer-2-disk".to_string()),
            DeleteObject::Disk("layer-3-disk".to_string()),
            DeleteObject::Snapshot("layer-1-snapshot".to_string()),
            DeleteObject::Snapshot("layer-2-snapshot".to_string()),
            DeleteObject::Snapshot("layer-3-snapshot".to_string()),
        ];

        let mut rng = StdRng::from_os_rng();
        objects.shuffle(&mut rng);

        objects
    };

    // If this test fails, this should print out the order that caused the
    // failure.
    dbg!(&objects);

    for object in &objects {
        assert!(!disk_test.crucible_resources_deleted().await);

        match object {
            DeleteObject::Disk(name) => {
                NexusRequest::object_delete(client, &get_disk_url(name))
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete disk");
            }

            DeleteObject::Snapshot(name) => {
                NexusRequest::object_delete(client, &get_snapshot_url(name))
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete snapshot");
            }
        }
    }

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_create_image_from_snapshot(cptestctx: &ControlPlaneTestContext) {
    // 1. Create a disk from an image that uses a URL source
    // 2. Take a snapshot of that disk
    // 3. Create an image from that snapshot

    let client = &cptestctx.external_client;
    let _disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    let image = create_image(&client).await;

    // Create a disk from this image
    let _base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    // Create an image from the snapshot
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "debian-11".parse().unwrap(),
            description: String::from("debian's cool too"),
        },
        source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        os: "debian".parse().unwrap(),
        version: "11".into(),
    };

    let _image: views::Image =
        NexusRequest::objects_post(client, "/v1/images", &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
}

#[nexus_test]
async fn test_create_image_from_snapshot_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    // 1. Create a disk from an image that uses a URL source
    // 2. Take a snapshot of that disk
    // 3. Create an image from that snapshot
    // 4. Delete the disk
    // 5. Delete the snapshot
    // 6. Delete the image.

    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    let image = create_image(&client).await;

    // Create a disk from this image
    let _base_disk =
        create_base_disk(&client, &image, &disks_url, &base_disk_name).await;

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    // Create an image from the snapshot
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "debian-11".parse().unwrap(),
            description: String::from("debian's cool too"),
        },
        source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        os: "debian".parse().unwrap(),
        version: "11".into(),
    };

    let _image: views::Image =
        NexusRequest::objects_post(client, "/v1/images", &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Still some crucible resources
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the snapshot
    let snapshot_url = get_snapshot_url("a-snapshot");
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Still some Crucible resources - importantly, the image has
    // incremented the resource counts associated with its own volume
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the image
    let image_url = "/v1/images/debian-11";
    NexusRequest::object_delete(client, &image_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete image");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

enum DeleteImageTestParam {
    Image,
    Disk,
    Snapshot,
}

async fn delete_image_test(
    cptestctx: &ControlPlaneTestContext,
    order: &[DeleteImageTestParam],
) {
    // 1. Create a blank disk
    // 2. Take a snapshot of that disk
    // 3. Create an image from that snapshot
    // 4. Delete each of these items in some order

    let disk_test = DiskTest::new(&cptestctx).await;

    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disks_url = get_disks_url();

    // Create a blank disk

    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("all your base disk are belong to us"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let _base_disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: String::from("you are on the way to destruction"),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    // Create an image from the snapshot
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "debian-11".parse().unwrap(),
            description: String::from(
                "you have no chance to survive make your time",
            ),
        },
        source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        os: "debian".parse().unwrap(),
        version: "12".into(),
    };

    let _image: views::Image =
        NexusRequest::objects_post(client, "/v1/images", &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(order.len(), 3);
    for item in order {
        // Still some crucible resources
        assert!(!disk_test.crucible_resources_deleted().await);

        match item {
            DeleteImageTestParam::Image => {
                let image_url = "/v1/images/debian-11";
                NexusRequest::object_delete(client, &image_url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete image");
            }

            DeleteImageTestParam::Disk => {
                NexusRequest::object_delete(client, &get_disk_url("base-disk"))
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete disk");
            }

            DeleteImageTestParam::Snapshot => {
                let snapshot_url = get_snapshot_url("a-snapshot");
                NexusRequest::object_delete(client, &snapshot_url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete snapshot");
            }
        }
    }

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

// Make sure that whatever order disks, images, and snapshots are deleted, the
// Crucible resource accounting that Nexus does is correct.

#[nexus_test]
async fn test_delete_image_order_1(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Disk,
            DeleteImageTestParam::Image,
            DeleteImageTestParam::Snapshot,
        ],
    )
    .await;
}

#[nexus_test]
async fn test_delete_image_order_2(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Disk,
            DeleteImageTestParam::Snapshot,
            DeleteImageTestParam::Image,
        ],
    )
    .await;
}

#[nexus_test]
async fn test_delete_image_order_3(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Image,
            DeleteImageTestParam::Disk,
            DeleteImageTestParam::Snapshot,
        ],
    )
    .await;
}

#[nexus_test]
async fn test_delete_image_order_4(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Image,
            DeleteImageTestParam::Snapshot,
            DeleteImageTestParam::Disk,
        ],
    )
    .await;
}

#[nexus_test]
async fn test_delete_image_order_5(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Snapshot,
            DeleteImageTestParam::Disk,
            DeleteImageTestParam::Image,
        ],
    )
    .await;
}

#[nexus_test]
async fn test_delete_image_order_6(cptestctx: &ControlPlaneTestContext) {
    delete_image_test(
        cptestctx,
        &[
            DeleteImageTestParam::Snapshot,
            DeleteImageTestParam::Image,
            DeleteImageTestParam::Disk,
        ],
    )
    .await;
}

// A test function to create a volume with the provided read only parent.
async fn create_volume(
    datastore: &Arc<DataStore>,
    volume_id: VolumeUuid,
    rop_option: Option<VolumeConstructionRequest>,
) {
    let block_size = 512;

    // Make the SubVolume
    let sub_volume = VolumeConstructionRequest::File {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        path: "/lol".to_string(),
    };
    let sub_volumes = vec![sub_volume];

    let rop = match rop_option {
        Some(x) => Some(Box::new(x)),
        None => None,
    };

    // Create the volume from the parts above and insert into the database.
    datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size,
                sub_volumes,
                read_only_parent: rop,
            },
        )
        .await
        .unwrap();
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_base(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a volume with a read only parent.
    // The ROP should end up on the t_vid volume.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let t_vid = VolumeUuid::new_v4();
    let block_size = 512;

    // Make our read_only_parent
    let rop = VolumeConstructionRequest::Url {
        id: Uuid::new_v4(),
        block_size,
        url: "http://oxide.computer/rop".to_string(),
    };

    // Create the Volume with a read_only_parent, and the temp volume that
    // the saga would create for us.
    create_volume(&datastore, volume_id, Some(rop)).await;
    create_volume(&datastore, t_vid, None).await;

    // We should get Ok(true) back after removal of the ROP.
    let res = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(res);

    // Go and get the volume from the database, verify it no longer
    // has a read only parent.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_none());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }

    // Verify the t_vid now has a ROP.
    let new_vol = datastore
        .volume_checkout(
            t_vid,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_some());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }

    // Try to remove the read only parent a 2nd time, it should
    // return Ok(false) as there is now no volume to remove.
    let res = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!res);

    // Verify the t_vid still has the read_only_parent.
    // We want to verify we can call volume_remove_rop twice and the second
    // time through it won't change what it did the first time. This is
    // critical to supporting replay of the saga, should it be needed.
    let new_vol = datastore
        .volume_checkout(
            t_vid,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_some());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_no_parent(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a read only parent from a volume
    // without a read only parent.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let t_vid = VolumeUuid::new_v4();
    create_volume(&datastore, volume_id, None).await;

    // We will get Ok(false) back from this operation.
    let res = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!res);
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_volume_not_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    // test removal of a read only volume for a volume that is not
    // of a type to have a read only parent.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let t_vid = VolumeUuid::new_v4();

    datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::File {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                path: "/lol".to_string(),
            },
        )
        .await
        .unwrap();

    let removed = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!removed);
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_bad_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a read only parent from a volume
    // that does not exist
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let t_vid = VolumeUuid::new_v4();

    // Nothing should be removed, but we also don't return error.
    let removed = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!removed);
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_volume_deleted(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a read_only_parent from a deleted volume.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Make our read_only_parent
    let rop = VolumeConstructionRequest::Url {
        id: Uuid::new_v4(),
        block_size,
        url: "http://oxide.computer/rop".to_string(),
    };
    // Make the volume
    create_volume(&datastore, volume_id, Some(rop)).await;

    // Soft delete the volume
    let _cr = datastore.soft_delete_volume(volume_id).await.unwrap();

    let t_vid = VolumeUuid::new_v4();
    // Nothing should be removed, but we also don't return error.
    let removed = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!removed);
}

#[nexus_test]
async fn test_volume_remove_rop_saga(cptestctx: &ControlPlaneTestContext) {
    // Test the saga for removal of a volume with a read only parent.
    // We create a volume with a read only parent, then call the saga on it.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Make our read_only_parent
    let rop = VolumeConstructionRequest::Url {
        id: Uuid::new_v4(),
        block_size,
        url: "http://oxide.computer/rop".to_string(),
    };

    create_volume(&datastore, volume_id, Some(rop)).await;

    println!("Created this volume: {:?}", volume_id);
    // disk to volume id, to then remove ROP?
    let int_client = &cptestctx.internal_client;
    let rop_url = format!("/volume/{}/remove-read-only-parent", volume_id);

    // Call the internal API endpoint for removal of the read only parent
    int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_none());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }
}

#[nexus_test]
async fn test_volume_remove_rop_saga_twice(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test calling the saga for removal of a volume with a read only parent
    // two times, the first will remove the read_only_parent, the second will
    // do nothing.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Make our read_only_parent
    let rop = VolumeConstructionRequest::Url {
        id: Uuid::new_v4(),
        block_size,
        url: "http://oxide.computer/rop".to_string(),
    };

    create_volume(&datastore, volume_id, Some(rop)).await;

    println!("Created this volume: {:?}", volume_id);
    // disk to volume id, to then remove ROP?
    let int_client = &cptestctx.internal_client;
    let rop_url = format!("/volume/{}/remove-read-only-parent", volume_id);

    // Call the internal API endpoint for removal of the read only parent
    let res = int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    println!("first returns {:?}", res);
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_none());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }

    // Call the internal API endpoint a second time. Should be okay.
    let res = int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    println!("twice returns {:?}", res);
}

#[nexus_test]
async fn test_volume_remove_rop_saga_no_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test calling the saga on a volume that does not exist.
    let volume_id = VolumeUuid::new_v4();

    println!("Non-existant volume: {:?}", volume_id);
    let int_client = &cptestctx.internal_client;
    let rop_url = format!("/volume/{}/remove-read-only-parent", volume_id);

    // Call the internal API endpoint for removal of the read only parent
    int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

#[nexus_test]
async fn test_volume_remove_rop_saga_volume_not_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test saga removal of a read only volume for a volume that is not
    // of a type to have a read only parent.
    let nexus = &cptestctx.server.server_context().nexus;
    let volume_id = VolumeUuid::new_v4();
    let datastore = nexus.datastore();

    datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::File {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                path: "/lol".to_string(),
            },
        )
        .await
        .unwrap();

    let int_client = &cptestctx.internal_client;
    // Call the saga on this volume
    let rop_url = format!("/volume/{}/remove-read-only-parent", volume_id);

    // Call the internal API endpoint for removal of the read only parent
    int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

#[nexus_test]
async fn test_volume_remove_rop_saga_deleted_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test that a saga removal of a read_only_parent from a deleted volume
    // takes no action on that deleted volume.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Make our read_only_parent
    let rop = VolumeConstructionRequest::Url {
        id: Uuid::new_v4(),
        block_size,
        url: "http://oxide.computer/rop".to_string(),
    };
    // Make the volume
    create_volume(&datastore, volume_id, Some(rop)).await;

    // Soft delete the volume
    let _cr = datastore.soft_delete_volume(volume_id).await.unwrap();

    let int_client = &cptestctx.internal_client;
    let rop_url = format!("/volume/{}/remove-read-only-parent", volume_id);

    // Call the internal API endpoint for removal of the read only parent
    int_client
        .make_request(
            Method::POST,
            &rop_url,
            None as Option<&serde_json::Value>,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(new_vol.data()).unwrap();

    // Volume should still have read only parent
    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes: _,
            read_only_parent,
        } => {
            assert!(read_only_parent.is_some());
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }
}

#[nexus_test]
async fn test_volume_checkout(cptestctx: &ControlPlaneTestContext) {
    // Verify that a volume_checkout will update the generation number in the
    // database when the volume type is Volume with sub_volume Region.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Create a sub_vol with generation 1.
    let subvol = create_region(block_size, 1, Uuid::new_v4());
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol],
        read_only_parent: None,
    };

    // Take our VCR from above and insert into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // The first time back, we get 1 but internally the generation number goes
    // to 2.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(1)]);

    // Request again, we should get 2 now.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(2)]);
}

#[nexus_test]
async fn test_volume_checkout_updates_nothing(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will do nothing for a volume that does
    // not contain a sub_volume with a generation field.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Build our sub_vol and VCR from parts.
    let subvol = VolumeConstructionRequest::File {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        path: "/lol".to_string(),
    };
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol],
        read_only_parent: None,
    };

    // Take our VCR from above and insert into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // Verify nothing happens to our non generation number volume.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![None]);
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![None]);
}

#[nexus_test]
async fn test_volume_checkout_updates_multiple_gen(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will update the generation number in the
    // database when the volume type is Volume with multiple sub_volumes of
    // type Region.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Create two regions.
    let subvol_one = create_region(block_size, 3, Uuid::new_v4());
    let subvol_two = create_region(block_size, 8, Uuid::new_v4());

    // Make the volume with our two regions as sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol_one, subvol_two],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(3), Some(8)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(4), Some(9)]);

    // Request one more, because why not.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(5), Some(10)]);
}

#[nexus_test]
async fn test_volume_checkout_updates_sparse_multiple_gen(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will update the generation number in the
    // database when the volume type is Volume with multiple sub_volumes of
    // type Region and also verify that a non generation sub_volume won't be a
    // problem
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Create three sub_vols.
    let subvol_one = VolumeConstructionRequest::File {
        id: Uuid::new_v4(),
        block_size,
        path: "/lol".to_string(),
    };
    let subvol_two = create_region(block_size, 7, Uuid::new_v4());
    let subvol_three = create_region(block_size, 9, Uuid::new_v4());

    // Make the volume with our three regions as sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![None, Some(7), Some(9)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![None, Some(8), Some(10)]);
}

#[nexus_test]
async fn test_volume_checkout_updates_sparse_mid_multiple_gen(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will update the generation number in the
    // database when the volume type is Volume with multiple sub_volumes of
    // type Region and also verify that a non generation sub_volume in the
    // middle of the sub_volumes won't be a problem
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Create three sub_vols.
    let subvol_one = create_region(block_size, 7, Uuid::new_v4());
    let subvol_two = VolumeConstructionRequest::File {
        id: Uuid::new_v4(),
        block_size,
        path: "/lol".to_string(),
    };
    let subvol_three = create_region(block_size, 9, Uuid::new_v4());

    // Make the volume with our three sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(7), None, Some(9)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore
        .volume_checkout(
            volume_id,
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await
        .unwrap();
    volume_match_gen(new_vol, vec![Some(8), None, Some(10)]);
}

#[nexus_test]
async fn test_volume_checkout_randomize_ids_only_read_only(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout_randomize_ids will not work for
    // non-read-only Regions
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let volume_id = VolumeUuid::new_v4();
    let block_size = 512;

    // Create three sub_vols.
    let subvol_one = create_region(block_size, 7, Uuid::new_v4());
    let subvol_two = create_region(block_size, 7, Uuid::new_v4());
    let subvol_three = create_region(block_size, 7, Uuid::new_v4());

    // Make the volume with our three sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *volume_id.as_untyped_uuid(),
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(volume_id, volume_construction_request)
        .await
        .unwrap();

    // volume_checkout_randomize_ids should fail
    let r = datastore
        .volume_checkout_randomize_ids(
            SourceVolume(volume_id),
            DestVolume(VolumeUuid::new_v4()),
            db::datastore::VolumeCheckoutReason::CopyAndModify,
        )
        .await;
    assert!(r.is_err());
}

/// Test that the Crucible agent's port reuse does not confuse
/// `soft_delete_volume`, due to the
/// `[ipv6]:port` targets being reused.
#[nexus_test]
async fn test_keep_your_targets_straight(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    // Four zpools, one dataset each
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    let mut iter = disk_test.zpools();
    let zpool0 = iter.next().expect("Expected four zpools");
    let zpool1 = iter.next().expect("Expected four zpools");
    let zpool2 = iter.next().expect("Expected four zpools");
    let zpool3 = iter.next().expect("Expected four zpools");

    // This bug occurs when region_snapshot records share a snapshot_addr, so
    // insert those here manually.

    // (dataset_id, region_id, snapshot_id, snapshot_addr)
    let region_snapshots: Vec<(DatasetUuid, Uuid, Uuid, SocketAddr)> = vec![
        // first snapshot-create
        (
            zpool0.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:101::7]:19016".parse().unwrap(),
        ),
        (
            zpool1.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:102::7]:19016".parse().unwrap(),
        ),
        (
            zpool2.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:103::7]:19016".parse().unwrap(),
        ),
        // second snapshot-create
        (
            zpool0.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:101::7]:19016".parse().unwrap(),
        ),
        (
            zpool3.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:104::7]:19016".parse().unwrap(),
        ),
        (
            zpool2.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:103::7]:19017".parse().unwrap(),
        ),
    ];

    // First, three `region_snapshot` records created in the snapshot-create
    // saga, which are then used to make snapshot's volume construction request

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, snapshot_addr) =
            &region_snapshots[i];
        datastore
            .region_snapshot_create(nexus_db_model::RegionSnapshot {
                dataset_id: (*dataset_id).into(),
                region_id: *region_id,
                snapshot_id: *snapshot_id,
                snapshot_addr: snapshot_addr.to_string(),
                volume_references: 0,
                deleting: false,
            })
            .await
            .unwrap();
    }

    let volume_id = VolumeUuid::new_v4();
    let volume = datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 1,
                        extent_count: 1,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                region_snapshots[0].3,
                                region_snapshots[1].3,
                                region_snapshots[2].3,
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            },
        )
        .await
        .unwrap();

    // Sanity check

    assert_eq!(volume.id(), volume_id);

    // Make sure the volume has only three read-only targets:

    let crucible_targets = datastore
        .read_only_resources_associated_with_volume(volume_id)
        .await
        .unwrap();
    assert_eq!(crucible_targets.read_only_targets.len(), 3);

    // Also validate the volume's region_snapshots had volume resource usage
    // records created by volume_create

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_id);
    }

    // Soft delete the volume, and validate that the volume's region_snapshots
    // had their volume resource usage records deleted

    let cr = datastore.soft_delete_volume(volume_id).await.unwrap();

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());
    }

    let datasets_and_regions = datastore.regions_to_delete(&cr).await.unwrap();
    let datasets_and_snapshots =
        datastore.snapshots_to_delete(&cr).await.unwrap();

    assert!(datasets_and_regions.is_empty());
    assert_eq!(datasets_and_snapshots.len(), 3);

    // Now, let's say we're at a spot where the running snapshots have been
    // deleted, but before volume_hard_delete or region_snapshot_remove are
    // called. Pretend another snapshot-create and snapshot-delete snuck in
    // here, and the second snapshot hits a agent that reuses the first target.

    for i in 3..6 {
        let (dataset_id, region_id, snapshot_id, snapshot_addr) =
            &region_snapshots[i];
        datastore
            .region_snapshot_create(nexus_db_model::RegionSnapshot {
                dataset_id: (*dataset_id).into(),
                region_id: *region_id,
                snapshot_id: *snapshot_id,
                snapshot_addr: snapshot_addr.to_string(),
                volume_references: 0,
                deleting: false,
            })
            .await
            .unwrap();
    }

    let volume_id = VolumeUuid::new_v4();
    let volume = datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 1,
                        extent_count: 1,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                region_snapshots[3].3,
                                region_snapshots[4].3,
                                region_snapshots[5].3,
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            },
        )
        .await
        .unwrap();

    // Sanity check

    assert_eq!(volume.id(), volume_id);

    // Make sure the volume has only three read-only targets:

    let crucible_targets = datastore
        .read_only_resources_associated_with_volume(volume_id)
        .await
        .unwrap();
    assert_eq!(crucible_targets.read_only_targets.len(), 3);

    // Also validate only the new volume's region_snapshots had usage records
    // created.

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());
    }

    for i in 3..6 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_id);
    }

    // Soft delete the volume, and validate that only three region_snapshot
    // records are returned.

    let cr = datastore.soft_delete_volume(volume_id).await.unwrap();

    // Make sure every region_snapshot has no usage records now

    for i in 0..6 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());
    }

    let datasets_and_regions = datastore.regions_to_delete(&cr).await.unwrap();
    let datasets_and_snapshots =
        datastore.snapshots_to_delete(&cr).await.unwrap();

    assert!(datasets_and_regions.is_empty());
    assert_eq!(datasets_and_snapshots.len(), 3);
}

#[nexus_test]
async fn test_disk_create_saga_unwinds_correctly(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test that Nexus properly unwinds when not all regions are successfully
    // created.

    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    // Set the third agent to fail creating the region
    let zpool = &disk_test.zpools().nth(2).expect("Expected three zpools");
    let dataset = zpool.crucible_dataset();
    cptestctx
        .first_sled_agent()
        .get_crucible_dataset(zpool.id, dataset.id)
        .set_region_creation_error(true);

    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::INTERNAL_SERVER_ERROR)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_snapshot_create_saga_unwinds_correctly(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test that Nexus properly unwinds when not all regions are successfully
    // created.

    let client = &cptestctx.external_client;
    create_project_and_pool(client).await;

    let disk_test = DiskTest::new(&cptestctx).await;
    let disks_url = get_disks_url();
    let base_disk_name: Name = "base-disk".parse().unwrap();

    // Create a disk

    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let _disk: external::Disk =
        object_create(client, &disks_url, &base_disk).await;

    // Set the third agent to fail creating the region for the snapshot
    let zpool =
        &disk_test.zpools().nth(2).expect("Expected at least three zpools");
    let dataset = zpool.crucible_dataset();
    cptestctx
        .first_sled_agent()
        .get_crucible_dataset(zpool.id, dataset.id)
        .set_region_creation_error(true);

    // Create a snapshot
    let snapshot_create = params::SnapshotCreate {
        identity: IdentityMetadataCreateParams {
            name: "a-snapshot".parse().unwrap(),
            description: "a snapshot!".to_string(),
        },
        disk: base_disk_name.clone().into(),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &get_snapshots_url())
            .body(Some(&snapshot_create))
            .expect_status(Some(StatusCode::INTERNAL_SERVER_ERROR)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url("base-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

// Test function that creates a VolumeConstructionRequest::Region With gen,
// and UUID you passed in.
fn create_region(
    block_size: u64,
    gen: u64,
    id: Uuid,
) -> VolumeConstructionRequest {
    VolumeConstructionRequest::Region {
        block_size,
        blocks_per_extent: 1,
        extent_count: 1,
        gen,
        opts: CrucibleOpts {
            id,
            target: Vec::new(),
            lossy: false,
            flush_timeout: None,
            key: None,
            cert_pem: None,
            key_pem: None,
            root_cert_pem: None,
            control: None,
            read_only: false,
        },
    }
}

// Test function that expects a very specific type of volume and expects
// the generation number provided to match the generation number in the
// sub-volume.  The value 0 in the expected_gen vec tells this test function
// that index in the list of sub_volumes should not be a Region, and therefore
// will not have a generation number field.
fn volume_match_gen(
    volume: nexus_db_model::Volume,
    expected_gen: Vec<Option<u64>>,
) {
    let vcr: VolumeConstructionRequest =
        serde_json::from_str(volume.data()).unwrap();

    println!("VCR is: {:?}", vcr);
    // Volume should have what we started with.
    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes,
            read_only_parent: _,
        } => {
            for (index, sv) in sub_volumes.iter().enumerate() {
                match sv {
                    VolumeConstructionRequest::Region {
                        block_size: _,
                        blocks_per_extent: _,
                        extent_count: _,
                        gen,
                        opts: _,
                    } => {
                        assert_eq!(*gen, expected_gen[index].unwrap());
                    }
                    _ => {
                        assert!(expected_gen[index].is_none());
                    }
                }
            }
        }
        x => {
            panic!("Unexpected volume type returned: {:?}", x);
        }
    }
}

// volume_delete saga node idempotency tests

#[nexus_test]
async fn test_volume_hard_delete_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let volume_id = VolumeUuid::new_v4();
    datastore
        .volume_create(
            volume_id,
            VolumeConstructionRequest::File {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                path: "/lol".to_string(),
            },
        )
        .await
        .unwrap();

    datastore.volume_hard_delete(volume_id).await.unwrap();
    datastore.volume_hard_delete(volume_id).await.unwrap();
}

// internal API related tests

/// Test that an Upstairs can reissue live repair notifications
#[nexus_test]
async fn test_upstairs_repair_notify_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    // Send the same start request.
    let notify_url = format!("/crucible/0/upstairs/{upstairs_id}/repair-start");

    let request = internal::nexus::RepairStartInfo {
        time: Utc::now(),
        session_id,
        repair_id,
        repair_type: internal::nexus::UpstairsRepairType::Live,
        repairs: vec![internal::nexus::DownstairsUnderRepair {
            region_uuid: region_id,
            target_addr: "[fd00:1122:3344:101::8]:12345".parse().unwrap(),
        }],
    };

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(request.clone()),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Send the same finish request.
    let notify_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-finish");

    let request = internal::nexus::RepairFinishInfo {
        time: Utc::now(),
        session_id,
        repair_id,
        repair_type: internal::nexus::UpstairsRepairType::Live,
        repairs: vec![internal::nexus::DownstairsUnderRepair {
            region_uuid: region_id,
            target_addr: "[fd00:1122:3344:101::8]:12345".parse().unwrap(),
        }],
        aborted: false,
    };

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(request.clone()),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Test that an Upstairs cannot issue different finish statuses for the same
/// repair.
#[nexus_test]
async fn test_upstairs_repair_notify_different_finish_status(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    let notify_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-finish");

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: false, // live repair was ok
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: true, // live repair failed?
            }),
            StatusCode::CONFLICT,
        )
        .await
        .unwrap_err();
}

/// Test that the same Upstairs can rerun a repair again.
#[nexus_test]
async fn test_upstairs_repair_same_upstairs_retry(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    // Simulate one failed repair

    let notify_start_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-start");
    let notify_finish_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-finish");

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_finish_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: true,
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Simulate the same Upstairs restarting the repair, which passes this time

    let repair_id = UpstairsRepairUuid::new_v4();

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_finish_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: false,
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Test that a different Upstairs session can rerun a repair again.
#[nexus_test]
async fn test_upstairs_repair_different_upstairs_retry(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    // Simulate one failed repair by one Upstairs

    let notify_start_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-start");
    let notify_finish_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-finish");

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_finish_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: true,
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Simulate a different Upstairs session restarting the repair, which passes this time

    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_finish_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: false,
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Test that a different Upstairs session can rerun an interrupted repair
#[nexus_test]
async fn test_upstairs_repair_different_upstairs_retry_interrupted(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    // Simulate one failed repair by one Upstairs, which was interrupted (which
    // leads to no finish message).

    let notify_start_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-start");
    let notify_finish_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-finish");

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // Simulate a different Upstairs session restarting the interrupted repair,
    // which passes this time

    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_finish_url,
            Some(internal::nexus::RepairFinishInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
                aborted: false,
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Test that the same repair ID cannot be used for different repair types
#[nexus_test]
async fn test_upstairs_repair_repair_id_and_type_conflict(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    let notify_start_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-start");

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type:
                    internal::nexus::UpstairsRepairType::Reconciliation,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::CONFLICT,
        )
        .await
        .unwrap_err();
}

/// Test that an Upstairs can submit progress for a repair
#[nexus_test]
async fn test_upstairs_repair_submit_progress(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let session_id = UpstairsSessionUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();
    let region_id = DownstairsRegionUuid::new_v4();

    // A repair must be started before progress can be submitted

    let notify_start_url =
        format!("/crucible/0/upstairs/{upstairs_id}/repair-start");

    int_client
        .make_request(
            Method::POST,
            &notify_start_url,
            Some(internal::nexus::RepairStartInfo {
                time: Utc::now(),
                session_id,
                repair_id,
                repair_type: internal::nexus::UpstairsRepairType::Live,
                repairs: vec![internal::nexus::DownstairsUnderRepair {
                    region_uuid: region_id,
                    target_addr: "[fd00:1122:3344:101::8]:12345"
                        .parse()
                        .unwrap(),
                }],
            }),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    let progress_url = format!(
        "/crucible/0/upstairs/{upstairs_id}/repair/{repair_id}/progress"
    );

    for i in 0..100 {
        int_client
            .make_request(
                Method::POST,
                &progress_url,
                Some(internal::nexus::RepairProgress {
                    time: Utc::now(),
                    current_item: i,
                    total_items: 100,
                }),
                StatusCode::NO_CONTENT,
            )
            .await
            .unwrap();
    }
}

/// Test that an Upstairs can't submit progress unless a repair was started
#[nexus_test]
async fn test_upstairs_repair_reject_submit_progress_when_no_repair(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let repair_id = UpstairsRepairUuid::new_v4();

    let progress_url = format!(
        "/crucible/0/upstairs/{upstairs_id}/repair/{repair_id}/progress"
    );

    int_client
        .make_request(
            Method::POST,
            &progress_url,
            Some(internal::nexus::RepairProgress {
                time: Utc::now(),
                current_item: 10,
                total_items: 100,
            }),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
}

/// Test that an Upstairs can notify Nexus when a Downstairs client task is
/// requested to stop
#[nexus_test]
async fn test_upstairs_notify_downstairs_client_stop_request(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let downstairs_id = DownstairsUuid::new_v4();

    let stop_request_url = format!(
        "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stop-request"
    );

    // Make sure an Upstairs can re-send the notification

    let request = internal::nexus::DownstairsClientStopRequest {
        time: Utc::now(),
        reason:
            internal::nexus::DownstairsClientStopRequestReason::TooManyOutstandingJobs,
    };

    int_client
        .make_request(
            Method::POST,
            &stop_request_url,
            Some(request.clone()),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &stop_request_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // The client can be requested to stop for the same reason a different time

    let request = internal::nexus::DownstairsClientStopRequest {
        time: Utc::now(),
        reason:
            internal::nexus::DownstairsClientStopRequestReason::TooManyOutstandingJobs,
    };

    int_client
        .make_request(
            Method::POST,
            &stop_request_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // The client can also be requested to stop for a different reason

    let request = internal::nexus::DownstairsClientStopRequest {
        time: Utc::now(),
        reason: internal::nexus::DownstairsClientStopRequestReason::IOError,
    };

    int_client
        .make_request(
            Method::POST,
            &stop_request_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Test that an Upstairs can notify Nexus when a Downstairs client task stops
#[nexus_test]
async fn test_upstairs_notify_downstairs_client_stops(
    cptestctx: &ControlPlaneTestContext,
) {
    let int_client = &cptestctx.internal_client;

    let upstairs_id = UpstairsUuid::new_v4();
    let downstairs_id = DownstairsUuid::new_v4();

    let stopped_url = format!(
        "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stopped"
    );

    // Make sure an Upstairs can re-send the notification

    let request = internal::nexus::DownstairsClientStopped {
        time: Utc::now(),
        reason: internal::nexus::DownstairsClientStoppedReason::ReadFailed,
    };

    int_client
        .make_request(
            Method::POST,
            &stopped_url,
            Some(request.clone()),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    int_client
        .make_request(
            Method::POST,
            &stopped_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // The client can stop for the same reason a different time

    let request = internal::nexus::DownstairsClientStopped {
        time: Utc::now(),
        reason: internal::nexus::DownstairsClientStoppedReason::ReadFailed,
    };

    int_client
        .make_request(
            Method::POST,
            &stopped_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    // The client can also stop for a different reason

    let request = internal::nexus::DownstairsClientStopped {
        time: Utc::now(),
        reason: internal::nexus::DownstairsClientStoppedReason::Timeout,
    };

    int_client
        .make_request(
            Method::POST,
            &stopped_url,
            Some(request),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
}

/// Assert `soft_delete_volume` returns the regions associated with the volume.
#[nexus_test]
async fn test_cte_returns_regions(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let _disk_test = DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    let disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk".parse().unwrap(),
            description: String::from("disk"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: ByteCount::from_gibibytes_u32(2),
    };

    let disk: external::Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let disk_id = disk.identity.id;

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk_id)
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 3);

    let resources_to_clean_up =
        datastore.soft_delete_volume(db_disk.volume_id()).await.unwrap();

    let datasets_and_regions_to_clean =
        datastore.regions_to_delete(&resources_to_clean_up).await.unwrap();

    assert_eq!(datasets_and_regions_to_clean.len(), 3);

    assert_eq!(
        datasets_and_regions_to_clean
            .into_iter()
            .map(|(_, region)| region.id())
            .collect::<Vec<Uuid>>(),
        allocated_regions
            .into_iter()
            .map(|(_, region)| region.id())
            .collect::<Vec<Uuid>>(),
    );
}

struct TestReadOnlyRegionReferenceUsage {
    datastore: Arc<DataStore>,

    region: db::model::Region,
    region_address: SocketAddrV6,

    first_volume_id: VolumeUuid,
    second_volume_id: VolumeUuid,

    last_resources_to_delete: Option<CrucibleResources>,
}

impl TestReadOnlyRegionReferenceUsage {
    pub async fn new(cptestctx: &ControlPlaneTestContext) -> Self {
        let client = &cptestctx.external_client;
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        DiskTestBuilder::new(&cptestctx)
            .on_specific_sled(cptestctx.first_sled_id())
            .with_zpool_count(4)
            .build()
            .await;

        create_project_and_pool(client).await;

        let first_volume_id = VolumeUuid::new_v4();
        let second_volume_id = VolumeUuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let datasets_and_regions = datastore
            .arbitrary_region_allocate(
                &opctx,
                RegionAllocationFor::SnapshotVolume {
                    volume_id: first_volume_id,
                    snapshot_id,
                },
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

        let (_, region) = &datasets_and_regions[0];

        assert_eq!(region.volume_id(), first_volume_id);
        assert!(region.read_only());

        // We're not sending the allocation request to any simulated crucible agent,
        // so fill in a random port here.
        datastore.region_set_port(region.id(), 12345).await.unwrap();

        let region_address =
            datastore.region_addr(region.id()).await.unwrap().unwrap();

        let region = datastore.get_region(region.id()).await.unwrap();

        TestReadOnlyRegionReferenceUsage {
            datastore: datastore.clone(),

            region,
            region_address,

            first_volume_id,
            second_volume_id,

            last_resources_to_delete: None,
        }
    }

    pub async fn create_first_volume(&self) {
        self.datastore
            .volume_create(
                self.first_volume_id,
                VolumeConstructionRequest::Volume {
                    id: *self.first_volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: self.region.blocks_per_extent(),
                        extent_count: self.region.extent_count() as u32,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![self.region_address.into()],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();
    }

    pub async fn validate_only_first_volume_referenced(&self) {
        let usage = self
            .datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: self.region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), self.first_volume_id);
    }

    pub async fn validate_only_second_volume_referenced(&self) {
        let usage = self
            .datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: self.region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), self.second_volume_id);
    }

    pub async fn delete_first_volume(&mut self) {
        let resources_to_delete = self
            .datastore
            .soft_delete_volume(self.first_volume_id)
            .await
            .unwrap();

        self.last_resources_to_delete = Some(resources_to_delete);
    }

    pub async fn delete_second_volume(&mut self) {
        let resources_to_delete = self
            .datastore
            .soft_delete_volume(self.second_volume_id)
            .await
            .unwrap();

        self.last_resources_to_delete = Some(resources_to_delete);
    }

    pub async fn validate_no_usage_records(&self) {
        let usage = self
            .datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: self.region.id(),
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());
    }

    pub async fn validate_region_returned_for_cleanup(&self) {
        assert!(
            self.datastore
                .regions_to_delete(
                    &self.last_resources_to_delete.as_ref().unwrap()
                )
                .await
                .unwrap()
                .into_iter()
                .any(|(_, r)| r.id() == self.region.id())
        );
    }

    pub async fn validate_region_not_returned_for_cleanup(&self) {
        assert!(
            !self
                .datastore
                .regions_to_delete(
                    &self.last_resources_to_delete.as_ref().unwrap()
                )
                .await
                .unwrap()
                .into_iter()
                .any(|(_, r)| r.id() == self.region.id())
        );
    }

    // read-only regions should never be returned by find_deleted_volume_regions
    pub async fn region_not_returned_by_find_deleted_volume_regions(&self) {
        let freed_crucible_resources =
            self.datastore.find_deleted_volume_regions().await.unwrap();

        assert!(
            !freed_crucible_resources
                .datasets_and_regions
                .into_iter()
                .any(|(_, r)| r.id() == self.region.id())
        );
    }

    pub async fn create_first_volume_region_in_rop(&self) {
        self.datastore
            .volume_create(
                self.first_volume_id,
                VolumeConstructionRequest::Volume {
                    id: *self.first_volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: self.region.blocks_per_extent(),
                            extent_count: self.region.extent_count() as u32,
                            gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec![self.region_address.into()],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();
    }

    pub async fn create_second_volume(&self) {
        self.datastore
            .volume_create(
                self.second_volume_id,
                VolumeConstructionRequest::Volume {
                    id: *self.second_volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: self.region.blocks_per_extent(),
                        extent_count: self.region.extent_count() as u32,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![self.region_address.into()],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();
    }

    pub async fn create_second_volume_region_in_rop(&self) {
        self.datastore
            .volume_create(
                self.second_volume_id,
                VolumeConstructionRequest::Volume {
                    id: *self.second_volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: self.region.blocks_per_extent(),
                            extent_count: self.region.extent_count() as u32,
                            gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec![self.region_address.into()],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();
    }

    pub async fn validate_both_volumes_referenced(&self) {
        let usage = self
            .datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: self.region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 2);
        assert!(usage.iter().any(|r| r.volume_id() == self.first_volume_id));
        assert!(usage.iter().any(|r| r.volume_id() == self.second_volume_id));
    }
}

/// Assert that creating a volume with a read-only region in a subvolume creates
/// an appropriate usage record, and that deleting that volume removes it
#[nexus_test]
async fn test_read_only_region_reference_usage_sanity(
    cptestctx: &ControlPlaneTestContext,
) {
    let mut harness = TestReadOnlyRegionReferenceUsage::new(cptestctx).await;

    // Create one volume referencing the harness' read-only region

    harness.create_first_volume().await;

    // Validate only one volume resource usage record was created

    harness.validate_only_first_volume_referenced().await;

    // Now, soft-delete the volume, and make sure that the associated volume
    // resource usage record is gone too.

    harness.delete_first_volume().await;

    harness.validate_no_usage_records().await;

    // If the read-only volume references for a read-only region are gone, then
    // it should be returned for cleanup.

    harness.validate_region_returned_for_cleanup().await;

    // It should not be returned by find_deleted_volume_regions.

    harness.region_not_returned_by_find_deleted_volume_regions().await;
}

/// Assert that creating a volume with a read-only region in the ROP creates an
/// appropriate reference, and that deleting that volume removes it
#[nexus_test]
async fn test_read_only_region_reference_sanity_rop(
    cptestctx: &ControlPlaneTestContext,
) {
    let mut harness = TestReadOnlyRegionReferenceUsage::new(cptestctx).await;

    // Create one volume referencing the harness' read-only region

    harness.create_first_volume_region_in_rop().await;

    // Validate that the appropriate volume resource usage record was created

    harness.validate_only_first_volume_referenced().await;

    // It should be _not_ returned by find_deleted_volume_regions.

    harness.region_not_returned_by_find_deleted_volume_regions().await;

    // Now, soft-delete the volume, and make sure that read-only volume
    // reference is gone too.

    harness.delete_first_volume().await;

    harness.validate_no_usage_records().await;

    // If the read-only volume references for a read-only region are gone, then
    // it should be returned for cleanup.

    harness.validate_region_returned_for_cleanup().await;

    // It should not be returned by find_deleted_volume_regions.

    harness.region_not_returned_by_find_deleted_volume_regions().await;
}

/// Assert that creating multiple volumes with a read-only region creates the
/// appropriate references, and that deleting only one of those volumes does not
/// mean the read-only region gets cleaned up
#[nexus_test]
async fn test_read_only_region_reference_sanity_multi(
    cptestctx: &ControlPlaneTestContext,
) {
    let mut harness = TestReadOnlyRegionReferenceUsage::new(cptestctx).await;

    // Create two volumes this time

    harness.create_first_volume().await;
    harness.create_second_volume().await;

    // Validate that the appropriate volume resource usage records were created

    harness.validate_both_volumes_referenced().await;

    // Now, soft-delete the first volume, and make sure that only one read-only
    // volume reference is gone.

    harness.delete_first_volume().await;

    harness.validate_only_second_volume_referenced().await;

    // If any read-only volume reference remains, then the region should not be
    // returned for deletion, and it still should not be returned by
    // `find_deleted_volume_regions`.

    harness.validate_region_not_returned_for_cleanup().await;

    harness.region_not_returned_by_find_deleted_volume_regions().await;

    // Deleting the second volume should free up the read-only region for
    // deletion

    harness.delete_second_volume().await;

    harness.validate_no_usage_records().await;

    harness.validate_region_returned_for_cleanup().await;

    // It should not be returned by find_deleted_volume_regions.

    harness.region_not_returned_by_find_deleted_volume_regions().await;
}

/// Assert that creating multiple volumes with a read-only region in the ROP
/// creates the appropriate references, and that deleting only one of those
/// volumes does not mean the read-only region gets cleaned up
#[nexus_test]
async fn test_read_only_region_reference_sanity_rop_multi(
    cptestctx: &ControlPlaneTestContext,
) {
    let mut harness = TestReadOnlyRegionReferenceUsage::new(cptestctx).await;

    // Create two volumes this time

    harness.create_first_volume_region_in_rop().await;
    harness.create_second_volume_region_in_rop().await;

    // Validate that the appropriate volume resource usage records were created

    harness.validate_both_volumes_referenced().await;

    // Now, soft-delete the volume, and make sure that only one read-only volume
    // reference is gone.

    harness.delete_first_volume().await;

    harness.validate_only_second_volume_referenced().await;

    // If any read-only volume reference remains, then the region should not be
    // returned for deletion, and it still should not be returned by
    // `find_deleted_volume_regions`.

    harness.validate_region_not_returned_for_cleanup().await;

    harness.region_not_returned_by_find_deleted_volume_regions().await;

    // Deleting the second volume should free up the read-only region for
    // deletion

    harness.delete_second_volume().await;

    harness.validate_no_usage_records().await;

    harness.validate_region_returned_for_cleanup().await;

    // It should not be returned by find_deleted_volume_regions.

    harness.region_not_returned_by_find_deleted_volume_regions().await;
}

/// Assert that a read-only region is properly reference counted and not
/// prematurely deleted:
///
/// 1) create a disk, then a snapshot of that disk, then a disk from that
///    snapshot
///
/// 2) issue a region snapshot replacement request for one of the region
///    snapshots in that snapshot, then run that process to completion
///
/// 3) delete the snapshot
///
/// 4) expect that the reference to the read-only region in the disk created
///    from the snapshot means that read-only region will not be cleaned up by
///    Nexus
///
/// 5) clean up all the objects, and expect the crucible resources are properly
///    cleaned up
#[nexus_test(extra_sled_agents = 3)]
async fn test_read_only_region_reference_counting(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 4 sleds. This is required for region
    // replacement or region snapshot replacement
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    // Perform region snapshot replacement for one of the snapshot's regions,
    // causing a read-only region to be created.

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 3);

    let (dataset, region) = &allocated_regions[0];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    // The snapshot's allocated regions should have the one read-only region

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 1);
    let (_, read_only_region) = &allocated_regions[0];
    assert!(read_only_region.read_only());

    let db_read_only_dataset = datastore
        .crucible_dataset_get(read_only_region.dataset_id())
        .await
        .unwrap();

    // The disk-from-snap VCR should also reference that read-only region

    let Disk::Crucible(db_disk_from_snapshot) = datastore
        .disk_get(&opctx, disk_from_snapshot.identity.id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "disk_from_snapshot {:?} should exist",
                disk_from_snapshot.identity.id
            )
        });

    let read_only_region_address: SocketAddrV6 =
        nexus.region_addr(&opctx.log, read_only_region.id()).await.unwrap();

    assert!(
        datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                read_only_region_address.into()
            )
            .await
            .unwrap()
            .iter()
            .any(|volume| volume.id() == db_disk_from_snapshot.volume_id())
    );

    // Expect that there are two read-only region references now: one in the
    // snapshot volume, and one in the disk-from-snap volume.

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 2);
    assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
    assert!(
        usage
            .iter()
            .any(|r| r.volume_id() == db_disk_from_snapshot.volume_id())
    );

    // Deleting the snapshot should _not_ cause the region to get deleted from
    // CRDB

    NexusRequest::object_delete(client, &get_snapshot_url("snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    let post_delete_region =
        datastore.get_region(read_only_region.id()).await.unwrap();
    assert_eq!(post_delete_region, *read_only_region);

    // or cause Nexus to send delete commands to the appropriate Crucible
    // agent.

    let mut region_found = false;
    let mut region_still_state_crated = false;

    for sled_agent in cptestctx.all_sled_agents() {
        let zpool_id = db_read_only_dataset.pool_id();
        if !sled_agent.sled_agent.has_zpool(zpool_id) {
            continue;
        }

        if let Some(region) = sled_agent
            .sled_agent
            .get_crucible_dataset(zpool_id, db_read_only_dataset.id())
            .get(crucible_agent_client::types::RegionId(
                read_only_region.id().to_string(),
            ))
        {
            region_found = true;

            if region.state == crucible_agent_client::types::State::Created {
                region_still_state_crated = true;
            }

            break;
        }
    }

    assert!(region_found);
    assert!(region_still_state_crated);

    // Expect that there is one read-only region reference now, and that's from
    // disk-from-snap

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].volume_id(), db_disk_from_snapshot.volume_id());

    // Delete the disk, and expect that does not alter the volume usage records

    NexusRequest::object_delete(client, &get_disk_url("disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].volume_id(), db_disk_from_snapshot.volume_id());

    // Delete the disk from snapshot, verify everything is cleaned up

    NexusRequest::object_delete(client, &get_disk_url("disk-from-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert!(usage.is_empty());

    let mut region_found = false;
    let mut region_destroyed = false;

    for sled_agent in cptestctx.all_sled_agents() {
        let zpool_id = db_read_only_dataset.pool_id();
        if !sled_agent.sled_agent.has_zpool(zpool_id) {
            continue;
        }

        if let Some(region) = sled_agent
            .sled_agent
            .get_crucible_dataset(zpool_id, db_read_only_dataset.id())
            .get(crucible_agent_client::types::RegionId(
                read_only_region.id().to_string(),
            ))
        {
            region_found = true;

            if region.state == crucible_agent_client::types::State::Destroyed {
                region_destroyed = true;
            }

            break;
        }
    }

    assert!(region_found);
    assert!(region_destroyed);

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

/// Assert that a snapshot of a volume with a read-only region is properly
/// reference counted.
#[nexus_test(extra_sled_agents = 3)]
async fn test_read_only_region_reference_counting_layers(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 4 sleds. This is required for region
    // replacement or region snapshot replacement
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    // Perform region snapshot replacement for one of the snapshot's regions,
    // causing a read-only region to be created.

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 3);

    let (dataset, region) = &allocated_regions[0];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    // Grab the read-only region in the snapshot volume

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 1);
    let (_, read_only_region) = &allocated_regions[0];
    assert!(read_only_region.read_only());

    // The disk-from-snap VCR should also reference that read-only region

    let Disk::Crucible(db_disk_from_snapshot) = datastore
        .disk_get(&opctx, disk_from_snapshot.identity.id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "disk_from_snapshot {:?} should exist",
                disk_from_snapshot.identity.id
            )
        });

    let read_only_region_address: SocketAddrV6 =
        nexus.region_addr(&opctx.log, read_only_region.id()).await.unwrap();

    assert!(
        datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                read_only_region_address.into()
            )
            .await
            .unwrap()
            .iter()
            .any(|volume| volume.id() == db_disk_from_snapshot.volume_id())
    );

    // Expect that there are two read-only region references now: one in the
    // snapshot volume, and one in the disk-from-snap volume.

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 2);
    assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
    assert!(
        usage
            .iter()
            .any(|r| r.volume_id() == db_disk_from_snapshot.volume_id())
    );

    // Take a snapshot of the disk-from-snapshot disk

    let double_snapshot = create_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        "double-snapshot",
    )
    .await;

    // Assert correct volume usage records

    let (.., db_double_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(double_snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!(
                "double_snapshot {:?} should exist",
                double_snapshot.identity.id
            )
        });

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 3);
    assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
    assert!(
        usage
            .iter()
            .any(|r| r.volume_id() == db_disk_from_snapshot.volume_id())
    );
    assert!(
        usage.iter().any(|r| r.volume_id() == db_double_snapshot.volume_id())
    );

    // Delete resources, assert volume resource usage records along the way

    NexusRequest::object_delete(client, &get_disk_url("disk-from-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 2);
    assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
    assert!(
        usage.iter().any(|r| r.volume_id() == db_double_snapshot.volume_id())
    );

    NexusRequest::object_delete(client, &get_snapshot_url("snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 1);
    assert!(
        usage.iter().any(|r| r.volume_id() == db_double_snapshot.volume_id())
    );

    NexusRequest::object_delete(client, &get_snapshot_url("double-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion {
                region_id: read_only_region.id(),
            },
        )
        .await
        .unwrap();

    assert!(usage.is_empty());

    NexusRequest::object_delete(client, &get_disk_url("disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test]
async fn test_volume_replace_snapshot_respects_accounting(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create a disk, then a snapshot of that disk

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    // There won't be any regions for the snapshot volume, only region snapshots

    assert!(allocated_regions.is_empty());

    // Get another region to use with volume_replace_snapshot

    datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: db_snapshot.volume_id(),
                snapshot_id: db_snapshot.id(),
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            allocated_regions.len() + 1,
        )
        .await
        .unwrap();

    // Get the newly allocated region

    let mut new_allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(new_allocated_regions.len(), 1);

    let (_, new_region) =
        new_allocated_regions.pop().expect("we just checked the length!");

    // We're not sending the allocation request to any simulated crucible agent,
    // so fill in a random port here.
    datastore.region_set_port(new_region.id(), 12345).await.unwrap();

    // Create a blank region to use as the "volume to delete"

    let volume_to_delete_id = VolumeUuid::new_v4();

    datastore
        .volume_create(
            volume_to_delete_id,
            VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: None,
            },
        )
        .await
        .unwrap();

    // Assert the correct volume resource usage record before the replacement:
    // nothing should reference the newly allocated region yet

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion { region_id: new_region.id() },
        )
        .await
        .unwrap();

    assert!(usage.is_empty());

    // Perform replacement of the first region in the snapshot

    let existing = datastore
        .region_snapshot_get(
            disk_allocated_regions[0].1.dataset_id(),
            disk_allocated_regions[0].1.id(),
            db_snapshot.id(),
        )
        .await
        .expect("region snapshot exists!")
        .unwrap();

    let replacement = datastore
        .region_addr(new_region.id())
        .await
        .expect("new region has address!")
        .unwrap();

    let replacement_result = datastore
        .volume_replace_snapshot(
            VolumeWithTarget(db_snapshot.volume_id()),
            ExistingTarget(existing.snapshot_addr.parse().unwrap()),
            ReplacementTarget(replacement),
            VolumeToDelete(volume_to_delete_id),
        )
        .await
        .unwrap();

    match replacement_result {
        VolumeReplaceResult::Done => {
            // ok!
        }

        _ => {
            panic!("replacement result was {replacement_result:?}");
        }
    }

    // Assert the volume resource usage record after volume_replace_snapshot:
    // the new region should have a usage for the snapshot's volume

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion { region_id: new_region.id() },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].volume_id(), db_snapshot.volume_id());

    // Now, reverse the replacement

    let replacement_result = datastore
        .volume_replace_snapshot(
            VolumeWithTarget(db_snapshot.volume_id()),
            ExistingTarget(replacement), // swapped!
            ReplacementTarget(existing.snapshot_addr.parse().unwrap()), // swapped!
            VolumeToDelete(volume_to_delete_id),
        )
        .await
        .unwrap();

    match replacement_result {
        VolumeReplaceResult::Done => {
            // ok!
        }

        _ => {
            panic!("replacement result was {replacement_result:?}");
        }
    }

    // Assert the new region's volume resource usage record now references the
    // volume to delete

    let usage = datastore
        .volume_usage_records_for_resource(
            VolumeResourceUsage::ReadOnlyRegion { region_id: new_region.id() },
        )
        .await
        .unwrap();

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].volume_id(), volume_to_delete_id);
}

/// Test that the `volume_remove_rop` function correctly updates volume resource
/// usage records
#[nexus_test]
async fn test_volume_remove_rop_respects_accounting(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create a disk, then a snapshot of that disk, then a disk from that
    // snapshot.

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    let Disk::Crucible(db_disk_from_snapshot) = datastore
        .disk_get(&opctx, disk_from_snapshot.identity.id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "disk_from_snapshot {:?} should exist",
                disk_from_snapshot.identity.id
            )
        });

    // Assert the correct volume resource usage records before the removal:
    // both the snapshot volume and disk_from_snapshot volume should have usage
    // records for the three region snapshots.

    for (_, disk_allocated_region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_region.dataset_id(),
                disk_allocated_region.id(),
                db_snapshot.id(),
            )
            .await
            .expect("region snapshot exists!")
            .unwrap();

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 2);
        assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
        assert!(
            usage
                .iter()
                .any(|r| r.volume_id() == db_disk_from_snapshot.volume_id())
        );
    }

    // Remove the ROP from disk-from-snapshot

    let volume_to_delete_id = VolumeUuid::new_v4();

    datastore
        .volume_create(
            volume_to_delete_id,
            VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: None,
            },
        )
        .await
        .unwrap();

    let result = datastore
        .volume_remove_rop(
            db_disk_from_snapshot.volume_id(),
            volume_to_delete_id,
        )
        .await
        .unwrap();

    // Assert that there was a removal

    assert!(result);

    // Assert the correct volume resource usage records after the removal:
    // the snapshot volume should still have usage records for the three region
    // snapshots, and now so should the volume to delete

    for (_, disk_allocated_region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_region.dataset_id(),
                disk_allocated_region.id(),
                db_snapshot.id(),
            )
            .await
            .expect("region snapshot exists!")
            .unwrap();

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 2);
        assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
        assert!(usage.iter().any(|r| r.volume_id() == volume_to_delete_id));
    }
}

/// Test that the `volume_remove_rop` function only updates volume resource
/// usage records for the volume being operated on
#[nexus_test]
async fn test_volume_remove_rop_respects_accounting_no_modify_others(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create a disk, then a snapshot of that disk, then a disk from that
    // snapshot.

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let disk_allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(disk_allocated_regions.len(), 3);

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    let Disk::Crucible(db_disk_from_snapshot) = datastore
        .disk_get(&opctx, disk_from_snapshot.identity.id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "disk_from_snapshot {:?} should exist",
                disk_from_snapshot.identity.id
            )
        });

    let another_disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "another-disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    let Disk::Crucible(db_another_disk_from_snapshot) = datastore
        .disk_get(&opctx, another_disk_from_snapshot.identity.id)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "another_disk_from_snapshot {:?} should exist",
                another_disk_from_snapshot.identity.id
            )
        });

    // Assert the correct volume resource usage records before the removal: the
    // snapshot volume, disk_from_snapshot volume, and
    // another_disk_from_snapshot volume should have usage records for the three
    // region snapshots.

    for (_, disk_allocated_region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_region.dataset_id(),
                disk_allocated_region.id(),
                db_snapshot.id(),
            )
            .await
            .expect("region snapshot exists!")
            .unwrap();

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 3);
        assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
        assert!(
            usage
                .iter()
                .any(|r| r.volume_id() == db_disk_from_snapshot.volume_id())
        );
        assert!(usage.iter().any(
            |r| r.volume_id() == db_another_disk_from_snapshot.volume_id()
        ));
    }

    // Remove the ROP from disk-from-snapshot

    let volume_to_delete_id = VolumeUuid::new_v4();

    datastore
        .volume_create(
            volume_to_delete_id,
            VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: None,
            },
        )
        .await
        .unwrap();

    let result = datastore
        .volume_remove_rop(
            db_disk_from_snapshot.volume_id(),
            volume_to_delete_id,
        )
        .await
        .unwrap();

    // Assert that there was a removal

    assert!(result);

    // Assert the correct volume resource usage records after the removal: the
    // snapshot volume and another_disk_from_snapshot volume should still have
    // usage records for the three region snapshots, and now so should the
    // volume to delete.

    for (_, disk_allocated_region) in &disk_allocated_regions {
        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_region.dataset_id(),
                disk_allocated_region.id(),
                db_snapshot.id(),
            )
            .await
            .expect("region snapshot exists!")
            .unwrap();

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 3);
        assert!(usage.iter().any(|r| r.volume_id() == db_snapshot.volume_id()));
        assert!(usage.iter().any(|r| r.volume_id() == volume_to_delete_id));
        assert!(usage.iter().any(
            |r| r.volume_id() == db_another_disk_from_snapshot.volume_id()
        ));
    }
}

async fn delete_all_volume_resource_usage_records(datastore: &DataStore) {
    use nexus_db_schema::schema::volume_resource_usage::dsl;

    let conn = datastore.pool_connection_for_tests().await.unwrap();

    diesel::delete(dsl::volume_resource_usage)
        .filter(dsl::usage_id.ne(Uuid::new_v4()))
        .execute_async(&*conn)
        .await
        .unwrap();
}

async fn perform_migration(datastore: &DataStore) {
    const MIGRATION_TO_REF_COUNT_WITH_RECORDS_SQL: &str = include_str!(
        "../../../schema/crdb/crucible-ref-count-records/up08.sql"
    );

    assert!(
        MIGRATION_TO_REF_COUNT_WITH_RECORDS_SQL
            .contains("INSERT INTO volume_resource_usage")
    );

    let conn = datastore.pool_connection_for_tests().await.unwrap();

    // To make sure that the migration is idempotent, perform it twice
    diesel::sql_query(MIGRATION_TO_REF_COUNT_WITH_RECORDS_SQL)
        .execute_async(&*conn)
        .await
        .unwrap();

    diesel::sql_query(MIGRATION_TO_REF_COUNT_WITH_RECORDS_SQL)
        .execute_async(&*conn)
        .await
        .unwrap();
}

async fn get_volume_resource_usage_records(
    datastore: &DataStore,
) -> HashSet<VolumeResourceUsageRecord> {
    use nexus_db_schema::schema::volume_resource_usage::dsl;

    let mut records: Vec<VolumeResourceUsageRecord> = Vec::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);
    let conn = datastore.pool_connection_for_tests().await.unwrap();

    while let Some(p) = paginator.next() {
        let page = paginated(
            dsl::volume_resource_usage,
            dsl::usage_id,
            &p.current_pagparams(),
        )
        .get_results_async::<VolumeResourceUsageRecord>(&*conn)
        .await
        .unwrap();

        paginator = p.found_batch(&page, &|r| r.usage_id);

        for record in page {
            records.push(record);
        }
    }

    records
        .into_iter()
        .map(|mut record: VolumeResourceUsageRecord| {
            // Zero out usage_id for comparison
            record.usage_id = Uuid::nil();
            record
        })
        .collect()
}

#[nexus_test]
async fn test_migrate_to_ref_count_with_records(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();

    DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create a disk

    create_disk(&client, PROJECT_NAME, "disk").await;

    // Test migration

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);

    // Create a snapshot

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    // Test migration

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);

    // Create a disk from that snapshot

    create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    // Test migration

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);

    // Delete the snapshot

    NexusRequest::object_delete(client, &get_snapshot_url("snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Test the migration

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);

    // Delete the disk from snapshot

    NexusRequest::object_delete(client, &get_disk_url("disk-from-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk-from-snapshot");

    // Test the migration

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);
}

#[nexus_test]
async fn test_migrate_to_ref_count_with_records_soft_delete_volume(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create a disk, then a snapshot from that disk, then an image based on
    // that snapshot

    create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "windows98".parse().unwrap(),
            description: String::from("as soon as we get CSM support!"),
        },
        source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        os: "windows98".to_string(),
        version: "se".to_string(),
    };

    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    NexusRequest::objects_post(client, &images_url, &params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // Soft-delete the snapshot's volume

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let resources =
        datastore.soft_delete_volume(db_snapshot.volume_id()).await.unwrap();

    // Assert that the region snapshots did not have deleted set to true

    assert!(
        datastore.snapshots_to_delete(&resources).await.unwrap().is_empty()
    );

    // This means that the snapshot volume is soft-deleted, make sure the
    // migration does not make usage records for it!

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);
}

#[nexus_test]
async fn test_migrate_to_ref_count_with_records_region_snapshot_deleting(
    cptestctx: &ControlPlaneTestContext,
) {
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();

    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(cptestctx.first_sled_id())
        .with_zpool_count(4)
        .build()
        .await;

    let mut iter = disk_test.zpools();
    let zpool0 = iter.next().expect("Expected four zpools");
    let zpool1 = iter.next().expect("Expected four zpools");
    let zpool2 = iter.next().expect("Expected four zpools");
    let zpool3 = iter.next().expect("Expected four zpools");

    // (dataset_id, region_id, snapshot_id, snapshot_addr)
    let region_snapshots: Vec<(DatasetUuid, Uuid, Uuid, SocketAddr)> = vec![
        (
            zpool0.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:101::7]:19016".parse().unwrap(),
        ),
        (
            zpool1.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:102::7]:19016".parse().unwrap(),
        ),
        (
            zpool2.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:103::7]:19016".parse().unwrap(),
        ),
        (
            zpool3.crucible_dataset().id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "[fd00:1122:3344:104::7]:19016".parse().unwrap(),
        ),
    ];

    for i in 0..4 {
        let (dataset_id, region_id, snapshot_id, snapshot_addr) =
            &region_snapshots[i];

        datastore
            .region_snapshot_create(nexus_db_model::RegionSnapshot {
                dataset_id: to_db_typed_uuid(*dataset_id),
                region_id: *region_id,
                snapshot_id: *snapshot_id,
                snapshot_addr: snapshot_addr.to_string(),
                volume_references: 0,
                deleting: false,
            })
            .await
            .unwrap();
    }

    // Create two volumes, one with the first three region snapshots, one with
    // the last three region snapshots

    let first_volume_id = VolumeUuid::new_v4();
    datastore
        .volume_create(
            first_volume_id,
            VolumeConstructionRequest::Volume {
                id: *first_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 1,
                        extent_count: 1,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                region_snapshots[0].3,
                                region_snapshots[1].3,
                                region_snapshots[2].3,
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            },
        )
        .await
        .unwrap();

    let second_volume_id = VolumeUuid::new_v4();
    datastore
        .volume_create(
            second_volume_id,
            VolumeConstructionRequest::Volume {
                id: *second_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 1,
                        extent_count: 1,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: Uuid::new_v4(),
                            target: vec![
                                region_snapshots[1].3,
                                region_snapshots[2].3,
                                region_snapshots[3].3,
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            },
        )
        .await
        .unwrap();

    // Deleting the first volume should only return region_snapshot[0] for
    // deletion.

    let resources =
        datastore.soft_delete_volume(first_volume_id).await.unwrap();

    let snapshots_to_delete =
        datastore.snapshots_to_delete(&resources).await.unwrap();

    assert_eq!(snapshots_to_delete.len(), 1);

    let region_snapshot_to_delete = &snapshots_to_delete[0].1;

    assert_eq!(
        region_snapshot_to_delete.dataset_id,
        to_db_typed_uuid(region_snapshots[0].0)
    );
    assert_eq!(region_snapshot_to_delete.region_id, region_snapshots[0].1);
    assert_eq!(region_snapshot_to_delete.snapshot_id, region_snapshots[0].2);
    assert_eq!(
        region_snapshot_to_delete.snapshot_addr.parse::<SocketAddr>().unwrap(),
        region_snapshots[0].3
    );
    assert_eq!(region_snapshot_to_delete.volume_references, 0);
    assert_eq!(region_snapshot_to_delete.deleting, true);

    // Test the migration does not incorrectly think a region snapshot with
    // deleting = true is used by any volume

    let records_before = get_volume_resource_usage_records(&datastore).await;

    delete_all_volume_resource_usage_records(&datastore).await;
    perform_migration(&datastore).await;

    let records_after = get_volume_resource_usage_records(&datastore).await;

    assert_eq!(records_before, records_after);
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_double_layer_with_read_only_region_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    // 1) Create disk, snapshot, then two disks from those snapshots
    // 2) Replace one of the region snapshots in that snapshot volume with a
    //    read-only region
    // 3) Delete in the following order: disk, snapshot, then two disks from the
    //    snapshot - after each delete, verify that crucible resources were not
    //    prematurely deleted
    // 6) At the end, assert that all Crucible resources were cleaned up

    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the four sleds. This is required for region
    // replacement or region snapshot replacement
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    let _another_disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "another-disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    // Perform region snapshot replacement for one of the snapshot's targets,
    // causing a read-only region to be created.

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 3);

    let (dataset, region) = &allocated_regions[0];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk and snapshot

    NexusRequest::object_delete(client, &get_disk_url("disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    NexusRequest::object_delete(client, &get_snapshot_url("snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete disk-from-snapshot

    NexusRequest::object_delete(client, &get_disk_url("disk-from-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk-from-snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Finally, delete another-disk-from-snapshot

    NexusRequest::object_delete(
        client,
        &get_disk_url("another-disk-from-snapshot"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to delete another-disk-from-snapshot");

    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_double_layer_snapshot_with_read_only_region_delete_2(
    cptestctx: &ControlPlaneTestContext,
) {
    // 1) Create disk, then a snapshot
    // 2) Replace two of the region snapshots in that snapshot volume with
    //    read-only regions
    // 3) Create a disk from the snapshot
    // 4) Replace the last of the region snapshots in that snapshot volume with
    //    a read-only region
    // 5) Delete in the following order: disk, snapshot, then the disk from the
    //    snapshot - after each delete, verify that crucible resources were not
    //    prematurely deleted
    // 6) At the end, assert that all Crucible resources were cleaned up

    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the four sleds. This is required for region
    // replacement or region snapshot replacement
    let disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    // Perform region snapshot replacement for two of the snapshot's targets,
    // causing two read-only regions to be created.

    let Disk::Crucible(db_disk) = datastore
        .disk_get(&opctx, disk.identity.id)
        .await
        .unwrap_or_else(|_| panic!("disk {:?} should exist", disk.identity.id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 3);

    let (dataset, region) = &allocated_regions[0];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    let request_id = request.id;

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    wait_for_condition(
        || {
            let datastore = datastore.clone();
            let opctx = OpContext::for_tests(
                cptestctx.logctx.log.new(o!()),
                datastore.clone(),
            );

            async move {
                let request = datastore
                    .get_region_snapshot_replacement_request_by_id(
                        &opctx, request_id,
                    )
                    .await
                    .unwrap();

                let state = request.replacement_state;

                if state == RegionSnapshotReplacementState::Complete {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            }
        },
        &std::time::Duration::from_millis(50),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("request transitioned to expected state");

    let (dataset, region) = &allocated_regions[1];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    assert!(!disk_test.crucible_resources_deleted().await);

    // Create a disk from the snapshot

    let _disk_from_snapshot = create_disk_from_snapshot(
        &client,
        PROJECT_NAME,
        "disk-from-snapshot",
        snapshot.identity.id,
    )
    .await;

    // Replace the last of the region snapshot targets

    let (dataset, region) = &allocated_regions[2];

    let request = RegionSnapshotReplacement::new_from_region_snapshot(
        dataset.id(),
        region.id(),
        snapshot.identity.id,
    );

    datastore
        .insert_region_snapshot_replacement_request(&opctx, request)
        .await
        .unwrap();

    wait_for_all_replacements(datastore, &lockstep_client).await;

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk and snapshot

    NexusRequest::object_delete(client, &get_disk_url("disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    NexusRequest::object_delete(client, &get_snapshot_url("snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete disk-from-snapshot

    NexusRequest::object_delete(client, &get_disk_url("disk-from-snapshot"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk-from-snapshot");

    // Assert everything was cleaned up

    assert!(disk_test.crucible_resources_deleted().await);
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_no_zombie_region_snapshots(cptestctx: &ControlPlaneTestContext) {
    // 1) Create disk, then a snapshot
    // 2) Delete the disk
    // 3) Create a volume that uses the snapshot volume as a read-only parent
    // 4) Test that a race of the following steps does not cause a region
    //    snapshot to have volume usage records even though it is marked for
    //    deletion:
    //
    //    a) delete the snapshot volume
    //    b) delete the volume created in step 3
    //    c) create another volume that uses the snapshot volume as a read-only
    //       parent

    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 4 sleds. This is required for region
    // replacement or region snapshot replacement
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    // Create disk, then a snapshot

    let _disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    // Delete the disk

    NexusRequest::object_delete(client, &get_disk_url("disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Create a volume that uses the snapshot volume as a read-only parent

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let snapshot_volume: Volume = datastore
        .volume_get(db_snapshot.volume_id())
        .await
        .expect("volume_get without error")
        .expect("volume exists");

    let snapshot_vcr: VolumeConstructionRequest =
        serde_json::from_str(snapshot_volume.data()).unwrap();

    let step_3_volume_id = VolumeUuid::new_v4();
    datastore
        .volume_create(
            step_3_volume_id,
            VolumeConstructionRequest::Volume {
                id: *step_3_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(snapshot_vcr.clone())),
            },
        )
        .await
        .unwrap();

    // Soft-delete the snapshot volume

    let cr =
        datastore.soft_delete_volume(db_snapshot.volume_id()).await.unwrap();

    // Assert that no resources are returned for clean-up

    assert!(datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Soft-delete the volume created as part of step 3

    let cr = datastore.soft_delete_volume(step_3_volume_id).await.unwrap();

    // Assert that region snapshots _are_ returned for clean-up

    assert!(datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(!datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Pretend that there's a racing call to volume_create that has a volume
    // that uses the snapshot volume as a read-only parent. This call should
    // fail!

    let racing_volume_id = VolumeUuid::new_v4();
    let result = datastore
        .volume_create(
            racing_volume_id,
            VolumeConstructionRequest::Volume {
                id: *racing_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(snapshot_vcr.clone())),
            },
        )
        .await;

    assert!(result.is_err());
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_no_zombie_read_only_regions(cptestctx: &ControlPlaneTestContext) {
    // 1) Create a volume with three read-only regions
    // 2) Create another volume that uses the first step's volume as a read-only
    //    parent
    // 3) Test that a race of the following steps does not cause a region to
    //    have volume usage records:
    //
    //    a) delete the step 1 volume
    //    b) delete the step 2 volume
    //    c) create another volume that uses the step 1 volume as a read-only
    //       parent

    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 4 sleds. This is required for region
    // replacement or region snapshot replacement
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Create a volume with three read-only regions

    let step_1_volume_id = VolumeUuid::new_v4();
    let snapshot_id = Uuid::new_v4();

    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: step_1_volume_id,
                snapshot_id,
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            3,
        )
        .await
        .unwrap();

    // We're not sending allocation requests to any simulated crucible agent, so
    // fill in a random port here.
    for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
        datastore.region_set_port(region.id(), 20000 + i as u16).await.unwrap();
    }

    let region_addrs: Vec<SocketAddrV6> = vec![
        datastore
            .region_addr(datasets_and_regions[0].1.id())
            .await
            .unwrap()
            .unwrap(),
        datastore
            .region_addr(datasets_and_regions[1].1.id())
            .await
            .unwrap()
            .unwrap(),
        datastore
            .region_addr(datasets_and_regions[2].1.id())
            .await
            .unwrap()
            .unwrap(),
    ];

    // Assert they're all unique

    assert_ne!(region_addrs[0], region_addrs[1]);
    assert_ne!(region_addrs[0], region_addrs[2]);
    assert_ne!(region_addrs[1], region_addrs[2]);

    // Mimic what a snapshot volume has: three read-only regions in a volume's
    // subvolume, not the read-only parent

    datastore
        .volume_create(
            step_1_volume_id,
            VolumeConstructionRequest::Volume {
                id: *step_1_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            region_addrs[0].into(),
                            region_addrs[1].into(),
                            region_addrs[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        )
        .await
        .unwrap();

    // Create another volume that uses the first step's volume as a read-only
    // parent

    let step_1_volume: Volume = datastore
        .volume_get(step_1_volume_id)
        .await
        .expect("volume_get without error")
        .expect("volume exists");

    let step_1_vcr: VolumeConstructionRequest =
        serde_json::from_str(step_1_volume.data()).unwrap();

    let step_2_volume_id = VolumeUuid::new_v4();

    datastore
        .volume_create(
            step_2_volume_id,
            VolumeConstructionRequest::Volume {
                id: *step_2_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(step_1_vcr.clone())),
            },
        )
        .await
        .unwrap();

    // Soft-delete the step 1 volume

    let cr = datastore.soft_delete_volume(step_1_volume_id).await.unwrap();

    // Assert that no resources are returned for clean-up

    assert!(datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Soft-delete the step 2 volume

    let cr = datastore.soft_delete_volume(step_2_volume_id).await.unwrap();

    // Assert that the read-only regions _are_ returned for clean-up

    assert!(!datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Pretend that there's a racing call to volume_create that has a volume
    // that uses the step 1 volume as a read-only parent. This call should
    // fail!

    let racing_volume_id = VolumeUuid::new_v4();
    let result = datastore
        .volume_create(
            racing_volume_id,
            VolumeConstructionRequest::Volume {
                id: *racing_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(step_1_vcr)),
            },
        )
        .await;

    assert!(result.is_err());
}

#[nexus_test(extra_sled_agents = 3)]
async fn test_no_zombie_read_write_regions(
    cptestctx: &ControlPlaneTestContext,
) {
    // 1) Create a volume with three read-write regions
    // 2) Create another volume that uses the first step's volume as a read-only
    //    parent
    // 3) Test that a race of the following steps does not cause a region to
    //    have volume usage records:
    //
    //    a) delete the step 1 volume
    //    b) delete the step 2 volume
    //    c) create another volume that uses the step 1 volume as a read-only
    //       parent

    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 4 sleds. This is required for region
    // replacement or region snapshot replacement
    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    // Create a volume with three read-only regions

    let step_1_volume_id = VolumeUuid::new_v4();
    let snapshot_id = Uuid::new_v4();

    let datasets_and_regions = datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: step_1_volume_id,
                snapshot_id,
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
                size: ByteCount::from_gibibytes_u32(1),
            },
            &RegionAllocationStrategy::Random { seed: None },
            3,
        )
        .await
        .unwrap();

    // We're not sending allocation requests to any simulated crucible agent, so
    // fill in a random port here.
    for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
        datastore.region_set_port(region.id(), 20000 + i as u16).await.unwrap();
    }

    let region_addrs: Vec<SocketAddrV6> = vec![
        datastore
            .region_addr(datasets_and_regions[0].1.id())
            .await
            .unwrap()
            .unwrap(),
        datastore
            .region_addr(datasets_and_regions[1].1.id())
            .await
            .unwrap()
            .unwrap(),
        datastore
            .region_addr(datasets_and_regions[2].1.id())
            .await
            .unwrap()
            .unwrap(),
    ];

    // Assert they're all unique

    assert_ne!(region_addrs[0], region_addrs[1]);
    assert_ne!(region_addrs[0], region_addrs[2]);
    assert_ne!(region_addrs[1], region_addrs[2]);

    // Mimic what a snapshot volume has: three read-only regions in a volume's
    // subvolume, not the read-only parent

    datastore
        .volume_create(
            step_1_volume_id,
            VolumeConstructionRequest::Volume {
                id: *step_1_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            region_addrs[0].into(),
                            region_addrs[1].into(),
                            region_addrs[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        )
        .await
        .unwrap();

    // Create another volume that uses the first step's volume as a read-only
    // parent

    let step_1_volume: Volume = datastore
        .volume_get(step_1_volume_id)
        .await
        .expect("volume_get without error")
        .expect("volume exists");

    let step_1_vcr: VolumeConstructionRequest =
        serde_json::from_str(step_1_volume.data()).unwrap();

    let step_2_volume_id = VolumeUuid::new_v4();

    datastore
        .volume_create(
            step_2_volume_id,
            VolumeConstructionRequest::Volume {
                id: *step_2_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(step_1_vcr.clone())),
            },
        )
        .await
        .unwrap();

    // Soft-delete the step 1 volume

    let cr = datastore.soft_delete_volume(step_1_volume_id).await.unwrap();

    // Assert that no resources are returned for clean-up

    assert!(datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Soft-delete the step 2 volume

    let cr = datastore.soft_delete_volume(step_2_volume_id).await.unwrap();

    // Assert that the read-only regions _are_ returned for clean-up

    assert!(!datastore.regions_to_delete(&cr).await.unwrap().is_empty());
    assert!(datastore.snapshots_to_delete(&cr).await.unwrap().is_empty());

    // Pretend that there's a racing call to volume_create that has a volume
    // that uses the step 1 volume as a read-only parent. This call should
    // fail!

    let racing_volume_id = VolumeUuid::new_v4();
    let result = datastore
        .volume_create(
            racing_volume_id,
            VolumeConstructionRequest::Volume {
                id: *racing_volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(step_1_vcr)),
            },
        )
        .await;

    assert!(result.is_err());
}

/// Ensure that regions are not allocated to the same physical sled during
/// arbitrary region allocation
#[nexus_test(extra_sled_agents = 3)]
async fn test_proper_region_sled_redundancy(
    cptestctx: &ControlPlaneTestContext,
) {
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let conn = datastore.pool_connection_for_tests().await.unwrap();

    DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(10)
        .build()
        .await;

    // Assert 4 sleds, all in service and provisionable
    assert_eq!(cptestctx.all_sled_agents().count(), 4);

    let client = &cptestctx.external_client;
    let sleds_url = "/v1/system/hardware/sleds";
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), 4);
    for sled in sleds_found {
        assert_eq!(sled.state, views::SledState::Active);
        assert!(matches!(
            sled.policy,
            views::SledPolicy::InService {
                provision_policy: views::SledProvisionPolicy::Provisionable
            },
        ));
    }

    // Create a volume with three read-only regions, then simulate a replacement
    // that will allocate with a bumped redundancy

    let volume_id = VolumeUuid::new_v4();
    let snapshot_id = Uuid::new_v4();

    for redundancy in 3..=4 {
        let datasets_and_regions = datastore
            .arbitrary_region_allocate(
                &opctx,
                RegionAllocationFor::SnapshotVolume { volume_id, snapshot_id },
                RegionAllocationParameters::FromDiskSource {
                    disk_source: &params::DiskSource::Blank {
                        block_size: params::BlockSize::try_from(512).unwrap(),
                    },
                    size: ByteCount::from_gibibytes_u32(1),
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                redundancy,
            )
            .await
            .unwrap();

        // Ensure distinct sleds for each region

        let mut sled_ids = HashSet::new();

        for (_, region) in &datasets_and_regions {
            let sled_id = {
                let dataset = {
                    use nexus_db_schema::schema::crucible_dataset::dsl;
                    dsl::crucible_dataset
                        .filter(
                            dsl::id.eq(to_db_typed_uuid(region.dataset_id())),
                        )
                        .select(CrucibleDataset::as_select())
                        .get_result_async::<CrucibleDataset>(&*conn)
                        .await
                        .unwrap()
                };

                let zpool = {
                    use nexus_db_schema::schema::zpool::dsl;
                    dsl::zpool
                        .filter(dsl::id.eq(dataset.pool_id))
                        .select(db::model::Zpool::as_select())
                        .get_result_async::<db::model::Zpool>(&*conn)
                        .await
                        .unwrap()
                };

                zpool.sled_id()
            };

            assert!(
                sled_ids.insert(sled_id),
                "region {} shares sled {}",
                region.id(),
                sled_id
            );
        }
    }
}

/// Ensure that volume create won't create a volume with deleted resources
#[nexus_test(extra_sled_agents = 2)]
async fn test_volume_create_wont_use_deleted_region_snapshots(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create one zpool on each of the 3 sleds
    let _disk_test = DiskTestBuilder::new(&cptestctx)
        .on_all_sleds()
        .with_zpool_count(1)
        .build()
        .await;

    create_project_and_pool(client).await;

    let _disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshot =
        create_snapshot(&client, PROJECT_NAME, "disk", "snapshot").await;

    // Manually create the following race:
    //
    // 1) creating a disk from a snapshot performs a checkout (with a checkout
    //    reason = read-only copy) of the snapshot volume to use as a read-only
    //    parent.
    //
    // 2) the snapshot is deleted, which decreases the ref counts of the region
    //    snapshots, which in this case marks them for deletion
    //
    // 3) pass the disk volume to volume_create

    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot.identity.id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("snapshot {:?} should exist", snapshot.identity.id)
        });

    let volume_copy = datastore
        .volume_checkout(
            db_snapshot.volume_id(),
            db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
        )
        .await
        .unwrap();

    let _cr =
        datastore.soft_delete_volume(db_snapshot.volume_id()).await.unwrap();

    let vcr: VolumeConstructionRequest =
        serde_json::from_str(volume_copy.data()).unwrap();

    assert!(datastore.volume_create(VolumeUuid::new_v4(), vcr).await.is_err());
}
