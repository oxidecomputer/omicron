// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that Nexus properly manages and cleans up Crucible resources
//! associated with Volumes

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::db::DataStore;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::internal;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
use omicron_uuid_kinds::UpstairsSessionKind;
use rand::prelude::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use sled_agent_client::types::{CrucibleOpts, VolumeConstructionRequest};
use std::sync::Arc;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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
) -> Disk {
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

    // Attempt disk allocation, which will fail - the presense of the snapshot
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
    let _next_disk: Disk = NexusRequest::new(
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

    let first_disk: Disk = NexusRequest::new(
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

    let second_disk: Disk = NexusRequest::new(
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

    let first_disk: Disk = NexusRequest::new(
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

    let second_disk: Disk = NexusRequest::new(
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

    let layer_1_disk: Disk = NexusRequest::new(
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

    let layer_2_disk: Disk = NexusRequest::new(
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

    let layer_3_disk: Disk = NexusRequest::new(
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

        let mut rng = StdRng::from_entropy();
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

    let _base_disk: Disk = NexusRequest::new(
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
    volume_id: Uuid,
    rop_option: Option<VolumeConstructionRequest>,
) {
    let block_size = 512;

    // Make the SubVolume
    let sub_volume = VolumeConstructionRequest::File {
        id: volume_id,
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
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size,
                sub_volumes,
                read_only_parent: rop,
            })
            .unwrap(),
        ))
        .await
        .unwrap();
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_base(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a volume with a read only parent.
    // The ROP should end up on the t_vid volume.
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
    let t_vid = Uuid::new_v4();
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
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let new_vol = datastore.volume_checkout(t_vid).await.unwrap();
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
    let new_vol = datastore.volume_checkout(t_vid).await.unwrap();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
    let t_vid = Uuid::new_v4();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
    let t_vid = Uuid::new_v4();

    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::File {
                id: volume_id,
                block_size: 512,
                path: "/lol".to_string(),
            })
            .unwrap(),
        ))
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
    let t_vid = Uuid::new_v4();

    // Nothing should be removed, but we also don't return error.
    let removed = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!removed);
}

#[nexus_test]
async fn test_volume_remove_read_only_parent_volume_deleted(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test the removal of a read_only_parent from a deleted volume.
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
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
    let _cr = datastore
        .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
        .await
        .unwrap();

    let t_vid = Uuid::new_v4();
    // Nothing should be removed, but we also don't return error.
    let removed = datastore.volume_remove_rop(volume_id, t_vid).await.unwrap();
    assert!(!removed);
}

#[nexus_test]
async fn test_volume_remove_rop_saga(cptestctx: &ControlPlaneTestContext) {
    // Test the saga for removal of a volume with a read only parent.
    // We create a volume with a read only parent, then call the saga on it.
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
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

    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
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
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let volume_id = Uuid::new_v4();

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
    let nexus = &cptestctx.server.apictx().nexus;
    let volume_id = Uuid::new_v4();
    let datastore = nexus.datastore();

    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::File {
                id: volume_id,
                block_size: 512,
                path: "/lol".to_string(),
            })
            .unwrap(),
        ))
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
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
    let _cr = datastore
        .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
        .await
        .unwrap();

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

    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
    let block_size = 512;

    // Create a sub_vol with generation 1.
    let subvol = create_region(block_size, 1, Uuid::new_v4());
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol],
        read_only_parent: None,
    };

    // Take our VCR from above and insert into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // The first time back, we get 1 but internally the generation number goes
    // to 2.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(1)]);

    // Request again, we should get 2 now.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(2)]);
}

#[nexus_test]
async fn test_volume_checkout_updates_nothing(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will do nothing for a volume that does
    // not contain a sub_volume with a generation field.
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
    let block_size = 512;

    // Build our sub_vol and VCR from parts.
    let subvol = VolumeConstructionRequest::File {
        id: volume_id,
        block_size,
        path: "/lol".to_string(),
    };
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol],
        read_only_parent: None,
    };

    // Take our VCR from above and insert into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // Verify nothing happens to our non generation number volume.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![None]);
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![None]);
}

#[nexus_test]
async fn test_volume_checkout_updates_multiple_gen(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout will update the generation number in the
    // database when the volume type is Volume with multiple sub_volumes of
    // type Region.
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
    let block_size = 512;

    // Create two regions.
    let subvol_one = create_region(block_size, 3, Uuid::new_v4());
    let subvol_two = create_region(block_size, 8, Uuid::new_v4());

    // Make the volume with our two regions as sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol_one, subvol_two],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(3), Some(8)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(4), Some(9)]);

    // Request one more, because why not.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
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
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![None, Some(7), Some(9)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
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
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // The first time back, we get our original values, but internally the
    // generation number goes up.
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(7), None, Some(9)]);

    // Request again, we should see the incremented values now..
    let new_vol = datastore.volume_checkout(volume_id).await.unwrap();
    volume_match_gen(new_vol, vec![Some(8), None, Some(10)]);
}

#[nexus_test]
async fn test_volume_checkout_randomize_ids_only_read_only(
    cptestctx: &ControlPlaneTestContext,
) {
    // Verify that a volume_checkout_randomize_ids will not work for
    // non-read-only Regions
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    let volume_id = Uuid::new_v4();
    let block_size = 512;

    // Create three sub_vols.
    let subvol_one = create_region(block_size, 7, Uuid::new_v4());
    let subvol_two = create_region(block_size, 7, Uuid::new_v4());
    let subvol_three = create_region(block_size, 7, Uuid::new_v4());

    // Make the volume with our three sub_volumes
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: volume_id,
        block_size,
        sub_volumes: vec![subvol_one, subvol_two, subvol_three],
        read_only_parent: None,
    };

    // Insert the volume into the database.
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&volume_construction_request).unwrap(),
        ))
        .await
        .unwrap();

    // volume_checkout_randomize_ids should fail
    let r = datastore.volume_checkout_randomize_ids(volume_id).await;
    assert!(r.is_err());
}

/// Test that the Crucible agent's port reuse does not confuse
/// `decrease_crucible_resource_count_and_soft_delete_volume`, due to the
/// `[ipv6]:port` targets being reused.
#[nexus_test]
async fn test_keep_your_targets_straight(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    // Four zpools, one dataset each
    let mut disk_test = DiskTest::new(&cptestctx).await;
    disk_test
        .add_zpool_with_dataset(&cptestctx, DiskTest::DEFAULT_ZPOOL_SIZE_GIB)
        .await;

    // This bug occurs when region_snapshot records share a snapshot_addr, so
    // insert those here manually.

    // (dataset_id, region_id, snapshot_id, snapshot_addr)
    let region_snapshots = vec![
        // first snapshot-create
        (
            disk_test.zpools[0].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:101:7]:19016"),
        ),
        (
            disk_test.zpools[1].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:102:7]:19016"),
        ),
        (
            disk_test.zpools[2].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:103:7]:19016"),
        ),
        // second snapshot-create
        (
            disk_test.zpools[0].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:101:7]:19016"), // duplicate!
        ),
        (
            disk_test.zpools[3].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:104:7]:19016"),
        ),
        (
            disk_test.zpools[2].datasets[0].id,
            Uuid::new_v4(),
            Uuid::new_v4(),
            String::from("[fd00:1122:3344:103:7]:19017"),
        ),
    ];

    // First, three `region_snapshot` records created in the snapshot-create
    // saga, which are then used to make snapshot's volume construction request

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, snapshot_addr) =
            &region_snapshots[i];
        datastore
            .region_snapshot_create(nexus_db_model::RegionSnapshot {
                dataset_id: *dataset_id,
                region_id: *region_id,
                snapshot_id: *snapshot_id,
                snapshot_addr: snapshot_addr.clone(),
                volume_references: 0,
                deleting: false,
            })
            .await
            .unwrap();
    }

    let volume_id = Uuid::new_v4();
    let volume = datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::Volume {
                id: volume_id,
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
                                region_snapshots[0].3.clone(),
                                region_snapshots[1].3.clone(),
                                region_snapshots[2].3.clone(),
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
            })
            .unwrap(),
        ))
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

    // Also validate the volume's region_snapshots got incremented by
    // volume_create

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];
        let region_snapshot = datastore
            .region_snapshot_get(dataset_id, region_id, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(region_snapshot.volume_references, 1);
        assert_eq!(region_snapshot.deleting, false);
    }

    // Soft delete the volume, and validate that only three region_snapshot
    // records are returned.

    let cr = datastore
        .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
        .await
        .unwrap();

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];
        let region_snapshot = datastore
            .region_snapshot_get(dataset_id, region_id, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(region_snapshot.volume_references, 0);
        assert_eq!(region_snapshot.deleting, true);
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
                dataset_id: *dataset_id,
                region_id: *region_id,
                snapshot_id: *snapshot_id,
                snapshot_addr: snapshot_addr.clone(),
                volume_references: 0,
                deleting: false,
            })
            .await
            .unwrap();
    }

    let volume_id = Uuid::new_v4();
    let volume = datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::Volume {
                id: volume_id,
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
                                region_snapshots[3].3.clone(),
                                region_snapshots[4].3.clone(),
                                region_snapshots[5].3.clone(),
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
            })
            .unwrap(),
        ))
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

    // Also validate only the volume's region_snapshots got incremented by
    // volume_create.

    for i in 0..3 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];
        let region_snapshot = datastore
            .region_snapshot_get(dataset_id, region_id, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(region_snapshot.volume_references, 0);
        assert_eq!(region_snapshot.deleting, true);
    }
    for i in 3..6 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];
        let region_snapshot = datastore
            .region_snapshot_get(dataset_id, region_id, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(region_snapshot.volume_references, 1);
        assert_eq!(region_snapshot.deleting, false);
    }

    // Soft delete the volume, and validate that only three region_snapshot
    // records are returned.

    let cr = datastore
        .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
        .await
        .unwrap();

    // Make sure every region_snapshot is now 0, and deleting

    for i in 0..6 {
        let (dataset_id, region_id, snapshot_id, _) = region_snapshots[i];
        let region_snapshot = datastore
            .region_snapshot_get(dataset_id, region_id, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(region_snapshot.volume_references, 0);
        assert_eq!(region_snapshot.deleting, true);
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
    let zpool = &disk_test.zpools[2];
    let dataset = &zpool.datasets[0];
    disk_test
        .sled_agent
        .get_crucible_dataset(zpool.id, dataset.id)
        .await
        .set_region_creation_error(true)
        .await;

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

    let _disk: Disk = object_create(client, &disks_url, &base_disk).await;

    // Set the third agent to fail creating the region for the snapshot
    let zpool = &disk_test.zpools[2];
    let dataset = &zpool.datasets[0];
    disk_test
        .sled_agent
        .get_crucible_dataset(zpool.id, dataset.id)
        .await
        .set_region_creation_error(true)
        .await;

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
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let volume_id = Uuid::new_v4();
    datastore
        .volume_create(nexus_db_model::Volume::new(
            volume_id,
            serde_json::to_string(&VolumeConstructionRequest::File {
                id: volume_id,
                block_size: 512,
                path: "/lol".to_string(),
            })
            .unwrap(),
        ))
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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let session_id: TypedUuid<UpstairsSessionKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();
    let region_id: TypedUuid<DownstairsRegionKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let repair_id: TypedUuid<UpstairsRepairKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let downstairs_id: TypedUuid<DownstairsKind> = TypedUuid::new_v4();

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

    let upstairs_id: TypedUuid<UpstairsKind> = TypedUuid::new_v4();
    let downstairs_id: TypedUuid<DownstairsKind> = TypedUuid::new_v4();

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
