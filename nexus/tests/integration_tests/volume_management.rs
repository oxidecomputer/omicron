// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that Nexus properly manages and cleans up Crucible resources
//! associated with Volumes

use dropshot::test_util::ClientTestContext;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_nexus::db::DataStore;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::views;
use rand::prelude::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use sled_agent_client::types::VolumeConstructionRequest;
use std::sync::Arc;
use uuid::Uuid;

use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};

const ORG_NAME: &str = "test-org";
const PROJECT_NAME: &str = "springfield-squidport-disks";

fn get_project_url() -> String {
    format!("/organizations/{}/projects/{}", ORG_NAME, PROJECT_NAME)
}

fn get_disks_url() -> String {
    format!("{}/disks", get_project_url())
}

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    create_organization(&client, ORG_NAME).await;
    let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
    project.identity.id
}

async fn create_global_image(client: &ClientTestContext) -> views::GlobalImage {
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;

    // Define a global image
    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                ),
            ),
    );

    let image_create_params = params::GlobalImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        distribution: params::Distribution {
            name: "alpine".parse().unwrap(),
            version: "edge".into(),
        },
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    NexusRequest::objects_post(client, "/system/images", &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap()
}

async fn create_base_disk(
    client: &ClientTestContext,
    global_image: &views::GlobalImage,
    disks_url: &String,
    base_disk_name: &Name,
) -> Disk {
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::GlobalImage {
            image_id: global_image.identity.id,
        },
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

    let global_image = create_global_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &global_image, &disks_url, &base_disk_name)
            .await;

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshot still remains
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    let disk_url = format!("{}/{}", disks_url, base_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // The Crucible snapshot still remains
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the snapshot
    let snapshot_url =
        format!("{}/snapshots/{}", get_project_url(), "a-snapshot");
    NexusRequest::object_delete(client, &snapshot_url)
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

    // Define a global image
    let global_image = create_global_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &global_image, &disks_url, &base_disk_name)
            .await;

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshot still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the snapshot
    let snapshot_url =
        format!("{}/snapshots/{}", get_project_url(), "a-snapshot");
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // The Crucible regions still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    let disk_url = format!("{}/{}", disks_url, base_disk_name);
    NexusRequest::object_delete(client, &disk_url)
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

    let global_image = create_global_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &global_image, &disks_url, &base_disk_name)
            .await;

    // Issue snapshot requests
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    for i in 0..4 {
        let snapshot: views::Snapshot = object_create(
            client,
            &snapshots_url,
            &params::SnapshotCreate {
                identity: IdentityMetadataCreateParams {
                    name: format!("a-snapshot-{}", i).parse().unwrap(),
                    description: "a snapshot!".to_string(),
                },
                disk: base_disk_name.clone(),
            },
        )
        .await;

        assert_eq!(snapshot.disk_id, base_disk.identity.id);
        assert_eq!(snapshot.size, base_disk.size);
    }

    // The Crucible regions and snapshots still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    let disk_url = format!("{}/{}", disks_url, base_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Delete the snapshots
    for i in 0..4 {
        // The Crucible snapshots still remain
        assert!(!disk_test.crucible_resources_deleted().await);

        let snapshot_url =
            format!("{}/snapshots/a-snapshot-{}", get_project_url(), i);
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

    let global_image = create_global_image(&client).await;
    // Create a disk from this image
    let base_disk =
        create_base_disk(&client, &global_image, &disks_url, &base_disk_name)
            .await;

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "a-snapshot".parse().unwrap(),
                description: "a snapshot!".to_string(),
            },
            disk: base_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // The Crucible regions and snapshots still remain
    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the disk
    let disk_url = format!("{}/{}", disks_url, base_disk_name);
    NexusRequest::object_delete(client, &disk_url)
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
        disk_source: params::DiskSource::GlobalImage {
            image_id: global_image.identity.id,
        },
        size: disk_size,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&next_disk))
            .expect_status(Some(StatusCode::SERVICE_UNAVAILABLE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Delete the snapshot
    let snapshot_url = format!("{}/snapshots/a-snapshot", get_project_url());
    NexusRequest::object_delete(client, &snapshot_url)
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
    let disk_url = format!("{}/{}", disks_url, next_disk_name);
    NexusRequest::object_delete(client, &disk_url)
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
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;
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
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let first_snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-snapshot".parse().unwrap(),
                description: "first snapshot!".to_string(),
            },
            disk: first_disk_name.clone(),
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
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-snapshot".parse().unwrap(),
                description: "second snapshot!".to_string(),
            },
            disk: second_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(second_snapshot.disk_id, second_disk.identity.id);
    assert_eq!(second_snapshot.size, second_disk.size);

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first disk
    let disk_url = format!("{}/{}", disks_url, first_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second snapshot
    let snapshot_url =
        format!("{}/snapshots/second-snapshot", get_project_url());
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second disk
    let disk_url = format!("{}/{}", disks_url, second_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first snapshot
    let snapshot_url =
        format!("{}/snapshots/first-snapshot", get_project_url());
    NexusRequest::object_delete(client, &snapshot_url)
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
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;
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
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let first_snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-snapshot".parse().unwrap(),
                description: "first snapshot!".to_string(),
            },
            disk: first_disk_name.clone(),
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
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-snapshot".parse().unwrap(),
                description: "second snapshot!".to_string(),
            },
            disk: second_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(second_snapshot.disk_id, second_disk.identity.id);
    assert_eq!(second_snapshot.size, second_disk.size);

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first disk
    let disk_url = format!("{}/{}", disks_url, first_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second disk
    let disk_url = format!("{}/{}", disks_url, second_disk_name);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the second snapshot
    let snapshot_url =
        format!("{}/snapshots/second-snapshot", get_project_url());
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    assert!(!disk_test.crucible_resources_deleted().await);

    // Delete the first snapshot
    let snapshot_url =
        format!("{}/snapshots/first-snapshot", get_project_url());
    NexusRequest::object_delete(client, &snapshot_url)
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
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let layer_1_snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-1-snapshot".parse().unwrap(),
                description: "layer 1 snapshot!".to_string(),
            },
            disk: layer_1_disk_name.clone(),
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
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-2-snapshot".parse().unwrap(),
                description: "layer 2 snapshot!".to_string(),
            },
            disk: layer_2_disk_name.clone(),
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
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "layer-3-snapshot".parse().unwrap(),
                description: "layer 3 snapshot!".to_string(),
            },
            disk: layer_3_disk_name.clone(),
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
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;

    prepare_for_test_multiple_layers_of_snapshots(&client).await;

    // Delete the disks
    for name in ["layer-1-disk", "layer-2-disk", "layer-3-disk"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        let disk_url = format!("{}/{}", get_disks_url(), name);
        NexusRequest::object_delete(client, &disk_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete disk");
    }

    // Delete the snapshots
    for name in ["layer-1-snapshot", "layer-2-snapshot", "layer-3-snapshot"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        let snapshot_url = format!("{}/snapshots/{}", get_project_url(), name);
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
async fn test_multiple_layers_of_snapshots_delete_all_snapshots_first(
    cptestctx: &ControlPlaneTestContext,
) {
    // Test layering read-only snapshots through multiple disks:
    // delete all snapshots, then delete all disks
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;

    prepare_for_test_multiple_layers_of_snapshots(&client).await;

    // Delete the snapshots
    for name in ["layer-1-snapshot", "layer-2-snapshot", "layer-3-snapshot"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        let snapshot_url = format!("{}/snapshots/{}", get_project_url(), name);
        NexusRequest::object_delete(client, &snapshot_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to delete snapshot");
    }

    // Delete the disks
    for name in ["layer-1-disk", "layer-2-disk", "layer-3-disk"] {
        assert!(!disk_test.crucible_resources_deleted().await);

        let disk_url = format!("{}/{}", get_disks_url(), name);
        NexusRequest::object_delete(client, &disk_url)
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
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;

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
                let disk_url = format!("{}/{}", get_disks_url(), name);
                NexusRequest::object_delete(client, &disk_url)
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await
                    .expect("failed to delete disk");
            }

            DeleteObject::Snapshot(name) => {
                let snapshot_url =
                    format!("{}/snapshots/{}", get_project_url(), name);
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let new_vol = datastore.volume_get(volume_id).await.unwrap();
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
    let new_vol = datastore.volume_get(t_vid).await.unwrap();
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
    let new_vol = datastore.volume_get(t_vid).await.unwrap();
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let nexus = &cptestctx.server.apictx.nexus;
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

    let new_vol = datastore.volume_get(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let new_vol = datastore.volume_get(volume_id).await.unwrap();
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
    let nexus = &cptestctx.server.apictx.nexus;
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
    let nexus = &cptestctx.server.apictx.nexus;
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

    let new_vol = datastore.volume_get(volume_id).await.unwrap();
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

// volume_delete saga node idempotency tests

#[nexus_test]
async fn test_volume_hard_delete_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.apictx.nexus;
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
