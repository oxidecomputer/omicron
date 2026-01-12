// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests images support in the API

use dropshot::ResultsPage;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::FLEET_ID;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::fixed_data::silo_user::USER_TEST_UNPRIVILEGED;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::ProjectRole;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::{params, views};
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_common::api::external::Disk;
use omicron_common::api::external::{ByteCount, IdentityMetadataCreateParams};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "myproj";

fn get_project_images_url(project_name: &str) -> String {
    format!("/v1/images?project={}", project_name)
}

fn get_image_create(source: params::ImageSource) -> params::ImageCreate {
    params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        source,
    }
}

#[nexus_test]
async fn test_image_create(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    let images_url = get_project_images_url(PROJECT_NAME);

    // Before project exists, image list 404s
    NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &images_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Expected 404");

    // create the project, now we expect an empty list
    let _project = create_project(client, PROJECT_NAME).await;

    let images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 0);

    // Create an image in the project
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    NexusRequest::objects_post(client, &images_url, &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    // one image in the project
    let images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 1);
    assert_eq!(images[0].identity.name, "alpine-edge");
}

#[nexus_test]
async fn test_silo_image_create(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    let silo_images_url = "/v1/images";

    // Expect no images in the silo
    let images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 0);

    // Create an image in the project
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    // Create image
    NexusRequest::objects_post(client, &silo_images_url, &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    let images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 1);
    assert_eq!(images[0].identity.name, "alpine-edge");
}

#[nexus_test]
async fn test_make_disk_from_image(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post both disk and image to
    create_project(client, PROJECT_NAME).await;

    // Create an image in the project
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let images_url = get_project_images_url(PROJECT_NAME);

    let alpine_image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_backend: params::DiskBackend::Distributed {
            disk_source: params::DiskSource::Image {
                image_id: alpine_image.identity.id,
            },
        },
        size: ByteCount::from_gibibytes_u32(1),
    };

    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    NexusRequest::objects_post(client, &disks_url, &new_disk)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

#[nexus_test]
async fn test_make_disk_from_other_project_image_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    create_project(client, PROJECT_NAME).await;
    let another_project = create_project(client, "another-proj").await;

    let images_url = get_project_images_url(PROJECT_NAME);
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "stolen-disk".parse().unwrap(),
            description: String::from("yoink"),
        },
        disk_backend: params::DiskBackend::Distributed {
            disk_source: params::DiskSource::Image {
                image_id: image.identity.id,
            },
        },
        size: ByteCount::from_gibibytes_u32(1),
    };
    let disks_url =
        format!("/v1/disks?project={}", another_project.identity.name);
    let error = NexusRequest::expect_failure_with_body(
        client,
        StatusCode::BAD_REQUEST,
        Method::POST,
        &disks_url,
        &new_disk,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("expected 400")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(error.message, "image does not belong to this project");
}

#[nexus_test]
async fn test_make_disk_from_image_too_small(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post both disk and image to
    create_project(client, PROJECT_NAME).await;

    // Create an image in the project
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let images_url = get_project_images_url(PROJECT_NAME);

    let alpine_image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk".parse().unwrap(),
            description: String::from("sells rainsticks"),
        },
        disk_backend: params::DiskBackend::Distributed {
            disk_source: params::DiskSource::Image {
                image_id: alpine_image.identity.id,
            },
        },
        // Nexus defines YouCanBootAnythingAsLongAsItsAlpine size as 100M
        size: ByteCount::from(90 * 1024 * 1024),
    };

    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&new_disk))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "disk size {} must be greater than or equal to image size {}",
            94371840_u32, 104857600_u32,
        )
    );
}

#[nexus_test]
async fn test_image_promotion(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    let silo_images_url = "/v1/images";
    let images_url = get_project_images_url(PROJECT_NAME);

    create_project(client, PROJECT_NAME).await;

    let images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 0);

    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    NexusRequest::objects_post(client, &images_url, &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 1);
    assert_eq!(project_images[0].identity.name, "alpine-edge");

    let image_id = project_images[0].identity.id;
    let project_image_url = format!("/v1/images/{}", image_id);

    let project_image = NexusRequest::object_get(client, &project_image_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    assert_eq!(project_image.identity.id, image_id);

    // promote the image to the silo
    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    let silo_images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(silo_images.len(), 1);
    assert_eq!(silo_images[0].identity.name, "alpine-edge");

    // Ensure there are no more project images
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 0);

    let silo_image_url = format!("/v1/images/{}", image_id);
    let silo_image = NexusRequest::object_get(client, &silo_image_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    assert_eq!(silo_image.identity.id, image_id);

    // Create another project image with the same name
    NexusRequest::objects_post(client, &images_url, &image_create_params)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<views::Image>()
        .await;

    // Ensure project image was created
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 1);
    assert_eq!(project_images[0].identity.name, "alpine-edge");

    let image_id = project_images[0].identity.id;
    let promote_url = format!("/v1/images/{}/promote", image_id);

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
}

#[nexus_test]
async fn test_image_from_other_project_snapshot_fails(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    create_project(client, PROJECT_NAME).await;
    let images_url = get_project_images_url(PROJECT_NAME);
    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    // Create an image
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );
    let image: views::Image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;
    // Create a disk from this image
    let disk_create_params = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "disk".parse().unwrap(),
            description: "meow".into(),
        },
        disk_backend: params::DiskBackend::Distributed {
            disk_source: params::DiskSource::Image {
                image_id: image.identity.id,
            },
        },
        size: ByteCount::from_gibibytes_u32(1),
    };
    let disk: Disk =
        NexusRequest::objects_post(client, &disks_url, &disk_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    // Create a snapshot from this disk
    let snapshot_create_params = params::SnapshotCreate {
        identity: IdentityMetadataCreateParams {
            name: "snapshot".parse().unwrap(),
            description: "meow".into(),
        },
        disk: disk.identity.id.into(),
    };
    let snapshot: views::Snapshot = NexusRequest::objects_post(
        client,
        &snapshots_url,
        &snapshot_create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Create a second project and ensure image creation fails
    let another_project = create_project(client, "another-proj").await;
    let images_url =
        get_project_images_url(another_project.identity.name.as_str());
    let image_create_params = get_image_create(params::ImageSource::Snapshot {
        id: snapshot.identity.id,
    });
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &images_url)
            .body(Some(&image_create_params))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(error.message, "snapshot does not belong to this project");
}

#[nexus_test]
async fn test_image_deletion_permissions(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // Create a project

    create_project(client, PROJECT_NAME).await;

    // Grant the unprivileged user viewer on the silo and admin on that project

    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.id());
    grant_iam(
        client,
        &silo_url,
        SiloRole::Viewer,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let project_url = format!("/v1/projects/{}", PROJECT_NAME);
    grant_iam(
        client,
        &project_url,
        ProjectRole::Admin,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create an image in the default silo using the privileged user

    let silo_images_url = "/v1/images";
    let images_url = get_project_images_url(PROJECT_NAME);

    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let image_id = image.identity.id;

    // promote the image to the silo

    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    let silo_images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(silo_images.len(), 1);
    assert_eq!(silo_images[0].identity.name, "alpine-edge");

    // the unprivileged user should not be able to delete that image

    let image_url = format!("/v1/images/{}", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &image_url)
            .expect_status(Some(http::StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("should not be able to delete silo image as unpriv user!");

    // Demote that image

    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // now the unpriviledged user should be able to delete that image

    let image_url = format!("/v1/images/{}", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &image_url)
            .expect_status(Some(http::StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("should be able to delete project image as unpriv user!");
}

/// Test that creating and deleting images correctly updates virtual
/// provisioning at project, silo, and fleet levels.
///
/// This test creates images from snapshots of blank disks. Since disks,
/// snapshots, and images all contribute to virtual_disk_bytes_provisioned,
/// we track the expected total at each step.
#[nexus_test]
async fn test_image_virtual_provisioning_collection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    DiskTest::new(&cptestctx).await;

    let project = create_project(client, PROJECT_NAME).await;
    let project_id = project.identity.id;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Use 1 GiB for all resources (disk, snapshot, image)
    let one_gib = ByteCount::from_gibibytes_u32(1);

    // Helper to get provisioning for a collection
    async fn get_provisioned_bytes(
        datastore: &nexus_db_queries::db::DataStore,
        opctx: &OpContext,
        id: uuid::Uuid,
    ) -> u64 {
        datastore
            .virtual_provisioning_collection_get(opctx, id)
            .await
            .unwrap()
            .virtual_disk_bytes_provisioned
            .to_bytes()
    }

    // Initially, all collections should have 0 virtual_disk_bytes_provisioned
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, project_id).await, 0);
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        0
    );
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await, 0);

    // =========================================================================
    // Create resources for project-scoped image: disk1 -> snapshot1 -> image1
    // =========================================================================

    // Create disk1 (1 GiB blank disk)
    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    let _disk1 = NexusRequest::objects_post(
        client,
        &disks_url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "disk1".parse().unwrap(),
                description: String::from("first test disk"),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
            size: one_gib,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<Disk>()
    .await;

    // After disk1: project/silo/fleet all have 1 GiB
    let after_disk1 = one_gib.to_bytes();
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_disk1
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_disk1
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_disk1
    );

    // Create snapshot1 from disk1
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);
    let snapshot1: views::Snapshot = NexusRequest::objects_post(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot1".parse().unwrap(),
                description: String::from("first test snapshot"),
            },
            disk: "disk1".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    // After snapshot1: disk1 + snapshot1 = 2 GiB each
    let after_snapshot1 = one_gib.to_bytes() * 2;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_snapshot1
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_snapshot1
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_snapshot1
    );

    // Create project-scoped image from snapshot1
    let project_images_url = get_project_images_url(PROJECT_NAME);
    let project_image = NexusRequest::objects_post(
        client,
        &project_images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: "project-image".parse().unwrap(),
                description: String::from("a project-scoped image"),
            },
            os: "testOS".to_string(),
            version: "1.0".to_string(),
            source: params::ImageSource::Snapshot { id: snapshot1.identity.id },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // After project_image: disk1 + snapshot1 + image1 = 3 GiB each
    let after_project_image = one_gib.to_bytes() * 3;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_project_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_project_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_project_image
    );

    // =========================================================================
    // Create resources for silo-scoped image: disk2 -> snapshot2 -> image2
    // =========================================================================

    // Create disk2 (another 1 GiB blank disk)
    let _disk2 = NexusRequest::objects_post(
        client,
        &disks_url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "disk2".parse().unwrap(),
                description: String::from("second test disk"),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
            size: one_gib,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<Disk>()
    .await;

    // After disk2: previous + disk2 = 4 GiB
    let after_disk2 = one_gib.to_bytes() * 4;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_disk2
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_disk2
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_disk2
    );

    // Create snapshot2 from disk2
    let snapshot2: views::Snapshot = NexusRequest::objects_post(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot2".parse().unwrap(),
                description: String::from("second test snapshot"),
            },
            disk: "disk2".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    // After snapshot2: previous + snapshot2 = 5 GiB
    let after_snapshot2 = one_gib.to_bytes() * 5;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_snapshot2
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_snapshot2
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_snapshot2
    );

    // Create silo-scoped image from snapshot2
    // Note: silo images don't add to project provisioning, only silo and fleet
    let silo_images_url = "/v1/images";
    let silo_image = NexusRequest::objects_post(
        client,
        &silo_images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: "silo-image".parse().unwrap(),
                description: String::from("a silo-scoped image"),
            },
            os: "testOS".to_string(),
            version: "1.0".to_string(),
            source: params::ImageSource::Snapshot { id: snapshot2.identity.id },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // After silo_image:
    // - Project still has 5 GiB (disk1 + snapshot1 + image1 + disk2 + snapshot2)
    // - Silo/Fleet have 6 GiB (above + silo_image)
    let project_after_silo_image = one_gib.to_bytes() * 5;
    let silo_after_silo_image = one_gib.to_bytes() * 6;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        project_after_silo_image,
        "Project should not include silo-scoped image"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        silo_after_silo_image,
        "Silo should include silo-scoped image"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        silo_after_silo_image,
        "Fleet should include silo-scoped image"
    );

    // =========================================================================
    // Delete images and verify provisioning decreases correctly
    // =========================================================================

    // Delete the project-scoped image
    let project_image_url = format!("/v1/images/{}", project_image.identity.id);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &project_image_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to delete project image");

    // After deleting project_image:
    // - Project: disk1 + snapshot1 + disk2 + snapshot2 = 4 GiB
    // - Silo/Fleet: above + silo_image = 5 GiB
    let project_after_delete_project_image = one_gib.to_bytes() * 4;
    let silo_after_delete_project_image = one_gib.to_bytes() * 5;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        project_after_delete_project_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        silo_after_delete_project_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        silo_after_delete_project_image
    );

    // Delete the silo-scoped image
    let silo_image_url = format!("/v1/images/{}", silo_image.identity.id);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &silo_image_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to delete silo image");

    // After deleting silo_image:
    // - All collections: disk1 + snapshot1 + disk2 + snapshot2 = 4 GiB
    let after_delete_silo_image = one_gib.to_bytes() * 4;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_delete_silo_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_delete_silo_image
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_delete_silo_image
    );

    // =========================================================================
    // Clean up disks and snapshots
    // =========================================================================

    // Delete snapshots first (they depend on disks conceptually, but can be
    // deleted independently)
    let snapshot1_url =
        format!("/v1/snapshots/snapshot1?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &snapshot1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot1");

    let snapshot2_url =
        format!("/v1/snapshots/snapshot2?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &snapshot2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot2");

    // After deleting snapshots: disk1 + disk2 = 2 GiB
    let after_delete_snapshots = one_gib.to_bytes() * 2;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_delete_snapshots
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_delete_snapshots
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_delete_snapshots
    );

    // Delete disks
    let disk1_url = format!("/v1/disks/disk1?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &disk1_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk1");

    let disk2_url = format!("/v1/disks/disk2?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &disk2_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk2");

    // After deleting disks: all collections should be 0
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, project_id).await, 0);
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        0
    );
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await, 0);
}

/// Test that promoting and demoting images correctly updates virtual
/// provisioning accounting.
///
/// When a project image is promoted to a silo image:
/// - Project accounting should decrease by the image size
/// - Silo and Fleet accounting should stay the same
///
/// When a silo image is demoted to a project image:
/// - Project accounting should increase by the image size
/// - Silo and Fleet accounting should stay the same
#[nexus_test]
async fn test_image_promote_demote_virtual_provisioning(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    DiskTest::new(&cptestctx).await;

    let project = create_project(client, PROJECT_NAME).await;
    let project_id = project.identity.id;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let one_gib = ByteCount::from_gibibytes_u32(1);

    // Helper to get provisioning for a collection
    async fn get_provisioned_bytes(
        datastore: &nexus_db_queries::db::DataStore,
        opctx: &OpContext,
        id: uuid::Uuid,
    ) -> u64 {
        datastore
            .virtual_provisioning_collection_get(opctx, id)
            .await
            .unwrap()
            .virtual_disk_bytes_provisioned
            .to_bytes()
    }

    // =========================================================================
    // Create a project image: disk -> snapshot -> image
    // =========================================================================

    // Create disk
    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    let _disk = NexusRequest::objects_post(
        client,
        &disks_url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-disk".parse().unwrap(),
                description: String::from("test disk"),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
            size: one_gib,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<Disk>()
    .await;

    // Create snapshot
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);
    let snapshot: views::Snapshot = NexusRequest::objects_post(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-snapshot".parse().unwrap(),
                description: String::from("test snapshot"),
            },
            disk: "test-disk".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    // Create project image from snapshot
    let project_images_url = get_project_images_url(PROJECT_NAME);
    let image = NexusRequest::objects_post(
        client,
        &project_images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-image".parse().unwrap(),
                description: String::from("test image for promote/demote"),
            },
            os: "testOS".to_string(),
            version: "1.0".to_string(),
            source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    let image_id = image.identity.id;

    // At this point we have disk (1 GiB) + snapshot (1 GiB) + image (1 GiB) = 3 GiB
    // All in the project, so project/silo/fleet all have 3 GiB
    let initial_bytes = one_gib.to_bytes() * 3;
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        initial_bytes,
        "Project should have 3 GiB initially"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        initial_bytes,
        "Silo should have 3 GiB initially"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        initial_bytes,
        "Fleet should have 3 GiB initially"
    );

    // =========================================================================
    // Promote the image from project to silo
    // =========================================================================

    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // After promote:
    // - Project: disk + snapshot = 2 GiB (image moved to silo)
    // - Silo/Fleet: disk + snapshot + image = 3 GiB (unchanged)
    let project_after_promote = one_gib.to_bytes() * 2;
    let silo_after_promote = one_gib.to_bytes() * 3;

    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        project_after_promote,
        "Project should decrease by image size after promote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        silo_after_promote,
        "Silo should be unchanged after promote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        silo_after_promote,
        "Fleet should be unchanged after promote"
    );

    // =========================================================================
    // Demote the image back from silo to project
    // =========================================================================

    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // After demote:
    // - Project: disk + snapshot + image = 3 GiB (back to original)
    // - Silo/Fleet: disk + snapshot + image = 3 GiB (unchanged)
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        initial_bytes,
        "Project should increase by image size after demote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        initial_bytes,
        "Silo should be unchanged after demote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        initial_bytes,
        "Fleet should be unchanged after demote"
    );

    // =========================================================================
    // Clean up
    // =========================================================================

    // Delete image (it's a project image after demote, no project param needed)
    let image_url = format!("/v1/images/{}", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &image_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to delete image");

    // Delete snapshot
    let snapshot_url =
        format!("/v1/snapshots/test-snapshot?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    // Delete disk
    let disk_url = format!("/v1/disks/test-disk?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Verify everything is cleaned up
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, project_id).await, 0);
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        0
    );
    assert_eq!(get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await, 0);
}

/// Test that attempting to promote an already-promoted image (silo image)
/// returns an error and doesn't corrupt accounting.
///
/// Similarly, test that demoting a project image returns an error.
#[nexus_test]
async fn test_image_double_promote_demote_fails_safely(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    DiskTest::new(&cptestctx).await;

    let project = create_project(client, PROJECT_NAME).await;
    let project_id = project.identity.id;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let one_gib = ByteCount::from_gibibytes_u32(1);

    async fn get_provisioned_bytes(
        datastore: &nexus_db_queries::db::DataStore,
        opctx: &OpContext,
        id: uuid::Uuid,
    ) -> u64 {
        datastore
            .virtual_provisioning_collection_get(opctx, id)
            .await
            .unwrap()
            .virtual_disk_bytes_provisioned
            .to_bytes()
    }

    // Create disk -> snapshot -> project image
    let disks_url = format!("/v1/disks?project={}", PROJECT_NAME);
    let _disk = NexusRequest::objects_post(
        client,
        &disks_url,
        &params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-disk".parse().unwrap(),
                description: String::from("test disk"),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
            size: one_gib,
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<Disk>()
    .await;

    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);
    let snapshot: views::Snapshot = NexusRequest::objects_post(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-snapshot".parse().unwrap(),
                description: String::from("test snapshot"),
            },
            disk: "test-disk".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    let project_images_url = get_project_images_url(PROJECT_NAME);
    let image = NexusRequest::objects_post(
        client,
        &project_images_url,
        &params::ImageCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-image".parse().unwrap(),
                description: String::from("test image"),
            },
            os: "testOS".to_string(),
            version: "1.0".to_string(),
            source: params::ImageSource::Snapshot { id: snapshot.identity.id },
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    let image_id = image.identity.id;

    // =========================================================================
    // Test: demoting a project image should fail
    // =========================================================================

    let initial_project =
        get_provisioned_bytes(&datastore, &opctx, project_id).await;
    let initial_silo =
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await;
    let initial_fleet =
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await;

    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("demoting a project image should fail with 400");

    // Verify accounting is unchanged after failed demote
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        initial_project,
        "Project accounting should be unchanged after failed demote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        initial_silo,
        "Silo accounting should be unchanged after failed demote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        initial_fleet,
        "Fleet accounting should be unchanged after failed demote"
    );

    // =========================================================================
    // Promote the image, then test double-promote
    // =========================================================================

    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    let after_promote_project =
        get_provisioned_bytes(&datastore, &opctx, project_id).await;
    let after_promote_silo =
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await;
    let after_promote_fleet =
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await;

    // Trying to promote again should fail
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("promoting a silo image should fail with 400");

    // Verify accounting is unchanged after failed double-promote
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, project_id).await,
        after_promote_project,
        "Project accounting should be unchanged after failed double-promote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, DEFAULT_SILO_ID).await,
        after_promote_silo,
        "Silo accounting should be unchanged after failed double-promote"
    );
    assert_eq!(
        get_provisioned_bytes(&datastore, &opctx, *FLEET_ID).await,
        after_promote_fleet,
        "Fleet accounting should be unchanged after failed double-promote"
    );

    // =========================================================================
    // Clean up
    // =========================================================================

    // Delete image (now a silo image)
    let image_url = format!("/v1/images/{}", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &image_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to delete image");

    let snapshot_url =
        format!("/v1/snapshots/test-snapshot?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &snapshot_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete snapshot");

    let disk_url = format!("/v1/disks/test-disk?project={}", PROJECT_NAME);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");
}
