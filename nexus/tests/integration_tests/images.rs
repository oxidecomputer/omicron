// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests images support in the API

use dropshot::ResultsPage;
use http::StatusCode;
use http::method::Method;
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
        disk_source: params::DiskSource::Image {
            image_id: alpine_image.identity.id,
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
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
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
        disk_source: params::DiskSource::Image {
            image_id: alpine_image.identity.id,
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
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
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

#[nexus_test]
async fn test_limited_collaborator_can_promote_demote_images(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // Create a project
    create_project(client, PROJECT_NAME).await;

    // Grant the unprivileged user limited-collaborator on the silo
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.id());
    grant_iam(
        client,
        &silo_url,
        SiloRole::LimitedCollaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let images_url = get_project_images_url(PROJECT_NAME);
    let silo_images_url = "/v1/images";

    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let image_id = image.identity.id;

    // Verify the image is in the project
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 1);
    assert_eq!(project_images[0].identity.id, image_id);

    // Promote the image to the silo as limited-collaborator
    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Verify the image is now a silo image
    let silo_images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(silo_images.len(), 1);
    assert_eq!(silo_images[0].identity.id, image_id);

    // Verify no project images remain
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 0);

    // Demote the image back to the project as limited-collaborator
    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Verify the image is back in the project
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 1);
    assert_eq!(project_images[0].identity.id, image_id);

    // Verify no silo images remain
    let silo_images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(silo_images.len(), 0);
}

#[nexus_test]
async fn test_viewer_cannot_promote_demote_images(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // Create a project
    create_project(client, PROJECT_NAME).await;

    // Grant the unprivileged user viewer on the silo
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.id());
    grant_iam(
        client,
        &silo_url,
        SiloRole::Viewer,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let images_url = get_project_images_url(PROJECT_NAME);

    // Create a project image as privileged user
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let image_id = image.identity.id;

    // Attempt to promote the image as viewer - should fail
    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("expected viewer to be blocked from promoting image");

    // Promote the image as privileged user
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Attempt to demote the image as viewer - should fail
    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("expected viewer to be blocked from demoting image");
}

#[nexus_test]
async fn test_project_collaborator_cannot_promote_demote_images(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // Create a project
    create_project(client, PROJECT_NAME).await;

    // Grant the unprivileged user collaborator on the project only (no silo role)
    let project_url = format!("/v1/projects/{}", PROJECT_NAME);
    grant_iam(
        client,
        &project_url,
        ProjectRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let images_url = get_project_images_url(PROJECT_NAME);

    // Create a project image as the unprivileged user (project collaborator can do this)
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let image_id = image.identity.id;

    // Attempt to promote the image as project collaborator - should fail
    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("expected project collaborator to be blocked from promoting image");

    // Promote the image as privileged user (who has silo role)
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Attempt to demote the image as project collaborator - should fail
    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("expected project collaborator to be blocked from demoting image");
}

#[nexus_test]
async fn test_silo_collaborator_can_promote_demote_images(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // Create a project
    create_project(client, PROJECT_NAME).await;

    // Grant the unprivileged user collaborator on the silo
    let silo_url = format!("/v1/system/silos/{}", DEFAULT_SILO.id());
    grant_iam(
        client,
        &silo_url,
        SiloRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    let images_url = get_project_images_url(PROJECT_NAME);
    let silo_images_url = "/v1/images";

    // Create a project image as the unprivileged user (silo collaborator can do this)
    let image_create_params = get_image_create(
        params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    );

    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::UnprivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    let image_id = image.identity.id;

    // Promote the image to the silo as collaborator
    let promote_url = format!("/v1/images/{}/promote", image_id);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &promote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Verify the image is now a silo image
    let silo_images = NexusRequest::object_get(client, &silo_images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(silo_images.len(), 1);
    assert_eq!(silo_images[0].identity.id, image_id);

    // Demote the image back to the project as collaborator
    let demote_url =
        format!("/v1/images/{}/demote?project={}", image_id, PROJECT_NAME);
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::POST, &demote_url)
            .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute_and_parse_unwrap::<views::Image>()
    .await;

    // Verify the image is back in the project
    let project_images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(project_images.len(), 1);
    assert_eq!(project_images[0].identity.id, image_id);
}
