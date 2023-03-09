// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests images support in the API

use dropshot::ResultsPage;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils_macros::nexus_test;

use omicron_common::api::external::{ByteCount, IdentityMetadataCreateParams};
use omicron_nexus::external_api::{params, views};

use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const ORG_NAME: &str = "myorg";
const PROJECT_NAME: &str = "myproj";

const ORG_NAME_2: &str = "myorg2";
const PROJECT_NAME_2: &str = "myproj2";

fn get_images_url(org_name: &str, project_name: &str) -> String {
    format!("/v1/images?organization={}&project={}", org_name, project_name)
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
        block_size: params::BlockSize::try_from(512).unwrap(),
        source,
    }
}

#[nexus_test]
async fn test_image_create(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

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

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
    create_organization(client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let images = NexusRequest::object_get(client, &images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;

    assert_eq!(images.len(), 0);

    // Create an image in the project
    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/image.raw").to_string(),
    });

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

    // create another project, which is empty until we promote the image to global
    create_organization(client, ORG_NAME_2).await;
    create_project(client, ORG_NAME_2, PROJECT_NAME_2).await;

    let project_2_images_url = get_images_url(ORG_NAME_2, PROJECT_NAME_2);

    let images = NexusRequest::object_get(client, &project_2_images_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<ResultsPage<views::Image>>()
        .await
        .items;
    assert_eq!(images.len(), 0);

    // TODO: once we reimplement promote, promote image to global and check that
    // it shows up in the other project
}

#[nexus_test]
async fn test_image_create_url_404(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(status_code(404)),
    );

    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/image.raw").to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
    assert_eq!(
        error.message,
        format!("unsupported value for \"url\": querying url returned: 404 Not Found")
    );
}

#[nexus_test]
async fn test_image_create_bad_url(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let image_create_params = get_image_create(params::ImageSource::Url {
        url: "not_a_url".to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
    assert_eq!(
        error.message,
        format!("unsupported value for \"url\": error querying url: builder error: relative URL without a base")
    );
}

#[nexus_test]
async fn test_image_create_bad_content_length(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header("Content-Length", "bad"),
            ),
    );

    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/image.raw").to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
    assert_eq!(
        error.message,
        format!("unsupported value for \"url\": content length invalid: invalid digit found in string")
    );
}

#[nexus_test]
async fn test_image_create_bad_image_size(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(status_code(200).append_header(
                "Content-Length",
                format!("{}", 4096 * 1000 + 100),
            )),
    );

    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/image.raw").to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
    assert_eq!(
        error.message,
        format!("unsupported value for \"size\": total size {} must be divisible by block size {}", 4096*1000 + 100, 512)
    );
}

#[nexus_test]
async fn test_make_disk_from_image(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post both disk and image to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/alpine/edge.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                ),
            ),
    );

    // Create an image in the project
    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/alpine/edge.raw").to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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

    let disks_url =
        format!("/v1/disks?organization={}&project={}", ORG_NAME, PROJECT_NAME);
    NexusRequest::objects_post(client, &disks_url, &new_disk)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
}

#[nexus_test]
async fn test_make_disk_from_image_too_small(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;

    // need a project to post both disk and image to
    create_organization(&client, ORG_NAME).await;
    create_project(client, ORG_NAME, PROJECT_NAME).await;

    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/alpine/edge.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header("Content-Length", "2147483648"),
            ),
    );

    // Create an image in the project
    let image_create_params = get_image_create(params::ImageSource::Url {
        url: server.url("/alpine/edge.raw").to_string(),
    });

    let images_url = get_images_url(ORG_NAME, PROJECT_NAME);

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
        size: ByteCount::from(1073741824),
    };

    let disks_url =
        format!("/v1/disks?organization={}&project={}", ORG_NAME, PROJECT_NAME);
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
            1073741824_u32, 2147483648_u32,
        )
    );
}
