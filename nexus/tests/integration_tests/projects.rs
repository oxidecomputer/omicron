// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::{
    create_disk, create_organization, create_project, create_vpc,
    object_create, populate_ip_pool, project_get, DiskTest,
};
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::views;
use omicron_nexus::external_api::views::Project;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_projects(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let p1_name = "springfield-squidport";
    let p2_name = "cairo-airport";
    let org_p1_id =
        create_project(&client, &org_name, &p1_name).await.identity.id;

    create_project(&client, &org_name, &p2_name).await;

    let p1_url = format!("/v1/projects/{}?organization={}", p1_name, org_name);
    let project: Project = project_get(&client, &p1_url).await;
    assert_eq!(project.identity.name, p1_name);

    let p2_url = format!("/v1/projects/{}?organization={}", p2_name, org_name);
    let project: Project = project_get(&client, &p2_url).await;
    assert_eq!(project.identity.name, p2_name);

    // Verify the list of Projects.
    let projects_url = format!("/organizations/{}/projects", org_name);
    let projects = NexusRequest::iter_collection_authn::<Project>(
        &client,
        &projects_url,
        "",
        None,
    )
    .await
    .expect("failed to list projects")
    .all_items;
    assert_eq!(projects.len(), 2);
    // alphabetical order for now
    assert_eq!(projects[0].identity.name, p2_name);
    assert_eq!(projects[1].identity.name, p1_name);

    // Create a second organization and make sure we can have two projects with
    // the same name across organizations
    let org2_name = "test-org2";
    create_organization(&client, &org2_name).await;
    let org2_p1_id =
        create_project(&client, &org2_name, &p1_name).await.identity.id;
    assert_ne!(org_p1_id, org2_p1_id);

    // Make sure the list projects results for the new org make sense
    let projects = NexusRequest::iter_collection_authn::<Project>(
        &client,
        &format!("/v1/projects?organization={}", org2_name),
        "",
        None,
    )
    .await
    .expect("failed to list projects")
    .all_items;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].identity.name, p1_name);
}

async fn delete_project_default_subnet(url: &str, client: &ClientTestContext) {
    NexusRequest::object_delete(
        &client,
        &format!("{url}/vpcs/default/subnets/default"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to make request");
}

async fn delete_project_default_vpc(url: &str, client: &ClientTestContext) {
    NexusRequest::object_delete(&client, &format!("{url}/vpcs/default"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
}

async fn delete_project(url: &str, client: &ClientTestContext) {
    NexusRequest::object_delete(&client, url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
}

async fn delete_project_expect_fail(
    url: &str,
    client: &ClientTestContext,
) -> String {
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to make request")
    .parsed_body()
    .expect("Failed to parse body of request");
    error.message
}

#[nexus_test]
async fn test_project_deletion(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    // Project deletion will fail while the subnet & VPC remain.
    create_project(&client, &org_name, &name).await;
    assert_eq!(
        "project to be deleted contains a vpc: default",
        delete_project_expect_fail(&url, &client).await,
    );
    delete_project_default_subnet(&url, &client).await;
    assert_eq!(
        "project to be deleted contains a vpc: default",
        delete_project_expect_fail(&url, &client).await,
    );
    delete_project_default_vpc(&url, &client).await;
    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    populate_ip_pool(&client, "default", None).await;
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    create_project(&client, &org_name, &name).await;
    delete_project_default_subnet(&url, &client).await;
    delete_project_default_vpc(&url, &client).await;

    let _: Instance = object_create(
        client,
        &format!("{url}/instances"),
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-instance".parse().unwrap(),
                description: format!("description"),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: String::from("the_host"),
            user_data: b"none".to_vec(),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            external_ips: vec![],
            disks: vec![],
            start: false,
        },
    )
    .await;

    assert_eq!(
        "project to be deleted contains an instance: my-instance",
        delete_project_expect_fail(&url, &client).await,
    );

    NexusRequest::object_delete(
        client,
        &format!("{url}/instances/my-instance"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_disk(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let _test = DiskTest::new(&cptestctx).await;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    create_project(&client, &org_name, &name).await;
    delete_project_default_subnet(&url, &client).await;
    delete_project_default_vpc(&url, &client).await;
    create_disk(&client, &org_name, &name, "my-disk").await;
    assert_eq!(
        "project to be deleted contains a disk: my-disk",
        delete_project_expect_fail(&url, &client).await,
    );
    NexusRequest::object_delete(&client, &format!("{url}/disks/my-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_image(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    create_project(&client, &org_name, &name).await;
    delete_project_default_subnet(&url, &client).await;
    delete_project_default_vpc(&url, &client).await;

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        block_size: params::BlockSize::try_from(512).unwrap(),
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    };

    // TODO: This test is incomplete, because project-scoped images have not
    // been implemented.
    //
    // When project-scoped images are supported, we should do the following:
    // - Change this POST request, making it succeed
    // - Attempt to delete the project, verify that it fails with the
    // appropriate error ("project to be deleted contains an image").
    // - Delete the image
    // - Delete the project without error.
    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &format!("{url}/images"),
        &image_create_params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

#[nexus_test]
async fn test_project_deletion_with_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _test = DiskTest::new(&cptestctx).await;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    create_project(&client, &org_name, &name).await;
    delete_project_default_subnet(&url, &client).await;
    delete_project_default_vpc(&url, &client).await;
    create_disk(&client, &org_name, &name, "my-disk").await;

    let _: views::Snapshot = object_create(
        client,
        &format!("{url}/snapshots"),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-snapshot".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: "my-disk".parse().unwrap(),
        },
    )
    .await;

    NexusRequest::object_delete(&client, &format!("{url}/disks/my-disk"))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert_eq!(
        "project to be deleted contains a snapshot: my-snapshot",
        delete_project_expect_fail(&url, &client).await,
    );

    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::DELETE,
            &format!("{url}/snapshots/my-snapshot"),
        )
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_vpc(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let org_name = "test-org";
    create_organization(&client, &org_name).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/organizations/{}/projects/{}", org_name, name);

    create_project(&client, &org_name, &name).await;
    delete_project_default_subnet(&url, &client).await;
    delete_project_default_vpc(&url, &client).await;

    let vpc_name = "just-rainsticks";
    create_vpc(&client, org_name, name, vpc_name).await;

    assert_eq!(
        "project to be deleted contains a vpc: just-rainsticks",
        delete_project_expect_fail(&url, &client).await,
    );

    let vpc_url = format!("{url}/vpcs/{vpc_name}");
    let default_subnet_url = format!("{vpc_url}/subnets/default");
    NexusRequest::object_delete(client, &default_subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    NexusRequest::object_delete(client, &vpc_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    delete_project(&url, &client).await;
}
