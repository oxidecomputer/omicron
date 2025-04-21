// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_affinity_group;
use nexus_test_utils::resource_helpers::create_anti_affinity_group;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_vpc;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::project_get;
use nexus_test_utils::resource_helpers::projects_list;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::external_api::views::Project;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use std::str::FromStr;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_projects(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let p1_name = "springfield-squidport";
    let p2_name = "cairo-airport";
    create_project(&client, &p1_name).await;
    create_project(&client, &p2_name).await;

    let p1_url = format!("/v1/projects/{}", p1_name);
    let project: Project = project_get(&client, &p1_url).await;
    assert_eq!(project.identity.name, p1_name);

    let p2_url = format!("/v1/projects/{}", p2_name);
    let project: Project = project_get(&client, &p2_url).await;
    assert_eq!(project.identity.name, p2_name);

    // Verify the list of Projects.
    let projects = projects_list(&client, "/v1/projects", "", None).await;
    assert_eq!(projects.len(), 2);
    // alphabetical order for now
    assert_eq!(projects[0].identity.name, p2_name);
    assert_eq!(projects[1].identity.name, p1_name);

    // TODO: test that we can make a project with the same name in another silo
    // and when we list projects we only get the ones in each silo
}

async fn delete_project_default_subnet(
    project: &str,
    client: &ClientTestContext,
) {
    let subnet_url =
        format!("/v1/vpc-subnets/default?project={}&vpc=default", project);
    NexusRequest::object_delete(&client, &subnet_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to make request");
}

async fn delete_project_default_vpc(project: &str, client: &ClientTestContext) {
    let vpc_url = format!("/v1/vpcs/default?project={}", project);
    NexusRequest::object_delete(&client, &vpc_url)
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

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/v1/projects/{}", name);

    // Project deletion will fail while the subnet & VPC remain.
    create_project(&client, &name).await;
    assert_eq!(
        "project to be deleted contains a vpc: default",
        delete_project_expect_fail(&url, &client).await,
    );
    delete_project_default_subnet(&name, &client).await;
    assert_eq!(
        "project to be deleted contains a vpc: default",
        delete_project_expect_fail(&url, &client).await,
    );
    delete_project_default_vpc(&name, &client).await;
    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;

    let _: Instance = object_create(
        client,
        &format!("/v1/instances?project={}", name),
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-instance".parse().unwrap(),
                description: "description".to_string(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: "the-host".parse().unwrap(),
            user_data: b"none".to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            external_ips: vec![],
            disks: vec![],
            boot_disk: None,
            min_cpu_platform: None,
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
    )
    .await;

    assert_eq!(
        "project to be deleted contains an instance: my-instance",
        delete_project_expect_fail(&url, &client).await,
    );

    NexusRequest::object_delete(
        client,
        &format!("/v1/instances/my-instance?project={}", name),
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

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;
    create_disk(&client, &name, "my-disk").await;
    assert_eq!(
        "project to be deleted contains a disk: my-disk",
        delete_project_expect_fail(&url, &client).await,
    );
    let disk_url = format!("/v1/disks/my-disk?project={}", name);
    NexusRequest::object_delete(&client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_floating_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _test = DiskTest::new(&cptestctx).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/v1/projects/{}", name);

    create_default_ip_pool(&client).await;

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;
    let fip = create_floating_ip(&client, "my-fip", &name, None, None).await;
    assert_eq!(
        "project to be deleted contains a floating ip: my-fip",
        delete_project_expect_fail(&url, &client).await,
    );
    let disk_url =
        super::external_ips::get_floating_ip_by_id_url(&fip.identity.id);
    NexusRequest::object_delete(&client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete floating IP");

    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_image(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
    };

    let images_url = format!("/v1/images?project={}", name);
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    assert_eq!(
        "project to be deleted contains a project image: alpine-edge",
        delete_project_expect_fail(&url, &client).await,
    );

    let image_url = format!("/v1/images/{}", image.identity.id);
    NexusRequest::object_delete(&client, &image_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete image");

    // Expect that trying to GET the image results in a 404
    NexusRequest::new(
        RequestBuilder::new(&client, http::Method::GET, &image_url)
            .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("GET of a deleted image did not return 404");

    delete_project(&url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _test = DiskTest::new(&cptestctx).await;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let project_url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;
    create_disk(&client, &name, "my-disk").await;

    let _: views::Snapshot = object_create(
        client,
        &format!("/v1/snapshots?project={}", name),
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-snapshot".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: Name::from_str("my-disk").unwrap().into(),
        },
    )
    .await;

    let disk_url = format!("/v1/disks/my-disk?project={}", name);
    NexusRequest::object_delete(&client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    assert_eq!(
        "project to be deleted contains a snapshot: my-snapshot",
        delete_project_expect_fail(&project_url, &client).await,
    );

    let snapshot_url = format!("/v1/snapshots/my-snapshot?project={}", name);
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &snapshot_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    delete_project(&project_url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_vpc(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let project_url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;

    let vpc_name = "just-rainsticks";
    create_vpc(&client, name, vpc_name).await;

    assert_eq!(
        "project to be deleted contains a vpc: just-rainsticks",
        delete_project_expect_fail(&project_url, &client).await,
    );

    let vpc_url = format!("/v1/vpcs/{vpc_name}?project={}", name);
    let default_subnet_url =
        format!("/v1/vpc-subnets/default?project={}&vpc={}", name, vpc_name);
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
    delete_project(&project_url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_affinity_group(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let project_url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;

    let group_name = "just-rainsticks";
    create_affinity_group(&client, name, group_name).await;

    assert_eq!(
        "project to be deleted contains an affinity group: just-rainsticks",
        delete_project_expect_fail(&project_url, &client).await,
    );

    let group_url =
        format!("/v1/affinity-groups/{group_name}?project={}", name);
    NexusRequest::object_delete(client, &group_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    delete_project(&project_url, &client).await;
}

#[nexus_test]
async fn test_project_deletion_with_anti_affinity_group(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let name = "springfield-squidport";
    let project_url = format!("/v1/projects/{}", name);

    create_project(&client, &name).await;
    delete_project_default_subnet(&name, &client).await;
    delete_project_default_vpc(&name, &client).await;

    let group_name = "just-rainsticks";
    create_anti_affinity_group(&client, name, group_name).await;

    assert_eq!(
        "project to be deleted contains an anti affinity group: just-rainsticks",
        delete_project_expect_fail(&project_url, &client).await,
    );

    let group_url =
        format!("/v1/anti-affinity-groups/{group_name}?project={}", name);
    NexusRequest::object_delete(client, &group_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();
    delete_project(&project_url, &client).await;
}
