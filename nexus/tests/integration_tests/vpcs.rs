// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::authn::USER_TEST_UNPRIVILEGED;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::{
    create_local_user, create_project, create_vpc, create_vpc_with_error,
    grant_iam, objects_list_page_authz, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared::ProjectRole;
use nexus_types::external_api::views::{Silo, Vpc};
use nexus_types::identity::{Asset, Resource};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

static PROJECT_NAME: &str = "springfield-squidport";
static PROJECT_NAME_2: &str = "peeky-park";

fn get_vpc_url(vpc_name: &str) -> String {
    format!("/v1/vpcs/{vpc_name}?project={}", PROJECT_NAME)
}

fn get_subnet_url(vpc_name: &str, subnet_name: &str) -> String {
    format!(
        "/v1/vpc-subnets/{subnet_name}?project={}&vpc={}",
        PROJECT_NAME, vpc_name
    )
}

#[nexus_test]
async fn test_vpcs(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let vpcs_url = format!("/v1/vpcs?project={}", PROJECT_NAME);
    let _ = create_project(&client, &PROJECT_NAME).await;
    let _ = create_project(&client, &PROJECT_NAME_2).await;

    // List vpcs.  We see the default VPC, and nothing else.
    let mut vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    assert_eq!(vpcs[0].identity.name, "default");
    assert_eq!(vpcs[0].dns_name, "default");
    let default_vpc = vpcs.remove(0);

    // Make sure we get a 404 if we fetch or delete one.
    let vpc_url = get_vpc_url("just-rainsticks");
    for method in &[Method::GET, Method::DELETE] {
        let error: HttpErrorResponseBody = NexusRequest::expect_failure(
            client,
            StatusCode::NOT_FOUND,
            method.clone(),
            &vpc_url,
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
        assert_eq!(
            error.message,
            "not found: vpc with name \"just-rainsticks\""
        );
    }

    // Make sure creating a VPC fails if we specify an IPv6 prefix that is
    // not a valid ULA range.
    let bad_prefix = "2000:1000::/48".parse().unwrap();
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &vpcs_url)
            .expect_status(Some(StatusCode::BAD_REQUEST))
            .body(Some(&params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: "just-rainsticks".parse().unwrap(),
                    description: String::from("vpc description"),
                },
                ipv6_prefix: Some(bad_prefix),
                dns_name: "abc".parse().unwrap(),
            })),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Create a VPC.
    let vpc_name = "just-rainsticks";
    let vpc = create_vpc(&client, PROJECT_NAME, vpc_name).await;
    assert_eq!(vpc.identity.name, "just-rainsticks");
    assert_eq!(vpc.identity.description, "vpc description");
    assert_eq!(vpc.dns_name, "abc");
    assert_eq!(
        vpc.ipv6_prefix.width(),
        48,
        "Expected a 48-bit ULA IPv6 address prefix"
    );
    assert!(
        vpc.ipv6_prefix.is_unique_local(),
        "Expected a ULA IPv6 address prefix"
    );

    // Attempt to create a second VPC with a conflicting name.
    let error = create_vpc_with_error(
        &client,
        PROJECT_NAME,
        vpc_name,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.message, "already exists: vpc \"just-rainsticks\"");

    // creating a VPC with the same name in another project works, though
    let vpc2: Vpc = create_vpc(&client, PROJECT_NAME_2, vpc_name).await;
    assert_eq!(vpc2.identity.name, "just-rainsticks");

    // List VPCs again and expect to find the one we just created.
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 2);
    vpcs_eq(&vpcs[0], &default_vpc);
    vpcs_eq(&vpcs[1], &vpc);

    // Fetch the VPC and expect it to match.
    let vpc = vpc_get(&client, &vpc_url).await;
    vpcs_eq(&vpcs[1], &vpc);

    // Update the VPC
    let update_params = params::VpcUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("new-name".parse().unwrap()),
            description: Some("another description".to_string()),
        },
        dns_name: Some("def".parse().unwrap()),
    };
    let updated_vpc = vpc_put(&client, &vpc_url, update_params).await;
    assert_eq!(updated_vpc.identity.name, "new-name");
    assert_eq!(updated_vpc.identity.description, "another description");
    assert_eq!(updated_vpc.dns_name, "def");

    // fetching by old name fails
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &vpc_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc with name \"just-rainsticks\"");

    // new url with new name
    let vpc_url = get_vpc_url("new-name");

    // Fetch the VPC again. It should have the updated properties.
    let vpc = vpc_get(&client, &vpc_url).await;
    assert_eq!(vpc.identity.name, "new-name");
    assert_eq!(vpc.identity.description, "another description");
    assert_eq!(vpc.dns_name, "def");

    // Deleting the VPC should fail, since the subnet still exists.
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &vpc_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "VPC cannot be deleted while VPC Subnets exist",);

    // Delete the default VPC Subnet and VPC.
    let default_subnet_url = get_subnet_url("new-name", "default");
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

    // Now we expect a 404 on fetch
    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &vpc_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc with name \"new-name\"");

    // And the list should be empty (aside from default VPC) again
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 1);
    vpcs_eq(&vpcs[0], &default_vpc);
}

async fn vpcs_list(client: &ClientTestContext, vpcs_url: &str) -> Vec<Vpc> {
    objects_list_page_authz::<Vpc>(client, vpcs_url).await.items
}

async fn vpc_get(client: &ClientTestContext, vpc_url: &str) -> Vpc {
    NexusRequest::object_get(client, vpc_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap()
        .await
}

async fn vpc_put(
    client: &ClientTestContext,
    vpc_url: &str,
    params: params::VpcUpdate,
) -> Vpc {
    NexusRequest::object_put(client, vpc_url, Some(&params))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap()
        .await
}

fn vpcs_eq(vpc1: &Vpc, vpc2: &Vpc) {
    identity_eq(&vpc1.identity, &vpc2.identity);
    assert_eq!(vpc1.project_id, vpc2.project_id);
}

#[nexus_test]
async fn test_vpc_collaborator_no_networking_role(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project
    let project_name = "test-project";
    create_project(&client, &project_name).await;
    let project_url = format!("/v1/projects/{}", project_name);
    let vpcs_url = format!("/v1/vpcs?project={}", project_name);

    use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
    let silo: Silo = NexusRequest::object_get(
        client,
        &format!("/v1/system/silos/{}", DEFAULT_SILO.name()),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;

    // Grant the Project Collaborator role to a local user
    grant_iam(
        client,
        &project_url,
        ProjectRole::Collaborator,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a local user and give them the CollaboratorNoNetworking role
    let no_networking_user = create_local_user(
        client,
        &silo,
        &"no-networking-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    grant_iam(
        client,
        &project_url,
        ProjectRole::CollaboratorNoNetworking,
        no_networking_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Test 1: Verify the PrivilegedUser can create a VPC
    let vpc_name = "test-vpc";
    let _vpc_priv: Vpc = NexusRequest::objects_post(
        client,
        &vpcs_url,
        &params::VpcCreate {
            identity: IdentityMetadataCreateParams {
                name: vpc_name.parse().unwrap(),
                description: "test vpc".to_string(),
            },
            ipv6_prefix: None,
            dns_name: "test".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("privileged user should be able to create VPC")
    .parsed_body()
    .unwrap();

    // Test 2: Unprivileged user with Collaborator role CAN create a VPC
    let vpc_name2 = "test-vpc-2";
    let vpc: Vpc = NexusRequest::objects_post(
        client,
        &vpcs_url,
        &params::VpcCreate {
            identity: IdentityMetadataCreateParams {
                name: vpc_name2.parse().unwrap(),
                description: "test vpc 2".to_string(),
            },
            ipv6_prefix: None,
            dns_name: "test2".parse().unwrap(),
        },
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect(
        "unprivileged user with Collaborator role should be able to create VPC",
    )
    .parsed_body()
    .unwrap();
    assert_eq!(vpc.identity.name, vpc_name2);

    // Test 3: User with CollaboratorNoNetworking role CAN read/list VPCs (viewer inheritance)
    let vpcs_list: Vec<Vpc> = NexusRequest::object_get(client, &vpcs_url)
        .authn_as(AuthnMode::SiloUser(no_networking_user.id))
        .execute()
        .await
        .expect("CollaboratorNoNetworking should be able to list VPCs")
        .parsed_body::<dropshot::ResultsPage<Vpc>>()
        .unwrap()
        .items;
    assert!(
        vpcs_list.len() >= 2,
        "Should see default VPC and the created VPCs"
    );

    // Test 4: User with CollaboratorNoNetworking role CANNOT create a VPC
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &vpcs_url)
            .body(Some(&params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: "forbidden-vpc".parse().unwrap(),
                    description: "should not be created".to_string(),
                },
                ipv6_prefix: None,
                dns_name: "forbidden".parse().unwrap(),
            }))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(no_networking_user.id))
    .execute()
    .await
    .expect("request should complete")
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "Forbidden");
}
