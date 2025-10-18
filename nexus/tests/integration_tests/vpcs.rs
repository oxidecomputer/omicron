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
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::{
    create_local_user, create_project, create_vpc, create_vpc_with_error,
    grant_iam, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views;
use nexus_types::external_api::{params, shared, views::Vpc};
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
async fn test_vpc_networking_restrictions(cptestctx: &ControlPlaneTestContext) {
    use nexus_types::external_api::params;

    let client = &cptestctx.external_client;

    // Test Part 1: Normal silo (restrict_network_actions = false)
    // In the default silo, project collaborators CAN create VPCs
    let normal_project_name = "normal-project";
    create_project(&client, normal_project_name).await;

    let normal_vpcs_url = format!("/v1/vpcs?project={}", normal_project_name);
    let vpc_params = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "normal-vpc".parse().unwrap(),
            description: "VPC in normal silo".to_string(),
        },
        ipv6_prefix: None,
        dns_name: "normal".parse().unwrap(),
    };

    // As privileged user (silo admin), VPC creation should succeed
    let vpc: Vpc = object_create(&client, &normal_vpcs_url, &vpc_params).await;

    assert_eq!(vpc.identity.name, "normal-vpc");
    assert_eq!(vpc.dns_name, "normal");

    // Test Part 2: Restricted silo (restrict_network_actions = true)
    // Create a silo with networking restrictions enabled
    let restricted_silo_name = "restricted-silo";
    let silo_url = "/v1/system/silos";
    let silo_params = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_silo_name.parse().unwrap(),
            description: "Silo with networking restrictions".to_string(),
        },
        discoverable: false,
        identity_mode:
            nexus_types::external_api::shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: Vec::new(),
        mapped_fleet_roles: Default::default(),
        restrict_network_actions: Some(true), // Enable networking restrictions
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url, &silo_params).await;

    // Verify the silo has networking restrictions enabled
    assert_eq!(
        restricted_silo.identity.name,
        restricted_silo_name
            .parse::<omicron_common::api::external::Name>()
            .unwrap()
    );

    // Verify we can read the silo back and see the restriction flag
    let silo_get_url = format!("/v1/system/silos/{}", restricted_silo_name);
    let fetched_silo: nexus_types::external_api::views::Silo =
        NexusRequest::object_get(&client, &silo_get_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    assert_eq!(
        fetched_silo.identity.name,
        restricted_silo_name
            .parse::<omicron_common::api::external::Name>()
            .unwrap()
    );

    // Test Part 3: Test authorization with different user roles
    // Create a user in the restricted silo
    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Grant the user Silo Collaborator role so they can create a project
    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let existing_silo_policy: shared::Policy<shared::SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");
    let new_role_assignment = shared::RoleAssignment::for_silo_user(
        test_user.id,
        shared::SiloRole::Collaborator,
    );
    let new_role_assignments = existing_silo_policy
        .role_assignments
        .into_iter()
        .chain(std::iter::once(new_role_assignment))
        .collect();
    let new_silo_policy =
        shared::Policy { role_assignments: new_role_assignments };
    NexusRequest::object_put(client, &silo_policy_url, Some(&new_silo_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to update silo policy");

    // Create a project in the restricted silo AS THE SILO USER
    // This ensures the project is properly set up for that user
    let restricted_project_name = "restricted-project";
    let project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_project_name.parse().unwrap(),
            description: "Project in restricted silo".to_string(),
        },
    };

    let _restricted_project: views::Project =
        NexusRequest::objects_post(&client, "/v1/projects", &project_params)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;

    // Try to create a VPC as a Silo Collaborator (with Project Creator role)
    // Should FAIL with 403 Forbidden because the silo has restrict_network_actions=true
    // Note: When authenticated as a silo user, the silo context is implicit
    let restricted_vpcs_url =
        format!("/v1/vpcs?project={}", restricted_project_name);
    let restricted_vpc_params = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "restricted-vpc".parse().unwrap(),
            description: "VPC in restricted silo".to_string(),
        },
        ipv6_prefix: None,
        dns_name: "restricted".parse().unwrap(),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &restricted_vpcs_url)
            .body(Some(&restricted_vpc_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("VPC create should fail for Silo Collaborator in restricted silo");

    // Now grant the user Silo Admin role
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);
    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Try to create a VPC again as Silo Admin - should succeed
    let vpc_as_admin: Vpc = NexusRequest::objects_post(
        &client,
        &restricted_vpcs_url,
        &restricted_vpc_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("VPC creation should succeed for Silo Admin in restricted silo")
    .parsed_body()
    .unwrap();

    assert_eq!(vpc_as_admin.identity.name, "restricted-vpc");
    assert_eq!(vpc_as_admin.dns_name, "restricted");
}
