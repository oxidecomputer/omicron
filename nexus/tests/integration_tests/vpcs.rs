// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpErrorResponseBody;
use dropshot::ResultsPage;
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
use omicron_common::api::external::NameOrId;
use omicron_uuid_kinds::GenericUuid;

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

    // STEP 1: Setup - Create restricted silo and admin user
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
        restrict_network_actions: Some(true),
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url, &silo_params).await;

    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

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

    let restricted_vpcs_url =
        format!("/v1/vpcs?project={}", restricted_project_name);

    // STEP 2: As Admin - Create VPC
    let test_vpc_params = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-vpc".parse().unwrap(),
            description: "VPC for testing".to_string(),
        },
        ipv6_prefix: None,
        dns_name: "test".parse().unwrap(),
    };

    let created_vpc: Vpc = NexusRequest::objects_post(
        &client,
        &restricted_vpcs_url,
        &test_vpc_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(created_vpc.identity.name, "test-vpc");
    assert_eq!(created_vpc.dns_name, "test");

    // STEP 3: As Admin - Update VPC to verify it works
    let vpc_update_url =
        format!("/v1/vpcs/test-vpc?project={}", restricted_project_name);
    let vpc_update_params = params::VpcUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Updated by admin".to_string()),
        },
        dns_name: None,
    };

    let updated_vpc: Vpc = NexusRequest::object_put(
        &client,
        &vpc_update_url,
        Some(&vpc_update_params),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(updated_vpc.identity.description, "Updated by admin");

    // STEP 4: Demote to Collaborator
    let silo_policy: shared::Policy<shared::SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();

    let mut new_assignments: Vec<_> = silo_policy
        .role_assignments
        .into_iter()
        .filter(|ra| {
            !matches!(
                ra,
                shared::RoleAssignment {
                    identity_type: shared::IdentityType::SiloUser,
                    identity_id,
                    role_name,
                } if *identity_id == test_user_uuid && *role_name == shared::SiloRole::Admin
            )
        })
        .collect();

    new_assignments.push(shared::RoleAssignment::for_silo_user(
        test_user.id,
        shared::SiloRole::Collaborator,
    ));

    let collaborator_policy =
        shared::Policy { role_assignments: new_assignments };

    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // STEP 5: As Collaborator - Try to CREATE VPC (should fail)
    let collab_vpc_params = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-vpc".parse().unwrap(),
            description: "Collaborator creation attempt".to_string(),
        },
        ipv6_prefix: None,
        dns_name: "collab".parse().unwrap(),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &restricted_vpcs_url)
            .body(Some(&collab_vpc_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to create VPC");

    // STEP 6: As Collaborator - Try to UPDATE VPC (should fail)
    let vpc_update_params_collab = params::VpcUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Collaborator update attempt".to_string()),
        },
        dns_name: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &vpc_update_url)
            .body(Some(&vpc_update_params_collab))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to update VPC");

    // STEP 7: As Collaborator - Try to DELETE VPC (should fail)
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &vpc_update_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to delete VPC");

    // STEP 8: Promote back to Admin
    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 9: As Admin - Delete resources created in step 2
    // Delete the default subnet first (VPCs can't be deleted if they have subnets)
    let default_subnet_url = format!(
        "/v1/vpc-subnets/default?project={}&vpc=test-vpc",
        restricted_project_name
    );
    NexusRequest::object_delete(&client, &default_subnet_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete subnet");

    // Delete the VPC
    NexusRequest::object_delete(&client, &vpc_update_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete VPC");
}

#[nexus_test]
async fn test_vpc_subnet_networking_restrictions(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_types::external_api::params;

    let client = &cptestctx.external_client;

    // STEP 1: Setup - Create restricted silo and admin user
    let restricted_silo_name = "subnet-restricted-silo";
    let silo_url = "/v1/system/silos";
    let silo_params = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_silo_name.parse().unwrap(),
            description: "Silo with subnet networking restrictions".to_string(),
        },
        discoverable: false,
        identity_mode:
            nexus_types::external_api::shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: Vec::new(),
        mapped_fleet_roles: Default::default(),
        restrict_network_actions: Some(true),
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url, &silo_params).await;

    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"subnet-test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a project (this will automatically create a default VPC with a default subnet)
    let restricted_project_name = "subnet-restricted-project";
    let project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_project_name.parse().unwrap(),
            description: "Project in subnet restricted silo".to_string(),
        },
    };

    let _restricted_project: views::Project =
        NexusRequest::objects_post(&client, "/v1/projects", &project_params)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;

    let restricted_subnets_url = format!(
        "/v1/vpc-subnets?project={}&vpc=default",
        restricted_project_name
    );

    // STEP 2: As Admin - Create subnet
    let test_subnet_params = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-subnet".parse().unwrap(),
            description: "Subnet for testing".to_string(),
        },
        ipv4_block: "10.1.0.0/22".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
    };

    let created_subnet: views::VpcSubnet = NexusRequest::objects_post(
        &client,
        &restricted_subnets_url,
        &test_subnet_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(created_subnet.identity.name, "test-subnet");
    assert_eq!(created_subnet.identity.description, "Subnet for testing");

    // STEP 3: As Admin - Update subnet to verify it works
    let subnet_update_url = format!(
        "/v1/vpc-subnets/test-subnet?project={}&vpc=default",
        restricted_project_name
    );
    let subnet_update_params = params::VpcSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Updated by admin".to_string()),
        },
        custom_router: None,
    };

    let updated_subnet: views::VpcSubnet = NexusRequest::object_put(
        &client,
        &subnet_update_url,
        Some(&subnet_update_params),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(updated_subnet.identity.description, "Updated by admin");

    // STEP 4: Demote to Collaborator
    let silo_policy: shared::Policy<shared::SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();

    let mut new_assignments: Vec<_> = silo_policy
        .role_assignments
        .into_iter()
        .filter(|ra| {
            !matches!(
                ra,
                shared::RoleAssignment {
                    identity_type: shared::IdentityType::SiloUser,
                    identity_id,
                    role_name,
                } if *identity_id == test_user_uuid && *role_name == shared::SiloRole::Admin
            )
        })
        .collect();

    new_assignments.push(shared::RoleAssignment::for_silo_user(
        test_user.id,
        shared::SiloRole::Collaborator,
    ));

    let collaborator_policy =
        shared::Policy { role_assignments: new_assignments };

    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // STEP 5: As Collaborator - Try to CREATE subnet (should fail)
    let collab_subnet_params = params::VpcSubnetCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-subnet".parse().unwrap(),
            description: "Collaborator creation attempt".to_string(),
        },
        ipv4_block: "10.2.0.0/22".parse().unwrap(),
        ipv6_block: None,
        custom_router: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &restricted_subnets_url)
            .body(Some(&collab_subnet_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to create VPC subnet");

    // STEP 6: As Collaborator - Try to UPDATE subnet (should fail)
    let subnet_update_params_collab = params::VpcSubnetUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Collaborator update attempt".to_string()),
        },
        custom_router: None,
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &subnet_update_url)
            .body(Some(&subnet_update_params_collab))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to update VPC subnet");

    // STEP 7: As Collaborator - Try to DELETE subnet (should fail)
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &subnet_update_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to delete VPC subnet");

    // STEP 8: Promote back to Admin
    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 9: As Admin - Delete subnet created in step 2
    NexusRequest::object_delete(&client, &subnet_update_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete VPC subnet");
}

#[nexus_test]
async fn test_internet_gateway_firewall_networking_restrictions(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_test_utils::resource_helpers::{
        create_local_user, grant_iam, object_create, test_params,
    };
    use nexus_types::external_api::shared::SiloRole;
    use nexus_types::external_api::{params, shared, views};
    use omicron_common::api::external::{
        L4Port, L4PortRange, VpcFirewallRuleAction, VpcFirewallRuleDirection,
        VpcFirewallRuleFilter, VpcFirewallRulePriority,
        VpcFirewallRuleProtocol, VpcFirewallRuleStatus, VpcFirewallRuleUpdate,
        VpcFirewallRuleUpdateParams, VpcFirewallRules,
    };
    use std::convert::TryFrom;

    let client = &cptestctx.external_client;

    // STEP 1: Setup - Create restricted silo and admin user
    let restricted_silo_name = "igw-restricted-silo";
    let silo_url = "/v1/system/silos";
    let silo_params = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_silo_name.parse().unwrap(),
            description: "Silo with IGW networking restrictions".to_string(),
        },
        discoverable: false,
        identity_mode:
            nexus_types::external_api::shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: Vec::new(),
        mapped_fleet_roles: Default::default(),
        restrict_network_actions: Some(true),
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url, &silo_params).await;

    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"igw-test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let restricted_project_name = "igw-restricted-project";
    let project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_project_name.parse().unwrap(),
            description: "Project in IGW restricted silo".to_string(),
        },
    };

    let _restricted_project: views::Project =
        NexusRequest::objects_post(&client, "/v1/projects", &project_params)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;

    let restricted_igws_url = format!(
        "/v1/internet-gateways?project={}&vpc=default",
        restricted_project_name
    );

    // STEP 2: As Admin - Create internet gateway
    let test_igw_params = params::InternetGatewayCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-igw".parse().unwrap(),
            description: "IGW for testing".to_string(),
        },
    };

    let created_igw: views::InternetGateway = NexusRequest::objects_post(
        &client,
        &restricted_igws_url,
        &test_igw_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(created_igw.identity.name, "test-igw");
    assert_eq!(created_igw.identity.description, "IGW for testing");

    // STEP 3: As Admin - Update firewall rules to verify it works
    let firewall_rules_url = format!(
        "/v1/vpc-firewall-rules?project={}&vpc=default",
        restricted_project_name
    );

    let initial_firewall_params = VpcFirewallRuleUpdateParams {
        rules: vec![VpcFirewallRuleUpdate {
            name: "allow-ssh".parse().unwrap(),
            description: "Allow SSH".to_string(),
            action: VpcFirewallRuleAction::Allow,
            direction: VpcFirewallRuleDirection::Inbound,
            filters: VpcFirewallRuleFilter {
                hosts: None,
                ports: Some(vec![L4PortRange {
                    first: L4Port::try_from(22).unwrap(),
                    last: L4Port::try_from(22).unwrap(),
                }]),
                protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
            },
            priority: VpcFirewallRulePriority(100),
            status: VpcFirewallRuleStatus::Enabled,
            targets: vec![],
        }],
    };

    let initial_rules: VpcFirewallRules = NexusRequest::object_put(
        &client,
        &firewall_rules_url,
        Some(&initial_firewall_params),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(initial_rules.rules.len(), 1);
    assert_eq!(initial_rules.rules[0].identity.name, "allow-ssh");

    // STEP 4: Demote to Collaborator
    let silo_policy: shared::Policy<SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();

    let mut new_assignments: Vec<_> = silo_policy
        .role_assignments
        .into_iter()
        .filter(|ra| {
            !matches!(
                ra,
                shared::RoleAssignment {
                    identity_type: shared::IdentityType::SiloUser,
                    identity_id,
                    role_name,
                } if *identity_id == test_user_uuid && *role_name == SiloRole::Admin
            )
        })
        .collect();

    new_assignments.push(shared::RoleAssignment::for_silo_user(
        test_user.id,
        SiloRole::Collaborator,
    ));

    let collaborator_policy =
        shared::Policy { role_assignments: new_assignments };

    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // STEP 5: As Collaborator - Try to CREATE internet gateway (should fail)
    let collab_igw_params = params::InternetGatewayCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-igw".parse().unwrap(),
            description: "Collaborator IGW creation attempt".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &restricted_igws_url)
            .body(Some(&collab_igw_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to create internet gateway");

    // STEP 6: As Collaborator - Try to UPDATE firewall rules (should fail)
    let collab_firewall_params = VpcFirewallRuleUpdateParams {
        rules: vec![VpcFirewallRuleUpdate {
            name: "allow-icmp".parse().unwrap(),
            description: "Allow ICMP".to_string(),
            action: VpcFirewallRuleAction::Allow,
            direction: VpcFirewallRuleDirection::Inbound,
            filters: VpcFirewallRuleFilter {
                hosts: None,
                ports: None,
                protocols: Some(vec![VpcFirewallRuleProtocol::Icmp(None)]),
            },
            priority: VpcFirewallRulePriority(100),
            status: VpcFirewallRuleStatus::Enabled,
            targets: vec![],
        }],
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &firewall_rules_url)
            .body(Some(&collab_firewall_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to update firewall rules");

    // STEP 7: As Collaborator - Try to DELETE internet gateway (should fail)
    let igw_delete_url = format!(
        "/v1/internet-gateways/test-igw?project={}&vpc=default",
        restricted_project_name
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &igw_delete_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to delete internet gateway");

    // STEP 8: Promote back to Admin
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 9: As Admin - Delete internet gateway created in step 2
    NexusRequest::object_delete(&client, &igw_delete_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete internet gateway");
}

#[nexus_test]
async fn test_igw_ip_pool_address_attach_detach_restrictions(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_test_utils::resource_helpers::{
        create_local_user, grant_iam, object_create, test_params,
    };
    use nexus_types::external_api::shared::SiloRole;
    use nexus_types::external_api::{params, shared};
    use omicron_common::address::IpRange;
    use std::net::Ipv4Addr;

    let client = &cptestctx.external_client;

    // STEP 1: Setup - Create restricted silo and admin user
    let restricted_silo_name = "igw-pool-addr-restricted-silo";
    let silo_url_base = "/v1/system/silos";
    let silo_params = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_silo_name.parse().unwrap(),
            description:
                "Silo with IGW IP pool/address networking restrictions"
                    .to_string(),
        },
        discoverable: false,
        identity_mode:
            nexus_types::external_api::shared::SiloIdentityMode::LocalOnly,
        admin_group_name: None,
        tls_certificates: Vec::new(),
        mapped_fleet_roles: Default::default(),
        restrict_network_actions: Some(true),
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url_base, &silo_params).await;

    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"igw-pool-addr-test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let restricted_project_name = "igw-pool-addr-restricted-project";
    let project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_project_name.parse().unwrap(),
            description: "Project in IGW pool/addr restricted silo".to_string(),
        },
    };

    let _restricted_project: views::Project =
        NexusRequest::objects_post(&client, "/v1/projects", &project_params)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;

    // STEP 2: As Admin - Create internet gateway
    let restricted_igws_url = format!(
        "/v1/internet-gateways?project={}&vpc=default",
        restricted_project_name
    );

    let test_igw_params = params::InternetGatewayCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-igw-pools".parse().unwrap(),
            description: "IGW for pool/address testing".to_string(),
        },
    };

    let created_igw: views::InternetGateway = NexusRequest::objects_post(
        &client,
        &restricted_igws_url,
        &test_igw_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(created_igw.identity.name, "test-igw-pools");

    // STEP 3: As Admin (privileged) - Create IP pool and link it to the silo
    let pool_name = "test-pool-igw";
    let pool_params = params::IpPoolCreate::new(
        IdentityMetadataCreateParams {
            name: pool_name.parse().unwrap(),
            description: String::from("IP pool for IGW testing"),
        },
        views::IpVersion::v4(),
    );
    let _pool: views::IpPool =
        object_create(client, "/v1/system/ip-pools", &pool_params).await;

    // Add IP range to the pool
    let ip_range = IpRange::try_from((
        Ipv4Addr::new(198, 51, 100, 1),
        Ipv4Addr::new(198, 51, 100, 254),
    ))
    .unwrap();
    let url = format!("/v1/system/ip-pools/{}/ranges/add", pool_name);
    let _range: views::IpPoolRange =
        object_create(client, &url, &ip_range).await;

    // Link pool to silo
    let link = params::IpPoolLinkSilo {
        silo: NameOrId::Id(restricted_silo.identity.id),
        is_default: true,
    };
    let url = format!("/v1/system/ip-pools/{}/silos", pool_name);
    let _link: views::IpPoolSiloLink = object_create(client, &url, &link).await;

    // STEP 4: As Admin - Attach IP pool to internet gateway
    let attach_pool_url = format!(
        "/v1/internet-gateway-ip-pools?project={}&vpc=default&gateway=test-igw-pools",
        restricted_project_name
    );

    let attach_pool_params = params::InternetGatewayIpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: "pool-attachment-1".parse().unwrap(),
            description: "Initial pool attachment".to_string(),
        },
        ip_pool: NameOrId::Name(pool_name.parse().unwrap()),
    };

    let attached_pool: views::InternetGatewayIpPool =
        NexusRequest::objects_post(
            &client,
            &attach_pool_url,
            &attach_pool_params,
        )
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute_and_parse_unwrap()
        .await;

    assert_eq!(attached_pool.identity.name, "pool-attachment-1");

    // STEP 5: As Admin - Attach IP address to internet gateway
    let attach_address_url = format!(
        "/v1/internet-gateway-ip-addresses?project={}&vpc=default&gateway=test-igw-pools",
        restricted_project_name
    );

    let test_ip = Ipv4Addr::new(198, 51, 100, 42);
    let attach_address_params = params::InternetGatewayIpAddressCreate {
        identity: IdentityMetadataCreateParams {
            name: "address-attachment-1".parse().unwrap(),
            description: "Initial address attachment".to_string(),
        },
        address: test_ip.into(),
    };

    let attached_address: views::InternetGatewayIpAddress =
        NexusRequest::objects_post(
            &client,
            &attach_address_url,
            &attach_address_params,
        )
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute_and_parse_unwrap()
        .await;

    assert_eq!(attached_address.identity.name, "address-attachment-1");

    // STEP 6: Demote to Collaborator
    let silo_policy: shared::Policy<SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();

    let mut new_assignments: Vec<_> = silo_policy
        .role_assignments
        .into_iter()
        .filter(|ra| {
            !matches!(
                ra,
                shared::RoleAssignment {
                    identity_type: shared::IdentityType::SiloUser,
                    identity_id,
                    role_name,
                } if *identity_id == test_user_uuid && *role_name == SiloRole::Admin
            )
        })
        .collect();

    new_assignments.push(shared::RoleAssignment::for_silo_user(
        test_user.id,
        SiloRole::Collaborator,
    ));

    let collaborator_policy =
        shared::Policy { role_assignments: new_assignments };

    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // STEP 7: As Collaborator - Try to ATTACH IP pool (should fail)
    let collab_pool_params = params::InternetGatewayIpPoolCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-pool-attachment".parse().unwrap(),
            description: "Collaborator pool attachment attempt".to_string(),
        },
        ip_pool: NameOrId::Name(pool_name.parse().unwrap()),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &attach_pool_url)
            .body(Some(&collab_pool_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to attach IP pool");

    // STEP 8: As Collaborator - Try to DETACH IP pool (should fail)
    let detach_pool_url = format!(
        "/v1/internet-gateway-ip-pools/pool-attachment-1?project={}&vpc=default&gateway=test-igw-pools&cascade=false",
        restricted_project_name
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &detach_pool_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to detach IP pool");

    // STEP 9: As Collaborator - Try to ATTACH IP address (should fail)
    let another_test_ip = Ipv4Addr::new(198, 51, 100, 99);
    let collab_address_params = params::InternetGatewayIpAddressCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-address-attachment".parse().unwrap(),
            description: "Collaborator address attachment attempt".to_string(),
        },
        address: another_test_ip.into(),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &attach_address_url)
            .body(Some(&collab_address_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to attach IP address");

    // STEP 10: As Collaborator - Try to DETACH IP address (should fail)
    let detach_address_url = format!(
        "/v1/internet-gateway-ip-addresses/address-attachment-1?project={}&vpc=default&gateway=test-igw-pools&cascade=false",
        restricted_project_name
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &detach_address_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to detach IP address");

    // STEP 11: Promote back to Admin
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 12: As Admin - Detach IP address
    NexusRequest::object_delete(&client, &detach_address_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to detach IP address");

    // STEP 13: As Admin - Detach IP pool
    NexusRequest::object_delete(&client, &detach_pool_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to detach IP pool");

    // STEP 14: As Admin - Delete internet gateway (cleanup)
    let igw_delete_url = format!(
        "/v1/internet-gateways/test-igw-pools?project={}&vpc=default",
        restricted_project_name
    );

    NexusRequest::object_delete(&client, &igw_delete_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete internet gateway");
}

/// Test that project creation respects networking restrictions:
/// - Silo admins can create projects with default VPCs
/// - Non-admins in restricted silos create projects WITHOUT default VPCs
#[nexus_test]
async fn test_project_create_networking_restrictions(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_types::external_api::params;

    let client = &cptestctx.external_client;

    // STEP 1: Setup - Create restricted silo and admin user
    let restricted_silo_name = "restricted-silo";
    let silo_url_path = "/v1/system/silos";
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
        restrict_network_actions: Some(true),
        quotas: params::SiloQuotasCreate::empty(),
    };

    let restricted_silo: views::Silo =
        object_create(&client, silo_url_path, &silo_params).await;

    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    // Grant Silo Admin role
    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 2: As Admin - Create project and verify it has default VPC
    let admin_project_name = "admin-project";
    let admin_project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: admin_project_name.parse().unwrap(),
            description: "Project created by admin".to_string(),
        },
    };

    let _admin_project: views::Project = NexusRequest::objects_post(
        &client,
        "/v1/projects",
        &admin_project_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    // Verify default VPC was created
    let admin_vpcs_url = format!("/v1/vpcs?project={}", admin_project_name);
    let admin_vpcs_result = NexusRequest::object_get(&client, &admin_vpcs_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute_and_parse_unwrap()
        .await;
    let admin_vpcs: ResultsPage<Vpc> = admin_vpcs_result;

    assert_eq!(
        admin_vpcs.items.len(),
        1,
        "Admin project should have default VPC"
    );
    assert_eq!(admin_vpcs.items[0].identity.name, "default");

    // STEP 3: Demote to Collaborator
    let silo_policy: shared::Policy<shared::SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();
    let updated_role_assignments = silo_policy
        .role_assignments
        .into_iter()
        .map(|assignment| {
            if assignment.identity_id == test_user_uuid {
                shared::RoleAssignment {
                    identity_type: assignment.identity_type,
                    identity_id: assignment.identity_id,
                    role_name: shared::SiloRole::Collaborator,
                }
            } else {
                assignment
            }
        })
        .collect();

    let updated_policy =
        shared::Policy { role_assignments: updated_role_assignments };

    NexusRequest::object_put(client, &silo_policy_url, Some(&updated_policy))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Failed to update silo policy");

    // STEP 4: As Collaborator - Create project and verify NO default VPC
    let collab_project_name = "collab-project";
    let collab_project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: collab_project_name.parse().unwrap(),
            description: "Project created by collaborator".to_string(),
        },
    };

    let _collab_project: views::Project = NexusRequest::objects_post(
        &client,
        "/v1/projects",
        &collab_project_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    // Verify NO default VPC was created
    let collab_vpcs_url = format!("/v1/vpcs?project={}", collab_project_name);
    let collab_vpcs_result =
        NexusRequest::object_get(&client, &collab_vpcs_url)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;
    let collab_vpcs: ResultsPage<Vpc> = collab_vpcs_result;

    assert_eq!(
        collab_vpcs.items.len(),
        0,
        "Collaborator project should NOT have default VPC"
    );

    // STEP 5: Promote back to Admin
    grant_iam(
        client,
        &silo_url,
        shared::SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // STEP 6: As Admin - Create another project and verify it has default VPC
    let admin_project_name_2 = "admin-project-2";
    let admin_project_params_2 = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: admin_project_name_2.parse().unwrap(),
            description: "Second project created by admin".to_string(),
        },
    };

    let _admin_project_2: views::Project = NexusRequest::objects_post(
        &client,
        "/v1/projects",
        &admin_project_params_2,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    // Verify default VPC was created
    let admin_vpcs_url_2 = format!("/v1/vpcs?project={}", admin_project_name_2);
    let admin_vpcs_2_result =
        NexusRequest::object_get(&client, &admin_vpcs_url_2)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;
    let admin_vpcs_2: ResultsPage<Vpc> = admin_vpcs_2_result;

    assert_eq!(
        admin_vpcs_2.items.len(),
        1,
        "Admin project should have default VPC"
    );
    assert_eq!(admin_vpcs_2.items[0].identity.name, "default");
}
