// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::instances::assert_sled_vpc_routes;
use crate::integration_tests::instances::instance_simulate;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance_with;
use nexus_test_utils::resource_helpers::create_route;
use nexus_test_utils::resource_helpers::create_router;
use nexus_test_utils::resource_helpers::create_vpc_subnet;
use nexus_test_utils::resource_helpers::object_delete;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::{create_project, create_vpc};
use nexus_test_utils::resource_helpers::{object_put, object_put_error};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::InstanceNetworkInterfaceAttachment;
use nexus_types::external_api::params::InstanceNetworkInterfaceCreate;
use nexus_types::external_api::params::VpcSubnetUpdate;
use nexus_types::external_api::views::VpcRouter;
use nexus_types::external_api::views::VpcRouterKind;
use nexus_types::external_api::views::VpcSubnet;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::SimpleIdentityOrName;
use omicron_common::api::internal::shared::ResolvedVpcRoute;
use omicron_common::api::internal::shared::RouterTarget;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use std::collections::HashMap;

pub const PROJECT_NAME: &str = "cartographer";
pub const VPC_NAME: &str = "the-isles";
pub const SUBNET_NAMES: &[&str] = &["scotia", "albion", "eire"];
const INSTANCE_NAMES: &[&str] = &["glaschu", "londinium"];
pub const ROUTER_NAMES: &[&str] = &["cycle-network", "motorways"];

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_vpc_routers_crud_operations(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let _ = create_project(&client, PROJECT_NAME).await;

    // Create a VPC.
    let vpc = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;

    let routers_url =
        format!("/v1/vpc-routers?project={}&vpc={}", PROJECT_NAME, VPC_NAME);

    // get routers should have only the system router created w/ the VPC
    let routers = list_routers(client, &VPC_NAME).await;
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].kind, VpcRouterKind::System);

    // This router should not be deletable.
    let system_router_url = format!("/v1/vpc-routers/{}", routers[0].id());
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::DELETE,
        &system_router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "cannot delete system router");

    let router_name = ROUTER_NAMES[0];
    let router_url = format!(
        "/v1/vpc-routers/{}?project={}&vpc={}",
        router_name, PROJECT_NAME, VPC_NAME
    );

    // fetching a particular router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: vpc-router with name \"{router_name}\"")
    );

    // Create a VPC Router.
    let router =
        create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;
    assert_eq!(router.identity.name, router_name);
    assert_eq!(router.identity.description, "router description");
    assert_eq!(router.vpc_id, vpc.identity.id);
    assert_eq!(router.kind, VpcRouterKind::Custom);

    // get router, should be the same
    let same_router = NexusRequest::object_get(client, &router_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body()
        .unwrap();
    routers_eq(&router, &same_router);

    // routers list should now have the one in it
    let routers = list_routers(client, &VPC_NAME).await;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router);

    // creating another router in the same VPC with the same name fails
    let error: dropshot::HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(&client, Method::POST, &routers_url)
            .body(Some(&params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: router_name.parse().unwrap(),
                    description: String::from("this is not a router"),
                },
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("already exists: vpc-router \"{router_name}\"")
    );

    let router2_name = ROUTER_NAMES[1];
    let router2_url = format!(
        "/v1/vpc-routers/{}?project={}&vpc={}",
        router2_name, PROJECT_NAME, VPC_NAME
    );

    // second router 404s before it's created
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router2_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: vpc-router with name \"{router2_name}\"")
    );

    // create second custom router
    let router2 =
        create_router(client, PROJECT_NAME, VPC_NAME, router2_name).await;
    assert_eq!(router2.identity.name, router2_name);
    assert_eq!(router2.vpc_id, vpc.identity.id);
    assert_eq!(router2.kind, VpcRouterKind::Custom);

    // routers list should now have two custom and one system
    let routers = list_routers(client, &VPC_NAME).await;
    assert_eq!(routers.len(), 3);
    routers_eq(&routers[0], &router);
    routers_eq(&routers[1], &router2);

    // update first router
    let update_params = params::VpcRouterUpdate {
        identity: IdentityMetadataUpdateParams {
            name: Some("new-name".parse().unwrap()),
            description: Some("another description".to_string()),
        },
    };
    let update: VpcRouter =
        NexusRequest::object_put(&client, &router_url, Some(&update_params))
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(update.identity.id, router.identity.id);
    assert_eq!(update.identity.name, update_params.identity.name.unwrap());
    assert_eq!(
        update.identity.description,
        update_params.identity.description.unwrap()
    );

    // fetching by old name 404s
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        &client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        format!("not found: vpc-router with name \"{router_name}\"")
    );

    let router_url = format!(
        "/v1/vpc-routers/new-name?project={}&vpc={}",
        PROJECT_NAME, VPC_NAME
    );

    // fetching by new name works
    let updated_router: VpcRouter =
        NexusRequest::object_get(&client, &router_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    routers_eq(&update, &updated_router);
    assert_eq!(&updated_router.identity.description, "another description");

    // fetching list should show updated one
    let routers = list_routers(client, &VPC_NAME).await;
    assert_eq!(routers.len(), 3);
    routers_eq(
        &routers.iter().find(|v| v.name().as_str() == "new-name").unwrap(),
        &updated_router,
    );

    // delete first router
    NexusRequest::object_delete(&client, &router_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

    // routers list should now have two again, one system and one custom
    let routers = list_routers(client, &VPC_NAME).await;
    assert_eq!(routers.len(), 2);
    routers_eq(&routers[0], &router2);

    // get router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"new-name\"");

    // delete router should 404
    let error: dropshot::HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::DELETE,
        &router_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: vpc-router with name \"new-name\"");

    // Creating a router with the same name in a different VPC is allowed
    let vpc2_name = "vpc2";
    let vpc2 = create_vpc(&client, PROJECT_NAME, vpc2_name).await;

    let router_same_name =
        create_router(&client, PROJECT_NAME, vpc2_name, router2_name).await;
    assert_eq!(router_same_name.identity.name, router2_name);
    assert_eq!(router_same_name.vpc_id, vpc2.identity.id);
}

#[nexus_test]
async fn test_vpc_routers_attach_to_subnet(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a project that we'll use for testing.
    let _ = create_project(&client, PROJECT_NAME).await;
    let _ = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;

    let subnet_name = "default";

    let subnets_url =
        format!("/v1/vpc-subnets?project={}&vpc={}", PROJECT_NAME, VPC_NAME);

    // get routers should have only the system router created w/ the VPC
    let routers = list_routers(client, VPC_NAME).await;
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].kind, VpcRouterKind::System);

    // Create a custom router for later use.
    let router_name = ROUTER_NAMES[0];
    let router =
        create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;
    assert_eq!(router.kind, VpcRouterKind::Custom);

    // Attaching a system router should fail.
    let err = object_put_error(
        client,
        &format!(
            "/v1/vpc-subnets/{subnet_name}?project={PROJECT_NAME}&vpc={VPC_NAME}"
        ),
        &VpcSubnetUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            custom_router: Some(routers[0].identity.id.into()),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(err.message, "cannot attach a system router to a VPC subnet");

    // Attaching a new custom router should succeed.
    let default_subnet = set_custom_router(
        client,
        "default",
        VPC_NAME,
        Some(router.identity.id.into()),
    )
    .await;
    assert_eq!(default_subnet.custom_router_id, Some(router.identity.id));

    // Attaching a custom router to another subnet (same VPC) should succeed:
    // ... at create time.
    let subnet2_name = SUBNET_NAMES[0];
    let subnet2 = create_vpc_subnet(
        &client,
        &PROJECT_NAME,
        &VPC_NAME,
        &subnet2_name,
        "192.168.0.0/24".parse().unwrap(),
        None,
        Some(router_name),
    )
    .await;
    assert_eq!(subnet2.custom_router_id, Some(router.identity.id));

    // ... and via update.
    let subnet3_name = SUBNET_NAMES[1];
    let _ = create_vpc_subnet(
        &client,
        &PROJECT_NAME,
        &VPC_NAME,
        &subnet3_name,
        "192.168.1.0/24".parse().unwrap(),
        None,
        None,
    )
    .await;

    let subnet3 = set_custom_router(
        client,
        subnet3_name,
        VPC_NAME,
        Some(router.identity.id.into()),
    )
    .await;
    assert_eq!(subnet3.custom_router_id, Some(router.identity.id));

    // Attaching a custom router to another VPC's subnet should fail.
    create_vpc(&client, PROJECT_NAME, "vpc1").await;
    let err = object_put_error(
        client,
        &format!("/v1/vpc-subnets/default?project={PROJECT_NAME}&vpc=vpc1"),
        &VpcSubnetUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            custom_router: Some(router.identity.id.into()),
        },
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        err.message,
        "a router can only be attached to a subnet when both belong to the same VPC"
    );

    // Detach (and double detach) should succeed without issue.
    let subnet3 = set_custom_router(client, subnet3_name, VPC_NAME, None).await;
    assert_eq!(subnet3.custom_router_id, None);
    let subnet3 = set_custom_router(client, subnet3_name, VPC_NAME, None).await;
    assert_eq!(subnet3.custom_router_id, None);

    // Assigning a new router should not require that we first detach the old one.
    let router2_name = ROUTER_NAMES[1];
    let router2 =
        create_router(&client, PROJECT_NAME, VPC_NAME, router2_name).await;
    let subnet2 = set_custom_router(
        client,
        subnet2_name,
        VPC_NAME,
        Some(router2.identity.id.into()),
    )
    .await;
    assert_eq!(subnet2.custom_router_id, Some(router2.identity.id));

    // Reset subnet2 back to our first router.
    let subnet2 = set_custom_router(
        client,
        subnet2_name,
        VPC_NAME,
        Some(router.identity.id.into()),
    )
    .await;
    assert_eq!(subnet2.custom_router_id, Some(router.identity.id));

    // Deleting a custom router should detach from remaining subnets.
    object_delete(
        &client,
        &format!(
            "/v1/vpc-routers/{router_name}?vpc={VPC_NAME}&project={PROJECT_NAME}",
        ),
    )
    .await;

    for subnet in
        objects_list_page_authz::<VpcSubnet>(client, &subnets_url).await.items
    {
        assert!(subnet.custom_router_id.is_none(), "{subnet:?}");
    }
}

#[nexus_test]
async fn test_vpc_routers_custom_delivered_to_instance(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.server_context();
    let nexus = &apictx.nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create some instances, one per subnet, and a default pool etc.
    create_default_ip_pool(client).await;
    create_project(client, PROJECT_NAME).await;

    let vpc = create_vpc(&client, PROJECT_NAME, VPC_NAME).await;

    let mut subnets = vec![];
    let mut instances = vec![];
    let mut instance_nics = HashMap::new();
    for (i, (subnet_name, instance_name)) in
        SUBNET_NAMES.iter().zip(INSTANCE_NAMES.iter()).enumerate()
    {
        let subnet = create_vpc_subnet(
            &client,
            PROJECT_NAME,
            VPC_NAME,
            subnet_name,
            format!("192.168.{i}.0/24").parse().unwrap(),
            None,
            None,
        )
        .await;

        let instance = create_instance_with(
            client,
            PROJECT_NAME,
            instance_name,
            &InstanceNetworkInterfaceAttachment::Create(vec![
                InstanceNetworkInterfaceCreate {
                    identity: IdentityMetadataCreateParams {
                        name: format!("nic-{i}").parse().unwrap(),
                        description: "".into(),
                    },
                    vpc_name: vpc.name().clone(),
                    subnet_name: subnet_name.parse().unwrap(),
                    ip: Some(format!("192.168.{i}.10").parse().unwrap()),
                    transit_ips: vec![],
                },
            ]),
            vec![],
            vec![],
            true,
            Default::default(),
            None,
        )
        .await;
        instance_simulate(
            nexus,
            &InstanceUuid::from_untyped_uuid(instance.identity.id),
        )
        .await;

        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance.identity.id)
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await
            .unwrap();

        let guest_nics = datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await
            .unwrap();

        instance_nics.insert(*instance_name, guest_nics);
        subnets.push(subnet);
        instances.push(instance);
    }

    let sled_agent = cptestctx.first_sled_agent();

    // Create some routers!
    let mut routers = vec![];
    for router_name in ROUTER_NAMES {
        let router =
            create_router(&client, PROJECT_NAME, VPC_NAME, router_name).await;

        routers.push(router);
    }

    let vni = instance_nics[INSTANCE_NAMES[0]][0].vni;

    // Installing a custom router onto a subnet with a live instance
    // should install routes at that sled. We should only have one sled.
    // First, assert the default state.
    for subnet in &subnets {
        let (_system, custom) = assert_sled_vpc_routes(
            &sled_agent,
            &opctx,
            &datastore,
            subnet.id(),
            vni,
        )
        .await;

        assert!(custom.is_empty());
    }

    // Push a distinct route into each router and attach to each subnet.
    for i in 0..2 {
        create_route(
            &client,
            PROJECT_NAME,
            VPC_NAME,
            ROUTER_NAMES[i],
            "a-sharp-drop",
            format!("ipnet:24{i}.0.0.0/8").parse().unwrap(),
            "drop".parse().unwrap(),
        )
        .await;

        set_custom_router(
            &client,
            SUBNET_NAMES[i],
            VPC_NAME,
            Some(NameOrId::Name(ROUTER_NAMES[i].parse().unwrap())),
        )
        .await;
    }

    // Re-verify, assert that new routes are resolved correctly.
    // Vec<(System, Custom)>.
    let mut last_routes = vec![];
    for subnet in &subnets {
        last_routes.push(
            assert_sled_vpc_routes(
                &sled_agent,
                &opctx,
                &datastore,
                subnet.id(),
                vni,
            )
            .await,
        );
    }

    assert!(last_routes[0].1.contains(&ResolvedVpcRoute {
        dest: "240.0.0.0/8".parse().unwrap(),
        target: RouterTarget::Drop,
    }));
    assert!(last_routes[1].1.contains(&ResolvedVpcRoute {
        dest: "241.0.0.0/8".parse().unwrap(),
        target: RouterTarget::Drop,
    }));

    // Adding a new route should propagate that out to sleds.
    create_route(
        &client,
        PROJECT_NAME,
        VPC_NAME,
        ROUTER_NAMES[0],
        "ncn-74",
        "ipnet:2.0.7.0/24".parse().unwrap(),
        format!("instance:{}", INSTANCE_NAMES[1]).parse().unwrap(),
    )
    .await;

    let (new_system, new_custom) = assert_sled_vpc_routes(
        &sled_agent,
        &opctx,
        &datastore,
        subnets[0].id(),
        vni,
    )
    .await;

    assert_eq!(last_routes[0].0, new_system);
    assert!(new_custom.contains(&ResolvedVpcRoute {
        dest: "2.0.7.0/24".parse().unwrap(),
        target: RouterTarget::Ip(instance_nics[INSTANCE_NAMES[1]][0].ip),
    }));

    // Swapping router should change the installed routes at that sled.
    set_custom_router(
        &client,
        SUBNET_NAMES[0],
        VPC_NAME,
        Some(NameOrId::Name(ROUTER_NAMES[1].parse().unwrap())),
    )
    .await;
    let (new_system, new_custom) = assert_sled_vpc_routes(
        &sled_agent,
        &opctx,
        &datastore,
        subnets[0].id(),
        vni,
    )
    .await;
    assert_eq!(last_routes[0].0, new_system);
    assert_eq!(last_routes[1].1, new_custom);

    // Unsetting a router should remove affected non-system routes.
    set_custom_router(&client, SUBNET_NAMES[0], VPC_NAME, None).await;
    let (new_system, new_custom) = assert_sled_vpc_routes(
        &sled_agent,
        &opctx,
        &datastore,
        subnets[0].id(),
        vni,
    )
    .await;
    assert_eq!(last_routes[0].0, new_system);
    assert!(new_custom.is_empty());
}

async fn set_custom_router(
    client: &ClientTestContext,
    subnet_name: &str,
    vpc_name: &str,
    custom_router: Option<NameOrId>,
) -> VpcSubnet {
    object_put(
        client,
        &format!(
            "/v1/vpc-subnets/{subnet_name}?project={PROJECT_NAME}&vpc={vpc_name}"
        ),
        &VpcSubnetUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            custom_router,
        },
    )
    .await
}

async fn list_routers(
    client: &ClientTestContext,
    vpc_name: &str,
) -> Vec<VpcRouter> {
    let routers_url =
        format!("/v1/vpc-routers?project={}&vpc={}", PROJECT_NAME, vpc_name);
    let out = objects_list_page_authz::<VpcRouter>(client, &routers_url).await;
    out.items
}

fn routers_eq(sn1: &VpcRouter, sn2: &VpcRouter) {
    identity_eq(&sn1.identity, &sn2.identity);
    assert_eq!(sn1.vpc_id, sn2.vpc_id);
}

#[nexus_test]
async fn test_vpc_router_networking_restrictions(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_test_utils::resource_helpers::{
        create_local_user, grant_iam, object_create, test_params,
    };
    use nexus_types::external_api::shared::SiloRole;
    use nexus_types::external_api::{params, shared, views};
    use omicron_common::api::external::RouterRoute;

    let client = &cptestctx.external_client;

    // Test Part 1: Create a restricted silo with networking restrictions enabled
    let restricted_silo_name = "router-restricted-silo";
    let silo_url = "/v1/system/silos";
    let silo_params = params::SiloCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_silo_name.parse().unwrap(),
            description: "Silo with router networking restrictions".to_string(),
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

    // Test Part 2: Create a user with Admin role (needed to create project with default VPC)
    let test_user = create_local_user(
        client,
        &restricted_silo,
        &"router-test-user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    let silo_policy_url =
        format!("/v1/system/silos/{}/policy", restricted_silo_name);
    let silo_url = format!("/v1/system/silos/{}", restricted_silo_name);

    // Grant the user Admin role first so they can create a project
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Create a project in the restricted silo AS THE SILO USER (who is currently Admin)
    let restricted_project_name = "router-restricted-project";
    let project_params = params::ProjectCreate {
        identity: IdentityMetadataCreateParams {
            name: restricted_project_name.parse().unwrap(),
            description: "Project in router restricted silo".to_string(),
        },
    };

    let _restricted_project: views::Project =
        NexusRequest::objects_post(&client, "/v1/projects", &project_params)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;

    // Test Part 3: Demote to Collaborator
    let silo_policy: shared::Policy<SiloRole> =
        NexusRequest::object_get(client, &silo_policy_url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("failed to fetch silo policy")
            .parsed_body()
            .expect("failed to parse silo policy");

    let test_user_uuid = test_user.id.into_untyped_uuid();

    // Add Collaborator and remove Admin
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

    // Test Part 4: Test VPC Router operations as Collaborator - CREATE, UPDATE, DELETE should all FAIL
    let restricted_routers_url = format!(
        "/v1/vpc-routers?project={}&vpc=default",
        restricted_project_name
    );

    // Try to CREATE a router as Collaborator - should FAIL
    let collab_router_params = params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-router".parse().unwrap(),
            description: "Collaborator creation attempt".to_string(),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &restricted_routers_url)
            .body(Some(&collab_router_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to create VPC router");

    // Try to UPDATE the system router as Collaborator - should FAIL
    // First, get the system router ID
    let routers_list: dropshot::ResultsPage<VpcRouter> =
        NexusRequest::object_get(client, &restricted_routers_url)
            .authn_as(AuthnMode::SiloUser(test_user.id))
            .execute_and_parse_unwrap()
            .await;
    let system_router = &routers_list.items[0];

    let router_update_url = format!("/v1/vpc-routers/{}", system_router.id());
    let router_update_params_collab = params::VpcRouterUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Collaborator update attempt".to_string()),
        },
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &router_update_url)
            .body(Some(&router_update_params_collab))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to update VPC router");

    // Test Part 5: Test as Admin - CREATE and UPDATE should SUCCEED
    // Grant the user Silo Admin role again
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // Try to CREATE a router as Admin - should SUCCEED
    let admin_router_params = params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: "admin-router".parse().unwrap(),
            description: "Router created by admin".to_string(),
        },
    };

    let created_router: VpcRouter = NexusRequest::objects_post(
        &client,
        &restricted_routers_url,
        &admin_router_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(created_router.identity.name, "admin-router");
    assert_eq!(created_router.identity.description, "Router created by admin");

    // Try to UPDATE the system router as Admin - should SUCCEED
    let router_update_params_admin = params::VpcRouterUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Admin update successful".to_string()),
        },
    };

    let updated_router: VpcRouter = NexusRequest::object_put(
        &client,
        &router_update_url,
        Some(&router_update_params_admin),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(updated_router.identity.description, "Admin update successful");

    // Try to DELETE the admin-created router as Admin - should SUCCEED
    let admin_router_url = format!("/v1/vpc-routers/{}", created_router.id());
    NexusRequest::object_delete(&client, &admin_router_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete VPC router");

    // Test Part 6: Test Router Routes - Collaborator should be blocked, Admin should succeed
    // Demote back to Collaborator
    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // Try to CREATE a route as Collaborator - should FAIL (but will fail with system router error first)
    // So we'll test this with a custom router instead
    // First create a custom router as Admin
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let custom_router_params = params::VpcRouterCreate {
        identity: IdentityMetadataCreateParams {
            name: "custom-router".parse().unwrap(),
            description: "Custom router for route testing".to_string(),
        },
    };

    let _custom_router: VpcRouter = NexusRequest::objects_post(
        &client,
        &restricted_routers_url,
        &custom_router_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    let custom_routes_url = format!(
        "/v1/vpc-router-routes?project={}&vpc=default&router=custom-router",
        restricted_project_name
    );

    // Create a route as Admin
    let test_route_params = params::RouterRouteCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-route".parse().unwrap(),
            description: "Route for testing".to_string(),
        },
        target: omicron_common::api::external::RouteTarget::Ip(
            "10.0.0.1".parse().unwrap(),
        ),
        destination: omicron_common::api::external::RouteDestination::IpNet(
            "192.168.0.0/24".parse().unwrap(),
        ),
    };

    let created_route: RouterRoute = NexusRequest::objects_post(
        &client,
        &custom_routes_url,
        &test_route_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    // Demote back to Collaborator
    NexusRequest::object_put(
        client,
        &silo_policy_url,
        Some(&collaborator_policy),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to update silo policy");

    // Try to CREATE a route as Collaborator - should FAIL
    let collab_route_params = params::RouterRouteCreate {
        identity: IdentityMetadataCreateParams {
            name: "collab-route".parse().unwrap(),
            description: "Collaborator route creation attempt".to_string(),
        },
        target: omicron_common::api::external::RouteTarget::Ip(
            "10.0.0.2".parse().unwrap(),
        ),
        destination: omicron_common::api::external::RouteDestination::IpNet(
            "192.168.1.0/24".parse().unwrap(),
        ),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &custom_routes_url)
            .body(Some(&collab_route_params))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to create router route");

    // Try to UPDATE a route as Collaborator - should FAIL
    let route_update_url = format!(
        "/v1/vpc-router-routes/test-route?project={}&vpc=default&router=custom-router",
        restricted_project_name
    );
    let route_update_params_collab = params::RouterRouteUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Collaborator route update attempt".to_string()),
        },
        target: created_route.target.clone(),
        destination: created_route.destination.clone(),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &route_update_url)
            .body(Some(&route_update_params_collab))
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to update router route");

    // Try to DELETE a route as Collaborator - should FAIL
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &route_update_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute()
    .await
    .expect("Collaborator should not be able to delete router route");

    // Test Part 7: Route operations as Admin - should all SUCCEED
    grant_iam(
        client,
        &silo_url,
        SiloRole::Admin,
        test_user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    // CREATE route as Admin - should SUCCEED
    let admin_route_params = params::RouterRouteCreate {
        identity: IdentityMetadataCreateParams {
            name: "admin-route".parse().unwrap(),
            description: "Route created by admin".to_string(),
        },
        target: omicron_common::api::external::RouteTarget::Ip(
            "10.0.0.3".parse().unwrap(),
        ),
        destination: omicron_common::api::external::RouteDestination::IpNet(
            "192.168.2.0/24".parse().unwrap(),
        ),
    };

    let admin_created_route: RouterRoute = NexusRequest::objects_post(
        &client,
        &custom_routes_url,
        &admin_route_params,
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(admin_created_route.identity.name, "admin-route");

    // UPDATE route as Admin - should SUCCEED
    let route_update_params_admin = params::RouterRouteUpdate {
        identity: IdentityMetadataUpdateParams {
            name: None,
            description: Some("Admin route update successful".to_string()),
        },
        target: created_route.target,
        destination: created_route.destination,
    };

    let updated_route: RouterRoute = NexusRequest::object_put(
        &client,
        &route_update_url,
        Some(&route_update_params_admin),
    )
    .authn_as(AuthnMode::SiloUser(test_user.id))
    .execute_and_parse_unwrap()
    .await;

    assert_eq!(
        updated_route.identity.description,
        "Admin route update successful"
    );

    // DELETE route as Admin - should SUCCEED
    NexusRequest::object_delete(&client, &route_update_url)
        .authn_as(AuthnMode::SiloUser(test_user.id))
        .execute()
        .await
        .expect("Admin should be able to delete router route");
}
