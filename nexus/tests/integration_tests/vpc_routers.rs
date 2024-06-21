// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::integration_tests::instances::assert_sled_vpc_routes;
use crate::integration_tests::instances::instance_simulate;
use dropshot::test_util::ClientTestContext;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
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
use omicron_common::api::external::SimpleIdentity;
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
    assert_eq!(err.message, "router and subnet must belong to the same VPC");

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
                },
            ]),
            vec![],
            vec![],
            true,
        )
        .await;
        instance_simulate(
            nexus,
            &InstanceUuid::from_untyped_uuid(instance.identity.id),
        )
        .await;

        let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
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

    let sled_agent = &cptestctx.sled_agent.sled_agent;

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
        target: RouterTarget::Drop
    }));
    assert!(last_routes[1].1.contains(&ResolvedVpcRoute {
        dest: "241.0.0.0/8".parse().unwrap(),
        target: RouterTarget::Drop
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
        target: RouterTarget::Ip(instance_nics[INSTANCE_NAMES[1]][0].ip)
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
