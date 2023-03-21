// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::ACTION_GENERATE_ID;
use crate::app::sagas::declare_saga_actions;
use crate::db::queries::vpc_subnet::SubnetError;
use crate::external_api::params;
use crate::{authn, authz, db};
use nexus_defaults as defaults;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteKind;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// vpc create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub vpc_create: params::VpcCreate,
    pub authz_project: authz::Project,
}

// vpc create saga: actions

declare_saga_actions! {
    vpc_create;
    VPC_CREATE_VPC -> "vpc" {
        + svc_create_vpc
        - svc_create_vpc_undo
    }
    VPC_CREATE_ROUTER -> "router" {
        + svc_create_router
        - svc_create_router_undo
    }
    VPC_CREATE_ROUTE -> "route" {
        + svc_create_route
        - svc_create_route_undo
    }
    VPC_CREATE_SUBNET -> "subnet" {
        + svc_create_subnet
        - svc_create_subnet_undo
    }
    VPC_UPDATE_FIREWALL -> "firewall" {
        + svc_update_firewall
        - svc_update_firewall_undo
    }
    VPC_NOTIFY_SLEDS -> "no_result" {
        + svc_notify_sleds
    }
}

// vpc create saga: definition

/// Identical to [SagaVpcCreate::make_saga_dag], but using types
/// to identify that parameters do not need to be supplied as input.
pub fn create_dag(
    mut builder: steno::DagBuilder,
) -> Result<steno::Dag, super::SagaInitError> {
    builder.append(Node::action(
        "vpc_id",
        "GenerateVpcId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "system_router_id",
        "GenerateSystemRouterId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "default_route_id",
        "GenerateDefaultRouteId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(Node::action(
        "default_subnet_id",
        "GenerateDefaultSubnetId",
        ACTION_GENERATE_ID.as_ref(),
    ));
    builder.append(vpc_create_vpc_action());
    builder.append(vpc_create_router_action());
    builder.append(vpc_create_route_action());
    builder.append(vpc_create_subnet_action());
    builder.append(vpc_update_firewall_action());
    builder.append(vpc_notify_sleds_action());

    Ok(builder.build()?)
}

#[derive(Debug)]
pub struct SagaVpcCreate;
impl NexusSaga for SagaVpcCreate {
    const NAME: &'static str = "vpc-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        vpc_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        create_dag(builder)
    }
}

// vpc create saga: action implementations

async fn svc_create_vpc(
    sagactx: NexusActionContext,
) -> Result<(authz::Vpc, db::model::Vpc), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;

    let vpc = db::model::IncompleteVpc::new(
        vpc_id,
        params.authz_project.id(),
        system_router_id,
        params.vpc_create.clone(),
    )
    .map_err(ActionError::action_failed)?;
    let (authz_vpc, db_vpc) = osagactx
        .datastore()
        .project_create_vpc(&opctx, &params.authz_project, vpc)
        .await
        .map_err(ActionError::action_failed)?;
    Ok((authz_vpc, db_vpc))
}

async fn svc_create_vpc_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (authz_vpc, db_vpc) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    osagactx
        .datastore()
        .project_delete_vpc(&opctx, &db_vpc, &authz_vpc)
        .await?;
    Ok(())
}

async fn svc_create_router(
    sagactx: NexusActionContext,
) -> Result<authz::VpcRouter, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;
    let (authz_vpc, _) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;

    // TODO: Ultimately when the VPC is created a system router w/ an
    // appropriate setup should also be created.  Given that the underlying
    // systems aren't wired up yet this is a naive implementation to
    // populate the database with a starting router.
    let router = db::model::VpcRouter::new(
        system_router_id,
        vpc_id,
        db::model::VpcRouterKind::System,
        params::VpcRouterCreate {
            identity: IdentityMetadataCreateParams {
                name: "system".parse().unwrap(),
                description: "Routes are automatically added to this \
                    router as vpc subnets are created"
                    .into(),
            },
        },
    );
    let (authz_router, _) = osagactx
        .datastore()
        .vpc_create_router(&opctx, &authz_vpc, router)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(authz_router)
}

async fn svc_create_router_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let authz_router = sagactx.lookup::<authz::VpcRouter>("router")?;

    osagactx.datastore().vpc_delete_router(&opctx, &authz_router).await?;
    Ok(())
}

async fn svc_create_route(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let default_route_id = sagactx.lookup::<Uuid>("default_route_id")?;
    let system_router_id = sagactx.lookup::<Uuid>("system_router_id")?;
    let authz_router = sagactx.lookup::<authz::VpcRouter>("router")?;

    let route = db::model::RouterRoute::new(
        default_route_id,
        system_router_id,
        RouterRouteKind::Default,
        params::RouterRouteCreate {
            identity: IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: "The default route of a vpc".to_string(),
            },
            target: RouteTarget::InternetGateway("outbound".parse().unwrap()),
            destination: RouteDestination::Vpc(
                params.vpc_create.identity.name.clone(),
            ),
        },
    );

    osagactx
        .datastore()
        .router_create_route(&opctx, &authz_router, route)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn svc_create_route_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let authz_router = sagactx.lookup::<authz::VpcRouter>("router")?;
    let route_id = sagactx.lookup::<Uuid>("default_route_id")?;
    let authz_route = authz::RouterRoute::new(
        authz_router,
        route_id,
        LookupType::ById(route_id),
    );
    osagactx.datastore().router_delete_route(&opctx, &authz_route).await?;
    Ok(())
}

async fn svc_create_subnet(
    sagactx: NexusActionContext,
) -> Result<(authz::VpcSubnet, db::model::VpcSubnet), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let vpc_id = sagactx.lookup::<Uuid>("vpc_id")?;
    let (authz_vpc, db_vpc) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let default_subnet_id = sagactx.lookup::<Uuid>("default_subnet_id")?;

    // Allocate the first /64 sub-range from the requested or created
    // prefix.
    let ipv6_block = external::Ipv6Net(
        ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
            .map_err(|_| {
                external::Error::internal_error(
                    "Failed to allocate default IPv6 subnet",
                )
            })
            .map_err(ActionError::action_failed)?,
    );

    let subnet = db::model::VpcSubnet::new(
        default_subnet_id,
        vpc_id,
        IdentityMetadataCreateParams {
            name: "default".parse().unwrap(),
            description: format!(
                "The default subnet for {}",
                params.vpc_create.identity.name
            ),
        },
        *defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
        ipv6_block,
    );

    // Create the subnet record in the database. Overlapping IP ranges
    // should be translated into an internal error. That implies that
    // there's already an existing VPC Subnet, but we're explicitly creating
    // the _first_ VPC in the project. Something is wrong, and likely a bug
    // in our code.
    osagactx
        .datastore()
        .vpc_create_subnet(&opctx, &authz_vpc, subnet)
        .await
        .map_err(|err| match err {
            SubnetError::OverlappingIpRange(ip) => {
                let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                let log = sagactx.user_data().log();
                error!(
                    log,
                    concat!(
                        "failed to create default VPC Subnet, IP address ",
                        "range '{}' overlaps with existing",
                    ),
                    ip;
                    "vpc_id" => ?vpc_id,
                    "subnet_id" => ?default_subnet_id,
                    "ipv4_block" => ?**ipv4_block,
                    "ipv6_block" => ?ipv6_block,
                );
                external::Error::internal_error(
                    "Failed to create default VPC Subnet, \
                        found overlapping IP address ranges",
                )
            }
            SubnetError::External(e) => e,
        })
        .map_err(ActionError::action_failed)
}

async fn svc_create_subnet_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (authz_subnet, db_subnet) =
        sagactx.lookup::<(authz::VpcSubnet, db::model::VpcSubnet)>("subnet")?;

    osagactx
        .datastore()
        .vpc_delete_subnet(&opctx, &db_subnet, &authz_subnet)
        .await?;
    Ok(())
}

async fn svc_update_firewall(
    sagactx: NexusActionContext,
) -> Result<Vec<db::model::VpcFirewallRule>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (authz_vpc, _) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let rules = osagactx
        .nexus()
        .default_firewall_rules_for_vpc(
            authz_vpc.id(),
            params.vpc_create.identity.name.clone().into(),
        )
        .await
        .map_err(ActionError::action_failed)?;
    osagactx
        .datastore()
        .vpc_update_firewall_rules(&opctx, &authz_vpc, rules.clone())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(rules)
}

async fn svc_update_firewall_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (authz_vpc, _) =
        sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    osagactx
        .datastore()
        .vpc_update_firewall_rules(&opctx, &authz_vpc, vec![])
        .await?;
    Ok(())
}

async fn svc_notify_sleds(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let (_, db_vpc) = sagactx.lookup::<(authz::Vpc, db::model::Vpc)>("vpc")?;
    let rules =
        sagactx.lookup::<Vec<db::model::VpcFirewallRule>>("firewall")?;

    osagactx
        .nexus()
        .send_sled_agents_firewall_rules(&opctx, &db_vpc, &rules)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::vpc_create::Params,
        app::sagas::vpc_create::SagaVpcCreate, authn::saga::Serialized, authz,
        db::datastore::DataStore, db::lookup::LookupPath, external_api::params,
    };
    use async_bb8_diesel::{AsyncRunQueryDsl, OptionalExtension};
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::populate_ip_pool;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_common::api::external::NameOrId;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let project = create_project(client, PROJECT_NAME).await;
        project.identity.id
    }

    // Helper for creating VPC create parameters
    fn new_test_params(
        opctx: &OpContext,
        authz_project: authz::Project,
    ) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            vpc_create: params::VpcCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-vpc".parse().unwrap(),
                    description: "My VPC".to_string(),
                },
                ipv6_prefix: None,
                dns_name: "abc".parse().unwrap(),
            },
            authz_project,
        }
    }

    fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx().nexus.datastore().clone(),
        )
    }

    async fn get_authz_project(
        cptestctx: &ControlPlaneTestContext,
        project_id: Uuid,
        action: authz::Action,
    ) -> authz::Project {
        let nexus = &cptestctx.server.apictx().nexus;
        let project_selector =
            params::ProjectSelector { project: NameOrId::Id(project_id) };
        let opctx = test_opctx(&cptestctx);
        let (.., authz_project) = nexus
            .project_lookup(&opctx, &project_selector)
            .expect("Invalid parameters constructing project lookup")
            .lookup_for(action)
            .await
            .expect("Project does not exist");
        authz_project
    }

    async fn delete_project_vpc_defaults(
        cptestctx: &ControlPlaneTestContext,
        project_id: Uuid,
    ) {
        let opctx = test_opctx(&cptestctx);
        let datastore = cptestctx.server.apictx().nexus.datastore();
        let default_name = Name::try_from("default".to_string()).unwrap();
        let system_name = Name::try_from("system".to_string()).unwrap();

        // Default Subnet
        let (.., authz_subnet, subnet) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.clone().into())
            .vpc_subnet_name(&default_name.clone().into())
            .fetch()
            .await
            .expect("Failed to fetch default Subnet");
        datastore
            .vpc_delete_subnet(&opctx, &subnet, &authz_subnet)
            .await
            .expect("Failed to delete default Subnet");

        // Default route
        let (.., authz_route, _route) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.clone().into())
            .vpc_router_name(&system_name.clone().into())
            .router_route_name(&default_name.clone().into())
            .fetch()
            .await
            .expect("Failed to fetch default route");
        datastore
            .router_delete_route(&opctx, &authz_route)
            .await
            .expect("Failed to delete default route");

        // System router
        let (.., authz_router, _router) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.clone().into())
            .vpc_router_name(&system_name.into())
            .fetch()
            .await
            .expect("Failed to fetch system router");
        datastore
            .vpc_delete_router(&opctx, &authz_router)
            .await
            .expect("Failed to delete system router");

        // Default VPC & Firewall Rules
        let (.., authz_vpc, vpc) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.into())
            .fetch()
            .await
            .expect("Failed to fetch default VPC");
        datastore
            .vpc_delete_all_firewall_rules(&opctx, &authz_vpc)
            .await
            .expect("Failed to delete all firewall rules for VPC");
        datastore
            .project_delete_vpc(&opctx, &vpc, &authz_vpc)
            .await
            .expect("Failed to delete VPC");
    }

    pub(crate) async fn verify_clean_slate(datastore: &DataStore) {
        assert!(no_vpcs_exist(datastore).await);
        assert!(no_routers_exist(datastore).await);
        assert!(no_routes_exist(datastore).await);
        assert!(no_subnets_exist(datastore).await);
        assert!(no_firewall_rules_exist(datastore).await);
    }

    async fn no_vpcs_exist(datastore: &DataStore) -> bool {
        use crate::db::model::Vpc;
        use crate::db::schema::vpc::dsl;

        dsl::vpc
            .filter(dsl::time_deleted.is_null())
            .select(Vpc::as_select())
            .first_async::<Vpc>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .map(|vpc| {
                eprintln!("VPC exists: {vpc:?}");
            })
            .is_none()
    }

    async fn no_routers_exist(datastore: &DataStore) -> bool {
        use crate::db::model::VpcRouter;
        use crate::db::schema::vpc_router::dsl;

        dsl::vpc_router
            .filter(dsl::time_deleted.is_null())
            .select(VpcRouter::as_select())
            .first_async::<VpcRouter>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .map(|router| {
                eprintln!("Router exists: {router:?}");
            })
            .is_none()
    }

    async fn no_routes_exist(datastore: &DataStore) -> bool {
        use crate::db::model::RouterRoute;
        use crate::db::schema::router_route::dsl;

        dsl::router_route
            .filter(dsl::time_deleted.is_null())
            .select(RouterRoute::as_select())
            .first_async::<RouterRoute>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|route| {
                eprintln!("Route exists: {route:?}");
            })
            .is_none()
    }

    async fn no_subnets_exist(datastore: &DataStore) -> bool {
        use crate::db::model::VpcSubnet;
        use crate::db::schema::vpc_subnet::dsl;

        dsl::vpc_subnet
            .filter(dsl::time_deleted.is_null())
            .select(VpcSubnet::as_select())
            .first_async::<VpcSubnet>(datastore.pool_for_tests().await.unwrap())
            .await
            .optional()
            .unwrap()
            .map(|subnet| {
                eprintln!("Subnet exists: {subnet:?}");
            })
            .is_none()
    }

    async fn no_firewall_rules_exist(datastore: &DataStore) -> bool {
        use crate::db::model::VpcFirewallRule;
        use crate::db::schema::vpc_firewall_rule::dsl;

        dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            .select(VpcFirewallRule::as_select())
            .first_async::<VpcFirewallRule>(
                datastore.pool_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|rule| {
                eprintln!("Firewall rule exists: {rule:?}");
            })
            .is_none()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_and_project(&client).await;
        delete_project_vpc_defaults(&cptestctx, project_id).await;

        // Before running the test, confirm we have no records of any VPCs.
        verify_clean_slate(nexus.datastore()).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let authz_project = get_authz_project(
            &cptestctx,
            project_id,
            authz::Action::CreateChild,
        )
        .await;
        let params = new_test_params(&opctx, authz_project);
        let dag = create_saga_dag::<SagaVpcCreate>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        nexus.run_saga(runnable_saga).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let project_id = create_org_and_project(&client).await;
        delete_project_vpc_defaults(&cptestctx, project_id).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let authz_project = get_authz_project(
            &cptestctx,
            project_id,
            authz::Action::CreateChild,
        )
        .await;
        let params = new_test_params(&opctx, authz_project);
        let dag = create_saga_dag::<SagaVpcCreate>(params).unwrap();

        for node in dag.get_nodes() {
            // Create a new saga for this node.
            info!(
                log,
                "Creating new saga which will fail at index {:?}", node.index();
                "node_name" => node.name().as_ref(),
                "label" => node.label(),
            );

            let runnable_saga =
                nexus.create_runnable_saga(dag.clone()).await.unwrap();

            // Inject an error instead of running the node.
            //
            // This should cause the saga to unwind.
            nexus
                .sec()
                .saga_inject_error(runnable_saga.id(), node.index())
                .await
                .unwrap();
            nexus
                .run_saga(runnable_saga)
                .await
                .expect_err("Saga should have failed");

            verify_clean_slate(nexus.datastore()).await;
        }
    }
}
