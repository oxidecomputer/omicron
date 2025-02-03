// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::SagaInitError;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// vpc subnet delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_vpc: authz::Vpc,
    pub authz_subnet: authz::VpcSubnet,
    pub db_subnet: db::model::VpcSubnet,
}

// vpc subnet delete saga: actions

declare_saga_actions! {
    vpc_subnet_delete;
    VPC_SUBNET_DELETE_SUBNET -> "no_result_0" {
        + svsd_delete_subnet
    }
    VPC_SUBNET_DELETE_SYS_ROUTE -> "no_result_1" {
        + svsd_delete_route
    }
    VPC_NOTIFY_RPW -> "no_result_2" {
        + svsd_notify_rpw
    }
}

// vpc subnet delete saga: definition

/// Identical to [SagaVpcSubnetDelete::make_saga_dag], but using types
/// to identify that parameters do not need to be supplied as input.
pub fn create_dag(
    mut builder: steno::DagBuilder,
) -> Result<steno::Dag, SagaInitError> {
    builder.append(vpc_subnet_delete_subnet_action());
    builder.append(vpc_subnet_delete_sys_route_action());
    builder.append(vpc_notify_rpw_action());

    Ok(builder.build()?)
}

#[derive(Debug)]
pub(crate) struct SagaVpcSubnetDelete;
impl NexusSaga for SagaVpcSubnetDelete {
    const NAME: &'static str = "vpc-subnet-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        vpc_subnet_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        create_dag(builder)
    }
}

// vpc subnet delete saga: action implementations

async fn svsd_delete_subnet(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .vpc_delete_subnet(&opctx, &params.db_subnet, &params.authz_subnet)
        .await
        .map_err(ActionError::action_failed)
}

async fn svsd_delete_route(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .vpc_delete_subnet_route(&opctx, &params.authz_subnet)
        .await
        .map_err(ActionError::action_failed)
}

async fn svsd_notify_rpw(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    osagactx
        .datastore()
        .vpc_increment_rpw_version(&opctx, params.authz_vpc.id())
        .await
        .map_err(ActionError::action_failed)
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::sagas::vpc_create::Params, app::sagas::vpc_create::SagaVpcCreate,
        external_api::params,
    };
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::db::fixed_data::vpc::SERVICES_INTERNET_GATEWAY_ID;
    use nexus_db_queries::{
        authn::saga::Serialized, authz, context::OpContext,
        db::datastore::DataStore, db::fixed_data::vpc::SERVICES_VPC_ID,
        db::lookup::LookupPath,
    };
    use nexus_test_utils::resource_helpers::create_default_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_common::api::external::NameOrId;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(&client).await;
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
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    async fn get_authz_project(
        cptestctx: &ControlPlaneTestContext,
        project_id: Uuid,
        action: authz::Action,
    ) -> authz::Project {
        let nexus = &cptestctx.server.server_context().nexus;
        let project_selector =
            params::ProjectSelector { project: NameOrId::Id(project_id) };
        let opctx = test_opctx(&cptestctx);
        let (.., authz_project) = nexus
            .project_lookup(&opctx, project_selector)
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
        let datastore = cptestctx.server.server_context().nexus.datastore();
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

        // Default gateway routes
        let (.., authz_route, _route) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.clone().into())
            .vpc_router_name(&system_name.clone().into())
            .router_route_name(&"default-v4".parse::<Name>().unwrap().into())
            .fetch()
            .await
            .expect("Failed to fetch default route");
        datastore
            .router_delete_route(&opctx, &authz_route)
            .await
            .expect("Failed to delete default route");

        let (.., authz_route, _route) = LookupPath::new(&opctx, &datastore)
            .project_id(project_id)
            .vpc_name(&default_name.clone().into())
            .vpc_router_name(&system_name.clone().into())
            .router_route_name(&"default-v6".parse::<Name>().unwrap().into())
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

        // Default gateway
        let (.., authz_vpc, authz_igw, _igw) =
            LookupPath::new(&opctx, &datastore)
                .project_id(project_id)
                .vpc_name(&default_name.clone().into())
                .internet_gateway_name(&default_name.clone().into())
                .fetch()
                .await
                .expect("Failed to fetch default gateway");
        datastore
            .vpc_delete_internet_gateway(
                &opctx,
                &authz_igw,
                authz_vpc.id(),
                true,
            )
            .await
            .expect("Failed to delete default gateway");

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
        assert!(no_gateways_exist(datastore).await);
        assert!(no_gateway_links_exist(datastore).await);
        assert!(no_firewall_rules_exist(datastore).await);
    }

    async fn no_vpcs_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::Vpc;
        use nexus_db_queries::db::schema::vpc::dsl;

        dsl::vpc
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(dsl::id.ne(*SERVICES_VPC_ID))
            .select(Vpc::as_select())
            .first_async::<Vpc>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|vpc| {
                eprintln!("VPC exists: {vpc:?}");
            })
            .is_none()
    }

    async fn no_routers_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::VpcRouter;
        use nexus_db_queries::db::schema::vpc_router::dsl;

        dsl::vpc_router
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(dsl::vpc_id.ne(*SERVICES_VPC_ID))
            .select(VpcRouter::as_select())
            .first_async::<VpcRouter>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|router| {
                eprintln!("Router exists: {router:?}");
            })
            .is_none()
    }

    async fn no_gateways_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::InternetGateway;
        use nexus_db_queries::db::schema::internet_gateway::dsl;

        dsl::internet_gateway
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(dsl::vpc_id.ne(*SERVICES_VPC_ID))
            .select(InternetGateway::as_select())
            .first_async::<InternetGateway>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|igw| {
                eprintln!("Internet gateway exists: {igw:?}");
            })
            .is_none()
    }

    async fn no_gateway_links_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::InternetGatewayIpPool;
        use nexus_db_queries::db::schema::internet_gateway_ip_pool::dsl;

        dsl::internet_gateway_ip_pool
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::internet_gateway_id.ne(*SERVICES_INTERNET_GATEWAY_ID))
            .select(InternetGatewayIpPool::as_select())
            .first_async::<InternetGatewayIpPool>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|igw_ip_pool| {
                eprintln!(
                    "Internet gateway ip pool links exists: {igw_ip_pool:?}"
                );
            })
            .is_none()
    }

    async fn no_routes_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::RouterRoute;
        use nexus_db_queries::db::schema::router_route::dsl;
        use nexus_db_queries::db::schema::vpc_router::dsl as vpc_router_dsl;

        dsl::router_route
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(
                dsl::vpc_router_id.ne_all(
                    vpc_router_dsl::vpc_router
                        .select(vpc_router_dsl::id)
                        .filter(vpc_router_dsl::vpc_id.eq(*SERVICES_VPC_ID))
                        .filter(vpc_router_dsl::time_deleted.is_null()),
                ),
            )
            .select(RouterRoute::as_select())
            .first_async::<RouterRoute>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
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
        use nexus_db_queries::db::model::VpcSubnet;
        use nexus_db_queries::db::schema::vpc_subnet::dsl;

        dsl::vpc_subnet
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(dsl::vpc_id.ne(*SERVICES_VPC_ID))
            .select(VpcSubnet::as_select())
            .first_async::<VpcSubnet>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|subnet| {
                eprintln!("Subnet exists: {subnet:?}");
            })
            .is_none()
    }

    async fn no_firewall_rules_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::model::VpcFirewallRule;
        use nexus_db_queries::db::schema::vpc_firewall_rule::dsl;

        dsl::vpc_firewall_rule
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services VPC
            .filter(dsl::vpc_id.ne(*SERVICES_VPC_ID))
            .select(VpcFirewallRule::as_select())
            .first_async::<VpcFirewallRule>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
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
        let nexus = &cptestctx.server.server_context().nexus;
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
        nexus.sagas.saga_execute::<SagaVpcCreate>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_org_and_project(&client).await;
        delete_project_vpc_defaults(&cptestctx, project_id).await;

        let opctx = test_opctx(&cptestctx);
        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaVpcCreate,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin(async {
                    new_test_params(
                        &opctx,
                        get_authz_project(
                            &cptestctx,
                            project_id,
                            authz::Action::CreateChild,
                        )
                        .await,
                    )
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(nexus.datastore()).await;
                })
            },
            log,
        )
        .await;
    }
}
