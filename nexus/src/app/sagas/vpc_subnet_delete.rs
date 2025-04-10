// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use omicron_common::api::external;
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
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(vpc_subnet_delete_subnet_action());
        builder.append(vpc_subnet_delete_sys_route_action());
        builder.append(vpc_notify_rpw_action());

        Ok(builder.build()?)
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

    let res = osagactx
        .datastore()
        .vpc_delete_subnet_raw(&opctx, &params.db_subnet, &params.authz_subnet)
        .await;

    match res {
        Ok(_) | Err(external::Error::ObjectNotFound { .. }) => Ok(()),
        Err(e) => Err(ActionError::action_failed(e)),
    }
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
    use crate::app::saga::create_saga_dag;
    use crate::app::sagas::test_helpers;
    use crate::{
        app::sagas::vpc_subnet_delete::Params,
        app::sagas::vpc_subnet_delete::SagaVpcSubnetDelete,
        external_api::params,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::{authn::saga::Serialized, context::OpContext};
    use nexus_test_utils::resource_helpers::create_default_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::NameOrId;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        create_default_ip_pool(client).await;
        let project = create_project(client, PROJECT_NAME).await;
        project.identity.id
    }

    fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    async fn new_test_params(
        cptestctx: &ControlPlaneTestContext,
        project_id: Uuid,
    ) -> Params {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(cptestctx);
        let (.., authz_vpc, authz_subnet, db_subnet) = nexus
            .vpc_subnet_lookup(
                &opctx,
                params::SubnetSelector {
                    project: Some(project_id.into()),
                    vpc: Some(NameOrId::Name("default".parse().unwrap())),
                    subnet: NameOrId::Name("default".parse().unwrap()),
                },
            )
            .unwrap()
            .fetch()
            .await
            .unwrap();

        Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            authz_vpc,
            authz_subnet,
            db_subnet,
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_org_and_project(client).await;

        let params = new_test_params(cptestctx, project_id).await;
        nexus.sagas.saga_execute::<SagaVpcSubnetDelete>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_org_and_project(client).await;

        let params = new_test_params(cptestctx, project_id).await;
        let dag = create_saga_dag::<SagaVpcSubnetDelete>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;
    }
}
