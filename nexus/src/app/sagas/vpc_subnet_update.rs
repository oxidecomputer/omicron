// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_model::VpcSubnet;
use nexus_db_queries::{authn, authz, db};
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// vpc subnet update saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_vpc: authz::Vpc,
    pub authz_subnet: authz::VpcSubnet,
    pub custom_router: Option<authz::VpcRouter>,
    pub update: db::model::VpcSubnetUpdate,
}

// vpc subnet update saga: actions

declare_saga_actions! {
    vpc_subnet_update;
    VPC_SUBNET_UPDATE -> "output" {
        + svsu_do_update
    }
    VPC_NOTIFY_RPW -> "no_result" {
        + svsu_notify_rpw
    }
}

// vpc subnet update saga: definition

#[derive(Debug)]
pub(crate) struct SagaVpcSubnetUpdate;
impl NexusSaga for SagaVpcSubnetUpdate {
    const NAME: &'static str = "vpc-subnet-update";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        vpc_subnet_update_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(vpc_subnet_update_action());
        builder.append(vpc_notify_rpw_action());

        Ok(builder.build()?)
    }
}

// vpc subnet update saga: action implementations

async fn svsu_do_update(
    sagactx: NexusActionContext,
) -> Result<VpcSubnet, ActionError> {
    let osagactx = sagactx.user_data();
    let Params {
        serialized_authn, authz_subnet, custom_router, update, ..
    } = sagactx.saga_params::<Params>()?;
    let opctx =
        crate::context::op_context_for_saga_action(&sagactx, &serialized_authn);

    osagactx
        .datastore()
        .vpc_update_subnet(
            &opctx,
            &authz_subnet,
            custom_router.as_ref(),
            update,
        )
        .await
        .map_err(ActionError::action_failed)
}

async fn svsu_notify_rpw(
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
        app::sagas::vpc_subnet_update::Params,
        app::sagas::vpc_subnet_update::SagaVpcSubnetUpdate,
        external_api::params,
    };
    use chrono::Utc;
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::db;
    use nexus_db_queries::{authn::saga::Serialized, context::OpContext};
    use nexus_test_utils::resource_helpers::create_default_ip_pool;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_router;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::NameOrId;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "springfield-squidport";
    const ROUTER_NAME: &str = "test-router";

    async fn create_org_and_project(
        client: &ClientTestContext,
    ) -> (Uuid, Uuid) {
        create_default_ip_pool(&client).await;
        let project = create_project(client, PROJECT_NAME).await;
        let router =
            create_router(client, PROJECT_NAME, "default", ROUTER_NAME).await;
        (project.identity.id, router.identity.id)
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
        router_id: Uuid,
    ) -> Params {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);
        let (.., authz_vpc, authz_subnet, _) = nexus
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

        let (.., custom_router, _) = nexus
            .vpc_router_lookup(
                &opctx,
                params::RouterSelector {
                    project: None,
                    vpc: None,
                    router: NameOrId::Id(router_id),
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
            custom_router: Some(custom_router),
            update: db::model::VpcSubnetUpdate {
                name: Some(db::model::Name("defaulter".parse().unwrap())),
                description: None,
                time_modified: Utc::now(),
            },
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let (project_id, router_id) = create_org_and_project(&client).await;

        let params = new_test_params(cptestctx, project_id, router_id).await;
        nexus.sagas.saga_execute::<SagaVpcSubnetUpdate>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let (project_id, router_id) = create_org_and_project(&client).await;

        let params = new_test_params(cptestctx, project_id, router_id).await;
        let dag = create_saga_dag::<SagaVpcSubnetUpdate>(params).unwrap();
        test_helpers::actions_succeed_idempotently(nexus, dag).await;
    }
}
