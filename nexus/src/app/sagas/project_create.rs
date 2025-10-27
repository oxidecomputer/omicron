// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use crate::external_api::params;
use nexus_db_queries::{authn, authz, db};
use nexus_defaults as defaults;
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// project create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_create: params::ProjectCreate,
    pub authz_silo: authz::Silo,
}

// project create saga: actions

declare_saga_actions! {
    project_create;
    PROJECT_CREATE_RECORD -> "project" {
        + spc_create_record
        - spc_create_record_undo
    }
    PROJECT_CREATE_VPC_PARAMS -> "vpc_create_params" {
        + spc_create_vpc_params
    }
}

// project create saga: definition

#[derive(Debug)]
pub(crate) struct SagaProjectCreate;
impl NexusSaga for SagaProjectCreate {
    const NAME: &'static str = "project-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        project_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(project_create_record_action());
        builder.append(project_create_vpc_params_action());

        let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
            sagas::vpc_create::SagaVpcCreate::NAME,
        ));
        builder.append(steno::Node::subsaga(
            "vpc",
            sagas::vpc_create::create_dag(subsaga_builder)?,
            "vpc_create_params",
        ));
        Ok(builder.build()?)
    }
}

// project create saga: action implementations

async fn spc_create_record(
    sagactx: NexusActionContext,
) -> Result<(authz::Project, db::model::Project), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let db_project =
        db::model::Project::new(params.authz_silo.id(), params.project_create);
    osagactx
        .datastore()
        .project_create(&opctx, db_project)
        .await
        .map_err(ActionError::action_failed)
}

async fn spc_create_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., project) =
        sagactx.lookup::<(authz::Project, db::model::Project)>("project")?;

    let (.., authz_project, project) =
        nexus_db_lookup::LookupPath::new(&opctx, osagactx.datastore())
            .project_id(project.id())
            .fetch_for(authz::Action::Delete)
            .await?;

    osagactx
        .datastore()
        .project_delete(&opctx, &authz_project, &project)
        .await?;
    Ok(())
}

async fn spc_create_vpc_params(
    sagactx: NexusActionContext,
) -> Result<sagas::vpc_create::Params, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (authz_project, _project) =
        sagactx.lookup::<(authz::Project, db::model::Project)>("project")?;
    let ipv6_prefix = Some(
        defaults::random_vpc_ipv6_prefix()
            .map_err(ActionError::action_failed)?,
    );

    let vpc_create = params::VpcCreate {
        identity: IdentityMetadataCreateParams {
            name: "default".parse().unwrap(),
            description: "Default VPC".to_string(),
        },
        ipv6_prefix,
        // TODO-robustness this will need to be None if we decide to
        // handle the logic around name and dns_name by making
        // dns_name optional
        dns_name: "default".parse().unwrap(),
    };
    let saga_params = sagas::vpc_create::Params {
        serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
        vpc_create,
        authz_project,
    };
    Ok(saga_params)
}

#[cfg(test)]
mod test {
    use crate::{
        app::sagas::project_create::Params,
        app::sagas::project_create::SagaProjectCreate, external_api::params,
    };
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use diesel::{
        ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
    };
    use nexus_db_queries::{
        authn::saga::Serialized, authz, context::OpContext,
        db::datastore::DataStore,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Helper for creating project create parameters
    fn new_test_params(opctx: &OpContext, authz_silo: authz::Silo) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            project_create: params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-project".parse().unwrap(),
                    description: "My Project".to_string(),
                },
            },
            authz_silo,
        }
    }

    fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    async fn verify_clean_slate(datastore: &DataStore) {
        assert!(no_projects_exist(datastore).await);
        assert!(
            no_virtual_provisioning_collection_records_for_projects(datastore)
                .await
        );
        crate::app::sagas::vpc_create::test::verify_clean_slate(datastore)
            .await;
    }

    async fn no_projects_exist(datastore: &DataStore) -> bool {
        use nexus_db_queries::db::fixed_data::project::SERVICES_PROJECT_ID;
        use nexus_db_queries::db::model::Project;
        use nexus_db_schema::schema::project::dsl;

        dsl::project
            .filter(dsl::time_deleted.is_null())
            // ignore built-in services project
            .filter(dsl::id.ne(*SERVICES_PROJECT_ID))
            .select(Project::as_select())
            .first_async::<Project>(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .optional()
            .unwrap()
            .map(|project| {
                eprintln!("Project exists: {project:?}");
            })
            .is_none()
    }

    async fn no_virtual_provisioning_collection_records_for_projects(
        datastore: &DataStore,
    ) -> bool {
        use nexus_db_queries::db::fixed_data::project::SERVICES_PROJECT_ID;
        use nexus_db_queries::db::model::VirtualProvisioningCollection;
        use nexus_db_schema::schema::virtual_provisioning_collection::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .transaction_retry_wrapper("no_virtual_provisioning_collection_records_for_projects")
            .transaction(&conn, |conn| async move {
                conn
                    .batch_execute_async(nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .unwrap();
                Ok(
                    dsl::virtual_provisioning_collection
                        .filter(dsl::collection_type.eq(nexus_db_queries::db::model::CollectionTypeProvisioned::Project.to_string()))
                        // ignore built-in services project
                        .filter(dsl::id.ne(*SERVICES_PROJECT_ID))
                        .select(VirtualProvisioningCollection::as_select())
                        .get_results_async::<VirtualProvisioningCollection>(&conn)
                        .await
                        .unwrap()
                        .is_empty()
                )
            }).await.unwrap()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;

        // Before running the test, confirm we have no records of any projects.
        verify_clean_slate(nexus.datastore()).await;

        // Build the saga DAG with the provided test parameters and run it.
        let opctx = test_opctx(&cptestctx);
        let authz_silo = opctx.authn.silo_required().unwrap();
        let params = new_test_params(&opctx, authz_silo);
        nexus.sagas.saga_execute::<SagaProjectCreate>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let log = &cptestctx.logctx.log;
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);
        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaProjectCreate,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin({
                    async {
                        new_test_params(
                            &opctx,
                            opctx.authn.silo_required().unwrap(),
                        )
                    }
                })
            },
            || {
                Box::pin({
                    async {
                        verify_clean_slate(nexus.datastore()).await;
                    }
                })
            },
            log,
        )
        .await;
    }
}
