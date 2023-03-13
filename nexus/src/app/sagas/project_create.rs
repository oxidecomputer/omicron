// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use crate::external_api::params;
use crate::{authn, authz, db};
use nexus_defaults as defaults;
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

// project create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_create: params::ProjectCreate,
    pub authz_org: authz::Organization,
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
pub struct SagaProjectCreate;
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
        db::model::Project::new(params.authz_org.id(), params.project_create);
    osagactx
        .datastore()
        .project_create(&opctx, &params.authz_org, db_project)
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

    let (_authz_project, project) =
        sagactx.lookup::<(authz::Project, db::model::Project)>("project")?;

    let (.., authz_project, project) =
        db::lookup::LookupPath::new(&opctx, osagactx.datastore())
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
        app::saga::create_saga_dag, app::sagas::project_create::Params,
        app::sagas::project_create::SagaProjectCreate, authn::saga::Serialized,
        authz, db::datastore::DataStore, external_api::params,
    };
    use async_bb8_diesel::{
        AsyncConnection, AsyncRunQueryDsl, AsyncSimpleConnection,
        OptionalExtension,
    };
    use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_organization;
    use nexus_test_utils::resource_helpers::populate_ip_pool;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::NameOrId;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const ORG_NAME: &str = "test-org";

    async fn create_org(client: &ClientTestContext) -> Uuid {
        populate_ip_pool(&client, "default", None).await;
        let org = create_organization(&client, ORG_NAME).await;
        org.identity.id
    }

    // Helper for creating project create parameters
    fn new_test_params(
        opctx: &OpContext,
        authz_org: authz::Organization,
    ) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            project_create: params::ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "my-project".parse().unwrap(),
                    description: "My Project".to_string(),
                },
            },
            authz_org,
        }
    }

    fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx().nexus.datastore().clone(),
        )
    }

    async fn get_authz_org(
        cptestctx: &ControlPlaneTestContext,
        org_id: Uuid,
        action: authz::Action,
    ) -> authz::Organization {
        let nexus = &cptestctx.server.apictx().nexus;
        let org_selector =
            params::OrganizationSelector { organization: NameOrId::Id(org_id) };
        let opctx = test_opctx(&cptestctx);
        let (.., authz_org) = nexus
            .organization_lookup(&opctx, &org_selector)
            .expect("Invalid parameters constructing organization lookup")
            .lookup_for(action)
            .await
            .expect("Organization does not exist");
        authz_org
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
        use crate::db::model::Project;
        use crate::db::schema::project::dsl;

        dsl::project
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .first_async::<Project>(datastore.pool_for_tests().await.unwrap())
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
        use crate::db::model::VirtualProvisioningCollection;
        use crate::db::schema::virtual_provisioning_collection::dsl;

        datastore.pool_for_tests()
            .await
            .unwrap()
            .transaction_async(|conn| async move {
                conn
                    .batch_execute_async(nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .unwrap();
                Ok::<_, crate::db::TransactionError<()>>(
                    dsl::virtual_provisioning_collection
                        .filter(dsl::collection_type.eq(crate::db::model::CollectionTypeProvisioned::Project.to_string()))

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
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx().nexus;
        let org_id = create_org(&client).await;

        // Before running the test, confirm we have no records of any projects.
        verify_clean_slate(nexus.datastore()).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let authz_org =
            get_authz_org(&cptestctx, org_id, authz::Action::CreateChild).await;
        let params = new_test_params(&opctx, authz_org);
        let dag = create_saga_dag::<SagaProjectCreate>(params).unwrap();
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
        let org_id = create_org(&client).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let authz_org =
            get_authz_org(&cptestctx, org_id, authz::Action::CreateChild).await;
        let params = new_test_params(&opctx, authz_org);
        let dag = create_saga_dag::<SagaProjectCreate>(params).unwrap();

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
