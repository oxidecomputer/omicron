// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use crate::authn;
use crate::db;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub disk_id: Uuid,
}

// disk delete saga: actions

declare_saga_actions! {
    disk_delete;
    DELETE_DISK_RECORD -> "deleted_disk" {
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        + sdd_delete_disk_record
    }
    SPACE_ACCOUNT -> "no_result1" {
        + sdd_account_space
        - sdd_account_space_undo
    }
    DELETE_VOLUME -> "no_result2" {
        + sdd_delete_volume
    }
}

// disk delete saga: definition

#[derive(Debug)]
pub struct SagaDiskDelete;
impl NexusSaga for SagaDiskDelete {
    const NAME: &'static str = "disk-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        disk_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_disk_record_action());
        builder.append(space_account_action());
        builder.append(delete_volume_action());
        Ok(builder.build()?)
    }
}

// disk delete saga: action implementations

async fn sdd_delete_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(disk)
}

async fn sdd_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            deleted_disk.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            deleted_disk.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let volume_id =
        sagactx.lookup::<db::model::Disk>("deleted_disk")?.volume_id;
    osagactx
        .nexus()
        .volume_delete(&opctx, volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_delete::Params,
        app::sagas::disk_delete::SagaDiskDelete, authn::saga::Serialized,
    };
    use dropshot::test_util::ClientTestContext;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_ip_pool;
    use nexus_test_utils::resource_helpers::create_organization;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params;
    use omicron_common::api::external::Name;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const ORG_NAME: &str = "test-org";
    const PROJECT_NAME: &str = "springfield-squidport";

    async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
        create_ip_pool(&client, "p0", None).await;
        create_organization(&client, ORG_NAME).await;
        let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
        project.identity.id
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.apictx.nexus.datastore().clone(),
        )
    }

    async fn create_disk(cptestctx: &ControlPlaneTestContext) -> Uuid {
        let nexus = &cptestctx.server.apictx.nexus;
        let opctx = test_opctx(&cptestctx);

        let project_selector = params::ProjectSelector::new(
            Some(Name::try_from(ORG_NAME.to_string()).unwrap().into()),
            Name::try_from(PROJECT_NAME.to_string()).unwrap().into(),
        );
        let project_lookup =
            nexus.project_lookup(&opctx, &project_selector).unwrap();

        nexus
            .project_create_disk(
                &opctx,
                &project_lookup,
                &crate::app::sagas::disk_create::test::new_disk_create_params(),
            )
            .await
            .expect("Failed to create disk")
            .id()
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        let project_id = create_org_and_project(&client).await;
        let disk_id = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk_id,
        };
        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();
        let runnable_saga = nexus.create_runnable_saga(dag).await.unwrap();

        // Actually run the saga
        nexus.run_saga(runnable_saga).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.apictx.nexus;
        let project_id = create_org_and_project(&client).await;
        let disk_id = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk_id,
        };
        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        // Cause all actions to run twice. The saga should succeed regardless!
        for node in dag.get_nodes() {
            nexus
                .sec()
                .saga_inject_repeat(
                    runnable_saga.id(),
                    node.index(),
                    steno::RepeatInjected {
                        action: NonZeroU32::new(2).unwrap(),
                        undo: NonZeroU32::new(1).unwrap(),
                    },
                )
                .await
                .unwrap();
        }

        // Verify that the saga's execution succeeded.
        nexus
            .run_saga(runnable_saga)
            .await
            .expect("Saga should have succeeded");

        crate::app::sagas::disk_create::test::verify_clean_slate(
            &cptestctx, &test,
        )
        .await;
    }
}
