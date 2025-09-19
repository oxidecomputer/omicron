// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::SagaInitError;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::volume_delete;
use nexus_db_queries::authn;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore;
use omicron_common::api::external::DiskState;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub disk: datastore::CrucibleDisk,
}

// disk delete saga: actions

declare_saga_actions! {
    disk_delete;
    DELETE_DISK_RECORD -> "deleted_disk" {
        + sdd_delete_disk_record
        - sdd_delete_disk_record_undo
    }
    SPACE_ACCOUNT -> "no_result1" {
        + sdd_account_space
        - sdd_account_space_undo
    }
}

// disk delete saga: definition

#[derive(Debug)]
pub(crate) struct SagaDiskDelete;
impl NexusSaga for SagaDiskDelete {
    const NAME: &'static str = "disk-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        disk_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_disk_record_action());
        builder.append(space_account_action());

        let subsaga_params = volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.disk.volume_id(),
        };

        let subsaga_dag = {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                volume_delete::SagaVolumeDelete::NAME,
            ));
            volume_delete::SagaVolumeDelete::make_saga_dag(
                &subsaga_params,
                subsaga_builder,
            )?
        };

        builder.append(Node::constant(
            "params_for_volume_delete_subsaga",
            serde_json::to_value(&subsaga_params).map_err(|e| {
                SagaInitError::SerializeError(
                    "params_for_volume_delete_subsaga".to_string(),
                    e,
                )
            })?,
        ));

        builder.append(Node::subsaga(
            "volume_delete_subsaga_no_result",
            subsaga_dag,
            "params_for_volume_delete_subsaga",
        ));

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
        .project_delete_disk_no_auth(
            &params.disk.id(),
            &[DiskState::Detached, DiskState::Faulted],
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(disk)
}

async fn sdd_delete_disk_record_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    osagactx
        .datastore()
        .project_undelete_disk_set_faulted_no_auth(&params.disk.id())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
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

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::saga::create_saga_dag, app::sagas::disk_delete::Params,
        app::sagas::disk_delete::SagaDiskDelete,
    };
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::CrucibleDisk;
    use nexus_db_queries::db::datastore::Disk;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::params;
    use omicron_common::api::external::Name;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "springfield-squidport";

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    async fn create_disk(cptestctx: &ControlPlaneTestContext) -> CrucibleDisk {
        let nexus = &cptestctx.server.server_context().nexus;
        let opctx = test_opctx(&cptestctx);

        let project_selector = params::ProjectSelector {
            project: Name::try_from(PROJECT_NAME.to_string()).unwrap().into(),
        };

        let project_lookup =
            nexus.project_lookup(&opctx, project_selector).unwrap();

        let disk = nexus
            .project_create_disk(
                &opctx,
                &project_lookup,
                &crate::app::sagas::disk_create::test::new_disk_create_params(),
            )
            .await
            .expect("Failed to create disk");

        match disk {
            Disk::Crucible(disk) => disk,
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters and run it.
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        nexus.sagas.saga_execute::<SagaDiskDelete>(params).await.unwrap();
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let test = DiskTest::new(cptestctx).await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let project_id = create_project(client, PROJECT_NAME).await.identity.id;
        let disk = create_disk(&cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            project_id,
            disk,
        };

        let dag = create_saga_dag::<SagaDiskDelete>(params).unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        crate::app::sagas::disk_create::test::verify_clean_slate(
            &cptestctx, &test,
        )
        .await;
    }
}
