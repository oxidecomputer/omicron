// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! When a user deletes a resource that is backed by read-only data, a
//! background task will call this saga to undo the process of attaching a copy
//! of that resource's volume to a Pantry for use with the related export API.
//!
//! This saga will, for the argument user data export object:
//!
//! 1. Detach the user data export's volume from the appropriate Pantry
//! 2. Delete that volume (using the volume delete sub-saga)
//! 3. Delete the user data export record.
//!
//! This saga handles the following state transitions:
//!
//! ```text
//!           Live     <--
//!                       |
//!             |         |
//!             v         |
//!                       |
//!         Deleting    --
//!
//!             |
//!             v
//!
//!          Deleted
//! ```

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::common_storage::call_pantry_detach_ok_if_gone;
use crate::app::sagas::ACTION_GENERATE_ID;
use crate::app::sagas::SagaInitError;
use crate::app::sagas::declare_saga_actions;
use crate::app::sagas::volume_delete;
use nexus_db_queries::authn;
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetryError;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::UserDataExportUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// user data export delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub user_data_export_id: UserDataExportUuid,
    pub volume_id: VolumeUuid,
}

// user data export delete saga: actions

declare_saga_actions! {
    user_data_export_delete;
    GET_TARGET_GENERATION_NUMBER -> "target_generation" {
        + suded_get_target_generation_number
    }
    SET_SAGA_ID -> "unused_1" {
        + suded_set_saga_id
        - suded_set_saga_id_undo
    }
    CALL_PANTRY_DETACH_FOR_EXPORT -> "call_pantry_detach_for_export" {
        + suded_call_pantry_detach_for_export
    }
    UPDATE_USER_DATA_EXPORT_RECORD -> "unused_2" {
        + suded_update_user_data_export_record
    }
}

// user data export delete saga: definition

#[derive(Debug)]
pub(crate) struct SagaUserDataExportDelete;
impl NexusSaga for SagaUserDataExportDelete {
    const NAME: &'static str = "user-data-export-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        user_data_export_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "saga_id",
            "GenerateSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(get_target_generation_number_action());
        builder.append(set_saga_id_action());

        builder.append(call_pantry_detach_for_export_action());

        let subsaga_params = volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.volume_id,
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

        // Set the user data export state to Deleted last. There's no way to
        // re-trigger the delete otherwise.
        builder.append(update_user_data_export_record_action());

        Ok(builder.build()?)
    }
}

// user data export delete saga: action implementations

async fn suded_get_target_generation_number(
    sagactx: NexusActionContext,
) -> Result<i64, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Query first for the record's generation number, then bump it and use that
    // as the target generation in the rest of the saga.

    let Some(record) = osagactx
        .datastore()
        .user_data_export_lookup_by_id(&opctx, params.user_data_export_id)
        .await
        .map_err(saga_action_failed)?
    else {
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export record hard-deleted!",
        ))));
    };

    Ok(record.generation() + 1)
}

async fn suded_set_saga_id(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;
    let target_generation = sagactx.lookup::<i64>("target_generation")?;

    // Change the request record here to an intermediate "deleting" state to
    // block out other sagas that will be triggered for the same request.

    osagactx
        .datastore()
        .set_user_data_export_live_to_deleting(
            &opctx,
            params.user_data_export_id,
            saga_id,
            target_generation,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

async fn suded_set_saga_id_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;
    let target_generation = sagactx.lookup::<i64>("target_generation")?;

    osagactx
        .datastore()
        .unset_user_data_export_live_to_deleting(
            &opctx,
            params.user_data_export_id,
            saga_id,
            target_generation,
        )
        .await?;

    Ok(())
}

async fn suded_call_pantry_detach_for_export(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let maybe_record = osagactx
        .datastore()
        .user_data_export_lookup_by_id(&opctx, params.user_data_export_id)
        .await
        .map_err(saga_action_failed)?;

    let Some(record) = maybe_record else {
        info!(
            log,
            "user data export {} hard deleted!", params.user_data_export_id,
        );
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export hard deleted!",
        ))));
    };

    let Some(pantry_address) = record.pantry_address() else {
        info!(
            log,
            "user data export {} has no pantry address!",
            params.user_data_export_id,
        );
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export has no pantry address!",
        ))));
    };

    let Some(volume_id) = record.volume_id() else {
        info!(
            log,
            "user data export {} has no volume id!", params.user_data_export_id,
        );
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export has no volume id!",
        ))));
    };

    // It's possible the Pantry bounced, and lost the attachment of this volume,
    // and this record was deleted so that another attachment could be created
    // (possibly somewhere else!). Call the detach function that's ok with this.

    info!(log, "detaching {volume_id} from pantry at {pantry_address}");

    match call_pantry_detach_ok_if_gone(
        sagactx.user_data().nexus(),
        &log,
        volume_id.into_untyped_uuid(),
        pantry_address,
    )
    .await
    {
        // We can treat the pantry being permanently gone as success.
        Ok(()) | Err(ProgenitorOperationRetryError::Gone) => Ok(()),

        Err(err) => Err(saga_action_failed(Error::internal_error(format!(
            "failed to detach {volume_id} from pantry at {pantry_address}: {}",
            InlineErrorChain::new(&err)
        )))),
    }
}

async fn suded_update_user_data_export_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;
    let target_generation = sagactx.lookup::<i64>("target_generation")?;

    osagactx
        .datastore()
        .set_user_data_export_deleting_to_deleted(
            &opctx,
            params.user_data_export_id,
            saga_id,
            target_generation,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::authz;
    use crate::app::saga::create_saga_dag;
    use nexus_db_lookup::LookupPath;
    use uuid::Uuid;

    use nexus_db_model::SnapshotState;
    use nexus_db_model::UserDataExportRecord;
    use nexus_db_model::UserDataExportState;

    use nexus_db_queries::context::OpContext;

    use nexus_test_utils::background::run_user_data_export_coordinator;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_snapshot;

    use nexus_test_utils_macros::nexus_test;

    use omicron_common::api::external::Error;

    use omicron_test_utils::dev::poll;

    use std::time::Duration;

    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "bobs-barrel-of-bits";
    const DISK_NAME: &str = "bobs-disk";
    const SNAPSHOT_NAME: &str = "bobs-snapshot";

    async fn create_all_the_stuff(
        cptestctx: &ControlPlaneTestContext,
    ) -> (Uuid, UserDataExportRecord) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();

        create_default_ip_pools(&client).await;
        create_project(client, PROJECT_NAME).await;
        create_disk(client, PROJECT_NAME, DISK_NAME).await;

        let snapshot_id =
            create_snapshot(client, PROJECT_NAME, DISK_NAME, SNAPSHOT_NAME)
                .await
                .identity
                .id;

        let opctx = test_opctx(cptestctx);

        // Run the background task to create the record
        run_user_data_export_coordinator(&cptestctx.lockstep_client).await;

        let (.., authz_snapshot, db_snapshot) =
            LookupPath::new(&opctx, datastore)
                .snapshot_id(snapshot_id)
                .fetch_for(authz::Action::Read)
                .await
                .unwrap();

        let export_object = poll::wait_for_condition(
            || {
                let opctx = test_opctx(cptestctx);
                let datastore = datastore.clone();

                async move {
                    let maybe_record = datastore
                        .user_data_export_lookup_for_snapshot(
                            &opctx,
                            snapshot_id,
                        )
                        .await
                        .unwrap();

                    match maybe_record {
                        Some(record) => {
                            // Only return when the record is in state Live
                            if record.state() == UserDataExportState::Live {
                                Ok(record)
                            } else {
                                Err(poll::CondCheckError::<Error>::NotYet)
                            }
                        }

                        None => Err(poll::CondCheckError::<Error>::NotYet),
                    }
                }
            },
            &Duration::from_millis(10),
            &Duration::from_secs(20),
        )
        .await
        .unwrap();

        // Delete the snapshot, then run the changeset function to mark the
        // export object as deleted.

        datastore
            .project_delete_snapshot(
                &opctx,
                &authz_snapshot,
                &db_snapshot,
                vec![SnapshotState::Creating, SnapshotState::Ready],
            )
            .await
            .unwrap();

        let _changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        (snapshot_id, export_object)
    }

    pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
        OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            cptestctx.server.server_context().nexus.datastore().clone(),
        )
    }

    #[nexus_test(server = crate::Server)]
    async fn test_saga_basic_usage_succeeds(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let (_, export_object) = create_all_the_stuff(cptestctx).await;

        // Create the delete saga dag
        let opctx = test_opctx(cptestctx);

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            user_data_export_id: export_object.id(),
            volume_id: export_object.volume_id().unwrap(),
        };

        nexus
            .sagas
            .saga_execute::<SagaUserDataExportDelete>(params)
            .await
            .unwrap();

        // Make sure the record was deleted ok

        let export_object = nexus
            .datastore()
            .user_data_export_lookup_by_id(&opctx, export_object.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(export_object.state(), UserDataExportState::Deleted);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let (_, export_object) = create_all_the_stuff(cptestctx).await;

        // Create the delete saga dag
        let opctx = test_opctx(cptestctx);

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            user_data_export_id: export_object.id(),
            volume_id: export_object.volume_id().unwrap(),
        };

        let dag = create_saga_dag::<SagaUserDataExportDelete>(params).unwrap();

        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        // Make sure the record was deleted ok

        let export_object = nexus
            .datastore()
            .user_data_export_lookup_by_id(&opctx, export_object.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(export_object.state(), UserDataExportState::Deleted);
    }

    async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        user_data_export_id: UserDataExportUuid,
    ) {
        let opctx = test_opctx(cptestctx);
        let nexus = &cptestctx.server.server_context().nexus;

        crate::app::sagas::test_helpers::assert_no_failed_undo_steps(
            &cptestctx.logctx.log,
            nexus.datastore(),
        )
        .await;

        // Validate the record is in the beginning state

        let record = nexus
            .datastore()
            .user_data_export_lookup_by_id(&opctx, user_data_export_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.state(), UserDataExportState::Live);
        assert!(record.pantry_address().is_some());
        assert!(record.volume_id().is_some());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let nexus = &cptestctx.server.server_context().nexus;

        let (_, export_object) = create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let export_id = export_object.id();
        let volume_id = export_object.volume_id().unwrap();

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaUserDataExportDelete,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin(async {
                    Params {
                        serialized_authn: authn::saga::Serialized::for_opctx(
                            &opctx,
                        ),
                        user_data_export_id: export_id,
                        volume_id,
                    }
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(cptestctx, export_id).await;
                })
            },
            log,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let nexus = &cptestctx.server.server_context().nexus;

        let (_, export_object) = create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        let export_id = export_object.id();
        let volume_id = export_object.volume_id().unwrap();

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaUserDataExportDelete,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin(async {
                    Params {
                        serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
                        user_data_export_id: export_id,
                        volume_id,
                    }
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(cptestctx, export_id).await;
                })
            },
            log,
        )
        .await;
    }
}
