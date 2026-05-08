// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! When a user creates a resource that is backed by read-only data, like a
//! snapshot or image, a background task will call this saga to attach a copy of
//! the backing volume to a Pantry so that data can be exported through Nexus.
//! The intention is that users can export any read-only data at any time by
//! proxying through Nexus, without having to manage any implementation details.
//!
//! This saga will, for the argument record that's in state Requested:
//!
//! 1. Make a copy of the associate read-only object's volume
//! 2. Attach that volume to a random Pantry
//! 3. Transition the record to Live, setting the fields for the selected Pantry
//!    and the volume id.
//!
//! This saga handles the following state transitions:
//!
//! ```text
//!         Requested   <--
//!                       |
//!             |         |
//!             v         |
//!                       |
//!         Assigning   --
//!
//!             |
//!             v
//!
//!           Live
//! ```

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
    common_storage::{
        call_pantry_attach_for_volume, call_pantry_detach, get_pantry_address,
    },
};
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, db};
use anyhow::anyhow;
use nexus_db_lookup::LookupPath;
use nexus_db_model::UserDataExportResource;
use nexus_db_queries::db::identity::Resource;
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetryError;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::UserDataExportUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// user data export create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub id: UserDataExportUuid,
}

// user data export create saga: actions
declare_saga_actions! {
    user_data_export_create;
    GET_TARGET_GENERATION_NUMBER -> "target_generation" {
        + sudec_get_target_generation_number
    }
    SET_SAGA_ID -> "unused_1" {
        + sudec_set_saga_id
        - sudec_set_saga_id_undo
    }
    CREATE_EXPORT_VOLUME -> "export_volume" {
        + sudec_create_export_volume
        - sudec_create_export_volume_undo
    }
    GET_PANTRY_ADDRESS -> "pantry_address" {
        + sudec_get_pantry_address
    }
    CALL_PANTRY_ATTACH_FOR_EXPORT -> "call_pantry_attach_for_export" {
        + sudec_call_pantry_attach_for_export
        - sudec_call_pantry_attach_for_export_undo
    }
    UPDATE_USER_DATA_EXPORT_RECORD -> "unused_2" {
        + sudec_update_user_data_export_record
    }
}

// user data export create saga: definition

#[derive(Debug)]
pub(crate) struct SagaUserDataExportCreate;
impl NexusSaga for SagaUserDataExportCreate {
    const NAME: &'static str = "user-data-export-create";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        user_data_export_create_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        // Generate IDs
        builder.append(Node::action(
            "saga_id",
            "GenerateSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "volume_id",
            "GenerateVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(get_target_generation_number_action());
        builder.append(set_saga_id_action());
        builder.append(create_export_volume_action());
        builder.append(get_pantry_address_action());
        builder.append(call_pantry_attach_for_export_action());
        builder.append(update_user_data_export_record_action());

        Ok(builder.build()?)
    }
}

// user data export saga: action implementations

async fn sudec_get_target_generation_number(
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
        .user_data_export_lookup_by_id(&opctx, params.id)
        .await
        .map_err(saga_action_failed)?
    else {
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export record hard-deleted!",
        ))));
    };

    Ok(record.generation() + 1)
}

async fn sudec_set_saga_id(
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

    // Change the request record here to an intermediate "assigning" state to
    // block out other sagas that will be triggered for the same request.

    osagactx
        .datastore()
        .set_user_data_export_requested_to_assigning(
            &opctx,
            params.id,
            saga_id,
            target_generation,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

async fn sudec_set_saga_id_undo(
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
        .unset_user_data_export_requested_to_assigning(
            &opctx,
            params.id,
            saga_id,
            target_generation,
        )
        .await?;

    Ok(())
}

async fn sudec_create_export_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    let Some(record) = osagactx
        .datastore()
        .user_data_export_lookup_by_id(&opctx, params.id)
        .await
        .map_err(saga_action_failed)?
    else {
        return Err(saga_action_failed(Error::internal_error(String::from(
            "user data export record hard-deleted!",
        ))));
    };

    let source_volume_id = match record.resource() {
        UserDataExportResource::Snapshot { id } => {
            debug!(log, "grabbing snapshot {id}");

            let (.., db_snapshot) =
                LookupPath::new(&opctx, osagactx.datastore())
                    .snapshot_id(id)
                    .fetch()
                    .await
                    .map_err(saga_action_failed)?;

            debug!(
                log,
                "copying snapshot {} volume {} to {volume_id}",
                db_snapshot.id(),
                db_snapshot.volume_id(),
            );

            db_snapshot.volume_id()
        }

        UserDataExportResource::Image { id } => {
            debug!(log, "grabbing image {id}");

            let (.., db_image) = LookupPath::new(&opctx, osagactx.datastore())
                .image_id(id)
                .fetch()
                .await
                .map_err(saga_action_failed)?;

            debug!(
                log,
                "copying image {} volume {} to {volume_id}",
                db_image.id(),
                db_image.volume_id(),
            );

            db_image.volume_id()
        }
    };

    osagactx
        .datastore()
        .volume_checkout_randomize_ids(
            db::datastore::SourceVolume(source_volume_id),
            db::datastore::DestVolume(volume_id),
            db::datastore::VolumeCheckoutReason::ReadOnlyCopy,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

async fn sudec_create_export_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // `volume_checkout_randomize_ids` calls `volume_create`, which will
    // increase the resource count for read only resources in a volume, which
    // there are guaranteed to be for these volumes. Decreasing crucible
    // resources is necessary as an undo step. Do not call `volume_hard_delete`
    // here: soft deleting volumes is necessary for
    // `find_deleted_volume_regions` to work.

    info!(log, "calling soft delete for volume {}", volume_id);

    osagactx.datastore().soft_delete_volume(volume_id).await?;

    Ok(())
}

async fn sudec_get_pantry_address(
    sagactx: NexusActionContext,
) -> Result<SocketAddrV6, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let pantry_address = get_pantry_address(osagactx.nexus()).await?;

    info!(log, "using pantry at {}", pantry_address);

    Ok(pantry_address)
}

async fn sudec_call_pantry_attach_for_export(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;
    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    let volume = match osagactx.datastore().volume_get(volume_id).await {
        Ok(Some(volume)) => volume,

        Ok(None) => {
            return Err(saga_action_failed(Error::internal_error(&format!(
                "volume {volume_id} gone!"
            ))));
        }

        Err(e) => {
            return Err(saga_action_failed(Error::internal_error(&format!(
                "failed to get volume {volume_id}: {e}"
            ))));
        }
    };

    let volume_construction_request = serde_json::from_str(&volume.data())
        .map_err(|e| {
            saga_action_failed(Error::internal_error(&format!(
                "failed to deserialize volume {volume_id} data: {e}"
            )))
        })?;

    info!(log, "sending attach for volume {volume_id} to {pantry_address}");

    call_pantry_attach_for_volume(
        &log,
        &osagactx.nexus(),
        volume_id.into_untyped_uuid(),
        volume_construction_request,
        pantry_address,
    )
    .await
}

async fn sudec_call_pantry_attach_for_export_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();

    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;
    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    info!(log, "undo: detaching {volume_id} from pantry at {pantry_address}");

    match call_pantry_detach(
        sagactx.user_data().nexus(),
        &log,
        volume_id.into_untyped_uuid(),
        pantry_address,
    )
    .await
    {
        // We can treat the pantry being permanently gone as success.
        Ok(()) | Err(ProgenitorOperationRetryError::Gone) => Ok(()),

        Err(err) => Err(anyhow!(
            "failed to detach {volume_id} from pantry at {pantry_address}: {}",
            InlineErrorChain::new(&err)
        )),
    }
}

async fn sudec_update_user_data_export_record(
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
    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;
    let volume_id = sagactx.lookup::<VolumeUuid>("volume_id")?;

    // Set the record's state to Live and clear the operating saga id. There is
    // no undo step for this, it should succeed idempotently.

    osagactx
        .datastore()
        .set_user_data_export_assigning_to_live(
            &opctx,
            params.id,
            saga_id,
            target_generation,
            pantry_address,
            volume_id,
        )
        .await
        .map_err(saga_action_failed)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::saga::create_saga_dag;
    use nexus_db_model::UserDataExportRecord;
    use nexus_db_model::UserDataExportState;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_snapshot;
    use nexus_test_utils_macros::nexus_test;
    use uuid::Uuid;

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

        create_default_ip_pools(&client).await;
        create_project(client, PROJECT_NAME).await;
        create_disk(client, PROJECT_NAME, DISK_NAME).await;

        let snapshot_id =
            create_snapshot(client, PROJECT_NAME, DISK_NAME, SNAPSHOT_NAME)
                .await
                .identity
                .id;

        // Manually create the record so it remains in state Requested

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let export_object = datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                snapshot_id,
            )
            .await
            .unwrap();

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
        let (snapshot_id, export_object) =
            create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            id: export_object.id(),
        };

        // Actually run the saga
        nexus
            .sagas
            .saga_execute::<SagaUserDataExportCreate>(params)
            .await
            .unwrap();

        // Make sure the record was transitioned ok

        let record = nexus
            .datastore()
            .user_data_export_lookup_for_snapshot(&opctx, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.state(), UserDataExportState::Live);
        assert!(record.pantry_address().is_some());
        assert!(record.volume_id().is_some());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let (snapshot_id, export_object) =
            create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(cptestctx);

        let params = Params {
            serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
            id: export_object.id(),
        };

        let dag = create_saga_dag::<SagaUserDataExportCreate>(params).unwrap();

        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;

        // Make sure the record was transitioned ok

        let record = nexus
            .datastore()
            .user_data_export_lookup_for_snapshot(&opctx, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.state(), UserDataExportState::Live);
        assert!(record.pantry_address().is_some());
        assert!(record.volume_id().is_some());
    }

    async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        snapshot_id: Uuid,
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
            .user_data_export_lookup_for_snapshot(&opctx, snapshot_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.state(), UserDataExportState::Requested);
        assert_eq!(record.pantry_address(), None);
        assert_eq!(record.volume_id(), None);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        DiskTest::new(cptestctx).await;
        let log = &cptestctx.logctx.log;

        let nexus = &cptestctx.server.server_context().nexus;

        let (snapshot_id, export_object) =
            create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaUserDataExportCreate,
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
                        id: export_object.id(),
                    }
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(cptestctx, snapshot_id).await;
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

        let (snapshot_id, export_object) =
            create_all_the_stuff(cptestctx).await;

        // Build the saga DAG with the provided test parameters
        let opctx = test_opctx(&cptestctx);

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaUserDataExportCreate,
            _,
            _,
        >(
            nexus,
            || {
                Box::pin(async {
                    Params {
                        serialized_authn: authn::saga::Serialized::for_opctx(&opctx),
                        id: export_object.id(),
                    }
                })
            },
            || {
                Box::pin(async {
                    verify_clean_slate(cptestctx, snapshot_id).await;
                })
            },
            log,
        )
        .await;
    }
}
