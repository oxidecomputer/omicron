// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! For disks in state ImportReady, "finalize" them: detach them from their
//! attached Pantry, and set to Detached.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::SagaInitError;
use super::declare_saga_actions;
use crate::app::sagas::common_storage::call_pantry_detach;
use crate::app::sagas::snapshot_create;
use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use nexus_db_model::Generation;
use nexus_db_queries::{authn, authz, db::datastore};
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetryError;
use serde::Deserialize;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub silo_id: Uuid,
    pub project_id: Uuid,
    pub disk: datastore::CrucibleDisk,
    pub snapshot_name: Option<Name>,
}

declare_saga_actions! {
    finalize_disk;
    SET_FINALIZING_STATE -> "disk_generation_number" {
        + sfd_set_finalizing_state
        - sfd_set_finalizing_state_undo
    }
    GET_PANTRY_ADDRESS -> "pantry_address" {
        + sfd_get_pantry_address
    }
    CALL_PANTRY_DETACH_FOR_DISK -> "call_pantry_detach_for_disk" {
        + sfd_call_pantry_detach_for_disk
    }
    CLEAR_PANTRY_ADDRESS -> "clear_pantry_address" {
        + sfd_clear_pantry_address
    }
    SET_DETACHED_STATE -> "set_detached_state" {
        + sfd_set_detached_state
    }
}

#[derive(Debug)]
pub(crate) struct SagaFinalizeDisk;
impl NexusSaga for SagaFinalizeDisk {
    const NAME: &'static str = "finalize-disk";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        finalize_disk_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(set_finalizing_state_action());

        builder.append(get_pantry_address_action());

        if let Some(snapshot_name) = &params.snapshot_name {
            let subsaga_params = snapshot_create::Params {
                serialized_authn: params.serialized_authn.clone(),
                silo_id: params.silo_id,
                project_id: params.project_id,
                disk: params.disk.clone(),
                attach_instance_id: None,
                use_the_pantry: true,
                create_params: params::SnapshotCreate {
                    identity: external::IdentityMetadataCreateParams {
                        name: snapshot_name.clone(),
                        description: format!(
                            "snapshot of finalized disk {}",
                            params.disk.id()
                        ),
                    },
                    disk: params.disk.id().into(),
                },
            };

            let subsaga_dag = {
                let subsaga_builder =
                    steno::DagBuilder::new(steno::SagaName::new(
                        snapshot_create::SagaSnapshotCreate::NAME,
                    ));
                snapshot_create::SagaSnapshotCreate::make_saga_dag(
                    &subsaga_params,
                    subsaga_builder,
                )?
            };

            builder.append(Node::constant(
                "params_for_snapshot_subsaga",
                serde_json::to_value(&subsaga_params).map_err(|e| {
                    SagaInitError::SerializeError(
                        "params_for_snapshot_subsaga".to_string(),
                        e,
                    )
                })?,
            ));

            builder.append(Node::subsaga(
                "snapshot_subsaga_no_result",
                subsaga_dag,
                "params_for_snapshot_subsaga",
            ));
        }

        builder.append(call_pantry_detach_for_disk_action());

        builder.append(clear_pantry_address_action());

        builder.append(set_detached_state_action());

        Ok(builder.build()?)
    }
}

async fn sfd_set_finalizing_state(
    sagactx: NexusActionContext,
) -> Result<Generation, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    match db_disk.state().into() {
        external::DiskState::ImportReady => {
            info!(log, "setting disk {} to state finalizing", db_disk.id(),);

            osagactx
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &db_disk.runtime().finalizing(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            // Record the disk's new generation number as this saga node's output. It
            // will be important later to *only* transition this disk out of finalizing
            // if the generation number matches what *this* saga is doing.
            let (.., db_disk) = LookupPath::new(&opctx, osagactx.datastore())
                .disk_id(params.disk.id())
                .fetch_for(authz::Action::Read)
                .await
                .map_err(ActionError::action_failed)?;

            Ok(db_disk.runtime().generation)
        }

        _ => Err(ActionError::action_failed(Error::invalid_request(&format!(
            "cannot finalize disk in state {:?}",
            db_disk.state()
        )))),
    }
}

async fn sfd_set_finalizing_state_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    let expected_disk_generation_number =
        sagactx.lookup::<Generation>("disk_generation_number")?;

    match db_disk.state().into() {
        external::DiskState::Finalizing => {
            // A previous execution of *this* saga may have already transitioned this disk to
            // import_read. Another saga racing with this one may have transitioned the disk to
            // finalizing - only set this disk to import_ready if the generation number matches this
            // saga.
            if expected_disk_generation_number == db_disk.runtime().generation {
                info!(
                    log,
                    "undo: setting disk {} state from finalizing to import_ready",
                    params.disk.id()
                );

                osagactx
                    .datastore()
                    .disk_update_runtime(
                        &opctx,
                        &authz_disk,
                        &db_disk.runtime().import_ready(),
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            } else {
                info!(
                    log,
                    "disk {} has generation number {:?}, which doesn't match the expected {:?}: skip setting to import_ready",
                    params.disk.id(),
                    db_disk.runtime().generation,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::ImportReady => {
            info!(log, "disk {} already import_ready", params.disk.id());
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}

async fn sfd_get_pantry_address(
    sagactx: NexusActionContext,
) -> Result<SocketAddrV6, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Re-fetch the disk, and use the lookup first to check for modify
    // permission.
    let (.., db_disk) = LookupPath::new(&opctx, osagactx.datastore())
        .disk_id(params.disk.id())
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    let disk = match osagactx
        .datastore()
        .disk_get(&opctx, params.disk.id())
        .await
        .map_err(ActionError::action_failed)?
    {
        datastore::Disk::Crucible(disk) => disk,
    };

    // At any stage of executing this saga, if the disk moves from state
    // importing to detached, it will be detached from the corresponding Pantry.
    // Any subsequent saga nodes will fail because the pantry address is stored
    // as part of the saga state, and requests sent to that Pantry with the
    // disk's id will fail.
    let pantry_address = disk.pantry_address().ok_or_else(|| {
        ActionError::action_failed(String::from("disk not attached to pantry!"))
    })?;

    info!(log, "disk {} is using pantry at {}", db_disk.id(), pantry_address);

    Ok(pantry_address)
}

async fn sfd_call_pantry_detach_for_disk(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;
    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;

    match call_pantry_detach(
        sagactx.user_data().nexus(),
        &log,
        params.disk.id(),
        pantry_address,
    )
    .await
    {
        // If the detach succeeds, then proceed with finalization. If the detach
        // fails because the associated pantry is gone, then we have to be able
        // to proceed with finalization in order to be able to eventually delete
        // the disk. The associated pantry may have been expunged at any time
        // during the import and this part of the code doesn't know the state of
        // the disk, but we can't fail and leave the disk un-deleteable.
        Ok(()) | Err(ProgenitorOperationRetryError::Gone) => Ok(()),

        Err(e) => Err(ActionError::action_failed(format!(
            "pantry detach failed: {}",
            InlineErrorChain::new(&e)
        ))),
    }
}

async fn sfd_clear_pantry_address(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    info!(log, "setting disk {} pantry to None", params.disk.id());

    let (.., authz_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(params.disk.id())
        .lookup_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    datastore
        .disk_clear_pantry(&opctx, &authz_disk)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn sfd_set_detached_state(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, osagactx.datastore())
            .disk_id(params.disk.id())
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    let expected_disk_generation_number =
        sagactx.lookup::<Generation>("disk_generation_number")?;

    match db_disk.state().into() {
        external::DiskState::Finalizing => {
            if expected_disk_generation_number == db_disk.runtime().generation {
                info!(
                    log,
                    "setting disk {} state from finalizing to detached",
                    params.disk.id()
                );

                osagactx
                    .datastore()
                    .disk_update_runtime(
                        &opctx,
                        &authz_disk,
                        &db_disk.runtime().detach(),
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            } else {
                info!(
                    log,
                    "disk {} has generation number {:?}, which doesn't match the expected {:?}: skip setting to detached",
                    params.disk.id(),
                    db_disk.runtime().generation,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::Detached => {
            info!(log, "disk {} already detached", params.disk.id());
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}
