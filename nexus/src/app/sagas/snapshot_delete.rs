// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, ACTION_GENERATE_ID,
};
use crate::app::sagas;
use crate::app::sagas::declare_saga_actions;
use crate::{authn, authz, db};
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_snapshot: authz::Snapshot,
    pub snapshot: db::model::Snapshot,
}

declare_saga_actions! {
    snapshot_delete;
    LOCK_SNAPSHOT_FOR_DELETE -> "lock_snapshot" {
        + ssd_lock_snapshot_for_delete
        - ssd_lock_snapshot_for_delete_undo
    }
    DELETE_SNAPSHOT_RECORD -> "no_result1" {
        + ssd_delete_snapshot_record
    }
    SPACE_ACCOUNT -> "no_result2" {
        + ssd_account_space
    }
    FREE_SNAPSHOT_LOCK -> "free_snapshot_lock" {
        + ssd_free_snapshot_lock
    }
}

#[derive(Debug)]
pub struct SagaSnapshotDelete;
impl NexusSaga for SagaSnapshotDelete {
    const NAME: &'static str = "snapshot-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        snapshot_delete_register_actions(registry);
    }

    fn make_saga_dag(
        params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        // Generate a lock ID
        builder.append(Node::action(
            "lock_id",
            "GenerateLockId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(lock_snapshot_for_delete_action());
        builder.append(delete_snapshot_record_action());
        builder.append(space_account_action());
        builder.append(free_snapshot_lock_action());

        const DELETE_VOLUME_PARAMS: &'static str = "delete_volume_params";
        const DELETE_VOLUME_DESTINATION_PARAMS: &'static str =
            "delete_volume_destination_params";

        let volume_delete_params = sagas::volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.snapshot.volume_id,
        };
        builder.append(Node::constant(
            DELETE_VOLUME_PARAMS,
            serde_json::to_value(&volume_delete_params).map_err(|e| {
                super::SagaInitError::SerializeError(
                    String::from("volume_id"),
                    e,
                )
            })?,
        ));

        let volume_delete_params = sagas::volume_delete::Params {
            serialized_authn: params.serialized_authn.clone(),
            volume_id: params.snapshot.destination_volume_id,
        };
        builder.append(Node::constant(
            DELETE_VOLUME_DESTINATION_PARAMS,
            serde_json::to_value(&volume_delete_params).map_err(|e| {
                super::SagaInitError::SerializeError(
                    String::from("volume_id"),
                    e,
                )
            })?,
        ));

        let make_volume_delete_dag = || {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                sagas::volume_delete::SagaVolumeDelete::NAME,
            ));
            sagas::volume_delete::create_dag(subsaga_builder)
        };
        builder.append(steno::Node::subsaga(
            "delete_volume",
            make_volume_delete_dag()?,
            DELETE_VOLUME_PARAMS,
        ));
        builder.append(steno::Node::subsaga(
            "delete_destination_volume",
            make_volume_delete_dag()?,
            DELETE_VOLUME_DESTINATION_PARAMS,
        ));

        Ok(builder.build()?)
    }
}

// snapshot delete saga: action implementations

async fn ssd_lock_snapshot_for_delete(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let log = osagactx.log();
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;
    let snapshot_id = params.authz_snapshot.id();

    info!(
        log,
        "attempting resource lock of snapshot {} with lock id {}",
        snapshot_id,
        lock_id,
    );

    osagactx
        .datastore()
        .attempt_resource_lock(snapshot_id, lock_id)
        .await
        .map_err(|_e| {
            ActionError::action_failed(Error::conflict(&format!(
                "snapshot {} is locked by some other saga",
                snapshot_id,
            )))
        })?;

    Ok(())
}

async fn ssd_lock_snapshot_for_delete_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let log = osagactx.log();
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;
    let snapshot_id = params.authz_snapshot.id();

    info!(
        log,
        "undo: freeing resource lock of snapshot {} with lock id {}",
        snapshot_id,
        lock_id,
    );

    osagactx
        .datastore()
        .free_resource_lock(snapshot_id, lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn ssd_delete_snapshot_record(
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
        .project_delete_snapshot(
            &opctx,
            &params.authz_snapshot,
            &params.snapshot,
            vec![
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn ssd_account_space(
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
        .virtual_provisioning_collection_delete_snapshot(
            &opctx,
            params.authz_snapshot.id(),
            params.snapshot.project_id,
            params.snapshot.size,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn ssd_free_snapshot_lock(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let log = osagactx.log();
    let lock_id = sagactx.lookup::<Uuid>("lock_id")?;
    let snapshot_id = params.authz_snapshot.id();

    info!(
        log,
        "freeing resource lock of snapshot {} with lock id {}",
        snapshot_id,
        lock_id,
    );

    osagactx
        .datastore()
        .free_resource_lock(snapshot_id, lock_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
