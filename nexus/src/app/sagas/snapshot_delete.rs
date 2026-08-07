// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga};
use crate::app::sagas::declare_saga_actions;
use nexus_db_queries::{authn, authz, db};
use nexus_types::saga::saga_action_failed;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub authz_snapshot: authz::Snapshot,
    pub snapshot: db::model::Snapshot,
}

declare_saga_actions! {
    snapshot_delete;
    DELETE_SNAPSHOT_RECORD -> "no_result1" {
        + ssd_delete_snapshot_record
    }
    SPACE_ACCOUNT -> "no_result2" {
        + ssd_account_space
    }
    SOFT_DELETE_VOLUME -> "soft_delete_volume" {
        + ssd_soft_delete_volume
    }
    SOFT_DELETE_DEST_VOLUME -> "soft_delete_dest_volume" {
        + ssd_soft_delete_dest_volume
    }
}

#[derive(Debug)]
pub(crate) struct SagaSnapshotDelete;
impl NexusSaga for SagaSnapshotDelete {
    const NAME: &'static str = "snapshot-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        snapshot_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(delete_snapshot_record_action());
        builder.append(space_account_action());
        builder.append(soft_delete_volume_action());
        builder.append(soft_delete_dest_volume_action());

        Ok(builder.build()?)
    }
}

// snapshot delete saga: action implementations

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
        .map_err(saga_action_failed)?;
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
        .map_err(saga_action_failed)?;
    Ok(())
}

async fn ssd_soft_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();

    osagactx
        .datastore()
        .soft_delete_volume(params.snapshot.volume_id())
        .await
        .map_err(|e| {
            saga_action_failed(Error::internal_error(&format!(
                "failed to soft_delete_volume: {:?}",
                e,
            )))
        })?;

    Ok(())
}

async fn ssd_soft_delete_dest_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();

    osagactx
        .datastore()
        .soft_delete_volume(params.snapshot.destination_volume_id())
        .await
        .map_err(|e| {
            saga_action_failed(Error::internal_error(&format!(
                "failed to soft_delete_volume: {:?}",
                e,
            )))
        })?;

    Ok(())
}
