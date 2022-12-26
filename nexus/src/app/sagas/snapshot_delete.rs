// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::NexusAction;
use crate::authn;
use crate::external_api::params;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// snapshot create saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub snapshot_id: Uuid,
    pub project_id: Uuid,
    pub disk_id: Uuid,
    pub create_params: params::SnapshotCreate,
}

// snapshot create saga: actions

lazy_static! {
    static ref DELETE_SNAPSHOT_RECORD: NexusAction = new_action_noop_undo(
        "snapshot-delete.delete-snapshot-record",
        ssd_delete_snapshot_record,
    );
    static ref SPACE_ACCOUNT: NexusAction = new_action_noop_undo(
        "snapshot-delete.account-space",
        ssd_account_space,
    );
    static ref DELETE_SOURCE_VOLUME: NexusAction = new_action_noop_undo(
        "snapshot-delete.delete-source-volume",
        ssd_delete_source_volume,
    );
    static ref DELETE_DESTINATION_VOLUME: NexusAction = new_action_noop_undo(
        "snapshot-delete.delete-destination-volume",
        ssd_delete_destination_volume,
    );
}

// snapshot delete saga: definition

#[derive(Debug)]
pub struct SagaSnapshotDelete;
impl NexusSaga for SagaSnapshotDelete {
    const NAME: &'static str = "snapshot-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*DELETE_SNAPSHOT_RECORD));
        registry.register(Arc::clone(&*SPACE_ACCOUNT));
        registry.register(Arc::clone(&*DELETE_SOURCE_VOLUME));
        registry.register(Arc::clone(&*DELETE_DESTINATION_VOLUME));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        // Generate IDs
        builder.append(Node::action(
            "delete_source_volume_saga_id",
            "GenerateSourceVolumeSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "delete_destination_volume_saga_id",
            "GenerateDestinationVolumeSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(Node::action(
            "deleted_snapshot",
            "DeleteSnapshotRecord",
            DELETE_SNAPSHOT_RECORD.as_ref(),
        ));
        builder.append(Node::action(
            "accounted_space",
            "SpaceAccount",
            SPACE_ACCOUNT.as_ref(),
        ));
        builder.append(Node::action(
            "deleted_source_volume_saga",
            "DeleteSourceVolumeSaga",
            DELETE_SOURCE_VOLUME.as_ref(),
        ));
        builder.append(Node::action(
            "deleted_destination_volume_saga",
            "DeleteDestinationVolumeSaga",
            DELETE_DESTINATION_VOLUME.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

// snapshot delete saga: action implementations

async fn ssd_delete_snapshot_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!();
}

async fn ssd_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!();
    /*
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let snapshot_created =
        sagactx.lookup::<db::model::Snapshot>("created_snapshot")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_snapshot(
            &opctx,
            snapshot_created.id(),
            params.project_id,
            // TODO: How many bytes? read while deleting?
        )
        .await
        .map_err(ActionError::action_failed)?;
    */
    Ok(())
}

async fn ssd_delete_source_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!();
}

async fn ssd_delete_destination_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    todo!();
}
