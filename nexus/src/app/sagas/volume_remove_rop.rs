// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
	ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::sagas::NexusAction;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::ActionError;
use steno::{new_action_noop_undo, Node};
use uuid::Uuid;

// Volume remove read only parent saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub volume_id: Uuid,
}

// Volume remove_read_only_parent saga: actions

lazy_static! {
    // We remove a read_only parent by
    // - Creating a temporary volume.
    // - Remove the read_only_parent from our source volume and attach it
    // to the temporary volume.
    // - Delete the temporary volume.

    // remove the read_only_parent,  attach it to the temp volume.
    static ref REMOVE_READ_ONLY_PARENT: NexusAction = new_action_noop_undo(
        "volume-remove-rop.remove-read-only-parent",
        svr_remove_read_only_parent
    );

    // remove temp volume.
    static ref REMOVE_NEW_VOLUME: NexusAction = new_action_noop_undo(
        "volume-remove-rop.remove-new-volume",
        svr_remove_new_volume
    );
}

// volume remove read only parent saga: definition

#[derive(Debug)]
pub struct SagaVolumeRemoveROP;
impl NexusSaga for SagaVolumeRemoveROP {
    const NAME: &'static str = "volume-remove-read-only-parent";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*REMOVE_READ_ONLY_PARENT));
        registry.register(Arc::clone(&*REMOVE_NEW_VOLUME));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "temp_volume_id",
            "GenerateVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));
        builder.append(Node::action(
            "no_result_1",
            "RemoveReadOnlyParent",
            REMOVE_READ_ONLY_PARENT.as_ref(),
        ));
        builder.append(Node::action(
            "final_no_result",
            "RemoveNewVolume",
            REMOVE_NEW_VOLUME.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

// volume remove read only parent saga: action implementations
async fn svr_remove_read_only_parent(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let temp_volume_id =
        sagactx.lookup::<uuid::Uuid>("temp_volume_id")?;

	println!("svr_remove_read_only_parent nv:{}", temp_volume_id);
    osagactx
        .datastore()
        .volume_remove_rop(params.volume_id, temp_volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn svr_remove_new_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let temp_volume_id =
        sagactx.lookup::<uuid::Uuid>("temp_volume_id")?;

	println!("svr_remove_new_volume nv:{}", temp_volume_id);
	osagactx
		.nexus()
        .volume_delete(temp_volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
