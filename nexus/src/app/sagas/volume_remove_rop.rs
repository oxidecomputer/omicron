// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ActionRegistry, NexusActionContext, NexusSaga, SagaInitError};
use crate::app::sagas;
use crate::app::sagas::NexusAction;
use crate::db;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::VolumeConstructionRequest;
use std::sync::Arc;
use steno::{new_action_noop_undo, ActionError, Node};
use uuid::Uuid;

// Volume remove read only parent saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub volume_id: Uuid,
}

// Volume remove_read_only_parent saga: actions

lazy_static! {
    // A read-only parent is a structure in a volume that indicates that the
    // volume is logically created from this parent. The initial data for the
    // volume (implicitly) comes from the parent volume. In the background,
    // we'll copy data from the parent into the physical storage allocated
    // for this volume and, when that copy has completed, it will no longer
    // be necessary to maintain this link to the read-only parent. At that
    // point, we execute this saga.
    // If this volume was the only one referencing that parent, then it's time
    // to free the underlying storage resources of the parent as well. We can
    // do this with the volume-delete saga, which takes care of correctly
    // identifying whether other volumes are still referencing this parent.
    // But we don't actually want to delete this volume. Instead, we create a
    // temporary volume, move the read-only parent information from this volume
    // to the temporary volume (so that this volume is now independent of the
    // parent, and the temporary volume appears to depend on the parent), and
    // then delete that temporary volume.

    // Create the temporary volume
    static ref CREATE_TEMP_VOLUME: NexusAction = new_action_noop_undo(
        "volume-remove-rop.create-temp-volume",
        svr_create_temp_volume
    );

    // remove the read_only_parent,  attach it to the temp volume.
    static ref REMOVE_READ_ONLY_PARENT: NexusAction = new_action_noop_undo(
        "volume-remove-rop.remove-read-only-parent",
        svr_remove_read_only_parent
    );
}

// volume remove read only parent saga: definition

#[derive(Debug)]
pub struct SagaVolumeRemoveROP;
impl NexusSaga for SagaVolumeRemoveROP {
    const NAME: &'static str = "volume-remove-read-only-parent";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*CREATE_TEMP_VOLUME));
        registry.register(Arc::clone(&*REMOVE_READ_ONLY_PARENT));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        // Generate the temp volume ID this saga will use.
        let temp_volume_id = Uuid::new_v4();
        // Generate the params for the subsaga called at the end.
        let subsaga_params =
            sagas::volume_delete::Params { volume_id: temp_volume_id };
        let subsaga_dag = {
            let subsaga_builder = steno::DagBuilder::new(steno::SagaName::new(
                sagas::volume_delete::SagaVolumeDelete::NAME,
            ));
            sagas::volume_delete::SagaVolumeDelete::make_saga_dag(
                &subsaga_params,
                subsaga_builder,
            )?
        };

        // Add the temp_volume_id to the saga.
        builder.append(Node::constant(
            "temp_volume_id",
            serde_json::to_value(&temp_volume_id).map_err(|e| {
                SagaInitError::SerializeError(String::from("temp_volume_id"), e)
            })?,
        ));

        // Create the temporary volume
        builder.append(Node::action(
            "temp_volume",
            "CreateTempVolume",
            CREATE_TEMP_VOLUME.as_ref(),
        ));

        // Remove the read only parent, attach to temp volume
        builder.append(Node::action(
            "no_result_1",
            "RemoveReadOnlyParent",
            REMOVE_READ_ONLY_PARENT.as_ref(),
        ));

        // Build the params for the subsaga to delete the temp volume
        builder.append(Node::constant(
            "params_for_delete_subsaga",
            serde_json::to_value(&subsaga_params).map_err(|e| {
                SagaInitError::SerializeError(
                    String::from("params_for_delete_subsaga"),
                    e,
                )
            })?,
        ));

        // Call the subsaga to delete the temp volume
        builder.append(Node::subsaga(
            "final_no_result",
            subsaga_dag,
            "params_for_delete_subsaga",
        ));

        Ok(builder.build()?)
    }
}

// volume remove read only parent saga: action implementations

async fn svr_create_temp_volume(
    sagactx: NexusActionContext,
) -> Result<db::model::Volume, ActionError> {
    let osagactx = sagactx.user_data();

    let temp_volume_id = sagactx.lookup::<Uuid>("temp_volume_id")?;

    // Create the crucible VolumeConstructionRequest which we use
    // for the temporary volume.
    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: temp_volume_id,
        block_size: 512,
        sub_volumes: vec![],
        read_only_parent: None,
    };
    let temp_volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to deserialize volume data: {}",
                e,
            )))
        })?;

    let volume = db::model::Volume::new(temp_volume_id, temp_volume_data);
    let volume_created = osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(volume_created)
}

async fn svr_remove_read_only_parent(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let temp_volume_id = sagactx.lookup::<uuid::Uuid>("temp_volume_id")?;

    osagactx
        .datastore()
        .volume_remove_rop(params.volume_id, temp_volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
