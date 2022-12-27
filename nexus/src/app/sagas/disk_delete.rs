// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub disk_id: Uuid,
}

// disk delete saga: actions

declare_saga_actions! {
    disk_delete;
    DELETE_DISK_RECORD -> "volume_id" {
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        + sdd_delete_disk_record
    }
    DELETE_VOLUME -> "no_result" {
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
        builder.append(delete_volume_action());
        Ok(builder.build()?)
    }
}

// disk delete saga: action implementations

async fn sdd_delete_disk_record(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let volume_id = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(volume_id)
}

async fn sdd_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .nexus()
        .volume_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
