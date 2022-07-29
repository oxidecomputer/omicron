// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::disk_create::delete_regions;
use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::NexusAction;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub disk_id: Uuid,
}

// disk delete saga: actions

lazy_static! {
    static ref DELETE_DISK_RECORD: NexusAction = new_action_noop_undo(
        "disk-delete.delete-disk-record",
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        sdd_delete_disk_record
    );
    static ref DELETE_REGIONS: NexusAction = new_action_noop_undo(
        "disk-delete.delete-regions",
        // TODO(https://github.com/oxidecomputer/omicron/issues/612):
        // We need a way to deal with this operation failing, aside from
        // propagating the error to the user.
        //
        // What if the Sled goes offline? Nexus must ultimately be
        // responsible for reconciling this scenario.
        //
        // The current behavior causes the disk deletion saga to
        // fail, but still marks the disk as destroyed.
        sdd_delete_regions
    );
    static ref DELETE_REGION_RECORDS: NexusAction = new_action_noop_undo(
        "disk-delete.delete-region-records",
        sdd_delete_region_records
    );
    static ref DELETE_VOLUME_RECORD: NexusAction = new_action_noop_undo(
        "disk-delete.delete-volume-record",
        sdd_delete_volume_record
    );
}

// disk delete saga: definition

#[derive(Debug)]
pub struct SagaDiskDelete;
impl NexusSaga for SagaDiskDelete {
    const NAME: &'static str = "disk-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*DELETE_DISK_RECORD));
        registry.register(Arc::clone(&*DELETE_REGIONS));
        registry.register(Arc::clone(&*DELETE_REGION_RECORDS));
        registry.register(Arc::clone(&*DELETE_VOLUME_RECORD));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "volume_id",
            "DeleteDiskRecord",
            DELETE_DISK_RECORD.as_ref(),
        ));
        builder.append(Node::action(
            "no_result1",
            "DeleteRegions",
            DELETE_REGIONS.as_ref(),
        ));
        builder.append(Node::action(
            "no_result2",
            "DeleteRegionRecords",
            DELETE_REGION_RECORDS.as_ref(),
        ));
        builder.append(Node::action(
            "no_result3",
            "DeleteVolumeRecord",
            DELETE_VOLUME_RECORD.as_ref(),
        ));
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

async fn sdd_delete_regions(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    delete_regions(datasets_and_regions)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_region_records(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .datastore()
        .regions_hard_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_volume_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let volume_id = sagactx.lookup::<Uuid>("volume_id")?;
    osagactx
        .datastore()
        .volume_delete(volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
