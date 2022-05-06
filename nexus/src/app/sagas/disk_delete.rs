// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::disk_create::delete_regions;
use crate::saga_interface::SagaContext;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "disk-delete";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaDiskDelete>> =
        Arc::new(saga_disk_delete());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub disk_id: Uuid,
}

#[derive(Debug)]
pub struct SagaDiskDelete;
impl SagaType for SagaDiskDelete {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}

fn saga_disk_delete() -> SagaTemplate<SagaDiskDelete> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "volume_id",
        "DeleteDiskRecord",
        // TODO: See the comment on the "DeleteRegions" step,
        // we may want to un-delete the disk if we cannot remove
        // underlying regions.
        new_action_noop_undo(sdd_delete_disk_record),
    );

    template_builder.append(
        "no_result",
        "DeleteRegions",
        // TODO(https://github.com/oxidecomputer/omicron/issues/612):
        // We need a way to deal with this operation failing, aside from
        // propagating the error to the user.
        //
        // What if the Sled goes offline? Nexus must ultimately be
        // responsible for reconciling this scenario.
        //
        // The current behavior causes the disk deletion saga to
        // fail, but still marks the disk as destroyed.
        new_action_noop_undo(sdd_delete_regions),
    );

    template_builder.append(
        "no_result",
        "DeleteRegionRecords",
        new_action_noop_undo(sdd_delete_region_records),
    );

    template_builder.append(
        "no_result",
        "DeleteVolumeRecord",
        new_action_noop_undo(sdd_delete_volume_record),
    );

    template_builder.build()
}

async fn sdd_delete_disk_record(
    sagactx: ActionContext<SagaDiskDelete>,
) -> Result<Uuid, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params();

    let volume_id = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(volume_id)
}

async fn sdd_delete_regions(
    sagactx: ActionContext<SagaDiskDelete>,
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
    sagactx: ActionContext<SagaDiskDelete>,
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
    sagactx: ActionContext<SagaDiskDelete>,
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
