// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::NexusAction;
use crate::authn;
use crate::context::OpContext;
use crate::db;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionError;
use steno::ActionFunc;
use steno::Node;
use uuid::Uuid;

// disk delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
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
    static ref SPACE_ACCOUNT: NexusAction = ActionFunc::new_action(
        "disk-delete.account-space",
        sdd_account_space,
        sdd_account_space_undo,
    );
    static ref DELETE_VOLUME: NexusAction = new_action_noop_undo(
        "disk-delete.delete-volume",
        sdd_delete_volume
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
        registry.register(Arc::clone(&*SPACE_ACCOUNT));
        registry.register(Arc::clone(&*DELETE_VOLUME));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "deleted_disk",
            "DeleteDiskRecord",
            DELETE_DISK_RECORD.as_ref(),
        ));
        builder.append(Node::action(
            "no-result",
            "SpaceAccount",
            SPACE_ACCOUNT.as_ref(),
        ));
        builder.append(Node::action(
            "no_result",
            "DeleteVolume",
            DELETE_VOLUME.as_ref(),
        ));
        Ok(builder.build()?)
    }
}

// disk delete saga: action implementations

async fn sdd_delete_disk_record(
    sagactx: NexusActionContext,
) -> Result<db::model::Disk, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let disk = osagactx
        .datastore()
        .project_delete_disk_no_auth(&params.disk_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(disk)
}

// TODO: Not yet idempotent
async fn sdd_account_space(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_provisioning_collection_delete_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            -i64::try_from(deleted_disk.size.to_bytes())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "updating resource provisioning: {e}"
                    ))
                })
                .map_err(ActionError::action_failed)?,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

// TODO: Not yet idempotent
async fn sdd_account_space_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let deleted_disk = sagactx.lookup::<db::model::Disk>("deleted_disk")?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    osagactx
        .datastore()
        .virtual_provisioning_collection_insert_disk(
            &opctx,
            deleted_disk.id(),
            params.project_id,
            i64::try_from(deleted_disk.size.to_bytes())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "updating resource provisioning: {e}"
                    ))
                })
                .map_err(ActionError::action_failed)?,
        )
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}

async fn sdd_delete_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = OpContext::for_saga_action(&sagactx, &params.serialized_authn);
    let volume_id =
        sagactx.lookup::<db::model::Disk>("deleted_disk")?.volume_id;
    osagactx
        .nexus()
        .volume_delete(&opctx, volume_id)
        .await
        .map_err(ActionError::action_failed)?;
    Ok(())
}
