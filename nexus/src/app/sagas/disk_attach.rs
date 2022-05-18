// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::external_api::params;
use crate::saga_interface::SagaContext;
use crate::{authn, db};
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

pub const SAGA_NAME: &'static str = "disk-attach";

lazy_static! {
    pub static ref SAGA_TEMPLATE: Arc<SagaTemplate<SagaDiskAttach>> =
        Arc::new(saga_disk_attach());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub instance_id: Uuid,
    pub attach_params: params::InstanceDiskAttach,
}

#[derive(Debug)]
pub struct SagaDiskAttach;
impl SagaType for SagaDiskAttach {
    type SagaParamsType = Arc<Params>;
    type ExecContextType = Arc<SagaContext>;
}

fn saga_disk_attach() -> SagaTemplate<SagaDiskAttach> {
    let mut template_builder = SagaTemplateBuilder::new();

    template_builder.append(
        "attaching_disk",
        "SetDiskStateAttaching",
        ActionFunc::new_action(
            sda_set_disk_record_attaching,
            sda_set_disk_record_attaching_undo,
        ),
    );

    template_builder.append(
        "sled_reported_runtime",
        "UpdateSledAgent",
        ActionFunc::new_action(
            sda_update_sled_agent,
            sda_update_sled_agent_undo,
        ),
    );

    template_builder.append(
        "disk_runtime",
        "SetDiskStateAttached",
        new_action_noop_undo(sda_set_disk_record_attached),
    );

    template_builder.build()
}

async fn sda_set_disk_record_attaching(
    sagactx: ActionContext<SagaDiskAttach>,
) -> Result<db::model::Disk, ActionError> {
    let _osagactx = sagactx.user_data();
    let _params = sagactx.saga_params();

    // TODO: Issue CTE
    //
    // To actually perform the update:
    //
    // - Disk State must be:
    //   Attaching (w/Instance ID = ID | Attached (w/Instance ID = ID) | Detached
    //
    // - Instance state must be:
    //   Running | Starting | Rebooting | Migrating -> Issue attach to sled
    //   Stopping | Stopped | Repairing -> Update DB
    //   _ -> Error
    //
    // - # of attached disks must be less than capacity

    todo!();
}

async fn sda_set_disk_record_attaching_undo(
    sagactx: ActionContext<SagaDiskAttach>,
) -> Result<(), anyhow::Error> {
    let _osagactx = sagactx.user_data();

    // TODO: If we get here, we must have attached the disk.
    // Ergo, set the state to "detached"?
    todo!();
}

async fn sda_update_sled_agent(
    sagactx: ActionContext<SagaDiskAttach>,
) -> Result<String, ActionError> {
    let _log = sagactx.user_data().log();

    // TODO: call "disk_put"
    todo!();
}

async fn sda_update_sled_agent_undo(
    _sagactx: ActionContext<SagaDiskAttach>,
) -> Result<(), anyhow::Error> {

    // TODO: Undo the "disk_put".
    todo!();
}

async fn sda_set_disk_record_attached(
    sagactx: ActionContext<SagaDiskAttach>,
) -> Result<db::model::Volume, ActionError> {
    let _osagactx = sagactx.user_data();

    // TODO: Move the disk state from "Attaching" -> "Attached"

    todo!();
}
