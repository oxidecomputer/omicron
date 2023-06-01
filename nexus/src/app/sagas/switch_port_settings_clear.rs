// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use crate::app::sagas::switch_port_settings_apply::api_to_dpd_port_settings;
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::authn;
use crate::db::datastore::UpdatePrecondition;
use anyhow::Error;
use dpd_client::types::PortId;
use omicron_common::api::external::{self, NameOrId};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub switch_port_id: Uuid,
    pub port_name: String,
}

declare_saga_actions! {
    switch_port_settings_clear;
    DISASSOCIATE_SWITCH_PORT -> "original_switch_port_settings" {
        + spa_disassociate_switch_port
        - spa_reassociate_switch_port
    }
    CLEAR_SWITCH_PORT_SETTINGS -> "switch_port_settings" {
        + spa_clear_switch_port_settings
        - spa_undo_clear_switch_port_settings
    }
}

#[derive(Debug)]
pub struct SagaSwitchPortSettingsClear;
impl NexusSaga for SagaSwitchPortSettingsClear {
    const NAME: &'static str = "switch-port-settings-clear";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        switch_port_settings_clear_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(disassociate_switch_port_action());
        builder.append(clear_switch_port_settings_action());
        Ok(builder.build()?)
    }
}

async fn spa_disassociate_switch_port(
    sagactx: NexusActionContext,
) -> Result<Option<Uuid>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // first get the current association so we fall back to this on failure
    let port = nexus
        .get_switch_port(&opctx, params.switch_port_id)
        .await
        .map_err(ActionError::action_failed)?;

    // update the switch port settings association
    nexus
        .set_switch_port_settings_id(
            &opctx,
            params.switch_port_id,
            None,
            UpdatePrecondition::DontCare,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(port.port_settings_id)
}

async fn spa_reassociate_switch_port(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // set the port settings id back to what it was before the saga started
    let orig_port_settings_id =
        sagactx.lookup::<Option<Uuid>>("original_switch_port_settings")?;

    nexus
        .set_switch_port_settings_id(
            &opctx,
            params.switch_port_id,
            orig_port_settings_id,
            UpdatePrecondition::Null,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn spa_clear_switch_port_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    dpd_client
        .port_settings_clear(&port_id)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}

async fn spa_undo_clear_switch_port_settings(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| external::Error::internal_error(e))?;

    let orig_port_settings_id = sagactx
        .lookup::<Option<Uuid>>("original_switch_port_settings_id")
        .map_err(|e| external::Error::internal_error(&e.to_string()))?;

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let id = match orig_port_settings_id {
        Some(id) => id,
        None => return Ok(()),
    };

    let settings = nexus
        .switch_port_settings_get(&opctx, &NameOrId::Id(id))
        .await
        .map_err(ActionError::action_failed)?;

    let dpd_port_settings = api_to_dpd_port_settings(&settings)
        .map_err(ActionError::action_failed)?;

    dpd_client
        .port_settings_apply(&port_id, &dpd_port_settings)
        .await
        .map_err(|e| external::Error::internal_error(&e.to_string()))?;

    Ok(())
}
