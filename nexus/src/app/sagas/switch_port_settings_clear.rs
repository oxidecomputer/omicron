// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use crate::app::sagas::switch_port_settings_common::{
    apply_bootstore_update, bootstore_update, read_bootstore_config,
    switch_sled_agent, write_bootstore_config,
};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use anyhow::Error;
use nexus_db_model::NETWORK_KEY;
use nexus_db_queries::authn;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use omicron_common::api::external::{NameOrId, SwitchLocation};
use serde::{Deserialize, Serialize};
use steno::ActionError;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
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
    CLEAR_SWITCH_PORT_BOOTSTORE_NETWORK_SETTINGS -> "clear_switch_port_bootstore_network_settings" {
        + spa_clear_switch_port_bootstore_network_settings
        - spa_undo_clear_switch_port_bootstore_network_settings
    }
}

#[derive(Debug)]
pub(crate) struct SagaSwitchPortSettingsClear;
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
        builder.append(clear_switch_port_bootstore_network_settings_action());
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

async fn spa_clear_switch_port_bootstore_network_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let nexus = sagactx.user_data().nexus();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Just choosing the sled agent associated with switch0 for no reason.
    let sa = switch_sled_agent(SwitchLocation::Switch0, &sagactx).await?;

    let mut config =
        nexus.bootstore_network_config(&opctx).await.map_err(|e| {
            ActionError::action_failed(format!(
                "read nexus bootstore network config: {e}"
            ))
        })?;

    let generation = nexus
        .datastore()
        .bump_bootstore_generation(&opctx, NETWORK_KEY.into())
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "bump bootstore network generation number: {e}"
            ))
        })?;

    config.generation = generation as u64;
    write_bootstore_config(&sa, &config).await?;

    Ok(())
}

async fn spa_undo_clear_switch_port_bootstore_network_settings(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let orig_port_settings_id = sagactx
        .lookup::<Option<Uuid>>("original_switch_port_settings_id")
        .map_err(|e| {
            ActionError::action_failed(format!(
                "original port settings id lookup: {e}"
            ))
        })?;

    let id = match orig_port_settings_id {
        Some(id) => id,
        None => return Ok(()),
    };

    let settings = nexus
        .switch_port_settings_get(&opctx, &NameOrId::Id(id))
        .await
        .map_err(ActionError::action_failed)?;

    // Just choosing the sled agent associated with switch0 for no reason.
    let sa = switch_sled_agent(SwitchLocation::Switch0, &sagactx).await?;

    // Read the current bootstore config, perform the update and write it back.
    let mut config = read_bootstore_config(&sa).await?;
    let update = bootstore_update(
        &nexus,
        &opctx,
        params.switch_port_id,
        &params.port_name,
        &settings,
    )
    .await?;
    apply_bootstore_update(&mut config, &update)?;
    write_bootstore_config(&sa, &config).await?;

    Ok(())
}
