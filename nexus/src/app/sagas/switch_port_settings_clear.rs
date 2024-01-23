// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NEXUS_DPD_TAG};
use crate::app::sagas::retry_until_known_result;
use crate::app::sagas::switch_port_settings_common::{
    api_to_dpd_port_settings, apply_bootstore_update, bootstore_update,
    ensure_switch_port_bgp_settings, ensure_switch_port_uplink,
    read_bootstore_config, select_dendrite_client, select_mg_client,
    switch_sled_agent, write_bootstore_config,
};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use anyhow::Error;
use dpd_client::types::PortId;
use mg_admin_client::types::DeleteNeighborRequest;
use nexus_db_model::NETWORK_KEY;
use nexus_db_queries::authn;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use omicron_common::api::external::{self, NameOrId, SwitchLocation};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
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
    CLEAR_SWITCH_PORT_SETTINGS -> "switch_port_settings" {
        + spa_clear_switch_port_settings
        - spa_undo_clear_switch_port_settings
    }
    CLEAR_SWITCH_PORT_UPLINK -> "clear_switch_port_uplink" {
        + spa_clear_switch_port_uplink
        - spa_undo_clear_switch_port_uplink
    }
    CLEAR_SWITCH_PORT_BGP_SETTINGS -> "clear_switch_port_bgp_settings" {
        + spa_clear_switch_port_bgp_settings
        - spa_undo_clear_switch_port_bgp_settings
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
        builder.append(clear_switch_port_settings_action());
        builder.append(clear_switch_port_uplink_action());
        builder.append(clear_switch_port_bgp_settings_action());
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

async fn spa_clear_switch_port_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let log = sagactx.user_data().log();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    let dpd_client =
        select_dendrite_client(&sagactx, &opctx, params.switch_port_id).await?;

    retry_until_known_result(log, || async {
        dpd_client.port_settings_clear(&port_id, Some(NEXUS_DPD_TAG)).await
    })
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
    let log = sagactx.user_data().log();

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| external::Error::internal_error(e.to_string().as_str()))?;

    let orig_port_settings_id = sagactx
        .lookup::<Option<Uuid>>("original_switch_port_settings_id")
        .map_err(|e| external::Error::internal_error(&e.to_string()))?;

    let id = match orig_port_settings_id {
        Some(id) => id,
        None => return Ok(()),
    };

    let settings = nexus
        .switch_port_settings_get(&opctx, &NameOrId::Id(id))
        .await
        .map_err(ActionError::action_failed)?;

    let dpd_client =
        select_dendrite_client(&sagactx, &opctx, params.switch_port_id).await?;

    let dpd_port_settings = api_to_dpd_port_settings(&settings)
        .map_err(ActionError::action_failed)?;

    retry_until_known_result(log, || async {
        dpd_client
            .port_settings_apply(
                &port_id,
                Some(NEXUS_DPD_TAG),
                &dpd_port_settings,
            )
            .await
    })
    .await
    .map_err(|e| external::Error::internal_error(&e.to_string()))?;

    Ok(())
}

async fn spa_clear_switch_port_uplink(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    ensure_switch_port_uplink(
        sagactx,
        &opctx,
        true,
        None,
        params.switch_port_id,
        params.port_name.clone(),
    )
    .await
}

async fn spa_undo_clear_switch_port_uplink(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let id = sagactx
        .lookup::<Option<Uuid>>("original_switch_port_settings_id")
        .map_err(|e| external::Error::internal_error(&e.to_string()))?;
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    Ok(ensure_switch_port_uplink(
        sagactx,
        &opctx,
        false,
        id,
        params.switch_port_id,
        params.port_name.clone(),
    )
    .await?)
}

async fn spa_clear_switch_port_bgp_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
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

    let mg_client: Arc<mg_admin_client::Client> =
        select_mg_client(&sagactx, &opctx, params.switch_port_id)
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "select mg client (undo): {e}"
                ))
            })?;

    for peer in settings.bgp_peers {
        let config = nexus
            .bgp_config_get(&opctx, peer.bgp_config_id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("delete bgp config: {e}"))
            })?;

        mg_client
            .inner
            .delete_neighbor(&DeleteNeighborRequest {
                asn: *config.asn,
                addr: peer.addr.ip(),
            })
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("delete neighbor: {e}"))
            })?;
    }

    Ok(())
}

async fn spa_undo_clear_switch_port_bgp_settings(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let orig_port_settings_id =
        sagactx.lookup::<Option<Uuid>>("original_switch_port_settings_id")?;

    let id = match orig_port_settings_id {
        Some(id) => id,
        None => return Ok(()),
    };

    let settings =
        nexus.switch_port_settings_get(&opctx, &NameOrId::Id(id)).await?;

    Ok(ensure_switch_port_bgp_settings(
        sagactx,
        &opctx,
        settings,
        params.port_name.clone(),
        params.switch_port_id,
    )
    .await?)
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
