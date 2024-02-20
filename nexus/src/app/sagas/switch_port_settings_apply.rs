// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::NexusActionContext;
use crate::app::sagas::switch_port_settings_common::{
    ensure_switch_port_bgp_settings, select_mg_client, switch_sled_agent,
    write_bootstore_config,
};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use anyhow::Error;
use db::datastore::SwitchPortSettingsCombinedResult;
use nexus_db_model::NETWORK_KEY;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use nexus_db_queries::{authn, db};
use omicron_common::api::external::NameOrId;
use omicron_common::api::internal::shared::SwitchLocation;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

// switch port settings apply saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub switch_port_id: Uuid,
    pub switch_port_settings_id: Uuid,
    pub switch_port_name: String,
}

// switch port settings apply: actions

declare_saga_actions! {
    switch_port_settings_apply;
    ASSOCIATE_SWITCH_PORT -> "original_switch_port_settings_id" {
        + spa_associate_switch_port
        - spa_disassociate_switch_port
    }
    GET_SWITCH_PORT_SETTINGS -> "switch_port_settings" {
        + spa_get_switch_port_settings
    }
    ENSURE_SWITCH_PORT_BGP_SETTINGS -> "ensure_switch_port_bgp_settings" {
        + spa_ensure_switch_port_bgp_settings
        - spa_undo_ensure_switch_port_bgp_settings
    }
    ENSURE_SWITCH_PORT_BOOTSTORE_NETWORK_SETTINGS -> "ensure_switch_port_bootstore_network_settings" {
        + spa_ensure_switch_port_bootstore_network_settings
        - spa_undo_ensure_switch_port_bootstore_network_settings
    }
}

// switch port settings apply saga: definition

#[derive(Debug)]
pub(crate) struct SagaSwitchPortSettingsApply;

impl NexusSaga for SagaSwitchPortSettingsApply {
    const NAME: &'static str = "switch-port-settings-apply";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        switch_port_settings_apply_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(associate_switch_port_action());
        builder.append(get_switch_port_settings_action());
        builder.append(ensure_switch_port_bgp_settings_action());
        builder.append(ensure_switch_port_bootstore_network_settings_action());
        Ok(builder.build()?)
    }
}

async fn spa_associate_switch_port(
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
    let port =
        nexus.get_switch_port(&opctx, params.switch_port_id).await.map_err(
            |e| ActionError::action_failed(format!("get switch port: {e}")),
        )?;

    // update the switch port settings association
    nexus
        .set_switch_port_settings_id(
            &opctx,
            params.switch_port_id,
            Some(params.switch_port_settings_id),
            UpdatePrecondition::DontCare,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "set switch port settings id {e}"
            ))
        })?;

    Ok(port.port_settings_id)
}

async fn spa_get_switch_port_settings(
    sagactx: NexusActionContext,
) -> Result<SwitchPortSettingsCombinedResult, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let port_settings = nexus
        .switch_port_settings_get(
            &opctx,
            &NameOrId::Id(params.switch_port_settings_id),
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("get switch port settings: {e}"))
        })?;

    Ok(port_settings)
}

async fn spa_undo_ensure_switch_port_bgp_settings(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    use mg_admin_client::types::DeleteNeighborRequest;

    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")
        .map_err(|e| {
            ActionError::action_failed(format!(
                "lookup switch port settings (bgp undo): {e}"
            ))
        })?;

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

async fn spa_ensure_switch_port_bootstore_network_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
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

async fn spa_undo_ensure_switch_port_bootstore_network_settings(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    // The overall saga update failed but the bootstore udpate succeeded.
    // Between now and then other updates may have happened which prevent us
    // from simply undoing the changes we did before, as we may inadvertently
    // roll back changes at the intersection of this failed update and other
    // succesful updates. The only thing we can really do here is attempt a
    // complete update of the bootstore network settings based on the current
    // state in the Nexus databse which, we assume to be consistent at any point
    // in time.

    let nexus = sagactx.user_data().nexus();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Just choosing the sled agent associated with switch0 for no reason.
    let sa = switch_sled_agent(SwitchLocation::Switch0, &sagactx).await?;

    let config = nexus.bootstore_network_config(&opctx).await?;
    write_bootstore_config(&sa, &config).await?;

    Ok(())
}

// a common route representation for dendrite and port settings
#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) struct Route {
    pub dst: IpAddr,
    pub masklen: u8,
    pub nexthop: Option<IpAddr>,
}

async fn spa_disassociate_switch_port(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // set the port settings id back to what it was before the saga started
    let orig_port_settings_id =
        sagactx.lookup::<Option<Uuid>>("original_switch_port_settings_id")?;

    nexus
        .set_switch_port_settings_id(
            &opctx,
            params.switch_port_id,
            orig_port_settings_id,
            UpdatePrecondition::Value(params.switch_port_settings_id),
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "set switch port settings id for disassociate: {e}"
            ))
        })?;

    Ok(())
}

async fn spa_ensure_switch_port_bgp_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")
        .map_err(|e| {
            ActionError::action_failed(format!(
                "lookup switch port settings: {e}"
            ))
        })?;

    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    ensure_switch_port_bgp_settings(
        sagactx,
        &opctx,
        settings,
        params.switch_port_name.clone(),
        params.switch_port_id,
    )
    .await
}
