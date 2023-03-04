// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{
    ensure_switch_port_addresses, ensure_switch_port_routes,
    NexusActionContext, Route,
};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::authn;
use crate::db::datastore::UpdatePrecondition;
use anyhow::Error;
use dpd_client::types::{Link, LinkCreate, LinkId, PortId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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
    CLEAR_SWITCH_PORT_ROUTES -> "original_switch_port_routes" {
        + spa_clear_switch_port_routes
        - spa_undo_clear_switch_port_routes
    }
    CLEAR_SWITCH_PORT_ADDRS -> "original_switch_port_addrs" {
        + spa_clear_switch_port_addrs
        - spa_undo_clear_switch_port_addrs
    }
    CLEAR_SWITCH_PORT_LINK -> "original_switch_link" {
        + spa_clear_switch_port_link
        - spa_undo_clear_switch_port_link
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
        builder.append(clear_switch_port_routes_action());
        builder.append(clear_switch_port_addrs_action());
        builder.append(clear_switch_port_link_action());

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

async fn spa_clear_switch_port_link(
    sagactx: NexusActionContext,
) -> Result<Option<Link>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| ActionError::action_failed(e.to_string()))?;
    //TODO breakouts
    let link_id = &LinkId(0);

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let link = match dpd_client.link_get(&port_id, &link_id).await {
        Err(_) => {
            //TODO handle not found vs other errors
            None
        }
        Ok(link) => Some(link.into_inner()),
    };

    dpd_client
        .link_delete(&port_id, &link_id)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(link)
}

async fn spa_undo_clear_switch_port_link(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let port_id: PortId = PortId::from_str(&params.port_name)
        .map_err(|e| ActionError::action_failed(e.to_string()))?;
    //TODO breakouts
    let link_id = &LinkId(0);

    let original_link: Option<Link> = sagactx.lookup("original_switch_link")?;

    match original_link {
        Some(link) => {
            dpd_client.link_delete(&port_id, &link_id).await?;
            dpd_client
                .link_create(
                    &port_id,
                    &LinkCreate {
                        fec: link.fec,
                        speed: link.speed,
                        autoneg: true, //TODO not in link? dpd bug?
                        kr: false,     //TODO not in link? dpd bug?
                    },
                )
                .await?;
        }
        None => {
            dpd_client.link_delete(&port_id, &link_id).await?;
        }
    }

    Ok(())
}

async fn spa_clear_switch_port_addrs(
    sagactx: NexusActionContext,
) -> Result<(HashSet<Ipv4Addr>, HashSet<Ipv6Addr>), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    ensure_switch_port_addresses(&params.port_name, &dpd_client, &Vec::new())
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))
}

async fn spa_undo_clear_switch_port_addrs(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let (v4, v6): (HashSet<Ipv4Addr>, HashSet<Ipv6Addr>) =
        sagactx.lookup("original_switch_port_addrs")?;

    let mut addresses: Vec<IpAddr> = Vec::new();
    for a in v4 {
        addresses.push(a.into())
    }
    for a in v6 {
        addresses.push(a.into())
    }

    ensure_switch_port_addresses(&params.port_name, &dpd_client, &addresses)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}

async fn spa_clear_switch_port_routes(
    sagactx: NexusActionContext,
) -> Result<(HashSet<Route>, HashSet<Route>), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    ensure_switch_port_routes(&params.port_name, &dpd_client, &Vec::new())
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))
}

async fn spa_undo_clear_switch_port_routes(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let (v4, v6): (HashSet<Route>, HashSet<Route>) =
        sagactx.lookup("original_switch_port_routes")?;

    let mut routes: Vec<Route> = Vec::new();
    for r in v4 {
        routes.push(r.clone())
    }
    for r in v6 {
        routes.push(r.clone())
    }

    ensure_switch_port_routes(&params.port_name, &dpd_client, &routes)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}
