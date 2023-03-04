// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NEXUS_DPD_TAG};
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::db::datastore::UpdatePrecondition;
use crate::{authn, db};
use anyhow::{anyhow, Error};
use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    Ipv4Entry, Ipv6Entry, Link, LinkCreate, LinkId, PortFec, PortId, PortSpeed,
};
use dpd_client::{Cidr, Ipv4Cidr, Ipv6Cidr};
use futures::StreamExt;
use ipnetwork::IpNetwork;
use omicron_common::api::external::NameOrId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

// switch port settings apply saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
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
    ENSURE_SWITCH_PORT_LINK -> "original_switch_link" {
        + spa_ensure_switch_port_link
        - spa_undo_ensure_switch_port_link
    }
    ENSURE_SWITCH_PORT_ADDRS -> "original_switch_port_addrs" {
        + spa_ensure_switch_port_addrs
        - spa_undo_ensure_switch_port_addrs
    }
    ENSURE_SWITCH_PORT_ROUTES -> "original_switch_port_routes" {
        + spa_ensure_switch_port_routes
        - spa_undo_ensure_switch_port_routes
    }
    //TODO links
    //TODO lldp
    //TODO interfaces
    //TODO vlan interfaces
    //TODO bgp peers
}

// switch port settings apply saga: definition

#[derive(Debug)]
pub struct SagaSwitchPortSettingsApply;

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
        builder.append(ensure_switch_port_link_action());
        builder.append(ensure_switch_port_addrs_action());
        builder.append(ensure_switch_port_routes_action());

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
    let port = nexus
        .get_switch_port(&opctx, params.switch_port_id)
        .await
        .map_err(ActionError::action_failed)?;

    // update the switch port settings association
    nexus
        .set_switch_port_settings_id(
            &opctx,
            params.switch_port_id,
            Some(params.switch_port_settings_id),
            UpdatePrecondition::DontCare,
        )
        .await
        .map_err(ActionError::action_failed)?;

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
        .map_err(ActionError::action_failed)?;

    Ok(port_settings)
}

async fn spa_ensure_switch_port_link(
    sagactx: NexusActionContext,
) -> Result<Option<Link>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let port_id: PortId = PortId::from_str(&params.switch_port_name)
        .map_err(|e| ActionError::action_failed(e.to_string()))?;
    //TODO breakouts
    let link_id = &LinkId(0);

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let link = match dpd_client.link_get(&port_id, &link_id).await {
        Err(_) => {
            //TODO handle not found vs other errors
            dpd_client
                .link_create(
                    &port_id,
                    &LinkCreate {
                        fec: PortFec::None,          // TODO as parameter
                        speed: PortSpeed::Speed100G, // TODO as parameter
                        autoneg: true,               // TODO as parameter
                        kr: false,                   // TODO as parameter
                    },
                )
                .await
                .map_err(|e| ActionError::action_failed(e.to_string()))?;
            None
        }
        Ok(link) => Some(link.into_inner()),
    };

    dpd_client
        .link_enabled_set(&port_id, &LinkId(0), true)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(link)
}

async fn spa_undo_ensure_switch_port_link(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let port_id: PortId = PortId::from_str(&params.switch_port_name)
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

async fn spa_ensure_switch_port_addrs(
    sagactx: NexusActionContext,
) -> Result<(HashSet<Ipv4Addr>, HashSet<Ipv6Addr>), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")?;

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let addresses = settings.addresses.iter().map(|a| a.address.ip()).collect();

    ensure_switch_port_addresses(
        &params.switch_port_name,
        &dpd_client,
        &addresses,
    )
    .await
    .map_err(|e| ActionError::action_failed(e.to_string()))
}

async fn spa_undo_ensure_switch_port_addrs(
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

    ensure_switch_port_addresses(
        &params.switch_port_name,
        &dpd_client,
        &addresses,
    )
    .await
    .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}

pub(crate) async fn ensure_switch_port_addresses(
    port_name: &str,
    dpd_client: &Arc<dpd_client::Client>,
    addresses: &Vec<IpAddr>,
) -> Result<(HashSet<Ipv4Addr>, HashSet<Ipv6Addr>), Error> {
    let port_id: PortId = PortId::from_str(port_name)
        .map_err(|e| anyhow!("bad portname: {}: {}", port_name, e))?;
    //TODO breakouts
    let link_id = &LinkId(0);

    // collect live switch port addresses by family
    let switch_v4_addrs: HashSet<Ipv4Addr> = dpd_client
        .link_ipv4_list(&port_id, &link_id, None, None)
        .await?
        .items
        .iter()
        .map(|x| x.addr)
        .collect();

    let switch_v6_addrs: HashSet<Ipv6Addr> = dpd_client
        .link_ipv6_list(&port_id, &link_id, None, None)
        .await?
        .items
        .iter()
        .map(|x| x.addr)
        .collect();

    // collect settings addresses by family
    let mut settings_v4_addrs: HashSet<Ipv4Addr> = HashSet::new();
    let mut settings_v6_addrs: HashSet<Ipv6Addr> = HashSet::new();

    for address in addresses {
        match address {
            IpAddr::V4(a) => settings_v4_addrs.insert(*a),
            IpAddr::V6(a) => settings_v6_addrs.insert(*a),
        };
    }

    // determine addresses to be removed as the set difference of the settings
    // addresses from the live addresses

    let v4_to_remove = switch_v4_addrs.difference(&settings_v4_addrs);
    let v6_to_remove = switch_v6_addrs.difference(&settings_v6_addrs);

    // TODO handle partial dpd updates. Maybe we should think about a
    // transactional interface for dpd?
    for a in v4_to_remove {
        dpd_client.link_ipv4_delete(&port_id, &link_id, a).await?;
    }
    for a in v6_to_remove {
        dpd_client.link_ipv6_delete(&port_id, &link_id, a).await?;
    }

    // determine addresses to add as the set difference of the live addresses
    // from the settings addresses

    let v4_to_add = settings_v4_addrs.difference(&switch_v4_addrs);
    let v6_to_add = settings_v6_addrs.difference(&switch_v6_addrs);

    for a in v4_to_add {
        dpd_client
            .link_ipv4_create(
                &port_id,
                &link_id,
                &Ipv4Entry { addr: *a, tag: NEXUS_DPD_TAG.into() },
            )
            .await?;
    }
    for a in v6_to_add {
        dpd_client
            .link_ipv6_create(
                &port_id,
                &link_id,
                &Ipv6Entry { addr: *a, tag: NEXUS_DPD_TAG.into() },
            )
            .await?;
    }

    Ok((switch_v4_addrs, switch_v6_addrs))
}

// a common route representation for dendrite and port settings
#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) struct Route {
    pub dst: IpAddr,
    pub masklen: u8,
    pub nexthop: Option<IpAddr>,
}

async fn spa_ensure_switch_port_routes(
    sagactx: NexusActionContext,
) -> Result<(HashSet<Route>, HashSet<Route>), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")?;

    let dpd_client: Arc<dpd_client::Client> =
        Arc::clone(&osagactx.nexus().dpd_client);

    let mut routes: Vec<Route> = Vec::new();
    for r in &settings.routes {
        match &r.dst {
            IpNetwork::V4(n) => {
                routes.push(Route {
                    dst: n.ip().into(),
                    masklen: n.prefix(),
                    nexthop: Some(r.gw.ip()),
                });
            }
            IpNetwork::V6(n) => {
                routes.push(Route {
                    dst: n.ip().into(),
                    masklen: n.prefix(),
                    nexthop: Some(r.gw.ip()),
                });
            }
        }
    }

    ensure_switch_port_routes(&params.switch_port_name, &dpd_client, &routes)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))
}

async fn spa_undo_ensure_switch_port_routes(
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

    ensure_switch_port_routes(&params.switch_port_name, &dpd_client, &routes)
        .await
        .map_err(|e| ActionError::action_failed(e.to_string()))?;

    Ok(())
}

pub(crate) async fn ensure_switch_port_routes(
    port_name: &str,
    dpd_client: &Arc<dpd_client::Client>,
    routes: &Vec<Route>,
) -> Result<(HashSet<Route>, HashSet<Route>), Error> {
    let port_id: PortId = PortId::from_str(port_name)
        .map_err(|e| anyhow!("bad portname: {}: {}", port_name, e))?;

    //TODO breakouts
    let link_id = LinkId(0);

    // collect live switch routes
    let mut switch_v4_routes: HashSet<Route> = HashSet::new();

    let mut st = dpd_client.route_ipv4_list_stream(None);
    loop {
        let x = match st.next().await {
            Some(x) => x?,
            None => break,
        };

        if x.switch_port.as_str() != port_name {
            continue;
        }
        let cidr = match x.cidr {
            Cidr::V4(c) => c,
            _ => continue,
        };
        switch_v4_routes.insert(Route {
            dst: cidr.prefix.into(),
            masklen: cidr.prefix_len,
            nexthop: x.nexthop,
        });
    }

    let mut switch_v6_routes: HashSet<Route> = HashSet::new();
    let mut st = dpd_client.route_ipv6_list_stream(None);
    loop {
        let x = match st.next().await {
            Some(x) => x?,
            None => break,
        };

        if x.switch_port.as_str() != port_name {
            continue;
        }
        let cidr = match x.cidr {
            Cidr::V6(c) => c,
            _ => continue,
        };
        switch_v6_routes.insert(Route {
            dst: cidr.prefix.into(),
            masklen: cidr.prefix_len,
            nexthop: x.nexthop,
        });
    }

    // collect setting routes
    let mut settings_v4_routes: HashSet<Route> = HashSet::new();
    let mut settings_v6_routes: HashSet<Route> = HashSet::new();

    for r in routes {
        match &r.dst {
            IpAddr::V4(_) => settings_v4_routes.insert(r.clone()),
            IpAddr::V6(_) => settings_v6_routes.insert(r.clone()),
        };
    }

    // determine routes to be removed as the set difference of the settings
    // routes from the live routes

    let v4_to_remove = switch_v4_routes.difference(&settings_v4_routes);
    let v6_to_remove = switch_v6_routes.difference(&settings_v6_routes);

    for r in v4_to_remove {
        dpd_client
            .route_ipv4_delete(&Ipv4Cidr {
                prefix: match r.dst {
                    IpAddr::V4(a) => a,
                    _ => continue,
                },
                prefix_len: r.masklen,
            })
            .await?;
    }

    for r in v6_to_remove {
        dpd_client
            .route_ipv6_delete(&Ipv6Cidr {
                prefix: match r.dst {
                    IpAddr::V6(a) => a,
                    _ => continue,
                },
                prefix_len: r.masklen,
            })
            .await?;
    }

    // determine addresses to add as the set difference of the live addresses
    // from the settings addresses

    let v4_to_add = settings_v4_routes.difference(&switch_v4_routes);
    let v6_to_add = settings_v6_routes.difference(&switch_v6_routes);

    for r in v4_to_add {
        dpd_client
            .route_ipv4_create(&dpd_client::types::Route {
                cidr: Cidr::V4(Ipv4Cidr {
                    prefix: match r.dst {
                        IpAddr::V4(a) => a,
                        _ => continue,
                    },
                    prefix_len: r.masklen,
                }),
                switch_port: port_id.clone(),
                link: link_id.clone(),
                nexthop: r.nexthop,
                tag: NEXUS_DPD_TAG.into(),
            })
            .await?;
    }

    for r in v6_to_add {
        dpd_client
            .route_ipv6_create(&dpd_client::types::Route {
                cidr: Cidr::V6(Ipv6Cidr {
                    prefix: match r.dst {
                        IpAddr::V6(a) => a,
                        _ => continue,
                    },
                    prefix_len: r.masklen,
                }),
                switch_port: port_id.clone(),
                link: link_id.clone(),
                nexthop: r.nexthop,
                tag: NEXUS_DPD_TAG.into(),
            })
            .await?;
    }

    Ok((switch_v4_routes, switch_v6_routes))
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
        .map_err(ActionError::action_failed)?;

    Ok(())
}
