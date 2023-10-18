// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{NexusActionContext, NEXUS_DPD_TAG};
use crate::app::sagas::retry_until_known_result;
use crate::app::sagas::{
    declare_saga_actions, ActionRegistry, NexusSaga, SagaInitError,
};
use crate::Nexus;
use anyhow::Error;
use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortFec, PortId, PortSettings, PortSpeed,
    RouteSettingsV4, RouteSettingsV6,
};
use dpd_client::{Ipv4Cidr, Ipv6Cidr};
use internal_dns::ServiceName;
use ipnetwork::IpNetwork;
use mg_admin_client::types::Prefix4;
use mg_admin_client::types::{ApplyRequest, BgpPeerConfig, BgpRoute};
use nexus_db_model::{SwitchLinkFec, SwitchLinkSpeed};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use nexus_db_queries::{authn, db};
use nexus_types::external_api::params;
use omicron_common::api::external::{self, NameOrId};
use omicron_common::api::internal::shared::{
    ParseSwitchLocationError, SwitchLocation,
};
use serde::{Deserialize, Serialize};
use sled_agent_client::types::PortConfigV1;
use sled_agent_client::types::RouteConfig;
use sled_agent_client::types::{BgpConfig, EarlyNetworkConfig};
use sled_agent_client::types::{
    BgpPeerConfig as OmicronBgpPeerConfig, HostPortConfig,
};
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddrV6;
use std::str::FromStr;
use std::sync::Arc;
use steno::ActionError;
use uuid::Uuid;

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

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
    ENSURE_SWITCH_PORT_SETTINGS -> "ensure_switch_port_settings" {
        + spa_ensure_switch_port_settings
        - spa_undo_ensure_switch_port_settings
    }
    ENSURE_SWITCH_PORT_UPLINK -> "ensure_switch_port_uplink" {
        + spa_ensure_switch_port_uplink
        - spa_undo_ensure_switch_port_uplink
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
        builder.append(ensure_switch_port_settings_action());
        builder.append(ensure_switch_port_uplink_action());
        builder.append(ensure_switch_port_bgp_settings_action());
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

pub(crate) fn api_to_dpd_port_settings(
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<PortSettings, String> {
    let mut dpd_port_settings = PortSettings {
        tag: NEXUS_DPD_TAG.into(),
        links: HashMap::new(),
        v4_routes: HashMap::new(),
        v6_routes: HashMap::new(),
    };

    // TODO handle breakouts
    // https://github.com/oxidecomputer/omicron/issues/3062
    let link_id = LinkId(0);

    let link_settings = LinkSettings {
        // TODO Allow user to configure link properties
        // https://github.com/oxidecomputer/omicron/issues/3061
        params: LinkCreate {
            autoneg: false,
            kr: false,
            fec: PortFec::None,
            speed: PortSpeed::Speed100G,
        },
        addrs: settings.addresses.iter().map(|a| a.address.ip()).collect(),
    };
    dpd_port_settings.links.insert(link_id.to_string(), link_settings);

    for r in &settings.routes {
        match &r.dst {
            IpNetwork::V4(n) => {
                let gw = match r.gw.ip() {
                    IpAddr::V4(gw) => gw,
                    IpAddr::V6(_) => {
                        return Err(
                            "IPv4 destination cannot have IPv6 nexthop".into()
                        )
                    }
                };
                dpd_port_settings.v4_routes.insert(
                    Ipv4Cidr { prefix: n.ip(), prefix_len: n.prefix() }
                        .to_string(),
                    RouteSettingsV4 {
                        link_id: link_id.0,
                        nexthop: gw,
                        vid: r.vid.map(Into::into),
                    },
                );
            }
            IpNetwork::V6(n) => {
                let gw = match r.gw.ip() {
                    IpAddr::V6(gw) => gw,
                    IpAddr::V4(_) => {
                        return Err(
                            "IPv6 destination cannot have IPv4 nexthop".into()
                        )
                    }
                };
                dpd_port_settings.v6_routes.insert(
                    Ipv6Cidr { prefix: n.ip(), prefix_len: n.prefix() }
                        .to_string(),
                    RouteSettingsV6 {
                        link_id: link_id.0,
                        nexthop: gw,
                        vid: r.vid.map(Into::into),
                    },
                );
            }
        }
    }

    Ok(dpd_port_settings)
}

async fn spa_ensure_switch_port_settings(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let log = sagactx.user_data().log();

    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")?;

    let port_id: PortId =
        PortId::from_str(&params.switch_port_name).map_err(|e| {
            ActionError::action_failed(format!("parse port id: {e}"))
        })?;

    let dpd_client: Arc<dpd_client::Client> =
        select_dendrite_client(&sagactx).await?;

    let dpd_port_settings =
        api_to_dpd_port_settings(&settings).map_err(|e| {
            ActionError::action_failed(format!(
                "translate api port settings to dpd port settings: {e}",
            ))
        })?;

    retry_until_known_result(log, || async {
        dpd_client.port_settings_apply(&port_id, &dpd_port_settings).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("dpd port settings apply {e}"))
    })?;

    Ok(())
}

async fn spa_undo_ensure_switch_port_settings(
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

    let port_id: PortId = PortId::from_str(&params.switch_port_name)
        .map_err(|e| external::Error::internal_error(e))?;

    let orig_port_settings_id = sagactx
        .lookup::<Option<Uuid>>("original_switch_port_settings_id")
        .map_err(|e| external::Error::internal_error(&e.to_string()))?;

    let dpd_client: Arc<dpd_client::Client> =
        select_dendrite_client(&sagactx).await?;

    let id = match orig_port_settings_id {
        Some(id) => id,
        None => {
            retry_until_known_result(log, || async {
                dpd_client.port_settings_clear(&port_id).await
            })
            .await
            .map_err(|e| external::Error::internal_error(&e.to_string()))?;

            return Ok(());
        }
    };

    let settings = nexus
        .switch_port_settings_get(&opctx, &NameOrId::Id(id))
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("switch port settings get: {e}"))
        })?;

    let dpd_port_settings =
        api_to_dpd_port_settings(&settings).map_err(|e| {
            ActionError::action_failed(format!(
                "translate api to dpd port settings {e}"
            ))
        })?;

    retry_until_known_result(log, || async {
        dpd_client.port_settings_apply(&port_id, &dpd_port_settings).await
    })
    .await
    .map_err(|e| external::Error::internal_error(&e.to_string()))?;

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

    ensure_switch_port_bgp_settings(sagactx, settings).await
}

pub(crate) async fn ensure_switch_port_bgp_settings(
    sagactx: NexusActionContext,
    settings: SwitchPortSettingsCombinedResult,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let mg_client: Arc<mg_admin_client::Client> =
        select_mg_client(&sagactx).await.map_err(|e| {
            ActionError::action_failed(format!("select mg client: {e}"))
        })?;

    let mut bgp_peer_configs = Vec::new();

    for peer in settings.bgp_peers {
        let config = nexus
            .bgp_config_get(&opctx, peer.bgp_config_id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("get bgp config: {e}"))
            })?;

        let announcements = nexus
            .bgp_announce_list(
                &opctx,
                &params::BgpAnnounceSetSelector {
                    name_or_id: NameOrId::Id(config.bgp_announce_set_id),
                },
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get bgp announcements: {e}"
                ))
            })?;

        // TODO picking the first configured address by default, but this needs
        // to be something that can be specified in the API.
        let nexthop = match settings.addresses.get(0) {
            Some(switch_port_addr) => Ok(switch_port_addr.address.ip()),
            None => Err(ActionError::action_failed(
                "at least one address required for bgp peering".to_string(),
            )),
        }?;

        let nexthop = match nexthop {
            IpAddr::V4(nexthop) => Ok(nexthop),
            IpAddr::V6(_) => Err(ActionError::action_failed(
                "IPv6 nexthop not yet supported".to_string(),
            )),
        }?;

        let mut prefixes = Vec::new();
        for a in &announcements {
            let value = match a.network.ip() {
                IpAddr::V4(value) => Ok(value),
                IpAddr::V6(_) => Err(ActionError::action_failed(
                    "IPv6 announcement not yet supported".to_string(),
                )),
            }?;
            prefixes.push(Prefix4 { value, length: a.network.prefix() });
        }

        let bpc = BgpPeerConfig {
            asn: *config.asn,
            name: format!("{}", peer.addr.ip()), //TODO user defined name?
            host: format!("{}:179", peer.addr.ip()),
            hold_time: peer.hold_time.0.into(),
            idle_hold_time: peer.idle_hold_time.0.into(),
            delay_open: peer.delay_open.0.into(),
            connect_retry: peer.connect_retry.0.into(),
            keepalive: peer.keepalive.0.into(),
            resolution: BGP_SESSION_RESOLUTION,
            routes: vec![BgpRoute { nexthop, prefixes }],
        };

        bgp_peer_configs.push(bpc);
    }

    mg_client
        .inner
        .bgp_apply(&ApplyRequest {
            peer_group: params.switch_port_name.clone(),
            peers: bgp_peer_configs,
        })
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("apply bgp settings: {e}"))
        })?;

    Ok(())
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
        select_mg_client(&sagactx).await.map_err(|e| {
            ActionError::action_failed(format!("select mg client (undo): {e}"))
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

    let settings = sagactx
        .lookup::<SwitchPortSettingsCombinedResult>("switch_port_settings")
        .map_err(|e| {
            ActionError::action_failed(format!(
                "lookup switch port settings (bgp undo): {e}"
            ))
        })?;

    // Just choosing the sled agent associated with switch0 for no reason.
    let sa = switch_sled_agent(SwitchLocation::Switch0, &sagactx).await?;

    // Read the current bootstore config, perform the update and write it back.
    let mut config = read_bootstore_config(&sa).await?;
    let update = bootstore_update(
        &nexus,
        &opctx,
        params.switch_port_id,
        &params.switch_port_name,
        &settings,
    )
    .await?;
    apply_bootstore_update(&mut config, &update)?;
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

async fn spa_ensure_switch_port_uplink(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    ensure_switch_port_uplink(sagactx, false, None).await
}

async fn spa_undo_ensure_switch_port_uplink(
    sagactx: NexusActionContext,
) -> Result<(), Error> {
    Ok(ensure_switch_port_uplink(sagactx, true, None).await?)
}

pub(crate) async fn ensure_switch_port_uplink(
    sagactx: NexusActionContext,
    skip_self: bool,
    inject: Option<Uuid>,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();

    let switch_port = nexus
        .get_switch_port(&opctx, params.switch_port_id)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for uplink: {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(|e| {
            ActionError::action_failed(format!(
                "get switch location for uplink: {e:?}",
            ))
        })?;

    let mut uplinks: Vec<HostPortConfig> = Vec::new();

    // The sled agent uplinks interface is an all or nothing interface, so we
    // need to get all the uplink configs for all the ports.
    let active_ports =
        nexus.active_port_settings(&opctx).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get active switch port settings: {e}"
            ))
        })?;

    for (port, info) in &active_ports {
        // Since we are undoing establishing uplinks for the settings
        // associated with this port we skip adding this ports uplinks
        // to the list - effectively removing them.
        if skip_self && port.id == switch_port.id {
            continue;
        }
        uplinks.push(HostPortConfig {
            port: port.port_name.clone(),
            addrs: info.addresses.iter().map(|a| a.address).collect(),
        })
    }

    if let Some(id) = inject {
        let opctx = crate::context::op_context_for_saga_action(
            &sagactx,
            &params.serialized_authn,
        );
        let settings = nexus
            .switch_port_settings_get(&opctx, &id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get switch port settings for injection: {e}"
                ))
            })?;
        uplinks.push(HostPortConfig {
            port: params.switch_port_name.clone(),
            addrs: settings.addresses.iter().map(|a| a.address).collect(),
        })
    }

    let sc = switch_sled_agent(switch_location, &sagactx).await?;
    sc.uplink_ensure(&sled_agent_client::types::SwitchPorts { uplinks })
        .await
        .map_err(|e| {
            ActionError::action_failed(format!("ensure uplink: {e}"))
        })?;

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

pub(crate) async fn select_dendrite_client(
    sagactx: &NexusActionContext,
) -> Result<Arc<dpd_client::Client>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let switch_port = nexus
        .get_switch_port(&opctx, params.switch_port_id)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for dendrite client selection {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(
            |e: ParseSwitchLocationError| {
                ActionError::action_failed(format!(
                    "get switch location for uplink: {e:?}",
                ))
            },
        )?;

    let dpd_client: Arc<dpd_client::Client> = osagactx
        .nexus()
        .dpd_clients
        .get(&switch_location)
        .ok_or_else(|| {
            ActionError::action_failed(format!(
                "requested switch not available: {switch_location}"
            ))
        })?
        .clone();
    Ok(dpd_client)
}

pub(crate) async fn select_mg_client(
    sagactx: &NexusActionContext,
) -> Result<Arc<mg_admin_client::Client>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let nexus = osagactx.nexus();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let switch_port = nexus
        .get_switch_port(&opctx, params.switch_port_id)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for mg client selection: {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(
            |e: ParseSwitchLocationError| {
                ActionError::action_failed(format!(
                    "get switch location for uplink: {e:?}",
                ))
            },
        )?;

    let mg_client: Arc<mg_admin_client::Client> = osagactx
        .nexus()
        .mg_clients
        .get(&switch_location)
        .ok_or_else(|| {
            ActionError::action_failed(format!(
                "requested switch not available: {switch_location}"
            ))
        })?
        .clone();
    Ok(mg_client)
}

pub(crate) async fn get_scrimlet_address(
    location: SwitchLocation,
    nexus: &Arc<Nexus>,
) -> Result<SocketAddrV6, ActionError> {
    nexus
        .resolver()
        .await
        .lookup_socket_v6(ServiceName::Scrimlet(location))
        .await
        .map_err(|e| e.to_string())
        .map_err(|e| {
            ActionError::action_failed(format!(
                "scrimlet dns lookup failed {e}",
            ))
        })
}

#[derive(Clone, Debug)]
pub struct EarlyNetworkPortUpdate {
    port: PortConfigV1,
    bgp_configs: Vec<BgpConfig>,
}

pub(crate) async fn bootstore_update(
    nexus: &Arc<Nexus>,
    opctx: &OpContext,
    switch_port_id: Uuid,
    switch_port_name: &str,
    settings: &SwitchPortSettingsCombinedResult,
) -> Result<EarlyNetworkPortUpdate, ActionError> {
    let switch_port =
        nexus.get_switch_port(&opctx, switch_port_id).await.map_err(|e| {
            ActionError::action_failed(format!(
                "get switch port for uplink: {e}"
            ))
        })?;

    let switch_location: SwitchLocation =
        switch_port.switch_location.parse().map_err(
            |e: ParseSwitchLocationError| {
                ActionError::action_failed(format!(
                    "get switch location for uplink: {e:?}",
                ))
            },
        )?;

    let mut peer_info = Vec::new();
    let mut bgp_configs = Vec::new();
    for p in &settings.bgp_peers {
        let bgp_config = nexus
            .bgp_config_get(&opctx, p.bgp_config_id.into())
            .await
            .map_err(|e| {
                ActionError::action_failed(format!("get bgp config: {e}"))
            })?;

        let announcements = nexus
            .bgp_announce_list(
                &opctx,
                &params::BgpAnnounceSetSelector {
                    name_or_id: NameOrId::Id(bgp_config.bgp_announce_set_id),
                },
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "get bgp announcements: {e}"
                ))
            })?;

        peer_info.push((p, bgp_config.asn.0));
        bgp_configs.push(BgpConfig {
            asn: bgp_config.asn.0,
            originate: announcements
                .iter()
                .filter_map(|a| match a.network {
                    IpNetwork::V4(net) => Some(net.into()),
                    //TODO v6
                    _ => None,
                })
                .collect(),
        });
    }

    let update = EarlyNetworkPortUpdate {
        port: PortConfigV1 {
            routes: settings
                .routes
                .iter()
                .map(|r| RouteConfig { destination: r.dst, nexthop: r.gw.ip() })
                .collect(),
            addresses: settings.addresses.iter().map(|a| a.address).collect(),
            switch: switch_location,
            port: switch_port_name.into(),
            uplink_port_fec: settings
                .links
                .get(0)
                .map(|l| l.fec)
                .unwrap_or(SwitchLinkFec::None)
                .into(),
            uplink_port_speed: settings
                .links
                .get(0)
                .map(|l| l.speed)
                .unwrap_or(SwitchLinkSpeed::Speed100G)
                .into(),
            bgp_peers: peer_info
                .iter()
                .filter_map(|(p, asn)| {
                    //TODO v6
                    if let IpAddr::V4(addr) = p.addr.ip() {
                        Some(OmicronBgpPeerConfig {
                            asn: *asn,
                            port: switch_port_name.into(),
                            addr,
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        },
        bgp_configs,
    };

    Ok(update)
}

pub(crate) async fn read_bootstore_config(
    sa: &sled_agent_client::Client,
) -> Result<EarlyNetworkConfig, ActionError> {
    Ok(sa
        .read_network_bootstore_config()
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "read bootstore network config: {e}"
            ))
        })?
        .into_inner())
}

pub(crate) async fn write_bootstore_config(
    sa: &sled_agent_client::Client,
    config: &EarlyNetworkConfig,
) -> Result<(), ActionError> {
    sa.write_network_bootstore_config(config).await.map_err(|e| {
        ActionError::action_failed(format!(
            "write bootstore network config: {e}"
        ))
    })?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BootstoreNetworkPortChange {
    previous_port_config: Option<PortConfigV1>,
    changed_bgp_configs: Vec<BgpConfig>,
    added_bgp_configs: Vec<BgpConfig>,
}

pub(crate) fn apply_bootstore_update(
    config: &mut EarlyNetworkConfig,
    update: &EarlyNetworkPortUpdate,
) -> Result<BootstoreNetworkPortChange, ActionError> {
    let mut change = BootstoreNetworkPortChange::default();

    let rack_net_config = match &mut config.body.rack_network_config {
        Some(cfg) => cfg,
        None => {
            return Err(ActionError::action_failed(
                "rack network config not yet initialized".to_string(),
            ))
        }
    };

    for port in &mut rack_net_config.ports {
        if port.port == update.port.port {
            change.previous_port_config = Some(port.clone());
            *port = update.port.clone();
            break;
        }
    }
    if change.previous_port_config.is_none() {
        rack_net_config.ports.push(update.port.clone());
    }

    for updated_bgp in &update.bgp_configs {
        let mut exists = false;
        for resident_bgp in &mut rack_net_config.bgp {
            if resident_bgp.asn == updated_bgp.asn {
                change.changed_bgp_configs.push(resident_bgp.clone());
                *resident_bgp = updated_bgp.clone();
                exists = true;
                break;
            }
        }
        if !exists {
            change.added_bgp_configs.push(updated_bgp.clone());
        }
    }
    rack_net_config.bgp.extend_from_slice(&change.added_bgp_configs);

    Ok(change)
}

pub(crate) async fn switch_sled_agent(
    location: SwitchLocation,
    sagactx: &NexusActionContext,
) -> Result<sled_agent_client::Client, ActionError> {
    let nexus = sagactx.user_data().nexus();
    let sled_agent_addr = get_scrimlet_address(location, nexus).await?;
    Ok(sled_agent_client::Client::new(
        &format!("http://{}", sled_agent_addr),
        sagactx.user_data().log().clone(),
    ))
}
