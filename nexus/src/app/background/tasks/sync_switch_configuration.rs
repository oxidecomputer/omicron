// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating user provided switch configurations
//! to relevant management daemons (dendrite, mgd, sled-agent, etc.)

use crate::app::{
    background::{
        LoadedTargetBlueprint,
        tasks::networking::{api_to_dpd_port_settings, resolve_mgd_clients},
    },
    dpd_clients, switch_zone_address_mappings,
};
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use slog::{Logger, o};

use internal_dns_resolver::Resolver;
use ipnetwork::IpNetwork;
use nexus_db_model::{
    BgpConfig, BootstoreConfig, LoopbackAddress, NETWORK_KEY,
};
use tokio::sync::watch;

use crate::app::background::BackgroundTask;
use display_error_chain::DisplayErrorChain;
use dpd_client::{Client as DpdClient, types as DpdTypes};
use futures::FutureExt;
use futures::future::BoxFuture;
use mg_admin_client::types::{
    ApplyRequest, BgpPeerConfig,
    UnnumberedBgpPeerConfig as MgUnnumberedBgpPeerConfig,
};
use mg_api_types::bgp::policy::{
    ImportExportPolicy4 as MgImportExportPolicy4,
    ImportExportPolicy6 as MgImportExportPolicy6,
};
use mg_api_types::rib::BestpathFanoutRequest;
use mg_api_types::static_routes::{
    AddStaticRoute4Request, AddStaticRoute6Request, StaticRoute4,
    StaticRoute4List, StaticRoute6, StaticRoute6List,
};
use mg_api_types::{
    bgp::config::{
        CheckerSource, Ipv4UnicastConfig, Ipv6UnicastConfig, JitterRange,
        ShaperSource,
    },
    static_routes::{DeleteStaticRoute4Request, DeleteStaticRoute6Request},
};
use nexus_db_queries::{
    context::OpContext,
    db::{DataStore, datastore::SwitchPortSettingsCombinedResult},
};
use nexus_types::external_api::networking;
use nexus_types::identity::{Asset, Resource};
use nexus_types::internal_api::background::IncompleteBootstoreConfigReport;
use nexus_types::internal_api::background::SwitchPortSettingsManagerStatus;
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::{
    address::{Ipv6Subnet, get_sled_address},
    api::external::DataPageParams,
};
use omicron_uuid_kinds::{BgpAnnounceSetUuid, BgpConfigUuid, GenericUuid};
use serde_json::json;
use sled_agent_client::types::HostPortConfig;
use sled_agent_types::early_networking::EarlyNetworkConfigEnvelope;
use sled_agent_types::early_networking::InvalidIpAddrError;
use sled_agent_types::early_networking::LldpAdminStatus;
use sled_agent_types::early_networking::LldpPortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouterPeerType;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_agent_types::system_networking::BlueprintExternalNetworkingConfig;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use sled_agent_types::system_networking::WriteNetworkConfigRequest;
use slog_error_chain::InlineErrorChain;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

const DPD_TAG: Option<&'static str> = Some(OMICRON_DPD_TAG);

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

// This is the default RIB Priority used for static routes.  This mirrors
// the const defined in maghemite in rdb/src/lib.rs.
const DEFAULT_RIB_PRIORITY_STATIC: u8 = 1;

/// Convenience struct for holding both v4 and v6 static route delete requests.
#[derive(Debug)]
struct DeleteStaticRouteRequest {
    v4: DeleteStaticRoute4Request,
    v6: DeleteStaticRoute6Request,
}
impl Default for DeleteStaticRouteRequest {
    fn default() -> Self {
        Self {
            v4: DeleteStaticRoute4Request {
                routes: StaticRoute4List { list: Vec::default() },
            },
            v6: DeleteStaticRoute6Request {
                routes: StaticRoute6List { list: Vec::default() },
            },
        }
    }
}

/// Convenience struct for holding both v4 and v6 static route add requests.
#[derive(Debug)]
struct AddStaticRouteRequest {
    v4: AddStaticRoute4Request,
    v6: AddStaticRoute6Request,
}
impl Default for AddStaticRouteRequest {
    fn default() -> Self {
        Self {
            v4: AddStaticRoute4Request {
                routes: StaticRoute4List { list: Vec::default() },
            },
            v6: AddStaticRoute6Request {
                routes: StaticRoute6List { list: Vec::default() },
            },
        }
    }
}

pub struct SwitchPortSettingsManager {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
}

impl SwitchPortSettingsManager {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        rx_blueprint: watch::Receiver<Option<LoadedTargetBlueprint>>,
    ) -> Self {
        Self { datastore, resolver, rx_blueprint }
    }

    async fn switch_ports(
        &mut self,
        opctx: &OpContext,
        log: &slog::Logger,
    ) -> Result<Vec<nexus_db_model::SwitchPort>, serde_json::Value> {
        let port_list = match self
            .datastore
            .switch_port_list(opctx, &DataPageParams::max_page())
            .await
        {
            Ok(port_list) => port_list,
            Err(e) => {
                error!(
                    &log,
                    "failed to enumerate switch ports";
                    "error" => format!("{:#}", e)
                );
                return Err(json!({
                    "error":
                        format!(
                            "failed enumerate switch ports: \
                                {:#}",
                            e
                        )
                }));
            }
        };
        Ok(port_list)
    }

    async fn changes(
        &mut self,
        port_list: Vec<nexus_db_model::SwitchPort>,
        opctx: &OpContext,
        log: &slog::Logger,
    ) -> Result<
        Vec<(SwitchSlot, nexus_db_model::SwitchPort, PortSettingsChange)>,
        serde_json::Value,
    > {
        let mut changes = Vec::new();
        for port in port_list {
            let switch_slot = SwitchSlot::from(port.switch_slot);
            let id = match port.port_settings_id {
                Some(id) => id,
                _ => {
                    changes.push((
                        switch_slot,
                        port,
                        PortSettingsChange::Clear,
                    ));
                    continue;
                }
            };

            info!(
                log,
                "fetching switch port settings";
                "switch_slot" => ?switch_slot,
                "port" => ?port,
            );

            let settings = match self
                .datastore
                .switch_port_settings_get(opctx, &id.into())
                .await
            {
                Ok(settings) => settings,
                Err(e) => {
                    error!(
                        &log,
                        "failed to get switch port settings";
                        "switch_port_settings_id" => ?id,
                        "error" => format!("{:#}", e)
                    );
                    return Err(json!({
                        "error":
                            format!(
                                "failed to get switch port settings: \
                                    {:#}",
                                e
                            )
                    }));
                }
            };

            changes.push((
                switch_slot,
                port,
                PortSettingsChange::Apply(Box::new(settings)),
            ));
        }
        Ok(changes)
    }

    async fn db_loopback_addresses(
        &mut self,
        opctx: &OpContext,
    ) -> Result<
        HashSet<(SwitchSlot, IpAddr)>,
        omicron_common::api::external::Error,
    > {
        let values = self
            .datastore
            .loopback_address_list(opctx, &DataPageParams::max_page())
            .await?;

        let mut set: HashSet<(SwitchSlot, IpAddr)> = HashSet::new();

        // TODO: are we doing anything special with anycast addresses at the moment?
        for LoopbackAddress { switch_slot, address, .. } in values.iter() {
            set.insert((SwitchSlot::from(*switch_slot), address.ip()));
        }

        Ok(set)
    }
}

#[derive(Debug)]
enum PortSettingsChange {
    Apply(Box<SwitchPortSettingsCombinedResult>),
    Clear,
}

impl BackgroundTask for SwitchPortSettingsManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let log = opctx.log.clone();

            let racks = match self.datastore.rack_list_initialized(opctx, &DataPageParams::max_page()).await {
                Ok(racks) => racks,
                Err(e) => {
                    error!(log, "failed to retrieve racks from database";
                        "error" => %DisplayErrorChain::new(&e)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to retrieve racks from database : {}",
                                DisplayErrorChain::new(&e)
                            )
                    });
                },
            };

            let mut status = SwitchPortSettingsManagerStatus::default();

            // TODO: https://github.com/oxidecomputer/omicron/issues/3090
            // Here we're iterating over racks because that's technically the correct thing to do,
            // but our logic for pulling switch ports and their related configurations
            // *isn't* per-rack, so that's something we'll need to revisit in the future.
            for rack in &racks {
                let rack_id = rack.id().to_string();
                let log = log.new(o!("rack_id" => rack_id));

                // lookup switch zones via DNS
                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                let mappings = match
                    switch_zone_address_mappings(&self.resolver, &log).await
                {
                    Ok(mappings) => mappings,
                    Err(e) => {
                        error!(
                            log,
                            "failed to resolve addresses for switch services";
                            "error" => %e);
                        continue;
                    },
                };

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                // build sled agent clients for sleds that are connected to the switches
                let scrimlet_sled_agent_clients = build_sled_agent_clients(&mappings, &log);

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                let dpd_clients = match
                    dpd_clients(&self.resolver, &log).await
                {
                    Ok(clients) => clients,
                    Err(e) => {
                        error!(
                            log,
                            "failed to resolve addresses for Dendrite";
                            "error" => %e);
                        continue;
                    },
                };

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                // build mgd clients
                let mgd_clients =
                    match resolve_mgd_clients(&self.resolver, &log).await {
                        Ok(clients) => clients,
                        Err(e) => {
                            error!(
                                log,
                                "failed to resolve addresses for MGD";
                                InlineErrorChain::new(&e),
                            );
                            continue;
                        },
                    };

                let port_list = match self.switch_ports(opctx, &log).await {
                    Ok(value) => value,
                    Err(e) => {
                        error!(log, "failed to generate switchports for rack"; "error" => %e);
                        continue;
                    },
                };

                //
                // calculate and apply switch port changes
                //

                let changes = match self.changes(port_list, opctx, &log).await {
                    Ok(value) => value,
                    Err(e) => {
                        error!(log, "failed to generate changeset for switchport settings"; "error" => %e);
                        continue;
                    },
                };

                apply_switch_port_changes(&dpd_clients, &changes, &log).await;

                //
                // calculate and apply routing changes
                //

                // get the static routes on each switch
                let current_static_routes =
                    static_routes_on_switch(&mgd_clients, &log).await;
                info!(&log, "retrieved existing routes"; "routes" => ?current_static_routes);

                // generate the complete set of static routes that should be on a given switch
                let desired_static_routes = static_routes_in_db(&log, &changes);
                info!(&log, "retrieved desired routes"; "routes" => ?desired_static_routes);

                // diff the current and desired routes.
                // Add what is missing from current, remove what is not present in desired.
                let routes_to_add = static_routes_to_add(
                    &desired_static_routes,
                    &current_static_routes,
                    &log,
                );
                info!(&log, "calculated static routes to add"; "routes" => ?routes_to_add);

                let routes_to_del = static_routes_to_del(
                    current_static_routes,
                    desired_static_routes,
                );
                info!(&log, "calculated static routes to delete"; "routes" => ?routes_to_del);

                // delete the unneeded routes first, just in case there is a conflicting route for
                // one we need to add
                if !routes_to_del.is_empty() {
                    info!(&log, "deleting static routes"; "routes" => ?routes_to_del);
                    delete_static_routes(&mgd_clients, routes_to_del, &log).await;
                }

                // add the new routes
                if !routes_to_add.is_empty() {
                    info!(&log, "adding static routes"; "routes" => ?routes_to_add);
                    add_static_routes(&mgd_clients, routes_to_add, &log).await;
                }


                //
                // calculate and apply loopback address changes
                //

                info!(&log, "checking for changes to loopback addresses");
                match self.db_loopback_addresses(opctx).await {
                    Ok(desired_loopback_addresses) => {
                        let current_loopback_addresses = switch_loopback_addresses(&dpd_clients, &log).await;

                        let loopbacks_to_add: Vec<(SwitchSlot, IpAddr)> = desired_loopback_addresses
                            .difference(&current_loopback_addresses)
                            .map(|i| (i.0, i.1))
                            .collect();
                        let loopbacks_to_del: Vec<(SwitchSlot, IpAddr)> = current_loopback_addresses
                            .difference(&desired_loopback_addresses)
                            .map(|i| (i.0, i.1))
                            .collect();

                        if !loopbacks_to_del.is_empty() {
                            info!(&log, "deleting loopback addresses"; "addresses" => ?loopbacks_to_del);
                            delete_loopback_addresses_from_switch(&loopbacks_to_del, &dpd_clients, &log).await;
                        }

                        if !loopbacks_to_add.is_empty() {
                            info!(&log, "adding loopback addresses"; "addresses" => ?loopbacks_to_add);
                            add_loopback_addresses_to_switch(&loopbacks_to_add, dpd_clients, &log).await;
                        }
                    },
                    Err(e) => {
                        error!(
                            log,
                            "error fetching loopback addresses from db, skipping loopback config";
                            "error" => %DisplayErrorChain::new(&e)
                        );
                    },
                };

                //
                // calculate and apply switch zone SMF changes
                //
                let uplinks = uplinks(&changes, &log);

                // yeet the messages
                for (switch_slot, config) in &uplinks {
                    let client: &sled_agent_client::Client =
                        match scrimlet_sled_agent_clients.get(switch_slot) {
                            Some(client) => client,
                            None => {
                                error!(log, "sled-agent client is missing, cannot send updates"; "switch_slot" => ?switch_slot);
                                continue;
                            },
                        };

                    info!(
                        &log,
                        "applying SMF config uplink updates to switch zone";
                        "switch_slot" => ?switch_slot,
                        "config" => ?config,
                    );
                    if let Err(e) = client
                        .uplink_ensure(&sled_agent_client::types::SwitchPorts {
                            uplinks: config.clone(),
                        })
                        .await
                    {
                        error!(
                            log,
                            "error while applying smf updates to switch zone";
                            "switch_slot" => ?switch_slot,
                            "error" => %DisplayErrorChain::new(&e)
                        );
                    }
                }

                //
                // calculate and apply BGP changes
                //

                // build a list of desired settings for each switch
                let mut desired_bgp_configs: HashMap<
                        SwitchSlot, (ApplyRequest, BestpathFanoutRequest)
                        > = HashMap::new();

                // we currently only support one bgp config per switch
                let mut switch_bgp_config: HashMap<SwitchSlot, (BgpConfigUuid, BgpConfig)> = HashMap::new();

                // Prefixes are associated to BgpConfig via the config id
                let mut bgp_announce_prefixes: HashMap<BgpAnnounceSetUuid, Vec<IpNet>> = HashMap::new();

                for (switch_slot, port, change) in &changes {
                    let PortSettingsChange::Apply(settings) = change else {
                        continue;
                    };

                    // desired peer configurations for a given switch port
                    let mut peers: HashMap<String, Vec<BgpPeerConfig>> = HashMap::new();
                    let mut unnumbered_peers: HashMap<String, Vec<MgUnnumberedBgpPeerConfig>> = HashMap::new();

                    for peer in &settings.bgp_peers {
                        let bgp_config_id = peer.bgp_config_id();
                        let port_settings_id = peer.port_settings_id();
                        let interface_name = peer.interface_name();
                        let peer = peer.as_bgp_peer();

                        // since we only have one bgp config per switch, we only need to fetch it once
                        let bgp_config = match switch_bgp_config.entry(*switch_slot) {
                            Entry::Occupied(occupied_entry) => {
                                let (existing_id, existing_config) = occupied_entry.get().clone();
                                // verify peers don't have differing configs
                                if existing_id != bgp_config_id {
                                    // should we flag the switch and not do *any* updates to it?
                                    // with the logic as-is, it will skip the config for this port and move on
                                    error!(
                                        log,
                                        "peers do not have matching asn (only one asn allowed per switch)";
                                        "switch_slot" => ?switch_slot,
                                        "first_config_id" => ?existing_id,
                                        "second_config_id" => ?bgp_config_id,
                                    );
                                    break;
                                }
                                existing_config
                            },
                            Entry::Vacant(vacant_entry) => {
                                // get the bgp config for this peer
                                let config = match self
                                    .datastore
                                    .bgp_config_get(
                                        opctx,
                                        &bgp_config_id
                                            .into_untyped_uuid()
                                            .into(),
                                    )
                                    .await
                                {
                                    Ok(config) => config,
                                    Err(e) => {
                                        error!(
                                            log,
                                            "error while fetching bgp peer config from db";
                                            "switch_slot" => ?switch_slot,
                                            "port_name" => %port.port_name,
                                            "error" => %DisplayErrorChain::new(&e)
                                        );
                                        continue;
                                    },
                                };
                                vacant_entry.insert((bgp_config_id, config.clone()));
                                config
                            },
                        };

                        //
                        // build a list of prefixes from the announcements in the bgp config
                        //

                        // Same thing as above, check to see if we've already built the announce set,
                        // if so we'll skip this step
                        #[allow(clippy::map_entry)]
                        if !bgp_announce_prefixes.contains_key(&bgp_config.bgp_announce_set_id()) {
                            let announcements = match self
                                .datastore
                                .bgp_announcement_list(
                                    opctx,
                                    &networking::BgpAnnounceSetSelector {
                                        announce_set: bgp_config
                                            .bgp_announce_set_id()
                                            .into_untyped_uuid()
                                            .into(),
                                    },
                                )
                                .await
                            {
                                Ok(a) => a,
                                Err(e) => {
                                    error!(
                                        log,
                                        "error while fetching bgp announcements from db";
                                        "switch_slot" => ?switch_slot,
                                        "bgp_announce_set_id" => %bgp_config.bgp_announce_set_id(),
                                        "error" => %DisplayErrorChain::new(&e)
                                    );
                                    continue;
                                },
                            };

                            let mut prefixes: Vec<IpNet> = vec![];

                            for announcement in &announcements {
                                match announcement.network.ip() {
                                    IpAddr::V4(value) => {
                                        let ipnet = Ipv4Net::new(value, announcement.network.prefix()).unwrap();
                                        prefixes.push(IpNet::V4(ipnet));
                                    },
                                    IpAddr::V6(value) => {
                                        let ipnet = Ipv6Net::new(value,     announcement.network.prefix()).unwrap();
                                        prefixes.push(IpNet::V6(ipnet));
                                    },
                                };
                            }
                            bgp_announce_prefixes.insert(bgp_config.bgp_announce_set_id(), prefixes);
                        }

                        // NOTE: this mgd-apply path re-reads each peer's
                        // communities and import/export policies via
                        // `communities_for_peer` / `allow_*_for_peer`, even
                        // though `switch_port_settings_get` already loaded
                        // them. That redundancy could be removed too, but is
                        // left for now.
                        //TODO consider awaiting in parallel and joining
                        let communities = match self.datastore.communities_for_peer(
                            opctx,
                            port_settings_id,
                            interface_name,
                            peer.addr,
                        ).await {
                            Ok(cs) => cs,
                            Err(e) => {
                                error!(log,
                                    "failed to get communities for peer";
                                    "peer" => ?peer,
                                    "error" => %DisplayErrorChain::new(&e)
                                );
                                return json!({
                                    "error":
                                        format!(
                                            "failed to get port settings for peer {:?}: {}",
                                            peer,
                                            DisplayErrorChain::new(&e)
                                        )
                                });
                            }
                        };

                        let allow_import = match self.datastore.allow_import_for_peer(
                            opctx,
                            port_settings_id,
                            interface_name,
                            peer.addr,
                        ).await {
                            Ok(cs) => cs,
                            Err(e) => {
                                error!(log,
                                    "failed to get peer allowed imports";
                                    "peer" => ?peer,
                                    "error" => %DisplayErrorChain::new(&e)
                                );
                                return json!({
                                    "error":
                                        format!(
                                            "failed to get allowed imports peer {:?}: {}",
                                            peer,
                                            DisplayErrorChain::new(&e)
                                        )
                                });
                            }
                        };

                        let import_policy4 = match &allow_import {
                            Some(list) => {
                                MgImportExportPolicy4::Allow(list
                                    .clone()
                                    .into_iter()
                                    .filter_map(|x|
                                        match x.prefix {
                                            IpNetwork::V4(p) => Some(p.into()),
                                            IpNetwork::V6(_) => None,
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy4::NoFiltering,
                        };

                        let import_policy6 = match &allow_import {
                            Some(list) => {
                                MgImportExportPolicy6::Allow(list
                                    .clone()
                                    .into_iter()
                                    .filter_map(|x|
                                        match x.prefix {
                                            IpNetwork::V6(p) => Some(p.into()),
                                            IpNetwork::V4(_) => None,
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy6::NoFiltering,
                        };

                        let allow_export = match self.datastore.allow_export_for_peer(
                            opctx,
                            port_settings_id,
                            interface_name,
                            peer.addr,
                        ).await {
                            Ok(cs) => cs,
                            Err(e) => {
                                error!(log,
                                    "failed to get peer allowed exportss";
                                    "peer" => ?peer,
                                    "error" => %DisplayErrorChain::new(&e),
                                );
                                return json!({
                                    "error":
                                        format!(
                                            "failed to get allowed exports peer {:?}: {}",
                                            peer,
                                            DisplayErrorChain::new(&e)
                                        )
                                });
                            }
                        };

                        let export_policy4 = match &allow_export {
                            Some(list) => {
                                MgImportExportPolicy4::Allow(list
                                    .clone()
                                    .into_iter()
                                    .filter_map(|x|
                                        match x.prefix {
                                            IpNetwork::V4(p) => Some(p.into()),
                                            IpNetwork::V6(_) => None,
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy4::NoFiltering,
                        };

                        let export_policy6 = match &allow_export {
                            Some(list) => {
                                MgImportExportPolicy6::Allow(list
                                    .clone()
                                    .into_iter()
                                    .filter_map(|x|
                                        match x.prefix {
                                            IpNetwork::V6(p) => Some(p.into()),
                                            IpNetwork::V4(_) => None,
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy6::NoFiltering,
                        };

                        match peer.addr {
                            // Numbered peer - identified by address
                            RouterPeerType::Numbered { ip } => {
                                // now that the peer passes the above validations, add it to the list for configuration
                                let peer_config = BgpPeerConfig {
                                    name: format!("{ip}"),
                                    host: SocketAddr::new(ip.into(), 179),
                                    hold_time: peer.hold_time.into(),
                                    idle_hold_time: peer.idle_hold_time.into(),
                                    delay_open: peer.delay_open.into(),
                                    connect_retry: peer.connect_retry.into(),
                                    keepalive: peer.keepalive.into(),
                                    resolution: BGP_SESSION_RESOLUTION,
                                    passive: false,
                                    remote_asn: peer.remote_asn,
                                    min_ttl: peer.min_ttl,
                                    md5_auth_key: peer.md5_auth_key.clone(),
                                    multi_exit_discriminator: peer.multi_exit_discriminator,
                                    local_pref: peer.local_pref,
                                    enforce_first_as: peer.enforce_first_as,
                                    communities: communities.into_iter().map(|c| c.community.0).collect(),
                                    ipv4_unicast: Some(Ipv4UnicastConfig{
                                        nexthop: None,
                                        import_policy: import_policy4,
                                        export_policy: export_policy4,
                                    }),
                                    ipv6_unicast: Some(Ipv6UnicastConfig{
                                        nexthop: None,
                                        import_policy: import_policy6,
                                        export_policy: export_policy6,
                                    }),
                                    vlan_id: peer.vlan_id,
                                    connect_retry_jitter: Some(JitterRange {
                                        max: 1.0,
                                        min: 0.75,
                                    }),
                                    deterministic_collision_resolution: false,
                                    idle_hold_jitter: None,
                                    src_port: None,
                                    src_addr: peer.src_addr,
                                };

                                // update the stored vec if it exists, create a new on if it doesn't exist
                                match peers.entry(port.port_name.clone().to_string()) {
                                    Entry::Occupied(mut occupied_entry) => {
                                        occupied_entry.get_mut().push(peer_config);
                                    },
                                    Entry::Vacant(vacant_entry) => {
                                        vacant_entry.insert(vec![peer_config]);
                                    },
                                }
                            }
                            // Unnumbered peer - identified by interface
                            RouterPeerType::Unnumbered { router_lifetime } => {
                                let peer_config = MgUnnumberedBgpPeerConfig {
                                    name: format!("unnumbered-{}", port.port_name),
                                    interface: format!("tfport{}_0", port.port_name),
                                    router_lifetime: router_lifetime.as_u16(),
                                    hold_time: peer.hold_time.into(),
                                    idle_hold_time: peer.idle_hold_time.into(),
                                    delay_open: peer.delay_open.into(),
                                    connect_retry: peer.connect_retry.into(),
                                    keepalive: peer.keepalive.into(),
                                    resolution: BGP_SESSION_RESOLUTION,
                                    passive: false,
                                    remote_asn: peer.remote_asn,
                                    min_ttl: peer.min_ttl,
                                    md5_auth_key: peer.md5_auth_key.clone(),
                                    multi_exit_discriminator: peer.multi_exit_discriminator,
                                    local_pref: peer.local_pref,
                                    enforce_first_as: peer.enforce_first_as,
                                    communities: communities.into_iter().map(|c| c.community.0).collect(),
                                    ipv4_unicast: Some(Ipv4UnicastConfig{
                                        nexthop: None,
                                        import_policy: import_policy4,
                                        export_policy: export_policy4,
                                    }),
                                    ipv6_unicast: Some(Ipv6UnicastConfig{
                                        nexthop: None,
                                        import_policy: import_policy6,
                                        export_policy: export_policy6,
                                    }),
                                    vlan_id: peer.vlan_id,
                                    connect_retry_jitter: Some(JitterRange {
                                        max: 1.0,
                                        min: 0.75,
                                    }),
                                    deterministic_collision_resolution: false,
                                    idle_hold_jitter: None,
                                    src_port: None,
                                    src_addr: peer.src_addr,
                                };

                                // update the stored vec if it exists, create a new on if it doesn't exist
                                match unnumbered_peers.entry(port.port_name.clone().to_string()) {
                                    Entry::Occupied(mut occupied_entry) => {
                                        occupied_entry.get_mut().push(peer_config);
                                    },
                                    Entry::Vacant(vacant_entry) => {
                                        vacant_entry.insert(vec![peer_config]);
                                    },
                                }
                            }
                        }
                    }

                    let (config_id, request_bgp_config) = match switch_bgp_config.get(switch_slot) {
                        Some(config) => config,
                        None => {
                            info!(log, "no bgp config found for switch, skipping."; "switch" => ?switch_slot);
                            continue;
                        },
                    };

                    let request_prefixes = match bgp_announce_prefixes.get(
                        &request_bgp_config.bgp_announce_set_id()) {
                        Some(prefixes) => prefixes,
                        None => {
                            error!(
                                log,
                                "no prefixes to announce found for bgp config";
                                "switch" => ?switch_slot,
                                "announce_set_id" => ?request_bgp_config.bgp_announce_set_id(),
                                "bgp_config_id" => ?config_id,
                            );
                            continue;
                        },
                    };

                    match desired_bgp_configs.entry(*switch_slot) {
                        Entry::Occupied(mut occupied_entry) => {
                            let (config, _) = occupied_entry.get_mut();
                            // peers are the only per-port part of the config.
                            config.peers.extend(peers);
                            config.unnumbered_peers.extend(unnumbered_peers);
                        }
                        Entry::Vacant(vacant_entry) => {
                            let apply_request = ApplyRequest {
                                    asn: *request_bgp_config.asn,
                                    peers,
                                    unnumbered_peers,
                                    originate: request_prefixes.clone(),
                                    checker: request_bgp_config.checker.as_ref().map(|code| CheckerSource{
                                        asn: *request_bgp_config.asn,
                                        code: code.clone(),
                                    }),
                                    shaper: request_bgp_config.shaper.as_ref().map(|code| ShaperSource{
                                        asn: *request_bgp_config.asn,
                                        code: code.clone(),
                                    }),
                                };

                            let fanout = match std::num::NonZeroU8::new(*request_bgp_config.max_paths) {
                                Some(v) => v,
                                None => {
                                    error!(
                                        log,
                                        "Bgp config max_paths was set to zero! Configuring with default value of 1!";
                                        "switch" => ?switch_slot,
                                        "bgp_config_id" => ?config_id,
                                    );
                                    std::num::NonZeroU8::MIN
                                },
                            };

                            let fanout_request = BestpathFanoutRequest { fanout };

                            vacant_entry.insert((apply_request, fanout_request));
                        }
                    }
                }

                for (switch_slot, (config, fanout)) in &desired_bgp_configs {
                    let client = match mgd_clients.get(switch_slot) {
                        Some(client) => client,
                        None => {
                            error!(log, "no mgd client found for switch"; "switch_slot" => ?switch_slot);
                            continue;
                        },
                    };
                    info!(
                        &log,
                        "applying bgp config";
                        "switch_slot" => ?switch_slot,
                        "config" => ?config,
                    );
                    if let Err(e) = client.bgp_apply(config).await {
                        error!(log, "error while applying bgp configuration"; "error" => ?e);
                    }

                    if let Err(e) = client.update_bestpath_fanout(fanout).await {
                        error!(log, "error while updating bestpath fanout"; "error" => ?e);
                    }
                }

                //
                // calculate and apply bootstore changes
                //

                let input = match nexus_switch_config_preparation::read_and_assemble(
                    &self.datastore,
                    opctx,
                    &log,
                    rack.rack_subnet,
                )
                .await
                {
                    Ok(input) => input,
                    Err(e) => {
                        error!(
                            log,
                            "failed to read bootstore network config; \
                             skipping rack";
                            "error" => InlineErrorChain::new(&e),
                        );
                        continue;
                    }
                };

                let rack_network_config =
                    match nexus_switch_config::build_rack_network_config(
                        input,
                    ) {
                        Ok(rack_network_config) => rack_network_config,
                        Err(report) => {
                            error!(
                                log,
                                "incomplete bootstore network config; \
                                 skipping rack";
                                "problems" => %report,
                            );
                            status.incomplete_bootstore_configs.push(
                                IncompleteBootstoreConfigReport {
                                    rack_id: rack.id(),
                                    problems: report
                                        .problems
                                        .iter()
                                        .map(|problem| problem.to_string())
                                        .collect(),
                                },
                            );
                            continue;
                        }
                    };

                let (
                    blueprint_external_networking_generation,
                    service_zone_nat_entries,
                ) = match self
                    .rx_blueprint
                    .borrow_and_update()
                    .clone()
                    .map(|bp| bp.blueprint.to_service_zone_nat_entries().map(
                        |entries| (
                            bp.blueprint.external_networking_generation,
                            entries,
                        )
                    ))
                {
                    Some(Ok((generation, entries))) => (generation, entries),
                    Some(Err(err)) => {
                        error!(
                            log,
                            "cannot construct service zone NAT entries \
                             from blueprint";
                            InlineErrorChain::new(&err),
                        );
                        continue;
                    }
                    None => {
                        warn!(log, "blueprint not yet loaded - skipping sync");
                        continue;
                    }
                };

                // The construction here is slightly weird - we start with
                // `blueprint_external_networking_config: None` and then
                // immediately fill it in. This gives us a non-optional
                // reference to the config we supplied, which we need below to
                // call `does_bootstore_need_update()`.
                let mut desired_config = SystemNetworkingConfig {
                    rack_network_config,
                    blueprint_external_networking_config: None,
                };
                let desired_blueprint_networking_config = &*desired_config
                    .blueprint_external_networking_config
                    .insert(
                        BlueprintExternalNetworkingConfig {
                            blueprint_external_networking_generation,
                            service_zone_nat_entries,
                        },
                    );

                // bootstore_needs_update is a boolean value that determines
                // whether or not we need to increment the bootstore version and
                // push a new config to the sled agents.
                //
                // * If the config we've built from the switchport configuration
                //   information is different from the last config we've cached
                //   in the db, we update the config, cache it in the db, and
                //   apply it.
                // * If the last cached config cannot be succesfully
                //   deserialized into our current bootstore format, we assume
                //   that it is an older format and update the config,
                //   cache it in the db, and apply it.
                // * If there is no last cached config, we assume that this is
                //   the first time this rpw has run for the given rack, so we
                //   update the config, cache it in the db, and apply it.
                // * If we cannot fetch the latest version due to a db error,
                //   something is broken so we don't do anything.
                let bootstore_needs_update = match self
                    .datastore
                    .get_latest_bootstore_config(opctx, NETWORK_KEY.into())
                    .await
                {
                    Ok(Some(BootstoreConfig { data, .. })) => {
                        match EarlyNetworkConfigEnvelope::deserialize_from_value(data.clone())
                            .and_then(|envelope| envelope.deserialize_body())
                        {
                            Ok(config) => {
                                does_bootstore_need_update(
                                    &config,
                                    &desired_config.rack_network_config,
                                    desired_blueprint_networking_config,
                                    &log,
                                )
                            },
                            Err(e) => {
                                error!(
                                    log,
                                    "bootstore config failed to deserialize \
                                     to current EarlyNetworkConfig format";
                                    "key" => %NETWORK_KEY,
                                    "value" => %data,
                                    "error" => %e,
                                );
                                true
                            },
                        }
                    },
                    Ok(None) => {
                        warn!(
                            log,
                            "no bootstore config found in db";
                            "key" => %NETWORK_KEY,
                        );
                        true
                    },
                    Err(e) => {
                        error!(
                            log,
                            "error while fetching last applied bootstore config";
                            "key" => %NETWORK_KEY,
                            "error" => %e,
                        );
                        continue;
                    },
                };

                // The following code is designed to give us the following
                // properties
                // * We only push updates to the bootstore (sled-agents) if
                //   configuration on our side (nexus) has relevant changes.
                // * If the RPW encounters a critical error or crashes at any
                //   point of the operation, it will retry the configuration
                //   again during the next run
                // * We are able to accomplish the above without inspecting
                //   the bootstore on the sled-agents
                //
                // For example, in the event that we crash after pushing to
                // the sled-agents successfully, but before writing the
                // results to the db
                // 1. RPW will restart
                // 2. RPW will build a new network config
                // 3. RPW will compare against the last version stored in the db
                // 4. RPW will decide to apply the config (again)
                // 5. RPW will bump the version (again)
                // 6. RPW will send a new bootstore update to the agents (with
                //    the same info as last time, but with a new version)
                // 7. RPW will record the update in the db
                // 8. We are now back on the happy path
                if bootstore_needs_update {
                    let generation = match self.datastore
                        .bump_bootstore_generation(opctx, NETWORK_KEY.into())
                        .await
                    {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                log,
                                "error while fetching next bootstore generation from db";
                                "key" => %NETWORK_KEY,
                                "error" => %e,
                            );
                            continue;
                        },
                    };

                    info!(
                        &log,
                        "updating bootstore config";
                        "config" => ?desired_config,
                    );

                    let write_request = {
                        let generation = match u64::try_from(generation) {
                            Ok(generation) => generation,
                            Err(_) => {
                                error!(
                                    log,
                                    "got negative generation from db";
                                    "generation" => generation,
                                );
                                continue;
                            }
                        };
                        WriteNetworkConfigRequest {
                            generation,
                            body: desired_config,
                        }
                    };

                    // push the updates to both scrimlets
                    // if both scrimlets are down, bootstore updates aren't happening anyway
                    let mut one_succeeded = false;
                    for (switch_slot, client) in &scrimlet_sled_agent_clients {
                        if let Err(e) = client.write_network_bootstore_config(&write_request).await {
                            error!(
                                log,
                                "error updating bootstore";
                                "switch_slot" => ?switch_slot,
                                "request" => ?write_request,
                                "error" => %e,
                            )
                        } else {
                            one_succeeded = true;
                        }
                    }

                    // if at least one succeeded, record this update in the db
                    if one_succeeded {
                        // Wrap the new config in an envelope to attach the
                        // current body schema version.
                        let envelope = EarlyNetworkConfigEnvelope::from(
                            &write_request.body,
                        );
                        let config = BootstoreConfig {
                            key: NETWORK_KEY.into(),
                            generation,
                            // We're serializing an envelope (guaranteed to be
                            // representable as JSON) to JSON in memory, so this
                            // can't fail.
                            data: serde_json::to_value(&envelope).expect(
                                "EarlyNetworkConfigEnvelope can be serialized \
                                 as JSON",
                            ),
                            time_created: chrono::Utc::now(),
                            time_deleted: None,
                        };
                        if let Err(e) = self.datastore.ensure_bootstore_config(opctx, config.clone()).await {
                            // if this fails, worst case scenario is that we will send the bootstore
                            // information it already has on the next run
                            error!(
                                log,
                                "error while caching bootstore config in db";
                                "config" => ?config,
                                "error" => %e,
                            );
                        }
                    }
                }
            }
            // TODO: we have some early returns in this task. We should instead
            // collect all problems and return them in the response.
            //
            // As part of that, we should move the body into an
            // `activate_impl(...)` function which returns a
            // `SwitchPortManagerStatus`. That'll force us to not do early
            // returns.
            json!(status)
        }
        .boxed()
    }
}

fn hashset_eq<T>(left: &[T], right: &[T]) -> bool
where
    T: Hash + Eq,
{
    let left = left.iter().collect::<HashSet<&T>>();
    let right = right.iter().collect::<HashSet<&T>>();
    left == right
}

/// Ensure that a loopback address is created.
///
/// loopback_ipv\[46\]_create are not idempotent (see
/// oxidecomputer/dendrite#343), but this wrapper function is. Call this
/// from sagas instead.
async fn ensure_loopback_created(
    log: &slog::Logger,
    client: &DpdClient,
    address: IpAddr,
    tag: &str,
) -> Result<(), serde_json::Value> {
    let result = match &address {
        IpAddr::V4(a) => {
            client
                .loopback_ipv4_create(&DpdTypes::Ipv4Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
        }
        IpAddr::V6(a) => {
            client
                .loopback_ipv6_create(&DpdTypes::Ipv6Entry {
                    addr: *a,
                    tag: tag.into(),
                })
                .await
        }
    };

    match result {
        Ok(_) => {
            info!(log, "created loopback address"; "address" => ?address);
            Ok(())
        }
        Err(e) => match e.status() {
            Some(http::StatusCode::CONFLICT) => {
                info!(log, "loopback address already created"; "address" => ?address);

                Ok(())
            }

            _ => Err(json!({
            "error":
                format!(
                    "failed to create loopback address: \
                        {:#}",
                    e
                )})),
        },
    }
}

/// Ensure that a loopback address is deleted.
///
/// loopback_ipv\[46\]_delete are not idempotent (see
/// oxidecomputer/dendrite#343), but this wrapper function is. Call this
/// from sagas instead.
async fn ensure_loopback_deleted(
    log: &slog::Logger,
    client: &DpdClient,
    address: IpAddr,
) -> Result<(), serde_json::Value> {
    let result = match &address {
        IpAddr::V4(a) => client.loopback_ipv4_delete(&a).await,
        IpAddr::V6(a) => client.loopback_ipv6_delete(&a).await,
    };

    match result {
        Ok(_) => {
            info!(log, "deleted loopback address"; "address" => ?address);
            Ok(())
        }
        Err(e) => match e.status() {
            Some(http::StatusCode::NOT_FOUND) => {
                info!(log, "loopback address already deleted"; "address" => ?address);

                Ok(())
            }

            _ => Err(json!({
            "error":
                format!(
                    "failed to deleted loopback address: \
                        {:#}",
                    e
                )})),
        },
    }
}

async fn add_loopback_addresses_to_switch(
    loopbacks_to_add: &[(SwitchSlot, IpAddr)],
    dpd_clients: HashMap<SwitchSlot, dpd_client::Client>,
    log: &slog::Logger,
) {
    for (switch_slot, address) in loopbacks_to_add {
        let client = match dpd_clients.get(switch_slot) {
            Some(v) => v,
            None => {
                error!(log, "dpd_client is missing, cannot create loopback addresses"; "switch_slot" => ?switch_slot);
                continue;
            }
        };

        if let Err(e) =
            ensure_loopback_created(log, client, *address, OMICRON_DPD_TAG)
                .await
        {
            error!(log, "error while creating loopback address"; "error" => %e);
        };
    }
}

async fn delete_loopback_addresses_from_switch(
    loopbacks_to_del: &[(SwitchSlot, IpAddr)],
    dpd_clients: &HashMap<SwitchSlot, dpd_client::Client>,
    log: &slog::Logger,
) {
    for (switch_slot, address) in loopbacks_to_del {
        let client = match dpd_clients.get(switch_slot) {
            Some(v) => v,
            None => {
                error!(log, "dpd_client is missing, cannot delete loopback addresses"; "switch_slot" => ?switch_slot);
                continue;
            }
        };

        if let Err(e) = ensure_loopback_deleted(log, client, *address).await {
            error!(log, "error while deleting loopback address"; "error" => %e);
        };
    }
}

async fn switch_loopback_addresses(
    dpd_clients: &HashMap<SwitchSlot, dpd_client::Client>,
    log: &slog::Logger,
) -> HashSet<(SwitchSlot, IpAddr)> {
    let mut current_loopback_addresses: HashSet<(SwitchSlot, IpAddr)> =
        HashSet::new();

    for (switch_slot, client) in dpd_clients {
        let ipv4_loopbacks = match client.loopback_ipv4_list().await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    log,
                    "error fetching ipv4 loopback addresses from switch";
                    "switch_slot" => ?switch_slot,
                    "error" => %e,
                );
                continue;
            }
        };

        let ipv6_loopbacks = match client.loopback_ipv6_list().await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    log,
                    "error fetching ipv6 loopback addresses from switch";
                    "switch_slot" => ?switch_slot,
                    "error" => %e,
                );
                continue;
            }
        };

        for entry in ipv4_loopbacks.iter() {
            current_loopback_addresses
                .insert((*switch_slot, IpAddr::V4(entry.addr)));
        }

        for entry in ipv6_loopbacks.iter().filter(|x| x.tag == OMICRON_DPD_TAG)
        {
            current_loopback_addresses
                .insert((*switch_slot, IpAddr::V6(entry.addr)));
        }
    }
    current_loopback_addresses
}

fn uplinks(
    changes: &[(SwitchSlot, nexus_db_model::SwitchPort, PortSettingsChange)],
    log: &slog::Logger,
) -> HashMap<SwitchSlot, Vec<HostPortConfig>> {
    let mut uplinks: HashMap<SwitchSlot, Vec<HostPortConfig>> = HashMap::new();
    for (switch_slot, port, change) in changes {
        let PortSettingsChange::Apply(config) = change else {
            continue;
        };

        let lldp = if config.link_lldp.is_empty() {
            None
        } else {
            let x = &config.link_lldp[0];
            Some(LldpPortConfig {
                status: if x.enabled {
                    LldpAdminStatus::Enabled
                } else {
                    LldpAdminStatus::Disabled
                },
                port_id: x.link_name.clone().map(|p| p.to_string()),
                port_description: x.link_description.clone(),
                chassis_id: x.chassis_id.clone(),
                system_name: x.system_name.clone(),
                system_description: x.system_description.clone(),
                management_addrs: x.management_ip.map(|a| {
                    let ip: oxnet::IpNet = a.into();
                    vec![ip.addr()]
                }),
            })
        };

        let tx_eq = if let Some(c) = config.tx_eq.get(0) {
            Some(TxEqConfig {
                pre1: c.pre1,
                pre2: c.pre2,
                main: c.main,
                post2: c.post2,
                post1: c.post1,
            })
        } else {
            None
        };

        let addrs = match config
            .addresses
            .iter()
            .map(|a| {
                 let address = UplinkAddress::try_from_ip_net_treating_unspecified_as_addrconf(a.address)?;
                 Ok(UplinkAddressConfig {
                     address,
                     vlan_id: a.vlan_id
                 })
            })
            .collect::<Result<_, InvalidIpAddrError>>()
        {
            Ok(addresses) => addresses,
            Err(err) => {
                error!(
                    log,
                    "failed to convert database uplink addresses to \
                     API uplink addresses";
                    "switch_slot" => ?switch_slot,
                    "port" => &port.port_name.to_string(),
                    InlineErrorChain::new(&err),
                );
                continue;
            }
        };

        let config = HostPortConfig {
            port: port.port_name.to_string(),
            addrs,
            lldp,
            tx_eq,
        };

        match uplinks.entry(*switch_slot) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().push(config);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(vec![config]);
            }
        }
    }
    uplinks
}

fn build_sled_agent_clients(
    mappings: &HashMap<SwitchSlot, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchSlot, sled_agent_client::Client> {
    let sled_agent_clients: HashMap<SwitchSlot, sled_agent_client::Client> =
        mappings
            .iter()
            .map(|(switch_slot, addr)| {
                // build sled agent address from switch zone address
                let addr = get_sled_address(Ipv6Subnet::new(*addr));
                let client = sled_agent_client::Client::new(
                    &format!("http://{}", addr),
                    log.clone(),
                );
                (*switch_slot, client)
            })
            .collect();
    sled_agent_clients
}

#[derive(PartialEq, Eq, Hash, Debug)]
enum SwitchStaticRoute {
    V4(StaticRoute4),
    V6(StaticRoute6),
}

type SwitchStaticRoutes = HashSet<SwitchStaticRoute>;

fn static_routes_to_del(
    current_static_routes: HashMap<SwitchSlot, SwitchStaticRoutes>,
    desired_static_routes: HashMap<SwitchSlot, SwitchStaticRoutes>,
) -> HashMap<SwitchSlot, DeleteStaticRouteRequest> {
    let mut routes_to_del = HashMap::new();

    // find routes to remove
    for (switch_slot, routes_on_switch) in &current_static_routes {
        let mut result = DeleteStaticRouteRequest::default();

        let delete = |r: &SwitchStaticRoute| match r {
            SwitchStaticRoute::V4(x) => {
                result.v4.routes.list.push(x.clone());
            }
            SwitchStaticRoute::V6(x) => {
                result.v6.routes.list.push(x.clone());
            }
        };

        if let Some(routes_wanted) = desired_static_routes.get(switch_slot) {
            // if it's on the switch but not desired (in our db), it should be removed
            routes_on_switch.difference(routes_wanted).for_each(delete);
        } else {
            // if no desired routes are present, all routes on this switch should be deleted
            routes_on_switch.into_iter().for_each(delete);
        };

        // Only insert if there are routes to delete
        if !result.v4.routes.list.is_empty()
            || !result.v6.routes.list.is_empty()
        {
            routes_to_del.insert(*switch_slot, result);
        }
    }

    routes_to_del
}

#[allow(clippy::type_complexity)]
fn static_routes_to_add(
    desired_static_routes: &HashMap<SwitchSlot, SwitchStaticRoutes>,
    current_static_routes: &HashMap<SwitchSlot, SwitchStaticRoutes>,
    log: &slog::Logger,
) -> HashMap<SwitchSlot, AddStaticRouteRequest> {
    let mut routes_to_add: HashMap<SwitchSlot, AddStaticRouteRequest> =
        HashMap::new();

    // find routes to add
    for (switch_slot, routes_wanted) in desired_static_routes {
        let routes_on_switch = match current_static_routes.get(&switch_slot) {
            Some(routes) => routes,
            None => {
                warn!(
                    &log,
                    "no discovered routes from switch. it is possible that an earlier api call failed.";
                    "switch_slot" => ?switch_slot,
                );
                continue;
            }
        };
        let mut result = AddStaticRouteRequest::default();
        let missing_routes = routes_wanted.difference(routes_on_switch);
        for r in missing_routes.into_iter() {
            match r {
                SwitchStaticRoute::V4(x) => {
                    result.v4.routes.list.push(x.clone());
                }
                SwitchStaticRoute::V6(x) => {
                    result.v6.routes.list.push(x.clone());
                }
            }
        }

        routes_to_add.insert(*switch_slot, result);
    }

    // filter out switches with no routes to add
    let routes_to_add = routes_to_add
        .into_iter()
        .filter(|(_location, request)| {
            !(request.v4.routes.list.is_empty()
                && request.v6.routes.list.is_empty())
        })
        .collect();

    routes_to_add
}

fn static_routes_in_db(
    log: &Logger,
    changes: &[(SwitchSlot, nexus_db_model::SwitchPort, PortSettingsChange)],
) -> HashMap<SwitchSlot, SwitchStaticRoutes> {
    let mut routes_from_db: HashMap<SwitchSlot, SwitchStaticRoutes> =
        HashMap::new();

    for (switch_slot, _port, change) in changes {
        // we only need to check for ports that have a configuration present. No config == no routes.
        let PortSettingsChange::Apply(settings) = change else {
            continue;
        };
        let mut routes = HashSet::new();
        for route in &settings.routes {
            // convert to appropriate types for comparison and insertion

            // TODO: https://github.com/oxidecomputer/omicron/issues/9801
            // This is a workaround until we have bootstore type versioning.
            // We want to stop using `None` as a sentinel value for DEFAULT,
            // and instead want to use an enum to more accurately represent what
            // is happening.
            let rib_priority = match route.rib_priority {
                Some(v) => v.0,
                None => DEFAULT_RIB_PRIORITY_STATIC,
            };
            let vlan_id = route.vid.map(|x| x.0);

            match (route.gw.ip(), route.dst) {
                (nexthop, IpNetwork::V4(dst)) => {
                    routes.insert(SwitchStaticRoute::V4(StaticRoute4 {
                        nexthop,
                        prefix: dst.into(),
                        vlan_id,
                        rib_priority,
                    }));
                }
                (IpAddr::V6(nexthop), IpNetwork::V6(dst)) => {
                    routes.insert(SwitchStaticRoute::V6(StaticRoute6 {
                        nexthop,
                        prefix: dst.into(),
                        vlan_id,
                        rib_priority,
                    }));
                }
                (nexthop @ IpAddr::V4(_), dst @ IpNetwork::V6(_)) => {
                    error!(log,
                        "v6 destination over v4 nexthop not supported";
                        "nexthop" => nexthop.to_string(),
                        "destination" => dst.to_string(),
                    );
                }
            };
        }

        match routes_from_db.entry(*switch_slot) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().extend(routes);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(routes);
            }
        }
    }
    routes_from_db
}

// apply changes for each port
// if we encounter an error, we log it and keep going instead of bailing
async fn apply_switch_port_changes(
    dpd_clients: &HashMap<SwitchSlot, dpd_client::Client>,
    changes: &[(SwitchSlot, nexus_db_model::SwitchPort, PortSettingsChange)],
    log: &slog::Logger,
) {
    for (switch_slot, switch_port, change) in changes {
        let client = match dpd_clients.get(&switch_slot) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "no DPD client for switch switch_slot";
                    "switch_location" => ?switch_slot
                );
                continue;
            }
        };

        let port_name = switch_port.port_name.clone();

        let dpd_port_id = match DpdTypes::PortId::from_str(port_name.as_str()) {
            Ok(port_id) => port_id,
            Err(e) => {
                error!(
                    &log,
                    "failed to parse switch port id";
                    "db_switch_port_name" => ?switch_port.port_name,
                    "switch_location" => ?switch_slot,
                    "error" => format!("{:#}", e)
                );
                continue;
            }
        };

        let mut config_on_switch =
            match client.port_settings_get(&dpd_port_id, DPD_TAG).await {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        log,
                        "failed to retrieve port setttings from switch";
                        "switch_port_id" => ?port_name,
                        "switch_location" => ?switch_slot,
                        "error" => format!("{:#}", e)
                    );
                    continue;
                }
            };

        // dont consider link local addresses in change computation
        for lnk in config_on_switch.links.values_mut() {
            lnk.addrs.retain(|x| match x {
                IpAddr::V6(addr) => !addr.is_unicast_link_local(),
                _ => true,
            })
        }

        info!(
            log,
            "retrieved port settings from switch";
            "switch_port_id" => ?port_name,
            "settings" => ?config_on_switch,
        );

        match change {
            PortSettingsChange::Apply(settings) => {
                let dpd_port_settings = match api_to_dpd_port_settings(
                    &settings,
                ) {
                    Ok(settings) => settings,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to convert switch port settings";
                            "switch_port_id" => ?port_name,
                            "switch_location" => ?switch_slot,
                            "switch_port_settings_id" => ?settings.settings.id(),
                            "error" => format!("{:#}", e)
                        );
                        continue;
                    }
                };

                if config_on_switch.into_inner() == dpd_port_settings {
                    info!(
                        &log,
                        "port settings up to date, skipping";
                        "switch_port_id" => ?port_name,
                        "switch_location" => ?switch_slot,
                        "switch_port_settings_id" => ?settings.settings.id(),
                    );
                    continue;
                }

                // apply settings via dpd client
                info!(
                    &log,
                    "applying settings to switch port";
                    "switch_location" => ?switch_slot,
                    "port_id" => ?dpd_port_id,
                    "settings" => ?dpd_port_settings,
                );
                match client
                    .port_settings_apply(
                        &dpd_port_id,
                        DPD_TAG,
                        &dpd_port_settings,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            &log,
                            "failed to apply switch port settings";
                            "switch_port_id" => ?port_name,
                            "switch_location" => ?switch_slot,
                            "error" => format!("{:#}", e)
                        );
                    }
                }
            }
            PortSettingsChange::Clear => {
                // clear settings via dpd client
                info!(
                    &log,
                    "clearing switch port settings";
                    "switch_location" => ?switch_slot,
                    "port_id" => ?dpd_port_id,
                );

                if config_on_switch.into_inner().links.is_empty() {
                    info!(
                        &log,
                        "port settings up to date, skipping";
                        "switch_port_id" => ?port_name,
                        "switch_location" => ?switch_slot,
                    );
                    continue;
                }

                match client.port_settings_clear(&dpd_port_id, DPD_TAG).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            &log,
                            "failed to clear switch port settings";
                            "switch_port_id" => ?port_name,
                            "switch_location" => ?switch_slot,
                            "error" => format!("{:#}", e)
                        );
                    }
                }
            }
        }
    }
}

async fn static_routes_on_switch(
    mgd_clients: &HashMap<SwitchSlot, mg_admin_client::Client>,
    log: &slog::Logger,
) -> HashMap<SwitchSlot, SwitchStaticRoutes> {
    let mut routes_on_switch = HashMap::new();

    for (switch_slot, client) in mgd_clients {
        let v4_static_routes = match client.static_list_v4_routes().await {
            Ok(routes) => routes.into_inner(),
            Err(e) => {
                error!(
                    &log,
                    "unable to retrieve v4 routes from switch";
                    "error" => e.to_string(),
                    "switch_location" => ?switch_slot,
                );
                continue;
            }
        };
        let v6_static_routes = match client.static_list_v6_routes().await {
            Ok(routes) => routes.into_inner(),
            Err(e) => {
                error!(
                    &log,
                    "unable to retrieve v6 routes from switch";
                    "error" => e.to_string(),
                    "switch_location" => ?switch_slot,
                );
                continue;
            }
        };

        let routes: Vec<_> =
            v4_static_routes.into_iter().chain(v6_static_routes).collect();

        let mut flattened = HashSet::new();
        for (destination, paths) in &routes {
            for p in paths.iter() {
                if let Ok(prefix) = destination.parse() {
                    flattened.insert(SwitchStaticRoute::V4(StaticRoute4 {
                        prefix,
                        nexthop: p.nexthop,
                        vlan_id: p.vlan_id,
                        rib_priority: p.rib_priority,
                    }));
                } else if let Ok(prefix) = destination.parse::<Ipv6Net>() {
                    let IpAddr::V6(nexthop) = p.nexthop else {
                        error!(log,
                            "v6 destination over v4 nexthop not supported";
                            "nexthop" => p.nexthop.to_string(),
                            "destination" => prefix.to_string(),
                        );
                        continue;
                    };
                    flattened.insert(SwitchStaticRoute::V6(StaticRoute6 {
                        nexthop,
                        prefix,
                        vlan_id: p.vlan_id,
                        rib_priority: p.rib_priority,
                    }));
                } else {
                    error!(
                        log,
                        "failed to parse static route destination: {destination}"
                    );
                }
            }
        }
        routes_on_switch.insert(*switch_slot, flattened);
    }
    routes_on_switch
}

async fn delete_static_routes(
    mgd_clients: &HashMap<SwitchSlot, mg_admin_client::Client>,
    routes_to_del: HashMap<SwitchSlot, DeleteStaticRouteRequest>,
    log: &slog::Logger,
) {
    for (switch_slot, request) in routes_to_del {
        let client = match mgd_clients.get(&switch_slot) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "mgd client not found for switch slot";
                    "switch_slot" => ?switch_slot,
                );
                continue;
            }
        };

        info!(
            &log,
            "removing static routes";
            "switch_slot" => ?switch_slot,
            "request" => ?request,
        );
        if let Err(e) = client.static_remove_v4_route(&request.v4).await {
            error!(
                &log,
                "failed to delete v4 routes from mgd";
                "switch_slot" => ?switch_slot,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
        if let Err(e) = client.static_remove_v6_route(&request.v6).await {
            error!(
                &log,
                "failed to delete v6 routes from mgd";
                "switch_slot" => ?switch_slot,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
    }
}

async fn add_static_routes(
    mgd_clients: &HashMap<SwitchSlot, mg_admin_client::Client>,
    routes_to_add: HashMap<SwitchSlot, AddStaticRouteRequest>,
    log: &slog::Logger,
) {
    for (switch_slot, request) in routes_to_add {
        let client = match mgd_clients.get(&switch_slot) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "mgd client not found for switch slot";
                    "switch_slot" => ?switch_slot,
                );
                continue;
            }
        };

        info!(
            &log,
            "adding static routes";
            "switch_slot" => ?switch_slot,
            "request" => ?request,
        );
        if let Err(e) = client.static_add_v4_route(&request.v4).await {
            error!(
                &log,
                "failed to add v4 routes to mgd";
                "switch_slot" => ?switch_slot,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
        if let Err(e) = client.static_add_v6_route(&request.v6).await {
            error!(
                &log,
                "failed to add v6 routes to mgd";
                "switch_slot" => ?switch_slot,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
    }
}

// Helper to decide whether we should update the replicated bootstore.
//
// `current_contents` are the most-recently-written bootstore contents; it
// contains both a rack network config and a blueprint external networking
// config.
//
// `desired_rack_network_config` and `desired_blueprint_networking_config` are
// what this activation of this background task believes the current
// configuration should be.
//
// At a high level, there are three general possibilities:
//
// 1. Our desired configs match `current_contents`. (Easy and common case: we
//    return `false`.)
// 2. Our desired configs are different from `current_contents`, but we're
//    operating on stale data (i.e., data older than what was used to produce
//    `current_contents`). This can occur if another Nexus executed this same
//    task with a different and slightly-newer view of the world; e.g., if the
//    target blueprint recently changed, that Nexus had loaded the change, and
//    we haven't yet. We must return `false` in this case to avoid overwriting
//    new data with our stale data.
// 3. Our desired config is different from `current_contents` and we are not
//    operating on stale data. We return `true`.
//
// Today, we only partially handle case 2. We store generation numbers that
// allow us to detect a stale `desired_blueprint_networking_config`, but we have
// no way of detecting a stale `desired_rack_network_config`. If
// `desired_blueprint_networking_config` is not stale and either desired config
// is different from `current_contents`, we'll return true.
fn does_bootstore_need_update(
    current_contents: &SystemNetworkingConfig,
    desired_rack_network_config: &RackNetworkConfig,
    desired_blueprint_networking_config: &BlueprintExternalNetworkingConfig,
    log: &slog::Logger,
) -> bool {
    // We should make our decision based on four boolean values: "is the config
    // different" and "is our desired config based on out of date information"
    // for each of our two desired configs. Define a couple of enums here to use
    // instead of `bool` for clarity distinguishing between "are we looking at
    // staleness" or "are we looking at whether there have been changes".
    macro_rules! named_bool_yes_no {
        ($newtype:ident) => {
            #[derive(Clone, Copy)]
            enum $newtype {
                Yes,
                No,
            }
            impl $newtype {
                fn as_bool(self) -> bool {
                    match self {
                        Self::Yes => true,
                        Self::No => false,
                    }
                }
            }
        };
    }
    named_bool_yes_no!(DesiredConfigOutOfDate);
    named_bool_yes_no!(ConfigChanged);

    // Compute staleness and "are there changes" for
    // `desired_blueprint_networking_config`.
    let (is_blueprint_out_of_date, is_blueprint_different) =
        if let Some(current_blueprint_networking_config) =
            current_contents.blueprint_external_networking_config.as_ref()
        {
            let BlueprintExternalNetworkingConfig {
                blueprint_external_networking_generation: current_gen,
                service_zone_nat_entries: current_nat,
            } = current_blueprint_networking_config;

            let BlueprintExternalNetworkingConfig {
                blueprint_external_networking_generation: desired_gen,
                service_zone_nat_entries: desired_nat,
            } = desired_blueprint_networking_config;

            // This check must be "strictly less than", not "<=". It's very
            // possible the blueprint config has not changed (i.e., we'd expect
            // equal generation numbers) but the rack network config (checked
            // below) has. We're only out of date if we know we're strictly
            // older than what's in the bootstore.
            let is_blueprint_out_of_date = if desired_gen < current_gen {
                warn!(
                    log, "our loaded blueprint generation is out of date";
                    "bootstore-gen" => current_gen,
                    "our-blueprint-gen" => desired_gen,
                );
                DesiredConfigOutOfDate::Yes
            } else {
                DesiredConfigOutOfDate::No
            };

            let is_blueprint_different = if current_nat != desired_nat {
                ConfigChanged::Yes
            } else {
                ConfigChanged::No
            };

            (is_blueprint_out_of_date, is_blueprint_different)
        } else {
            // If the bootstore has no blueprint config, we have no way of
            // detecting stale data; we have to assume it's not stale, because
            // there's definitely been a change we need to write!
            (DesiredConfigOutOfDate::No, ConfigChanged::Yes)
        };

    // Compute staleness and "are there changes" for
    // `desired_rack_network_config`.
    //
    // TODO-correctness We have no way of computing staleness! We must always
    // assume `desired_rack_network_config` is not out of date.
    let is_network_config_out_of_date = DesiredConfigOutOfDate::No;
    let is_network_config_different = {
        let RackNetworkConfig {
            rack_subnet: current_subnet,
            infra_ip_first: current_infra_ip_first,
            infra_ip_last: current_infra_ip_last,
            ports: current_ports,
            bgp: current_bgp,
            bfd: current_bfd,
        } = &current_contents.rack_network_config;

        let RackNetworkConfig {
            rack_subnet: desired_subnet,
            infra_ip_first: desired_infra_ip_first,
            infra_ip_last: desired_infra_ip_last,
            ports: desired_ports,
            bgp: desired_bgp,
            bfd: desired_bfd,
        } = desired_rack_network_config;

        let rnc_differs = !hashset_eq(current_bgp, desired_bgp)
            || !hashset_eq(current_bfd, desired_bfd)
            || !hashset_eq(current_ports.as_slice(), desired_ports.as_slice())
            || current_subnet != desired_subnet
            || current_infra_ip_first != desired_infra_ip_first
            || current_infra_ip_last != desired_infra_ip_last;

        if rnc_differs { ConfigChanged::Yes } else { ConfigChanged::No }
    };

    match (
        is_blueprint_out_of_date,
        is_network_config_out_of_date,
        is_blueprint_different,
        is_network_config_different,
    ) {
        // If either config is out of date, we must not make changes to avoid
        // overwriting newer data. A future task activation will load a
        // different (and newer) set of desired config.
        (DesiredConfigOutOfDate::Yes, _, _, _)
        | (_, DesiredConfigOutOfDate::Yes, _, _) => {
            warn!(
                log, "skipping bootstore update due to stale data";
                "is_blueprint_out_of_date" =>
                    is_blueprint_out_of_date.as_bool(),
                "is_blueprint_different" =>
                    is_blueprint_different.as_bool(),
                "is_network_config_out_of_date" =>
                    is_network_config_out_of_date.as_bool(),
                "is_network_config_different" =>
                    is_network_config_different.as_bool(),
            );
            false
        }

        // If neither config is out of date, has either changed? If so, we do
        // need to write new bootstore contents.
        (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            ConfigChanged::Yes,
            _,
        )
        | (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            _,
            ConfigChanged::Yes,
        ) => {
            info!(
                log, "will update bootstore with new contents";
                "is_network_config_out_of_date" =>
                    is_network_config_out_of_date.as_bool(),
                "is_network_config_different" =>
                    is_network_config_different.as_bool(),
            );
            true
        }

        // The most common case in practice: our desired config is not out of
        // date, but also hasn't changed since the last task activation. We
        // don't need to write anything to the bootstore; it's up to date.
        (
            DesiredConfigOutOfDate::No,
            DesiredConfigOutOfDate::No,
            ConfigChanged::No,
            ConfigChanged::No,
        ) => {
            info!(log, "will not update bootstore: it is up to date");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iddqd::IdOrdMap;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::Vni;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_types::early_networking::PortConfig;
    use sled_agent_types::early_networking::UplinkPorts;
    use sled_agent_types::inventory::SourceNatConfigGeneric;
    use sled_agent_types::system_networking::ServiceZoneNatEntries;
    use sled_agent_types::system_networking::ServiceZoneNatEntry;
    use sled_agent_types::system_networking::ServiceZoneNatKind;

    fn make_rack_network_config(rack_subnet: &str) -> RackNetworkConfig {
        RackNetworkConfig {
            rack_subnet: rack_subnet.parse().unwrap(),
            infra_ip_first: "172.20.15.21".parse().unwrap(),
            infra_ip_last: "172.20.15.22".parse().unwrap(),
            // `UplinkPorts` must be non-empty -- use a single placeholder port.
            ports: UplinkPorts::new(vec![PortConfig::empty_for_tests("qsfp0")])
                .expect("placeholder port list is non-empty"),
            bgp: vec![],
            bfd: vec![],
        }
    }

    fn make_nat_entries(nexus_external_ip: &str) -> ServiceZoneNatEntries {
        ServiceZoneNatEntries::try_from(
            [
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000001"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:101::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:00".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::BoundaryNtp {
                        snat_cfg: SourceNatConfigGeneric::new(
                            "172.20.26.1".parse().unwrap(),
                            0,
                            16383,
                        )
                        .expect("valid snat cfg"),
                    },
                },
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000002"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:102::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:01".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::ExternalDns {
                        external_ip: "172.20.26.2".parse().unwrap(),
                    },
                },
                ServiceZoneNatEntry {
                    zone_id: "00000000-0000-0000-0000-000000000003"
                        .parse()
                        .unwrap(),
                    sled_underlay_ip: "fd00:1122:3344:103::1".parse().unwrap(),
                    nic_mac: "A8:40:25:FF:80:02".parse().unwrap(),
                    vni: Vni::SERVICES_VNI,
                    kind: ServiceZoneNatKind::Nexus {
                        external_ip: nexus_external_ip.parse().unwrap(),
                    },
                },
            ]
            .into_iter()
            .collect::<IdOrdMap<_>>(),
        )
        .expect("valid service zone NAT entries")
    }

    fn make_blueprint_config(
        generation: u32,
        service_zone_nat_entries: ServiceZoneNatEntries,
    ) -> BlueprintExternalNetworkingConfig {
        BlueprintExternalNetworkingConfig {
            blueprint_external_networking_generation: Generation::from_u32(
                generation,
            ),
            service_zone_nat_entries,
        }
    }

    fn make_system_networking_config(
        rnc: RackNetworkConfig,
        blueprint: Option<BlueprintExternalNetworkingConfig>,
    ) -> SystemNetworkingConfig {
        SystemNetworkingConfig {
            rack_network_config: rnc,
            blueprint_external_networking_config: blueprint,
        }
    }

    #[test]
    fn bootstore_update_when_current_has_no_blueprint_config() {
        let logctx = test_setup_log(
            "bootstore_update_when_current_has_no_blueprint_config",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(rnc.clone(), None);
        let desired_blueprint =
            make_blueprint_config(1, make_nat_entries("172.20.26.3"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_no_update_when_desired_blueprint_is_strictly_older() {
        let logctx = test_setup_log(
            "bootstore_no_update_when_desired_blueprint_is_strictly_older",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(5, make_nat_entries("172.20.26.3"))),
        );

        // Intentionally use different NAT entries here; confirm that we do not
        // report needing an update because the generation here (2) is stale
        // (current is 5).
        let desired_blueprint =
            make_blueprint_config(2, make_nat_entries("172.20.26.4"));

        assert!(!does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_desired_blueprint_is_newer_and_nat_differs() {
        let logctx = test_setup_log(
            "bootstore_update_when_desired_blueprint_is_newer_and_nat_differs",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(2, make_nat_entries("172.20.26.3"))),
        );
        let desired_blueprint =
            make_blueprint_config(5, make_nat_entries("172.20.26.4"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    // Pins the "just-transitioned to tracking generation" case explicitly
    // noted in the comment inside `does_bootstore_need_update()`: at gen=1,
    // the bootstore may have been written with stale NAT entries by a Nexus
    // that pre-dates this generation field. Equal gens with different NATs
    // must still trigger an update so the correct gen=1 value gets written.
    //
    // With the current implementation the test would still pass with any
    // generation (1 isn't special), but we only need to test that we handle
    // this case for generation 1. We never expect a blueprint to have different
    // NAT entries without bumping the associated generation number.
    #[test]
    fn bootstore_update_when_blueprints_equal_and_nat_differs_at_gen_1() {
        let logctx = test_setup_log(
            "bootstore_update_when_blueprints_equal_and_nat_differs_at_gen_1",
        );

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let current = make_system_networking_config(
            rnc.clone(),
            Some(make_blueprint_config(1, make_nat_entries("172.20.26.3"))),
        );
        let desired_blueprint =
            make_blueprint_config(1, make_nat_entries("172.20.26.4"));

        assert!(does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_update_when_nat_matches_but_rnc_differs() {
        let logctx =
            test_setup_log("bootstore_update_when_nat_matches_but_rnc_differs");

        let nat = make_nat_entries("172.20.26.3");
        let current_rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let desired_rnc = make_rack_network_config("fd00:1122:3344:200::/56");
        let desired_blueprint = make_blueprint_config(3, nat);

        let current = make_system_networking_config(
            current_rnc,
            Some(desired_blueprint.clone()),
        );

        assert!(does_bootstore_need_update(
            &current,
            &desired_rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }

    #[test]
    fn bootstore_no_update_when_everything_matches() {
        let logctx =
            test_setup_log("bootstore_no_update_when_everything_matches");

        let rnc = make_rack_network_config("fd00:1122:3344:100::/56");
        let nat = make_nat_entries("172.20.26.3");
        let desired_blueprint = make_blueprint_config(3, nat);

        let current = make_system_networking_config(
            rnc.clone(),
            Some(desired_blueprint.clone()),
        );

        assert!(!does_bootstore_need_update(
            &current,
            &rnc,
            &desired_blueprint,
            &logctx.log,
        ));

        logctx.cleanup_successful();
    }
}
