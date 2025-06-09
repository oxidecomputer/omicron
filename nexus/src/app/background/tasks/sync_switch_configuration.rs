// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating user provided switch configurations
//! to relevant management daemons (dendrite, mgd, sled-agent, etc.)

use crate::app::{
    background::tasks::networking::{
        api_to_dpd_port_settings, build_dpd_clients, build_mgd_clients,
    },
    switch_zone_address_mappings,
};
use oxnet::Ipv4Net;
use slog::o;

use internal_dns_resolver::Resolver;
use ipnetwork::IpNetwork;
use nexus_db_model::{
    AddressLotBlock, BgpConfig, BootstoreConfig, INFRA_LOT, LoopbackAddress,
    NETWORK_KEY, SwitchLinkSpeed,
};
use uuid::Uuid;

use crate::app::background::BackgroundTask;
use display_error_chain::DisplayErrorChain;
use dpd_client::{Client as DpdClient, types as DpdTypes};
use futures::FutureExt;
use futures::future::BoxFuture;
use mg_admin_client::types::{
    AddStaticRoute4Request, ApplyRequest, BgpPeerConfig, CheckerSource,
    DeleteStaticRoute4Request, ImportExportPolicy as MgImportExportPolicy,
    Prefix as MgPrefix, Prefix4, Prefix6, ShaperSource, StaticRoute4,
    StaticRoute4List,
};
use nexus_db_queries::{
    context::OpContext,
    db::{DataStore, datastore::SwitchPortSettingsCombinedResult},
};
use nexus_types::identity::Asset;
use nexus_types::{external_api::params, identity::Resource};
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::{
    address::{Ipv6Subnet, get_sled_address},
    api::{
        external::{DataPageParams, ImportExportPolicy, SwitchLocation},
        internal::shared::ParseSwitchLocationError,
    },
};
use serde_json::json;
use sled_agent_client::types::{
    BgpConfig as SledBgpConfig, BgpPeerConfig as SledBgpPeerConfig,
    EarlyNetworkConfig, EarlyNetworkConfigBody, HostPortConfig,
    LldpAdminStatus, LldpPortConfig, PortConfigV2, RackNetworkConfigV2,
    RouteConfig as SledRouteConfig, TxEqConfig, UplinkAddressConfig,
};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::Arc,
};

const DPD_TAG: Option<&'static str> = Some(OMICRON_DPD_TAG);
const PHY0: &str = "phy0";

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

// This is the default RIB Priority used for static routes.  This mirrors
// the const defined in maghemite in rdb/src/lib.rs.
const DEFAULT_RIB_PRIORITY_STATIC: u8 = 1;

pub struct SwitchPortSettingsManager {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl SwitchPortSettingsManager {
    pub fn new(datastore: Arc<DataStore>, resolver: Resolver) -> Self {
        Self { datastore, resolver }
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
        Vec<(SwitchLocation, nexus_db_model::SwitchPort, PortSettingsChange)>,
        serde_json::Value,
    > {
        let mut changes = Vec::new();
        for port in port_list {
            let location: SwitchLocation =
                match port.switch_location.clone().parse() {
                    Ok(location) => location,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to parse switch location";
                            "switch_location" => ?port.switch_location,
                            "error" => ?e
                        );
                        continue;
                    }
                };

            let id = match port.port_settings_id {
                Some(id) => id,
                _ => {
                    changes.push((location, port, PortSettingsChange::Clear));
                    continue;
                }
            };

            info!(
                log,
                "fetching switch port settings";
                "switch_location" => ?location,
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
                location,
                port,
                PortSettingsChange::Apply(Box::new(settings)),
            ));
        }
        Ok(changes)
    }

    async fn db_loopback_addresses(
        &mut self,
        opctx: &OpContext,
        log: &slog::Logger,
    ) -> Result<
        HashSet<(SwitchLocation, IpAddr)>,
        omicron_common::api::external::Error,
    > {
        let values = self
            .datastore
            .loopback_address_list(opctx, &DataPageParams::max_page())
            .await?;

        let mut set: HashSet<(SwitchLocation, IpAddr)> = HashSet::new();

        // TODO: are we doing anything special with anycast addresses at the moment?
        for LoopbackAddress { switch_location, address, .. } in values.iter() {
            let location: SwitchLocation = match switch_location.parse() {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        log,
                        "failed to parse switch location for loopback address";
                        "address" => %address,
                        "location" => switch_location,
                        "error" => ?e,
                    );
                    continue;
                }
            };
            set.insert((location, address.ip()));
        }

        Ok(set)
    }

    async fn bfd_peer_configs_from_db(
        &mut self,
        opctx: &OpContext,
    ) -> Result<
        Vec<sled_agent_client::types::BfdPeerConfig>,
        omicron_common::api::external::Error,
    > {
        let db_data = self
            .datastore
            .bfd_session_list(opctx, &DataPageParams::max_page())
            .await?;

        let mut result = Vec::new();
        for spec in db_data.into_iter() {
            let config = sled_agent_client::types::BfdPeerConfig {
                local: spec.local.map(|x| x.ip()),
                remote: spec.remote.ip(),
                detection_threshold: spec
                    .detection_threshold
                    .0
                    .try_into()
                    .map_err(|_| {
                        omicron_common::api::external::Error::InternalError {
                            internal_message: format!(
                                "db_bfd_peer_configs: detection threshold \
                                 overflow: {}",
                                spec.detection_threshold.0,
                            ),
                        }
                    })?,
                required_rx: spec.required_rx.0.into(),
                mode: match spec.mode {
                    nexus_db_model::BfdMode::SingleHop => {
                        sled_agent_client::types::BfdMode::SingleHop
                    }
                    nexus_db_model::BfdMode::MultiHop => {
                        sled_agent_client::types::BfdMode::MultiHop
                    }
                },
                switch: spec.switch.parse().map_err(
                    |e: ParseSwitchLocationError| {
                        omicron_common::api::external::Error::InternalError {
                            internal_message: format!(
                                "db_bfd_peer_configs: failed to parse switch \
                                 name: {}: {:?}",
                                spec.switch, e,
                            ),
                        }
                    },
                )?,
            };
            result.push(config);
        }

        Ok(result)
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
                            "failed to resolve addresses for Dendrite services";
                            "error" => %e);
                        continue;
                    },
                };

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                // build sled agent clients
                let sled_agent_clients = build_sled_agent_clients(&mappings, &log);

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                // build dpd clients
                let dpd_clients = build_dpd_clients(&mappings, &log);

                // TODO https://github.com/oxidecomputer/omicron/issues/5201
                // build mgd clients
                let mgd_clients = build_mgd_clients(mappings, &log);

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
                let desired_static_routes = static_routes_in_db(&changes);
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
                match self.db_loopback_addresses(opctx, &log).await {
                    Ok(desired_loopback_addresses) => {
                        let current_loopback_addresses = switch_loopback_addresses(&dpd_clients, &log).await;

                        let loopbacks_to_add: Vec<(SwitchLocation, IpAddr)> = desired_loopback_addresses
                            .difference(&current_loopback_addresses)
                            .map(|i| (i.0, i.1))
                            .collect();
                        let loopbacks_to_del: Vec<(SwitchLocation, IpAddr)> = current_loopback_addresses
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
                let uplinks = uplinks(&changes);

                // yeet the messages
                for (location, config) in &uplinks {
                    let client: &sled_agent_client::Client =
                        match sled_agent_clients.get(location) {
                            Some(client) => client,
                            None => {
                                error!(log, "sled-agent client is missing, cannot send updates"; "location" => %location);
                                continue;
                            },
                        };

                    info!(
                        &log,
                        "applying SMF config uplink updates to switch zone";
                        "switch_location" => ?location,
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
                            "location" => %location,
                            "error" => %DisplayErrorChain::new(&e)
                        );
                    }
                }

                //
                // calculate and apply BGP changes
                //

                // build a list of desired settings for each switch
                let mut desired_bgp_configs: HashMap<
                    SwitchLocation,
                    ApplyRequest,
                > = HashMap::new();

                // we currently only support one bgp config per switch
                let mut switch_bgp_config: HashMap<SwitchLocation, (Uuid, BgpConfig)> = HashMap::new();

                // Prefixes are associated to BgpConfig via the config id
                let mut bgp_announce_prefixes: HashMap<Uuid, Vec<Prefix4>> = HashMap::new();

                for (location, port, change) in &changes {
                    let PortSettingsChange::Apply(settings) = change else {
                        continue;
                    };

                    // desired peer configurations for a given switch port
                    let mut peers: HashMap<String, Vec<BgpPeerConfig>> = HashMap::new();

                    for peer in &settings.bgp_peers {
                        let bgp_config_id = peer.bgp_config_id;

                        // since we only have one bgp config per switch, we only need to fetch it once
                        let bgp_config = match switch_bgp_config.entry(*location) {
                            Entry::Occupied(occupied_entry) => {
                                let (existing_id, existing_config) = occupied_entry.get().clone();
                                // verify peers don't have differing configs
                                if existing_id != bgp_config_id {
                                    // should we flag the switch and not do *any* updates to it?
                                    // with the logic as-is, it will skip the config for this port and move on
                                    error!(
                                        log,
                                        "peers do not have matching asn (only one asn allowed per switch)";
                                        "switch" => ?location,
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
                                    .bgp_config_get(opctx, &bgp_config_id.into())
                                    .await
                                {
                                    Ok(config) => config,
                                    Err(e) => {
                                        error!(
                                            log,
                                            "error while fetching bgp peer config from db";
                                            "location" => %location,
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
                        if !bgp_announce_prefixes.contains_key(&bgp_config.bgp_announce_set_id) {
                            let announcements = match self
                                .datastore
                                .bgp_announcement_list(
                                    opctx,
                                    &params::BgpAnnounceSetSelector {
                                        announce_set: bgp_config
                                            .bgp_announce_set_id
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
                                        "location" => %location,
                                        "bgp_announce_set_id" => %bgp_config.bgp_announce_set_id,
                                        "error" => %DisplayErrorChain::new(&e)
                                    );
                                    continue;
                                },
                            };

                            let mut prefixes: Vec<Prefix4> = vec![];

                            for announcement in &announcements {
                                let value = match announcement.network.ip() {
                                    IpAddr::V4(value) => value,
                                    IpAddr::V6(a) => {
                                        error!(log, "bad request, only ipv4 supported at this time"; "requested_address" => ?a);
                                        continue;
                                    },
                                };
                                prefixes.push(Prefix4 { value, length: announcement.network.prefix() });
                            }
                            bgp_announce_prefixes.insert(bgp_config.bgp_announce_set_id, prefixes);
                        }

                        let ttl = peer.min_ttl.map(|x| x.0);

                        //TODO consider awaiting in parallel and joining
                        let communities = match self.datastore.communities_for_peer(
                            opctx,
                            peer.port_settings_id,
                            &peer.interface_name.to_string(),
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
                            peer.port_settings_id,
                            &peer.interface_name.to_string(),
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

                        let import_policy = match allow_import {
                            Some(list) => {
                                MgImportExportPolicy::Allow(list
                                    .into_iter()
                                    .map(|x|
                                        match x.prefix {
                                            IpNetwork::V4(p) =>  MgPrefix::V4(
                                                Prefix4{
                                                    length: p.prefix(),
                                                    value: p.ip(),
                                                }
                                            ),
                                            IpNetwork::V6(p) =>  MgPrefix::V6(
                                                Prefix6{
                                                    length: p.prefix(),
                                                    value: p.ip(),
                                                }
                                            )
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy::NoFiltering,
                        };

                        let allow_export = match self.datastore.allow_export_for_peer(
                            opctx,
                            peer.port_settings_id,
                            &peer.interface_name.to_string(),
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

                        let export_policy = match allow_export {
                            Some(list) => {
                                MgImportExportPolicy::Allow(list
                                    .into_iter()
                                    .map(|x|
                                        match x.prefix {
                                            IpNetwork::V4(p) =>  MgPrefix::V4(
                                                Prefix4{
                                                    length: p.prefix(),
                                                    value: p.ip(),
                                                }
                                            ),
                                            IpNetwork::V6(p) =>  MgPrefix::V6(
                                                Prefix6{
                                                    length: p.prefix(),
                                                    value: p.ip(),
                                                }
                                            )
                                        }
                                    )
                                    .collect()
                                )
                            }
                            None => MgImportExportPolicy::NoFiltering,
                        };

                        // now that the peer passes the above validations, add it to the list for configuration
                        let peer_config = BgpPeerConfig {
                            name: format!("{}", peer.addr.ip()),
                            host: format!("{}:179", peer.addr.ip()),
                            hold_time: peer.hold_time.0.into(),
                            idle_hold_time: peer.idle_hold_time.0.into(),
                            delay_open: peer.delay_open.0.into(),
                            connect_retry: peer.connect_retry.0.into(),
                            keepalive: peer.keepalive.0.into(),
                            resolution: BGP_SESSION_RESOLUTION,
                            passive: false,
                            remote_asn: peer.remote_asn.as_ref().map(|x| x.0),
                            min_ttl: ttl,
                            md5_auth_key: peer.md5_auth_key.clone(),
                            multi_exit_discriminator: peer.multi_exit_discriminator.as_ref().map(|x| x.0),
                            local_pref: peer.local_pref.as_ref().map(|x| x.0),
                            enforce_first_as: peer.enforce_first_as,
                            communities: communities.into_iter().map(|c| c.community.0).collect(),
                            allow_export: export_policy,
                            allow_import: import_policy,
                            vlan_id: peer.vlan_id.map(|x| x.0),
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

                    let (config_id, request_bgp_config) = match switch_bgp_config.get(location) {
                        Some(config) => config,
                        None => {
                            info!(log, "no bgp config found for switch, skipping."; "switch" => ?location);
                            continue;
                        },
                    };

                    let request_prefixes = match bgp_announce_prefixes.get(&request_bgp_config.bgp_announce_set_id) {
                        Some(prefixes) => prefixes,
                        None => {
                            error!(
                                log,
                                "no prefixes to announce found for bgp config";
                                "switch" => ?location,
                                "announce_set_id" => ?request_bgp_config.bgp_announce_set_id,
                                "bgp_config_id" => ?config_id,
                            );
                            continue;
                        },
                    };

                    match desired_bgp_configs.entry(*location) {
                        Entry::Occupied(mut occupied_entry) => {
                            let config = occupied_entry.get_mut();
                            // peers are the only per-port part of the config.
                            config.peers.extend(peers);
                        }
                        Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(
                                ApplyRequest {
                                    asn: *request_bgp_config.asn,
                                    peers,
                                    originate: request_prefixes.clone(),
                                    checker: request_bgp_config.checker.as_ref().map(|code| CheckerSource{
                                        asn: *request_bgp_config.asn,
                                        code: code.clone(),
                                    }),
                                    shaper: request_bgp_config.shaper.as_ref().map(|code| ShaperSource{
                                        asn: *request_bgp_config.asn,
                                        code: code.clone(),
                                    }),
                                });
                        }
                    }
                }

                for (location, config) in &desired_bgp_configs {
                    let client = match mgd_clients.get(location) {
                        Some(client) => client,
                        None => {
                            error!(log, "no mgd client found for switch"; "switch_location" => ?location);
                            continue;
                        },
                    };
                    info!(
                        &log,
                        "applying bgp config";
                        "switch_location" => ?location,
                        "config" => ?config,
                    );
                    if let Err(e) = client.bgp_apply(config).await {
                        error!(log, "error while applying bgp configuration"; "error" => ?e);
                    }
                }

                //
                // calculate and apply bootstore changes
                //

                // TODO: #5232 Make ntp servers w/ generation tracking first-class citizens in the db
                // We're using the latest bootstore config from the sled agents to get the ntp
                // servers. We should instead be pulling this information from the db. However, it
                // seems that we're currently not storing the ntp servers in the db as a first-class
                // citizen, so we'll need to add that first.

                // find the active sled-agent bootstore config with the highest generation
                let mut latest_sled_agent_bootstore_config: Option<EarlyNetworkConfig> = None;

                // Since we update the first scrimlet we can reach (we failover to the second one
                // if updating the first one fails) we need to check them both.
                for (_location, client) in &sled_agent_clients {
                    let scrimlet_cfg  = match client.read_network_bootstore_config_cache().await {
                        Ok(config) => config,
                        Err(e) => {
                            error!(log, "unable to read bootstore config from scrimlet"; "error" => ?e);
                            continue;
                        }
                    };
                    if let Some(other_config) = latest_sled_agent_bootstore_config.as_mut() {
                        if other_config.generation < scrimlet_cfg.generation {
                            *other_config = scrimlet_cfg.clone();
                        }
                    } else {
                        latest_sled_agent_bootstore_config = Some(scrimlet_cfg.clone());
                    }
                }

                // TODO: this will also be removed once the above is resolved
                // Move on to the next rack if neither scrimlet is reachable.
                // if both scrimlets are unreachable we probably have bigger problems on this rack
                let ntp_servers = match latest_sled_agent_bootstore_config {
                    Some(config) => {
                        config.body.ntp_servers.clone()
                    },
                    None => {
                        error!(log, "both scrimlets are unreachable, cannot update bootstore");
                        continue;
                    }
                };

                // build the desired bootstore config from the records we've fetched
                let subnet = match rack.rack_subnet {
                    Some(IpNetwork::V6(subnet)) => subnet.into(),
                    Some(IpNetwork::V4(_)) => {
                        error!(log, "rack subnet must be ipv6"; "rack" => ?rack);
                        continue;
                    },
                    None => {
                        error!(log, "rack subnet not set"; "rack" => ?rack);
                        continue;
                    }
                };

                // TODO: is this correct? Do we place the BgpConfig for both switches in a single Vec to send to the bootstore?
                let mut bgp: Vec<SledBgpConfig> = switch_bgp_config.iter().map(|(_location, (_id, config))| {
                    let announcements = bgp_announce_prefixes
                        .get(&config.bgp_announce_set_id)
                        .expect("bgp config is present but announce set is not populated")
                        .iter()
                        .map(|prefix| {
                            Ipv4Net::new(prefix.value, prefix.length)
                                .expect("Prefix4 and Ipv4Net's value types have diverged")
                        }).collect();

                    SledBgpConfig {
                        asn: config.asn.0,
                        originate: announcements,
                        checker: config.checker.clone(),
                        shaper: config.shaper.clone(),
                    }
                }).collect();

                bgp.dedup();

                let mut ports: Vec<PortConfigV2> = vec![];

                for (location, port, change) in &changes {
                    let PortSettingsChange::Apply(info) = change else {
                        continue;
                    };

                    let peer_configs = match self.datastore.bgp_peer_configs(opctx, *location, port.port_name.to_string()).await {
                        Ok(v) => v,
                        Err(e) => {
                            error!(
                                log,
                                "failed to fetch bgp peer config for switch port";
                                "switch_location" => ?location,
                                "port" => &port.port_name.to_string(),
                                "error" => %DisplayErrorChain::new(&e)
                            );
                            continue;
                        },
                    };

                    // TODO https://github.com/oxidecomputer/omicron/issues/3062
                    let tx_eq = if let Some(c) = info.tx_eq.get(0) {
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

                    let mut port_config = PortConfigV2 {
                        addresses: info.addresses.iter().map(|a|
			    UplinkAddressConfig {
				    address: a.address,
				    vlan_id: a.vlan_id
			    }).collect(),
                        autoneg: info
                            .links
                            .get(0) //TODO breakout support
                            .map(|l| l.autoneg)
                            .unwrap_or(false),
                        bgp_peers: peer_configs.into_iter()
                            // filter maps are cool
                            .filter_map(|c| match c.addr.ip() {
                                IpAddr::V4(addr) => Some((c, addr)),
                                IpAddr::V6(_) => None,
                            })
                            .map(|(c, addr)| {
                                SledBgpPeerConfig {
                                    asn: *c.asn,
                                    port: c.port_name,
                                    addr,
                                    hold_time: Some(c.hold_time.0.into()),
                                    idle_hold_time: Some(c.idle_hold_time.0.into()),
                                    delay_open: Some(c.delay_open.0.into()),
                                    connect_retry: Some(c.connect_retry.0.into()),
                                    keepalive: Some(c.keepalive.0.into()),
                                    enforce_first_as: c.enforce_first_as,
                                    local_pref: c.local_pref.map(|x| x.into()),
                                    md5_auth_key: c.md5_auth_key,
                                    min_ttl: c.min_ttl.map(|x| x.0 as u8), //TODO avoid cast return error
                                    multi_exit_discriminator: c.multi_exit_discriminator.map(|x| x.into()),
                                    remote_asn: c.remote_asn.map(|x| x.into()),
                                    communities: Vec::new(),
                                    allowed_export: ImportExportPolicy::NoFiltering,
                                    allowed_import: ImportExportPolicy::NoFiltering,
                                    vlan_id: c.vlan_id.map(|x| x.0),
                                }
                        }).collect(),
                        port: port.port_name.to_string(),
                        routes: info
                            .routes
                            .iter()
                            .map(|r| SledRouteConfig {
                                destination: r.dst.into(),
                                nexthop: r.gw.ip(),
                                vlan_id: r.vid.map(|x| x.0),
                                rib_priority: r.rib_priority.map(|x| x.0),
                            })
                            .collect(),
                        switch: *location,
                        uplink_port_fec: info
                            .links
                            .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                            .map(|l| l.fec.map(|fec| fec.into()))
                            .unwrap_or(None),
                        uplink_port_speed: info
                            .links
                            .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
                            .map(|l| l.speed)
                            .unwrap_or(SwitchLinkSpeed::Speed100G)
                            .into(),
			lldp: info
			    .link_lldp
			    .get(0) //TODO https://github.com/oxidecomputer/omicron/issues/3062
			    .map(|c|  LldpPortConfig {
				status: match c.enabled {
				    true => LldpAdminStatus::Enabled,
				    false=> LldpAdminStatus::Disabled,
				},
				port_id: c.link_name.clone().map(|p| p.to_string()),
				port_description: c.link_description.clone(),
				chassis_id: c.chassis_id.clone(),
				system_name: c.system_name.clone(),
				system_description: c.system_description.clone(),
				management_addrs:c.management_ip.map(|a| vec![a.ip()]),
			    }),
			    tx_eq,
		    }
                    ;

                    for peer in port_config.bgp_peers.iter_mut() {
                        peer.communities = match self
                            .datastore
                            .communities_for_peer(
                                opctx,
                                port.port_settings_id.unwrap(),
                                PHY0, //TODO https://github.com/oxidecomputer/omicron/issues/3062
                                IpNetwork::from(IpAddr::from(peer.addr))
                            ).await {
                                Ok(cs) => cs.iter().map(|c| c.community.0).collect(),
                                Err(e) => {
                                    error!(log,
                                        "failed to get communities for peer";
                                        "peer" => ?peer,
                                        "error" => %DisplayErrorChain::new(&e)
                                    );
                                    continue;
                                }
                            };

                        //TODO consider awaiting in parallel and joining
                        let allow_import = match self.datastore.allow_import_for_peer(
                            opctx,
                            port.port_settings_id.unwrap(),
                            PHY0, //TODO https://github.com/oxidecomputer/omicron/issues/3062
                            IpNetwork::from(IpAddr::from(peer.addr)),
                        ).await {
                            Ok(cs) => cs,
                            Err(e) => {
                                error!(log,
                                    "failed to get peer allowed imports";
                                    "peer" => ?peer,
                                    "error" => %DisplayErrorChain::new(&e)
                                );
                                continue;
                            }
                        };

                        peer.allowed_import = match allow_import {
                            Some(list) =>  ImportExportPolicy::Allow(
                                list.clone().into_iter().map(|x| x.prefix.into()).collect()
                            ),
                            None => ImportExportPolicy::NoFiltering,
                        };

                        let allow_export = match self.datastore.allow_export_for_peer(
                            opctx,
                            port.port_settings_id.unwrap(),
                            PHY0, //TODO https://github.com/oxidecomputer/omicron/issues/3062
                            IpNetwork::from(IpAddr::from(peer.addr)),
                        ).await {
                            Ok(cs) => cs,
                            Err(e) => {
                                error!(log,
                                    "failed to get peer allowed exports";
                                    "peer" => ?peer,
                                    "error" => %DisplayErrorChain::new(&e)
                                );
                                continue;
                            }
                        };

                        peer.allowed_export = match allow_export {
                            Some(list) =>  ImportExportPolicy::Allow(
                                list.clone().into_iter().map(|x| x.prefix.into()).collect()
                            ),
                            None => ImportExportPolicy::NoFiltering,
                        };
                    }
                    ports.push(port_config);
                }

                let blocks = match self.datastore.address_lot_blocks_by_name(opctx, INFRA_LOT.into()).await {
                    Ok(blocks) => blocks,
                    Err(e) => {
                        error!(log, "error while fetching address lot blocks from db"; "error" => %e);
                        continue;
                    },
                };

                // currently there should only be one block assigned. If there is more than one
                // block, grab the first one and emit a warning.
                if blocks.len() > 1 {
                    warn!(log, "more than one block assigned to infra lot"; "blocks" => ?blocks);
                }

                let (infra_ip_first, infra_ip_last)= match blocks.get(0) {
                    Some(AddressLotBlock{ first_address, last_address, ..}) => {
                        match (first_address, last_address) {
                            (IpNetwork::V4(first), IpNetwork::V4(last)) => (first.ip(), last.ip()),
                            _ =>  {
                                error!(log, "infra lot block must be ipv4"; "block" => ?blocks.get(0));
                                continue;
                            },
                        }
                    },
                    None => {
                        error!(log, "no blocks assigned to infra lot");
                        continue;
                    },
                }
                ;


                let bfd = match self.bfd_peer_configs_from_db(opctx).await {
                    Ok(bfd) => bfd,
                    Err(e) => {
                        error!(log, "error fetching bfd config from db"; "error" => %e);
                        continue;
                    }
                };

                let mut desired_config = EarlyNetworkConfig {
                    generation: 0,
                    schema_version: 2,
                    body: EarlyNetworkConfigBody {
                        ntp_servers,
                        rack_network_config: Some(RackNetworkConfigV2 {
                            rack_subnet: subnet,
                            infra_ip_first,
                            infra_ip_last,
                            ports,
                            bgp,
                            bfd,
                        }),
                    },
                };

                // bootstore_needs_update is a boolean value that determines whether or not we need to
                // increment the bootstore version and push a new config to the sled agents.
                //
                // * If the config we've built from the switchport configuration information is
                //   different from the last config we've cached in the db, we update the config,
                //   cache it in the db, and apply it.
                // * If the last cached config cannot be succesfully deserialized into our current
                //   bootstore format, we assume that it is an older format and update the config,
                //   cache it in the db, and apply it.
                // * If there is no last cached config, we assume that this is the first time this
                //   rpw has run for the given rack, so we update the config, cache it in the db,
                //   and apply it.
                // * If we cannot fetch the latest version due to a db error, something is broken
                //   so we don't do anything.
                let bootstore_needs_update = match self.datastore.get_latest_bootstore_config(opctx, NETWORK_KEY.into()).await {
                    Ok(Some(BootstoreConfig { data, .. })) => {
                        match serde_json::from_value::<EarlyNetworkConfig>(data.clone()) {
                            Ok(config) => {
                                let current_ntp_servers: HashSet<String> = config.body.ntp_servers.clone().into_iter().collect();
                                let desired_ntp_servers: HashSet<String> = desired_config.body.ntp_servers.clone().into_iter().collect();

                                let rnc_differs = match (config.body.rack_network_config.clone(), desired_config.body.rack_network_config.clone()) {
                                    (Some(current_rnc), Some(desired_rnc)) => {
                                        !hashset_eq(current_rnc.bgp.clone(), desired_rnc.bgp.clone()) ||
                                        !hashset_eq(current_rnc.bfd.clone(), desired_rnc.bfd.clone()) ||
                                        !hashset_eq(current_rnc.ports.clone(), desired_rnc.ports.clone()) ||
                                        current_rnc.rack_subnet != desired_rnc.rack_subnet ||
                                        current_rnc.infra_ip_first != desired_rnc.infra_ip_first ||
                                        current_rnc.infra_ip_last != desired_rnc.infra_ip_last
                                    },
                                    (None, Some(_)) => true,
                                    _ => {
                                        todo!("error")
                                    }
                                };

                                if current_ntp_servers != desired_ntp_servers {
                                    info!(
                                        log,
                                        "ntp servers have changed";
                                        "old" => ?current_ntp_servers,
                                        "new" => ?desired_ntp_servers,
                                    );
                                    true
                                } else if rnc_differs {
                                    info!(
                                        log,
                                        "rack network config has changed";
                                        "old" => ?config.body.rack_network_config,
                                        "new" => ?desired_config.body.rack_network_config,
                                    );
                                    true
                                } else {
                                    false
                                }
                            },
                            Err(e) => {
                                error!(
                                    log,
                                    "bootstore config does not deserialized to current EarlyNetworkConfig format";
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
                        .await {
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

                    desired_config.generation = generation as u64;
                    info!(
                        &log,
                        "updating bootstore config";
                        "config" => ?desired_config,
                    );

                    // push the updates to both scrimlets
                    // if both scrimlets are down, bootstore updates aren't happening anyway
                    let mut one_succeeded = false;
                    for (location, client) in &sled_agent_clients {
                        if let Err(e) = client.write_network_bootstore_config(&desired_config).await {
                            error!(
                                log,
                                "error updating bootstore";
                                "location" => %location,
                                "config" => ?desired_config,
                                "error" => %e,
                            )
                        } else {
                            one_succeeded = true;
                        }
                    }

                    // if at least one succeeded, record this update in the db
                    if one_succeeded {
                        let config = BootstoreConfig {
                            key: NETWORK_KEY.into(),
                            generation: desired_config.generation as i64,
                            data: serde_json::to_value(&desired_config).unwrap(),
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
            json!({})
        }
        .boxed()
    }
}

fn hashset_eq<T>(left: Vec<T>, right: Vec<T>) -> bool
where
    T: Hash + Eq,
{
    let left = left.into_iter().collect::<HashSet<T>>();
    let right = right.into_iter().collect::<HashSet<T>>();
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
    loopbacks_to_add: &[(SwitchLocation, IpAddr)],
    dpd_clients: HashMap<SwitchLocation, dpd_client::Client>,
    log: &slog::Logger,
) {
    for (location, address) in loopbacks_to_add {
        let client = match dpd_clients.get(location) {
            Some(v) => v,
            None => {
                error!(log, "dpd_client is missing, cannot create loopback addresses"; "location" => %location);
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
    loopbacks_to_del: &[(SwitchLocation, IpAddr)],
    dpd_clients: &HashMap<SwitchLocation, dpd_client::Client>,
    log: &slog::Logger,
) {
    for (location, address) in loopbacks_to_del {
        let client = match dpd_clients.get(location) {
            Some(v) => v,
            None => {
                error!(log, "dpd_client is missing, cannot delete loopback addresses"; "location" => %location);
                continue;
            }
        };

        if let Err(e) = ensure_loopback_deleted(log, client, *address).await {
            error!(log, "error while deleting loopback address"; "error" => %e);
        };
    }
}

async fn switch_loopback_addresses(
    dpd_clients: &HashMap<SwitchLocation, dpd_client::Client>,
    log: &slog::Logger,
) -> HashSet<(SwitchLocation, IpAddr)> {
    let mut current_loopback_addresses: HashSet<(SwitchLocation, IpAddr)> =
        HashSet::new();

    for (location, client) in dpd_clients {
        let ipv4_loopbacks = match client.loopback_ipv4_list().await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    log,
                    "error fetching ipv4 loopback addresses from switch";
                    "location" => %location,
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
                    "location" => %location,
                    "error" => %e,
                );
                continue;
            }
        };

        for entry in ipv4_loopbacks.iter() {
            current_loopback_addresses
                .insert((*location, IpAddr::V4(entry.addr)));
        }

        for entry in ipv6_loopbacks.iter().filter(|x| x.tag == OMICRON_DPD_TAG)
        {
            current_loopback_addresses
                .insert((*location, IpAddr::V6(entry.addr)));
        }
    }
    current_loopback_addresses
}

fn uplinks(
    changes: &[(
        SwitchLocation,
        nexus_db_model::SwitchPort,
        PortSettingsChange,
    )],
) -> HashMap<SwitchLocation, Vec<HostPortConfig>> {
    let mut uplinks: HashMap<SwitchLocation, Vec<HostPortConfig>> =
        HashMap::new();
    for (location, port, change) in changes {
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

        let config = HostPortConfig {
            port: port.port_name.to_string(),
            addrs: config
                .addresses
                .iter()
                .map(|a| UplinkAddressConfig {
                    address: a.address,
                    vlan_id: a.vlan_id,
                })
                .collect(),
            lldp,
            tx_eq,
        };

        match uplinks.entry(*location) {
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
    mappings: &HashMap<SwitchLocation, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, sled_agent_client::Client> {
    let sled_agent_clients: HashMap<SwitchLocation, sled_agent_client::Client> =
        mappings
            .iter()
            .map(|(location, addr)| {
                // build sled agent address from switch zone address
                let addr = get_sled_address(Ipv6Subnet::new(*addr));
                let client = sled_agent_client::Client::new(
                    &format!("http://{}", addr),
                    log.clone(),
                );
                (*location, client)
            })
            .collect();
    sled_agent_clients
}

type SwitchStaticRoutes = HashSet<(Ipv4Addr, Prefix4, Option<u16>, Option<u8>)>;

fn static_routes_to_del(
    current_static_routes: HashMap<SwitchLocation, SwitchStaticRoutes>,
    desired_static_routes: HashMap<SwitchLocation, SwitchStaticRoutes>,
) -> HashMap<SwitchLocation, DeleteStaticRoute4Request> {
    let mut routes_to_del: HashMap<SwitchLocation, DeleteStaticRoute4Request> =
        HashMap::new();

    // find routes to remove
    for (switch_location, routes_on_switch) in &current_static_routes {
        if let Some(routes_wanted) = desired_static_routes.get(switch_location)
        {
            // if it's on the switch but not desired (in our db), it should be removed
            let stale_routes = routes_on_switch
                .difference(routes_wanted)
                .map(|(nexthop, prefix, vlan_id, rib_priority)| StaticRoute4 {
                    nexthop: *nexthop,
                    prefix: *prefix,
                    vlan_id: *vlan_id,
                    rib_priority: rib_priority
                        .unwrap_or(DEFAULT_RIB_PRIORITY_STATIC),
                })
                .collect::<Vec<StaticRoute4>>();

            routes_to_del.insert(
                *switch_location,
                DeleteStaticRoute4Request {
                    routes: StaticRoute4List { list: stale_routes },
                },
            );
        } else {
            // if no desired routes are present, all routes on this switch should be deleted
            let stale_routes = routes_on_switch
                .iter()
                .map(|(nexthop, prefix, vlan_id, rib_priority)| StaticRoute4 {
                    nexthop: *nexthop,
                    prefix: *prefix,
                    vlan_id: *vlan_id,
                    rib_priority: rib_priority
                        .unwrap_or(DEFAULT_RIB_PRIORITY_STATIC),
                })
                .collect::<Vec<StaticRoute4>>();

            let req = DeleteStaticRoute4Request {
                routes: StaticRoute4List { list: stale_routes },
            };

            routes_to_del.insert(*switch_location, req);
            continue;
        };
    }

    // filter out switches with no routes to remove
    let routes_to_del = routes_to_del
        .into_iter()
        .filter(|(_location, request)| !request.routes.list.is_empty())
        .collect();

    routes_to_del
}

#[allow(clippy::type_complexity)]
fn static_routes_to_add(
    desired_static_routes: &HashMap<SwitchLocation, SwitchStaticRoutes>,
    current_static_routes: &HashMap<SwitchLocation, SwitchStaticRoutes>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, AddStaticRoute4Request> {
    let mut routes_to_add: HashMap<SwitchLocation, AddStaticRoute4Request> =
        HashMap::new();

    // find routes to add
    for (switch_location, routes_wanted) in desired_static_routes {
        let routes_on_switch = match current_static_routes.get(&switch_location)
        {
            Some(routes) => routes,
            None => {
                warn!(
                    &log,
                    "no discovered routes from switch. it is possible that an earlier api call failed.";
                    "switch_location" => ?switch_location,
                );
                continue;
            }
        };
        let missing_routes = routes_wanted
            .difference(routes_on_switch)
            .map(|(nexthop, prefix, vlan_id, rib_priority)| StaticRoute4 {
                nexthop: *nexthop,
                prefix: *prefix,
                vlan_id: *vlan_id,
                rib_priority: rib_priority
                    .unwrap_or(DEFAULT_RIB_PRIORITY_STATIC),
            })
            .collect::<Vec<StaticRoute4>>();

        routes_to_add.insert(
            *switch_location,
            AddStaticRoute4Request {
                routes: StaticRoute4List { list: missing_routes },
            },
        );
    }

    // filter out switches with no routes to add
    let routes_to_add = routes_to_add
        .into_iter()
        .filter(|(_location, request)| !request.routes.list.is_empty())
        .collect();

    routes_to_add
}

fn static_routes_in_db(
    changes: &[(
        SwitchLocation,
        nexus_db_model::SwitchPort,
        PortSettingsChange,
    )],
) -> HashMap<SwitchLocation, SwitchStaticRoutes> {
    let mut routes_from_db: HashMap<SwitchLocation, SwitchStaticRoutes> =
        HashMap::new();

    for (location, _port, change) in changes {
        // we only need to check for ports that have a configuration present. No config == no routes.
        let PortSettingsChange::Apply(settings) = change else {
            continue;
        };
        let mut routes = HashSet::new();
        for route in &settings.routes {
            // convert to appropriate types for comparison and insertion
            let nexthop = match route.gw.ip() {
                IpAddr::V4(v4) => v4,
                IpAddr::V6(_) => continue,
            };
            let prefix = match route.dst.ip() {
                IpAddr::V4(v4) => {
                    Prefix4 { value: v4, length: route.dst.prefix() }
                }
                IpAddr::V6(_) => continue,
            };
            routes.insert((
                nexthop,
                prefix,
                route.vid.map(|x| x.0),
                route.rib_priority.map(|x| x.0),
            ));
        }

        match routes_from_db.entry(*location) {
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
    dpd_clients: &HashMap<SwitchLocation, dpd_client::Client>,
    changes: &[(
        SwitchLocation,
        nexus_db_model::SwitchPort,
        PortSettingsChange,
    )],
    log: &slog::Logger,
) {
    for (location, switch_port, change) in changes {
        let client = match dpd_clients.get(&location) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "no DPD client for switch location";
                    "switch_location" => ?location
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
                    "switch_location" => ?location,
                    "error" => format!("{:#}", e)
                );
                continue;
            }
        };

        let config_on_switch =
            match client.port_settings_get(&dpd_port_id, DPD_TAG).await {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        log,
                        "failed to retrieve port setttings from switch";
                        "switch_port_id" => ?port_name,
                        "switch_location" => ?location,
                        "error" => format!("{:#}", e)
                    );
                    continue;
                }
            };

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
                            "switch_location" => ?location,
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
                        "switch_location" => ?location,
                        "switch_port_settings_id" => ?settings.settings.id(),
                    );
                    continue;
                }

                // apply settings via dpd client
                info!(
                    &log,
                    "applying settings to switch port";
                    "switch_location" => ?location,
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
                            "switch_location" => ?location,
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
                    "switch_location" => ?location,
                    "port_id" => ?dpd_port_id,
                );

                if config_on_switch.into_inner().links.is_empty() {
                    info!(
                        &log,
                        "port settings up to date, skipping";
                        "switch_port_id" => ?port_name,
                        "switch_location" => ?location,
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
                            "switch_location" => ?location,
                            "error" => format!("{:#}", e)
                        );
                    }
                }
            }
        }
    }
}

async fn static_routes_on_switch(
    mgd_clients: &HashMap<SwitchLocation, mg_admin_client::Client>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, SwitchStaticRoutes> {
    let mut routes_on_switch = HashMap::new();

    for (location, client) in mgd_clients {
        let static_routes: SwitchStaticRoutes =
            match client.static_list_v4_routes().await {
                Ok(routes) => {
                    let mut flattened = HashSet::new();
                    for (destination, paths) in routes.iter() {
                        let Ok(dst) = destination.parse() else {
                            error!(
                                log,
                                "failed to parse static route destination: \
                                 {destination}"
                            );
                            continue;
                        };
                        for p in paths.iter() {
                            let nh = match p.nexthop {
                                IpAddr::V4(addr) => addr,
                                IpAddr::V6(addr) => {
                                    error!(
                                        log,
                                        "ipv6 nexthops not supported: {addr}"
                                    );
                                    continue;
                                }
                            };
                            flattened.insert((
                                nh,
                                dst,
                                p.vlan_id,
                                Some(p.rib_priority),
                            ));
                        }
                    }
                    flattened
                }
                Err(_) => {
                    error!(
                        &log,
                        "unable to retrieve routes from switch";
                        "switch_location" => ?location,
                    );
                    continue;
                }
            };
        routes_on_switch.insert(*location, static_routes);
    }
    routes_on_switch
}

async fn delete_static_routes(
    mgd_clients: &HashMap<SwitchLocation, mg_admin_client::Client>,
    routes_to_del: HashMap<SwitchLocation, DeleteStaticRoute4Request>,
    log: &slog::Logger,
) {
    for (switch_location, request) in routes_to_del {
        let client = match mgd_clients.get(&switch_location) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "mgd client not found for switch location";
                    "switch_location" => ?switch_location,
                );
                continue;
            }
        };

        info!(
            &log,
            "removing static v4 routes";
            "switch_location" => ?switch_location,
            "request" => ?request,
        );
        if let Err(e) = client.static_remove_v4_route(&request).await {
            error!(
                &log,
                "failed to delete routes from mgd";
                "switch_location" => ?switch_location,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
    }
}

async fn add_static_routes(
    mgd_clients: &HashMap<SwitchLocation, mg_admin_client::Client>,
    routes_to_add: HashMap<SwitchLocation, AddStaticRoute4Request>,
    log: &slog::Logger,
) {
    for (switch_location, request) in routes_to_add {
        let client = match mgd_clients.get(&switch_location) {
            Some(client) => client,
            None => {
                error!(
                    &log,
                    "mgd client not found for switch location";
                    "switch_location" => ?switch_location,
                );
                continue;
            }
        };

        info!(
            &log,
            "adding static v4 routes";
            "switch_location" => ?switch_location,
            "request" => ?request,
        );
        if let Err(e) = client.static_add_v4_route(&request).await {
            error!(
                &log,
                "failed to add routes to mgd";
                "switch_location" => ?switch_location,
                "request" => ?request,
                "error" => format!("{:#}", e)
            );
        };
    }
}
