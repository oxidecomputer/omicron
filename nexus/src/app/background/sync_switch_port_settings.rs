// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating switch port settings to downstream switch
//! management daemons (dendrite)

use crate::app::{
    map_switch_zone_addrs,
    sagas::switch_port_settings_common::api_to_dpd_port_settings,
};

use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use nexus_db_model::BgpConfig;
use uuid::Uuid;

use super::common::BackgroundTask;
use dpd_client::types::PortId;
use futures::future::BoxFuture;
use futures::FutureExt;
use mg_admin_client::types::{
    AddStaticRoute4Request, ApplyRequest, BgpPeerConfig,
    DeleteStaticRoute4Request, Prefix4, StaticRoute4, StaticRoute4List,
};
use nexus_db_queries::{
    context::OpContext,
    db::{datastore::SwitchPortSettingsCombinedResult, DataStore},
};
use nexus_types::{external_api::params, identity::Resource};
use omicron_common::{address::DENDRITE_PORT, OMICRON_DPD_TAG};
use omicron_common::{
    address::{get_sled_address, Ipv6Subnet, MGD_PORT},
    api::external::{DataPageParams, SwitchLocation},
};
use serde_json::json;
use sled_agent_client::types::HostPortConfig;
use std::{
    collections::{hash_map::Entry, HashMap},
    net::{IpAddr, Ipv4Addr, SocketAddrV6},
    str::FromStr,
    sync::Arc,
};

const DPD_TAG: Option<&'static str> = Some(OMICRON_DPD_TAG);

// This is more of an implementation detail of the BGP implementation. It
// defines the maximum time the peering engine will wait for external messages
// before breaking to check for shutdown conditions.
const BGP_SESSION_RESOLUTION: u64 = 100;

pub struct SwitchPortSettingsManager {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl SwitchPortSettingsManager {
    pub fn new(datastore: Arc<DataStore>, resolver: Resolver) -> Self {
        Self { datastore, resolver }
    }

    async fn switch_ports<'a>(
        &'a mut self,
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

    async fn changes<'a>(
        &'a mut self,
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
}

enum PortSettingsChange {
    Apply(Box<SwitchPortSettingsCombinedResult>),
    Clear,
}

impl BackgroundTask for SwitchPortSettingsManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            // lookup switch zones via DNS
            let switch_zone_addresses = match self
                .resolver
                .lookup_all_ipv6(ServiceName::Dendrite)
                .await
            {
                Ok(addrs) => addrs,
                Err(_) => todo!("handle error"),
            };

            let mappings =
                map_switch_zone_addrs(log, switch_zone_addresses).await;

            // build sled agent clients
            let sled_agent_clients = build_sled_agent_clients(&mappings, log);

            // build dpd clients
            let dpd_clients = build_dpd_clients(&mappings, log);

            // build mgd clients
            let mgd_clients = build_mgd_clients(mappings, log);

            let port_list = match self.switch_ports(opctx, log).await {
                Ok(value) => value,
                Err(_) => todo!("handle error"),
            };

            //
            // calculate and apply switch port changes
            //

            let changes = match self.changes(port_list, opctx, log).await {
                Ok(value) => value,
                Err(_) => todo!("handle error"),
            };

            apply_switch_port_changes(dpd_clients, &changes, log).await;

            //
            // calculate and apply routing changes
            //

            // get the static routes on each switch
            let downstream_static_routes =
                downstream_static_routes(&mgd_clients, log).await;

            // generate the complete set of static routes that should be on a given switch
            let upstream_static_routes = upstream_static_routes(&changes);

            // diff the downstream and upstream routes. Add what is missing from downstream, remove what is not present in upstream.
            let routes_to_add = static_routes_to_add(
                &upstream_static_routes,
                &downstream_static_routes,
                log,
            );

            let routes_to_del = static_routes_to_del(
                downstream_static_routes,
                upstream_static_routes,
            );

            // delete the unneeded routes first, just in case there is a conflicting route for one we need to add
            delete_static_routes(&mgd_clients, routes_to_del, log).await;

            // add the new routes
            add_static_routes(&mgd_clients, routes_to_add, log).await;

            //
            // calculate and apply switch zone SMF changes
            //
            let uplinks = uplinks(&changes);

            // yeet the messages
            for (location, config) in &uplinks {
                let client: &sled_agent_client::Client =
                    match sled_agent_clients.get(location) {
                        Some(client) => client,
                        None => todo!("handle missing client"),
                    };

                if let Err(_) = client
                    .uplink_ensure(&sled_agent_client::types::SwitchPorts {
                        uplinks: config.clone(),
                    })
                    .await
                {
                    todo!("handle error")
                }
            }

            //
            // calculate and apply BGP changes
            //

            // build a list of desired settings for each switch
            let mut desired_bgp_configs: HashMap<
                SwitchLocation,
                Vec<ApplyRequest>,
            > = HashMap::new();

            // we currently only support one bgp config per switch
            let mut switch_bgp_config: HashMap<SwitchLocation, (Uuid, BgpConfig)> = HashMap::new();

            // Prefixes are associated to BgpConfig via the config id
            let mut bgp_announce_set: HashMap<Uuid, Vec<Prefix4>> = HashMap::new();

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
                                Err(_) => todo!(),
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
                    if bgp_announce_set.get(&bgp_config.bgp_announce_set_id).is_none() {
                        let announcements = match self
                            .datastore
                            .bgp_announce_list(
                                opctx,
                                &params::BgpAnnounceSetSelector {
                                    name_or_id: bgp_config
                                        .bgp_announce_set_id
                                        .into(),
                                },
                            )
                            .await
                        {
                            Ok(a) => a,
                            Err(_) => todo!(),
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
                        bgp_announce_set.insert(bgp_config.bgp_announce_set_id, prefixes);
                    }


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
                    };

                    // update the stored vec if it exists, create a new on if it doesn't exist
                    match peers.entry(port.port_name.clone()) {
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

                let request_prefixes = match bgp_announce_set.get(&request_bgp_config.bgp_announce_set_id) {
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

                let request = ApplyRequest {
                    asn: *request_bgp_config.asn,
                    peers,
                    originate: request_prefixes.clone(),
                };

                match desired_bgp_configs.entry(*location) {
                    Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.get_mut().push(request);
                    }
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(vec![request]);
                    }
                }
            }

            // verify that each switch's peer group are all using the same asn
            // if they are not, reject the set of peers
            for (location, configs) in &desired_bgp_configs {
                let client = match mgd_clients.get(location) {
                    Some(client) => client,
                    None => {
                        error!(log, "no mgd client found for switch"; "switch_location" => ?location);
                        continue;
                    },
                };
                for config in configs {
                    if let Err(e) = client.inner.bgp_apply(config).await {
                        error!(log, "error while applying bgp configuration"; "error" => ?e);
                    }
                }
            }

            //
            // calculate and apply bootstore changes
            //

            json!({})
        }
        .boxed()
    }
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
        let config = HostPortConfig {
            port: port.port_name.clone(),
            addrs: config.addresses.iter().map(|a| a.address).collect(),
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

fn build_mgd_clients(
    mappings: HashMap<SwitchLocation, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, mg_admin_client::Client> {
    let mgd_clients: HashMap<SwitchLocation, mg_admin_client::Client> =
        mappings
            .iter()
            .map(|(location, addr)| {
                let port = MGD_PORT;
                let socketaddr = std::net::SocketAddr::V6(SocketAddrV6::new(
                    *addr, port, 0, 0,
                ));
                let client = match mg_admin_client::Client::new(
                    &log.clone(),
                    socketaddr,
                ) {
                    Ok(client) => client,
                    Err(_) => todo!(),
                };
                (*location, client)
            })
            .collect();
    mgd_clients
}

fn build_dpd_clients(
    mappings: &HashMap<SwitchLocation, std::net::Ipv6Addr>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, dpd_client::Client> {
    let dpd_clients: HashMap<SwitchLocation, dpd_client::Client> = mappings
        .iter()
        .map(|(location, addr)| {
            let port = DENDRITE_PORT;

            let client_state = dpd_client::ClientState {
                tag: String::from("nexus"),
                log: log.new(o!(
                    "component" => "DpdClient"
                )),
            };

            let dpd_client = dpd_client::Client::new(
                &format!("http://[{addr}]:{port}"),
                client_state,
            );
            (*location, dpd_client)
        })
        .collect();
    dpd_clients
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

fn static_routes_to_del(
    downstream_static_routes: HashMap<
        SwitchLocation,
        progenitor_client::ResponseValue<StaticRoute4List>,
    >,
    upstream_static_routes: HashMap<SwitchLocation, Vec<(Ipv4Addr, Prefix4)>>,
) -> HashMap<SwitchLocation, DeleteStaticRoute4Request> {
    let mut routes_to_del: HashMap<SwitchLocation, DeleteStaticRoute4Request> =
        HashMap::new();

    // find routes to remove
    for (switch_location, routes_on_switch) in &downstream_static_routes {
        let Some(routes_wanted) = upstream_static_routes.get(switch_location)
        else {
            // if no upstream routes are present, all downstream routes on this switch should be deleted
            let req = DeleteStaticRoute4Request {
                routes: StaticRoute4List {
                    list: routes_on_switch.list.clone(),
                },
            };
            routes_to_del.insert(*switch_location, req);
            continue;
        };

        for live_route in &routes_on_switch.list {
            // move on to the next route if the db says the route should still be there
            if routes_wanted.iter().any(|(nexthop, prefix)| {
                live_route.nexthop == *nexthop
                    && live_route.prefix.value == prefix.value
                    && live_route.prefix.length == prefix.length
            }) {
                continue;
            }
            // else, build a struct to remove the route
            match routes_to_del.entry(*switch_location) {
                Entry::Occupied(mut occupied_entry) => {
                    occupied_entry
                        .get_mut()
                        .routes
                        .list
                        .push(live_route.clone());
                }
                Entry::Vacant(vacant_entry) => {
                    let req = DeleteStaticRoute4Request {
                        routes: {
                            StaticRoute4List { list: vec![live_route.clone()] }
                        },
                    };
                    vacant_entry.insert(req);
                }
            }
        }
    }
    routes_to_del
}

fn static_routes_to_add(
    upstream_static_routes: &HashMap<SwitchLocation, Vec<(Ipv4Addr, Prefix4)>>,
    downstream_static_routes: &HashMap<
        SwitchLocation,
        progenitor_client::ResponseValue<StaticRoute4List>,
    >,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, AddStaticRoute4Request> {
    let mut routes_to_add: HashMap<SwitchLocation, AddStaticRoute4Request> =
        HashMap::new();

    // find routes to add
    for (switch_location, routes_wanted) in upstream_static_routes {
        let routes_on_switch = match downstream_static_routes
            .get(&switch_location)
        {
            Some(routes) => &routes.list,
            None => {
                warn!(
                    &log,
                    "no discovered routes from switch. it is possible that an earlier api call failed.";
                    "switch_location" => ?switch_location,
                );
                continue;
            }
        };

        for (nexthop, prefix) in routes_wanted {
            // move on to the next route if it is already on the switch
            if routes_on_switch.iter().any(|live_route| {
                live_route.nexthop == *nexthop
                    && live_route.prefix.value == prefix.value
                    && live_route.prefix.length == prefix.length
            }) {
                continue;
            }
            // build a struct to add the route if not

            let sr = StaticRoute4 { nexthop: *nexthop, prefix: prefix.clone() };

            match routes_to_add.entry(*switch_location) {
                Entry::Occupied(mut occupied_entry) => {
                    occupied_entry.get_mut().routes.list.push(sr);
                }
                Entry::Vacant(vacant_entry) => {
                    let req = AddStaticRoute4Request {
                        routes: { StaticRoute4List { list: vec![sr] } },
                    };
                    vacant_entry.insert(req);
                }
            }
        }
    }
    routes_to_add
}

fn upstream_static_routes(
    changes: &[(
        SwitchLocation,
        nexus_db_model::SwitchPort,
        PortSettingsChange,
    )],
) -> HashMap<SwitchLocation, Vec<(Ipv4Addr, Prefix4)>> {
    let mut upstream_static_routes: HashMap<
        SwitchLocation,
        Vec<(Ipv4Addr, Prefix4)>,
    > = HashMap::new();

    for (location, _port, change) in changes {
        // we only need to check for ports that have a configuration present. No config == no routes.
        let PortSettingsChange::Apply(settings) = change else {
            continue;
        };
        let mut routes = vec![];
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
            routes.push((nexthop, prefix))
        }

        match upstream_static_routes.entry(*location) {
            Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().append(&mut routes);
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(routes);
            }
        }
    }
    upstream_static_routes
}

// apply changes for each port
// if we encounter an error, we log it and keep going instead of bailing
async fn apply_switch_port_changes(
    dpd_clients: HashMap<SwitchLocation, dpd_client::Client>,
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

        let dpd_port_id = match PortId::from_str(port_name.as_str()) {
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

                // apply settings via dpd client
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

async fn downstream_static_routes<'a>(
    mgd_clients: &HashMap<SwitchLocation, mg_admin_client::Client>,
    log: &slog::Logger,
) -> HashMap<SwitchLocation, progenitor_client::ResponseValue<StaticRoute4List>>
{
    let mut downstream_static_routes = HashMap::new();

    for (location, client) in mgd_clients {
        let static_routes = match client.inner.static_list_v4_routes().await {
            Ok(routes) => routes,
            Err(_) => {
                error!(
                    &log,
                    "unable to retrieve routes from switch";
                    "switch_location" => ?location,
                );
                continue;
            }
        };
        downstream_static_routes.insert(*location, static_routes);
    }
    downstream_static_routes
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

        if let Err(e) = client.inner.static_remove_v4_route(&request).await {
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

async fn add_static_routes<'a>(
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

        if let Err(e) = client.inner.static_add_v4_route(&request).await {
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
