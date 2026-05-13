// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating user provided switch configurations
//! to relevant management daemons (dendrite, mgd, sled-agent, etc.)

use crate::app::{
    background::LoadedTargetBlueprint, dpd_clients,
    switch_zone_address_mappings,
};
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use slog::o;

use internal_dns_resolver::Resolver;
use nexus_db_model::{
    BgpConfig, BootstoreConfig, LoopbackAddress, NETWORK_KEY,
};
use tokio::sync::watch;

use crate::app::background::BackgroundTask;
use display_error_chain::DisplayErrorChain;
use dpd_client::{Client as DpdClient, types as DpdTypes};
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::{
    context::OpContext,
    db::{DataStore, datastore::SwitchPortSettingsCombinedResult},
};
use nexus_types::external_api::networking;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::IncompleteBootstoreConfigReport;
use nexus_types::internal_api::background::SwitchPortSettingsManagerStatus;
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::{
    address::{Ipv6Subnet, get_sled_address},
    api::external::DataPageParams,
};
use omicron_uuid_kinds::{BgpAnnounceSetUuid, BgpConfigUuid, GenericUuid};
use serde_json::json;
use sled_agent_types::early_networking::EarlyNetworkConfigEnvelope;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::system_networking::BlueprintExternalNetworkingConfig;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use sled_agent_types::system_networking::WriteNetworkConfigRequest;
use slog_error_chain::InlineErrorChain;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    hash::Hash,
    net::{IpAddr, Ipv6Addr},
    sync::Arc,
};

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
                // calculate BGP changes to update the bootstore
                //

                // we currently only support one bgp config per switch
                let mut switch_bgp_config: HashMap<SwitchSlot, (BgpConfigUuid, BgpConfig)> = HashMap::new();

                // Prefixes are associated to BgpConfig via the config id
                let mut bgp_announce_prefixes: HashMap<BgpAnnounceSetUuid, Vec<IpNet>> = HashMap::new();

                for (switch_slot, port, change) in &changes {
                    let PortSettingsChange::Apply(settings) = change else {
                        continue;
                    };

                    for peer in &settings.bgp_peers {
                        let bgp_config_id = peer.bgp_config_id();

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

fn build_sled_agent_clients(
    mappings: &HashMap<SwitchSlot, Ipv6Addr>,
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
