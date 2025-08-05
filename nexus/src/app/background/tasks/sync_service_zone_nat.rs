// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting changes to service zone locations and
//! updating the NAT rpw table accordingly

use crate::app::switch_zone_address_mappings;

use super::networking::build_dpd_clients;
use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_db_lookup::LookupPath;
use nexus_db_model::NatEntryValues;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use omicron_common::address::{MAX_PORT, MIN_PORT};
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use std::sync::Arc;

// Minumum number of boundary NTP zones that should be present in a valid
// set of service zone nat configurations.
const MIN_NTP_COUNT: usize = 1;

// Minumum number of nexus zones that should be present in a valid
// set of service zone nat configurations.
const MIN_NEXUS_COUNT: usize = 1;

// Minumum number of external DNS zones that should be present in a valid
// set of service zone nat configurations.
const MIN_EXTERNAL_DNS_COUNT: usize = 1;

/// Background task that ensures service zones have nat entries
/// persisted in the NAT RPW table
pub struct ServiceZoneNatTracker {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl ServiceZoneNatTracker {
    pub fn new(datastore: Arc<DataStore>, resolver: Resolver) -> Self {
        Self { datastore, resolver }
    }
}

impl BackgroundTask for ServiceZoneNatTracker {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            // check inventory
            let inventory = match self
                .datastore
                .inventory_get_latest_collection(
                    opctx,
                )
                .await
            {
                Ok(inventory) => inventory,
                Err(e) => {
                    error!(
                        &log,
                        "failed to collect inventory";
                        "error" => format!("{:#}", e)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed collect inventory: \
                                {:#}",
                                e
                            )
                    });
                }
            };

            // generate set of Service Zone NAT entries
            let collection = match inventory {
                Some(c) => c,
                // this could happen if we check the inventory table before the
                // inventory job has finished running for the first time
                None => {
                    warn!(
                        &log,
                        "inventory collection is None";
                    );
                    return json!({
                        "error": "inventory collection is None"
                    });
                }
            };

            let mut nat_values: Vec<NatEntryValues> = vec![];
            let mut ntp_count = 0;
            let mut nexus_count = 0;
            let mut dns_count = 0;

            for sa in &collection.sled_agents {
                let (_, sled) = match LookupPath::new(opctx, &*self.datastore)
                    .sled_id(sa.sled_id.into_untyped_uuid())
                    .fetch()
                    .await
                    .context("failed to look up sled")
                {
                    Ok(result) => result,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to lookup sled by id";
                            "id" => ?sa.sled_id,
                            "error" => ?e,
                        );
                        continue;
                    }
                };

                let sled_address = oxnet::Ipv6Net::host_net(*sled.ip);

                // TODO-correctness Looking at inventory here is a little
                // sketchy. We check the last reconciliation result, which
                // should be a view of what zones are actually running on the
                // sled. But maybe it would be better to act on a rendezvous
                // table populated by reconfigurator if the goal is to sync with
                // what's Nexus thinks is supposed to be running on the sled?
                let zones = sa
                    .last_reconciliation
                    .iter()
                    .flat_map(|reconciliation| {
                        reconciliation.running_omicron_zones().cloned()
                    });

                for zone in zones {
                    let zone_type: OmicronZoneType = zone.zone_type;
                    match zone_type {
                        OmicronZoneType::BoundaryNtp {
                            nic, snat_cfg, ..
                        } => {
                            let external_address = nexus_db_model::IpNet::from(
                                oxnet::IpNet::host_net(snat_cfg.ip)
                            );
                            let (snat_first_port, snat_last_port) =
                                snat_cfg.port_range_raw();
                            let nat_value = NatEntryValues {
                                external_address,
                                first_port: snat_first_port.into(),
                                last_port: snat_last_port.into(),
                                sled_address: sled_address.into(),
                                vni: nexus_db_model::Vni(nic.vni),
                                mac: nexus_db_model::MacAddr(nic.mac),
                            };

                            // Append NAT entry
                            nat_values.push(nat_value);
                            ntp_count += 1;
                        }
                        OmicronZoneType::Nexus { nic, external_ip, .. } => {
                            let external_address = nexus_db_model::IpNet::from(
                                oxnet::IpNet::host_net(external_ip)
                            );
                            let nat_value = NatEntryValues {
                                external_address,
                                first_port: MIN_PORT.into(),
                                last_port: MAX_PORT.into(),
                                sled_address: sled_address.into(),
                                vni: nexus_db_model::Vni(nic.vni),
                                mac: nexus_db_model::MacAddr(nic.mac),
                            };

                            // Append NAT entry
                            nat_values.push(nat_value);
                            nexus_count += 1;
                        },
                        OmicronZoneType::ExternalDns { nic, dns_address, .. } => {
                            let external_address = nexus_db_model::IpNet::from(
                                oxnet::IpNet::host_net(dns_address.ip())
                            );
                            let nat_value = NatEntryValues {
                                external_address,
                                first_port: MIN_PORT.into(),
                                last_port: MAX_PORT.into(),
                                sled_address: sled_address.into(),
                                vni: nexus_db_model::Vni(nic.vni),
                                mac: nexus_db_model::MacAddr(nic.mac),
                            };

                            // Append NAT entry
                            nat_values.push(nat_value);
                            dns_count += 1;
                        },
                        // we explictly list all cases instead of using a wildcard,
                        // that way if someone adds a new type to OmicronZoneType that
                        // requires NAT, they must come here to update this logic as
                        // well
                        OmicronZoneType::Clickhouse {..} => continue,
                        OmicronZoneType::ClickhouseKeeper {..} => continue,
                        OmicronZoneType::ClickhouseServer{..} => continue,
                        OmicronZoneType::CockroachDb {..} => continue,
                        OmicronZoneType::Crucible {..} => continue,
                        OmicronZoneType::CruciblePantry {..} => continue,
                        OmicronZoneType::InternalNtp {..} => continue,
                        OmicronZoneType::InternalDns {..} => continue,
                        OmicronZoneType::Oximeter { ..} => continue,
                    }
                }
            }

            // if we make it this far this should not be empty:
            // * nexus is running so we should at least have generated a nat value for it
            // * nexus requies other services zones that require nat to come up first
            if nat_values.is_empty() {
                error!(
                    &log,
                    "nexus is running but no service zone nat values could be generated from inventory";
                );
                return json!({
                    "error": "nexus is running but no service zone nat values could be generated from inventory"
                });
            }

            if dns_count < MIN_EXTERNAL_DNS_COUNT {
                error!(
                    &log,
                    "generated config for fewer than the minimum allowed number of dns zones";
                );
                return json!({
                    "error": "generated config for fewer than the minimum allowed number of dns zones"
                });
            }

            if ntp_count < MIN_NTP_COUNT {
                error!(
                    &log,
                    "generated config for fewer than the minimum allowed number of ntp zones";
                );
                return json!({
                    "error": "generated config for fewer than the minimum allowed number of ntp zones"

                });
            }

            if nexus_count < MIN_NEXUS_COUNT {
                error!(
                    &log,
                    "generated config for fewer than the minimum allowed number of nexus zones";
                );
                return json!({
                    "error": "generated config for fewer than the minimum allowed number of nexus zones"

                });
            }

            // reconcile service zone nat entries
            let result = match self.datastore.nat_sync_service_zones(opctx, &nat_values).await {
                Ok(num) => num,
                Err(e) => {
                    error!(
                        &log,
                        "failed to update service zone nat records";
                        "error" => format!("{:#}", e)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to update service zone nat records: \
                                {:#}",
                                e
                            )
                    });
                },
            };

            // notify dpd if we've added any new records
            if result > 0 {

                let mappings = match
                    switch_zone_address_mappings(&self.resolver, log).await
                {
                    Ok(mappings) => mappings,
                    Err(e) => {
                        error!(log, "failed to resolve addresses for Dendrite services"; "error" => %e);
                        return json!({
                            "error":
                                format!(
                                    "failed to resolve addresses for Dendrite services: {:#}",
                                    e
                                )
                        });
                    },
                };

                let dpd_clients = build_dpd_clients(&mappings, log);

                for (_location, client) in dpd_clients {
                    if let Err(e) = client.ipv4_nat_trigger_update().await {
                        error!(
                            &log,
                            "failed to trigger dpd rpw workflow";
                            "error" => ?e
                        );
                    };
                }
            }

            let rv = serde_json::to_value(&result).unwrap_or_else(|error| {
                json!({
                    "error":
                        format!(
                            "failed to serialize final value: {:#}",
                            error
                        )
                })
            });

            rv
        }
        .boxed()
    }
}
