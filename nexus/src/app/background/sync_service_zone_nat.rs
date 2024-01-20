// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting changes to service zone locations and
//! updating the NAT rpw table accordingly

use super::common::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::Ipv4NatValues;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external;
use serde_json::json;
use sled_agent_client::types::OmicronZoneType;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Background task that ensures service zones have nat entries
/// persisted in the NAT RPW table
pub struct ServiceZoneNatTracker {
    datastore: Arc<DataStore>,
    dpd_clients: Vec<Arc<dpd_client::Client>>,
}

impl ServiceZoneNatTracker {
    pub fn new(
        datastore: Arc<DataStore>,
        dpd_clients: Vec<Arc<dpd_client::Client>>,
    ) -> Self {
        Self { datastore, dpd_clients }
    }
}

impl BackgroundTask for ServiceZoneNatTracker {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            let log = &opctx.log;

            // check inventory
            let inventory = match self
                .datastore
                .inventory_get_latest_collection(
                    opctx,
                    NonZeroU32::new(u32::MAX).unwrap(),
                )
                .await
            {
                Ok(inventory) => inventory,
                Err(e) => {
                    warn!(
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

            let mut ipv4_nat_values: Vec<Ipv4NatValues> = vec![];

            for (sled_id, zones_found) in collection.omicron_zones {
                let (_, sled) = match LookupPath::new(opctx, &self.datastore)
                    .sled_id(sled_id)
                    .fetch()
                    .await
                    .context("failed to look up sled")
                {
                    Ok(result) => result,
                    Err(e) => {
                        warn!(
                            &log,
                            "inventory collection is None";
                            "error" => ?e,
                        );
                        continue;
                    }
                };

                let sled_address = external::Ipv6Net(
                    ipnetwork::Ipv6Network::new(*sled.ip, 128).unwrap(),
                );

                let zones_config: sled_agent_client::types::OmicronZonesConfig =
                    zones_found.zones;
                let zones: Vec<sled_agent_client::types::OmicronZoneConfig> =
                    zones_config.zones;
                for zone in zones {
                    let zone_type: OmicronZoneType = zone.zone_type;
                    match zone_type {
                        OmicronZoneType::BoundaryNtp {
                            nic, snat_cfg, ..
                        } => {
                            let external_ip = match snat_cfg.ip {
                                IpAddr::V4(addr) => addr,
                                IpAddr::V6(_) => {
                                    warn!(
                                        &log,
                                        "ipv6 addresses for service zone nat not implemented";
                                    );
                                    continue;
                                }
                            };

                            let external_address =
                                ipnetwork::Ipv4Network::new(external_ip, 32)
                                    .unwrap();

                            let nat_value = Ipv4NatValues {
                                external_address: nexus_db_model::Ipv4Net(
                                    omicron_common::api::external::Ipv4Net(
                                        external_address,
                                    ),
                                ),
                                first_port: snat_cfg.first_port.into(),
                                last_port: snat_cfg.last_port.into(),
                                sled_address: sled_address.into(),
                                vni: nexus_db_model::Vni(nic.vni.into()),
                                mac: nexus_db_model::MacAddr(nic.mac.into()),
                            };

                            // Append ipv4 nat entry
                            ipv4_nat_values.push(nat_value);
                        }
                        OmicronZoneType::Nexus { nic, external_ip, .. } => {
                            let external_ip = match external_ip {
                                IpAddr::V4(addr) => addr,
                                IpAddr::V6(_) => {
                                    warn!(
                                        &log,
                                        "ipv6 addresses for service zone nat not implemented";
                                    );
                                    continue;
                                }
                            };

                            let external_address =
                                ipnetwork::Ipv4Network::new(external_ip, 32)
                                    .unwrap();

                            let nat_value = Ipv4NatValues {
                                external_address: nexus_db_model::Ipv4Net(
                                    omicron_common::api::external::Ipv4Net(
                                        external_address,
                                    ),
                                ),
                                first_port: 0.into(),
                                last_port: 65535.into(),
                                sled_address: sled_address.into(),
                                vni: nexus_db_model::Vni(nic.vni.into()),
                                mac: nexus_db_model::MacAddr(nic.mac.into()),
                            };

                            // Append ipv4 nat entry
                            ipv4_nat_values.push(nat_value);
                        }

                        _ => continue,
                    }
                }
            }

            if ipv4_nat_values.is_empty() {
                warn!(
                    &log,
                    "no ipv4 nat values to reconcile";
                );
                return json!({
                    "error": "no service zone ipv4 nat values to reconcile"
                });
            }

            // reconcile service zone nat entries
            let result = self.datastore.ipv4_nat_sync_service_zones(opctx, &ipv4_nat_values).await;

            // notify dpd
            for client in &self.dpd_clients {
                if let Err(e) = client.ipv4_nat_trigger_update().await {
                    warn!(
                        &log,
                        "failed to trigger dpd rpw workflow";
                        "error" => ?e
                    );
                };
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
