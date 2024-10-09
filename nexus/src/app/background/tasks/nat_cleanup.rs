// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for garbage collecting ipv4_nat_entry table.
//! Responsible for cleaning up soft deleted entries once they
//! have been propagated to running dpd instances.

use crate::app::map_switch_zone_addrs;

use super::networking::build_dpd_clients;
use crate::app::background::BackgroundTask;
use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;

/// Background task that periodically prunes soft-deleted entries
/// from ipv4_nat_entry table
pub struct Ipv4NatGarbageCollector {
    datastore: Arc<DataStore>,
    resolver: Resolver,
}

impl Ipv4NatGarbageCollector {
    pub fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
    ) -> Ipv4NatGarbageCollector {
        Ipv4NatGarbageCollector { datastore, resolver }
    }
}

impl BackgroundTask for Ipv4NatGarbageCollector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;

            let result = self.datastore.ipv4_nat_current_version(opctx).await;

            let mut min_gen = match result {
                Ok(gen) => gen,
                Err(error) => {
                    warn!(
                        &log,
                        "failed to read generation of database";
                        "error" => format!("{:#}", error)
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to read generation of database: \
                                {:#}",
                                error
                            )
                    });
                }
            };

            let switch_zone_addresses = match self
                .resolver
                .lookup_all_ipv6(ServiceName::Dendrite)
                .await
            {
                Ok(addrs) => addrs,
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

            let mappings =
                map_switch_zone_addrs(log, switch_zone_addresses).await;

            let dpd_clients = build_dpd_clients(&mappings, log);

            for (_location, client) in dpd_clients {
                let response = client.ipv4_nat_generation().await;
                match response {
                    Ok(gen) => min_gen = std::cmp::min(min_gen, *gen),
                    Err(error) => {
                        warn!(
                            &log,
                            "failed to read generation of dpd";
                            "error" => format!("{:#}", error)
                        );
                        return json!({
                            "error":
                                format!(
                                    "failed to read generation of dpd: \
                                    {:#}",
                                    error
                                )
                        });
                    }
                }
            }

            let retention_threshold = Utc::now() - Duration::weeks(2);

            let result = match self
                .datastore
                .ipv4_nat_cleanup(opctx, min_gen, retention_threshold)
                .await {
                    Ok(v) => v,
                    Err(e) => {
                     return json!({
                        "error":
                            format!(
                                "failed to perform cleanup operation: {:#}",
                                e
                            )
                    });
                    },
                };

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
