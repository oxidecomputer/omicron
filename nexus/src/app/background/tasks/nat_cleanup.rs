// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for garbage collecting nat_entry table.
//! Responsible for cleaning up soft deleted entries once they
//! have been propagated to running dpd instances.

use crate::app::dpd_clients;

use crate::app::background::BackgroundTask;
use chrono::{Duration, Utc};
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::internal::shared::SwitchLocation;
use serde_json::json;
use std::sync::Arc;

/// Background task that periodically prunes soft-deleted entries
/// from nat_entry table
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

            let result = self.datastore.nat_current_version(opctx).await;

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

            let dpd_clients = match
                dpd_clients(&self.resolver, log).await
            {
                Ok(mappings) => mappings,
                Err(e) => {
                    error!(
                        log,
                        "failed to resolve addresses for Dendrite services"; "error" => %e
                    );
                    return json!({
                        "error":
                            format!(
                                "failed to resolve addresses for Dendrite services: {:#}",
                                e
                            )
                    });
                }
            };

            for location in [SwitchLocation::Switch0, SwitchLocation::Switch1] {
                if !dpd_clients.contains_key(&location) {
                    let message = format!("dendrite for {location} is unavailable, cannot perform nat cleanup");
                    error!(log, "{message}");
                    return json!({"error": message});
                }
            }

            for client in dpd_clients.values() {
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
                .nat_cleanup(opctx, min_gen, retention_threshold)
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
