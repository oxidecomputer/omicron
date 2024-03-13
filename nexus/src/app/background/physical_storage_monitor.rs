// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task to propagate storage information from inventory
//! to the rest of the database.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::inventory::Collection;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;

pub struct PhysicalStorageMonitor {
    datastore: Arc<DataStore>,
    inventory_rx: watch::Receiver<Option<Collection>>,
}

impl PhysicalStorageMonitor {
    pub fn new(
        datastore: Arc<DataStore>,
        inventory_rx: watch::Receiver<Option<Collection>>,
    ) -> Self {
        Self { datastore, inventory_rx }
    }
}

impl BackgroundTask for PhysicalStorageMonitor {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            let zpools = {
                let inventory = self.inventory_rx.borrow();
                let collection = match &*inventory {
                    Some(c) => c,
                    None => {
                        warn!(
                            &log,
                            "inventory collection not yet known";
                        );
                        return json!({
                            "error": "inventory collection is None"
                        });
                    }
                };

                collection
                    .sled_agents
                    .values()
                    .flat_map(|info| info.zpools.clone())
                    .collect::<Vec<_>>()
            };

            if zpools.is_empty() {
                error!(
                    &log,
                    "no zpools exist";
                );
                return json!({
                    "error": "no zpools exist"
                });
            }

            // reconcile what we observed in inventory with our representation
            // of zpools in the database
            let result =
                match self.datastore.zpool_update_all(opctx, &zpools).await {
                    Ok(num) => num,
                    Err(e) => {
                        error!(
                            &log,
                            "failed to update zpool records";
                            "error" => format!("{:#}", e)
                        );
                        return json!({
                            "error":
                                format!(
                                    "failed to update zpool records: \
                                    {:#}",
                                    e
                                )
                        });
                    }
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
