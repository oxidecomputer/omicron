// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for autmatically adopting physical disks.
//!
//! Removable disks may be arbitrarily attached and detached from
//! Oxide racks. When this happens, if they had not previously
//! been part of a cluster, they need to be explicitly added
//! to become usable.
//!
//! In the future, this may become more explicitly operator-controlled.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::PhysicalDisk;
use nexus_db_model::Zpool;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::inventory::Collection;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

pub struct PhysicalDiskAdoption {
    datastore: Arc<DataStore>,
    rx_inventory_collection: watch::Receiver<Option<Collection>>,
}

impl PhysicalDiskAdoption {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_inventory_collection: watch::Receiver<Option<Collection>>,
    ) -> Self {
        PhysicalDiskAdoption { datastore, rx_inventory_collection }
    }
}

impl BackgroundTask for PhysicalDiskAdoption {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let mut disks_added = 0;
            let log = &opctx.log;
            warn!(&log, "physical disk adoption task started");

            let collection = self.rx_inventory_collection.borrow().clone();
            let Some(collection) = collection else {
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: skipped";
                    "reason" => "no inventory"
                );
                return json!({ "error": "no inventory" });
            };

            for (sled_id, _) in collection.sled_agents {
                // TODO: Make sure "not found" doesn't stop execution
                let result = self.datastore.physical_disk_uninitialized_list(
                    opctx,
                    collection.id,
                    sled_id,
                ).await;

                let uninitialized = match result {
                    Ok(uninitialized) => uninitialized,
                    Err(err) => {
                        warn!(
                            &opctx.log,
                            "Physical Disk Adoption: failed to query for insertable disks";
                            "err" => %err,
                        );
                        return json!({ "error": format!("failed to query database: {:#}", err) });
                    },
                };

                for inv_disk in uninitialized {
                    let disk = PhysicalDisk::new(
                        Uuid::new_v4(),
                        inv_disk.vendor,
                        inv_disk.serial,
                        inv_disk.model,
                        inv_disk.variant,
                        inv_disk.sled_id,
                    );

                    let zpool = Zpool::new(
                        Uuid::new_v4(),
                        sled_id,
                        disk.id()
                    );

                    let result = self.datastore.physical_disk_and_zpool_insert(
                        opctx,
                        disk,
                        zpool
                    ).await;

                    if let Err(err) = result {
                        warn!(
                            &opctx.log,
                            "Physical Disk Adoption: failed to insert new disk and zpool";
                            "err" => %err
                        );
                        return json!({ "error": format!("failed to insert disk/zpool: {:#}", err) });
                    }

                    disks_added += 1;
                }
            }

            warn!(&log, "physical disk adoption task done");
            json!({
                "physical_disks_added": disks_added,
            })
        }
        .boxed()
    }
}
