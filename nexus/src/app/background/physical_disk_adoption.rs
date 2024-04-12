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
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

pub struct PhysicalDiskAdoption {
    datastore: Arc<DataStore>,
    disable: bool,
    rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
}

impl PhysicalDiskAdoption {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
        disable: bool,
    ) -> Self {
        PhysicalDiskAdoption { datastore, disable, rx_inventory_collection }
    }
}

impl BackgroundTask for PhysicalDiskAdoption {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            if self.disable {
                return json!({ "error": "task disabled" });
            }

            let mut disks_added = 0;
            let log = &opctx.log;
            warn!(&log, "physical disk adoption task started");

            let collection_id = *self.rx_inventory_collection.borrow();
            let Some(collection_id) = collection_id else {
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: skipped";
                    "reason" => "no inventory"
                );
                return json!({ "error": "no inventory" });
            };

            let result = self.datastore.physical_disk_uninitialized_list(
                opctx,
                collection_id,
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
                    inv_disk.sled_id.into_untyped_uuid(),
                );

                let zpool = Zpool::new(
                    Uuid::new_v4(),
                    inv_disk.sled_id.into_untyped_uuid(),
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

                info!(
                    &opctx.log,
                    "Physical Disk Adoption: Successfully added a new disk and zpool"
                );
            }

            warn!(&log, "physical disk adoption task done");
            json!({
                "physical_disks_added": disks_added,
            })
        }
        .boxed()
    }
}
