// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically adopting physical disks.
//!
//! Removable disks may be arbitrarily attached and detached from
//! Oxide racks. When this happens, if they had not previously
//! been part of a cluster, they need to be explicitly added
//! to become usable.
//!
//! In the future, this may become more explicitly operator-controlled.

use crate::app::CONTROL_PLANE_STORAGE_BUFFER;
use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_model::PhysicalDisk;
use nexus_db_model::Zpool;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

pub struct PhysicalDiskAdoption {
    datastore: Arc<DataStore>,
    disable: bool,
    rack_id: Uuid,
    rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
}

impl PhysicalDiskAdoption {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
        disable: bool,
        rack_id: Uuid,
    ) -> Self {
        PhysicalDiskAdoption {
            datastore,
            disable,
            rack_id,
            rx_inventory_collection,
        }
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

            // Only adopt physical disks after rack handoff has completed.
            //
            // This prevents a race condition where the same physical disks
            // are inserted simultaneously at handoff time and inside this
            // background task. This is bad because the handoff transaction will
            // fail if the same disk already exists.
            //
            // TODO-multirack: This will only work for clusters smaller than
            // a page.
            let result = self.datastore.rack_list_initialized(
                opctx,
                &DataPageParams::max_page()
            ).await;
            match result {
                Ok(racks) => {
                    if !racks.iter().any(|r| r.identity().id == self.rack_id) {
                        info!(
                            &opctx.log,
                            "Physical Disk Adoption: Rack not yet initialized";
                            "rack_id" => %self.rack_id,
                        );
                        let msg = format!("rack not yet initialized: {}", self.rack_id);
                        return json!({"error": msg});
                    }
                },
                Err(err) => {
                    warn!(
                        &opctx.log,
                        "Physical Disk Adoption: failed to query for initialized racks";
                        "err" => %err,
                    );
                    return json!({ "error": format!("failed to query database: {:#}", err) });
                }
            }

            let mut disks_added = 0;

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
                    PhysicalDiskUuid::new_v4(),
                    inv_disk.vendor,
                    inv_disk.serial,
                    inv_disk.model,
                    inv_disk.variant,
                    inv_disk.sled_id.into(),
                );

                let zpool = Zpool::new(
                    Uuid::new_v4(),
                    inv_disk.sled_id.into(),
                    disk.id(),
                    CONTROL_PLANE_STORAGE_BUFFER.into(),
                );

                let result = self.datastore.physical_disk_and_zpool_insert(
                    opctx,
                    disk.clone(),
                    zpool
                ).await;

                if let Err(err) = result {
                    warn!(
                        &opctx.log,
                        "Physical Disk Adoption: failed to insert new disk and zpool";
                        "err" => %err
                    );
                    let msg = format!(
                        "failed to insert disk/zpool: {:#}; disk = {:#?}",
                        err,
                        disk
                    );
                    return json!({ "error": msg});
                }

                disks_added += 1;

                info!(
                    &opctx.log,
                    "Physical Disk Adoption: Successfully added a new disk and zpool";
                    "disk" => #?disk
                );
            }

            json!({
                "physical_disks_added": disks_added,
            })
        }
        .boxed()
    }
}
