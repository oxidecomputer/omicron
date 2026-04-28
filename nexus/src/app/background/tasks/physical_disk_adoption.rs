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
use nexus_types::inventory::Collection;
use omicron_common::api::external;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

pub struct PhysicalDiskAdoption {
    datastore: Arc<DataStore>,
    disable: bool,
    rack_id: Uuid,
    rx_inventory_collection: watch::Receiver<Option<Arc<Collection>>>,
}

impl PhysicalDiskAdoption {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_inventory_collection: watch::Receiver<Option<Arc<Collection>>>,
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

    /// Insert disks and zpools for any adoptable physical disks
    async fn physical_disk_adopt(
        &self,
        opctx: &OpContext,
        collection_id: CollectionUuid,
    ) -> serde_json::Value {
        let mut disks_added = 0;

        let result = self
            .datastore
            .physical_disk_adoptable_list(opctx, collection_id)
            .await;

        let adoptable = match result {
            Ok(adoptable) => adoptable,
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: \
                     failed to query for insertable disks";
                    &err,
                );
                let err = format!("failed to query database: {err}");
                return json!({ "error": err });
            }
        };

        for inv_disk in adoptable {
            let sled_id = inv_disk.sled_id.into();
            let disk = PhysicalDisk::new(inv_disk);

            let zpool = Zpool::new(
                ZpoolUuid::new_v4(),
                sled_id,
                disk.id(),
                CONTROL_PLANE_STORAGE_BUFFER.into(),
            );

            let result = self
                .datastore
                .physical_disk_and_zpool_insert(opctx, disk.clone(), zpool)
                .await;

            if let Err(err) = result {
                // Skip reporting the error if we get back a `NotFound`.
                // This means that another nexus concurrently added the
                // disk or that the adoption request was deleted. We
                // don't want to report mistakenly one way or another and
                // so we just continue here.
                if let external::Error::NotFound { .. } = err {
                    continue;
                }

                let err = InlineErrorChain::new(&err);
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: \
                     failed to insert new disk and zpool";
                    &err,
                );
                let msg = format!(
                    "failed to insert disk/zpool: {err}; disk = {disk:#?}",
                );
                return json!({ "error": msg });
            }

            disks_added += 1;

            info!(
                &opctx.log,
                "Physical Disk Adoption: \
                 Successfully added a new disk and zpool";
                "disk" => #?disk
            );
        }

        json!({
            "physical_disks_added": disks_added,
        })
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
            let result = self
                .datastore
                .rack_list_initialized(opctx, &DataPageParams::max_page())
                .await;
            match result {
                Ok(racks) => {
                    if !racks.iter().any(|r| r.identity().id == self.rack_id) {
                        info!(
                            &opctx.log,
                            "Physical Disk Adoption: Rack not yet initialized";
                            "rack_id" => %self.rack_id,
                        );
                        let msg = format!(
                            "rack not yet initialized: {}",
                            self.rack_id
                        );
                        return json!({"error": msg});
                    }
                }
                Err(err) => {
                    let err = InlineErrorChain::new(&err);
                    warn!(
                        &opctx.log,
                        "Physical Disk Adoption: \
                         failed to query for initialized racks";
                        &err,
                    );
                    let err = format!("failed to query database: {err}");
                    return json!({ "error": err });
                }
            }

            // Grab the most-recently-loaded collection ID. The ID is `Copy`, so
            // we can grab it and then unlock the watch channel.
            let Some(collection_id) = self
                .rx_inventory_collection
                .borrow_and_update()
                .as_ref()
                .map(|c| c.id)
            else {
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: skipped";
                    "reason" => "no inventory"
                );
                return json!({ "error": "no inventory" });
            };

            // We only force manual re-adoption of expunged disks currently.
            // In the future we wish to enable manual adoption of all disks
            // for security reasons. Doing this now would be a somewhat large
            // undertaking and unergonomic if done naively. The naive approach
            // would require users to have to issue adoption requests for up to
            // 10 disks when they add a new sled to a rack.
            //
            // However, we also don't want to bifurcate the code paths that
            // check to see if adoption is enabled. Therefore in the case of
            // new disks we automatically fill in an adoption request as if an
            // operator had done so manually via the external API.
            if let Err(err) = self
                .datastore
                .physical_disk_enable_adoption_for_all_new_disks_in_inventory(
                    opctx,
                    collection_id,
                )
                .await
            {
                let err = InlineErrorChain::new(&err);
                warn!(
                    &opctx.log,
                    "Physical Disk Adoption: \
                     failed to enable adoption for new disks";
                    &err,
                );

                // We still want to adopt disks that already are enabled for
                // adoption below so we fall through.
            }

            self.physical_disk_adopt(opctx, collection_id).await
        }
        .boxed()
    }
}
