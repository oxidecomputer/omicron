// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for cleaning database state for expunged disks.
//!
//! Although disk expungement prevents new allocations from going to
//! that physical disk, many other tables exist (e.g., datasets, the
//! zpool table) so that old resources using that disk can be rebalanced.
//!
//! This task clears those old allocations out, once it is appropriate to
//! do so.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintTarget};
use nexus_types::identity::Asset;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

pub struct PhysicalDiskCleanup {
    datastore: Arc<DataStore>,
    disable: bool,
    rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
    rx_blueprint: watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
}

impl PhysicalDiskCleanup {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_inventory_collection: watch::Receiver<Option<CollectionUuid>>,
        rx_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
        disable: bool,
    ) -> Self {
        PhysicalDiskCleanup {
            datastore,
            disable,
            rx_inventory_collection,
            rx_blueprint,
        }
    }
}

const MAX_CLEANUP_BATCH: NonZeroU32 = unsafe {
    // Safety: last time I checked, 100 was greater than zero.
    NonZeroU32::new_unchecked(100)
};

impl BackgroundTask for PhysicalDiskCleanup {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            if self.disable {
                return json!({ "error": "task disabled" });
            }
            let mut disks_cleared = 0;
            let mut disk_clear_errors = 0;

            let log = &opctx.log;
            warn!(&log, "physical disk cleanup task started");

            // TODO:
            // - Add datasets for zones that are being deployed?

            let mut paginator = Paginator::new(MAX_CLEANUP_BATCH);
            while let Some(p) = paginator.next() {
                let maybe_batch = self
                    .datastore
                    .physical_disk_out_of_service_with_zpool_list(
                        opctx,
                        &p.current_pagparams(),
                    )
                    .await;
                let batch = match maybe_batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        slog::error!(
                            log,
                            "list expunged disks query failed: {e}"
                        );
                        return serde_json::json!({ "error": e.to_string() });
                    }
                };
                paginator = p.found_batch(&batch, &|(disk, _)| disk.id());

                let mut batch = batch.into_iter();
                if let Some((phys_disk, zpool)) = batch.next() {
                    info!(log,
                        "Found an out-of-service disk with a zpool";
                        "disk_id" => phys_disk.id().to_string(),
                        "zpool_id" => zpool.id().to_string(),
                    );

                    let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id());
                    let phys_disk_id =
                        PhysicalDiskUuid::from_untyped_uuid(phys_disk.id());

                    if let Err(e) = self
                        .datastore
                        .physical_disk_set_decommissioned(
                            opctx,
                            phys_disk_id,
                            Some(zpool_id),
                        )
                        .await
                    {
                        slog::error!(
                            log,
                            "failed to decommission physical disk: {e}"
                        );
                        // Keep going to avoid errors
                        disk_clear_errors += 1;
                        continue;
                    }

                    disks_cleared += 1;
                }
            }

            warn!(&log, "physical disk cleanup task done");
            json!({
                "physical_disks_cleared": disks_cleared,
                "disk_clear_errors": disk_clear_errors,
            })
        }
        .boxed()
    }
}
