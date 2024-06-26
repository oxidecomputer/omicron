// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for detecting and un-deleting phantom disks
//!
//! A "phantom" disk is one where a disk delete saga partially completed but
//! unwound: before a fix for customer-support#58, this would leave disks
//! deleted but would also leave a `virtual_provisioning_resource` record for
//! that disk. There would be no way to re-trigger the disk delete saga as the
//! disk was deleted, so the project that disk was in could not be deleted
//! because associated virtual provisioning resources were still being consumed.
//!
//! The fix for customer-support#58 changes the disk delete saga's unwind to
//! also un-delete the disk and set it to faulted. This enables it to be deleted
//! again. Correcting the disk delete saga's unwind means that phantom disks
//! will not be created in the future when the disk delete saga unwinds, but
//! this background task is required to apply the same fix for disks that are
//! already in this phantom state.

use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;

pub struct PhantomDiskDetector {
    datastore: Arc<DataStore>,
}

impl PhantomDiskDetector {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        PhantomDiskDetector { datastore }
    }
}

impl BackgroundTask for PhantomDiskDetector {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            warn!(&log, "phantom disk task started");

            let phantom_disks = match self.datastore.find_phantom_disks().await
            {
                Ok(phantom_disks) => phantom_disks,
                Err(e) => {
                    warn!(&log, "error from find_phantom_disks: {:?}", e);
                    return json!({
                    "error":
                        format!("failed find_phantom_disks: {:#}", e)
                    });
                }
            };

            let mut phantom_disk_deleted_ok = 0;
            let mut phantom_disk_deleted_err = 0;

            for disk in phantom_disks {
                warn!(&log, "phantom disk {} found!", disk.id());

                // If a phantom disk is found, then un-delete it and set it to
                // faulted: this will allow a user to request deleting it again.

                let result = self
                    .datastore
                    .project_undelete_disk_set_faulted_no_auth(&disk.id())
                    .await;

                if let Err(e) = result {
                    error!(
                        &log,
                        "error un-deleting disk {} and setting to faulted: \
                         {:#}",
                        disk.id(),
                        e,
                    );
                    phantom_disk_deleted_err += 1;
                } else {
                    info!(
                        &log,
                        "phandom disk {} un-deleted andset to faulted ok",
                        disk.id(),
                    );
                    phantom_disk_deleted_ok += 1;
                }
            }

            warn!(&log, "phantom disk task done");
            json!({
                "phantom_disk_deleted_ok": phantom_disk_deleted_ok,
                "phantom_disk_deleted_err": phantom_disk_deleted_err,
            })
        }
        .boxed()
    }
}
