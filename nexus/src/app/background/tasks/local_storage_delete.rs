// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! When the higher level disks backed by local storage are deleted, this
//! background task will delete any allocated local storage, and will delete
//! local storage allocation records when those resources have been cleaned up.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::LocalStorageAllocation;
use nexus_db_queries::db::model::LocalStorageUnencryptedDatasetAllocation;
use nexus_types::internal_api::background::LocalStorageDeleteStatus;
use serde_json::json;
use sled_agent_client::types::LocalStorageDatasetDeleteRequest;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

pub struct LocalStorageDeleter {
    datastore: Arc<DataStore>,
    reqwest_client: reqwest::Client,
}

#[derive(PartialEq)]
enum DeleteResult {
    /// This invocation of the task deleted the local storage allocation
    Deleted,

    /// No delete is required as the sled hosting the local storage allocation
    /// was expunged.
    SledExpunged,

    /// No delete is required as the zpool hosting the local storage allocation
    /// was expunged.
    ZpoolExpunged,

    /// This invocation of the task requested deletion but needs to wait
    WaitForNextActivation,

    Error {
        message: String,
    },
}

impl LocalStorageDeleter {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let duration = std::time::Duration::from_millis(250);

        LocalStorageDeleter {
            datastore,
            // Create a client with a _short_ timeout, don't block this task
            // waiting for request responses.
            reqwest_client: reqwest::ClientBuilder::new()
                .connect_timeout(duration)
                .timeout(duration)
                .build()
                .unwrap(),
        }
    }

    async fn delete_unencrypted_allocation(
        &self,
        log: &Logger,
        opctx: &OpContext,
        allocation: &LocalStorageUnencryptedDatasetAllocation,
    ) -> DeleteResult {
        let sled_id = allocation.sled_id();
        let zpool_id = allocation.pool_id().upcast();

        // Check if either the sled or disk backing the zpool was expunged. If
        // we can't determine then bail and wait for the next task activation.

        let sled_in_service =
            match self.datastore.check_sled_in_service(&opctx, sled_id).await {
                Ok(sled_in_service) => sled_in_service,

                Err(e) => {
                    let message = format!(
                        "error calling check_sled_in_service for sled \
                        {sled_id}: {}",
                        InlineErrorChain::new(&e),
                    );

                    return DeleteResult::Error { message };
                }
            };

        if !sled_in_service {
            // Sled's been expunged, so consider the local storage deleted.
            return DeleteResult::SledExpunged;
        }

        let zpool_in_service =
            match self.datastore.check_zpool_in_service(&opctx, zpool_id).await
            {
                Ok(zpool_in_service) => zpool_in_service,

                Err(e) => {
                    let message = format!(
                        "error calling check_zpool_in_service for zpool \
                        {zpool_id}: {}",
                        InlineErrorChain::new(&e),
                    );

                    return DeleteResult::Error { message };
                }
            };

        if !zpool_in_service {
            // The disk backing the zpool's been expunged, so consider the local
            // storage deleted.
            return DeleteResult::ZpoolExpunged;
        }

        // Now that all checks are done, get a sled agent client and make the
        // delete request.

        let request = LocalStorageDatasetDeleteRequest {
            zpool_id: allocation.pool_id(),
            dataset_id: allocation.id(),
            encrypted_at_rest: false,
        };

        let sled_agent_client = match nexus_networking::sled_client_ext(
            &self.datastore,
            opctx,
            sled_id,
            log,
            self.reqwest_client.clone(),
        )
        .await
        {
            Ok(client) => client,

            Err(e) => {
                let message = format!(
                    "error calling sled_client_ext for sled {sled_id}: {}",
                    InlineErrorChain::new(&e),
                );

                return DeleteResult::Error { message };
            }
        };

        match sled_agent_client.local_storage_dataset_delete(&request).await {
            Ok(_) => DeleteResult::Deleted,

            Err(progenitor_client::Error::CommunicationError(e))
                if e.is_timeout() =>
            {
                // The request timed out but is still being processed by the
                // remote sled-agent.
                DeleteResult::WaitForNextActivation
            }

            Err(e) => {
                let message = format!(
                    "error sending local_storage_dataset_delete: {}",
                    InlineErrorChain::new(&e),
                );

                DeleteResult::Error { message }
            }
        }
    }

    async fn activate_impl(
        &self,
        opctx: &OpContext,
    ) -> LocalStorageDeleteStatus {
        let log = &opctx.log;
        let mut status = LocalStorageDeleteStatus::default();

        let disks_needing_clean_up = match self
            .datastore
            .deleted_disks_with_undeleted_local_storage(opctx)
            .await
        {
            Ok(v) => v,

            Err(e) => {
                let s = format!(
                    "error calling \
                        deleted_disks_with_undeleted_local_storage: {}",
                    InlineErrorChain::new(&e),
                );

                error!(log, "{s}");
                status.errors.push(s);

                return status;
            }
        };

        // Report the remaining work to do in the status.

        status.total_allocations_to_delete = disks_needing_clean_up.len();

        // Operate on a maximum of 128 disks per task invocation. If users
        // create disks very quickly during the time between this task's
        // periodic invocation (and then deletes them all), we could be faced
        // with many disks to delete. Each Nexus that then activates this task
        // would fetch all those disks for deletion, potentially causing each of
        // the tasks to take a long time.
        //
        // The timeout for the reqwest client created by this task is 250
        // milliseconds, so the maximum time (assuming the requests to all
        // sled-agents time out) is 128 * 250 ms = 32 seconds, roughly
        // approximating the periodic task wakeup time. Note that each local
        // storage disk delete will activate this task, so the latency between
        // the delete request and the actual deletion should remain low,
        // assuming there aren't too many lingering problematic allocations.

        status.page_size = 128;

        for disk in disks_needing_clean_up.into_iter().take(status.page_size) {
            let Some(allocation) = &disk.local_storage_dataset_allocation
            else {
                // No allocation was made for this disk
                continue;
            };

            // Attempt deleting the local storage before removing the
            // database record. If the delete does not succeed, try again in
            // the next task activation.

            match allocation {
                LocalStorageAllocation::Unencrypted(allocation) => {
                    match self
                        .delete_unencrypted_allocation(log, opctx, &allocation)
                        .await
                    {
                        DeleteResult::Deleted => {
                            let s = format!(
                                "deleted disk {} allocation {}",
                                disk.id(),
                                allocation.id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Drop through to deallocation once deletion
                            // succeeds.
                        }

                        DeleteResult::SledExpunged => {
                            let s = format!(
                                "disk {} allocation {} sled expunged, \
                                considering deleted",
                                disk.id(),
                                allocation.id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Drop through to deallocation, deletion not
                            // required.
                        }

                        DeleteResult::ZpoolExpunged => {
                            let s = format!(
                                "disk {} allocation {} zpool expunged, \
                                considering deleted",
                                disk.id(),
                                allocation.id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Drop through to deallocation, deletion not
                            // required.
                        }

                        DeleteResult::WaitForNextActivation => {
                            let s = format!(
                                "requested deletion of disk {} allocation \
                                {}",
                                disk.id(),
                                allocation.id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Cannot deallocate the record until deletion
                            // succeeds.
                            continue;
                        }

                        DeleteResult::Error { message } => {
                            info!(log, "{message}");
                            status.errors.push(message);

                            // Cannot deallocate the record until deletion
                            // succeeds.
                            continue;
                        }
                    }
                }

                LocalStorageAllocation::Encrypted(allocation) => {
                    // Until encrypted local storage is supported, seeing a
                    // request to clean up disks of that type should be
                    // noted as a error.
                    let s = format!(
                        "request to delete disk {} encrypted allocation {}",
                        disk.id(),
                        allocation.id(),
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    continue;
                }
            }

            match self
                .datastore
                .delete_local_storage_dataset_allocation(opctx, &disk)
                .await
            {
                Ok(()) => {
                    let s = format!(
                        "deallocated disk {} allocation {}",
                        disk.id(),
                        allocation.id(),
                    );

                    info!(log, "{s}");
                    status.deallocate_results.push(s);
                }

                Err(e) => {
                    let s = format!(
                        "error calling \
                        delete_local_storage_dataset_allocation: {}",
                        InlineErrorChain::new(&e),
                    );

                    error!(log, "{s}");
                    status.errors.push(s);
                }
            }
        }

        status
    }
}

impl BackgroundTask for LocalStorageDeleter {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let status = self.activate_impl(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(e) => json!({
                    "error": format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&e),
                    )
                }),
            }
        }
        .boxed()
    }
}
