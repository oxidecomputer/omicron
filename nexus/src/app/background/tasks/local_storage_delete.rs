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

/// Functions that poll for an expected change will either return that the
/// change occurred, or that the current activation of this task has to wait for
/// the change to occur in a future activations of this task.
#[derive(PartialEq)]
enum DeleteResult {
    Deleted,

    WaitForNextActivation,
}

impl LocalStorageDeleter {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        LocalStorageDeleter {
            datastore,
            reqwest_client: reqwest::Client::new(),
        }
    }

    async fn delete_unencrypted_allocation(
        &self,
        log: &Logger,
        opctx: &OpContext,
        allocation: &LocalStorageUnencryptedDatasetAllocation,
        status: &mut LocalStorageDeleteStatus,
    ) -> DeleteResult {
        let sled_id = allocation.sled_id();
        let zpool_id = allocation.pool_id().upcast();

        // Check if either the sled or disk backing the zpool was expunged. If
        // we can't determine then bail and wait for the next task activation.

        let sled_in_service =
            match self.datastore.check_sled_in_service(&opctx, sled_id).await {
                Ok(sled_in_service) => sled_in_service,

                Err(e) => {
                    let s = format!(
                        "error calling check_sled_in_service for sled \
                        {sled_id}: {e}",
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    return DeleteResult::WaitForNextActivation;
                }
            };

        if !sled_in_service {
            // Sled's been expunged, so consider the local storage deleted.
            return DeleteResult::Deleted;
        }

        let zpool_in_service =
            match self.datastore.check_zpool_in_service(&opctx, zpool_id).await
            {
                Ok(zpool_in_service) => zpool_in_service,

                Err(e) => {
                    let s = format!(
                        "error calling check_zpool_in_service for zpool \
                        {zpool_id}: {e}",
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    return DeleteResult::WaitForNextActivation;
                }
            };

        if !zpool_in_service {
            // The disk backing the zpool's been expunged, so consider the local
            // storage deleted.
            return DeleteResult::Deleted;
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
                let s = format!(
                    "error calling sled_client_ext for allocation {}: {}",
                    allocation.id(),
                    InlineErrorChain::new(&e),
                );

                error!(log, "{s}");
                status.errors.push(s);

                return DeleteResult::WaitForNextActivation;
            }
        };

        match sled_agent_client.local_storage_dataset_delete(&request).await {
            Ok(_) => DeleteResult::Deleted,

            Err(e) => {
                let s = format!(
                    "error sending local_storage_dataset_delete: {}",
                    InlineErrorChain::new(&e),
                );

                error!(log, "{s}");
                status.errors.push(s);

                DeleteResult::WaitForNextActivation
            }
        }
    }
}

impl BackgroundTask for LocalStorageDeleter {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
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
                            deleted_disks_with_undeleted_local_storage: {e}"
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    return json!(status);
                }
            };

            for disk in disks_needing_clean_up {
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
                            .delete_unencrypted_allocation(
                                log,
                                opctx,
                                &allocation,
                                &mut status,
                            )
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

                            DeleteResult::WaitForNextActivation => {
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
                    .delete_local_storage_dataset_allocations(opctx, &disk)
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
                            delete_local_storage_dataset_allocations: {e}"
                        );

                        error!(log, "{s}");
                        status.errors.push(s);

                        continue;
                    }
                }
            }

            json!(status)
        }
        .boxed()
    }
}
