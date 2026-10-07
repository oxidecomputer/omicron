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
use nexus_db_queries::db::datastore::LocalStorageDisk;
use nexus_types::internal_api::background::LocalStorageDeleteStatus;
use omicron_common::api::external::DataPageParams;
use serde_json::json;
use sled_agent_client::types::LocalStorageDatasetDeleteRequest;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::num::NonZeroU32;
use std::sync::Arc;

pub struct LocalStorageDeleter {
    datastore: Arc<DataStore>,
    reqwest_client: reqwest::Client,
}

enum DeleteResult {
    /// This invocation of the task deleted the local storage allocation
    Deleted,

    /// This invocation of the task requested deletion but needs to wait
    TimedOut,

    /// Error while making delete request
    Error { message: String },
}

enum DeleteShortCircuitReason {
    /// This local storage disk does not have an allocation
    NoAllocation,

    /// No delete is required as the sled hosting the local storage allocation
    /// was expunged.
    SledExpunged,

    /// No delete is required as the zpool hosting the local storage allocation
    /// was expunged.
    ZpoolExpunged,
}

enum DeleteRequest {
    /// No delete request was made.
    ShortCircuit { disk: LocalStorageDisk, reason: DeleteShortCircuitReason },

    /// Error before making a delete request
    Error { disk: LocalStorageDisk, message: String },

    /// A delete is being performed in a spawned tokio task
    Spawned {
        disk: LocalStorageDisk,
        result: tokio::task::JoinHandle<DeleteResult>,
    },
}

impl LocalStorageDeleter {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        let duration = std::time::Duration::from_secs(15);

        LocalStorageDeleter {
            datastore,
            // Create a client with a short timeout, don't block this task
            // waiting for request responses.
            reqwest_client: reqwest::ClientBuilder::new()
                .connect_timeout(duration)
                .timeout(duration)
                .build()
                .unwrap(),
        }
    }

    async fn request_disk_allocation_deletion(
        &self,
        log: &Logger,
        opctx: &OpContext,
        disk: LocalStorageDisk,
    ) -> DeleteRequest {
        let Some(allocation) = &disk.local_storage_dataset_allocation else {
            // No allocation was made for this disk
            return DeleteRequest::ShortCircuit {
                disk,
                reason: DeleteShortCircuitReason::NoAllocation,
            };
        };

        match allocation {
            LocalStorageAllocation::Unencrypted(_) => {
                // continue with rest of function
            }

            LocalStorageAllocation::Encrypted(allocation) => {
                // Until encrypted local storage is supported, seeing a request
                // to clean up disks of that type should be noted as a error.
                let message = format!(
                    "request to delete disk {} encrypted allocation {}",
                    disk.id(),
                    allocation.id(),
                );

                return DeleteRequest::Error { disk, message };
            }
        }

        let sled_id = allocation.sled_id();
        let zpool_id = allocation.pool_id().upcast();

        // Check if either the sled or disk backing the zpool was expunged. If
        // we can't determine this then bail and wait for the next task
        // activation.

        let sled_in_service =
            match self.datastore.check_sled_in_service(&opctx, sled_id).await {
                Ok(sled_in_service) => sled_in_service,

                Err(e) => {
                    let message = format!(
                        "error calling check_sled_in_service for sled \
                        {sled_id}: {}",
                        InlineErrorChain::new(&e),
                    );

                    return DeleteRequest::Error { disk, message };
                }
            };

        if !sled_in_service {
            // Sled's been expunged, so consider the local storage deleted.
            return DeleteRequest::ShortCircuit {
                disk,
                reason: DeleteShortCircuitReason::SledExpunged,
            };
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

                    return DeleteRequest::Error { disk, message };
                }
            };

        if !zpool_in_service {
            // The disk backing the zpool's been expunged, so consider the local
            // storage deleted.
            return DeleteRequest::ShortCircuit {
                disk,
                reason: DeleteShortCircuitReason::ZpoolExpunged,
            };
        }

        // Now that all checks are done, get a sled agent client and make the
        // delete request.

        let request = LocalStorageDatasetDeleteRequest {
            zpool_id: allocation.pool_id(),
            dataset_id: allocation.id(),
            encrypted_at_rest: allocation.encrypted_at_rest(),
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

                return DeleteRequest::Error { disk, message };
            }
        };

        DeleteRequest::Spawned {
            disk,
            result: tokio::spawn(async move {
                match sled_agent_client
                    .local_storage_dataset_delete(&request)
                    .await
                {
                    Ok(_) => DeleteResult::Deleted,

                    Err(progenitor_client::Error::CommunicationError(e))
                        if e.is_timeout() =>
                    {
                        // The request timed out but is still being processed by the
                        // remote sled-agent.
                        DeleteResult::TimedOut
                    }

                    Err(e) => {
                        let message = format!(
                            "error sending local_storage_dataset_delete: {}",
                            InlineErrorChain::new(&e),
                        );

                        DeleteResult::Error { message }
                    }
                }
            }),
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
            .deleted_disks_with_undeleted_local_storage(
                opctx,
                // Operate on a maximum of 128 disks per task invocation. If
                // users create disks very quickly during the time between this
                // task's periodic invocation (and then deletes them all), we
                // could be faced with many disks to delete. Each Nexus that
                // then activates this task would fetch all those disks for
                // deletion, potentially causing each of the tasks to take a
                // long time.
                &DataPageParams::ascending_with_limit(
                    NonZeroU32::new(128).unwrap(),
                ),
            )
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

        let mut delete_requests =
            Vec::with_capacity(disks_needing_clean_up.len());

        // Attempt deleting the local storage allocation before marking the
        // allocation's database record as deleted. If the delete request to the
        // sled agent does not succeed, try again in the next task activation.
        // The delete requests themselves are spawned into tokio tasks.

        for disk in disks_needing_clean_up {
            delete_requests.push(
                self.request_disk_allocation_deletion(log, opctx, disk).await,
            );
        }

        for request in delete_requests {
            let disk = match request {
                DeleteRequest::ShortCircuit { disk, reason } => {
                    match reason {
                        DeleteShortCircuitReason::NoAllocation => {
                            // `deleted_disks_with_undeleted_local_storage`
                            // should not be returning these disks! Return an
                            // error.

                            let s = format!(
                                "disk {} does not have a local storage \
                                allocation",
                                disk.id(),
                            );

                            error!(log, "{s}");
                            status.errors.push(s);

                            continue;
                        }

                        DeleteShortCircuitReason::SledExpunged => {
                            let s = format!(
                                "disk {} allocation {:?} sled expunged, \
                                considering deleted",
                                disk.id(),
                                disk.allocation_id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Continue through to allocation record deletion,
                            // Nexus can treat this disks' allocation as gone.

                            disk
                        }

                        DeleteShortCircuitReason::ZpoolExpunged => {
                            let s = format!(
                                "disk {} allocation {:?} zpool expunged, \
                                considering deleted",
                                disk.id(),
                                disk.allocation_id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Continue through to allocation record deletion,
                            // Nexus can treat this disks' allocation as gone.

                            disk
                        }
                    }
                }

                DeleteRequest::Error { disk, message } => {
                    let s = format!(
                        "could not request deletion of disk {} allocation \
                        {:?}: {message}",
                        disk.id(),
                        disk.allocation_id(),
                    );

                    error!(log, "{s}");
                    status.errors.push(s);

                    continue;
                }

                DeleteRequest::Spawned { disk, result } => {
                    // Check the returned DeleteResult to see if allocation
                    // record can be deleted.

                    let result = match result.await {
                        Ok(result) => result,
                        Err(join_error) => {
                            let s = format!(
                                "request to delete disk {} allocation {:?} \
                                failed with join error {:?}",
                                disk.id(),
                                disk.allocation_id(),
                                join_error,
                            );

                            error!(log, "{s}");
                            status.errors.push(s);
                            continue;
                        }
                    };

                    match result {
                        DeleteResult::Deleted => {
                            let s = format!(
                                "deleted disk {} allocation {:?}",
                                disk.id(),
                                disk.allocation_id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Continue through to allocation record deletion.

                            disk
                        }

                        DeleteResult::TimedOut => {
                            let s = format!(
                                "requested deletion of disk {} allocation \
                                {:?} timed out",
                                disk.id(),
                                disk.allocation_id(),
                            );

                            info!(log, "{s}");
                            status.delete_results.push(s);

                            // Cannot delete allocation record until the
                            // deletion request to the sled agent succeeds.

                            continue;
                        }

                        DeleteResult::Error { message } => {
                            info!(log, "{message}");
                            status.errors.push(message);

                            // Cannot delete allocation record until the
                            // deletion request to the sled agent succeeds.

                            continue;
                        }
                    }
                }
            };

            match self
                .datastore
                .delete_local_storage_dataset_allocation(opctx, &disk)
                .await
            {
                Ok(()) => {
                    let s = format!(
                        "deleted disk {} allocation {:?} record",
                        disk.id(),
                        disk.allocation_id(),
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
