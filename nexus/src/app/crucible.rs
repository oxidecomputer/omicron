// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with Crucible agents

use super::*;

use anyhow::anyhow;
use crucible_agent_client::types::CreateRegion;
use crucible_agent_client::types::GetSnapshotResponse;
use crucible_agent_client::types::Region;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::types::State as RegionState;
use crucible_agent_client::Client as CrucibleAgentClient;
use futures::StreamExt;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetry;
use omicron_common::progenitor_operation_retry::ProgenitorOperationRetryError;
use slog::Logger;

// Arbitrary limit on concurrency, for operations issued on multiple regions
// within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;

/// Provides a way for (with BackoffError) Permanent errors to have a different error type than
/// Transient errors.
#[derive(Debug, thiserror::Error)]
enum WaitError {
    #[error("Transient error: {0}")]
    Transient(#[from] anyhow::Error),

    #[error("Permanent error: {0}")]
    Permanent(#[from] Error),
}

/// Convert an error returned from the ProgenitorOperationRetry loops in this
/// file into an external Error
fn into_external_error(
    e: ProgenitorOperationRetryError<crucible_agent_client::types::Error>,
) -> Error {
    match e {
        ProgenitorOperationRetryError::Gone => Error::Gone,

        ProgenitorOperationRetryError::GoneCheckError(e) => {
            Error::internal_error(&format!(
                "insufficient permission for crucible_agent_gone_check: {e}"
            ))
        }

        ProgenitorOperationRetryError::ProgenitorError(e) => match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                if rv.status().is_client_error() {
                    Error::invalid_request(&rv.message)
                } else {
                    Error::internal_error(&rv.message)
                }
            }

            _ => Error::internal_error(&format!("unexpected failure: {e}",)),
        },
    }
}

/// Application level operations on Crucible
#[derive(Clone)]
pub struct Crucible {
    datastore: Arc<db::DataStore>,
    reqwest_client: reqwest::Client,
}

impl Crucible {
    pub fn new(
        datastore: Arc<db::DataStore>,
        reqwest_client: reqwest::Client,
    ) -> Crucible {
        Crucible { datastore, reqwest_client }
    }

    fn crucible_agent_client_for_dataset(
        &self,
        dataset: &db::model::Dataset,
    ) -> CrucibleAgentClient {
        CrucibleAgentClient::new_with_client(
            &format!("http://{}", dataset.address()),
            self.reqwest_client.clone(),
        )
    }

    /// Return if the Crucible agent is expected to be there and answer Nexus:
    /// true means it's gone, and the caller should bail out of the
    /// ProgenitorOperationRetry loop.
    async fn crucible_agent_gone_check(
        &self,
        dataset_id: Uuid,
    ) -> Result<bool, Error> {
        let on_in_service_physical_disk =
            self.datastore.dataset_physical_disk_in_service(dataset_id).await?;

        Ok(!on_in_service_physical_disk)
    }

    /// Call out to Crucible agent and perform region creation.
    async fn ensure_region_in_dataset(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region: &db::model::Region,
    ) -> Result<Region, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let Ok(extent_count) = u32::try_from(region.extent_count()) else {
            return Err(Error::internal_error(
                "Extent count out of range for a u32",
            ));
        };

        let region_request = CreateRegion {
            block_size: region.block_size().to_bytes(),
            extent_count,
            extent_size: region.blocks_per_extent(),
            // TODO: Can we avoid casting from UUID to string?
            // NOTE: This'll require updating the crucible agent client.
            id: RegionId(region.id().to_string()),
            encrypted: region.encrypted(),
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        };

        let create_region = || async {
            let region = match ProgenitorOperationRetry::new(
                || async { client.region_create(&region_request).await },
                || async { self.crucible_agent_gone_check(dataset_id).await },
            )
            .run(log)
            .await
            {
                Ok(v) => Ok(v),

                Err(e) => {
                    error!(
                        log,
                        "region_create saw {:?}",
                        e;
                        "region_id" => %region.id(),
                        "dataset_id" => %dataset_id,
                    );

                    // Return an error if Nexus is unable to create the
                    // requested region
                    Err(BackoffError::Permanent(WaitError::Permanent(
                        into_external_error(e),
                    )))
                }
            }?;

            match region.state {
                RegionState::Requested => {
                    Err(BackoffError::transient(WaitError::Transient(anyhow!(
                        "Region creation in progress"
                    ))))
                }

                RegionState::Created => Ok(region),

                _ => Err(BackoffError::Permanent(WaitError::Permanent(
                    Error::internal_error(&format!(
                        "Failed to create region, unexpected state: {:?}",
                        region.state
                    )),
                ))),
            }
        };

        let log_create_failure = |_, delay| {
            warn!(
                log,
                "Region requested, not yet created. Retrying in {:?}",
                delay;
                "dataset" => %dataset.id(),
                "region" => %region.id(),
            );
        };

        let region = backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            create_region,
            log_create_failure,
        )
        .await
        .map_err(|e| match e {
            WaitError::Transient(e) => {
                // The backoff crate can be configured with a maximum elapsed
                // time before giving up, which means that Transient could be
                // returned here. Our current policies do **not** set this
                // though.
                Error::internal_error(&e.to_string())
            }

            WaitError::Permanent(e) => e,
        })?;

        Ok(region.into_inner())
    }

    /// Returns a Ok(Some(Region)) if a region with id {region_id} exists,
    /// Ok(None) if it does not (a 404 was seen), and Err otherwise.
    async fn maybe_get_crucible_region(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
    ) -> Result<Option<Region>, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let result = ProgenitorOperationRetry::new(
            || async {
                client.region_get(&RegionId(region_id.to_string())).await
            },
            || async { self.crucible_agent_gone_check(dataset_id).await },
        )
        .run(log)
        .await;

        match result {
            Ok(v) => Ok(Some(v.into_inner())),

            Err(e) => {
                if e.is_not_found() {
                    // A 404 Not Found is ok for this function, just return None
                    Ok(None)
                } else {
                    error!(
                        log,
                        "region_get saw {:?}",
                        e;
                        "region_id" => %region_id,
                        "dataset_id" => %dataset_id,
                    );

                    // Return an error if Nexus is unable to query the dataset's
                    // agent for the requested region
                    Err(into_external_error(e))
                }
            }
        }
    }

    async fn get_crucible_region_snapshots(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
    ) -> Result<GetSnapshotResponse, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let result = ProgenitorOperationRetry::new(
            || async {
                client
                    .region_get_snapshots(&RegionId(region_id.to_string()))
                    .await
            },
            || async { self.crucible_agent_gone_check(dataset_id).await },
        )
        .run(log)
        .await;

        match result {
            Ok(v) => Ok(v.into_inner()),

            Err(e) => {
                error!(
                    log,
                    "region_get_snapshots saw {:?}",
                    e;
                    "region_id" => %region_id,
                    "dataset_id" => %dataset_id,
                );

                // Return an error if Nexus is unable to query the dataset's
                // agent for the requested region 's snapshots
                Err(into_external_error(e))
            }
        }
    }

    /// Send a region deletion request
    async fn request_crucible_region_delete(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let result = ProgenitorOperationRetry::new(
            || async {
                client.region_delete(&RegionId(region_id.to_string())).await
            },
            || async { self.crucible_agent_gone_check(dataset_id).await },
        )
        .run(log)
        .await;

        match result {
            Ok(_) => Ok(()),

            Err(e) => {
                if e.is_gone() {
                    // Return Ok if the dataset's agent is gone, no delete call
                    // is required.
                    Ok(())
                } else {
                    error!(
                        log,
                        "region_delete saw {:?}",
                        e;
                        "region_id" => %region_id,
                        "dataset_id" => %dataset.id(),
                    );

                    Err(into_external_error(e))
                }
            }
        }
    }

    /// Send a running snapshot deletion request
    async fn request_crucible_running_snapshot_delete(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let result = ProgenitorOperationRetry::new(
            || async {
                client
                    .region_delete_running_snapshot(
                        &RegionId(region_id.to_string()),
                        &snapshot_id.to_string(),
                    )
                    .await
            },
            || async { self.crucible_agent_gone_check(dataset_id).await },
        )
        .run(log)
        .await;

        match result {
            Ok(_) => Ok(()),

            Err(e) => {
                if e.is_gone() {
                    // Return Ok if the dataset's agent is gone, no delete call
                    // is required.
                    Ok(())
                } else {
                    error!(
                        log,
                        "region_delete_running_snapshot saw {:?}",
                        e;
                        "dataset_id" => %dataset_id,
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    Err(into_external_error(e))
                }
            }
        }
    }

    /// Send a snapshot deletion request
    async fn request_crucible_snapshot_delete(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let result = ProgenitorOperationRetry::new(
            || async {
                client
                    .region_delete_snapshot(
                        &RegionId(region_id.to_string()),
                        &snapshot_id.to_string(),
                    )
                    .await
            },
            || async { self.crucible_agent_gone_check(dataset_id).await },
        )
        .run(log)
        .await;

        match result {
            Ok(_) => Ok(()),

            Err(e) => {
                if e.is_gone() {
                    // Return Ok if the dataset's agent is gone, no delete call
                    // is required.
                    Ok(())
                } else {
                    error!(
                        log,
                        "region_delete_snapshot saw {:?}",
                        e;
                        "dataset_id" => %dataset_id,
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    Err(into_external_error(e))
                }
            }
        }
    }

    /// Call out to a Crucible agent to delete a region
    async fn delete_crucible_region(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
    ) -> Result<(), Error> {
        // If the region never existed, then a `GET` will return 404, and so
        // will a `DELETE`. Catch this case, and return Ok if the region never
        // existed.  This can occur if an `ensure_all_datasets_and_regions`
        // partially fails.

        match self.maybe_get_crucible_region(log, dataset, region_id).await {
            Ok(Some(_)) => {
                // region found, proceed with deleting
            }

            Ok(None) => {
                // region never exited, return Ok
                return Ok(());
            }

            // Return Ok if the dataset's agent is gone, no delete call
            // is required.
            Err(Error::Gone) => {
                warn!(
                    log,
                    "dataset is gone";
                    "dataset_id" => %dataset.id(),
                );

                return Ok(());
            }

            Err(e) => return Err(e),
        }

        // Past here, the region exists (or existed at some point): ensure it is
        // deleted. Request the deletion (which is idempotent), then wait for
        // the appropriate state change.

        self.request_crucible_region_delete(log, dataset, region_id).await?;

        // Wait until the region is deleted

        backoff::retry_notify(
            backoff::retry_policy_internal_service_aggressive(),
            || async {
                let region = match self
                    .maybe_get_crucible_region(log, dataset, region_id)
                    .await
                {
                    Ok(None) => Err(BackoffError::Permanent(
                        WaitError::Permanent(Error::internal_error(&format!(
                            "dataset {} region {region_id} is missing now!",
                            dataset.id(),
                        ))),
                    )),

                    Ok(Some(v)) => Ok(v),

                    // Return Ok if the dataset's agent is gone, no
                    // delete call is required.
                    Err(Error::Gone) => {
                        warn!(
                            log,
                            "dataset is gone";
                            "dataset_id" => %dataset.id(),
                        );

                        return Ok(());
                    }

                    Err(e) => {
                        Err(BackoffError::Permanent(WaitError::Permanent(e)))
                    }
                }?;

                match region.state {
                    RegionState::Tombstoned => Err(BackoffError::transient(
                        WaitError::Transient(anyhow!("region not deleted yet")),
                    )),

                    RegionState::Destroyed => {
                        info!(
                            log,
                            "region deleted";
                            "region_id" => %region_id,
                        );

                        Ok(())
                    }

                    _ => Err(BackoffError::transient(WaitError::Transient(
                        anyhow!("region unexpected state {:?}", region.state),
                    ))),
                }
            },
            |e: WaitError, delay| {
                info!(
                    log,
                    "{:?}, trying again in {:?}",
                    e,
                    delay;
                    "region_id" => %region_id,
                );
            },
        )
        .await
        .map_err(|e| match e {
            WaitError::Transient(e) => {
                // The backoff crate can be configured with a maximum elapsed time
                // before giving up, which means that Transient could be returned
                // here. Our current policies do **not** set this though.
                Error::internal_error(&e.to_string())
            }

            WaitError::Permanent(e) => e,
        })
    }

    async fn delete_crucible_running_snapshot_impl(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // request running snapshot deletion

        self.request_crucible_running_snapshot_delete(
            log,
            dataset,
            region_id,
            snapshot_id,
        )
        .await?;

        // `region_delete_running_snapshot` is only a request: wait until
        // running snapshot is deleted
        backoff::retry_notify(
            backoff::retry_policy_internal_service_aggressive(),
            || async {
                let response = match self.get_crucible_region_snapshots(
                    log,
                    dataset,
                    region_id,
                )
                .await {
                    Ok(v) => Ok(v),

                    // Return Ok if the dataset's agent is gone, no
                    // delete call is required.
                    Err(Error::Gone) => {
                        warn!(
                            log,
                            "dataset is gone";
                            "dataset_id" => %dataset.id(),
                        );

                        return Ok(());
                    }

                    Err(e) => Err(BackoffError::Permanent(WaitError::Permanent(e))),
                }?;

                match response.running_snapshots.get(&snapshot_id.to_string()) {
                    Some(running_snapshot) => {
                        info!(
                            log,
                            "running_snapshot is Some, state is {}",
                            running_snapshot.state.to_string();
                            "region_id" => %region_id,
                            "snapshot_id" => %snapshot_id,
                        );

                        match running_snapshot.state {
                            RegionState::Tombstoned => {
                                Err(BackoffError::transient(
                                    WaitError::Transient(anyhow!(
                                        "running_snapshot tombstoned, not deleted yet",
                                    )
                                )))
                            }

                            RegionState::Destroyed => {
                                info!(
                                    log,
                                    "running_snapshot deleted",
                                );

                                Ok(())
                            }

                            _ => {
                                Err(BackoffError::transient(
                                    WaitError::Transient(anyhow!(
                                        "running_snapshot unexpected state",
                                    )
                                )))
                            }
                        }
                    }

                    None => {
                        // deleted?
                        info!(
                            log,
                            "running_snapshot is None";
                            "region_id" => %region_id,
                            "snapshot_id" => %snapshot_id,
                        );

                        // break here - it's possible that the running snapshot
                        // record was GCed, and it won't come back.
                        Ok(())
                    }
                }
            },
            |e: WaitError, delay| {
                info!(
                    log,
                    "{:?}, trying again in {:?}",
                    e,
                    delay;
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );
            }
        )
        .await
        .map_err(|e| match e {
            WaitError::Transient(e) => {
                // The backoff crate can be configured with a maximum elapsed time
                // before giving up, which means that Transient could be returned
                // here. Our current policies do **not** set this though.
                Error::internal_error(&e.to_string())
            }

            WaitError::Permanent(e) => {
                e
            }
        })
    }

    pub async fn delete_crucible_snapshot(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.delete_crucible_snapshot_impl(log, dataset, region_id, snapshot_id)
            .await
    }

    async fn delete_crucible_snapshot_impl(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // Unlike other Crucible agent endpoints, this one is synchronous in that it
        // is not only a request to the Crucible agent: `zfs destroy` is performed
        // right away. However this is still a request to illumos that may not take
        // effect right away. Wait until the snapshot no longer appears in the list
        // of region snapshots, meaning it was not returned from `zfs list`.

        let dataset_id = dataset.id();

        info!(
            log,
            "requesting region snapshot delete";
            "dataset_id" => %dataset_id,
            "region_id" => %region_id,
            "snapshot_id" => %snapshot_id,
        );

        self.request_crucible_snapshot_delete(
            log,
            dataset,
            region_id,
            snapshot_id,
        )
        .await?;

        backoff::retry_notify(
            backoff::retry_policy_internal_service_aggressive(),
            || async {
                let response = match self
                    .get_crucible_region_snapshots(log, dataset, region_id)
                    .await
                {
                    Ok(v) => Ok(v),

                    // Return Ok if the dataset's agent is gone, no
                    // delete call is required.
                    Err(Error::Gone) => {
                        warn!(
                            log,
                            "dataset is gone";
                            "dataset_id" => %dataset.id(),
                        );

                        return Ok(());
                    }

                    Err(e) => {
                        Err(BackoffError::Permanent(WaitError::Permanent(e)))
                    }
                }?;

                if response
                    .snapshots
                    .iter()
                    .any(|x| x.name == snapshot_id.to_string())
                {
                    info!(
                        log,
                        "snapshot still exists, waiting";
                        "dataset_id" => %dataset_id,
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    Err(BackoffError::transient(WaitError::Transient(anyhow!(
                        "snapshot not deleted yet",
                    ))))
                } else {
                    info!(
                        log,
                        "snapshot deleted";
                        "dataset_id" => %dataset_id,
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    Ok(())
                }
            },
            |e: WaitError, delay| {
                info!(
                    log,
                    "{:?}, trying again in {:?}",
                    e,
                    delay;
                    "dataset_id" => %dataset_id,
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );
            },
        )
        .await
        .map_err(|e| match e {
            WaitError::Transient(e) => {
                // The backoff crate can be configured with a maximum elapsed time
                // before giving up, which means that Transient could be returned
                // here. Our current policies do **not** set this though.
                Error::internal_error(&e.to_string())
            }

            WaitError::Permanent(e) => e,
        })
    }

    // PUBLIC API

    pub async fn ensure_all_datasets_and_regions(
        &self,
        log: &Logger,
        datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
    ) -> Result<Vec<(db::model::Dataset, Region)>, Error> {
        let request_count = datasets_and_regions.len();
        if request_count == 0 {
            return Ok(vec![]);
        }

        // Allocate regions, and additionally return the dataset that the region was
        // allocated in.
        let datasets_and_regions: Vec<(db::model::Dataset, Region)> =
            futures::stream::iter(datasets_and_regions)
                .map(|(dataset, region)| async move {
                    match self
                        .ensure_region_in_dataset(log, &dataset, &region)
                        .await
                    {
                        Ok(result) => Ok((dataset, result)),
                        Err(e) => Err(e),
                    }
                })
                // Execute the allocation requests concurrently.
                .buffer_unordered(std::cmp::min(
                    request_count,
                    MAX_CONCURRENT_REGION_REQUESTS,
                ))
                .collect::<Vec<Result<(db::model::Dataset, Region), Error>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<(db::model::Dataset, Region)>, Error>>(
                )?;

        // Assert each region has the same block size, otherwise Volume creation
        // will fail.
        let all_region_have_same_block_size = datasets_and_regions
            .windows(2)
            .all(|w| w[0].1.block_size == w[1].1.block_size);

        if !all_region_have_same_block_size {
            return Err(Error::internal_error(
                "volume creation will fail due to block size mismatch",
            ));
        }

        Ok(datasets_and_regions)
    }

    /// Given a list of datasets and regions, send DELETE calls to the datasets
    /// corresponding Crucible Agent for each region.
    pub async fn delete_crucible_regions(
        &self,
        log: &Logger,
        datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
    ) -> Result<(), Error> {
        let request_count = datasets_and_regions.len();
        if request_count == 0 {
            return Ok(());
        }

        futures::stream::iter(datasets_and_regions)
            .map(|(dataset, region)| async move {
                self.delete_crucible_region(log, &dataset, region.id()).await
            })
            // Execute the requests concurrently.
            .buffer_unordered(std::cmp::min(
                request_count,
                MAX_CONCURRENT_REGION_REQUESTS,
            ))
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    /// Ensure that a Crucible "running snapshot" is deleted.
    pub async fn delete_crucible_running_snapshot(
        &self,
        log: &Logger,
        dataset: &db::model::Dataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.delete_crucible_running_snapshot_impl(
            log,
            dataset,
            region_id,
            snapshot_id,
        )
        .await
    }

    /// Given a list of datasets and region snapshots, send DELETE calls to the
    /// datasets corresponding Crucible Agent for each running read-only
    /// downstairs corresponding to the snapshot.
    pub async fn delete_crucible_running_snapshots(
        &self,
        log: &Logger,
        datasets_and_snapshots: Vec<(
            db::model::Dataset,
            db::model::RegionSnapshot,
        )>,
    ) -> Result<(), Error> {
        let request_count = datasets_and_snapshots.len();
        if request_count == 0 {
            return Ok(());
        }

        futures::stream::iter(datasets_and_snapshots)
            .map(|(dataset, region_snapshot)| async move {
                self.delete_crucible_running_snapshot_impl(
                    &log,
                    &dataset,
                    region_snapshot.region_id,
                    region_snapshot.snapshot_id,
                )
                .await
            })
            // Execute the requests concurrently.
            .buffer_unordered(std::cmp::min(
                request_count,
                MAX_CONCURRENT_REGION_REQUESTS,
            ))
            .collect::<Vec<Result<(), Error>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    /// Given a list of datasets and region snapshots, send DELETE calls to the
    /// dataset's corresponding Crucible Agent for each snapshot.
    pub async fn delete_crucible_snapshots(
        &self,
        log: &Logger,
        datasets_and_snapshots: Vec<(
            db::model::Dataset,
            db::model::RegionSnapshot,
        )>,
    ) -> Result<(), Error> {
        let request_count = datasets_and_snapshots.len();
        if request_count == 0 {
            return Ok(());
        }

        futures::stream::iter(datasets_and_snapshots)
            .map(|(dataset, region_snapshot)| async move {
                self.delete_crucible_snapshot_impl(
                    &log,
                    &dataset,
                    region_snapshot.region_id,
                    region_snapshot.snapshot_id,
                )
                .await
            })
            // Execute the requests concurrently.
            .buffer_unordered(std::cmp::min(
                request_count,
                MAX_CONCURRENT_REGION_REQUESTS,
            ))
            .collect::<Vec<Result<(), Error>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}
