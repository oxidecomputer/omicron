// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with Crucible agents
//!
//! A note: there are multiple places in this file that have two layers of
//! retries. This is because the majority of the requests to the Crucible agent
//! are requests for something to happen in the background, and it's the
//! client's responsibility to poll for a state change. One example of this is
//! for creating regions: the inner loop retries the POST until it succeeds, and
//! the outer loop checks the state returned.

use super::*;

use anyhow::anyhow;
use crucible_agent_client::Client as CrucibleAgentClient;
use crucible_agent_client::types::CreateRegion;
use crucible_agent_client::types::GetSnapshotResponse;
use crucible_agent_client::types::Region;
use crucible_agent_client::types::RegionId;
use crucible_agent_client::types::RunningSnapshot;
use crucible_agent_client::types::Snapshot;
use crucible_agent_client::types::State as RegionState;
use futures::StreamExt;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use omicron_common::api::external::Error;
use omicron_common::backoff::backon_retry_policy_internal_service;
use omicron_common::backoff::{self, BackoffError};
use omicron_uuid_kinds::DatasetUuid;
use progenitor_extras::retry::GoneCheckResult;
use progenitor_extras::retry::IndefiniteRetryOperationWhileError;
use progenitor_extras::retry::IndefiniteRetryOperationWhileErrorKind;
use progenitor_extras::retry::retry_operation_while_indefinitely;
use slog::Logger;
use std::collections::VecDeque;

// Arbitrary limit on concurrency, for operations issued on multiple regions
// within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;

/// Provides a way for (with BackoffError) Permanent errors to have a different
/// error type than
/// Transient errors.
#[derive(Debug, thiserror::Error)]
enum WaitError {
    #[error("Transient error: {0}")]
    Transient(#[from] anyhow::Error),

    #[error("Permanent error: {0}")]
    Permanent(#[from] Error),
}

/// Convert an error returned from a retry loop into an external Error
fn into_external_error(
    e: IndefiniteRetryOperationWhileError<
        crucible_agent_client::types::Error,
        Error,
    >,
) -> Error {
    match e.kind {
        IndefiniteRetryOperationWhileErrorKind::Gone => Error::Gone,

        IndefiniteRetryOperationWhileErrorKind::GoneCheckError(e) => {
            Error::internal_error(&format!(
                "insufficient permission for crucible_agent_gone_check: {e}"
            ))
        }

        IndefiniteRetryOperationWhileErrorKind::OperationError(e) => match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                if rv.status().is_client_error() {
                    Error::invalid_request(&rv.message)
                } else {
                    Error::internal_error(&rv.message)
                }
            }

            _ => Error::internal_error(&format!("unexpected failure: {e}")),
        },
    }
}

impl super::Nexus {
    fn crucible_agent_client_for_dataset(
        &self,
        dataset: &db::model::CrucibleDataset,
    ) -> CrucibleAgentClient {
        CrucibleAgentClient::new_with_client(
            &format!("http://{}", dataset.address()),
            self.reqwest_client.clone(),
        )
    }

    /// Return if the Crucible agent is expected to be there and answer Nexus:
    /// if it's [`Gone`], the caller should bail out of the retry loop.
    async fn crucible_agent_gone_check(
        &self,
        dataset_id: DatasetUuid,
    ) -> Result<GoneCheckResult, Error> {
        let on_in_service_physical_disk = self
            .datastore()
            .crucible_dataset_physical_disk_in_service(dataset_id)
            .await?;

        Ok(match on_in_service_physical_disk {
            true => GoneCheckResult::StillAvailable,
            false => GoneCheckResult::Gone,
        })
    }

    /// Return a region's associated address
    pub async fn region_addr(
        &self,
        log: &Logger,
        region_id: Uuid,
    ) -> Result<SocketAddrV6, Error> {
        // If a region port was previously recorded, return the address right
        // away

        if let Some(addr) = self.datastore().region_addr(region_id).await? {
            return Ok(addr);
        }

        // Otherwise, ask the appropriate Crucible agent

        let dataset = {
            let region = self.datastore().get_region(region_id).await?;
            self.datastore().crucible_dataset_get(region.dataset_id()).await?
        };

        let Some(returned_region) =
            self.maybe_get_crucible_region(log, &dataset, region_id).await?
        else {
            // The Crucible agent didn't think the region exists? It could have
            // been concurrently deleted, or otherwise garbage collected.
            warn!(log, "no region for id {region_id} from Crucible Agent");
            return Err(Error::Gone);
        };

        // Record the returned port
        self.datastore()
            .region_set_port(region_id, returned_region.port_number)
            .await?;

        // Return the address with the port that was just recorded - guard again
        // against the case where the region record could have been concurrently
        // deleted
        match self.datastore().region_addr(region_id).await {
            Ok(Some(addr)) => Ok(addr),

            Ok(None) => {
                warn!(log, "region {region_id} deleted");
                Err(Error::Gone)
            }

            Err(e) => Err(e),
        }
    }

    /// Call out to Crucible agent and perform region creation. Optionally,
    /// supply a read-only source's repair address to invoke a clone.
    pub async fn ensure_region_in_dataset(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region: &db::model::Region,
        source: Option<String>,
    ) -> Result<Region, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let region_id = region.id();
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
            source,
        };

        let create_region = || async {
            let create_region_operation =
                || async { client.region_create(&region_request).await };

            let gone_check =
                || async { self.crucible_agent_gone_check(dataset_id).await };

            let region = match retry_operation_while_indefinitely(
                backon_retry_policy_internal_service(),
                create_region_operation,
                gone_check,
                |notification| {
                    slog::warn!(
                        log,
                        "failed to create region {region_id}, retrying in {:?}",
                        notification.delay;
                        InlineErrorChain::new(&notification.error),
                    );
                },
            )
            .await
            {
                Ok(v) => Ok(v.into_inner()),

                Err(e) => {
                    error!(
                        log,
                        "region_create saw {:?}",
                        e;
                        "region_id" => %region_id,
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
                "dataset" => %dataset_id,
                "region" => %region_id,
            );
        };

        let returned_region = backoff::retry_notify(
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

        // Record the returned port
        self.datastore()
            .region_set_port(region.id(), returned_region.port_number)
            .await?;

        Ok(returned_region)
    }

    /// Ensure that a running snapshot for a region snapshot exists and is
    /// running.
    async fn ensure_crucible_running_snapshot_impl(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(Region, Snapshot, RunningSnapshot), Error> {
        // Validate with the Crucible agent that both the underlying region and
        // snapshot exist

        info!(
            log,
            "contacting crucible agent to confirm region exists";
            "dataset" => ?dataset.id(),
            "region" => ?region_id,
        );

        let region = match self
            .maybe_get_crucible_region(log, dataset, region_id)
            .await?
        {
            Some(region) => {
                info!(
                    log,
                    "confirmed the region exists";
                    "dataset" => ?dataset.id(),
                    "region" => ?region,
                );

                region
            }

            None => {
                error!(
                    log,
                    "region does not exist!";
                    "dataset" => ?dataset.id(),
                    "region" => ?region_id,
                );

                return Err(Error::invalid_request(format!(
                    "dataset {:?} region {:?} does not exist!",
                    dataset.id(),
                    region_id,
                )));
            }
        };

        info!(
            log,
            "contacting crucible agent to confirm snapshot exists";
            "dataset" => ?dataset.id(),
            "region" => ?region_id,
            "snapshot" => ?snapshot_id,
        );

        let snapshot = match self
            .maybe_get_crucible_snapshot(log, dataset, region_id, snapshot_id)
            .await?
        {
            Some(snapshot) => {
                info!(
                    log,
                    "confirmed the snapshot exists";
                    "dataset" => ?dataset.id(),
                    "region" => ?region.id,
                    "snapshot" => ?snapshot,
                );

                snapshot
            }

            None => {
                // snapshot does not exist!
                error!(
                    log,
                    "snapshot does not exist!";
                    "dataset" => ?dataset.id(),
                    "region" => ?region_id,
                    "snapshot" => ?snapshot_id,
                );

                return Err(Error::invalid_request(format!(
                    "dataset {:?} region {:?} snapshot {:?} does not exist!",
                    dataset.id(),
                    region_id,
                    snapshot_id,
                )));
            }
        };

        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        // Request the running snapshot start, polling until the state
        // transitions from Requested to Created

        let create_running_snapshot = || async {
            let run_snapshot_operation = || async {
                client
                    .region_run_snapshot(
                        &RegionId(region_id.to_string()),
                        &snapshot_id.to_string(),
                    )
                    .await
            };

            let gone_check =
                || async { self.crucible_agent_gone_check(dataset_id).await };

            let running_snapshot = match retry_operation_while_indefinitely(
                backon_retry_policy_internal_service(),
                run_snapshot_operation,
                gone_check,
                |notification| {
                    slog::warn!(
                        log,
                        "failed to run region {region_id} snapshot \
                        {snapshot_id}, retrying in {:?}",
                        notification.delay;
                        InlineErrorChain::new(&notification.error),
                    );
                },
            )
            .await
            {
                Ok(v) => Ok(v.into_inner()),

                Err(e) => {
                    error!(
                        log,
                        "region_run_snapshot saw {:?}",
                        e;
                        "dataset" => %dataset_id,
                        "region" => %region_id,
                        "snapshot" => %snapshot_id,
                    );

                    // Return an error if Nexus is unable to create the
                    // requested running snapshot
                    Err(BackoffError::Permanent(WaitError::Permanent(
                        into_external_error(e),
                    )))
                }
            }?;

            match running_snapshot.state {
                RegionState::Requested => {
                    Err(BackoffError::transient(WaitError::Transient(anyhow!(
                        "Running snapshot creation in progress"
                    ))))
                }

                RegionState::Created => Ok(running_snapshot),

                _ => Err(BackoffError::Permanent(WaitError::Permanent(
                    Error::internal_error(&format!(
                        "Failed to create running snapshot, unexpected \
                        state: {:?}",
                        region.state
                    )),
                ))),
            }
        };

        let log_create_failure = |_, delay| {
            warn!(
                log,
                "Running snapshot requested, not yet created. Retrying in {:?}",
                delay;
                "dataset" => %dataset.id(),
                "region" => %region_id,
                "snapshot" => %snapshot_id,
            );
        };

        let running_snapshot = backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            create_running_snapshot,
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

        Ok((region, snapshot, running_snapshot))
    }

    /// Returns a Ok(Some(Region)) if a region with id {region_id} exists,
    /// Ok(None) if it does not (a 404 was seen), and Err otherwise.
    async fn maybe_get_crucible_region(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
    ) -> Result<Option<Region>, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let region_get_operation = || async {
            client.region_get(&RegionId(region_id.to_string())).await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            region_get_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to get region {region_id}, retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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

    /// Returns a Ok(Some(Snapshot)) if a snapshot exists, Ok(None) if it does
    /// not (a 404 was seen), and Err otherwise.
    async fn maybe_get_crucible_snapshot(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Option<Snapshot>, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let snapshot_get_operation = || async {
            client
                .region_get_snapshot(
                    &RegionId(region_id.to_string()),
                    &snapshot_id.to_string(),
                )
                .await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            snapshot_get_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to get region {region_id} snapshot {snapshot_id}, \
                    retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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
                        "region_get_snapshot saw {:?}",
                        e;
                        "dataset_id" => %dataset_id,
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    // Return an error if Nexus is unable to query the dataset's
                    // agent for the requested snapshot
                    Err(into_external_error(e))
                }
            }
        }
    }

    async fn get_crucible_region_snapshots(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
    ) -> Result<GetSnapshotResponse, Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let region_get_snapshots_operation = || async {
            client.region_get_snapshots(&RegionId(region_id.to_string())).await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            region_get_snapshots_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to get snapshots for region {region_id}, \
                    retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let region_delete_operation = || async {
            client.region_delete(&RegionId(region_id.to_string())).await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            region_delete_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to delete region {region_id}, retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let delete_running_snapshot_operation = || async {
            client
                .region_delete_running_snapshot(
                    &RegionId(region_id.to_string()),
                    &snapshot_id.to_string(),
                )
                .await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            delete_running_snapshot_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to delete region {region_id} running snapshot \
                    {snapshot_id}, retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.crucible_agent_client_for_dataset(dataset);
        let dataset_id = dataset.id();

        let delete_snapshot_operation = || async {
            client
                .region_delete_snapshot(
                    &RegionId(region_id.to_string()),
                    &snapshot_id.to_string(),
                )
                .await
        };

        let gone_check =
            || async { self.crucible_agent_gone_check(dataset_id).await };

        let result = retry_operation_while_indefinitely(
            backon_retry_policy_internal_service(),
            delete_snapshot_operation,
            gone_check,
            |notification| {
                slog::warn!(
                    log,
                    "failed to delete region {region_id} snapshot \
                    {snapshot_id}, retrying in {:?}",
                    notification.delay;
                    InlineErrorChain::new(&notification.error),
                );
            },
        )
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
        dataset: &db::model::CrucibleDataset,
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

                    RegionState::Failed => {
                        // If the delete failed, Nexus can re-request that the
                        // region be deleted, and it will move back to
                        // Tombstoned.

                        match self
                            .request_crucible_region_delete(
                                log, dataset, region_id,
                            )
                            .await
                        {
                            Ok(()) => {
                                // Either the request succeeded, or the
                                // dataset's agent is gone.
                                Err(BackoffError::transient(
                                    WaitError::Transient(anyhow!(
                                        "region is failed, re-requested delete"
                                    )),
                                ))
                            }

                            Err(e) => Err(BackoffError::transient(
                                WaitError::Transient(anyhow!(
                                    "region is failed, error re-requesting \
                                        delete: {e}"
                                )),
                            )),
                        }
                    }

                    RegionState::Requested | RegionState::Created => {
                        // It's unexpected that the region be here after a
                        // deletion request. We successfully requested the
                        // region deletion before entering this retry loop, and
                        // the Crucible agent should prevent the state
                        // transition from Tombstoned to either of these states.

                        Err(BackoffError::Permanent(WaitError::Permanent(
                            Error::internal_error(&format!(
                                "region is {:?} after successful deletion \
                                request!",
                                region.state,
                            )),
                        )))
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
                );
            },
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
        })
    }

    async fn delete_crucible_running_snapshot_impl(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
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
                                        "running_snapshot tombstoned, not \
                                        deleted yet",
                                    )),
                                ))
                            }

                            RegionState::Destroyed => {
                                info!(log, "running_snapshot deleted",);

                                Ok(())
                            }

                            _ => Err(BackoffError::transient(
                                WaitError::Transient(anyhow!(
                                    "running_snapshot unexpected state",
                                )),
                            )),
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
            },
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
        })
    }

    pub async fn delete_crucible_snapshot(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.delete_crucible_snapshot_impl(log, dataset, region_id, snapshot_id)
            .await
    }

    async fn delete_crucible_snapshot_impl(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // Unlike other Crucible agent endpoints, this one is synchronous in
        // that it is not only a request to the Crucible agent: `zfs destroy` is
        // performed right away. However this is still a request to illumos that
        // may not take effect right away. Wait until the snapshot no longer
        // appears in the list of region snapshots, meaning it was not returned
        // from `zfs list`.

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
                // The backoff crate can be configured with a maximum elapsed
                // time before giving up, which means that Transient could be
                // returned here. Our current policies do **not** set this
                // though.
                Error::internal_error(&e.to_string())
            }

            WaitError::Permanent(e) => e,
        })
    }

    // PUBLIC API

    pub async fn ensure_all_datasets_and_regions(
        &self,
        log: &Logger,
        datasets_and_regions: Vec<(
            db::model::CrucibleDataset,
            db::model::Region,
        )>,
    ) -> Result<Vec<(db::model::CrucibleDataset, Region)>, Error> {
        let request_count = datasets_and_regions.len();
        if request_count == 0 {
            return Ok(vec![]);
        }

        // Allocate regions, and additionally return the dataset that the region
        // was allocated in.
        let datasets_and_regions: Vec<(db::model::CrucibleDataset, Region)> =
            futures::stream::iter(datasets_and_regions)
                .map(|(dataset, region)| async move {
                    match self
                        .ensure_region_in_dataset(log, &dataset, &region, None)
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
                .collect::<Vec<Result<(db::model::CrucibleDataset, Region), Error>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<(db::model::CrucibleDataset, Region)>, Error>>(
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
        datasets_and_regions: Vec<(
            db::model::CrucibleDataset,
            db::model::Region,
        )>,
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
        dataset: &db::model::CrucibleDataset,
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
            db::model::CrucibleDataset,
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
            db::model::CrucibleDataset,
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

    /// Ensure that a Crucible "running snapshot" is created.
    pub async fn ensure_crucible_running_snapshot(
        &self,
        log: &Logger,
        dataset: &db::model::CrucibleDataset,
        region_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(Region, Snapshot, RunningSnapshot), Error> {
        self.ensure_crucible_running_snapshot_impl(
            log,
            dataset,
            region_id,
            snapshot_id,
        )
        .await
    }

    /// Given a list of datasets and region snapshots, send POST calls to the
    /// datasets corresponding Crucible Agent for each running read-only
    /// downstairs corresponding to the snapshot.
    pub async fn ensure_crucible_running_snapshots(
        &self,
        log: &Logger,
        datasets_and_snapshots: Vec<(
            db::model::CrucibleDataset,
            db::model::RegionSnapshot,
        )>,
    ) -> Result<Vec<(Region, Snapshot, RunningSnapshot)>, Error> {
        let request_count = datasets_and_snapshots.len();
        if request_count == 0 {
            return Ok(vec![]);
        }

        futures::stream::iter(datasets_and_snapshots)
            .map(|(dataset, region_snapshot)| async move {
                self.ensure_crucible_running_snapshot_impl(
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
            .collect::<Vec<Result<_, Error>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    }
}

pub enum VolumeHealth {
    Healthy,

    Degraded { reason: VolumeDegradedReason },
}

pub enum VolumeDegradedReason {
    /// The Volume is only partially active
    UpstairsNotActive,

    /// Not all three downstairs are present for one or more region sets.
    ReducedRedundancy,

    /// For one or more region sets, three downstairs are present but one or
    /// more is degraded.
    DownstairsDegraded,
}

impl std::fmt::Display for VolumeDegradedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeDegradedReason::UpstairsNotActive => {
                write!(f, "volume is only partially active")
            }

            VolumeDegradedReason::ReducedRedundancy => {
                write!(
                    f,
                    "part of the volume is operating at reduced redundancy",
                )
            }

            VolumeDegradedReason::DownstairsDegraded => {
                write!(f, "one or more downstairs is degraded")
            }
        }
    }
}

// Crucible can return a `VolumeInfo` that describes the state of the entire
// Volume in a tree structure. Both Propolis (from `propolis_client::types`) and
// the Crucible Pantry (from `crucible_pantry_client::types`) export this type
// from their import of the `crucible-client-types` crate, meaning two versions
// could exist that Nexus could read. Do the simplest thing: write two versions
// of the function that reads each type returns a `VolumeHealth`. These
// functions currently are the same, but in the future may temporarily look
// different if Propolis and the Crucible Pantry import different
// `crucible-client-types` versions. These types should eventually be derived
// from the same `crucible-client-types` version though as that is equivalent to
// both imports being up to date.

/// Given a [`propolis_client::types::VolumeInfo`], return if the Volume should
/// be considered healthy by Nexus.
pub fn propolis_client_volume_health(
    info: &propolis_client::types::VolumeInfo,
) -> VolumeHealth {
    use propolis_client::types::DownstairsInfoStatus;
    use propolis_client::types::UpstairsInfoStatus;
    use propolis_client::types::VolumeInfo;

    let mut parts: VecDeque<&VolumeInfo> = VecDeque::new();
    parts.push_back(info);

    while let Some(part) = parts.pop_front() {
        match part {
            VolumeInfo::Volume { sub_volumes, read_only_parent } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeInfo::Upstairs {
                state,
                block_size: _,
                upstairs_id: _,
                session_id: _,
                generation: _,
                read_only: _,
                encrypted: _,
                reconcile_in_progress,
                live_repair_in_progress,
                targets,
            } => {
                if *reconcile_in_progress || *live_repair_in_progress {
                    return VolumeHealth::Degraded {
                        reason: VolumeDegradedReason::DownstairsDegraded,
                    };
                }

                match state {
                    UpstairsInfoStatus::Initializing
                    | UpstairsInfoStatus::GoActive
                    | UpstairsInfoStatus::Deactivating
                    | UpstairsInfoStatus::Disabled => {
                        return VolumeHealth::Degraded {
                            reason: VolumeDegradedReason::UpstairsNotActive,
                        };
                    }

                    UpstairsInfoStatus::Active => {
                        // ok!
                    }
                }

                for target in targets {
                    match target.state {
                        DownstairsInfoStatus::Connecting { .. } => {
                            return VolumeHealth::Degraded {
                                reason: VolumeDegradedReason::ReducedRedundancy,
                            };
                        }

                        DownstairsInfoStatus::Active => {
                            // ok!
                        }

                        DownstairsInfoStatus::LiveRepair => {
                            return VolumeHealth::Degraded {
                                reason:
                                    VolumeDegradedReason::DownstairsDegraded,
                            };
                        }

                        DownstairsInfoStatus::Stopping => {
                            return VolumeHealth::Degraded {
                                reason: VolumeDegradedReason::ReducedRedundancy,
                            };
                        }
                    }
                }
            }
        }
    }

    VolumeHealth::Healthy
}

/// Given a [`crucible_pantry_client::types::VolumeInfo`], return if the Volume
/// should be considered healthy by Nexus.
pub fn crucible_pantry_client_volume_health(
    info: &crucible_pantry_client::types::VolumeInfo,
) -> VolumeHealth {
    use crucible_pantry_client::types::DownstairsInfoStatus;
    use crucible_pantry_client::types::UpstairsInfoStatus;
    use crucible_pantry_client::types::VolumeInfo;

    let mut parts: VecDeque<&VolumeInfo> = VecDeque::new();
    parts.push_back(info);

    while let Some(part) = parts.pop_front() {
        match part {
            VolumeInfo::Volume { sub_volumes, read_only_parent } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeInfo::Upstairs {
                state,
                block_size: _,
                upstairs_id: _,
                session_id: _,
                generation: _,
                read_only: _,
                encrypted: _,
                reconcile_in_progress,
                live_repair_in_progress,
                targets,
            } => {
                if *reconcile_in_progress || *live_repair_in_progress {
                    return VolumeHealth::Degraded {
                        reason: VolumeDegradedReason::DownstairsDegraded,
                    };
                }

                match state {
                    UpstairsInfoStatus::Initializing
                    | UpstairsInfoStatus::GoActive
                    | UpstairsInfoStatus::Deactivating
                    | UpstairsInfoStatus::Disabled => {
                        return VolumeHealth::Degraded {
                            reason: VolumeDegradedReason::UpstairsNotActive,
                        };
                    }

                    UpstairsInfoStatus::Active => {
                        // ok!
                    }
                }

                for target in targets {
                    match target.state {
                        DownstairsInfoStatus::Connecting { .. } => {
                            return VolumeHealth::Degraded {
                                reason: VolumeDegradedReason::ReducedRedundancy,
                            };
                        }

                        DownstairsInfoStatus::Active => {
                            // ok!
                        }

                        DownstairsInfoStatus::LiveRepair => {
                            return VolumeHealth::Degraded {
                                reason:
                                    VolumeDegradedReason::DownstairsDegraded,
                            };
                        }

                        DownstairsInfoStatus::Stopping => {
                            return VolumeHealth::Degraded {
                                reason: VolumeDegradedReason::ReducedRedundancy,
                            };
                        }
                    }
                }
            }
        }
    }

    VolumeHealth::Healthy
}
