// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with the Crucible agent in saga actions

use super::*;

use crate::db;
use crate::db::identity::Asset;
use anyhow::anyhow;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use slog::Logger;

// Arbitrary limit on concurrency, for operations issued on multiple regions
// within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;

/// Call out to Crucible agent and perform region creation.
pub async fn ensure_region_in_dataset(
    log: &Logger,
    dataset: &db::model::Dataset,
    region: &db::model::Region,
) -> Result<crucible_agent_client::types::Region, Error> {
    let url = format!("http://{}", dataset.address());
    let client = CrucibleAgentClient::new(&url);

    let region_request = CreateRegion {
        block_size: region.block_size().to_bytes(),
        extent_count: region.extent_count().try_into().unwrap(),
        extent_size: region.blocks_per_extent().try_into().unwrap(),
        // TODO: Can we avoid casting from UUID to string?
        // NOTE: This'll require updating the crucible agent client.
        id: RegionId(region.id().to_string()),
        encrypted: region.encrypted(),
        cert_pem: None,
        key_pem: None,
        root_pem: None,
    };

    let create_region = || async {
        let region = client
            .region_create(&region_request)
            .await
            .map_err(|e| BackoffError::Permanent(e.into()))?;
        match region.state {
            RegionState::Requested => Err(BackoffError::transient(anyhow!(
                "Region creation in progress"
            ))),
            RegionState::Created => Ok(region),
            _ => Err(BackoffError::Permanent(anyhow!(
                "Failed to create region, unexpected state: {:?}",
                region.state
            ))),
        }
    };

    let log_create_failure = |_, delay| {
        warn!(
            log,
            "Region requested, not yet created. Retrying in {:?}", delay
        );
    };

    let region = backoff::retry_notify(
        backoff::internal_service_policy_long(),
        create_region,
        log_create_failure,
    )
    .await
    .map_err(|e| Error::internal_error(&e.to_string()))?;

    Ok(region.into_inner())
}

pub async fn ensure_all_datasets_and_regions(
    log: &Logger,
    datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
) -> Result<
    Vec<(db::model::Dataset, crucible_agent_client::types::Region)>,
    ActionError,
> {
    let request_count = datasets_and_regions.len();

    // Allocate regions, and additionally return the dataset that the region was
    // allocated in.
    let datasets_and_regions: Vec<(
        db::model::Dataset,
        crucible_agent_client::types::Region,
    )> = futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            match ensure_region_in_dataset(log, &dataset, &region).await {
                Ok(result) => Ok((dataset, result)),
                Err(e) => Err(e),
            }
        })
        // Execute the allocation requests concurrently.
        .buffer_unordered(std::cmp::min(
            request_count,
            MAX_CONCURRENT_REGION_REQUESTS,
        ))
        .collect::<Vec<
            Result<
                (db::model::Dataset, crucible_agent_client::types::Region),
                Error,
            >,
        >>()
        .await
        .into_iter()
        .collect::<Result<
            Vec<(db::model::Dataset, crucible_agent_client::types::Region)>,
            Error,
        >>()
        .map_err(ActionError::action_failed)?;

    // Assert each region has the same block size, otherwise Volume creation
    // will fail.
    let all_region_have_same_block_size = datasets_and_regions
        .windows(2)
        .all(|w| w[0].1.block_size == w[1].1.block_size);

    if !all_region_have_same_block_size {
        return Err(ActionError::action_failed(Error::internal_error(
            "volume creation will fail due to block size mismatch",
        )));
    }

    Ok(datasets_and_regions)
}

// Given a list of datasets and regions, send DELETE calls to the datasets
// corresponding Crucible Agent for each region.
pub(super) async fn delete_crucible_regions(
    datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
) -> Result<(), Error> {
    let request_count = datasets_and_regions.len();
    if request_count == 0 {
        return Ok(());
    }

    futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            let url = format!("http://{}", dataset.address());
            let client = CrucibleAgentClient::new(&url);
            let id = RegionId(region.id().to_string());
            client.region_delete(&id).await.map_err(|e| match e {
                crucible_agent_client::Error::ErrorResponse(rv) => {
                    match rv.status() {
                        http::StatusCode::SERVICE_UNAVAILABLE => {
                            Error::unavail(&rv.message)
                        }
                        status if status.is_client_error() => {
                            Error::invalid_request(&rv.message)
                        }
                        _ => Error::internal_error(&rv.message),
                    }
                }
                _ => Error::internal_error(
                    "unexpected failure during `delete_crucible_regions`",
                ),
            })
        })
        // Execute the allocation requests concurrently.
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

// Given a list of datasets and region snapshots, send DELETE calls to the
// datasets corresponding Crucible Agent for each running read-only downstairs
// and snapshot.
pub(super) async fn delete_crucible_snapshots(
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
            let url = format!("http://{}", dataset.address());
            let client = CrucibleAgentClient::new(&url);

            // delete running snapshot
            client
                .region_delete_running_snapshot(
                    &RegionId(region_snapshot.region_id.to_string()),
                    &region_snapshot.snapshot_id.to_string(),
                )
                .await
                .map_err(|e| match e {
                    crucible_agent_client::Error::ErrorResponse(rv) => {
                        match rv.status() {
                            http::StatusCode::SERVICE_UNAVAILABLE => {
                                Error::unavail(&rv.message)
                            }
                            status if status.is_client_error() => {
                                Error::invalid_request(&rv.message)
                            }
                            _ => Error::internal_error(&rv.message),
                        }
                    }
                    _ => Error::internal_error(
                        "unexpected failure during `region_delete_running_snapshot`",
                    ),
                })?;

            // delete snapshot
            client
                .region_delete_snapshot(
                    &RegionId(region_snapshot.region_id.to_string()),
                    &region_snapshot.snapshot_id.to_string(),
                )
                .await
                .map_err(|e| match e {
                    crucible_agent_client::Error::ErrorResponse(rv) => {
                        match rv.status() {
                            http::StatusCode::SERVICE_UNAVAILABLE => {
                                Error::unavail(&rv.message)
                            }
                            status if status.is_client_error() => {
                                Error::invalid_request(&rv.message)
                            }
                            _ => Error::internal_error(&rv.message),
                        }
                    }
                    _ => Error::internal_error(
                        "unexpected failure during `region_delete_snapshot`",
                    ),
                })?;

            Ok(())
        })
        // Execute the allocation requests concurrently.
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
