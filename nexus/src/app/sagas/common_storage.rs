// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions common to interacting with the Crucible agent in saga actions

use super::*;

use crate::Nexus;
use anyhow::anyhow;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use internal_dns::ServiceName;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use omicron_common::retry_until_known_result;
use slog::Logger;
use std::net::SocketAddrV6;

// Arbitrary limit on concurrency, for operations issued on multiple regions
// within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;

/// Call out to Crucible agent and perform region creation.
pub(crate) async fn ensure_region_in_dataset(
    log: &Logger,
    dataset: &db::model::Dataset,
    region: &db::model::Region,
) -> Result<crucible_agent_client::types::Region, Error> {
    let url = format!("http://{}", dataset.address());
    let client = CrucibleAgentClient::new(&url);
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
            "Region requested, not yet created. Retrying in {:?}",
            delay;
            "region" => %region.id(),
        );
    };

    let region = backoff::retry_notify(
        backoff::retry_policy_internal_service(),
        create_region,
        log_create_failure,
    )
    .await
    .map_err(|e| Error::internal_error(&e.to_string()))?;

    Ok(region.into_inner())
}

pub(crate) async fn ensure_all_datasets_and_regions(
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

pub(super) async fn delete_crucible_region(
    log: &Logger,
    client: &CrucibleAgentClient,
    region_id: Uuid,
) -> Result<(), Error> {
    // If the region never existed, then a `GET` will return 404, and so will a
    // `DELETE`. Catch this case, and return Ok if the region never existed.
    // This can occur if an `ensure_all_datasets_and_regions` partially fails.
    let result = retry_until_known_result(log, || async {
        client.region_get(&RegionId(region_id.to_string())).await
    })
    .await;

    if let Err(e) = result {
        error!(
            log,
            "delete_crucible_region: region_get saw {:?}",
            e;
            "region_id" => %region_id,
        );
        match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                match rv.status() {
                    http::StatusCode::NOT_FOUND => {
                        // Bail out here!
                        return Ok(());
                    }

                    status if status.is_client_error() => {
                        return Err(Error::invalid_request(&rv.message));
                    }

                    _ => {
                        return Err(Error::internal_error(&rv.message));
                    }
                }
            }

            _ => {
                return Err(Error::internal_error(
                    "unexpected failure during `region_get`",
                ));
            }
        }
    }

    // Past here, the region exists: ensure it is deleted.

    retry_until_known_result(log, || async {
        client.region_delete(&RegionId(region_id.to_string())).await
    })
    .await
    .map_err(|e| {
        error!(
            log,
            "delete_crucible_region: region_delete saw {:?}",
            e;
            "region_id" => %region_id,
        );
        match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                match rv.status() {
                    status if status.is_client_error() => {
                        Error::invalid_request(&rv.message)
                    }
                    _ => Error::internal_error(&rv.message),
                }
            }
            _ => Error::internal_error(
                "unexpected failure during `region_delete`",
            ),
        }
    })?;

    #[derive(Debug, thiserror::Error)]
    enum WaitError {
        #[error("Transient error: {0}")]
        Transient(#[from] anyhow::Error),

        #[error("Permanent error: {0}")]
        Permanent(#[from] Error),
    }

    // `region_delete` is only a request: wait until the region is
    // deleted
    backoff::retry_notify(
        backoff::retry_policy_internal_service_aggressive(),
        || async {
            let region = retry_until_known_result(log, || async {
                client.region_get(&RegionId(region_id.to_string())).await
            })
            .await
            .map_err(|e| {
                error!(
                    log,
                    "delete_crucible_region: region_get saw {:?}",
                    e;
                    "region_id" => %region_id,
                );

                match e {
                    crucible_agent_client::Error::ErrorResponse(rv) => {
                        match rv.status() {
                            status if status.is_client_error() => {
                                BackoffError::Permanent(WaitError::Permanent(
                                    Error::invalid_request(&rv.message),
                                ))
                            }
                            _ => BackoffError::Permanent(WaitError::Permanent(
                                Error::internal_error(&rv.message),
                            )),
                        }
                    }
                    _ => BackoffError::Permanent(WaitError::Permanent(
                        Error::internal_error(
                            "unexpected failure during `region_get`",
                        ),
                    )),
                }
            })?;

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

// Given a list of datasets and regions, send DELETE calls to the datasets
// corresponding Crucible Agent for each region.
pub(super) async fn delete_crucible_regions(
    log: &Logger,
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

            delete_crucible_region(&log, &client, region.id()).await
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

pub(super) async fn delete_crucible_running_snapshot(
    log: &Logger,
    client: &CrucibleAgentClient,
    region_id: Uuid,
    snapshot_id: Uuid,
) -> Result<(), Error> {
    // delete running snapshot
    retry_until_known_result(log, || async {
        client
            .region_delete_running_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .await
    })
    .await
    .map_err(|e| {
        error!(
            log,
            "delete_crucible_running_snapshot: region_delete_running_snapshot saw {:?}",
            e;
            "region_id" => %region_id,
            "snapshot_id" => %snapshot_id,
        );
        match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                match rv.status() {
                    status if status.is_client_error() => {
                        Error::invalid_request(&rv.message)
                    }
                    _ => Error::internal_error(&rv.message),
                }
            }
            _ => Error::internal_error(
                "unexpected failure during `region_delete_running_snapshot`",
            ),
        }
    })?;

    #[derive(Debug, thiserror::Error)]
    enum WaitError {
        #[error("Transient error: {0}")]
        Transient(#[from] anyhow::Error),

        #[error("Permanent error: {0}")]
        Permanent(#[from] Error),
    }

    // `region_delete_running_snapshot` is only a request: wait until
    // running snapshot is deleted
    backoff::retry_notify(
        backoff::retry_policy_internal_service_aggressive(),
        || async {
            let snapshot = retry_until_known_result(log, || async {
                    client.region_get_snapshots(
                        &RegionId(region_id.to_string()),
                    ).await
                })
                .await
                .map_err(|e| {
                    error!(
                        log,
                        "delete_crucible_running_snapshot: region_get_snapshots saw {:?}",
                        e;
                        "region_id" => %region_id,
                        "snapshot_id" => %snapshot_id,
                    );

                    match e {
                        crucible_agent_client::Error::ErrorResponse(rv) => {
                            match rv.status() {
                                status if status.is_client_error() => {
                                    BackoffError::Permanent(
                                        WaitError::Permanent(
                                            Error::invalid_request(&rv.message)
                                        )
                                    )
                                }
                                _ => BackoffError::Permanent(
                                    WaitError::Permanent(
                                        Error::internal_error(&rv.message)
                                    )
                                )
                            }
                        }
                        _ => BackoffError::Permanent(
                            WaitError::Permanent(
                                Error::internal_error(
                                    "unexpected failure during `region_get_snapshots`",
                                )
                            )
                        )
                    }
                })?;

            match snapshot.running_snapshots.get(&snapshot_id.to_string()) {
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

pub(super) async fn delete_crucible_snapshot(
    log: &Logger,
    client: &CrucibleAgentClient,
    region_id: Uuid,
    snapshot_id: Uuid,
) -> Result<(), Error> {
    // Unlike other Crucible agent endpoints, this one is synchronous in that it
    // is not only a request to the Crucible agent: `zfs destroy` is performed
    // right away. However this is still a request to illumos that may not take
    // effect right away. Wait until the snapshot no longer appears in the list
    // of region snapshots, meaning it was not returned from `zfs list`.

    info!(log, "deleting region {region_id} snapshot {snapshot_id}");

    retry_until_known_result(log, || async {
        client
            .region_delete_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .await
    })
    .await
    .map_err(|e| {
        error!(
            log,
            "delete_crucible_snapshot: region_delete_snapshot saw {:?}",
            e;
            "region_id" => %region_id,
            "snapshot_id" => %snapshot_id,
        );
        match e {
            crucible_agent_client::Error::ErrorResponse(rv) => {
                match rv.status() {
                    status if status.is_client_error() => {
                        Error::invalid_request(&rv.message)
                    }
                    _ => Error::internal_error(&rv.message),
                }
            }
            _ => Error::internal_error(
                "unexpected failure during `region_delete_snapshot`",
            ),
        }
    })?;

    #[derive(Debug, thiserror::Error)]
    enum WaitError {
        #[error("Transient error: {0}")]
        Transient(#[from] anyhow::Error),

        #[error("Permanent error: {0}")]
        Permanent(#[from] Error),
    }

    backoff::retry_notify(
        backoff::retry_policy_internal_service_aggressive(),
        || async {
            let response = retry_until_known_result(log, || async {
                client
                    .region_get_snapshots(&RegionId(region_id.to_string()))
                    .await
            })
            .await
            .map_err(|e| {
                error!(
                    log,
                    "delete_crucible_snapshot: region_get_snapshots saw {:?}",
                    e;
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                );
                match e {
                    crucible_agent_client::Error::ErrorResponse(rv) => {
                        match rv.status() {
                            status if status.is_client_error() => {
                                BackoffError::Permanent(WaitError::Permanent(
                                    Error::invalid_request(&rv.message),
                                ))
                            }
                            _ => BackoffError::Permanent(WaitError::Permanent(
                                Error::internal_error(&rv.message),
                            )),
                        }
                    }
                    _ => BackoffError::Permanent(WaitError::Permanent(
                        Error::internal_error(
                            "unexpected failure during `region_get_snapshots`",
                        ),
                    )),
                }
            })?;

            if response
                .snapshots
                .iter()
                .any(|x| x.name == snapshot_id.to_string())
            {
                info!(
                    log,
                    "snapshot still exists, waiting";
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

// Given a list of datasets and region snapshots, send DELETE calls to the
// datasets corresponding Crucible Agent for each snapshot.
pub(super) async fn delete_crucible_snapshots(
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
            let url = format!("http://{}", dataset.address());
            let client = CrucibleAgentClient::new(&url);

            delete_crucible_snapshot(
                &log,
                &client,
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

// Given a list of datasets and region snapshots, send DELETE calls to the
// datasets corresponding Crucible Agent for each running read-only downstairs
// corresponding to the snapshot.
pub(super) async fn delete_crucible_running_snapshots(
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
            let url = format!("http://{}", dataset.address());
            let client = CrucibleAgentClient::new(&url);

            delete_crucible_running_snapshot(
                &log,
                &client,
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

pub(crate) async fn get_pantry_address(
    nexus: &Arc<Nexus>,
) -> Result<SocketAddrV6, ActionError> {
    nexus
        .resolver()
        .await
        .lookup_socket_v6(ServiceName::CruciblePantry)
        .await
        .map_err(|e| e.to_string())
        .map_err(ActionError::action_failed)
}

// Common Pantry operations

pub(crate) async fn call_pantry_attach_for_disk(
    log: &slog::Logger,
    opctx: &OpContext,
    nexus: &Arc<Nexus>,
    disk_id: Uuid,
    pantry_address: SocketAddrV6,
) -> Result<(), ActionError> {
    let endpoint = format!("http://{}", pantry_address);

    let (.., disk) = LookupPath::new(opctx, &nexus.datastore())
        .disk_id(disk_id)
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    let disk_volume = nexus
        .datastore()
        .volume_checkout(
            disk.volume_id,
            db::datastore::VolumeCheckoutReason::Pantry,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        log,
        "sending attach for disk {disk_id} volume {} to endpoint {endpoint}",
        disk.volume_id,
    );

    let volume_construction_request: crucible_pantry_client::types::VolumeConstructionRequest =
        serde_json::from_str(&disk_volume.data()).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "failed to deserialize disk {} volume data: {}",
                disk.id(),
                e,
            )))
        })?;

    let client = crucible_pantry_client::Client::new(&endpoint);

    let attach_request = crucible_pantry_client::types::AttachRequest {
        volume_construction_request,
    };

    retry_until_known_result(log, || async {
        client.attach(&disk_id.to_string(), &attach_request).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("pantry attach failed with {:?}", e))
    })?;

    Ok(())
}

pub(crate) async fn call_pantry_detach_for_disk(
    log: &slog::Logger,
    disk_id: Uuid,
    pantry_address: SocketAddrV6,
) -> Result<(), ActionError> {
    let endpoint = format!("http://{}", pantry_address);

    info!(log, "sending detach for disk {disk_id} to endpoint {endpoint}");

    let client = crucible_pantry_client::Client::new(&endpoint);

    retry_until_known_result(log, || async {
        client.detach(&disk_id.to_string()).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("pantry detach failed with {:?}", e))
    })?;

    Ok(())
}
