// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tasks acting as sinks for results.
//!
//! This includes the usual task that inserts data into ClickHouse, and a
//! printing task used in `oximeter` standalone.

// Copyright 2024 Oxide Computer Company

use crate::collection_task::CollectionTaskOutput;
use oximeter::types::ProducerResultsItem;
use oximeter_db::Client;
use oximeter_db::DbWrite as _;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::trace;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;

/// A sink that inserts all results into the ClickHouse database.
///
/// This sink is used in production, when running the `oximeter` collector
/// normally. It aggregates all results, from all collection tasks, and inserts
/// them into ClickHouse in batches.
pub async fn database_inserter(
    log: Logger,
    client: Client,
    cluster_client: Option<Client>,
    batch_size: usize,
    batch_interval: Duration,
    mut rx: mpsc::Receiver<CollectionTaskOutput>,
) {
    let mut timer = interval(batch_interval);
    timer.tick().await; // completes immediately
    let mut batch = Vec::with_capacity(batch_size);
    loop {
        let insert = tokio::select! {
            _ = timer.tick() => {
                if batch.is_empty() {
                    trace!(log, "batch interval expired, but no samples to insert");
                    false
                } else {
                    true
                }
            }
            results = rx.recv() => {
                match results {
                    Some(CollectionTaskOutput {
                        was_forced_collection,
                        results,
                    }) => {
                        let flattened_results = {
                            let mut flattened = Vec::with_capacity(results.len());
                            for inner_batch in results.into_iter() {
                                match inner_batch {
                                    ProducerResultsItem::Ok(samples) => flattened.extend(samples.into_iter()),
                                    ProducerResultsItem::Err(e) => {
                                        debug!(
                                            log,
                                            "received error (not samples) from a producer: {}",
                                            e.to_string()
                                        );
                                    }
                                }
                            }
                            flattened
                        };
                        batch.extend(flattened_results);

                        // Always insert if this was a forced collection request
                        was_forced_collection || batch.len() >= batch_size
                    }
                    None => {
                        warn!(log, "result queue closed, exiting");
                        return;
                    }
                }
            }
        };

        if insert {
            debug!(log, "inserting {} samples into database", batch.len());
            match client.insert_samples(&batch).await {
                Ok(()) => trace!(log, "successfully inserted samples"),
                Err(e) => {
                    warn!(
                        log,
                        "failed to insert some results into metric DB";
                        InlineErrorChain::new(&e)
                    );
                }
            }

            // Our internal testing rack will be running a ClickHouse cluster
            // alongside a single-node installation for a while. We want to handle
            // the case of these two installations running alongside each other, and
            // oximeter writing to both of them. On our production racks ClickHouse
            // will only be run on single-node modality, so we want to ignore all
            // cases where the `ClickhouseClusterNative` service is not available.
            //
            // Even though we set a `claim_timeout` of 100ms on the Qorb pool
            // policy, we need to ping to verify connectivity before proceeding.
            // Qorb by design keeps the claim in a queue if it fails to claim
            // a request. This is because backend may gain slots or come online
            // later.
            //
            // In this case, we do not want to wait because this functionality
            // clogs up oximeter when there is only a single-node installation
            // available.
            if let Some(cluster_client) = &cluster_client {
                match cluster_client.ping().await {
                    Ok(()) => {
                        debug!(
                            log,
                            "inserting {} samples into cluster database",
                            batch.len();
                        );
                        match cluster_client.insert_samples(&batch).await {
                            Ok(()) => trace!(
                                log,
                                "successfully inserted samples into cluster";
                            ),
                            Err(e) => {
                                warn!(
                                    log,
                                    "failed to insert some results into metric cluster DB";
                                    InlineErrorChain::new(&e)
                                );
                            }
                        }
                    }
                    Err(e) => info!(
                        log,
                        "ClickHouse cluster native connection unavailable";
                        InlineErrorChain::new(&e)
                    ),
                }
            }

            // TODO-correctness The `insert_samples` call above may fail. The method itself needs
            // better handling of partially-inserted results in that case, but we may need to retry
            // or otherwise handle an error here as well.
            //
            // See https://github.com/oxidecomputer/omicron/issues/740 for a
            // disucssion.
            batch.clear();
        }
    }
}

/// A sink run in `oximeter` standalone, that logs results on receipt.
pub async fn logger(log: Logger, mut rx: mpsc::Receiver<CollectionTaskOutput>) {
    loop {
        match rx.recv().await {
            Some(CollectionTaskOutput { results, .. }) => {
                for res in results.into_iter() {
                    match res {
                        ProducerResultsItem::Ok(samples) => {
                            for sample in samples.into_iter() {
                                info!(
                                    log,
                                    "";
                                    "sample" => ?sample,
                                );
                            }
                        }
                        ProducerResultsItem::Err(e) => {
                            error!(
                                log,
                                "received error from a producer";
                                InlineErrorChain::new(&e),
                            );
                        }
                    }
                }
            }
            None => {
                debug!(log, "result queue closed, exiting");
                return;
            }
        }
    }
}
