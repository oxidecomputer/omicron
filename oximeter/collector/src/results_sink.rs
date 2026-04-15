// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tasks acting as sinks for results.
//!
//! This includes the usual task that inserts data into ClickHouse, and a
//! printing task used in `oximeter` standalone.

// Copyright 2026 Oxide Computer Company

use crate::collection_task::CollectionTaskOutput;
use crate::probes;
use oximeter::Sample;
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
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio::time::interval;

/// A sink that inserts all results into the ClickHouse database.
///
/// This sink is used in production, when running the `oximeter` collector
/// normally. It aggregates all results, from all collection tasks, and inserts
/// them into ClickHouse in batches.
pub async fn database_inserter(
    log: Logger,
    client: Client,
    batch_size: usize,
    batch_interval: Duration,
    mut rx: mpsc::Receiver<CollectionTaskOutput>,
) {
    // Construct a handoff point between the batch task here, and the database
    // insertion task.
    //
    // As this task receives individual collection results from the collection
    // tasks, it batches them up in a shared buffer. When that buffer reaches at
    // least the batch size, or a timer expires, it then notifies the insertion
    // task that it should consume the buffer and insert those samples into the
    // database.
    let (batch_tx, batch_rx) = batch_handoff(batch_size);

    // Spawn a task for doing the actual insertion into the database.
    //
    // This outer task is responsible only for taking results from the
    // individual collection tasks and batching them up for insertion.
    let inserter = tokio::spawn(database_inserter_impl(
        log.new(slog::o!("component" => "database-inserter")),
        client,
        batch_rx,
    ));

    // Spawn a timer for ensuring we periodically notify the inserter to
    // actually flush to the database, regardless of the number of samples we've
    // collected in the batch.
    let mut batch_timer = interval(batch_interval);
    batch_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    batch_timer.tick().await;

    loop {
        tokio::select! {
            _ = batch_timer.tick() => batch_tx.notify_inserter(),
            results = rx.recv() => {
                match results {
                    Some(CollectionTaskOutput {
                        was_forced_collection,
                        results,
                    }) => {
                        probes::results__sink__item__dequeued!();

                        // Collect all samples from this one collection result
                        // into an intermediate batch.
                        //
                        // NOTE: We could call `batch_tx.send_and_notify()` here
                        // in the loop. I'm avoiding that for now because it
                        // takes a lock on the shared buffer of samples, so
                        // there's a lot of locking / unlocking in this loop.
                        // Instead, do it once at the end.
                        let mut n_samples = 0;
                        let mut batch = Vec::with_capacity(results.len());
                        for inner_batch in results.into_iter() {
                            match inner_batch {
                                ProducerResultsItem::Ok(mut samples) => {
                                    n_samples += samples.len();
                                    batch.append(&mut samples);
                                }
                                ProducerResultsItem::Err(e) => {
                                    debug!(
                                        log,
                                        "received error (not samples) from a producer";
                                        InlineErrorChain::new(&e),
                                    );
                                }
                            }
                        }
                        probes::results__sink__item__processed!(|| n_samples);

                        // Append the current batch to the handoff buffer.
                        let n_dropped =
                            batch_tx.send_and_notify(batch, was_forced_collection);
                        if n_dropped > 0 {
                            probes::dropped__old__samples!(|| n_dropped);
                            warn!(
                                log,
                                "sample buffer full, dropped oldest samples";
                                "n_dropped" => n_dropped,
                            );
                        }
                    }
                    None => {
                        warn!(log, "result queue closed, exiting");
                        inserter.abort();
                        return;
                    }
                }
            }
        };
    }
}

// The maximum number of samples the shared ring buffer can hold, expressed as a
// multiple of the batch size. If the insertion task falls behind (e.g., because
// ClickHouse is slow or unreachable), new samples evict the oldest ones once
// this limit is reached. This bounds memory consumption while preferring
// recent data.
const MAX_BUFFER_SIZE_MULTIPLIER: usize = 100;

// A handoff point for a batch of samples from collectors and the database
// inserter.
//
// This acts as a rendezvous point for the batching task, which collects results
// from individual collection tasks and aggregates them, and the actual database
// insertion task. As results are collected from the tasks, they are appended
// onto the existing ring buffer of samples, which is protected by a sync mutex.
// The other side is then notified if the existing batch is at least the
// insertion batch size.
#[derive(Clone)]
struct BatchHandoff {
    notify: Arc<Notify>,
    batch: Arc<Mutex<VecDeque<Sample>>>,
}

impl BatchHandoff {
    fn new(batch_size: usize) -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            batch: Arc::new(Mutex::new(VecDeque::with_capacity(batch_size))),
        }
    }
}

struct BatchSender {
    // Handoff point between the sender and database inserter.
    handoff: BatchHandoff,
    // Minimum size of the buffer before inserting into the database.
    batch_size: usize,
    // Maximum size we let the ring buffer grow before starting to drop older
    // samples. This is a relief value, in the case where the database is
    // completely partitioned or insertions have slowed dramatically.
    max_buffer_size: usize,
}

impl BatchSender {
    fn new(handoff: BatchHandoff, batch_size: usize) -> Self {
        Self {
            handoff,
            batch_size,
            max_buffer_size: batch_size * MAX_BUFFER_SIZE_MULTIPLIER,
        }
    }

    // Notify the insertion task that it should insert the batch of results.
    fn notify_inserter(&self) {
        self.handoff.notify.notify_one()
    }

    // Append a list of samples to the ring buffer, and possibly notify the
    // inserter.
    //
    // If appending would exceed the maximum buffer size, the oldest samples are
    // dropped from the front to make room. Returns the number of samples
    // dropped.
    //
    // This notifies the insertion task if either the current batch is at least
    // the batch size, or if the new data was the result of a forced collection
    // attempt.
    fn send_and_notify(
        &self,
        samples: Vec<Sample>,
        was_forced_collection: bool,
    ) -> usize {
        let mut batch = self.handoff.batch.lock().unwrap();
        batch.extend(samples);

        // Drop the oldest samples from the front if we've exceeded the limit.
        // VecDeque makes front-removal O(1) per element.
        let n_dropped = batch.len().saturating_sub(self.max_buffer_size);
        if n_dropped > 0 {
            drop(batch.drain(..n_dropped));
        }

        if was_forced_collection || batch.len() >= self.batch_size {
            self.notify_inserter();
        }
        n_dropped
    }
}

struct BatchReceiver {
    handoff: BatchHandoff,
    batch_size: usize,
}

impl BatchReceiver {
    // Wait for a notification and atomically swap out the entire buffer.
    //
    // Returns a `VecDeque` so the caller can use `make_contiguous()` to obtain
    // a contiguous `&[Sample]` without an additional heap allocation.
    async fn wait_for_batch(&self) -> VecDeque<Sample> {
        self.handoff.notify.notified().await;
        let mut empty = VecDeque::with_capacity(self.batch_size);
        let mut batch = self.handoff.batch.lock().unwrap();
        std::mem::swap(&mut empty, &mut *batch);
        empty
    }
}

fn batch_handoff(batch_size: usize) -> (BatchSender, BatchReceiver) {
    let handoff = BatchHandoff::new(batch_size);
    let sender = BatchSender::new(handoff.clone(), batch_size);
    let receiver = BatchReceiver { handoff, batch_size };
    (sender, receiver)
}

// The task that actually inserts results into the database.
async fn database_inserter_impl(
    log: Logger,
    client: Client,
    batch_rx: BatchReceiver,
) {
    loop {
        // Wait for a notification that there are samples to insert, and consume
        // them. This swaps out the buffer of samples shared with the batching
        // task.
        //
        // TODO-correctness The `insert_samples` call below may fail. The method itself needs
        // better handling of partially-inserted results in that case, but we may need to retry
        // or otherwise handle an error here as well.
        //
        // See https://github.com/oxidecomputer/omicron/issues/740 for a
        // disucssion.
        let mut batch = batch_rx.wait_for_batch().await;
        if batch.is_empty() {
            debug!(log, "batch interval expired, but no samples to insert");
            continue;
        }
        debug!(log, "inserting samples into database"; "n_samples" => batch.len());
        probes::insert__samples__start!(|| batch.len());
        match client.insert_samples(batch.make_contiguous()).await {
            Ok(()) => {
                probes::insert__samples__done!();
                trace!(log, "successfully inserted samples");
            }
            Err(e) => {
                let err = InlineErrorChain::new(&e);
                probes::insert__samples__failed!(|| err.to_string());
                warn!(
                    log,
                    "failed to insert some results into metric DB";
                    err,
                );
            }
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
