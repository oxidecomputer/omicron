// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tasks acting as sinks for results.
//!
//! This includes the usual task that inserts data into ClickHouse, and a
//! printing task used in `oximeter` standalone.

use crate::collection_task::CollectionTaskOutput;
use crate::probes;
use crate::self_stats;
use oximeter::Sample;
use oximeter::histogram::Record;
use oximeter::queue::BoundedQueue;
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

/// A sink that batches and inserts all results into the ClickHouse database.
///
/// This sink is used in production, when running the `oximeter` collector
/// normally. It aggregates all results, from all collection tasks, and inserts
/// them into ClickHouse in batches.
pub async fn database_batcher(
    log: Logger,
    client: Client,
    batch_size: usize,
    batch_interval: Duration,
    mut rx: mpsc::Receiver<CollectionTaskOutput>,
    sink_stats: Arc<self_stats::CollectorSinkStats>,
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
    // Our task is only responsible for taking results from individual
    // collection tasks and batching them for insertion. `database_inserter` is
    // responsible for the actual insertion.
    let inserter = tokio::spawn(database_inserter(
        log.new(slog::o!("component" => "database-inserter")),
        client,
        batch_rx,
        sink_stats.clone(),
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
                        // that would cause a lot of locking / unlocking in this
                        // loop. Instead, do it once at the end.
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
                        let (n_dropped, queue_depth) =
                            batch_tx.send_and_notify(batch, was_forced_collection);
                        if n_dropped > 0 {
                            probes::dropped__old__samples!(|| n_dropped);
                            warn!(
                                log,
                                "sample buffer full, dropped oldest samples";
                                "n_dropped" => n_dropped,
                            );
                            *sink_stats.samples_dropped.lock().unwrap() += n_dropped.try_into().unwrap();
                        }
                        // Note: Histogram::u64::sample should never error.
                        // The `sample` method only returns a `HistogramError`
                        // if its argument is non-finite, and `u64` values are
                        // always finite.
                        sink_stats.queue_depth.lock().unwrap().sample(queue_depth.try_into().unwrap()).unwrap();
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
struct BatchHandoff<T> {
    notify: Arc<Notify>,
    batch: Arc<Mutex<BoundedQueue<T>>>,
}

impl<T> BatchHandoff<T> {
    fn new(batch_size: usize) -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            batch: Arc::new(Mutex::new(BoundedQueue::new(
                batch_size * MAX_BUFFER_SIZE_MULTIPLIER,
            ))),
        }
    }
}

struct BatchSender<T> {
    // Handoff point between the sender and database inserter.
    handoff: BatchHandoff<T>,
    // Minimum size of the buffer before inserting into the database.
    batch_size: usize,
}

impl<T> BatchSender<T> {
    fn new(handoff: BatchHandoff<T>, batch_size: usize) -> Self {
        Self { handoff, batch_size }
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
        samples: Vec<T>,
        was_forced_collection: bool,
    ) -> (usize, usize) {
        let mut batch = self.handoff.batch.lock().unwrap();

        let n_dropped = batch.extend(samples);

        // Notify the insertion task if needed.
        if was_forced_collection || batch.len() >= self.batch_size {
            self.notify_inserter();
        }
        (n_dropped, batch.len())
    }
}

struct BatchReceiver<T> {
    handoff: BatchHandoff<T>,
}

impl<T> BatchReceiver<T> {
    // Wait for a notification and atomically swap out the entire buffer.
    //
    // Returns a `VecDeque` so the caller can use `make_contiguous()` to obtain
    // a contiguous `&[Sample]` without an additional heap allocation.
    async fn wait_for_batch(&self) -> VecDeque<T> {
        self.handoff.notify.notified().await;
        self.handoff.batch.lock().unwrap().drain()
    }
}

fn batch_handoff<T: Clone>(
    batch_size: usize,
) -> (BatchSender<T>, BatchReceiver<T>) {
    let handoff = BatchHandoff::new(batch_size);
    let sender = BatchSender::new(handoff.clone(), batch_size);
    let receiver = BatchReceiver { handoff };
    (sender, receiver)
}

// The task that actually inserts results into the database.
async fn database_inserter(
    log: Logger,
    client: Client,
    batch_rx: BatchReceiver<Sample>,
    sink_stats: Arc<self_stats::CollectorSinkStats>,
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
                sink_stats.insert_errors.lock().unwrap().increment();
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use omicron_test_utils::dev::test_setup_log;
    use proptest::prelude::*;
    use tokio::time::timeout;

    const BATCH_SIZE: usize = 10;
    const MAX_BUFFER_SIZE: usize = BATCH_SIZE * MAX_BUFFER_SIZE_MULTIPLIER;

    #[tokio::test]
    async fn batch_handoff_notifies_receiver_when_forced() {
        let (batch_tx, batch_rx) = batch_handoff::<()>(BATCH_SIZE);
        let n_samples = 3;
        let samples = vec![(); n_samples];
        let (n_dropped, _) = batch_tx.send_and_notify(samples, true);
        assert_eq!(n_dropped, 0);
        assert_eq!(batch_tx.handoff.batch.lock().unwrap().len(), n_samples);

        let batch =
            timeout(Duration::from_millis(10), batch_rx.wait_for_batch())
                .await
                .expect("Should be notified with force collection");
        assert_eq!(batch.len(), n_samples);
        assert_eq!(batch_tx.handoff.batch.lock().unwrap().len(), 0);
    }

    proptest! {
        #[test]
        fn test_batch_handoff_overflow(
            current_size in 0..=2*MAX_BUFFER_SIZE,
            new_size in 0..=2*MAX_BUFFER_SIZE,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            // Insert the first batch of samples.
            let (batch_tx, batch_rx) = batch_handoff::<usize>(BATCH_SIZE);
            let samples = (0..current_size).collect();
            let (n_dropped, _) = batch_tx.send_and_notify(samples, false);
            let expected_n_dropped = current_size.saturating_sub(MAX_BUFFER_SIZE);
            let expected_n_samples = current_size.min(MAX_BUFFER_SIZE);
            assert_eq!(n_dropped, expected_n_dropped);

            // Assert that we've retained only the tail of the samples.
            let tail = (current_size.saturating_sub(MAX_BUFFER_SIZE)..current_size)
                .collect::<VecDeque<_>>();
            {
                let buf = batch_tx.handoff.batch.lock().unwrap();
                assert_eq!(buf.len(), expected_n_samples);
                assert!(buf.iter().eq(tail.iter()));
            }

            // Check that we're either notified, or not, if the current batch is
            // large enough.
            //
            // NOTE: We're not calling `wait_for_batch()` here, but using the
            // shared buffer directly. That's so that we don't swap it out
            // during this part of the test. We do use that method later.
            const TIMEOUT: Duration = Duration::from_millis(1);
            let fut = batch_rx.handoff.notify.notified();
            if current_size >= BATCH_SIZE {
                rt.block_on(async {
                    timeout(TIMEOUT, fut).await
                }).expect("Should be notified");
                assert!(batch_rx.handoff.batch.lock().unwrap().iter().eq(tail.iter()));
            } else {
                rt.block_on(async {
                    timeout(TIMEOUT, fut).await
                }).expect_err("Should not be notified");
            }

            // Push the new set of samples, which start at the end of the first
            // batch, even if we've dropped some.
            let new_samples = (current_size..(current_size + new_size)).collect();
            let (n_dropped, _) = batch_tx.send_and_notify(new_samples, false);

            // We expect to drop all the samples beyond the maximum buffer size.
            // We need to include what we have in the buffer already (some of
            // which we could have dropped) and what we add in this next step.
            let expected_n_dropped = (expected_n_samples + new_size)
                .saturating_sub(MAX_BUFFER_SIZE);

            // We expect to retain the minimum of what we (had already plus what
            // we add) and the maximum buffer size.
            let expected_n_samples = (expected_n_samples + new_size)
                .min(MAX_BUFFER_SIZE);
            assert_eq!(n_dropped, expected_n_dropped);

            // Again, check that we retain the tail of _all_ the samples we've
            // added thus far, including the first and second batch.
            let total_size = current_size + new_size;
            let tail = (total_size.saturating_sub(MAX_BUFFER_SIZE)..total_size).collect::<VecDeque<_>>();
            {
                let buf = batch_tx.handoff.batch.lock().unwrap();
                assert_eq!(buf.len(), expected_n_samples);
                assert!(buf.iter().eq(tail.iter()));
            }

            // And check we've notified the receiver again.
            let fut = batch_rx.wait_for_batch();
            if expected_n_samples >= BATCH_SIZE {
                let received = rt.block_on(async {
                    timeout(TIMEOUT, fut).await
                }).expect("Should be notified");
                assert_eq!(received, tail);
            } else {
                rt.block_on(async {
                    timeout(TIMEOUT, fut).await
                }).expect_err("Should not be notified");
            }
        }
    }

    #[tokio::test]
    async fn batcher_increments_dropped_counter() {
        let logctx = test_setup_log("batcher_increments_dropped_counter");
        let log = &logctx.log;

        // Construct a database batcher with a dummy ClickHouse client.
        let client = Client::new("[::1]:0".parse().unwrap(), log);
        let (tx, rx) = mpsc::channel(1);
        let sink_stats = Arc::new(self_stats::CollectorSinkStats::new(
            "test".into(),
            Utc::now(),
        ));
        let batcher = tokio::spawn(database_batcher(
            log.clone(),
            client,
            BATCH_SIZE,
            Duration::from_secs(3600),
            rx,
            sink_stats.clone(),
        ));

        // Insert `want_dropped` too many samples into the batcher.
        let want_dropped = 50;
        let samples = (0..MAX_BUFFER_SIZE + want_dropped)
            .map(|_| oximeter_test_utils::make_sample())
            .collect();
        tx.send(CollectionTaskOutput {
            results: vec![ProducerResultsItem::Ok(samples)],
            was_forced_collection: false,
        })
        .await
        .unwrap();

        // Drop the sender so that the batcher loop exits.
        drop(tx);
        batcher.await.unwrap();

        let samples_dropped = sink_stats.samples_dropped.lock().unwrap();
        assert_eq!(samples_dropped.value(), want_dropped as u64);

        let queue_depth = sink_stats.queue_depth.lock().unwrap();
        assert_eq!(queue_depth.n_samples(), 1);
        assert_eq!(queue_depth.max(), MAX_BUFFER_SIZE as u64);

        logctx.cleanup_successful();
    }
}
