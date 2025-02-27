// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task responsible for collecting from a single producer.

// Copyright 2024 Oxide Computer Company

use crate::self_stats;
use crate::Error;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use oximeter::types::ProducerResults;
use oximeter::types::ProducerResultsItem;
use oximeter_api::FailedCollection;
use oximeter_api::ProducerDetails;
use oximeter_api::SuccessfulCollection;
use slog::debug;
use slog::error;
use slog::o;
use slog::trace;
use slog::warn;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::interval;
use tokio::time::Instant;
use tokio::time::Interval;

/// Error returned when a forced collection fails.
#[derive(Clone, Copy, Debug)]
pub enum ForcedCollectionError {
    /// The internal queue of requests is full.
    QueueFull,
    /// We failed to send the request because the channel was closed.
    Closed,
}

/// Timeout on any single collection from a producer.
const COLLECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// The number of forced collections queued before we start to deny them.
const N_QUEUED_FORCED_COLLECTIONS: usize = 1;

/// The number of timer-based collections queued before we start to deny them.
const N_QUEUED_TIMER_COLLECTIONS: usize = 1;

/// The number of queued messages from the main collector agent.
const N_QUEUED_TASK_MESSAGES: usize = 4;

/// The number of queued results from our internal collection task.
const N_QUEUED_RESULTS: usize = 1;

// Messages for controlling a collection task
#[derive(Debug)]
enum CollectionMessage {
    // Explicit request that the task collect data from its producer
    ForceCollect,
    // Request that the task update its interval and the socket address on which it collects data
    // from its producer.
    Update(ProducerEndpoint),
    // Request that the task exit
    Shutdown,
    // Return the current statistics from a single task.
    #[cfg(test)]
    Statistics {
        reply_tx: oneshot::Sender<self_stats::CollectionTaskStats>,
    },
    // Request details from the collection task about its producer.
    Details {
        reply_tx: oneshot::Sender<ProducerDetails>,
    },
}

/// Return type for `perform_collection`.
struct SingleCollectionResult {
    /// The result of the collection.
    result: Result<ProducerResults, self_stats::FailureReason>,
    /// The duration the collection took.
    duration: Duration,
}

/// Run a single collection from the producer.
async fn perform_collection(
    log: Logger,
    client: reqwest::Client,
    producer: ProducerEndpoint,
) -> SingleCollectionResult {
    let start = Instant::now();
    debug!(log, "collecting from producer");
    let res = client
        .get(format!("http://{}/{}", producer.address, producer.id))
        .send()
        .await;
    trace!(log, "sent collection request to producer");
    let result = match res {
        Ok(res) => {
            if res.status().is_success() {
                match res.json::<ProducerResults>().await {
                    Ok(results) => {
                        debug!(
                            log,
                            "collected results from producer";
                            "n_results" => results.len()
                        );
                        Ok(results)
                    }
                    Err(e) => {
                        warn!(
                            log,
                            "failed to collect results from producer";
                            InlineErrorChain::new(&e),
                        );
                        Err(self_stats::FailureReason::Deserialization)
                    }
                }
            } else {
                warn!(
                    log,
                    "failed to receive metric results from producer";
                    "status_code" => res.status().as_u16(),
                );
                Err(self_stats::FailureReason::Other(res.status()))
            }
        }
        Err(e) => {
            error!(
                log,
                "failed to send collection request to producer";
                InlineErrorChain::new(&e),
            );
            Err(self_stats::FailureReason::Unreachable)
        }
    };
    SingleCollectionResult { result, duration: start.elapsed() }
}

// The type of one collection task run to completion.
//
// An `Err(_)` means we failed to collect, and contains the reason so that we
// can bump the self-stat counter accordingly.
type CollectionResult = Result<ProducerResults, self_stats::FailureReason>;

/// Information about when we start a collection.
struct CollectionStartTimes {
    /// UTC timestamp at which the request was started.
    started_at: DateTime<Utc>,
    /// Instant right before we queued the response for processing.
    queued_at: Instant,
}

impl CollectionStartTimes {
    fn new() -> Self {
        Self { started_at: Utc::now(), queued_at: Instant::now() }
    }
}

/// Details about a forced collection.
struct ForcedCollectionRequest {
    /// Start time for this collection.
    start: CollectionStartTimes,
}

impl ForcedCollectionRequest {
    fn new() -> Self {
        Self { start: CollectionStartTimes::new() }
    }
}

/// Details about a completed collection.
struct CollectionResponse {
    /// Was the collection the result of a force collect attempt?
    was_forced_collection: bool,
    /// The actual result of the collection.
    result: CollectionResult,
    /// Time when the collection started.
    started_at: DateTime<Utc>,
    /// Time the request spent queued.
    time_queued: Duration,
    /// Time we spent processing the request.
    time_collecting: Duration,
}

/// Task that actually performs collections from the producer.
async fn collection_loop(
    log: Logger,
    mut producer_info_rx: watch::Receiver<ProducerEndpoint>,
    mut forced_collection_rx: mpsc::Receiver<ForcedCollectionRequest>,
    mut timer_collection_rx: mpsc::Receiver<CollectionStartTimes>,
    result_tx: mpsc::Sender<CollectionResponse>,
) {
    let client = reqwest::Client::builder()
        .timeout(COLLECTION_TIMEOUT)
        .build()
        // Safety: `build()` only fails if TLS couldn't be initialized or the
        // system DNS configuration could not be loaded.
        .unwrap();
    loop {
        // Wait for notification that we have a collection to perform, from
        // either the forced- or timer-collection queue.
        trace!(log, "top of inner collection loop, waiting for next request");
        let (was_forced_collection, start_time) = tokio::select! {
            maybe_request = forced_collection_rx.recv() => {
                let Some(ForcedCollectionRequest { start }) = maybe_request else {
                    debug!(
                        log,
                        "forced collection request queue closed, exiting"
                    );
                    return;
                };
                (true, start)
            }
            maybe_request = timer_collection_rx.recv() => {
                let Some(start) = maybe_request else {
                    debug!(
                        log,
                        "timer collection request queue closed, exiting"
                    );
                    return;
                };
                (false, start)
            }
        };

        // Record the time this request was queued. We'll include this along
        // with the time spent collecting, which is returned from the future
        // that actually does the collection.
        let CollectionStartTimes { started_at, queued_at } = start_time;
        let time_queued = queued_at.elapsed();

        // Make a future to represent the actual collection.
        let mut collection_fut = Box::pin(perform_collection(
            log.clone(),
            client.clone(),
            *producer_info_rx.borrow_and_update(),
        ));

        // Wait for that collection to complete or fail, or for an update to the
        // producer's information. In the latter case, recreate the future for
        // the collection itself with the new producer information.
        let SingleCollectionResult { result, duration } = 'collection: loop {
            tokio::select! {
                biased;

                maybe_update = producer_info_rx.changed() => {
                    match maybe_update {
                        Ok(_) => {
                            let update = *producer_info_rx.borrow_and_update();
                            debug!(
                                log,
                                "received producer info update with an outstanding \
                                collection running, cancelling it and recreating \
                                with the new info";
                                "new_info" => ?&update,
                            );
                            collection_fut = Box::pin(perform_collection(
                                log.new(o!("address" => update.address)),
                                client.clone(),
                                update,
                            ));
                            continue 'collection;
                        }
                        Err(e) => {
                            error!(
                                log,
                                "failed to receive on producer update \
                                watch channel, exiting";
                                InlineErrorChain::new(&e),
                            );
                            return;
                        }
                    }
                }

                collection_result = &mut collection_fut => {
                    break 'collection collection_result;
                }
            }
        };

        // Now that the collection has completed, send on the results, along
        // with the timing information we got with the request.
        let response = CollectionResponse {
            was_forced_collection,
            result,
            started_at,
            time_queued,
            time_collecting: duration,
        };
        match result_tx.send(response).await {
            Ok(_) => trace!(log, "forwarded results to main collection loop"),
            Err(_) => {
                error!(
                    log,
                    "failed to forward results to \
                    collection loop, channel is closed, exiting",
                );
                return;
            }
        }
    }
}

/// Type of each output sent from a collection task to the results sink.
pub(crate) struct CollectionTaskOutput {
    pub(crate) was_forced_collection: bool,
    pub(crate) results: ProducerResults,
}

/// Handle to the task which collects metric data from a single producer.
#[derive(Debug)]
pub struct CollectionTaskHandle {
    /// Information about the producer we're currently collecting from.
    pub producer: ProducerEndpoint,
    // Channel used to send messages from the agent to the actual task.
    //
    // The task owns the other side.
    task_tx: mpsc::Sender<CollectionMessage>,
    log: Logger,
}

impl CollectionTaskHandle {
    /// Create a new collection task handle.
    ///
    /// This spawns the actual task itself, and returns a handle to it. The
    /// latter is used to send messages to the task, through the handle's
    /// `inbox` field.
    pub async fn new(
        log: &Logger,
        collector: self_stats::OximeterCollector,
        producer: ProducerEndpoint,
        outbox: mpsc::Sender<CollectionTaskOutput>,
    ) -> Self {
        let (task, task_tx) =
            CollectionTask::new(log, collector, producer, outbox).await;
        tokio::spawn(task.run());
        let log = log.new(o!(
            "component" => "collection-task-handle",
            "producer_id" => producer.id.to_string(),
        ));
        Self { task_tx, producer, log }
    }

    /// Ask the task to update its producer endpoint information.
    ///
    /// # Panics
    ///
    /// This panics if we could not send a message to the internal collection
    /// task. That only happens when that task has exited.
    pub async fn update(&mut self, info: ProducerEndpoint) {
        match self.task_tx.send(CollectionMessage::Update(info)).await {
            Ok(_) => {
                trace!(
                    self.log,
                    "sent update message to task";
                    "new_info" => ?info,
                );
                self.producer = info;
            }
            Err(e) => {
                error!(
                    self.log,
                    "failed to send update message to task!";
                    "error" => InlineErrorChain::new(&e),
                );
                panic!("failed to send update message to task: {}", e);
            }
        }
    }

    /// Ask the collection task to shutdown.
    pub async fn shutdown(&self) {
        match self.task_tx.send(CollectionMessage::Shutdown).await {
            Ok(_) => trace!(self.log, "sent shutdown message to task"),
            Err(e) => error!(
                self.log,
                "failed to send shutdown message to task!";
                "error" => InlineErrorChain::new(&e),
            ),
        }
    }

    /// Return the current statistics from this task.
    #[cfg(test)]
    pub async fn statistics(&self) -> self_stats::CollectionTaskStats {
        let (reply_tx, rx) = oneshot::channel();
        self.task_tx
            .send(CollectionMessage::Statistics { reply_tx })
            .await
            .expect("Failed to send statistics message");
        rx.await.expect("Failed to receive statistics")
    }

    /// Return details about the current producer this task is collecting from.
    ///
    /// An error is returned if we either could not send the request to the
    /// producer because its queue is full, or because the task failed to send
    /// us the response.
    ///
    /// Note that this makes collecting details best-effort -- if the task is
    /// already doing lots of work and its queue is full, we fail rather than
    /// block.
    pub async fn details(&self) -> Result<ProducerDetails, Error> {
        let (reply_tx, rx) = oneshot::channel();
        if self
            .task_tx
            .try_send(CollectionMessage::Details { reply_tx })
            .is_err()
        {
            return Err(Error::CollectionError(
                self.producer.id,
                String::from(
                    "Failed to send detail request to collection task",
                ),
            ));
        }
        rx.await.map_err(|_| {
            Error::CollectionError(
                self.producer.id,
                String::from(
                    "Failed to receive detail response from collection task",
                ),
            )
        })
    }

    /// Explicitly request that the task collect from its producer.
    ///
    /// Note that this doesn't block. If it's able to notify the collection task
    /// of the request, it returns `Ok(())`; this _does not_ mean the collection
    /// has been performed, only that a request to perform a collection has been
    /// enqueued.
    pub(crate) fn try_force_collect(
        &self,
    ) -> Result<(), ForcedCollectionError> {
        self.task_tx.try_send(CollectionMessage::ForceCollect).map_err(|err| {
            match err {
                TrySendError::Full(_) => ForcedCollectionError::QueueFull,
                TrySendError::Closed(_) => ForcedCollectionError::Closed,
            }
        })
    }
}

/// Helper type used to simplify control flow in the main `CollectionTask::run`
/// method.
type TaskAction = std::ops::ControlFlow<()>;

/// Main task used to dispatch messages from the oximeter agent and request
/// collections from the producer.
#[derive(Debug)]
struct CollectionTask {
    log: Logger,

    // The details about past collections from this producer.
    details: ProducerDetails,

    // Statistics about all collections we've made so far.
    stats: self_stats::CollectionTaskStats,

    // Inbox for messages from the controlling task handle.
    inbox: mpsc::Receiver<CollectionMessage>,

    // Watch channel for broadcasting changes about the producer.
    producer_info_tx: watch::Sender<ProducerEndpoint>,

    // Channel for sending forced collection requests.
    forced_collection_tx: mpsc::Sender<ForcedCollectionRequest>,

    // Channel for sending timer-based collection requests.
    timer_collection_tx: mpsc::Sender<CollectionStartTimes>,

    // Channel for receiving collection responses from the inner collection
    // loop.
    result_rx: mpsc::Receiver<CollectionResponse>,

    // Outbox for forwarding the results to the sink.
    outbox: mpsc::Sender<CollectionTaskOutput>,

    // Timer for making collections periodically.
    collection_timer: Interval,

    // Timer for reporting our own collection statistics to the database.
    self_collection_timer: Interval,
}

impl CollectionTask {
    // Construct a new collection task.
    //
    // This also spawns the internal task which itself manages the collections
    // from our assigned producer. It then creates all the controlling queues
    // for talking to this task and the inner task.
    async fn new(
        log: &Logger,
        collector: self_stats::OximeterCollector,
        producer: ProducerEndpoint,
        outbox: mpsc::Sender<CollectionTaskOutput>,
    ) -> (Self, mpsc::Sender<CollectionMessage>) {
        // Create our own logger.
        let log = log.new(o!(
            "component" => "collection-task",
            "producer_id" => producer.id.to_string(),
        ));

        // Setup queues for talking between ourselves, our controlling task
        // handle, and the spawned collection loop itself.
        let (task_tx, inbox) = mpsc::channel(N_QUEUED_TASK_MESSAGES);
        let (producer_info_tx, producer_info_rx) = watch::channel(producer);
        let (forced_collection_tx, forced_collection_rx) =
            mpsc::channel(N_QUEUED_FORCED_COLLECTIONS);
        let (timer_collection_tx, timer_collection_rx) =
            mpsc::channel(N_QUEUED_TIMER_COLLECTIONS);
        let (result_tx, result_rx) = mpsc::channel(N_QUEUED_RESULTS);
        tokio::task::spawn(collection_loop(
            log.clone(),
            producer_info_rx,
            forced_collection_rx,
            timer_collection_rx,
            result_tx,
        ));

        // Construct ourself, and return our controlling input queue.
        let details = ProducerDetails::new(&producer);
        let stats = self_stats::CollectionTaskStats::new(collector, &producer);
        let collection_timer = Self::timer(producer.interval).await;
        let self_collection_timer =
            Self::timer(self_stats::COLLECTION_INTERVAL).await;
        let self_ = Self {
            log,
            details,
            stats,
            inbox,
            outbox,
            producer_info_tx,
            forced_collection_tx,
            timer_collection_tx,
            result_rx,
            collection_timer,
            self_collection_timer,
        };
        (self_, task_tx)
    }

    /// Helper to construct a timer and tick it.
    ///
    /// Since a `tokio::time::interval`'s first tick completes immediately, this
    /// constructs the timer and then _ticks it_ once.
    async fn timer(t: Duration) -> Interval {
        let mut timer = interval(t);
        timer.tick().await;
        timer
    }

    /// Run the main loop of this collection task.
    ///
    /// NOTE: This returns a `TaskAction`, but the value isn't used. It returns
    /// that value to simplify control-flow internally, which uses `?` to
    /// propagate the `TaskAction::Break` variant when we need to exit.
    async fn run(mut self) -> TaskAction {
        loop {
            tokio::select! {
                message = self.inbox.recv() => {
                    let Some(message) = message else {
                        debug!(
                            self.log,
                            "collection task inbox closed, shutting down"
                        );
                        return TaskAction::Break(());
                    };
                    self.handle_inbox_message(message).await?;
                }
                maybe_result = self.result_rx.recv() => {
                    let Some(response) = maybe_result else {
                        error!(
                            self.log,
                            "channel for receiving results from collection task \
                            is closed, exiting",
                        );
                        return TaskAction::Break(());
                    };
                    self.handle_collection_response(response).await?;
                }
                _ = self.self_collection_timer.tick() => {
                    debug!(
                        self.log,
                        "reporting oximeter self-collection statistics"
                    );
                    self.outbox.send(CollectionTaskOutput {
                        was_forced_collection: false,
                        results: self.stats.sample(),
                    }).await.unwrap();
                }
                _ = self.collection_timer.tick() => {
                    self.handle_collection_timer_tick().await?;
                }
            }
        }
    }

    /// Handle a single message from the task handle.
    ///
    /// This method takes messages from the main oximeter agent, passed through
    /// our controlling handle. This implements the main public API of the
    /// `CollectionTaskHandle` methods that the agent uses.
    async fn handle_inbox_message(
        &mut self,
        message: CollectionMessage,
    ) -> TaskAction {
        match message {
            CollectionMessage::Shutdown => {
                debug!(self.log, "collection task received shutdown request");
                return TaskAction::Break(());
            }
            CollectionMessage::ForceCollect => {
                debug!(
                    self.log,
                    "collection task received explicit request to collect"
                );
                let request = ForcedCollectionRequest::new();
                match self.forced_collection_tx.try_send(request) {
                    Ok(_) => {
                        trace!(
                            self.log,
                            "forwarded explicit request to collection task"
                        );
                    }
                    Err(e) => match e {
                        TrySendError::Closed(ForcedCollectionRequest {
                            ..
                        }) => {
                            debug!(
                                self.log,
                                "collection task forced collection \
                                queue is closed. Exiting.",
                            );
                            return TaskAction::Break(());
                        }
                        TrySendError::Full(ForcedCollectionRequest {
                            start,
                        }) => {
                            error!(
                                self.log,
                                "collection task forced collection \
                                queue is full! This should never \
                                happen, and probably indicates \
                                a bug in your test code, such as \
                                calling `try_force_collect()` many \
                                times"
                            );
                            let failure = FailedCollection {
                                started_at: start.started_at,
                                time_queued: Duration::ZERO,
                                time_collecting: Duration::ZERO,
                                reason: String::from(
                                    "forced collection queue full",
                                ),
                            };
                            self.details.on_failure(failure);
                        }
                    },
                }
            }
            CollectionMessage::Update(new_info) => {
                // If the collection interval is shorter than the
                // interval on which we receive these update messages,
                // we'll never actually collect anything! Instead, only
                // do the update if the information has changed. This
                // should also be guarded against by the main agent, but
                // we're being cautious here.
                let updated_producer_info = |info: &mut ProducerEndpoint| {
                    if new_info == *info {
                        false
                    } else {
                        *info = new_info;
                        true
                    }
                };
                if !self
                    .producer_info_tx
                    .send_if_modified(updated_producer_info)
                {
                    trace!(
                        self.log,
                        "collection task received update with \
                        identical producer information, no \
                        updates will be sent to the collection task"
                    );
                    return TaskAction::Continue(());
                }

                // We have an actual update to the producer information.
                //
                // Rebuild our timer to reflect the possibly-new
                // interval. The collection task has already been
                // notified above.
                debug!(
                    self.log,
                    "collection task received request to update \
                    its producer information";
                    "interval" => ?new_info.interval,
                    "address" => new_info.address,
                );
                self.details.update(&new_info);
                self.stats.update(&new_info);
                self.collection_timer = Self::timer(new_info.interval).await;
            }
            #[cfg(test)]
            CollectionMessage::Statistics { reply_tx } => {
                // Time should be paused when using this retrieval
                // mechanism. We advance time to cause a panic if this
                // message were to be sent with time *not* paused.
                tokio::time::advance(Duration::from_nanos(1)).await;
                // The collection timer *may* be ready to go in which
                // case we would do a collection right after
                // processesing this message, thus changing the actual
                // data. Instead we reset the timer to prevent
                // additional collections (i.e. since time is paused).
                self.collection_timer.reset();
                debug!(
                    self.log,
                    "received request for current task statistics"
                );
                reply_tx
                    .send(self.stats.clone())
                    .expect("failed to send statistics");
            }
            CollectionMessage::Details { reply_tx } => {
                match reply_tx.send(self.details.clone()) {
                    Ok(_) => trace!(
                        self.log,
                        "sent producer details reply to oximeter agent",
                    ),
                    Err(e) => error!(
                        self.log,
                        "failed to send producer details reply to \
                        oximeter agent";
                        "error" => ?e,
                    ),
                }
            }
        }

        // Continue unless we explicitly exit early.
        TaskAction::Continue(())
    }

    /// Handle a single collection response from the inner collection task.
    ///
    /// This takes responses from the spawned task that actually does
    /// collections, and dispatches them to the results sink. It also updates
    /// our own details and collection stats accordingly.
    async fn handle_collection_response(
        &mut self,
        response: CollectionResponse,
    ) -> TaskAction {
        let CollectionResponse {
            was_forced_collection,
            result,
            started_at,
            time_queued,
            time_collecting,
        } = response;
        match result {
            Ok(results) => {
                self.stats.collections.datum.increment();
                let n_samples: u64 = results
                    .iter()
                    .map(|each| match each {
                        ProducerResultsItem::Ok(samples) => {
                            samples.len() as u64
                        }
                        _ => 0,
                    })
                    .sum();
                let success = SuccessfulCollection {
                    started_at,
                    time_queued,
                    time_collecting,
                    n_samples,
                };
                self.details.on_success(success);
                if self
                    .outbox
                    .send(CollectionTaskOutput {
                        was_forced_collection,
                        results,
                    })
                    .await
                    .is_err()
                {
                    error!(
                        self.log,
                        "failed to send results to outbox, channel is \
                        closed, exiting",
                    );
                    return TaskAction::Break(());
                }
            }
            Err(reason) => {
                let failure = FailedCollection {
                    started_at,
                    time_queued,
                    time_collecting,
                    reason: reason.to_string(),
                };
                self.details.on_failure(failure);
                self.stats.failures_for_reason(reason).datum.increment();
            }
        }
        TaskAction::Continue(())
    }

    async fn handle_collection_timer_tick(&mut self) -> TaskAction {
        match self.timer_collection_tx.try_send(CollectionStartTimes::new()) {
            Ok(_) => {
                debug!(
                    self.log,
                    "sent timer-based collection request to \
                    the collection task"
                );
            }
            Err(TrySendError::Closed(_)) => {
                error!(
                    self.log,
                    "timer-based collection request queue is \
                    closed, exiting"
                );
                return TaskAction::Break(());
            }
            Err(TrySendError::Full(start)) => {
                let failure = FailedCollection {
                    started_at: start.started_at,
                    time_queued: Duration::ZERO,
                    time_collecting: Duration::ZERO,
                    reason: String::from("collections in progress"),
                };
                self.details.on_failure(failure);
                error!(
                    self.log,
                    "timer-based collection request queue is \
                    full! This may indicate that the producer \
                    has a sampling interval that is too fast \
                    for the amount of data it generates";
                    "interval" => ?self.producer_info_tx.borrow().interval,
                );
                self.stats
                    .failures_for_reason(
                        self_stats::FailureReason::CollectionsInProgress,
                    )
                    .datum
                    .increment()
            }
        }
        TaskAction::Continue(())
    }
}
