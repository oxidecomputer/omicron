// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task responsible for collecting from a single producer.

// Copyright 2025 Oxide Computer Company

use crate::agent::CollectionTaskSenderWrapper;
use crate::self_stats;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use oximeter::types::ProducerResults;
use oximeter::types::ProducerResultsItem;
use oximeter_api::FailedCollection;
use oximeter_api::ProducerDetails;
use oximeter_api::SuccessfulCollection;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::o;
use slog::trace;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio::time::Interval;
use tokio::time::interval;

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

// Messages for controlling a collection task.
#[derive(Debug)]
enum CollectionMessage {
    // Explicit request that the task collect data from its producer
    ForceCollect,
    // Return the current statistics from a single task.
    #[cfg(test)]
    Statistics {
        reply_tx: oneshot::Sender<self_stats::CollectionTaskStats>,
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
    mut shutdown: oneshot::Receiver<()>,
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
            _ = &mut shutdown => {
                debug!(
                    log,
                    "collection task asked to shutdown, exiting",
                );
                return;
            }
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

                _ = &mut shutdown => {
                    debug!(
                        log,
                        "collection task asked to shutdown, exiting",
                    );
                    return;
                }
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
#[derive(Debug, Clone)]
pub(crate) struct CollectionTaskOutput {
    pub(crate) was_forced_collection: bool,
    pub(crate) results: ProducerResults,
}

/// Handle to the task which collects metric data from a single producer.
#[derive(Debug)]
pub struct CollectionTaskHandle {
    // Notification mechanisms used to control the actual collection task.
    notifiers: CollectionTaskNotifiers,
    log: Logger,
}

impl CollectionTaskHandle {
    /// Create a new collection task handle.
    ///
    /// This spawns the actual task itself, and returns a handle to it. The
    /// latter is used to send messages to the task, through the handle's
    /// `inbox` field.
    pub fn new(
        log: &Logger,
        collector: self_stats::OximeterCollector,
        producer: ProducerEndpoint,
        outbox: CollectionTaskSenderWrapper,
    ) -> Self {
        let (task, notifiers) =
            CollectionTask::new(log, collector, producer, outbox);
        tokio::spawn(task.run());
        let log = log.new(o!(
            "component" => "collection-task-handle",
            "producer_id" => producer.id.to_string(),
        ));
        Self { notifiers, log }
    }

    /// Notify the task to update its producer endpoint information.
    pub fn update(&mut self, new_info: ProducerEndpoint) {
        let updated_producer_info = |info: &mut ProducerEndpoint| {
            if new_info == *info {
                false
            } else {
                *info = new_info;
                true
            }
        };
        if !self
            .notifiers
            .producer_info_tx
            .send_if_modified(updated_producer_info)
        {
            trace!(
                self.log,
                "collection task handle received update with \
                identical producer information, no \
                updates will be sent to the collection task"
            );
            return;
        }
        debug!(self.log, "notified collection task of new producer endpoint");
    }

    /// Ask the collection task to shutdown.
    pub fn shutdown(self) {
        if self.notifiers.shutdown.send(()).is_err() {
            warn!(self.log, "failed to notify collection task to shut down");
        } else {
            debug!(self.log, "notified collection task to shut down");
        }
    }

    /// Return a receiver to fetch current statistics from this task.
    #[cfg(test)]
    pub fn statistics(
        &self,
    ) -> oneshot::Receiver<self_stats::CollectionTaskStats> {
        let (reply_tx, rx) = oneshot::channel();
        self.notifiers
            .task_tx
            .try_send(CollectionMessage::Statistics { reply_tx })
            .expect("Failed to send statistics message");
        rx
    }

    /// Return details about the current producer this task is collecting from.
    pub fn details(&self) -> ProducerDetails {
        self.notifiers.producer_details_rx.borrow().clone()
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
        self.notifiers
            .task_tx
            .try_send(CollectionMessage::ForceCollect)
            .map_err(|err| match err {
                TrySendError::Full(_) => ForcedCollectionError::QueueFull,
                TrySendError::Closed(_) => ForcedCollectionError::Closed,
            })
    }

    /// Return the current producer endpoint information
    pub(crate) fn producer_info(&self) -> ProducerEndpoint {
        *self.notifiers.producer_info_tx.borrow()
    }
}

/// Helper type used to simplify control flow in the main `CollectionTask::run`
/// method.
type TaskAction = std::ops::ControlFlow<()>;

/// Notification mechanisms for controlling the actual collection task from the
/// task handle.
#[derive(Debug)]
pub(crate) struct CollectionTaskNotifiers {
    /// Notify the collection task to shutdown.
    pub shutdown: oneshot::Sender<()>,
    /// Sender to notify the collection task about changes to the producer's
    /// endpoint information.
    pub producer_info_tx: watch::Sender<ProducerEndpoint>,
    /// Receiver from which to read updated collection details from the task.
    pub producer_details_rx: watch::Receiver<ProducerDetails>,
    /// Channel used to send test / debugging messages to the task.
    task_tx: mpsc::Sender<CollectionMessage>,
}

/// Main task used to dispatch messages from the oximeter agent and request
/// collections from the producer.
#[derive(Debug)]
struct CollectionTask {
    log: Logger,

    // Channel used to update details about past collections from this producer.
    producer_details_tx: watch::Sender<ProducerDetails>,

    // Channel used to receive notifications of changes to the producer
    // endpoint.
    producer_info_rx: watch::Receiver<ProducerEndpoint>,

    // Statistics about all collections we've made so far.
    stats: self_stats::CollectionTaskStats,

    // Inbox for messages from the controlling task handle.
    inbox: mpsc::Receiver<CollectionMessage>,

    // Channel for sending forced collection requests.
    forced_collection_tx: mpsc::Sender<ForcedCollectionRequest>,

    // Channel for sending timer-based collection requests.
    timer_collection_tx: mpsc::Sender<CollectionStartTimes>,

    // Channel for receiving collection responses from the inner collection
    // loop.
    result_rx: mpsc::Receiver<CollectionResponse>,

    // Outbox for forwarding the results to the sink.
    outbox: CollectionTaskSenderWrapper,

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
    fn new(
        log: &Logger,
        collector: self_stats::OximeterCollector,
        producer: ProducerEndpoint,
        outbox: CollectionTaskSenderWrapper,
    ) -> (Self, CollectionTaskNotifiers) {
        // Create our own logger.
        let log = log.new(o!(
            "component" => "collection-task",
            "producer_id" => producer.id.to_string(),
        ));

        // Watch channel for changes to the producer's endpoint information.
        //
        // Updates to the producer information are made through the
        // `CollectionTaskHandle`. Both the `CollectionTask` and inner
        // `collection_loop` watch for changes to that.
        let (producer_info_tx, producer_info_rx) = watch::channel(producer);

        // Watch channel for changes to the collection details.
        //
        // The interior spawned task running `collection_loop` maintains the
        // sender. We return the receiver to be held by the
        // `CollectionTaskHandle`. The collection task updates things like the
        // number of successful collections through this channel.
        let (producer_details_tx, producer_details_rx) =
            watch::channel(ProducerDetails::new(&producer));

        // Channel for sending `CollectionMessages`, which are only used in test
        // settings.
        let (task_tx, inbox) = mpsc::channel(N_QUEUED_TASK_MESSAGES);

        // Channel for notifying the inner `collection_loop` to collect
        // on-demand...
        let (forced_collection_tx, forced_collection_rx) =
            mpsc::channel(N_QUEUED_FORCED_COLLECTIONS);

        // ...Or when the timer expires.
        let (timer_collection_tx, timer_collection_rx) =
            mpsc::channel(N_QUEUED_TIMER_COLLECTIONS);

        // Channel on which the `collection_loop` sends collection results. This
        // `CollectionTask` forwards them to the database insertion tasks, after
        // updating self-stats and last-collection details.
        let (result_tx, result_rx) = mpsc::channel(N_QUEUED_RESULTS);

        // Mechanism to notify the inner `collection_loop` to exit.
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Task running the actual collections in a loop.
        tokio::task::spawn(collection_loop(
            log.clone(),
            shutdown_rx,
            producer_info_rx.clone(),
            forced_collection_rx,
            timer_collection_rx,
            result_tx,
        ));

        // Construct ourself, and return our controlling input queue.
        let notifiers = CollectionTaskNotifiers {
            shutdown: shutdown_tx,
            producer_details_rx,
            producer_info_tx,
            task_tx,
        };

        // Construct self-collection statistics and our collection times.
        let stats = self_stats::CollectionTaskStats::new(collector, &producer);
        let collection_timer = interval(producer.interval);
        let self_collection_timer = interval(self_stats::COLLECTION_INTERVAL);
        let self_ = Self {
            log,
            producer_details_tx,
            producer_info_rx,
            stats,
            inbox,
            outbox,
            forced_collection_tx,
            timer_collection_tx,
            result_rx,
            collection_timer,
            self_collection_timer,
        };
        (self_, notifiers)
    }

    /// Run the main loop of this collection task.
    ///
    /// NOTE: This returns a `TaskAction`, but the value isn't used. It returns
    /// that value to simplify control-flow internally, which uses `?` to
    /// propagate the `TaskAction::Break` variant when we need to exit.
    async fn run(mut self) -> TaskAction {
        loop {
            tokio::select! {
                res = self.producer_info_rx.changed() => {
                    if res.is_err() {
                        error!(
                            self.log,
                            "producer info sender dropped!",
                        );
                        return TaskAction::Break(());
                    };
                    self.update_producer_info().await;
                }
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
                    }, &self.log).await.unwrap();
                }
                _ = self.collection_timer.tick() => {
                    self.handle_collection_timer_tick().await?;
                }
            }
        }
    }

    // Rebuild our timer to reflect the possibly-new collection interval, and
    // update the self-statistics.
    async fn update_producer_info(&mut self) {
        let new_info = *self.producer_info_rx.borrow_and_update();
        debug!(
            self.log,
            "collection task received request to update \
            its producer information";
            "interval" => ?new_info.interval,
            "address" => new_info.address,
        );
        self.producer_details_tx
            .send_modify(|details| details.update(&new_info));
        self.stats.update(&new_info);
        self.collection_timer = interval(new_info.interval);
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
                            self.producer_details_tx.send_modify(|details| {
                                details.on_failure(failure);
                            });
                        }
                    },
                }
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
                self.producer_details_tx
                    .send_modify(|details| details.on_success(success));
                if self
                    .outbox
                    .send(
                        CollectionTaskOutput { was_forced_collection, results },
                        &self.log,
                    )
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
                self.producer_details_tx
                    .send_modify(|details| details.on_failure(failure));
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
                self.producer_details_tx
                    .send_modify(|details| details.on_failure(failure));
                error!(
                    self.log,
                    "timer-based collection request queue is \
                    full! This may indicate that the producer \
                    has a sampling interval that is too fast \
                    for the amount of data it generates";
                    "interval" => ?self.producer_details_tx.borrow().interval,
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
