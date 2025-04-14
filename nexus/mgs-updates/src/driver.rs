// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Drive one or more in-progress MGS-managed updates

use crate::ArtifactCache;
use crate::driver_update::ApplyUpdateError;
use crate::driver_update::ApplyUpdateResult;
use crate::driver_update::SpComponentUpdate;
use crate::driver_update::apply_update;
use crate::sp_updater::ReconfiguratorSpUpdater;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use gateway_client::SpComponent;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::inventory::BaseboardId;
use qorb::resolver::AllBackends;
use slog::{error, info, o, warn};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue;
use uuid::Uuid;

/// How many recent completions to keep track of (for debugging)
const N_RECENT_COMPLETIONS: usize = 16;

/// Drive one or more MGS-managed updates
///
/// Use [`MgsUpdateDriver::new()`] to create a new one of these.  You configure
/// the set of updates that should be driven by writing to its watch channel.
/// Use [`MgsUpdateDriver::status_rx()`] to get a `watch::Receiver` where you
/// can check on the status of updates being managed by this driver.  Use
/// [`MgsUpdateDriver::run()`] to drive updates.
///
/// - If a requested update is added to the channel, an attempt will be made to
///   apply the update promptly.
/// - For each update attempt:
///   - If the live system state reflects that the update has been completed,
///     then no action is taken.
///   - If the live system state reflects that the preconditions aren't true
///     (e.g., the device is currently running from a different slot than
///     expected), then no action is taken.
///   - If the live system state reflects that an update is already in progress,
///     we'll wait for that one to complete.
///   - If the process appears to be stuck (whether this driver is running it or
///     a different one is), it will be aborted.  This is really only intended
///     to catch pathological cases like a partitioned Nexus.
///   - Once the update attempt completes, regardless of the outcome, it will be
///     tried again later.
/// - If an update is ongoing when it gets removed from the channel, it is *not*
///   cancelled.  It will run to completion.  But it will not be restarted again
///   even if it fails.
///
/// It's assumed that other instances may exist running the same process.  We
/// may find their updates in progress, we may need to cancel them if they
/// appear stuck, and ours may get cancelled at any point.  These are presumed
/// to be very unlikely.
pub struct MgsUpdateDriver {
    // helpers
    log: slog::Logger,
    /// source of artifacts used for updates
    artifacts: Arc<ArtifactCache>,
    /// dynamically-changing set of MGS backends (provided by qorb)
    mgs_rx: watch::Receiver<AllBackends>,

    // inputs
    /// set of updates requested by our consumer
    requests_rx: watch::Receiver<PendingMgsUpdates>,
    /// how long to wait between attempts (successful or otherwise)
    retry_timeout: Duration,

    // outputs
    /// status of updates we're working on or recently finished
    status_tx: watch::Sender<DriverStatus>,

    // internal state tracking
    /// holds the futures that are each performing one update attempt
    futures: FuturesUnordered<BoxFuture<'static, UpdateAttemptResult>>,
    /// tracks the next timer we're waiting on for retries
    delayq: DelayQueue<Arc<BaseboardId>>,

    /// tracks update attempts that are in-progress right now
    in_progress: BTreeMap<Arc<BaseboardId>, InProgressUpdate>,
    /// tracks update attempts that are not running right now
    /// (but waiting for a retry)
    waiting: BTreeMap<Arc<BaseboardId>, WaitingAttempt>,
}

impl MgsUpdateDriver {
    pub fn new(
        log: slog::Logger,
        artifacts: Arc<ArtifactCache>,
        requests_rx: watch::Receiver<PendingMgsUpdates>,
        mgs_rx: watch::Receiver<AllBackends>,
        retry_timeout: Duration,
    ) -> MgsUpdateDriver {
        let (status_tx, _) = watch::channel(DriverStatus {
            recent: VecDeque::with_capacity(N_RECENT_COMPLETIONS),
            in_progress: BTreeMap::new(),
            waiting: BTreeMap::new(),
        });

        MgsUpdateDriver {
            log,
            artifacts,
            mgs_rx,
            requests_rx,
            retry_timeout,
            status_tx,
            futures: FuturesUnordered::new(),
            delayq: DelayQueue::new(),
            in_progress: BTreeMap::new(),
            waiting: BTreeMap::new(),
        }
    }

    /// Returns a `watch::Receiver` that you can use to inspect the state of
    /// in-progress, waiting, and recently completed update attempts.
    pub fn status_rx(&self) -> watch::Receiver<DriverStatus> {
        self.status_tx.subscribe()
    }

    /// Runs the driver
    ///
    /// You generally want to run this in its own tokio task.  This will not
    /// return until one of the input channels has closed.
    pub async fn run(mut self) {
        info!(&self.log, "starting MgsUpdateDriver");
        loop {
            tokio::select! {
                // See if we've received an updated configuration.
                maybe_update = self.requests_rx.changed() => {
                    match maybe_update {
                        Ok(()) => {
                            self.on_config_changed();
                        }
                        Err(error) => {
                            info!(
                                &self.log,
                                "shutting down \
                                 (failed to read from input channel)";
                                InlineErrorChain::new(&error)
                            );
                            break;
                        }
                    }
                }

                // See if any update attempts have completed.
                //
                // Avoid waiting on an empty FuturesUnordered.  Doing so would
                // cause it to immediately return None, terminating the Stream
                // altogether.
                maybe_work_done = self.futures.next(),
                    if !self.futures.is_empty() => {
                    match maybe_work_done {
                        Some(result) => self.on_attempt_done(result),
                        None => {
                            error!(
                                &self.log,
                                "FutureUnordered unexpectedly ended"
                            );
                            break;
                        }
                    };
                },

                // See if the timer has fired for any update awaiting retry.
                maybe_timer_expired = self.delayq.next(),
                    if !self.delayq.is_empty() => {
                    match maybe_timer_expired {
                        Some(expired) => {
                            let baseboard_id = expired.into_inner();
                            self.on_retry_timer_expired(baseboard_id);
                        },
                        None => {
                            error!(
                                &self.log,
                                "DelayQueue unexpectedly ended"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Examines the configuration and decides what work needs to be kicked off.
    fn on_config_changed(&mut self) {
        // We'll take two passes:
        //
        // 1. Look at each request in the configuration and decide what to do
        //    with each one.
        // 2. Take the appropriate action for each one.
        //
        // Importantly, we can drop the config (unblocking the "watch" channel
        // and also dropping a shared reference on `self`) after pass 1.

        let (to_stop_waiting, to_dispatch) = {
            let new_config = self.requests_rx.borrow_and_update();

            // Stop waiting to retry any requests whose config has changed or
            // been removed,
            let to_stop_waiting: Vec<_> = self
                .waiting
                .iter()
                .filter_map(|(baseboard_id, waiting)| {
                    match new_config.get(baseboard_id) {
                        None => true,
                        Some(new_request) => {
                            *new_request != waiting.internal_request.request
                        }
                    }
                    .then(|| baseboard_id.clone())
                })
                .collect();

            // Dispatch new requests if either:
            //
            // - we're waiting to retry and the config has changed
            //   (overlaps with the case above)
            // - we're not waiting to retry and not already running an attempt
            let to_dispatch: Vec<_> = new_config
                .iter()
                .filter_map(|(baseboard_id, new_request)| {
                    let do_dispatch =
                        if let Some(waiting) = self.waiting.get(baseboard_id) {
                            *new_request != waiting.internal_request.request
                        } else {
                            !self.in_progress.contains_key(baseboard_id)
                        };
                    do_dispatch.then(|| InternalRequest {
                        request: (*new_request).clone(),
                        nattempts_done: 0,
                    })
                })
                .collect();

            (to_stop_waiting, to_dispatch)
        };

        // Process the requests for which we've decided to stop waiting.
        for baseboard_id in &to_stop_waiting {
            // Update our bookkeeping.
            // unwrap(): we filtered on this condition above.
            let waiting = self.waiting.remove(baseboard_id).unwrap();

            // Stop tracking this timeout.
            self.delayq.remove(&waiting.delay_key);
        }
        // Update the status to reflect that.
        self.status_tx.send_modify(|driver_status| {
            for baseboard_id in &to_stop_waiting {
                driver_status.waiting.remove(baseboard_id);
            }
        });

        // Now dispatch new update attempts.
        for internal_request in to_dispatch {
            self.start_attempt(internal_request);
        }
    }

    fn start_attempt(&mut self, internal_request: InternalRequest) {
        let request = &internal_request.request;
        let baseboard_id = &request.baseboard_id;
        assert!(!self.in_progress.contains_key(baseboard_id));

        let update_id = Uuid::new_v4();
        let log = self.log.new(o!(
            request.clone(),
            "update_id" => update_id.to_string()
        ));
        info!(&log, "begin update attempt for baseboard");

        let (sp_update, updater) = match &request.details {
            nexus_types::deployment::PendingMgsUpdateDetails::Sp { .. } => {
                let sp_update = SpComponentUpdate {
                    log: log.clone(),
                    component: SpComponent::SP_ITSELF,
                    target_sp_type: request.sp_type,
                    target_sp_slot: request.slot_id,
                    // The SP has two firmware slots, but they're aren't
                    // individually labeled. We always request an update to slot
                    // 0, which (confusingly in this context) means "the
                    // inactive slot".
                    firmware_slot: 0,
                    update_id,
                };

                (sp_update, Box::new(ReconfiguratorSpUpdater {}))
            }
        };

        let baseboard_id = baseboard_id.clone();
        let status_updater = UpdateAttemptStatusUpdater {
            tx: self.status_tx.clone(),
            baseboard_id: baseboard_id.clone(),
        };
        let artifacts = self.artifacts.clone();
        let mgs_rx = self.mgs_rx.clone();
        let request = internal_request.request.clone();
        let future = async move {
            let result = apply_update(
                artifacts,
                &sp_update,
                &*updater,
                mgs_rx,
                &request,
                status_updater,
            )
            .await;
            UpdateAttemptResult { baseboard_id: request.baseboard_id, result }
        }
        .boxed();

        let nattempts_done = internal_request.nattempts_done;
        let in_progress = InProgressUpdate {
            log: log.clone(),
            time_started: chrono::Utc::now(),
            instant_started: Instant::now(),
            internal_request,
        };

        // Keep track of the work.
        self.futures.push(future);

        // Update status.
        self.status_tx.send_modify(|driver_status| {
            driver_status.in_progress.insert(
                baseboard_id.clone(),
                InProgressUpdateStatus {
                    time_started: in_progress.time_started,
                    instant_started: in_progress.instant_started,
                    status: UpdateAttemptStatus::NotStarted,
                    nattempts_done,
                },
            );
        });

        // Update our bookkeeping.
        assert!(
            self.in_progress
                .insert(baseboard_id.clone(), in_progress)
                .is_none()
        );
    }

    /// Invoked when an in-progress update attempt has completed.
    fn on_attempt_done(&mut self, result: UpdateAttemptResult) {
        // Load the in-progress state and generate a CompletedAttempt status for
        // this attempt.
        let in_progress = self
            .in_progress
            .remove(&result.baseboard_id)
            .expect("in-progress record for attempt that just completed");
        let nattempts_done = in_progress.internal_request.nattempts_done + 1;
        let completed = CompletedAttempt {
            time_started: in_progress.time_started,
            time_done: chrono::Utc::now(),
            elapsed: in_progress.instant_started.elapsed(),
            request: in_progress.internal_request.request.clone(),
            result: match &result.result {
                Ok(success) => Ok(success.clone()),
                Err(error) => Err(InlineErrorChain::new(error).to_string()),
            },
            nattempts_done,
        };
        let internal_request = InternalRequest {
            request: in_progress.internal_request.request,
            nattempts_done,
        };

        // Log the result.
        match &completed.result {
            Ok(success) => {
                info!(
                    &in_progress.log,
                    "update attempt done";
                    "elapsed_millis" => completed.elapsed.as_millis(),
                    "result" => ?success,
                );
            }
            Err(error) => {
                info!(
                    &in_progress.log,
                    "update attempt done";
                    "elapsed_millis" => completed.elapsed.as_millis(),
                    "error" => error,
                )
            }
        };

        // Regardless of the result, set a timer for retrying.  Our job is to
        // ensure reality matches our configuration, so even if we succeeded, we
        // want to check again in a little while to see if anything has changed.
        let baseboard_id = completed.request.baseboard_id.clone();
        let retry_timeout = self.retry_timeout;
        let status_time_next = chrono::Utc::now() + retry_timeout;
        let delay_key = self.delayq.insert(baseboard_id.clone(), retry_timeout);
        self.waiting.insert(
            baseboard_id.clone(),
            WaitingAttempt { delay_key, internal_request },
        );

        // Update the overall status to reflect all these changes.
        self.status_tx.send_modify(|driver_status| {
            // Remove this item from the list of in-progress attempts.
            let found = driver_status.in_progress.remove(&baseboard_id);
            assert!(found.is_some());

            // Add this item to the list of requests waiting to be retried.
            driver_status.waiting.insert(
                baseboard_id.clone(),
                WaitingStatus {
                    next_attempt_time: status_time_next,
                    nattempts_done,
                },
            );

            // Report this recently-completed attempt.
            // This is a ringbuffer of recent attempts.  Make space if we're
            // already at the capacity.
            let recent = &mut driver_status.recent;
            if recent.len() == recent.capacity() {
                let _ = recent.pop_front();
            }
            recent.push_back(completed);
        });
    }

    /// Invoked when the timer fires to retry a particular request.
    fn on_retry_timer_expired(&mut self, baseboard_id: Arc<BaseboardId>) {
        // Remove this request from the set of requests waiting to be retried.
        let waiting = self
            .waiting
            .remove(&baseboard_id)
            .expect("waiting request for expired retry timer");
        // Update the external status to reflect that.
        self.status_tx.send_modify(|driver_status| {
            driver_status.waiting.remove(&baseboard_id);
        });

        // Find the current configuration for this request.
        //
        // It generally ought to be here because if it got removed, we'd have
        // gotten a notification and cancelled the retry timer.  But it's
        // conceivable that the retry timer fired, then it was removed from the
        // configuration (concurrently), and we just noticed that before having
        // processed the change notification.
        let my_request = {
            let current_config = self.requests_rx.borrow();
            let Some(my_request) = current_config.get(&baseboard_id).cloned()
            else {
                warn!(
                    &self.log,
                    "attempted retry of baseboard whose update is no longer \
                     configured";
                     &*baseboard_id
                );
                return;
            };

            my_request
        };

        // Dispatch another attempt.
        info!(
            &self.log,
            "dispatching new attempt (retry timer expired)"; &*baseboard_id
        );
        self.start_attempt(InternalRequest {
            request: my_request,
            nattempts_done: waiting.internal_request.nattempts_done,
        });
    }
}

/// information tracked for each request
struct InternalRequest {
    request: PendingMgsUpdate,
    nattempts_done: u32,
}

/// internal bookkeeping for each in-progress update
struct InProgressUpdate {
    log: slog::Logger,
    time_started: chrono::DateTime<chrono::Utc>,
    instant_started: Instant,
    internal_request: InternalRequest,
}

/// internal result of a completed update attempt
struct UpdateAttemptResult {
    baseboard_id: Arc<BaseboardId>,
    result: Result<ApplyUpdateResult, ApplyUpdateError>,
}

/// internal bookkeeping for an update that's awaiting retry
struct WaitingAttempt {
    delay_key: delay_queue::Key,
    internal_request: InternalRequest,
}

/// Interface used by update attempts to update just their part of the overall
/// `DriverStatus`
pub(crate) struct UpdateAttemptStatusUpdater {
    tx: watch::Sender<DriverStatus>,
    baseboard_id: Arc<BaseboardId>,
}

impl UpdateAttemptStatusUpdater {
    pub(crate) fn update(&self, new_status: UpdateAttemptStatus) {
        self.tx.send_modify(|driver_status| {
            // unwrap(): this UpdateAttemptStatusUpdater's lifetime is bound by
            // the future that owns it.  The status entry for this future lives
            // in the `in_progress` struct until it completes.  Thus, we should
            // always have a value here.
            let my_status =
                driver_status.in_progress.get_mut(&self.baseboard_id).unwrap();
            my_status.status = new_status;
        });
    }
}

// Externally-visible status

/// Status of ongoing update attempts, recently completed attempts, and update
/// requests that are waiting for retry.
#[derive(Debug)]
pub struct DriverStatus {
    pub recent: VecDeque<CompletedAttempt>,
    pub in_progress: BTreeMap<Arc<BaseboardId>, InProgressUpdateStatus>,
    pub waiting: BTreeMap<Arc<BaseboardId>, WaitingStatus>,
}

/// externally-exposed status for a completed attempt
#[derive(Debug)]
pub struct CompletedAttempt {
    pub time_started: chrono::DateTime<chrono::Utc>,
    pub time_done: chrono::DateTime<chrono::Utc>,
    pub elapsed: Duration,
    pub request: PendingMgsUpdate,
    pub result: Result<ApplyUpdateResult, String>,
    pub nattempts_done: u32,
}

/// externally-exposed status for each in-progress update
#[derive(Debug)]
pub struct InProgressUpdateStatus {
    pub time_started: chrono::DateTime<chrono::Utc>,
    pub instant_started: std::time::Instant,
    pub status: UpdateAttemptStatus,
    pub nattempts_done: u32,
}

/// status of a single update attempt
#[derive(Clone, Debug)]
pub enum UpdateAttemptStatus {
    NotStarted,
    FetchingArtifact,
    Precheck,
    Updating,
    UpdateWaiting,
    PostUpdate,
    PostUpdateWait,
    Done,
}

/// externally-exposed status for waiting updates
#[derive(Debug)]
pub struct WaitingStatus {
    pub next_attempt_time: chrono::DateTime<chrono::Utc>,
    pub nattempts_done: u32,
}
