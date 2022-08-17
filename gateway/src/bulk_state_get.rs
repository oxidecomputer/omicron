// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Implementation to fetch the state of all powered-on SPs, with support for
//! pagination across shorter timeouts than the overall timeout to collect all
//! state.
//!
//! The flow of this process, starting from the client(s), is below. This
//! modules comes into play at step 2.
//!
//! 1. A client requests the state of all SPs. The client provides a timeout for
//!    the overall request (or we provide a default timeout on their behalf),
//!    which specifies the point at which any SPs we believe are on but which we
//!    haven't heard from are classified as "unresponsive".
//!
//! 2. We enter [`BulkSpStateRequests::start()`].
//!    a. Ask our ignition controller for the ignition state of all SPs.
//!    b. We create a [`SpStateRequestId`] (internally a UUID) to identify this
//!       request.
//!    c. We create a set of futures that will be fulfilled with the state of
//!       all SPs. Any offline SPs will be fulfilled immediately. Any online SPs
//!       will have their states retreived via [`Communicator::get_state()`].
//!       Each of these futures is bounded by the timeout from step 1.
//!    d. Once the background task has reported a result for every SP, it enters
//!       a grace period in which the request ID from step 3 is still alive in
//!       memory and can be queried by clients, but there is no more work
//!       happening.
//!    e. Once the grace period ends, the background task purges the data
//!       associated with the request ID and exits.
//!
//! 3. We enter [`BulkSpStateRequests::get()`] with the request ID from
//!    step 2. This allows us to look up the corresponding `ResponseCollector`
//!    and wait for responses. The client endpoint is paginated, and we have the
//!    option of how (or even if) we want to return partial progress early;
//!    currently we choose to implement this via a timeout duration specified in
//!    the gateway configuration.
//!
//! 4. We wait in [`BulkSpStateRequests::get()`] until we hit one of three
//!    cases:
//!
//!    a. We've collected a partial set of responses of the size specified by
//!       the client's page limit
//!    b. We've collected at least one response, and our internal timeout for
//!       returning partial results has elapsed
//!    c. We've collected all responses
//!
//!    In any of these three cases, we return a page token to the client that
//!    includes the request ID, allowing them to fetch the next page (which
//!    enters this process at step 3). In case 4c we should _not_ send a page
//!    token, since we know we've reported the final page, but currently there's
//!    no clean way to handle this with dropshot.
//!
//!    The mechanics for this step are a little messy; see the comments in
//!    [`BulkSpStateRequests::get()`] for details.

use crate::error::Error;
use crate::error::InvalidPageToken;
use futures::StreamExt;
use gateway_messages::IgnitionState;
use gateway_sp_comms::Communicator;
use gateway_sp_comms::Elapsed;
use gateway_sp_comms::FuturesUnorderedImpl;
use gateway_sp_comms::SpIdentifier;
use gateway_sp_comms::Timeout;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::error;
use slog::trace;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::http_entrypoints::SpState;

/// Newtype wrapper around [`Uuid`] for long-running state requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SpStateRequestId(pub(crate) Uuid);

impl slog::Value for SpStateRequestId {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self.0))
    }
}

impl SpStateRequestId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BulkSpStateSingleResult {
    pub(crate) sp: SpIdentifier,
    pub(crate) state: IgnitionState,
    pub(crate) result: Result<SpState, Arc<gateway_sp_comms::error::Error>>,
}

/// Set of results for a single page of collecting state from all SPs.
#[derive(Debug)]
pub(crate) enum BulkStateProgress {
    /// The per-page timeout was reached before all results were collected; more
    /// results will (hopefully) be available later if the client requests the
    /// next page.
    PageTimeoutReached(Vec<BulkSpStateSingleResult>),
    /// The client-supplied limit of results per page was reached before all
    /// results were collected; more results will (presumably) be available
    /// later if the client requests the next page.
    PageLimitReached(Vec<BulkSpStateSingleResult>),
    /// All results have been returned.
    Complete(Vec<BulkSpStateSingleResult>),
}

type RequestsMap = HashMap<SpStateRequestId, Arc<RwLock<ResponseCollector>>>;

#[derive(Debug)]
pub struct BulkSpStateRequests {
    communicator: Arc<Communicator>,
    requests: Arc<Mutex<RequestsMap>>,
    log: Logger,
}

impl BulkSpStateRequests {
    pub(crate) fn new(communicator: Arc<Communicator>, log: &Logger) -> Self {
        Self {
            communicator,
            requests: Arc::default(),
            log: log.new(slog::o!("component" => "BulkSpStateRequests")),
        }
    }

    pub(crate) async fn start(
        &self,
        timeout: Timeout,
        retain_grace_period: Duration,
    ) -> Result<SpStateRequestId, Error> {
        // set up the receiving end of all SP responses
        let collector = Arc::new(RwLock::new(ResponseCollector::default()));
        let id = SpStateRequestId::new();
        self.requests.lock().unwrap().insert(id, Arc::clone(&collector));

        // query ignition controller to find out which SPs are powered on
        let all_sps = self.communicator.get_ignition_state_all().await?;

        // build collection of futures to contact all SPs
        let communicator = Arc::clone(&self.communicator);
        let response_stream = self.communicator.query_all_online_sps(
            &all_sps,
            timeout,
            move |sp| {
                let communicator = Arc::clone(&communicator);
                async move { communicator.get_state(sp).await }
            },
        );

        // spawn the background task. we don't keep a handle to this; we
        // attached timeouts to all the individual requests above, and after
        // they all complete this task is responsible for cleaning up after
        // itself.
        tokio::spawn(wait_for_sp_responses(
            id,
            response_stream,
            Arc::clone(&self.requests),
            collector,
            retain_grace_period,
            self.log.new(slog::o!(
                "request_kind" => "bulk_sp_state_start",
                "request_id" => id,
            )),
        ));

        Ok(id)
    }

    pub(super) async fn get(
        &self,
        id: &SpStateRequestId,
        last_seen: Option<SpIdentifier>,
        timeout: Timeout,
        limit: usize,
    ) -> Result<BulkStateProgress, Error> {
        let log = self.log.new(slog::o!(
            "request_kind" => "bulk_sp_state_get",
            "request_id" => *id,
        ));

        let mut out = Vec::new();

        // Go ahead and create (and pin) the timeout, but we don't actually
        // await it until the loop at the bottom of this function.
        let timeout = tokio::time::sleep_until(timeout.end());
        tokio::pin!(timeout);

        let collector =
            self.requests
                .lock()
                .unwrap()
                .get(id)
                .map(Arc::clone)
                .ok_or(Error::InvalidPageToken(InvalidPageToken::NoSuchId))?;

        // TODO The locking and notification in this method is a little
        // precarious. We really want something like an async condition
        // variable, which currently doesn't exist in tokio; the closest thing
        // it has is `Notify`. Using a `Notify` requires shared ownership, so
        // `collector` holds an `Arc<Notify>` that we can clone. Because
        // `collector` is using `notify_waiters()`, we must guarantee that
        // there's no window between when we check the current list of responses
        // and when we register for notifications in which `collector` could
        // racily push a response that we miss. We do this by only interacting
        // with the notification while holding the lock:
        //
        // * This function only registers for notifications while holding the
        //   lock for reading.
        // * `notify_waiters()` is only called while the lock is held for
        //   writing.
        let (notify, mut skip_results) = {
            let collector = collector.read().unwrap();

            // scan forward until we find the last seen target
            let mut skip_results = 0;
            if let Some(last_seen) = last_seen {
                for (i, result) in collector.received_states.iter().enumerate()
                {
                    if result.sp == last_seen {
                        skip_results = i + 1;
                        trace!(
                            log,
                            "skipping {} responses based on client progress",
                            skip_results
                        );
                        break;
                    }
                }

                if skip_results == 0 {
                    // caller claimed to have seen a target, but we don't have
                    // it (if we did, `skip_results` is >= 1).
                    error!(
                        log,
                        "client reported last seeing {:?}, but it isn't in our list of collected responses",
                        last_seen
                    );
                    return Err(Error::InvalidPageToken(
                        InvalidPageToken::InvalidLastSeenItem,
                    ));
                }

                // go ahead and check to see if we already have enough info to
                // return, even before we clone the `Arc<Notify>` and register
                // for notifications below.
                //
                // this is duplicated in the loop below, but seems likely to be
                // worth it if we think we'll frequently already have all the
                // responses the client wants? we could take this out and the
                // method still works correctly, but then it _always_ has to
                // acquire the read lock on `collector` at least twice.
                match AccumulationStatus::new(
                    &collector.received_states[skip_results..],
                    collector.done,
                    limit,
                    &mut out,
                    &log,
                ) {
                    AccumulationStatus::Complete => {
                        return Ok(BulkStateProgress::Complete(out));
                    }
                    AccumulationStatus::PageLimitReached => {
                        return Ok(BulkStateProgress::PageLimitReached(out));
                    }
                    AccumulationStatus::Partial(n) => {
                        skip_results += n;
                    }
                }
            }

            (Arc::clone(&collector.notify), skip_results)
        };

        // Our current page timeout logic is very simple: We're passed a timeout
        // for this page, and we return once we're past the timeout _and_ we
        // have at least one result. We can't return when we hit the timeout if
        // we don't have any results yet, because then dropshot will think we're
        // past the end of all pages, and will not give our caller a page token.
        //
        // We track whether we're past the timeout via `timeout.is_some()`.
        let mut timeout = Some(timeout);

        loop {
            // This is a bit of a wart - if this is the first iteration through
            // the loop, we _just_ released the lock on `collector`, and now we
            // immediately reacquire, check for any new responses (certainly
            // possible but presumably not the common case), and register for a
            // notification. I haven't figured out a way around this that
            // appeases the borrow checker - we can't call `notify.notified()`
            // in the block above where we clone `notify`, because it moves
            // `notify` out into the parent scope, which would invalidate any
            // references to it (i.e., the ref contained in the future returned
            // by `notify.notified()`).
            let notified = {
                let collector = collector.read().unwrap();

                match AccumulationStatus::new(
                    &collector.received_states[skip_results..],
                    collector.done,
                    limit,
                    &mut out,
                    &log,
                ) {
                    AccumulationStatus::Complete => {
                        return Ok(BulkStateProgress::Complete(out));
                    }
                    AccumulationStatus::PageLimitReached => {
                        return Ok(BulkStateProgress::PageLimitReached(out));
                    }
                    AccumulationStatus::Partial(n) => {
                        skip_results += n;
                    }
                }

                // We must register for a notification while we're holding the
                // read lock on `collector`; this guarantees we won't miss any
                // elements pushed into it between when we unlock and when we
                // register for notifications
                notify.notified()
            };

            if let Some(pate_timeout) = timeout.as_mut() {
                trace!(log, "waiting on either timeout or notify");
                tokio::select! {
                    _ = pate_timeout => {
                        if out.is_empty() {
                            // we hit the timeout but don't have any results
                            // yet; we'll keep going until we get at least one
                            // result (which themselves have a timeout as
                            // specified when this request was initially created
                            // - we're now dependent on that timeout to
                            // complete)
                            timeout = None;
                        } else {
                            return Ok(BulkStateProgress::PageTimeoutReached(
                                out,
                            ));
                        }
                    }
                    _ = notified => (), // nothing to do, just repeat
                }
            } else {
                trace!(log, "past timeout but no results yet - waiting for at least one");
                notified.await;
            }
        }
    }
}

type SpStateResult =
    Result<gateway_messages::SpState, gateway_sp_comms::error::Error>;

async fn wait_for_sp_responses<S>(
    id: SpStateRequestId,
    mut response_stream: S,
    requests: Arc<Mutex<RequestsMap>>,
    collector: Arc<RwLock<ResponseCollector>>,
    retain_grace_period: Duration,
    log: Logger,
) where
    S: FuturesUnorderedImpl<
        Item = (
            SpIdentifier,
            IgnitionState,
            Option<Result<SpStateResult, Elapsed>>,
        ),
    >,
{
    while let Some((sp, state, result)) = response_stream.next().await {
        let mut collector = collector.write().unwrap();

        // Unpack the nasty nested type:
        // 1. None => ignition indicated power was off; treat that as success
        //    (with state = disabled)
        // 2. Outer err => timeout; treat that as "success" (with state =
        //    unresponsive)
        // 3. Inner success => true success
        // 4. Inner error => wrap in an `Arc` so we can clone it
        let result = match result {
            None => Ok(SpState::Disabled),
            Some(Err(_)) => Ok(SpState::Unresponsive),
            Some(Ok(result)) => match result {
                Ok(state) => Ok(SpState::from(state)),
                Err(err) => Err(Arc::new(err)),
            },
        };
        collector.push(BulkSpStateSingleResult { sp, state, result });

        // this is a little goofy, but we don't want to put it after the loop
        // and have to reacquire the write lock; we want clients of `collector`
        // to know if the list is done as soon as they wake up.
        if response_stream.is_empty() {
            collector.done = true;
        }
    }

    // At this point we've reported the status for every SP, either because we
    // know the actual status or because it timed out. We now enter the grace
    // period where we keep this request in memory so clients can continue to
    // ask for additional pages.
    //
    // In the event that we get here _earlier_ than the client-requested
    // timeout (i.e., because we heard back from all the SPs and none timed
    // out), we ignore the client timeout and jump immediately to staying alive
    // for our internal post-completion grace period.

    debug!(log, "all responses collected; starting grace period");
    tokio::time::sleep(retain_grace_period).await;

    debug!(log, "grace period elapsed; dropping request");
    requests.lock().unwrap().remove(&id);
}

#[derive(Debug, Default)]
struct ResponseCollector {
    received_states: Vec<BulkSpStateSingleResult>,
    notify: Arc<Notify>,
    done: bool,
}

impl ResponseCollector {
    fn push(&mut self, result: BulkSpStateSingleResult) {
        self.received_states.push(result);
        self.notify.notify_waiters();
    }
}

enum AccumulationStatus {
    Complete,
    PageLimitReached,
    Partial(usize),
}

impl AccumulationStatus {
    fn new(
        mut received: &[BulkSpStateSingleResult],
        mut done: bool,
        limit: usize,
        out: &mut Vec<BulkSpStateSingleResult>,
        log: &Logger,
    ) -> Self {
        // cap the number of results to the limit, if necessary
        let space_remaining = limit.saturating_sub(out.len());
        if received.len() > space_remaining {
            received = &received[..space_remaining];

            // if received _had_ all the responses in it, it doesn't anymore.
            done = false;
        }

        // actually accumulate results
        out.extend_from_slice(received);

        if done {
            debug!(log, "all responses received");
            Self::Complete
        } else if out.len() == limit {
            debug!(log, "received {} responses (hit client page limit)", limit);
            Self::PageLimitReached
        } else {
            trace!(
                log,
                "collected {} responses; waiting for more",
                received.len(),
            );
            Self::Partial(received.len())
        }
    }
}
