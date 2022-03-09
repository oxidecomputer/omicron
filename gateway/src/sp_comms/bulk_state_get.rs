// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Implementation to fetch the state of all powered-on SPs, with support for
//! pagination across shorter timeouts than the overall timeout to collect all
//! state.
//!
//! The flow of this process, starting from the client(s), is below. This
//! modules comes into play at step 3.
//!
//! 1. A client requests the state of all SPs. They provide a timeout for the
//!    overall request (or we provide a default timeout on their behalf), which
//!    specifies the point at which any SPs we believe are on but which we
//!    haven't heard from are classified as "unresponsive".
//!
//! 2. We ask our ignition controller for the ignition state of all SPs.
//!
//! 3. We enter `OutstandingSpStateRequests::start()`, which creates a
//!    `ResponseCollector` (identified by an `SpStateRequestId`) and spawns a
//!    tokio task responsible for populating it.
//!    a. The background task will immediately populate results for any SPs the
//!       ignition controller reported were off. For any SPs the ignition
//!       controller reported were on, it will ask them for their state and wait
//!       for a response (up to the timeout from step 1).
//!    b. Once the background task has reported a result for every SP, it enters
//!       a grace period in which the request ID from step 3 is still alive in
//!       memory and can be queried by clients, but there is no more work
//!       happening.
//!    c. Once the grace period ends, the background task purges the data
//!       associated with the request ID and exits.
//!
//! 4. We enter `OutstandingSpStateRequests::get()` with the request ID from
//!    step 3. This allows us to look up the corresponding `ResponseCollector`
//!    and wait for responses. The client endpoint is paginated, and we have the
//!    option of how (or even if) we want to return partial progress early;
//!    currently we choose to implement this via a timeout duration specified in
//!    the gateway configuration.
//!
//! 5. We wait in `OutstandingSpStateRequests::get()` until we hit one of three
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
//!    enters this process at step 4). In case 5c we should _not_ send a page
//!    token, since we know we've reported the final page, but currently there's
//!    no clean way to handle this with dropshot.
//!
//!    The mechanics for this step are a little messy; see the comments in
//!    `OutstandingSpStateRequests::get()` for details.

use super::Error;
use super::SpCommunicator;
use crate::http_entrypoints::SpState;
use futures::future;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;
use gateway_messages::{IgnitionFlags, IgnitionState};
use serde::{Deserialize, Serialize};
use slog::debug;
use slog::trace;
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

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

#[derive(Debug, Default)]
pub(super) struct OutstandingSpStateRequests {
    requests: Arc<Mutex<RequestsMap>>,
}

impl OutstandingSpStateRequests {
    pub(super) fn start(
        &self,
        timeout: Instant,
        retain_grace_period: Duration,
        sps: impl Iterator<Item = (usize, IgnitionState, Option<SocketAddr>)>,
        communicator: &Arc<SpCommunicator>,
    ) -> SpStateRequestId {
        // set up the receiving end of all SP responses
        let collector = Arc::new(RwLock::new(ResponseCollector::default()));
        let id = SpStateRequestId::new();
        self.requests.lock().unwrap().insert(id, Arc::clone(&collector));

        // build collection of futures to contact all SPs
        let futures = sps
            .map(move |(target, state, addr)| {
                let communicator = Arc::clone(communicator);
                async move {
                    // only query the SP if it's powered on and we know its
                    // address
                    //
                    // TODO is `addr == None` even meaningful? this is dependent
                    // on how we end up mapping topology / management network
                    // ports, so leaving this as-is for now.
                    let fut = match (
                        state.flags.intersects(IgnitionFlags::POWER),
                        addr,
                    ) {
                        (true, Some(addr)) => Either::Left(
                            communicator.state_get(addr, timeout).map(
                                move |result| BulkSpStateSingleResult {
                                    target,
                                    state,
                                    result,
                                },
                            ),
                        ),
                        // either powered off or we don't have an address; mark
                        // it as disabled
                        _ => Either::Right(future::ready(
                            BulkSpStateSingleResult {
                                target,
                                state,
                                result: Ok(SpState::Disabled),
                            },
                        )),
                    };
                    fut.await
                }
            })
            .collect::<FuturesUnordered<_>>();

        // spawn the background task. we don't keep a handle to this; we
        // attached timeouts to all the individual requests above, and after
        // they all complete this task is responsible for cleaning up after
        // itself.
        tokio::spawn(wait_for_sp_responses(
            id,
            futures,
            Arc::clone(&self.requests),
            collector,
            retain_grace_period,
            communicator.log.new(slog::o!(
                "request_kind" => "bulk_sp_state_start",
                "request_id" => id,
            )),
        ));

        id
    }

    pub(super) async fn get(
        &self,
        id: &SpStateRequestId,
        last_seen_target: Option<u8>,
        timeout: Duration,
        limit: usize,
        log: &Logger,
    ) -> Result<BulkStateProgress, Error> {
        let log = log.new(slog::o!(
            "request_kind" => "bulk_sp_state_get",
            "request_id" => *id,
        ));

        let mut out = Vec::new();

        // Go ahead and create (and pin) the timeout, but we don't actually
        // await it until the loop at the bottom of this function.
        let timeout = tokio::time::sleep(timeout);
        tokio::pin!(timeout);

        let collector = self
            .requests
            .lock()
            .unwrap()
            .get(id)
            .map(Arc::clone)
            .ok_or(Error::NoSuchRequest)?;

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
            if let Some(last_seen_target) = last_seen_target.map(usize::from) {
                for (i, result) in collector.received_states.iter().enumerate()
                {
                    if result.target == last_seen_target {
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
                    return Err(Error::InvalidLastSpSeen);
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
    pub(crate) target: usize,
    pub(crate) state: IgnitionState,
    pub(crate) result: Result<SpState, Error>,
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

async fn wait_for_sp_responses<Fut>(
    id: SpStateRequestId,
    mut futures: FuturesUnordered<Fut>,
    requests: Arc<Mutex<RequestsMap>>,
    collector: Arc<RwLock<ResponseCollector>>,
    retain_grace_period: Duration,
    log: Logger,
) where
    Fut: Future<Output = BulkSpStateSingleResult>,
{
    while let Some(result) = futures.next().await {
        let mut collector = collector.write().unwrap();
        collector.push(result);

        // this is a little goofy, but we don't want to put it after the loop
        // and have to reacquire the write lock; we want clients of `collector`
        // to know if the list is done as soon as they wake up.
        if futures.is_empty() {
            collector.done = true;
        }
    }

    // At this point we've reported the status for every SP, either because we
    // know the actual status or because it timed out. We now enter the grace
    // period where we keep this request in memory so clients can continue to
    // ask for additional pages.
    //
    // TODO In the event that we get here _earlier_ than the client-requested
    // timeout (i.e., because we heard back from all the SPs and none timed
    // out), should we wait for "end of client timeout + grace period" or just
    // "end of last result collected + grace period"? Currently we choose the
    // latter, but maybe that's wrong.

    debug!(log, "all responses collected; starting grace period");
    tokio::time::sleep(retain_grace_period).await;

    debug!(log, "grace period elapsed; dropping request");
    requests.lock().unwrap().remove(&id);
}
