// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Quick-and-dirty polling within a test suite

use std::future::Future;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::watch;

/// Result of one attempt to check a condition (see [`wait_for_condition()`])
#[derive(Debug, Error)]
pub enum CondCheckError<E> {
    /// the condition we're waiting for is not true
    ///
    #[error(
        "poll condition not yet ready{}",
        .status
            .as_ref()
            .map_or_else(String::new, |status| format!(": {status}"))
    )]
    NotYet {
        /// A status string.
        ///
        /// Provide this when a plain timeout would not indicate which part of a
        /// compound condition never became true.
        ///
        /// If the poll ultimately times out, the most recent status is reported in
        /// [`Error::TimedOut`]'s `last_status` field.
        status: Option<String>,
    },
    #[error("non-retryable error while polling on condition")]
    Failed(#[from] E),
}

/// Result of [`wait_for_condition()`]
#[derive(Debug, Error)]
pub enum Error<E> {
    /// operation timed out before succeeding or failing permanently
    ///
    /// `last_status` is the most recent status carried by a
    /// [`CondCheckError::NotYet`] returned by the condition, if any.
    #[error(
        "timed out after {elapsed:?}{}",
        .last_status
            .as_ref()
            .map_or_else(String::new, |status| format!("; last status: {status}"))
    )]
    TimedOut { elapsed: Duration, last_status: Option<String> },
    #[error("non-retryable error while polling on condition: {0:#}")]
    PermanentError(E),
}

/// Poll the given closure until it succeeds, returns a permanent error, or
/// a given time has expired
///
/// This is intended in the test suite and developer tools for situations where
/// you've taken some action and want to wait for its effects to be observable
/// _and_ you have no way to directly wait for the observable event.  **This
/// approach is generally not applicable for production code.  See crate::backoff
/// for that.**  This is similar to the exponential backoff facility provided by
/// the "backoff" crate, but this is a non-randomized, constant-interval retry,
/// so it's not really a "backoff".
///
/// Note that `poll_max` is not a bound on how long this function can take.
/// Rather, it's the time beyond which this function will stop trying to check
/// `cond`.  If `cond` takes an arbitrary amount of time, this function will too.
///
/// This function is intended for those situations where it's tempting to sleep
/// for "long enough" (often people pick 1, 5, or 10 seconds) and then either
/// assume the thing happened or check the condition at that point.  We must
/// remember Clulow's lament:
///
///    Timeouts, timeouts: always wrong!
///    Some too short and some too long.
///
/// In fact, in trying to balance shorter test execution time against spurious
/// timeouts, people often choose a timeout value that is both too long _and_ too
/// short, resulting in both long test runs and spurious failures.  A better
/// pattern is provided here: check the condition relatively frequently with a
/// much longer maximum timeout -- long enough that timeout expiration
/// essentially reflects incorrect behavior.
///
/// But again: this mechanism is a last resort when no mechanism exists to
/// wait directly for the condition.
pub async fn wait_for_condition<O, E, Func, Fut>(
    mut cond: Func,
    poll_interval: &Duration,
    poll_max: &Duration,
) -> Result<O, Error<E>>
where
    Func: FnMut() -> Fut,
    Fut: Future<Output = Result<O, CondCheckError<E>>>,
{
    let poll_start = Instant::now();
    let mut last_status = None;
    loop {
        let duration = Instant::now().duration_since(poll_start);
        if duration > *poll_max {
            return Err(Error::TimedOut { elapsed: duration, last_status });
        }

        match cond().await {
            Ok(output) => return Ok(output),
            Err(CondCheckError::NotYet { status }) => {
                // Record this iteration's status (which may be `None`). `NotYet
                // { status: None }` clears the last status -- better to present
                // nothing than something out of date in the final error
                // message.
                last_status = status;
            }
            Err(CondCheckError::Failed(e)) => {
                return Err(Error::PermanentError(e));
            }
        }

        tokio::time::sleep(*poll_interval).await;
    }
}

/// Error returned by [`wait_for_watch_channel_condition()`].
#[derive(Debug, Error)]
pub enum WatchChannelError {
    /// the condition was not satisfied before the timeout elapsed
    #[error("timed out after {0:?}")]
    TimedOut(Duration),

    /// the watch channel's sender was dropped before the condition was
    /// satisfied, so the value can never change again
    #[error("watch channel sender dropped before the condition was satisfied")]
    SenderDropped,
}

/// Wait until the contents of the given watch channel satisfy `cond`, or until
/// `timeout` elapses.
///
/// This relies on the watch channel's change notifications: `cond` is checked
/// against the current value, then re-checked each time the channel changes.
///
/// Returns [`WatchChannelError::TimedOut`] if `cond` is not satisfied within
/// `timeout`, or [`WatchChannelError::SenderDropped`] if the sender is dropped
/// first (after which the value can never change again).
pub async fn wait_for_watch_channel_condition<T, Func>(
    rx: &mut watch::Receiver<T>,
    cond: Func,
    timeout: Duration,
) -> Result<(), WatchChannelError>
where
    Func: FnMut(&T) -> bool,
{
    match tokio::time::timeout(timeout, rx.wait_for(cond)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_recv_error)) => Err(WatchChannelError::SenderDropped),
        Err(_elapsed) => Err(WatchChannelError::TimedOut(timeout)),
    }
}
