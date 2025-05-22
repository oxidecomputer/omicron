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
    #[error("poll condition not yet ready")]
    NotYet,
    #[error("non-retryable error while polling on condition")]
    Failed(#[from] E),
}

/// Result of [`wait_for_condition()`]
#[derive(Debug, Error)]
pub enum Error<E> {
    /// operation timed out before succeeding or failing permanently
    #[error("timed out after {0:?}")]
    TimedOut(Duration),
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
    loop {
        let duration = Instant::now().duration_since(poll_start);
        if duration > *poll_max {
            return Err(Error::TimedOut(duration));
        }

        let check = cond().await;
        if let Ok(output) = check {
            return Ok(output);
        }

        if let Err(CondCheckError::Failed(e)) = check {
            return Err(Error::PermanentError(e));
        }

        tokio::time::sleep(*poll_interval).await;
    }
}

/// Poll the contents of the given watch channel until the given closure
/// succeeds, returns a permanent error, or a given time has expired
///
/// This function is similar to `wait_for_condition` above, and most of the same
/// caveats from it apply. The biggest difference is that instead of taking a
/// `poll_interval`, this function relies on the watch channel's `changed()`
/// notification to decide when to retry the given closure.
///
/// Note that `timeout` is not a bound on how long this function can take.
/// Rather, it's the time beyond which this function will stop trying to check
/// `cond`.  If `cond` takes an arbitrary amount of time, this function will
/// too.
pub async fn wait_for_watch_channel_condition<T, O, E, Func>(
    rx: &mut watch::Receiver<T>,
    mut cond: Func,
    timeout: Duration,
) -> Result<O, Error<E>>
where
    Func: AsyncFnMut(&T) -> Result<O, CondCheckError<E>>,
{
    let start = Instant::now();
    let deadline = start + timeout;
    loop {
        let check = cond(&*rx.borrow_and_update()).await;
        if let Ok(output) = check {
            return Ok(output);
        }

        if let Err(CondCheckError::Failed(e)) = check {
            return Err(Error::PermanentError(e));
        }

        tokio::select! {
            _ = rx.changed() => continue,
            // If we're already past the deadline, `duration_since` returns 0,
            // so this sleep will be ready immediately.
            _ = tokio::time::sleep(deadline.duration_since(start)) => {
                return Err(Error::TimedOut(timeout));
            }
        }
    }
}
