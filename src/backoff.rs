/*!
 * Module providing utilities for retrying operations with exponential backoff.
 */

use std::time::Duration;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{backoff::Backoff, ExponentialBackoff, Notify};

/**
 * Return a backoff policy appropriate for retrying internal services
 * indefinitely.
 */
pub fn internal_service_policy() -> ::backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(250);
    const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
    ::backoff::ExponentialBackoff {
        current_interval: INITIAL_INTERVAL,
        initial_interval: INITIAL_INTERVAL,
        multiplier: 2.0,
        max_interval: MAX_INTERVAL,
        max_elapsed_time: None,
        ..backoff::ExponentialBackoff::default()
    }
}

pub mod poll {
    use core::future::Future;
    use std::time::Duration;
    use std::time::Instant;

    /**
     * Result of one attempt to check a condition (see [`wait_for_condition()`])
     */
    pub enum CondCheckResult<E> {
        /** the condition we're waiting for is true */
        Ready,
        /** the condition we're waiting for is not true */
        NotYet,
        /** stop polling because we've encountered a non-retryable error */
        Failed(E),
    }

    /** Result of [`wait_for_condition()`] */
    pub enum Error<E> {
        /** operation timed out before succeeding or failing permanently */
        TimedOut(Duration),
        /** operation failed with a permanent error before timing out */
        PermanentError(E),
    }

    /**
     * Poll the given closure until it succeeds, returns a permanent error, or
     * times out
     *
     * You can think of this analogous to the exponential backoff facility
     * provided by the "backoff" crate, but this is a non-randomized,
     * constant-interval retry, so it's not really a "backoff" at all.
     *
     * This is intended for situations in the test suite where you've taken some
     * action and want to wait for its effects to be observable _and_ you have
     * no way to directly wait for the observable event.  It's tempting to sleep
     * for "long enough" (often people pick 1, 5, or 10 seconds) and then either
     * assume the thing happened or check the condition at that point.  We must
     * remember Clulow's adage:
     *
     *    Timeouts, timeouts: always wrong!
     *    Some too short and some too long.
     *
     * In fact, trying to balance shorter test execution time against spurious
     * timeouts, people often choose a timeout value that is both too long _and_
     * too short, resulting in both long test runs and spurious failures.  A
     * better pattern is provided here: check the condition relatively
     * frequently with a much longer maximum timeout -- long enough that timeout
     * expiration essentially reflects incorrect behavior.
     *
     * But again: this mechanism is a last resort when no mechanism exists to
     * wait directly for the condition.
     */
    pub async fn wait_for_condition<E, Func, Fut>(
        cond: Func,
        poll_interval: &Duration,
        poll_max: &Duration,
    ) -> Result<(), Error<E>>
    where
        Func: Fn() -> Fut,
        Fut: Future<Output = CondCheckResult<E>>,
    {
        let poll_start = Instant::now();
        loop {
            let duration = Instant::now().duration_since(poll_start);
            if duration > *poll_max {
                return Err(Error::TimedOut(duration));
            }

            let check = cond().await;
            if let CondCheckResult::Ready = check {
                return Ok(());
            }

            if let CondCheckResult::Failed(e) = check {
                return Err(Error::PermanentError(e));
            }

            tokio::time::sleep(*poll_interval).await;
        }
    }
}
