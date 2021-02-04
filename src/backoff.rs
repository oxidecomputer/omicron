/*!
 * Module providing utilities for retrying operations with exponential backoff.
 */

use std::time::Duration;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{backoff::Backoff, ExponentialBackoff, Notify};

/**
 * Return a backoff policy appropriate for retrying internal services indefinitely.
 */
pub fn internal_service_policy() -> ::backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_secs(5);
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
