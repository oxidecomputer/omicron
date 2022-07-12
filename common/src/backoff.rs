// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing utilities for retrying operations with exponential backoff.

use std::time::Duration;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{backoff::Backoff, ExponentialBackoff, Notify};

/// Return a backoff policy appropriate for retrying internal services
/// indefinitely.
pub fn internal_service_policy() -> ::backoff::ExponentialBackoff {
    const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
    internal_service_policy_with_max(MAX_INTERVAL)
}

pub fn internal_service_policy_with_max(
    max_duration: Duration,
) -> ::backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(250);
    ::backoff::ExponentialBackoff {
        current_interval: INITIAL_INTERVAL,
        initial_interval: INITIAL_INTERVAL,
        multiplier: 2.0,
        max_interval: max_duration,
        max_elapsed_time: None,
        ..backoff::ExponentialBackoff::default()
    }
}
