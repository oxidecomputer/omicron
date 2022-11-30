// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing utilities for retrying operations with exponential backoff.

use std::time::Duration;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{backoff::Backoff, ExponentialBackoff, Notify};

/// Return a backoff policy for querying internal services which may not be up
/// for a relatively long amount of time.
pub fn internal_service_policy_long() -> ::backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(250);
    const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);
    internal_service_policy_with_max(INITIAL_INTERVAL, MAX_INTERVAL)
}

/// Return a backoff policy for querying conditions that are expected to
/// complete in a relatively shorter amount of time than
/// [internal_service_policy_long].
pub fn internal_service_policy_short() -> ::backoff::ExponentialBackoff {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(50);
    const MAX_INTERVAL: Duration = Duration::from_secs(1);
    internal_service_policy_with_max(INITIAL_INTERVAL, MAX_INTERVAL)
}

fn internal_service_policy_with_max(
    initial_interval: Duration,
    max_interval: Duration,
) -> ::backoff::ExponentialBackoff {
    let current_interval = initial_interval;
    ::backoff::ExponentialBackoff {
        current_interval,
        initial_interval,
        multiplier: 2.0,
        max_interval,
        max_elapsed_time: None,
        ..backoff::ExponentialBackoff::default()
    }
}
