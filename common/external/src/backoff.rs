// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module providing utilities for retrying operations with exponential backoff.
//!
//! These retry policies should be used when attempting to access some
//! loosely-coupled component (often an external service) which may transiently
//! fail due to:
//! - A service which is still asynchronously initializing
//! - An inaccessible network
//! - An overloaded server

use std::future::Future;
use std::time::Duration;
use std::time::Instant;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{
    backoff::Backoff, ExponentialBackoff, ExponentialBackoffBuilder, Notify,
};

/// A helper function which modifies what information is tracked within the
/// callback of the notify function.
///
/// The default "Notify" function returns an error and the duration since the
/// *last* call of notify, but often the decision about "where should an error
/// be logged" depends on the *total* duration of time since we started
/// retrying.
///
/// By returning:
/// - A count of total calls
/// - The duration since we started retrying
///
/// The caller can more easily log:
/// - (info) Something non-erroneous on the first failure (indicating, to a log, that
/// retry is occuring).
/// - (warning) Something more concerned after a total time threshold has passed.
///
/// Identical to [::backoff::future::retry_notify], but invokes a method passing
/// the total count of calls and total duration instead of
/// [::backoff::Notify::notify].
pub async fn retry_notify_ext<I, E, Fut>(
    backoff: impl Backoff,
    operation: impl FnMut() -> Fut,
    mut notify: impl FnMut(E, usize, Duration),
) -> Result<I, E>
where
    Fut: Future<Output = Result<I, BackoffError<E>>>,
{
    let mut count = 0;
    let start = Instant::now();

    let backoff_notify = |error, _duration| {
        notify(error, count, start.elapsed());
        count += 1;
    };

    retry_notify(backoff, operation, backoff_notify).await
}

/// Return a backoff policy for querying internal services.
///
/// This policy makes attempts to retry under one second, but backs off
/// significantly to avoid overloading critical services.
pub fn retry_policy_internal_service() -> ::backoff::ExponentialBackoff {
    backoff_builder()
        .with_initial_interval(Duration::from_millis(250))
        .with_max_interval(Duration::from_secs(60 * 3))
        .build()
}

/// Return a backoff policy for querying internal services aggressively.
///
/// This policy is very similar to [retry_policy_internal_service], but should
/// be considered in cases where the request to the internal service can help
/// the rack initialize new resources.
///
/// The most significant difference is the "multiplier" - rather than backoff
/// roughly doubling, it backs off at a smaller interval.
pub fn retry_policy_internal_service_aggressive(
) -> ::backoff::ExponentialBackoff {
    backoff_builder()
        .with_initial_interval(Duration::from_millis(100))
        .with_multiplier(1.2)
        .with_max_interval(Duration::from_secs(60 * 3))
        .build()
}

/// Return a backoff policy for querying local-to-sled conditions.
///
/// This policy has a very small max interval, and should be used only in cases
/// where the request is local to the requester. In other words, it should only
/// be used when repeating the request does not risk overloading whatever
/// service is being queried.
pub fn retry_policy_local() -> ::backoff::ExponentialBackoff {
    backoff_builder()
        .with_initial_interval(Duration::from_millis(50))
        .with_max_interval(Duration::from_secs(1))
        .build()
}

fn backoff_builder() -> ::backoff::ExponentialBackoffBuilder {
    let mut builder = ::backoff::ExponentialBackoffBuilder::new();
    builder.with_multiplier(2.0).with_max_elapsed_time(None);
    builder
}
