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

use std::time::Duration;

pub use ::backoff::future::{retry, retry_notify};
pub use ::backoff::Error as BackoffError;
pub use ::backoff::{backoff::Backoff, ExponentialBackoff, Notify};

/// Return a backoff policy for querying internal services.
///
/// This policy makes attempts to retry under one second, but backs off
/// significantly to avoid overloading critical services.
pub fn retry_policy_internal_service() -> ::backoff::ExponentialBackoff {
    backoff_builder()
        .with_initial_interval(Duration::from_millis(250))
        .with_max_interval(Duration::from_secs(60 * 60))
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
        .with_max_interval(Duration::from_secs(60 * 60))
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
