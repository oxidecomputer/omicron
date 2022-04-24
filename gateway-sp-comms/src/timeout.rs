// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use futures::Future;
use futures::TryFutureExt;
use std::time::Duration;
use tokio::time::Instant;

/// Error type returned from [`Timeout::timeout_at()`].
#[derive(Debug, Clone, Copy)]
pub struct Elapsed(pub Timeout);

impl Elapsed {
    /// Get the duration of the timeout that elapsed.
    pub fn duration(&self) -> Duration {
        self.0.duration()
    }
}

/// Representation of a timeout as both its starting time and its duration.
#[derive(Debug, Clone, Copy)]
pub struct Timeout {
    start: Instant,
    duration: Duration,
}

impl Timeout {
    /// Create a new `Timeout` with the given duration starting from
    /// [`Instant::now()`].
    pub fn from_now(duration: Duration) -> Self {
        Self { start: Instant::now(), duration }
    }

    /// Get the [`Instant`] when this timeout expires.
    pub fn end(&self) -> Instant {
        self.start + self.duration
    }

    /// Get the duration of this timeout.
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Wrap a future with this timeout.
    pub fn timeout_at<T>(
        self,
        future: T,
    ) -> impl Future<Output = Result<T::Output, Elapsed>>
    where
        T: Future,
    {
        tokio::time::timeout_at(self.end(), future)
            .map_err(move |_| Elapsed(self))
    }
}
