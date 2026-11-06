// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Why an operation failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// [`time::advance`](crate::time::advance) would put the clock past what
    /// chrono can represent, either because the duration itself is too long to
    /// convert or because the sum overflows.
    #[error("the simulated clock cannot go that far")]
    TimeOutOfRange,
}
