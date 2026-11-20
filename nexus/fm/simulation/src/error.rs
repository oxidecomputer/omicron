// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Why an operation failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// [`time::advance`](crate::time::advance) was given a duration it cannot
    /// advance by: either zero, or so long that the clock would exceed what
    /// chrono can represent.
    #[error("duration out of range for the simulated clock")]
    TimeOutOfRange,
}
