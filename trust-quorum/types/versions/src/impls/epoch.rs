// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for Epoch.

use crate::latest::types::Epoch;

impl Epoch {
    /// Returns the next epoch.
    ///
    /// Panics if the epoch counter would overflow (more than 2^64 epochs).
    pub fn next(&self) -> Epoch {
        Epoch(self.0.checked_add(1).expect("fewer than 2^64 epochs"))
    }
}
