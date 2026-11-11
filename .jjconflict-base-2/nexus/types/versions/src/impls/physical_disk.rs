// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for physical disk types.

use crate::latest::physical_disk::{PhysicalDiskPolicy, PhysicalDiskState};
use std::fmt;

impl PhysicalDiskPolicy {
    /// Creates a new `PhysicalDiskPolicy` that is in-service.
    pub fn in_service() -> Self {
        Self::InService
    }

    /// Returns true if the disk can be decommissioned in this state.
    pub fn is_decommissionable(&self) -> bool {
        // This should be kept in sync with decommissionable_states below.
        match self {
            Self::InService => false,
            Self::Expunged => true,
        }
    }
}

impl fmt::Display for PhysicalDiskPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalDiskPolicy::InService => write!(f, "in service"),
            PhysicalDiskPolicy::Expunged => write!(f, "expunged"),
        }
    }
}

impl fmt::Display for PhysicalDiskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalDiskState::Active => write!(f, "active"),
            PhysicalDiskState::Decommissioned => write!(f, "decommissioned"),
        }
    }
}
