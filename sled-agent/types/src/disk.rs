// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk types for sled-agent.

pub use sled_agent_types_versions::latest::disk::*;

/// Extension trait for [`DiskStateRequested`].
pub trait DiskStateRequestedExt {
    /// Returns whether the requested state is attached to an Instance or not.
    fn is_attached(&self) -> bool;
}

impl DiskStateRequestedExt for DiskStateRequested {
    fn is_attached(&self) -> bool {
        match self {
            DiskStateRequested::Detached => false,
            DiskStateRequested::Destroyed => false,
            DiskStateRequested::Faulted => false,
            DiskStateRequested::Attached(_) => true,
        }
    }
}
