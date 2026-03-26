// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a physical disk's state as understood by Nexus.

use super::impl_enum_type;
use nexus_types::external_api::physical_disk;
use serde::{Deserialize, Serialize};
use std::fmt;
use strum::EnumIter;

impl_enum_type!(
    PhysicalDiskStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq, EnumIter)]
    pub enum PhysicalDiskState;

    // Enum values
    Active => b"active"
    Decommissioned => b"decommissioned"
);

impl fmt::Display for PhysicalDiskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Forward to the canonical implementation in nexus-types.
        physical_disk::PhysicalDiskState::from(*self).fmt(f)
    }
}

impl From<PhysicalDiskState> for physical_disk::PhysicalDiskState {
    fn from(state: PhysicalDiskState) -> Self {
        match state {
            PhysicalDiskState::Active => {
                physical_disk::PhysicalDiskState::Active
            }
            PhysicalDiskState::Decommissioned => {
                physical_disk::PhysicalDiskState::Decommissioned
            }
        }
    }
}

impl From<physical_disk::PhysicalDiskState> for PhysicalDiskState {
    fn from(state: physical_disk::PhysicalDiskState) -> Self {
        match state {
            physical_disk::PhysicalDiskState::Active => {
                PhysicalDiskState::Active
            }
            physical_disk::PhysicalDiskState::Decommissioned => {
                PhysicalDiskState::Decommissioned
            }
        }
    }
}
