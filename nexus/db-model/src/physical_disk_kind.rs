// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::physical_disk;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    PhysicalDiskKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum PhysicalDiskKind;

    // Enum values
    M2 => b"m2"
    U2 => b"u2"
);

impl From<physical_disk::PhysicalDiskKind> for PhysicalDiskKind {
    fn from(k: physical_disk::PhysicalDiskKind) -> Self {
        match k {
            physical_disk::PhysicalDiskKind::M2 => PhysicalDiskKind::M2,
            physical_disk::PhysicalDiskKind::U2 => PhysicalDiskKind::U2,
        }
    }
}

impl From<PhysicalDiskKind> for physical_disk::PhysicalDiskKind {
    fn from(value: PhysicalDiskKind) -> Self {
        match value {
            PhysicalDiskKind::M2 => physical_disk::PhysicalDiskKind::M2,
            PhysicalDiskKind::U2 => physical_disk::PhysicalDiskKind::U2,
        }
    }
}
