// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::disk::DiskStateRequested;
use crate::latest::disk::DiskVariant;
use crate::latest::disk::M2Slot;
use crate::latest::disk::OmicronPhysicalDiskConfig;
use anyhow::bail;
use iddqd::IdOrdItem;
use iddqd::id_upcast;
use omicron_common::zpool_name::ZpoolKind;
use omicron_uuid_kinds::PhysicalDiskUuid;
use std::fmt;
use std::str::FromStr;

impl DiskStateRequested {
    /// Returns whether the requested state is attached to an Instance or not.
    pub fn is_attached(&self) -> bool {
        match self {
            DiskStateRequested::Detached => false,
            DiskStateRequested::Destroyed => false,
            DiskStateRequested::Faulted => false,
            DiskStateRequested::Attached(_) => true,
        }
    }
}

impl M2Slot {
    /// Flip from `A` to `B` or vice versa.
    pub fn toggled(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }

    /// Convert this slot to an MGS "firmware slot" index.
    pub fn to_mgs_firmware_slot(self) -> u16 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    /// Convert a putative MGS "firmware slot" index to an `M2Slot`, returning
    /// `None` if `slot` is invalid.
    pub fn from_mgs_firmware_slot(slot: u16) -> Option<Self> {
        match slot {
            0 => Some(Self::A),
            1 => Some(Self::B),
            _ => None,
        }
    }
}

impl fmt::Display for M2Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::A => f.write_str("A"),
            Self::B => f.write_str("B"),
        }
    }
}

impl FromStr for M2Slot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "a" | "A" => Ok(Self::A),
            "b" | "B" => Ok(Self::B),
            _ => Err(format!(
                "unrecognized value {s} for M2 slot. \
                 Must be one of `a`, `A`, `b`, or `B`",
            )),
        }
    }
}

impl TryFrom<i64> for M2Slot {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            // Gimlet should have 2 M.2 drives: drive A is assigned slot 17, and
            // drive B is assigned slot 18.
            17 => Ok(Self::A),
            18 => Ok(Self::B),
            _ => bail!("unexpected M.2 slot {value}"),
        }
    }
}

impl From<ZpoolKind> for DiskVariant {
    fn from(kind: ZpoolKind) -> DiskVariant {
        match kind {
            ZpoolKind::External => DiskVariant::U2,
            ZpoolKind::Internal => DiskVariant::M2,
        }
    }
}

impl IdOrdItem for OmicronPhysicalDiskConfig {
    type Key<'a> = PhysicalDiskUuid;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}
