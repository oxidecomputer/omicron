// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::disk::CompressionAlgorithm;
use crate::latest::disk::DiskVariant;
use crate::latest::disk::GzipLevel;
use crate::latest::disk::M2Slot;
use anyhow::bail;
use omicron_common::zpool_name::ZpoolKind;
use std::fmt;
use std::str::FromStr;

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

    /// Look up the slot of an M.2 socket from its chassis label, returning
    /// `None` for any other label.
    ///
    /// Gimlet and Cosmo both pair M.2 East with boot storage unit A and
    /// M.2 West with B.
    pub fn from_chassis_location(location: &str) -> Option<Self> {
        match location {
            "M.2 East" => Some(Self::A),
            "M.2 West" => Some(Self::B),
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

impl From<ZpoolKind> for DiskVariant {
    fn from(kind: ZpoolKind) -> DiskVariant {
        match kind {
            ZpoolKind::External => DiskVariant::U2,
            ZpoolKind::Internal => DiskVariant::M2,
        }
    }
}

// Fastest compression level
const GZIP_LEVEL_MIN: u8 = 1;

// Best compression ratio
const GZIP_LEVEL_MAX: u8 = 9;

impl GzipLevel {
    pub const fn new<const N: u8>() -> Self {
        assert!(N >= GZIP_LEVEL_MIN, "Compression level too small");
        assert!(N <= GZIP_LEVEL_MAX, "Compression level too large");
        Self(N)
    }
}

impl FromStr for GzipLevel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let level = s.parse::<u8>()?;
        if level < GZIP_LEVEL_MIN || level > GZIP_LEVEL_MAX {
            bail!("Invalid gzip compression level: {level}");
        }
        Ok(Self(level))
    }
}

/// These match the arguments which can be passed to "zfs set compression=..."
impl fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::On => "on",
            Self::Off => "off",
            Self::Gzip => "gzip",
            Self::GzipN { level } => {
                return write!(f, "gzip-{}", level.0);
            }
            Self::Lz4 => "lz4",
            Self::Lzjb => "lzjb",
            Self::Zle => "zle",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c = match s {
            "on" => Self::On,
            "" | "off" => Self::Off,
            "gzip" => Self::Gzip,
            "lz4" => Self::Lz4,
            "lzjb" => Self::Lzjb,
            "zle" => Self::Zle,
            _ => {
                let Some(suffix) = s.strip_prefix("gzip-") else {
                    bail!("Unknown compression algorithm {s}");
                };
                Self::GzipN { level: suffix.parse()? }
            }
        };
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m2_slot_from_chassis_location() {
        assert_eq!(M2Slot::from_chassis_location("M.2 East"), Some(M2Slot::A));
        assert_eq!(M2Slot::from_chassis_location("M.2 West"), Some(M2Slot::B));
        assert_eq!(M2Slot::from_chassis_location("N0"), None);
        assert_eq!(M2Slot::from_chassis_location("M.2 2"), None);
    }
}
