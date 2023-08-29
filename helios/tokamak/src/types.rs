// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types which may be parsed from the CLI and used by a fake host

use std::fmt;

// TODO: nest under "dataset" module, eliminate prefix

/// The name of a ZFS filesystem, volume, or snapshot
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DatasetName(String);

impl DatasetName {
    pub fn new<S: Into<String>>(s: S) -> Result<Self, String> {
        let s: String = s.into();
        if s.is_empty() {
            return Err("Invalid name: Empty string".to_string());
        }
        if s.ends_with('/') {
            return Err(format!("Invalid name {s}: trailing slash in name"));
        }

        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DatasetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
#[strum(use_phf, serialize_all = "lowercase")]
pub(crate) enum DatasetType {
    Filesystem,
    Snapshot,
    Volume,
}

bitflags::bitflags! {
    /// The classes of datasets for which a property is valid.
    pub(crate) struct DatasetPropertyTarget: u8 {
        const FILESYSTEM = 0b0001;
        const SNAPSHOT =   0b0010;
        const VOLUME =     0b0100;
    }
}

impl From<DatasetType> for DatasetPropertyTarget {
    fn from(ty: DatasetType) -> Self {
        use DatasetType::*;
        match ty {
            Filesystem => DatasetPropertyTarget::FILESYSTEM,
            Snapshot => DatasetPropertyTarget::SNAPSHOT,
            Volume => DatasetPropertyTarget::VOLUME,
        }
    }
}

/// The ability of users to modify properties
#[derive(Eq, PartialEq)]
pub(crate) enum DatasetPropertyAccess {
    ReadOnly,
    ReadWrite,
}

/// A property which is applicable to datasets
#[derive(
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
#[strum(use_phf, serialize_all = "lowercase")]
pub(crate) enum DatasetProperty {
    Atime,
    #[strum(serialize = "available", serialize = "avail")]
    Available,
    Encryption,
    Logbias,
    Mounted,
    Mountpoint,
    Name,
    Keyformat,
    Keylocation,
    #[strum(serialize = "oxide:epoch")]
    OxideEpoch,
    Primarycache,
    #[strum(serialize = "reservation", serialize = "refreservation")]
    Reservation,
    Secondarycache,
    Type,
    Volblocksize,
    Volsize,
    Zoned,
}

impl DatasetProperty {
    pub fn access(&self) -> DatasetPropertyAccess {
        use DatasetProperty::*;
        use DatasetPropertyAccess::*;

        match self {
            Atime => ReadWrite,
            Available => ReadOnly,
            Encryption => ReadWrite,
            Logbias => ReadWrite,
            Mounted => ReadOnly,
            Mountpoint => ReadWrite,
            Name => ReadOnly,
            Keyformat => ReadWrite,
            Keylocation => ReadWrite,
            OxideEpoch => ReadWrite,
            Primarycache => ReadWrite,
            Reservation => ReadOnly,
            Secondarycache => ReadWrite,
            Type => ReadOnly,
            Volblocksize => ReadOnly,
            Volsize => ReadOnly,
            Zoned => ReadWrite,
        }
    }

    pub fn target(&self) -> DatasetPropertyTarget {
        let fs = DatasetPropertyTarget::FILESYSTEM;
        let all = DatasetPropertyTarget::all();
        let fs_and_vol =
            DatasetPropertyTarget::FILESYSTEM | DatasetPropertyTarget::VOLUME;
        let vol = DatasetPropertyTarget::VOLUME;

        use DatasetProperty::*;
        match self {
            Atime => fs,
            Available => all,
            Encryption => all,
            Logbias => fs_and_vol,
            Mounted => fs,
            Mountpoint => fs,
            Name => all,
            Keyformat => fs_and_vol,
            Keylocation => fs_and_vol,
            OxideEpoch => all,
            Primarycache => fs_and_vol,
            Reservation => vol,
            Secondarycache => fs_and_vol,
            Type => all,
            Volblocksize => vol,
            Volsize => vol,
            Zoned => all,
        }
    }
}
