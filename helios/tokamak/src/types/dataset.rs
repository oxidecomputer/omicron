// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt;

/// The name of a ZFS filesystem, volume, or snapshot
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Name(String);

impl Name {
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

impl fmt::Display for Name {
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
pub(crate) enum Type {
    Filesystem,
    Snapshot,
    Volume,
}

bitflags::bitflags! {
    /// The classes of datasets for which a property is valid.
    pub(crate) struct PropertyTarget: u8 {
        const FILESYSTEM = 0b0001;
        const SNAPSHOT =   0b0010;
        const VOLUME =     0b0100;
    }
}

impl From<Type> for PropertyTarget {
    fn from(ty: Type) -> Self {
        use Type::*;
        match ty {
            Filesystem => PropertyTarget::FILESYSTEM,
            Snapshot => PropertyTarget::SNAPSHOT,
            Volume => PropertyTarget::VOLUME,
        }
    }
}

/// The ability of users to modify properties
#[derive(Eq, PartialEq)]
pub(crate) enum PropertyAccess {
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
pub(crate) enum Property {
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

impl Property {
    pub fn access(&self) -> PropertyAccess {
        use Property::*;
        use PropertyAccess::*;

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

    pub fn target(&self) -> PropertyTarget {
        let fs = PropertyTarget::FILESYSTEM;
        let all = PropertyTarget::all();
        let fs_and_vol = PropertyTarget::FILESYSTEM | PropertyTarget::VOLUME;
        let vol = PropertyTarget::VOLUME;

        use Property::*;
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
