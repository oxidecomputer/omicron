// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces used to interact with the underlying host system.

pub mod addrobj;
mod error;
mod executor;
mod host;
mod input;
pub mod interfaces;
mod output;
pub mod zpool;

pub use error::*;
pub use executor::*;
pub use host::*;
pub use input::*;
pub use output::*;

pub const COREADM: &str = "/usr/bin/coreadm";
pub const DLADM: &str = "/usr/sbin/dladm";
pub const DUMPADM: &str = "/usr/sbin/dumpadm";
pub const FSTYP: &str = "/usr/sbin/fstyp";
pub const IPADM: &str = "/usr/sbin/ipadm";
pub const PFEXEC: &str = "/usr/bin/pfexec";
pub const ROUTE: &str = "/usr/sbin/route";
pub const SAVECORE: &str = "/usr/bin/savecore";
pub const SVCADM: &str = "/usr/sbin/svcadm";
pub const SVCCFG: &str = "/usr/sbin/svccfg";
pub const ZFS: &str = "/usr/sbin/zfs";
pub const ZLOGIN: &str = "/usr/sbin/zlogin";
pub const ZONEADM: &str = "/usr/sbin/zoneadm";
pub const ZONECFG: &str = "/usr/sbin/zonecfg";
pub const ZPOOL: &str = "/usr/sbin/zpool";
