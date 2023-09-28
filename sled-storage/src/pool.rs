// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ZFS storage pool

use crate::error::Error;
use illumos_utils::zpool::{ZpoolInfo, ZpoolName};
use omicron_common::disk::DiskIdentity;

#[cfg(test)]
use illumos_utils::zpool::MockZpool as Zpool;
#[cfg(not(test))]
use illumos_utils::zpool::Zpool;

/// A ZFS storage pool
#[derive(Debug, Clone)]
pub struct Pool {
    pub name: ZpoolName,
    pub info: ZpoolInfo,
    pub parent: DiskIdentity,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    pub fn new(name: ZpoolName, parent: DiskIdentity) -> Result<Pool, Error> {
        let info = Zpool::get_info(&name.to_string())?;
        Ok(Pool { name, info, parent })
    }
}
