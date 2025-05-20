// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ZFS storage pool

use crate::error::Error;
use illumos_utils::zpool::{Zpool, ZpoolInfo, ZpoolName};
use omicron_common::disk::DiskIdentity;

/// A ZFS storage pool wrapper that tracks information returned from
/// `zpool` commands
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pool {
    pub name: ZpoolName,
    pub info: ZpoolInfo,
    pub parent: DiskIdentity,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    pub async fn new(
        name: ZpoolName,
        parent: DiskIdentity,
    ) -> Result<Pool, Error> {
        let info = Zpool::get_info(&name.to_string()).await?;
        Ok(Pool { name, info, parent })
    }

    /// Return a Pool consisting of fake info
    #[cfg(all(test, feature = "testing"))]
    pub fn new_with_fake_info(name: ZpoolName, parent: DiskIdentity) -> Pool {
        let info = ZpoolInfo::new_hardcoded(name.to_string());
        Pool { name, info, parent }
    }
}
