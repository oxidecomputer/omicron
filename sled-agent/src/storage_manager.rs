//! Management of sled-local storage.

use omicron_common::api::external::Error;

use crate::illumos::zpool::ZpoolInfo;

#[cfg(test)]
use crate::illumos::zpool::MockZpool as Zpool;
#[cfg(not(test))]
use crate::illumos::zpool::Zpool;

/// A ZFS storage pool.
struct Pool {
    info: ZpoolInfo,
}

impl Pool {
    /// Queries for an existing Zpool.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &str) -> Result<Pool, Error> {
        let info = Zpool::get_info(name)?;
        Ok(Pool { info })
    }
}

/// A sled-local view of all attached storage.
pub struct StorageManager {
    pools: Vec<Pool>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should monitor for local storage.
    ///
    /// NOTE: Monitoring is not yet implemented - this option simply reports
    /// "no observed local disks".
    pub fn new() -> Result<Self, Error> {
        Ok(StorageManager { pools: vec![] })
    }

    /// Creates a new [`StorageManager`] from a list of pre-supplied zpools.
    pub fn new_from_zpools(zpools: Vec<String>) -> Result<Self, Error> {
        let pools = zpools
            .into_iter()
            .map(|name| Pool::new(&name))
            .collect::<Result<Vec<Pool>, Error>>()?;

        Ok(StorageManager { pools })
    }
}
