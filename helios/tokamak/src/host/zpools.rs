// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates zpools

use camino::Utf8PathBuf;
use helios_fusion::zpool::{ZpoolHealth, ZpoolName};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FakeZpool {
    name: ZpoolName,
    vdev: Utf8PathBuf,

    pub imported: bool,
    pub health: ZpoolHealth,
    pub properties: HashMap<String, String>,
}

impl FakeZpool {
    pub(crate) fn new(
        name: ZpoolName,
        vdev: Utf8PathBuf,
        imported: bool,
    ) -> Self {
        Self {
            name,
            vdev,
            imported,
            health: ZpoolHealth::Online,
            properties: HashMap::new(),
        }
    }
}

/// Describes access to zpools that exist within the system.
pub(crate) struct Zpools {
    zpools: HashMap<ZpoolName, FakeZpool>,
}

impl Zpools {
    pub(crate) fn new() -> Self {
        Self { zpools: HashMap::new() }
    }

    // Zpool access methods

    pub fn get(&self, name: &ZpoolName) -> Option<&FakeZpool> {
        self.zpools.get(name)
    }

    pub fn get_mut(&mut self, name: &ZpoolName) -> Option<&mut FakeZpool> {
        self.zpools.get_mut(name)
    }

    pub fn all(&self) -> impl Iterator<Item = (&ZpoolName, &FakeZpool)> {
        self.zpools.iter()
    }

    pub fn insert(
        &mut self,
        name: ZpoolName,
        vdev: Utf8PathBuf,
        import: bool,
    ) -> Result<(), String> {
        if self.zpools.contains_key(&name) {
            return Err(format!(
                "Cannot create pool name '{name}': already exists"
            ));
        }

        let pool = FakeZpool::new(name.clone(), vdev, import);
        self.zpools.insert(name.clone(), pool);
        Ok(())
    }
}
