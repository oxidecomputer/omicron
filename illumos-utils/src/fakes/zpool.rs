// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::zpool::Api;
use crate::zpool::CreateError;
use crate::zpool::ZpoolName;
use camino::Utf8Path;
use std::sync::Arc;

/// A fake implementation of [crate::zpool::Zpool].
///
/// This struct implements the [crate::zpool::Api] interface but avoids
/// interacting with the host OS.
pub struct Zpool {}

impl Zpool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait::async_trait]
impl Api for Zpool {
    async fn create(
        &self,
        _name: &ZpoolName,
        _vdev: &Utf8Path,
    ) -> Result<(), CreateError> {
        Ok(())
    }
}
