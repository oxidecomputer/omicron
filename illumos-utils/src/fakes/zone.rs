// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::zone::Api;
use crate::zpool::PathInPool;

use camino::Utf8Path;
use slog::Logger;
use std::sync::Arc;
use std::sync::Mutex;

/// A fake implementation of [crate::zone::Zones].
///
/// This struct implements the [crate::zone::Api] interface but avoids
/// interacting with the host OS.
pub struct Zones {
    zones: Mutex<Vec<zone::Zone>>,
}

impl Zones {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { zones: Mutex::new(vec![]) })
    }
}

#[async_trait::async_trait]
impl Api for Zones {
    async fn get(&self) -> Result<Vec<zone::Zone>, crate::zone::AdmError> {
        Ok(self.zones.lock().unwrap().clone())
    }

    async fn install_omicron_zone(
        &self,
        _log: &Logger,
        _zone_root_path: &PathInPool,
        _zone_name: &str,
        _zone_image: &Utf8Path,
        _datasets: &[zone::Dataset],
        _filesystems: &[zone::Fs],
        _devices: &[zone::Device],
        _links: Vec<String>,
        _limit_priv: Vec<String>,
    ) -> Result<(), crate::zone::AdmError> {
        Ok(())
    }

    async fn boot(&self, _name: &str) -> Result<(), crate::zone::AdmError> {
        Ok(())
    }

    // NOTE: Once we have better signal fidelity within our fake Zone
    // implementation (accurately tracking booted zones, and implementing 'get'
    // with a non-empty Vec) we can delete this implementation.
    async fn id(
        &self,
        _name: &str,
    ) -> Result<Option<i32>, crate::zone::AdmError> {
        Ok(Some(1))
    }

    async fn wait_for_service(
        &self,
        _zone: Option<&str>,
        _fmri: &str,
        _log: Logger,
    ) -> Result<(), omicron_common::api::external::Error> {
        Ok(())
    }

    async fn halt_and_remove(
        &self,
        _name: &str,
    ) -> Result<Option<zone::State>, crate::zone::AdmError> {
        Ok(Some(zone::State::Down))
    }
}
