// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::illumos::svc::wait_for_service;
use crate::vnic::{interface_name, Vnic};
use slog::Logger;
use std::net::SocketAddr;

#[cfg(test)]
use crate::illumos::zone::MockZones as Zones;
#[cfg(not(test))]
use crate::illumos::zone::Zones;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Zone not found")]
    NotFound,

    #[error("Zone is not running")]
    NotRunning,

    #[error("Zone operation failed: {0}")]
    Operation(#[from] crate::illumos::zone::Error),

    #[error("Timeout waiting for a service: {0}")]
    Timeout(String),
}

/// Represents a running zone.
pub struct RunningZone {
    log: Logger,

    // Name of the zone.
    name: String,

    // NIC used for control plane communication.
    _nic: Vnic,
    address: SocketAddr,
}

impl RunningZone {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(
        log: &Logger,
        zone_name: String,
        nic: Vnic,
        port: u16,
    ) -> Result<Self, Error> {
        // Boot the zone.
        info!(log, "Zone {} booting", zone_name);

        // TODO: "Ensure booted", to make this more idempotent?
        Zones::boot(&zone_name)?;

        // Wait for the network services to come online, then create an address
        // to use for communicating with the newly created zone.
        let fmri = "svc:/milestone/network:default";
        wait_for_service(Some(&zone_name), fmri)
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;

        let network =
            Zones::ensure_address(&zone_name, &interface_name(&nic.name()))?;

        Ok(RunningZone {
            log: log.clone(),
            name: zone_name,
            _nic: nic,
            address: SocketAddr::new(network.ip(), port),
        })
    }

    /// Looks up a running zone, if one already exists.
    pub async fn get(
        log: &Logger,
        zone_prefix: &str,
        port: u16,
    ) -> Result<Self, Error> {
        let zone = Zones::get()?
            .into_iter()
            .find(|zone| zone.name().starts_with(&zone_prefix))
            .ok_or_else(|| Error::NotFound)?;

        if zone.state() != zone::State::Running {
            return Err(Error::NotRunning);
        }

        let zone_name = zone.name();
        let vnic_name = Zones::get_control_interface(zone_name)?;
        let network =
            Zones::ensure_address(zone_name, &interface_name(&vnic_name))?;

        Ok(Self {
            log: log.clone(),
            name: zone_name.to_string(),
            _nic: Vnic::wrap_existing(vnic_name),
            address: SocketAddr::new(network.ip(), port),
        })
    }
}

impl Drop for RunningZone {
    fn drop(&mut self) {
        match Zones::halt_and_remove(&self.log, self.name()) {
            Ok(()) => {
                info!(self.log, "Stopped and uninstalled zone: {}", self.name)
            }
            Err(e) => {
                warn!(self.log, "Failed to stop zone {}: {}", self.name, e)
            }
        }
    }
}
