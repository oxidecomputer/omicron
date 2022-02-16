// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::addrobj::AddrObject;
use crate::illumos::svc::wait_for_service;
use crate::illumos::zone::AddrType;
use crate::vnic::Vnic;
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

    #[error("Zone is not running; it is in the {0:?} state instead")]
    NotRunning(zone::State),

    #[error("Execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error("Failed to parse output: {0}")]
    Parse(#[from] std::string::FromUtf8Error),

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

    /// Runs a command within the Zone, return the output.
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = std::process::Command::new(crate::illumos::PFEXEC);

        let name = self.name();
        let prefix = &[super::zone::ZLOGIN, name];
        let prefix_iter: Vec<_> =
            prefix.iter().map(|s| std::ffi::OsStr::new(s)).collect();
        let suffix: Vec<_> = args.into_iter().collect();
        let full_args =
            prefix_iter.into_iter().chain(suffix.iter().map(|a| a.as_ref()));

        let cmd = command.args(full_args);
        let output = crate::illumos::execute(cmd)?;
        let stdout = String::from_utf8(output.stdout)?;
        Ok(stdout)
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(
        log: &Logger,
        zone_name: String,
        nic: Vnic,
        addrtype: AddrType,
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

        let addrobj = AddrObject::new_control(nic.name());
        let network = Zones::ensure_address(
            &zone_name,
            &addrobj,
            addrtype,
        )?;

        Ok(RunningZone {
            log: log.clone(),
            name: zone_name,
            _nic: nic,
            address: SocketAddr::new(network.ip(), port),
        })
    }

    /// Looks up a running zone based on the `zone_prefix`, if one already exists.
    ///
    /// - If the zone was found, is running, and has a network interface, it is
    /// returned.
    /// - If the zone was not found `Error::NotFound` is returned.
    /// - If the zone was found, but not running, `Error::NotRunning` is
    /// returned.
    /// - Other errors may be returned attemping to look up and accessing an
    /// address on the zone.
    pub async fn get(
        log: &Logger,
        zone_prefix: &str,
        addrtype: AddrType,
        port: u16,
    ) -> Result<Self, Error> {
        let zone = Zones::get()?
            .into_iter()
            .find(|zone| zone.name().starts_with(&zone_prefix))
            .ok_or_else(|| Error::NotFound)?;

        if zone.state() != zone::State::Running {
            return Err(Error::NotRunning(zone.state()));
        }

        let zone_name = zone.name();
        let vnic_name = Zones::get_control_interface(zone_name)?;
        let addrobj = AddrObject::new_control(&vnic_name);
        let network = Zones::ensure_address(
            zone_name,
            &addrobj,
            addrtype,
        )?;

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
