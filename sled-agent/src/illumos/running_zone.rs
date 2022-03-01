// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::illumos::addrobj::AddrObject;
use crate::illumos::svc::wait_for_service;
use crate::illumos::vnic::{Vnic, VnicAllocator};
use crate::illumos::zone::{AddressRequest, ZONE_PREFIX};
use ipnetwork::IpNetwork;
use slog::Logger;
use std::path::PathBuf;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use crate::illumos::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

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

    #[error("Zone error accessing datalink: {0}")]
    Datalink(#[from] crate::illumos::dladm::Error),

    #[error("Timeout waiting for a service: {0}")]
    Timeout(String),
}

/// Represents a running zone.
pub struct RunningZone {
    inner: InstalledZone,
}

impl RunningZone {
    pub fn name(&self) -> &str {
        &self.inner.name
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
        let suffix: Vec<_> = args.into_iter().collect();
        let full_args = prefix
            .iter()
            .map(|s| std::ffi::OsStr::new(s))
            .chain(suffix.iter().map(|a| a.as_ref()));

        let cmd = command.args(full_args);
        let output = crate::illumos::execute(cmd)?;
        let stdout = String::from_utf8(output.stdout)?;
        Ok(stdout)
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(zone: InstalledZone) -> Result<Self, Error> {
        // Boot the zone.
        info!(zone.log, "Zone booting");

        // TODO: "Ensure booted", to make this more idempotent?
        Zones::boot(&zone.name)?;

        // Wait for the network services to come online, so future
        // requests to create addresses can operate immediately.
        let fmri = "svc:/milestone/network:default";
        wait_for_service(Some(&zone.name), fmri)
            .await
            .map_err(|_| Error::Timeout(fmri.to_string()))?;

        Ok(RunningZone { inner: zone })
    }

    pub async fn ensure_address(
        &self,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, Error> {
        info!(self.inner.log, "Adding address: {:?}", addrtype);
        let name = match addrtype {
            AddressRequest::Dhcp => "omicron",
            AddressRequest::Static(net) => match net.ip() {
                std::net::IpAddr::V4(_) => "omicron4",
                std::net::IpAddr::V6(_) => "omicron6",
            },
        };
        let addrobj = AddrObject::new(self.inner.control_vnic.name(), name);
        let network =
            Zones::ensure_address(Some(&self.inner.name), &addrobj, addrtype)?;
        Ok(network)
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
        addrtype: AddressRequest,
    ) -> Result<Self, Error> {
        let zone_info = Zones::get()?
            .into_iter()
            .find(|zone_info| zone_info.name().starts_with(&zone_prefix))
            .ok_or_else(|| Error::NotFound)?;

        if zone_info.state() != zone::State::Running {
            return Err(Error::NotRunning(zone_info.state()));
        }

        let zone_name = zone_info.name();
        let vnic_name = Zones::get_control_interface(zone_name)?;
        let addrobj = AddrObject::new_control(&vnic_name);
        Zones::ensure_address(Some(zone_name), &addrobj, addrtype)?;

        Ok(Self {
            inner: InstalledZone {
                log: log.new(o!("zone" => zone_name.to_string())),
                name: zone_name.to_string(),
                control_vnic: Vnic::wrap_existing(vnic_name),
                // TODO: How can the sled agent recoup other vnics?
                guest_vnics: vec![],
            },
        })
    }

    pub fn get_guest_vnics(&self) -> &Vec<Vnic> {
        &self.inner.guest_vnics
    }
}

impl Drop for RunningZone {
    fn drop(&mut self) {
        match Zones::halt_and_remove(&self.inner.log, self.name()) {
            Ok(()) => {
                info!(self.inner.log, "Stopped and uninstalled zone")
            }
            Err(e) => {
                warn!(self.inner.log, "Failed to stop zone: {}", e)
            }
        }
    }
}

pub struct InstalledZone {
    log: Logger,

    // Name of the Zone.
    name: String,

    // NIC used for control plane communication.
    control_vnic: Vnic,

    // Other NICs being used by the zone.
    guest_vnics: Vec<Vnic>,
}

impl InstalledZone {
    // TODO: Maybe should have a "get" method, like "RunningZone"?

    pub async fn install(
        log: &Logger,
        vnic_allocator: &VnicAllocator,
        service_name: &str,
        unique_name: Option<&str>,
        datasets: &[zone::Dataset],
        devices: &[zone::Device],
        vnics: Vec<Vnic>,
    ) -> Result<InstalledZone, Error> {
        let physical_dl = Dladm::find_physical()?;
        let control_vnic = vnic_allocator.new_control(&physical_dl, None)?;

        // The zone name is based on:
        // - A unique Oxide prefix ("oxz_")
        // - The name of the service being hosted (e.g., "nexus")
        // - An optional, service-unique identifier (typically a UUID).
        //
        // This results in a zone name which is distinct across different zpools,
        // but stable and predictable across reboots.
        let mut zone_name = format!("{}{}", ZONE_PREFIX, service_name);
        if let Some(suffix) = unique_name {
            zone_name.push_str(&format!("_{}", suffix));
        }

        let zone_image_path =
            PathBuf::from(&format!("/opt/oxide/{}.tar.gz", service_name));

        let vnic_names: Vec<String> = vnics
            .iter()
            .map(|vnic| vnic.name().to_string())
            .chain(std::iter::once(control_vnic.name().to_string()))
            .collect();

        Zones::install_omicron_zone(
            log,
            &zone_name,
            &zone_image_path,
            &datasets,
            &devices,
            vnic_names,
        )?;

        Ok(InstalledZone {
            log: log.new(o!("zone" => zone_name.clone())),
            name: zone_name,
            control_vnic,
            guest_vnics: vnics,
        })
    }
}
