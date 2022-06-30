// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::illumos::addrobj::AddrObject;
use crate::illumos::svc::wait_for_service;
use crate::illumos::vnic::{Vnic, VnicAllocator};
use crate::illumos::zone::{AddressRequest, ZONE_PREFIX};
use crate::opte::OptePort;
use ipnetwork::IpNetwork;
use slog::Logger;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

#[cfg(test)]
use crate::illumos::zone::MockZones as Zones;
#[cfg(not(test))]
use crate::illumos::zone::Zones;

/// Errors returned from [`RunningZone::run_cmd`].
#[derive(thiserror::Error, Debug)]
#[error("Error running command in zone '{zone}': {err}")]
pub struct RunCommandError {
    zone: String,
    #[source]
    err: crate::illumos::ExecutionError,
}

/// Errors returned from [`RunningZone::boot`].
#[derive(thiserror::Error, Debug)]
pub enum BootError {
    #[error("Error booting zone: {0}")]
    Booting(#[from] crate::illumos::zone::AdmError),

    #[error("Zone booted, but timed out waiting for {service} in {zone}")]
    Timeout { service: String, zone: String },
}

/// Errors returned from [`RunningZone::ensure_address`].
#[derive(thiserror::Error, Debug)]
pub enum EnsureAddressError {
    #[error("Failed ensuring address {request:?} in {zone}: could not construct addrobj name: {err}")]
    AddrObject {
        request: AddressRequest,
        zone: String,
        err: crate::illumos::addrobj::ParseError,
    },

    #[error(transparent)]
    EnsureAddressError(#[from] crate::illumos::zone::EnsureAddressError),
}

/// Erros returned from [`RunningZone::get_zone`].
#[derive(thiserror::Error, Debug)]
pub enum GetZoneError {
    #[error("While looking up zones with prefix '{prefix}', could not get zones: {err}")]
    GetZones {
        prefix: String,
        #[source]
        err: crate::illumos::zone::AdmError,
    },

    #[error("Zone with prefix '{prefix}' not found")]
    NotFound { prefix: String },

    #[error("Cannot get zone '{name}': it is in the {state:?} state instead of running")]
    NotRunning { name: String, state: zone::State },

    #[error(
        "Cannot get zone '{name}': Failed to acquire control interface {err}"
    )]
    ControlInterface {
        name: String,
        #[source]
        err: crate::illumos::zone::GetControlInterfaceError,
    },

    #[error("Cannot get zone '{name}': Failed to create addrobj: {err}")]
    AddrObject {
        name: String,
        #[source]
        err: crate::illumos::addrobj::ParseError,
    },

    #[error(
        "Cannot get zone '{name}': Failed to ensure address exists: {err}"
    )]
    EnsureAddress {
        name: String,
        #[source]
        err: crate::illumos::zone::EnsureAddressError,
    },
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
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, RunCommandError>
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
        let output = crate::illumos::execute(cmd)
            .map_err(|err| RunCommandError { zone: name.to_string(), err })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.to_string())
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(zone: InstalledZone) -> Result<Self, BootError> {
        // Boot the zone.
        info!(zone.log, "Zone booting");

        Zones::boot(&zone.name)?;

        // Wait for the network services to come online, so future
        // requests to create addresses can operate immediately.
        let fmri = "svc:/milestone/network:default";
        wait_for_service(Some(&zone.name), fmri).await.map_err(|_| {
            BootError::Timeout {
                service: fmri.to_string(),
                zone: zone.name.to_string(),
            }
        })?;

        Ok(RunningZone { inner: zone })
    }

    pub async fn ensure_address(
        &self,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, EnsureAddressError> {
        info!(self.inner.log, "Adding address: {:?}", addrtype);
        let name = match addrtype {
            AddressRequest::Dhcp => "omicron",
            AddressRequest::Static(net) => match net.ip() {
                std::net::IpAddr::V4(_) => "omicron4",
                std::net::IpAddr::V6(_) => "omicron6",
            },
        };
        let addrobj = AddrObject::new(self.inner.control_vnic.name(), name)
            .map_err(|err| EnsureAddressError::AddrObject {
                request: addrtype,
                zone: self.inner.name.clone(),
                err,
            })?;
        let network =
            Zones::ensure_address(Some(&self.inner.name), &addrobj, addrtype)?;
        Ok(network)
    }

    pub async fn add_default_route(
        &self,
        gateway: Ipv6Addr,
    ) -> Result<(), RunCommandError> {
        self.run_cmd(&[
            "/usr/sbin/route",
            "add",
            "-inet6",
            "default",
            "-inet6",
            &gateway.to_string(),
        ])?;
        Ok(())
    }

    pub async fn add_default_route4(
        &self,
        gateway: Ipv4Addr,
    ) -> Result<(), RunCommandError> {
        self.run_cmd(&[
            "/usr/sbin/route",
            "add",
            "default",
            &gateway.to_string(),
        ])?;
        Ok(())
    }

    /// Looks up a running zone based on the `zone_prefix`, if one already exists.
    ///
    /// - If the zone was found, is running, and has a network interface, it is
    /// returned.
    /// - If the zone was not found `Error::NotFound` is returned.
    /// - If the zone was found, but not running, `Error::NotRunning` is
    /// returned.
    /// - Other errors may be returned attempting to look up and accessing an
    /// address on the zone.
    pub async fn get(
        log: &Logger,
        zone_prefix: &str,
        addrtype: AddressRequest,
    ) -> Result<Self, GetZoneError> {
        let zone_info = Zones::get()
            .map_err(|err| GetZoneError::GetZones {
                prefix: zone_prefix.to_string(),
                err,
            })?
            .into_iter()
            .find(|zone_info| zone_info.name().starts_with(&zone_prefix))
            .ok_or_else(|| GetZoneError::NotFound {
                prefix: zone_prefix.to_string(),
            })?;

        if zone_info.state() != zone::State::Running {
            return Err(GetZoneError::NotRunning {
                name: zone_info.name().to_string(),
                state: zone_info.state(),
            });
        }

        let zone_name = zone_info.name();
        let vnic_name =
            Zones::get_control_interface(zone_name).map_err(|err| {
                GetZoneError::ControlInterface {
                    name: zone_name.to_string(),
                    err,
                }
            })?;
        let addrobj = AddrObject::new_control(&vnic_name).map_err(|err| {
            GetZoneError::AddrObject { name: zone_name.to_string(), err }
        })?;
        Zones::ensure_address(Some(zone_name), &addrobj, addrtype).map_err(
            |err| GetZoneError::EnsureAddress {
                name: zone_name.to_string(),
                err,
            },
        )?;

        let control_vnic = Vnic::wrap_existing(vnic_name)
            .expect("Failed to wrap valid control VNIC");

        Ok(Self {
            inner: InstalledZone {
                log: log.new(o!("zone" => zone_name.to_string())),
                name: zone_name.to_string(),
                control_vnic,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725)
                //
                // Re-initialize guest_vnic state by inspecting the zone.
                opte_ports: vec![],
            },
        })
    }

    pub fn get_opte_ports(&self) -> &Vec<OptePort> {
        &self.inner.opte_ports
    }
}

impl Drop for RunningZone {
    fn drop(&mut self) {
        match Zones::halt_and_remove_logged(&self.inner.log, self.name()) {
            Ok(()) => {
                info!(self.inner.log, "Stopped and uninstalled zone")
            }
            Err(e) => {
                warn!(self.inner.log, "Failed to stop zone: {}", e)
            }
        }
    }
}

/// Errors returned from [`InstalledZone::install`].
#[derive(thiserror::Error, Debug)]
pub enum InstallZoneError {
    #[error("Cannot create '{service}': failed to create control VNIC: {err}")]
    CreateVnic {
        service: String,
        #[source]
        err: crate::illumos::dladm::CreateVnicError,
    },

    #[error("Failed to install zone '{zone}' from '{image_path}': {err}")]
    InstallZone {
        zone: String,
        image_path: PathBuf,
        #[source]
        err: crate::illumos::zone::AdmError,
    },
}

pub struct InstalledZone {
    log: Logger,

    // Name of the Zone.
    name: String,

    // NIC used for control plane communication.
    control_vnic: Vnic,

    // OPTE devices for the guest network interfaces
    opte_ports: Vec<OptePort>,
}

impl InstalledZone {
    /// Returns the name of a zone, based on the service name plus any unique
    /// identifying info.
    ///
    /// The zone name is based on:
    /// - A unique Oxide prefix ("oxz_")
    /// - The name of the service being hosted (e.g., "nexus")
    /// - An optional, service-unique identifier (typically a UUID).
    ///
    /// This results in a zone name which is distinct across different zpools,
    /// but stable and predictable across reboots.
    pub fn get_zone_name(
        service_name: &str,
        unique_name: Option<&str>,
    ) -> String {
        let mut zone_name = format!("{}{}", ZONE_PREFIX, service_name);
        if let Some(suffix) = unique_name {
            zone_name.push_str(&format!("_{}", suffix));
        }
        zone_name
    }

    pub async fn install(
        log: &Logger,
        vnic_allocator: &VnicAllocator,
        service_name: &str,
        unique_name: Option<&str>,
        datasets: &[zone::Dataset],
        devices: &[zone::Device],
        opte_ports: Vec<OptePort>,
    ) -> Result<InstalledZone, InstallZoneError> {
        let control_vnic = vnic_allocator.new_control(None).map_err(|err| {
            InstallZoneError::CreateVnic {
                service: service_name.to_string(),
                err,
            }
        })?;

        let zone_name = Self::get_zone_name(service_name, unique_name);
        let zone_image_path =
            PathBuf::from(&format!("/opt/oxide/{}.tar.gz", service_name));

        let net_device_names: Vec<String> = opte_ports
            .iter()
            .map(|port| port.vnic().name().to_string())
            .chain(std::iter::once(control_vnic.name().to_string()))
            .collect();

        Zones::install_omicron_zone(
            log,
            &zone_name,
            &zone_image_path,
            &datasets,
            &devices,
            net_device_names,
        )
        .map_err(|err| InstallZoneError::InstallZone {
            zone: zone_name.to_string(),
            image_path: zone_image_path.clone(),
            err,
        })?;

        Ok(InstalledZone {
            log: log.new(o!("zone" => zone_name.clone())),
            name: zone_name,
            control_vnic,
            opte_ports,
        })
    }
}
