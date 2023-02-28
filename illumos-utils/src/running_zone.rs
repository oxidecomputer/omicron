// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::addrobj::AddrObject;
use crate::dladm::Etherstub;
use crate::link::{Link, VnicAllocator};
use crate::svc::wait_for_service;
use crate::zone::{AddressRequest, ZONE_PREFIX};
use ipnetwork::IpNetwork;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

#[cfg(test)]
use crate::zone::MockZones as Zones;
#[cfg(not(test))]
use crate::zone::Zones;

/// Trait required by the `Port` type passed to and held by [`RunningZone`] and
/// [`InstalledZone`].
pub trait OptePort {
    fn vnic_name(&self) -> &str;
}

/// Errors returned from [`RunningZone::run_cmd`].
#[derive(thiserror::Error, Debug)]
#[error("Error running command in zone '{zone}': {err}")]
pub struct RunCommandError {
    zone: String,
    #[source]
    err: crate::ExecutionError,
}

/// Errors returned from [`RunningZone::boot`].
#[derive(thiserror::Error, Debug)]
pub enum BootError {
    #[error("Error booting zone: {0}")]
    Booting(#[from] crate::zone::AdmError),

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
        err: crate::addrobj::ParseError,
    },

    #[error(transparent)]
    EnsureAddressError(#[from] crate::zone::EnsureAddressError),

    #[error(
        "Cannot allocate bootstrap {address} in {zone}: missing bootstrap vnic"
    )]
    MissingBootstrapVnic { address: String, zone: String },
}

/// Erros returned from [`RunningZone::get`].
#[derive(thiserror::Error, Debug)]
pub enum GetZoneError {
    #[error("While looking up zones with prefix '{prefix}', could not get zones: {err}")]
    GetZones {
        prefix: String,
        #[source]
        err: crate::zone::AdmError,
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
        err: crate::zone::GetControlInterfaceError,
    },

    #[error("Cannot get zone '{name}': Failed to create addrobj: {err}")]
    AddrObject {
        name: String,
        #[source]
        err: crate::addrobj::ParseError,
    },

    #[error(
        "Cannot get zone '{name}': Failed to ensure address exists: {err}"
    )]
    EnsureAddress {
        name: String,
        #[source]
        err: crate::zone::EnsureAddressError,
    },

    #[error(
        "Cannot get zone '{name}': Incorrect bootstrap interface access {err}"
    )]
    BootstrapInterface {
        name: String,
        #[source]
        err: crate::zone::GetBootstrapInterfaceError,
    },
}

/// Represents a running zone.
pub struct RunningZone<Port> {
    running: bool,
    inner: InstalledZone<Port>,
}

impl<Port> RunningZone<Port> {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Runs a command within the Zone, return the output.
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, RunCommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = std::process::Command::new(crate::PFEXEC);

        let name = self.name();
        let prefix = &[super::zone::ZLOGIN, name];
        let suffix: Vec<_> = args.into_iter().collect();
        let full_args = prefix
            .iter()
            .map(|s| std::ffi::OsStr::new(s))
            .chain(suffix.iter().map(|a| a.as_ref()));

        let cmd = command.args(full_args);
        let output = crate::execute(cmd)
            .map_err(|err| RunCommandError { zone: name.to_string(), err })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.to_string())
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(zone: InstalledZone<Port>) -> Result<Self, BootError> {
        // Boot the zone.
        info!(zone.log, "Zone booting");

        Zones::boot(&zone.name).await?;

        // Wait for the network services to come online, so future
        // requests to create addresses can operate immediately.
        let fmri = "svc:/milestone/network:default";
        wait_for_service(Some(&zone.name), fmri).await.map_err(|_| {
            BootError::Timeout {
                service: fmri.to_string(),
                zone: zone.name.to_string(),
            }
        })?;

        Ok(RunningZone { running: true, inner: zone })
    }

    pub async fn ensure_address(
        &self,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, EnsureAddressError> {
        let name = match addrtype {
            AddressRequest::Dhcp => "omicron",
            AddressRequest::Static(net) => match net.ip() {
                std::net::IpAddr::V4(_) => "omicron4",
                std::net::IpAddr::V6(_) => "omicron6",
            },
        };
        self.ensure_address_with_name(addrtype, name).await
    }

    pub async fn ensure_address_with_name(
        &self,
        addrtype: AddressRequest,
        name: &str,
    ) -> Result<IpNetwork, EnsureAddressError> {
        info!(self.inner.log, "Adding address: {:?}", addrtype);
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

    /// This is the API for creating a bootstrap address on the switch zone.
    pub async fn ensure_bootstrap_address(
        &self,
        address: Ipv6Addr,
    ) -> Result<(), EnsureAddressError> {
        info!(self.inner.log, "Adding bootstrap address");
        let vnic = self.inner.bootstrap_vnic.as_ref().ok_or_else(|| {
            EnsureAddressError::MissingBootstrapVnic {
                address: address.to_string(),
                zone: self.inner.name.clone(),
            }
        })?;
        let addrtype =
            AddressRequest::new_static(std::net::IpAddr::V6(address), None);
        let addrobj =
            AddrObject::new(vnic.name(), "bootstrap6").map_err(|err| {
                EnsureAddressError::AddrObject {
                    request: addrtype,
                    zone: self.inner.name.clone(),
                    err,
                }
            })?;
        let _ =
            Zones::ensure_address(Some(&self.inner.name), &addrobj, addrtype)?;
        Ok(())
    }

    // TODO: Remove once Nexus uses OPTE - external addresses should generally
    // be served via OPTE.
    pub async fn ensure_external_address_with_name(
        &self,
        addrtype: AddressRequest,
        name: &str,
    ) -> Result<IpNetwork, EnsureAddressError> {
        info!(self.inner.log, "Adding address: {:?}", addrtype);
        let addrobj = AddrObject::new(
                self.inner
                    .link
                    .as_ref()
                    .expect("Cannot allocate external address on zone without physical NIC")
                    .name(),
                name
            )
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
            .await
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

        let control_vnic = Link::wrap_existing(vnic_name)
            .expect("Failed to wrap valid control VNIC");

        // The bootstrap address for a running zone never changes,
        // so there's no need to call `Zones::ensure_address`.
        // Currently, only the switch zone has a bootstrap interface.
        let bootstrap_vnic = Zones::get_bootstrap_interface(zone_name)
            .map_err(|err| GetZoneError::BootstrapInterface {
                name: zone_name.to_string(),
                err,
            })?
            .map(|name| {
                Link::wrap_existing(name)
                    .expect("Failed to wrap valid bootstrap VNIC")
            });

        Ok(Self {
            running: true,
            inner: InstalledZone {
                log: log.new(o!("zone" => zone_name.to_string())),
                name: zone_name.to_string(),
                control_vnic,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725)
                //
                // Re-initialize guest_vnic state by inspecting the zone.
                opte_ports: vec![],
                link: None,
                bootstrap_vnic,
            },
        })
    }

    /// Return references to the OPTE ports for this zone.
    pub fn opte_ports(&self) -> &[Port] {
        &self.inner.opte_ports
    }

    /// Halts and removes the zone, awaiting its termination.
    ///
    /// Allows callers to synchronously stop a zone, and inspect an error.
    pub async fn stop(&mut self) -> Result<(), String> {
        if self.running {
            self.running = false;
            let log = self.inner.log.clone();
            let name = self.name().to_string();
            Zones::halt_and_remove_logged(&log, &name)
                .await
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

impl<Port> Drop for RunningZone<Port> {
    fn drop(&mut self) {
        if self.running {
            let log = self.inner.log.clone();
            let name = self.name().to_string();
            tokio::task::spawn(async move {
                match Zones::halt_and_remove_logged(&log, &name).await {
                    Ok(()) => {
                        info!(log, "Stopped and uninstalled zone")
                    }
                    Err(e) => {
                        warn!(log, "Failed to stop zone: {}", e)
                    }
                }
            });
        }
    }
}

/// Errors returned from [`InstalledZone::install`].
#[derive(thiserror::Error, Debug)]
pub enum InstallZoneError {
    #[error("Cannot create '{zone}': failed to create control VNIC: {err}")]
    CreateVnic {
        zone: String,
        #[source]
        err: crate::dladm::CreateVnicError,
    },

    #[error("Failed to install zone '{zone}' from '{image_path}': {err}")]
    InstallZone {
        zone: String,
        image_path: PathBuf,
        #[source]
        err: crate::zone::AdmError,
    },
}

pub struct InstalledZone<Port> {
    log: Logger,

    // Name of the Zone.
    name: String,

    // NIC used for control plane communication.
    control_vnic: Link,

    // Nic used for bootstrap network communication
    bootstrap_vnic: Option<Link>,

    // OPTE devices for the guest network interfaces
    opte_ports: Vec<Port>,

    // Physical NIC possibly provisioned to the zone.
    link: Option<Link>,
}

impl<Port> InstalledZone<Port> where Port: OptePort {
    /// Returns the name of a zone, based on the base zone name plus any unique
    /// identifying info.
    ///
    /// The zone name is based on:
    /// - A unique Oxide prefix ("oxz_")
    /// - The name of the zone type being hosted (e.g., "nexus")
    /// - An optional, zone-unique identifier (typically a UUID).
    ///
    /// This results in a zone name which is distinct across different zpools,
    /// but stable and predictable across reboots.
    pub fn get_zone_name(zone_name: &str, unique_name: Option<&str>) -> String {
        let mut zone_name = format!("{}{}", ZONE_PREFIX, zone_name);
        if let Some(suffix) = unique_name {
            zone_name.push_str(&format!("_{}", suffix));
        }
        zone_name
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn install(
        log: &Logger,
        underlay_vnic_allocator: &VnicAllocator<Etherstub>,
        zone_name: &str,
        unique_name: Option<&str>,
        datasets: &[zone::Dataset],
        devices: &[zone::Device],
        opte_ports: Vec<Port>,
        bootstrap_vnic: Option<Link>,
        link: Option<Link>,
        limit_priv: Vec<String>,
    ) -> Result<InstalledZone<Port>, InstallZoneError> {
        let control_vnic =
            underlay_vnic_allocator.new_control(None).map_err(|err| {
                InstallZoneError::CreateVnic {
                    zone: zone_name.to_string(),
                    err,
                }
            })?;

        let full_zone_name = Self::get_zone_name(zone_name, unique_name);
        let zone_image_path =
            PathBuf::from(&format!("/opt/oxide/{}.tar.gz", zone_name));

        let net_device_names: Vec<String> = opte_ports
            .iter()
            .map(|port| port.vnic_name().to_string())
            .chain(std::iter::once(control_vnic.name().to_string()))
            .chain(bootstrap_vnic.as_ref().map(|vnic| vnic.name().to_string()))
            .chain(link.as_ref().map(|vnic| vnic.name().to_string()))
            .collect();

        Zones::install_omicron_zone(
            log,
            &full_zone_name,
            &zone_image_path,
            &datasets,
            &devices,
            net_device_names,
            limit_priv,
        )
        .await
        .map_err(|err| InstallZoneError::InstallZone {
            zone: full_zone_name.to_string(),
            image_path: zone_image_path.clone(),
            err,
        })?;

        Ok(InstalledZone {
            log: log.new(o!("zone" => full_zone_name.clone())),
            name: full_zone_name,
            control_vnic,
            bootstrap_vnic,
            opte_ports,
            link,
        })
    }
}
