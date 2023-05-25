// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::addrobj::AddrObject;
use crate::dladm::Etherstub;
use crate::link::{Link, VnicAllocator};
use crate::opte::{Port, PortTicket};
use crate::svc::wait_for_service;
use crate::zone::{AddressRequest, IPADM, ZONE_PREFIX};
use camino::{Utf8Path, Utf8PathBuf};
use ipnetwork::IpNetwork;
use omicron_common::backoff;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[cfg(any(test, feature = "testing"))]
use crate::zone::MockZones as Zones;
#[cfg(not(any(test, feature = "testing")))]
use crate::zone::Zones;

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

    #[error("Zone booted, but running a command experienced an error: {0}")]
    RunCommandError(#[from] RunCommandError),
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

    #[error(transparent)]
    GetAddressesError(#[from] crate::zone::GetAddressesError),

    #[error("Failed ensuring link-local address in {zone}: {err}")]
    LinkLocal { zone: String, err: crate::ExecutionError },

    #[error("Failed to find non-link-local address in {zone}")]
    NoDhcpV6Addr { zone: String },

    #[error(
        "Cannot allocate bootstrap {address} in {zone}: missing bootstrap vnic"
    )]
    MissingBootstrapVnic { address: String, zone: String },

    #[error(
        "Failed ensuring address in {zone}: missing opte port ({port_idx})"
    )]
    MissingOptePort { zone: String, port_idx: usize },

    // TODO-remove(#2931): See comment in `ensure_address_for_port`
    #[error(transparent)]
    OpteGatewayConfig(#[from] RunCommandError),
}

/// Errors returned from [`RunningZone::get`].
#[derive(thiserror::Error, Debug)]
pub enum GetZoneError {
    #[error("While looking up zones with prefix '{prefix}', could not get zones: {err}")]
    GetZones {
        prefix: String,
        #[source]
        err: crate::zone::AdmError,
    },

    #[error("Invalid Utf8 path: {0}")]
    FromPathBuf(#[from] camino::FromPathBufError),

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
pub struct RunningZone {
    running: bool,
    inner: InstalledZone,
}

impl RunningZone {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the filesystem path to the zone's root
    pub fn root(&self) -> Utf8PathBuf {
        self.inner.zonepath.join("root")
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
    pub async fn boot(zone: InstalledZone) -> Result<Self, BootError> {
        // Boot the zone.
        info!(zone.log, "Zone booting");

        Zones::boot(&zone.name).await?;

        // Wait until the zone reaches the 'single-user' SMF milestone.
        // At this point, we know that the dependent
        //  - svc:/milestone/network
        //  - svc:/system/manifest-import
        // services are up, so future requests to create network addresses
        // or manipulate services will work.
        let fmri = "svc:/milestone/single-user:default";
        wait_for_service(Some(&zone.name), fmri).await.map_err(|_| {
            BootError::Timeout {
                service: fmri.to_string(),
                zone: zone.name.to_string(),
            }
        })?;

        let running_zone = RunningZone { running: true, inner: zone };

        // Make sure the control vnic has an IP MTU of 9000 inside the zone
        const CONTROL_VNIC_MTU: usize = 9000;
        let vnic = running_zone.inner.control_vnic.name().to_string();
        let commands = vec![
            vec![
                IPADM.to_string(),
                "create-if".to_string(),
                "-t".to_string(),
                vnic.clone(),
            ],
            vec![
                IPADM.to_string(),
                "set-ifprop".to_string(),
                "-t".to_string(),
                "-p".to_string(),
                format!("mtu={}", CONTROL_VNIC_MTU),
                "-m".to_string(),
                "ipv4".to_string(),
                vnic.clone(),
            ],
            vec![
                IPADM.to_string(),
                "set-ifprop".to_string(),
                "-t".to_string(),
                "-p".to_string(),
                format!("mtu={}", CONTROL_VNIC_MTU),
                "-m".to_string(),
                "ipv6".to_string(),
                vnic,
            ],
        ];

        for args in &commands {
            if let Err(e) = running_zone.run_cmd(args) {
                // `create-if` may fail if the vnic already has an interface.
                // eat that error and continue.
                if args[1] != "create-if" {
                    return Err(e.into());
                }
            }
        }

        Ok(running_zone)
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

    pub async fn ensure_address_for_port(
        &self,
        name: &str,
        port_idx: usize,
    ) -> Result<IpNetwork, EnsureAddressError> {
        info!(self.inner.log, "Ensuring address for OPTE port");
        let port = self.opte_ports().nth(port_idx).ok_or_else(|| {
            EnsureAddressError::MissingOptePort {
                zone: self.inner.name.clone(),
                port_idx,
            }
        })?;
        // TODO-remove(#2932): Switch to using port directly once vnic is no longer needed.
        let addrobj =
            AddrObject::new(port.vnic_name(), name).map_err(|err| {
                EnsureAddressError::AddrObject {
                    request: AddressRequest::Dhcp,
                    zone: self.inner.name.clone(),
                    err,
                }
            })?;
        let zone = Some(self.inner.name.as_ref());
        if let IpAddr::V4(gateway) = port.gateway().ip() {
            let addr =
                Zones::ensure_address(zone, &addrobj, AddressRequest::Dhcp)?;
            // TODO-remove(#2931): OPTE's DHCP "server" returns the list of routes
            // to add via option 121 (Classless Static Route). The illumos DHCP
            // client currently does not support this option, so we add the routes
            // manually here.
            let gateway_ip = gateway.to_string();
            let private_ip = addr.ip();
            self.run_cmd(&[
                "/usr/sbin/route",
                "add",
                "-host",
                &gateway_ip,
                &private_ip.to_string(),
                "-interface",
                "-ifp",
                port.vnic_name(),
            ])?;
            self.run_cmd(&[
                "/usr/sbin/route",
                "add",
                "-inet",
                "default",
                &gateway_ip,
            ])?;
            Ok(addr)
        } else {
            // If the port is using IPv6 addressing we still want it to use
            // DHCP(v6) which requires first creating a link-local address.
            Zones::ensure_has_link_local_v6_address(zone, &addrobj).map_err(
                |err| EnsureAddressError::LinkLocal {
                    zone: self.inner.name.clone(),
                    err,
                },
            )?;

            // Unlike DHCPv4, there's no blocking `ipadm` call we can
            // make as it just happens in the background. So we just poll
            // until we find a non link-local address.
            backoff::retry_notify(
                backoff::retry_policy_local(),
                || async {
                    // Grab all the address on the addrobj. There should
                    // always be at least one (the link-local we added)
                    let addrs = Zones::get_all_addresses(zone, &addrobj)
                        .map_err(|e| {
                            backoff::BackoffError::permanent(
                                EnsureAddressError::from(e),
                            )
                        })?;

                    // Ipv6Addr::is_unicast_link_local is sadly not stable
                    let is_ll =
                        |ip: Ipv6Addr| (ip.segments()[0] & 0xffc0) == 0xfe80;

                    // Look for a non link-local addr
                    addrs
                        .into_iter()
                        .find(|addr| match addr {
                            IpNetwork::V6(ip) => !is_ll(ip.ip()),
                            _ => false,
                        })
                        .ok_or_else(|| {
                            backoff::BackoffError::transient(
                                EnsureAddressError::NoDhcpV6Addr {
                                    zone: self.inner.name.clone(),
                                },
                            )
                        })
                },
                |error, delay| {
                    slog::debug!(
                        self.inner.log,
                        "No non link-local address yet (retrying in {:?})",
                        delay;
                        "error" => ?error
                    );
                },
            )
            .await
        }
    }

    pub fn add_default_route(
        &self,
        gateway: Ipv6Addr,
    ) -> Result<(), RunCommandError> {
        self.run_cmd([
            "/usr/sbin/route",
            "add",
            "-inet6",
            "default",
            "-inet6",
            &gateway.to_string(),
        ])?;
        Ok(())
    }

    pub fn add_default_route4(
        &self,
        gateway: Ipv4Addr,
    ) -> Result<(), RunCommandError> {
        self.run_cmd([
            "/usr/sbin/route",
            "add",
            "default",
            &gateway.to_string(),
        ])?;
        Ok(())
    }

    pub fn add_bootstrap_route(
        &self,
        bootstrap_prefix: u16,
        gz_bootstrap_addr: Ipv6Addr,
        zone_vnic_name: &str,
    ) -> Result<(), RunCommandError> {
        self.run_cmd([
            "/usr/sbin/route",
            "add",
            "-inet6",
            &format!("{bootstrap_prefix:x}::/16"),
            &gz_bootstrap_addr.to_string(),
            "-ifp",
            zone_vnic_name,
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
        vnic_allocator: &VnicAllocator<Etherstub>,
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

        let control_vnic = vnic_allocator
            .wrap_existing(vnic_name)
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
                vnic_allocator
                    .wrap_existing(name)
                    .expect("Failed to wrap valid bootstrap VNIC")
            });

        Ok(Self {
            running: true,
            inner: InstalledZone {
                log: log.new(o!("zone" => zone_name.to_string())),
                zonepath: zone_info.path().to_path_buf().try_into()?,
                name: zone_name.to_string(),
                control_vnic,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725)
                //
                // Re-initialize guest_vnic state by inspecting the zone.
                opte_ports: vec![],
                links: vec![],
                bootstrap_vnic,
            },
        })
    }

    /// Return references to the OPTE ports for this zone.
    pub fn opte_ports(&self) -> impl Iterator<Item = &Port> {
        self.inner.opte_ports.iter().map(|(port, _)| port)
    }

    /// Remove the OPTE ports on this zone from the port manager.
    pub fn release_opte_ports(&mut self) {
        for (_, ticket) in self.inner.opte_ports.drain(..) {
            ticket.release();
        }
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

    pub fn links(&self) -> &Vec<Link> {
        &self.inner.links
    }
}

impl Drop for RunningZone {
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
        image_path: Utf8PathBuf,
        #[source]
        err: crate::zone::AdmError,
    },

    #[error("Failed to find zone image '{image}' from {paths:?}")]
    ImageNotFound { image: String, paths: Vec<Utf8PathBuf> },
}

pub struct InstalledZone {
    log: Logger,

    // Filesystem path of the zone
    zonepath: Utf8PathBuf,

    // Name of the Zone.
    name: String,

    // NIC used for control plane communication.
    control_vnic: Link,

    // Nic used for bootstrap network communication
    bootstrap_vnic: Option<Link>,

    // OPTE devices for the guest network interfaces
    opte_ports: Vec<(Port, PortTicket)>,

    // Physical NICs possibly provisioned to the zone.
    links: Vec<Link>,
}

impl InstalledZone {
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
    pub fn get_zone_name(zone_type: &str, unique_name: Option<&str>) -> String {
        let mut zone_name = format!("{}{}", ZONE_PREFIX, zone_type);
        if let Some(suffix) = unique_name {
            zone_name.push_str(&format!("_{}", suffix));
        }
        zone_name
    }

    pub fn get_control_vnic_name(&self) -> &str {
        self.control_vnic.name()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the filesystem path to the zonepath
    pub fn zonepath(&self) -> &Utf8Path {
        &self.zonepath
    }

    // TODO: This would benefit from a "builder-pattern" interface.
    #[allow(clippy::too_many_arguments)]
    pub async fn install(
        log: &Logger,
        underlay_vnic_allocator: &VnicAllocator<Etherstub>,
        zone_root_path: &Utf8Path,
        zone_image_paths: &[Utf8PathBuf],
        zone_type: &str,
        unique_name: Option<&str>,
        datasets: &[zone::Dataset],
        filesystems: &[zone::Fs],
        devices: &[zone::Device],
        opte_ports: Vec<(Port, PortTicket)>,
        bootstrap_vnic: Option<Link>,
        links: Vec<Link>,
        limit_priv: Vec<String>,
    ) -> Result<InstalledZone, InstallZoneError> {
        let control_vnic =
            underlay_vnic_allocator.new_control(None).map_err(|err| {
                InstallZoneError::CreateVnic {
                    zone: zone_type.to_string(),
                    err,
                }
            })?;

        let full_zone_name = Self::get_zone_name(zone_type, unique_name);

        // Looks for the image within `zone_image_path`, in order.
        let image = format!("{}.tar.gz", zone_type);
        let zone_image_path = zone_image_paths
            .iter()
            .find_map(|image_path| {
                let path = image_path.join(&image);
                if path.exists() {
                    Some(path)
                } else {
                    None
                }
            })
            .ok_or_else(|| InstallZoneError::ImageNotFound {
                image: image.to_string(),
                paths: zone_image_paths
                    .iter()
                    .map(|p| p.to_path_buf())
                    .collect(),
            })?;

        let net_device_names: Vec<String> = opte_ports
            .iter()
            .map(|(port, _)| port.vnic_name().to_string())
            .chain(std::iter::once(control_vnic.name().to_string()))
            .chain(bootstrap_vnic.as_ref().map(|vnic| vnic.name().to_string()))
            .chain(links.iter().map(|nic| nic.name().to_string()))
            .collect();

        Zones::install_omicron_zone(
            log,
            &zone_root_path,
            &full_zone_name,
            &zone_image_path,
            datasets,
            filesystems,
            devices,
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
            zonepath: zone_root_path.join(&full_zone_name),
            name: full_zone_name,
            control_vnic,
            bootstrap_vnic,
            opte_ports,
            links,
        })
    }
}
