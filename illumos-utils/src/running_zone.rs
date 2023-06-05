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

    #[error("Zone booted, but failed to find zone ID for zone {zone}")]
    NoZoneId { zone: String },

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

// Helper module for setting up and running `zone_enter()` for subprocesses run
// inside a non-global zone.
#[cfg(target_os = "illumos")]
mod zenter {
    use libc::zoneid_t;
    use std::ffi::c_int;
    use std::ffi::c_uint;
    use std::ffi::CStr;

    #[link(name = "contract")]
    extern "C" {
        fn ct_tmpl_set_critical(fd: c_int, events: c_uint) -> c_int;
        fn ct_tmpl_set_informative(fd: c_int, events: c_uint) -> c_int;
        fn ct_pr_tmpl_set_fatal(fd: c_int, events: c_uint) -> c_int;
        fn ct_pr_tmpl_set_param(fd: c_int, params: c_uint) -> c_int;
        fn ct_tmpl_activate(fd: c_int) -> c_int;
        fn ct_tmpl_clear(fd: c_int) -> c_int;
    }

    #[link(name = "c")]
    extern "C" {
        pub fn zone_enter(zid: zoneid_t) -> c_int;
    }

    // A Rust wrapper around the process contract template.
    #[derive(Debug)]
    pub struct Template {
        fd: c_int,
    }

    impl Drop for Template {
        fn drop(&mut self) {
            if unsafe { libc::close(self.fd) } != 0 {
                let e = std::io::Error::last_os_error();
                eprintln!("failed to close ctfs template file: {e}");
            }
        }
    }

    impl Template {
        const TEMPLATE_PATH: &[u8] = b"/system/contract/process/template\0";

        // Constants related to how the contract below is managed. See
        // `usr/src/uts/common/sys/contract/process.h` in the illumos sources
        // for details.

        // Process experienced an uncorrectable error.
        const CT_PR_EV_HWERR: c_uint = 0x20;
        // Only kill process group on fatal errors.
        const CT_PR_PGRPONLY: c_uint = 0x04;
        // Automatically detach inherited contracts.
        const CT_PR_REGENT: c_uint = 0x08;

        pub fn new() -> Result<Self, crate::ExecutionError> {
            let path = CStr::from_bytes_with_nul(Self::TEMPLATE_PATH).unwrap();
            let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR) };
            if fd < 0 {
                let err = std::io::Error::last_os_error();
                return Err(crate::ExecutionError::ZoneEnter { err });
            }

            // Initialize the contract template.
            //
            // No events are delivered, nothing is inherited, and we do not
            // allow the contract to be orphaned.
            //
            // See illumos sources in `usr/src/cmd/zlogin/zlogin.c` in the
            // implementation of `init_template()` for details.
            if unsafe { ct_tmpl_set_critical(fd, 0) } != 0
                || unsafe { ct_tmpl_set_informative(fd, 0) } != 0
                || unsafe { ct_pr_tmpl_set_fatal(fd, Self::CT_PR_EV_HWERR) }
                    != 0
                || unsafe {
                    ct_pr_tmpl_set_param(
                        fd,
                        Self::CT_PR_PGRPONLY | Self::CT_PR_REGENT,
                    )
                } != 0
                || unsafe { ct_tmpl_activate(fd) } != 0
            {
                let err = std::io::Error::last_os_error();
                return Err(crate::ExecutionError::ZoneEnter { err });
            }
            Ok(Self { fd })
        }

        pub fn clear(&self) {
            unsafe { ct_tmpl_clear(self.fd) };
        }
    }
}

/// Represents a running zone.
pub struct RunningZone {
    // The `zoneid_t` for the zone, while it's running, or `None` if not.
    id: Option<i32>,
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
    #[cfg(target_os = "illumos")]
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, RunCommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        use std::os::unix::process::CommandExt;
        let id = self.id.expect("Must have a zone ID for a running zone");
        let template =
            std::sync::Arc::new(zenter::Template::new().map_err(|err| {
                RunCommandError { zone: self.name().to_string(), err }
            })?);
        let tmpl = std::sync::Arc::clone(&template);
        let mut command = std::process::Command::new(crate::PFEXEC);
        command.env_clear();
        unsafe {
            command.pre_exec(move || {
                // Clear the template in the child, so that any other children
                // it forks itself use the normal contract.
                tmpl.clear();

                // Enter the target zone itself, in which the `exec()` call will
                // be made.
                if zenter::zone_enter(id) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let command = command.args(args);

        // Capture the result, and be sure to clear the template for this
        // process itself before returning.
        let res = crate::execute(command).map_err(|err| RunCommandError {
            zone: self.name().to_string(),
            err,
        });
        template.clear();
        res.map(|output| String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Runs a command within the Zone, return the output.
    #[cfg(not(target_os = "illumos"))]
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, RunCommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        // NOTE: This implementation is useless, and will never work. However,
        // it must actually call `crate::execute()` for the testing purposes.
        // That's mocked by `mockall` to return known data, and so the command
        // that's actually run is irrelevant.
        let command = std::process::Command::new("echo").args(args);
        crate::execute(command)
            .map_err(|err| RunCommandError {
                zone: self.name().to_string(),
                err,
            })
            .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
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

        // Pull the zone ID.
        let id = Zones::id(&zone.name)
            .await?
            .ok_or_else(|| BootError::NoZoneId { zone: zone.name.clone() })?;
        let site_profile_xml_exists =
            std::path::Path::new(&zone.site_profile_xml_path()).exists();
        let running_zone = RunningZone { id: Some(id), inner: zone };

        // Make sure the control vnic has an IP MTU of 9000 inside the zone
        const CONTROL_VNIC_MTU: usize = 9000;
        let vnic = running_zone.inner.control_vnic.name().to_string();

        // If the zone is self-assembling, then SMF service(s) inside the zone
        // will be creating the listen address for the zone's service(s). This
        // will create IP interfaces, and means that `create-if` here will fail
        // due to the interface already existing. Checking the output of
        // `show-if` is also problematic due to TOCTOU. Use the check for the
        // existence of site.xml, which means the zone is performing this
        // self-assembly, and skip create-if if so.

        if !site_profile_xml_exists {
            let args = vec![
                IPADM.to_string(),
                "create-if".to_string(),
                "-t".to_string(),
                vnic.clone(),
            ];

            running_zone.run_cmd(args)?;
        } else {
            // If the zone is self-assembling, then it's possible that the IP
            // interface does not exist yet because it has not been brought up
            // by the software in the zone. Run `create-if` here, but eat the
            // error if there is one: this is safe unless the software that's
            // part of self-assembly inside the zone is also trying to run
            // `create-if` (instead of `create-addr`), and required for the
            // `set-ifprop` commands below to pass.
            let args = vec![
                IPADM.to_string(),
                "create-if".to_string(),
                "-t".to_string(),
                vnic.clone(),
            ];

            let _result = running_zone.run_cmd(args);
        }

        let commands = vec![
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
            running_zone.run_cmd(args)?;
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
            id: zone_info.id().map(|x| {
                x.try_into().expect("zoneid_t is expected to be an i32")
            }),
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
        if let Some(_) = self.id.take() {
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
        if let Some(_) = self.id.take() {
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

    pub fn site_profile_xml_path(&self) -> Utf8PathBuf {
        let mut path: Utf8PathBuf = self.zonepath().into();
        path.push("root/var/svc/profile/site.xml");
        path
    }
}
