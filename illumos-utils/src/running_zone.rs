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
use camino_tempfile::Utf8TempDir;
use ipnetwork::IpNetwork;
use omicron_common::backoff;
use slog::{error, info, o, warn, Logger};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
#[cfg(target_os = "illumos")]
use std::sync::OnceLock;
#[cfg(target_os = "illumos")]
use std::thread;
use uuid::Uuid;

#[cfg(any(test, feature = "testing"))]
use crate::zone::MockZones as Zones;
#[cfg(not(any(test, feature = "testing")))]
use crate::zone::Zones;

/// Errors returned from methods for fetching SMF services and log files
#[derive(thiserror::Error, Debug)]
pub enum ServiceError {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Failed to run a command")]
    RunCommand(#[from] RunCommandError),
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

#[cfg(target_os = "illumos")]
static REAPER_THREAD: OnceLock<thread::JoinHandle<()>> = OnceLock::new();

#[cfg(target_os = "illumos")]
pub fn ensure_contract_reaper(log: &Logger) {
    info!(log, "Ensuring contract reaper thread");
    REAPER_THREAD.get_or_init(|| {
        let log = log.new(o!("component" => "ContractReaper"));
        std::thread::spawn(move || zenter::contract_reaper(log))
    });
}

#[cfg(not(target_os = "illumos"))]
pub fn ensure_contract_reaper(log: &Logger) {
    info!(log, "Not illumos, skipping contract reaper thread");
}

// Helper module for setting up and running `zone_enter()` for subprocesses run
// inside a non-global zone.
#[cfg(target_os = "illumos")]
mod zenter {
    use libc::ctid_t;
    use libc::zoneid_t;
    use slog::{debug, error, Logger};
    use std::ffi::c_int;
    use std::ffi::c_uint;
    use std::ffi::c_void;
    use std::ffi::{CStr, CString};
    use std::process;
    use std::thread;
    use std::time::Duration;

    #[allow(non_camel_case_types)]
    type ct_evthdl_t = *mut c_void;

    #[link(name = "contract")]
    extern "C" {
        fn ct_tmpl_set_critical(fd: c_int, events: c_uint) -> c_int;
        fn ct_tmpl_set_informative(fd: c_int, events: c_uint) -> c_int;
        fn ct_pr_tmpl_set_fatal(fd: c_int, events: c_uint) -> c_int;
        fn ct_pr_tmpl_set_param(fd: c_int, params: c_uint) -> c_int;
        fn ct_tmpl_activate(fd: c_int) -> c_int;
        fn ct_tmpl_clear(fd: c_int) -> c_int;
        fn ct_ctl_abandon(fd: c_int) -> c_int;
        fn ct_event_read_critical(fd: c_int, ev: *mut ct_evthdl_t) -> c_int;
        fn ct_event_get_type(ev: ct_evthdl_t) -> u64;
        fn ct_event_get_ctid(ev: ct_evthdl_t) -> ctid_t;
        fn ct_event_free(ev: ct_evthdl_t);
    }

    #[link(name = "c")]
    extern "C" {
        pub fn zone_enter(zid: zoneid_t) -> c_int;
    }

    // This thread watches for critical events coming from all process
    // contracts held by sled-agent, and reaps (abandons) contracts which
    // become empty. Process contracts are used in conjunction with
    // zone_enter() in order to run commands within non-global zones, and
    // the contracts used for this come from templates that define becoming
    // empty as a critical event.
    pub fn contract_reaper(log: Logger) {
        const EVENT_PATH: &'static [u8] = b"/system/contract/process/pbundle";
        const CT_PR_EV_EMPTY: u64 = 1;

        let cpath = CString::new(EVENT_PATH).unwrap();
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };

        if fd < 0 {
            panic!(
                "Could not open {:?}: {}",
                cpath,
                std::io::Error::last_os_error()
            );
        }

        loop {
            let mut ev: ct_evthdl_t = std::ptr::null_mut();
            let evp: *mut ct_evthdl_t = &mut ev;
            // The event endpoint was not opened as non-blocking, so
            // ct_event_read_critical(3CONTRACT) will block until a new
            // critical event is available on the channel.
            match unsafe { ct_event_read_critical(fd, evp) } {
                0 => {
                    let typ = unsafe { ct_event_get_type(ev) };
                    if typ == CT_PR_EV_EMPTY {
                        let ctid = unsafe { ct_event_get_ctid(ev) };
                        match abandon_contract(ctid) {
                            Err(e) => error!(
                                &log,
                                "Failed to abandon contract {}: {}", ctid, e
                            ),
                            Ok(_) => {
                                debug!(&log, "Abandoned contract {}", ctid)
                            }
                        }
                    }
                    unsafe { ct_event_free(ev) };
                }
                err => {
                    // ct_event_read_critical(3CONTRACT) does not state any
                    // error values for this function if the file descriptor
                    // was not opened non-blocking, but inspection of the
                    // library code shows that various errnos could be returned
                    // in situations such as failure to allocate memory. In
                    // those cases, log a message and pause to avoid entering a
                    // tight loop if the problem persists.
                    error!(
                        &log,
                        "Unexpected response from contract event channel: {}",
                        std::io::Error::from_raw_os_error(err)
                    );
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    #[derive(thiserror::Error, Debug)]
    pub enum AbandonContractError {
        #[error("Error opening file {file}: {error}")]
        Open { file: String, error: std::io::Error },

        #[error("Error abandoning contract {ctid}: {error}")]
        Abandon { ctid: ctid_t, error: std::io::Error },

        #[error("Error closing file {file}: {error}")]
        Close { file: String, error: std::io::Error },
    }

    pub fn abandon_contract(ctid: ctid_t) -> Result<(), AbandonContractError> {
        let path = format!("/proc/{}/contracts/{}/ctl", process::id(), ctid);

        let cpath = CString::new(path.clone()).unwrap();
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_WRONLY) };
        if fd < 0 {
            return Err(AbandonContractError::Open {
                file: path,
                error: std::io::Error::last_os_error(),
            });
        }
        let ret = unsafe { ct_ctl_abandon(fd) };
        if ret != 0 {
            unsafe { libc::close(fd) };
            return Err(AbandonContractError::Abandon {
                ctid,
                error: std::io::Error::from_raw_os_error(ret),
            });
        }
        if unsafe { libc::close(fd) } != 0 {
            return Err(AbandonContractError::Close {
                file: path,
                error: std::io::Error::last_os_error(),
            });
        }

        Ok(())
    }

    // A Rust wrapper around the process contract template.
    #[derive(Debug)]
    pub struct Template {
        fd: c_int,
    }

    impl Drop for Template {
        fn drop(&mut self) {
            self.clear();
            // Ignore any error, since printing may interfere with `slog`'s
            // structured output.
            unsafe { libc::close(self.fd) };
        }
    }

    impl Template {
        const TEMPLATE_PATH: &'static [u8] =
            b"/system/contract/process/template\0";

        // Constants related to how the contract below is managed. See
        // `usr/src/uts/common/sys/contract/process.h` in the illumos sources
        // for details.

        // Contract has become empty.
        const CT_PR_EV_EMPTY: c_uint = 0x1;
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
                return Err(crate::ExecutionError::ContractFailure { err });
            }

            // Initialize the contract template.
            //
            // Nothing is inherited, we do not allow the contract to be
            // orphaned, and the only event which is delivered is EV_EMPTY,
            // indicating that the contract has become empty. These events are
            // consumed by contract_reaper() above.
            //
            // See illumos sources in `usr/src/cmd/zlogin/zlogin.c` in the
            // implementation of `init_template()` for details.
            if unsafe { ct_tmpl_set_critical(fd, Self::CT_PR_EV_EMPTY) } != 0
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
                return Err(crate::ExecutionError::ContractFailure { err });
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
    /// The path to the zone's root filesystem (i.e., `/`), within zonepath.
    pub const ROOT_FS_PATH: &'static str = "root";

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the filesystem path to the zone's root in the GZ.
    pub fn root(&self) -> Utf8PathBuf {
        self.inner.zonepath.join(Self::ROOT_FS_PATH)
    }

    pub fn control_interface(&self) -> AddrObject {
        AddrObject::new(self.inner.get_control_vnic_name(), "omicron6").unwrap()
    }

    /// Runs a command within the Zone, return the output.
    //
    // NOTE: It's important that this function is synchronous.
    //
    // Internally, we're setting the (thread-local) contract template before
    // forking a child to exec the command inside the target zone. In order for
    // that to all work correctly, that template must be set and then later
    // cleared in the _same_ OS thread. An async method here would open the
    // possibility that the template is set in some thread, and then cleared in
    // another, if the task is swapped out at an await point. That would leave
    // the first thread's template in a modified state.
    //
    // If we do need to make this method asynchronous, we will need to change
    // the internals to avoid changing the thread's contract. One possible
    // approach here would be to use `libscf` directly, rather than `exec`-ing
    // `svccfg` directly in a forked child. That would obviate the need to work
    // on the contract at all.
    #[cfg(target_os = "illumos")]
    pub fn run_cmd<I, S>(&self, args: I) -> Result<String, RunCommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        use std::os::unix::process::CommandExt;
        let Some(id) = self.id else {
            return Err(RunCommandError {
                zone: self.name().to_string(),
                err: crate::ExecutionError::NotRunning,
            });
        };
        let template =
            std::sync::Arc::new(zenter::Template::new().map_err(|err| {
                RunCommandError { zone: self.name().to_string(), err }
            })?);
        let tmpl = std::sync::Arc::clone(&template);
        let mut command = std::process::Command::new(crate::PFEXEC);
        let logger = self.inner.log.clone();
        let zone = self.name().to_string();
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
                    let err = std::io::Error::last_os_error();
                    error!(
                        logger,
                        "failed to enter zone: {}", &err;
                        "zone" => &zone,
                    );
                    Err(err)
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
        let mut command = std::process::Command::new("echo");
        let command = command.args(args);
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
        wait_for_service(Some(&zone.name), fmri, zone.log.clone())
            .await
            .map_err(|_| BootError::Timeout {
                service: fmri.to_string(),
                zone: zone.name.to_string(),
            })?;

        // If the zone is self-assembling, then SMF service(s) inside the zone
        // will be creating the listen address for the zone's service(s),
        // setting the appropriate ifprop MTU, and so on. The idea behind
        // self-assembling zones is that once they boot there should be *no*
        // zlogin required.

        // Use the zone ID in order to check if /var/svc/profile/site.xml
        // exists.
        let id = Zones::id(&zone.name)
            .await?
            .ok_or_else(|| BootError::NoZoneId { zone: zone.name.clone() })?;
        let site_profile_xml_exists =
            std::path::Path::new(&zone.site_profile_xml_path()).exists();

        let running_zone = RunningZone { id: Some(id), inner: zone };

        if !site_profile_xml_exists {
            // If the zone is not self-assembling, make sure the control vnic
            // has an IP MTU of 9000 inside the zone.
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
                running_zone.run_cmd(args)?;
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

    /// Return the running processes associated with all the SMF services this
    /// zone is intended to run.
    pub fn service_processes(
        &self,
    ) -> Result<Vec<ServiceProcess>, ServiceError> {
        let service_names = self.service_names()?;
        let mut services = Vec::with_capacity(service_names.len());
        for service_name in service_names.into_iter() {
            let output = self.run_cmd(["ptree", "-s", &service_name])?;

            // All Oxide SMF services currently run a single binary, though it
            // may be run in a contract via `ctrun`. We don't care about that
            // binary, but any others we _do_ want to collect data from.
            for line in output.lines() {
                if line.contains("ctrun") {
                    continue;
                }
                let line = line.trim();
                let mut parts = line.split_ascii_whitespace();

                // The first two parts should be the PID and the process binary
                // path, respectively.
                let Some(pid_s) = parts.next() else {
                    error!(
                        self.inner.log,
                        "failed to get service PID from ptree output";
                        "service" => &service_name,
                    );
                    continue;
                };
                let Ok(pid) = pid_s.parse() else {
                    error!(
                        self.inner.log,
                        "failed to parse service PID from ptree output";
                        "service" => &service_name,
                        "pid" => pid_s,
                    );
                    continue;
                };
                let Some(path) = parts.next() else {
                    error!(
                        self.inner.log,
                        "failed to get service binary from ptree output";
                        "service" => &service_name,
                    );
                    continue;
                };
                let binary = Utf8PathBuf::from(path);

                let Some(log_file) = self.service_log_file(&service_name)?
                else {
                    error!(
                        self.inner.log,
                        "failed to find log file for existing service";
                        "service_name" => &service_name,
                    );
                    continue;
                };

                services.push(ServiceProcess {
                    service_name: service_name.clone(),
                    binary,
                    pid,
                    log_file,
                });
            }
        }
        Ok(services)
    }

    /// Return the names of the Oxide SMF services this zone is intended to run.
    pub fn service_names(&self) -> Result<Vec<String>, ServiceError> {
        let output = self.run_cmd(&["svcs", "-H", "-o", "fmri"])?;
        Ok(output
            .lines()
            .filter(|line| is_oxide_smf_service(line))
            .map(|line| line.trim().to_string())
            .collect())
    }

    /// Return any SMF log file associated with the named service.
    ///
    /// Given a named service, this returns the path of the current log file.
    /// This can be used to find rotated or archived log files, but keep in mind
    /// this returns only the current, if it exists.
    pub fn service_log_file(
        &self,
        name: &str,
    ) -> Result<Option<Utf8PathBuf>, ServiceError> {
        let output = self.run_cmd(&["svcs", "-L", name])?;
        let mut lines = output.lines();
        let Some(current) = lines.next() else {
            return Ok(None);
        };
        return Ok(Some(Utf8PathBuf::from(current.trim())));
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

/// A process running in the zone associated with an SMF service.
#[derive(Clone, Debug)]
pub struct ServiceProcess {
    /// The name of the SMF service.
    pub service_name: String,
    /// The path of the binary in the process image.
    pub binary: Utf8PathBuf,
    /// The PID of the process.
    pub pid: u32,
    /// The path for the current log file.
    pub log_file: Utf8PathBuf,
}

/// Errors returned from [`ZoneBuilder::install`].
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

    #[error("Attempted to call install() on underspecified ZoneBuilder")]
    IncompleteBuilder,
}

pub struct InstalledZone {
    log: Logger,

    // Filesystem path of the zone
    zonepath: Utf8PathBuf,

    // Name of the Zone.
    name: String,

    // NIC used for control plane communication.
    control_vnic: Link,

    // NIC used for bootstrap network communication
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
    /// - An optional, zone-unique UUID
    ///
    /// This results in a zone name which is distinct across different zpools,
    /// but stable and predictable across reboots.
    pub fn get_zone_name(zone_type: &str, unique_name: Option<Uuid>) -> String {
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

    pub fn site_profile_xml_path(&self) -> Utf8PathBuf {
        let mut path: Utf8PathBuf = self.zonepath().into();
        path.push("root/var/svc/profile/site.xml");
        path
    }
}

#[derive(Clone)]
pub struct FakeZoneBuilderConfig {
    temp_dir: Arc<Utf8TempDir>,
}

#[derive(Clone, Default)]
pub struct ZoneBuilderFactory {
    // Why this is part of this builder/factory and not some separate builder
    // type: At time of writing, to the best of my knowledge:
    // - If we want builder pattern, we need to return some type of `Self`.
    // - If we have a trait that returns `Self` type, we can't turn it into a
    //   trait object (i.e. Box<dyn ZoneBuilderFactoryInterface>).
    // - Plumbing concrete types as generics through every other type that
    //   needs to construct zones (and anything else with a lot of parameters)
    //   seems like a worse idea.
    fake_cfg: Option<FakeZoneBuilderConfig>,
}

impl ZoneBuilderFactory {
    /// For use in unit tests that don't require actual zone creation to occur.
    pub fn fake() -> Self {
        Self {
            fake_cfg: Some(FakeZoneBuilderConfig {
                temp_dir: Arc::new(Utf8TempDir::new().unwrap()),
            }),
        }
    }

    /// Create a [ZoneBuilder] that inherits this factory's fakeness.
    pub fn builder<'a>(&self) -> ZoneBuilder<'a> {
        ZoneBuilder { fake_cfg: self.fake_cfg.clone(), ..Default::default() }
    }
}

/// Builder-pattern construct for creating an [InstalledZone].
/// Created by [ZoneBuilderFactory].
#[derive(Default)]
pub struct ZoneBuilder<'a> {
    log: Option<Logger>,
    underlay_vnic_allocator: Option<&'a VnicAllocator<Etherstub>>,
    zone_root_path: Option<&'a Utf8Path>,
    zone_image_paths: Option<&'a [Utf8PathBuf]>,
    zone_type: Option<&'a str>,
    unique_name: Option<Uuid>, // actually optional
    datasets: Option<&'a [zone::Dataset]>,
    filesystems: Option<&'a [zone::Fs]>,
    data_links: Option<&'a [String]>,
    devices: Option<&'a [zone::Device]>,
    opte_ports: Option<Vec<(Port, PortTicket)>>,
    bootstrap_vnic: Option<Link>, // actually optional
    links: Option<Vec<Link>>,
    limit_priv: Option<Vec<String>>,
    fake_cfg: Option<FakeZoneBuilderConfig>,
}

impl<'a> ZoneBuilder<'a> {
    pub fn with_log(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    pub fn with_underlay_vnic_allocator(
        mut self,
        vnic_allocator: &'a VnicAllocator<Etherstub>,
    ) -> Self {
        self.underlay_vnic_allocator = Some(vnic_allocator);
        self
    }

    pub fn with_zone_root_path(mut self, root_path: &'a Utf8Path) -> Self {
        self.zone_root_path = Some(root_path);
        self
    }

    pub fn with_zone_image_paths(
        mut self,
        image_paths: &'a [Utf8PathBuf],
    ) -> Self {
        self.zone_image_paths = Some(image_paths);
        self
    }

    pub fn with_zone_type(mut self, zone_type: &'a str) -> Self {
        self.zone_type = Some(zone_type);
        self
    }

    pub fn with_unique_name(mut self, uuid: Uuid) -> Self {
        self.unique_name = Some(uuid);
        self
    }

    pub fn with_datasets(mut self, datasets: &'a [zone::Dataset]) -> Self {
        self.datasets = Some(datasets);
        self
    }

    pub fn with_filesystems(mut self, filesystems: &'a [zone::Fs]) -> Self {
        self.filesystems = Some(filesystems);
        self
    }

    pub fn with_data_links(mut self, links: &'a [String]) -> Self {
        self.data_links = Some(links);
        self
    }

    pub fn with_devices(mut self, devices: &'a [zone::Device]) -> Self {
        self.devices = Some(devices);
        self
    }

    pub fn with_opte_ports(mut self, ports: Vec<(Port, PortTicket)>) -> Self {
        self.opte_ports = Some(ports);
        self
    }

    pub fn with_bootstrap_vnic(mut self, vnic: Link) -> Self {
        self.bootstrap_vnic = Some(vnic);
        self
    }

    pub fn with_links(mut self, links: Vec<Link>) -> Self {
        self.links = Some(links);
        self
    }

    pub fn with_limit_priv(mut self, limit_priv: Vec<String>) -> Self {
        self.limit_priv = Some(limit_priv);
        self
    }

    fn fake_install(self) -> Result<InstalledZone, InstallZoneError> {
        let zone = self
            .zone_type
            .ok_or(InstallZoneError::IncompleteBuilder)?
            .to_string();
        let control_vnic = self
            .underlay_vnic_allocator
            .ok_or(InstallZoneError::IncompleteBuilder)?
            .new_control(None)
            .map_err(move |err| InstallZoneError::CreateVnic { zone, err })?;
        let fake_cfg = self.fake_cfg.unwrap();
        let temp_dir = fake_cfg.temp_dir.path().to_path_buf();
        (|| {
            let full_zone_name = InstalledZone::get_zone_name(
                self.zone_type?,
                self.unique_name,
            );
            let zonepath = temp_dir
                .join(self.zone_root_path?.strip_prefix("/").unwrap())
                .join(&full_zone_name);
            let iz = InstalledZone {
                log: self.log?,
                zonepath,
                name: full_zone_name,
                control_vnic,
                bootstrap_vnic: self.bootstrap_vnic,
                opte_ports: self.opte_ports?,
                links: self.links?,
            };
            let xml_path = iz.site_profile_xml_path().parent()?.to_path_buf();
            std::fs::create_dir_all(&xml_path)
                .unwrap_or_else(|_| panic!("ZoneBuilder::fake_install couldn't create site profile xml path {:?}", xml_path));
            Some(iz)
        })()
        .ok_or(InstallZoneError::IncompleteBuilder)
    }

    pub async fn install(self) -> Result<InstalledZone, InstallZoneError> {
        if self.fake_cfg.is_some() {
            return self.fake_install();
        }

        let Self {
            log: Some(log),
            underlay_vnic_allocator: Some(underlay_vnic_allocator),
            zone_root_path: Some(zone_root_path),
            zone_image_paths: Some(zone_image_paths),
            zone_type: Some(zone_type),
            unique_name,
            datasets: Some(datasets),
            filesystems: Some(filesystems),
            data_links: Some(data_links),
            devices: Some(devices),
            opte_ports: Some(opte_ports),
            bootstrap_vnic,
            links: Some(links),
            limit_priv: Some(limit_priv),
            ..
        } = self
        else {
            return Err(InstallZoneError::IncompleteBuilder);
        };

        let control_vnic =
            underlay_vnic_allocator.new_control(None).map_err(|err| {
                InstallZoneError::CreateVnic {
                    zone: zone_type.to_string(),
                    err,
                }
            })?;

        let full_zone_name =
            InstalledZone::get_zone_name(zone_type, unique_name);

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

        let mut net_device_names: Vec<String> = opte_ports
            .iter()
            .map(|(port, _)| port.vnic_name().to_string())
            .chain(std::iter::once(control_vnic.name().to_string()))
            .chain(bootstrap_vnic.as_ref().map(|vnic| vnic.name().to_string()))
            .chain(links.iter().map(|nic| nic.name().to_string()))
            .chain(data_links.iter().map(|x| x.to_string()))
            .collect();

        // There are many sources for device names. In some cases they can
        // overlap, depending on the contents of user defined config files. This
        // can cause zones to fail to start if duplicate data links are given.
        net_device_names.sort();
        net_device_names.dedup();

        Zones::install_omicron_zone(
            &log,
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

/// Return true if the service with the given FMRI appears to be an
/// Oxide-managed service.
pub fn is_oxide_smf_service(fmri: impl AsRef<str>) -> bool {
    const SMF_SERVICE_PREFIXES: [&str; 2] =
        ["svc:/oxide/", "svc:/system/illumos/"];
    let fmri = fmri.as_ref();
    SMF_SERVICE_PREFIXES.iter().any(|prefix| fmri.starts_with(prefix))
}

/// Return true if the provided file name appears to be a valid log file for an
/// Oxide-managed SMF service.
///
/// Note that this operates on the _file name_. Any leading path components will
/// cause this check to return `false`.
pub fn is_oxide_smf_log_file(filename: impl AsRef<str>) -> bool {
    // Log files are named by the SMF services, with the `/` in the FMRI
    // translated to a `-`.
    const PREFIXES: [&str; 2] = ["oxide-", "system-illumos-"];
    let filename = filename.as_ref();
    PREFIXES
        .iter()
        .any(|prefix| filename.starts_with(prefix) && filename.contains(".log"))
}

#[cfg(test)]
mod tests {
    use super::is_oxide_smf_log_file;
    use super::is_oxide_smf_service;

    #[test]
    fn test_is_oxide_smf_service() {
        assert!(is_oxide_smf_service("svc:/oxide/blah:default"));
        assert!(is_oxide_smf_service("svc:/system/illumos/blah:default"));
        assert!(!is_oxide_smf_service("svc:/system/blah:default"));
        assert!(!is_oxide_smf_service("svc:/not/oxide/blah:default"));
    }

    #[test]
    fn test_is_oxide_smf_log_file() {
        assert!(is_oxide_smf_log_file("oxide-blah:default.log"));
        assert!(is_oxide_smf_log_file("oxide-blah:default.log.0"));
        assert!(is_oxide_smf_log_file("oxide-blah:default.log.1111"));
        assert!(is_oxide_smf_log_file("system-illumos-blah:default.log"));
        assert!(is_oxide_smf_log_file("system-illumos-blah:default.log.0"));
        assert!(!is_oxide_smf_log_file("not-oxide-blah:default.log"));
        assert!(!is_oxide_smf_log_file("not-system-illumos-blah:default.log"));
    }
}
