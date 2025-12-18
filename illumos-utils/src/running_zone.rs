// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to manage running zones.

use crate::addrobj::{
    AddrObject, DHCP_ADDROBJ_NAME, IPV4_STATIC_ADDROBJ_NAME,
    IPV6_STATIC_ADDROBJ_NAME,
};
#[cfg(target_os = "illumos")]
use crate::contract;
use crate::dladm::Etherstub;
use crate::link::{Link, VnicAllocator};
use crate::opte::{Port, PortTicket};
use crate::zone::AddressRequest;
use crate::zone::Zones;
use crate::zpool::{PathInPool, ZpoolOrRamdisk};
use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::Utf8TempDir;
use debug_ignore::DebugIgnore;
use ipnetwork::IpNetwork;
use omicron_common::backoff;
use omicron_common::zone_images::ZoneImageFileSource;
use omicron_uuid_kinds::OmicronZoneUuid;
pub use oxlog::is_oxide_smf_log_file;
use slog::{Logger, error, info, o, warn};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
#[cfg(target_os = "illumos")]
use std::sync::OnceLock;
#[cfg(target_os = "illumos")]
use std::thread;

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
    pub err: crate::ExecutionError,
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
    #[error(
        "Failed ensuring address {request:?} in {zone}: could not construct addrobj name: {err}"
    )]
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

#[cfg(target_os = "illumos")]
static REAPER_THREAD: OnceLock<thread::JoinHandle<()>> = OnceLock::new();

#[cfg(target_os = "illumos")]
pub fn ensure_contract_reaper(log: &Logger) {
    info!(log, "Ensuring contract reaper thread");
    REAPER_THREAD.get_or_init(|| {
        let log = log.new(o!("component" => "ContractReaper"));
        std::thread::spawn(move || contract::process_contract_reaper(log))
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
    use libc::zoneid_t;
    use std::ffi::c_int;

    #[link(name = "c")]
    unsafe extern "C" {
        pub fn zone_enter(zid: zoneid_t) -> c_int;
    }
}

/// Represents a running zone.
#[derive(Debug)]
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
        self.inner.root()
    }

    /// Returns the zpool on which the filesystem path has been placed.
    pub fn root_zpool(&self) -> &ZpoolOrRamdisk {
        &self.inner.zonepath.pool
    }

    /// Returns the zone's image path.
    pub fn image_path(&self) -> &Utf8Path {
        &self.inner.image_path
    }

    /// Return the name of a bootstrap VNIC in the zone, if any.
    pub fn bootstrap_vnic_name(&self) -> Option<&str> {
        self.inner.get_bootstrap_vnic_name()
    }

    /// Return the name of the control VNIC.
    pub fn control_vnic_name(&self) -> &str {
        self.inner.get_control_vnic_name()
    }

    /// Return the names of any OPTE ports in the zone.
    pub fn opte_port_names(&self) -> impl Iterator<Item = &str> {
        self.inner.opte_ports().map(|port| port.name())
    }

    /// Return the control IP address.
    pub fn control_interface(&self) -> AddrObject {
        AddrObject::new(
            self.inner.get_control_vnic_name(),
            IPV6_STATIC_ADDROBJ_NAME,
        )
        .unwrap()
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
        let template = std::sync::Arc::new(
            contract::Template::new(contract::ContractType::Process).map_err(
                |err| RunCommandError { zone: self.name().to_string(), err },
            )?,
        );
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
        let all_args = args
            .into_iter()
            .map(|arg| arg.as_ref().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        panic!(
            "Attempting to run a host OS command on a non-illumos platform: {all_args:?}"
        );
    }

    /// Boots a new zone.
    ///
    /// Note that the zone must already be configured to be booted.
    pub async fn boot(zone: InstalledZone) -> Result<Self, BootError> {
        // Boot the zone.
        info!(zone.log, "Booting {} zone", zone.name);

        zone.zones_api.boot(&zone.name).await?;

        // Wait until the zone reaches the 'single-user' SMF milestone.
        // At this point, we know that the dependent
        //  - svc:/milestone/network
        //  - svc:/system/manifest-import
        // services are up, so future requests to create network addresses
        // or manipulate services will work.
        let fmri = "svc:/milestone/single-user:default";
        zone.zones_api
            .wait_for_service(Some(&zone.name), fmri, zone.log.clone())
            .await
            .map_err(|_| BootError::Timeout {
                service: fmri.to_string(),
                zone: zone.name.to_string(),
            })?;

        let id =
            zone.zones_api.id(&zone.name).await?.ok_or_else(|| {
                BootError::NoZoneId { zone: zone.name.clone() }
            })?;

        let running_zone = RunningZone { id: Some(id), inner: zone };

        Ok(running_zone)
    }

    /// Create a fake running zone for use in tests.
    #[cfg(feature = "testing")]
    pub fn fake_boot(zone_id: i32, zone: InstalledZone) -> Self {
        RunningZone { id: Some(zone_id), inner: zone }
    }

    pub async fn ensure_address(
        &self,
        addrtype: AddressRequest,
    ) -> Result<IpNetwork, EnsureAddressError> {
        let name = match addrtype {
            AddressRequest::Dhcp => DHCP_ADDROBJ_NAME,
            AddressRequest::Static(net) => match net.ip() {
                std::net::IpAddr::V4(_) => IPV4_STATIC_ADDROBJ_NAME,
                std::net::IpAddr::V6(_) => IPV6_STATIC_ADDROBJ_NAME,
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
            Zones::ensure_address(Some(&self.inner.name), &addrobj, addrtype)
                .await?;
        Ok(network)
    }

    // TODO-completeness: Handle dual-stack OPTE ports here. This works for
    // either IPv4 or IPv6 addresses, but not both.
    // See https://github.com/oxidecomputer/omicron/issues/9247.
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
        let addrobj = AddrObject::new(port.name(), name).map_err(|err| {
            EnsureAddressError::AddrObject {
                request: AddressRequest::Dhcp,
                zone: self.inner.name.clone(),
                err,
            }
        })?;
        let zone = Some(self.inner.name.as_ref());
        if let Some(gateway) = port.gateway().ipv4_addr() {
            let addr =
                Zones::ensure_address(zone, &addrobj, AddressRequest::Dhcp)
                    .await?;
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
                port.name(),
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
            Zones::ensure_has_link_local_v6_address(zone, &addrobj)
                .await
                .map_err(|err| EnsureAddressError::LinkLocal {
                    zone: self.inner.name.clone(),
                    err,
                })?;

            // Unlike DHCPv4, there's no blocking `ipadm` call we can
            // make as it just happens in the background. So we just poll
            // until we find a non link-local address.
            backoff::retry_notify(
                backoff::retry_policy_local(),
                || async {
                    // Grab all the address on the addrobj. There should
                    // always be at least one (the link-local we added)
                    let addrs = Zones::get_all_addresses(zone, &addrobj)
                        .await
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
        let args = [
            "/usr/sbin/route",
            "add",
            "-inet6",
            &format!("{bootstrap_prefix:x}::/16"),
            &gz_bootstrap_addr.to_string(),
            "-ifp",
            zone_vnic_name,
        ];
        self.run_cmd(args)?;
        Ok(())
    }

    /// Return references to the OPTE ports for this zone.
    pub fn opte_ports(&self) -> impl Iterator<Item = &Port> {
        self.inner.opte_ports()
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
            self.inner
                .zones_api
                .halt_and_remove_logged(&log, &name)
                .await
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    /// Return a reference to the links for this zone.
    pub fn links(&self) -> &Vec<Link> {
        &self.inner.links()
    }

    /// Return a mutable reference to the links for this zone.
    pub fn links_mut(&mut self) -> &mut Vec<Link> {
        &mut self.inner.links
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
            let zones_api = self.inner.zones_api.clone();
            tokio::task::spawn(async move {
                match zones_api.halt_and_remove_logged(&log, &name).await {
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

    #[error(
        "Failed to find zone image '{}' from {:?}",
        file_source.file_name,
        file_source.search_paths,
    )]
    ImageNotFound { file_source: ZoneImageFileSource },
    #[error("Attempted to call install() on underspecified ZoneBuilder")]
    IncompleteBuilder,
}

#[derive(Debug)]
pub struct InstalledZone {
    log: Logger,

    // Filesystem path of the zone
    zonepath: PathInPool,

    // The path to the zone's image source.
    image_path: Utf8PathBuf,

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

    // API to underlying zone commands
    zones_api: DebugIgnore<Arc<dyn crate::zone::Api>>,
}

impl InstalledZone {
    /// The path to the zone's root filesystem (i.e., `/`), within zonepath.
    pub const ROOT_FS_PATH: &'static str = "root";

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
    pub fn get_zone_name(
        zone_type: &str,
        unique_name: Option<OmicronZoneUuid>,
    ) -> String {
        crate::zone::zone_name(zone_type, unique_name)
    }

    /// Get the name of the bootstrap VNIC in the zone, if any.
    pub fn get_bootstrap_vnic_name(&self) -> Option<&str> {
        self.bootstrap_vnic.as_ref().map(|link| link.name())
    }

    /// Get the name of the control VNIC in the zone.
    pub fn get_control_vnic_name(&self) -> &str {
        self.control_vnic.name()
    }

    /// Return the name of the zone itself.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the filesystem path to the zonepath
    pub fn zonepath(&self) -> &Utf8Path {
        &self.zonepath.path
    }

    pub fn site_profile_xml_path(&self) -> Utf8PathBuf {
        let mut path: Utf8PathBuf = self.zonepath().into();
        path.push("root/var/svc/profile/site.xml");
        path
    }

    /// Returns references to the OPTE ports for this zone.
    pub fn opte_ports(&self) -> impl Iterator<Item = &Port> {
        self.opte_ports.iter().map(|(port, _)| port)
    }

    /// Returns the filesystem path to the zone's root in the GZ.
    pub fn root(&self) -> Utf8PathBuf {
        self.zonepath.path.join(Self::ROOT_FS_PATH)
    }

    /// Return a reference to the links for this zone.
    pub fn links(&self) -> &Vec<Link> {
        &self.links
    }
}

#[derive(Clone)]
pub struct FakeZoneBuilderConfig {
    temp_dir: Arc<Utf8PathBuf>,
}

#[derive(Clone)]
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
    zones_api: Arc<dyn crate::zone::Api>,
}

impl ZoneBuilderFactory {
    pub fn new() -> Self {
        Self {
            fake_cfg: None,
            zones_api: Arc::new(crate::zone::Zones::real_api()),
        }
    }

    /// For use in unit tests that don't require actual zone creation to occur.
    pub fn fake(
        temp_dir: Option<&str>,
        zones_api: Arc<dyn crate::zone::Api>,
    ) -> Self {
        let temp_dir = match temp_dir {
            Some(dir) => Utf8PathBuf::from(dir),
            None => Utf8TempDir::new().unwrap().keep(),
        };
        Self {
            fake_cfg: Some(FakeZoneBuilderConfig {
                temp_dir: Arc::new(temp_dir),
            }),
            zones_api,
        }
    }

    pub fn zones_api(&self) -> &Arc<dyn crate::zone::Api> {
        &self.zones_api
    }

    /// Create a [ZoneBuilder] that inherits this factory's fakeness.
    pub fn builder<'a>(&self) -> ZoneBuilder<'a> {
        ZoneBuilder {
            fake_cfg: self.fake_cfg.clone(),
            zones_api: Some(self.zones_api.clone()),
            ..Default::default()
        }
    }
}

/// Builder-pattern construct for creating an [InstalledZone].
/// Created by [ZoneBuilderFactory].
#[derive(Default)]
pub struct ZoneBuilder<'a> {
    /// Logger to which status messages are written during zone installation.
    log: Option<Logger>,
    /// Allocates the NIC used for control plane communication.
    underlay_vnic_allocator: Option<&'a VnicAllocator<Etherstub>>,
    /// Filesystem path at which the installed zone will reside.
    zone_root_path: Option<PathInPool>,
    /// The file source.
    file_source: Option<&'a ZoneImageFileSource>,
    /// The name of the type of zone being created (e.g. "propolis-server")
    zone_type: Option<&'a str>,
    /// Unique ID of the instance of the zone being created. (optional)
    // *actually* optional (in contrast to other fields that are `Option` for
    // builder purposes - that is, skipping this field in the builder will
    // still result in an `Ok(InstalledZone)` from `.install()`, rather than
    // an `Err(InstallZoneError::IncompleteBuilder)`.
    unique_name: Option<OmicronZoneUuid>,
    /// ZFS datasets to be accessed from within the zone.
    datasets: Option<&'a [zone::Dataset]>,
    /// Filesystems to mount within the zone.
    filesystems: Option<&'a [zone::Fs]>,
    /// Additional network device names to add to the zone.
    data_links: Option<&'a [String]>,
    /// Device nodes to pass through to the zone.
    devices: Option<&'a [zone::Device]>,
    /// OPTE devices for the guest network interfaces.
    opte_ports: Option<Vec<(Port, PortTicket)>>,
    /// NIC to use for creating a bootstrap address on the switch zone.
    // actually optional (as above)
    bootstrap_vnic: Option<Link>,
    /// Physical NICs possibly provisioned to the zone.
    links: Option<Vec<Link>>,
    /// The maximum set of privileges any process in this zone can obtain.
    limit_priv: Option<Vec<String>>,
    /// For unit tests only: if `Some`, then no actual zones will be installed
    /// by this builder, and minimal facsimiles of them will be placed in
    /// temporary directories according to the contents of the provided
    /// `FakeZoneBuilderConfig`.
    fake_cfg: Option<FakeZoneBuilderConfig>,

    zones_api: Option<Arc<dyn crate::zone::Api>>,
}

impl<'a> ZoneBuilder<'a> {
    /// Logger to which status messages are written during zone installation.
    pub fn with_log(mut self, log: Logger) -> Self {
        self.log = Some(log);
        self
    }

    /// Allocates the NIC used for control plane communication.
    pub fn with_underlay_vnic_allocator(
        mut self,
        vnic_allocator: &'a VnicAllocator<Etherstub>,
    ) -> Self {
        self.underlay_vnic_allocator = Some(vnic_allocator);
        self
    }

    /// Filesystem path at which the installed zone will reside.
    pub fn with_zone_root_path(mut self, root_path: PathInPool) -> Self {
        self.zone_root_path = Some(root_path);
        self
    }

    /// The file name and image source.
    pub fn with_file_source(
        mut self,
        file_source: &'a ZoneImageFileSource,
    ) -> Self {
        self.file_source = Some(file_source);
        self
    }

    /// The name of the type of zone being created (e.g. "propolis-server")
    pub fn with_zone_type(mut self, zone_type: &'a str) -> Self {
        self.zone_type = Some(zone_type);
        self
    }

    /// Unique ID of the instance of the zone being created. (optional)
    pub fn with_unique_name(mut self, uuid: OmicronZoneUuid) -> Self {
        self.unique_name = Some(uuid);
        self
    }

    /// ZFS datasets to be accessed from within the zone.
    pub fn with_datasets(mut self, datasets: &'a [zone::Dataset]) -> Self {
        self.datasets = Some(datasets);
        self
    }

    /// Filesystems to mount within the zone.
    pub fn with_filesystems(mut self, filesystems: &'a [zone::Fs]) -> Self {
        self.filesystems = Some(filesystems);
        self
    }

    /// Additional network device names to add to the zone.
    pub fn with_data_links(mut self, links: &'a [String]) -> Self {
        self.data_links = Some(links);
        self
    }

    /// Device nodes to pass through to the zone.
    pub fn with_devices(mut self, devices: &'a [zone::Device]) -> Self {
        self.devices = Some(devices);
        self
    }

    /// OPTE devices for the guest network interfaces.
    pub fn with_opte_ports(mut self, ports: Vec<(Port, PortTicket)>) -> Self {
        self.opte_ports = Some(ports);
        self
    }

    /// NIC to use for creating a bootstrap address on the switch zone.
    /// (optional)
    pub fn with_bootstrap_vnic(mut self, vnic: Link) -> Self {
        self.bootstrap_vnic = Some(vnic);
        self
    }

    /// Physical NICs possibly provisioned to the zone.
    pub fn with_links(mut self, links: Vec<Link>) -> Self {
        self.links = Some(links);
        self
    }

    /// The maximum set of privileges any process in this zone can obtain.
    pub fn with_limit_priv(mut self, limit_priv: Vec<String>) -> Self {
        self.limit_priv = Some(limit_priv);
        self
    }

    // (used in unit tests)
    async fn fake_install(mut self) -> Result<InstalledZone, InstallZoneError> {
        let zones_api = self.zones_api.take().unwrap();
        let zone = self
            .zone_type
            .ok_or(InstallZoneError::IncompleteBuilder)?
            .to_string();
        let control_vnic = self
            .underlay_vnic_allocator
            .ok_or(InstallZoneError::IncompleteBuilder)?
            .new_control(None)
            .await
            .map_err(move |err| InstallZoneError::CreateVnic { zone, err })?;
        let fake_cfg = self.fake_cfg.unwrap();
        let temp_dir = fake_cfg.temp_dir;
        (|| {
            let zone_type = self.zone_type?;
            let full_zone_name = InstalledZone::get_zone_name(
                zone_type,
                self.unique_name,
            );
            let mut zonepath = self.zone_root_path?;
            zonepath.path = temp_dir
                .join(
                    zonepath.path.strip_prefix("/").unwrap()
                )
                .join(&full_zone_name);
            let iz = InstalledZone {
                log: self.log?,
                zonepath,
                image_path: Utf8PathBuf::from(format!("/fake/image/path/{zone_type}.tar.gz")),
                name: full_zone_name,
                control_vnic,
                bootstrap_vnic: self.bootstrap_vnic,
                opte_ports: self.opte_ports?,
                links: self.links?,
                zones_api: DebugIgnore(zones_api),
            };
            let xml_path = iz.site_profile_xml_path().parent()?.to_path_buf();
            std::fs::create_dir_all(&xml_path)
                .unwrap_or_else(|_| panic!("ZoneBuilder::fake_install couldn't create site profile xml path {:?}", xml_path));
            Some(iz)
        })()
        .ok_or(InstallZoneError::IncompleteBuilder)
    }

    /// Create the zone with the provided parameters.
    /// Returns `Err(InstallZoneError::IncompleteBuilder)` if a necessary
    /// parameter was not provided.
    pub async fn install(mut self) -> Result<InstalledZone, InstallZoneError> {
        if self.fake_cfg.is_some() {
            return self.fake_install().await;
        }

        let Self {
            log: Some(log),
            underlay_vnic_allocator: Some(underlay_vnic_allocator),
            zone_root_path: Some(mut zone_root_path),
            file_source: Some(file_source),
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

        let control_vnic = underlay_vnic_allocator
            .new_control(None)
            .await
            .map_err(|err| InstallZoneError::CreateVnic {
                zone: zone_type.to_string(),
                err,
            })?;

        let full_zone_name =
            InstalledZone::get_zone_name(zone_type, unique_name);

        // Look for the image within `file_source.search_paths`, in order.
        let zone_image_path = file_source
            .search_paths
            .iter()
            .find_map(|image_path| {
                let path = image_path.join(&file_source.file_name);
                if path.exists() { Some(path) } else { None }
            })
            .ok_or_else(|| InstallZoneError::ImageNotFound {
                file_source: file_source.clone(),
            })?;

        let mut net_device_names: Vec<String> = opte_ports
            .iter()
            .map(|(port, _)| port.name().to_string())
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

        zone_root_path.path = zone_root_path.path.join(&full_zone_name);

        let zones_api = self.zones_api.take().unwrap();
        zones_api
            .install_omicron_zone(
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
            zonepath: zone_root_path,
            image_path: zone_image_path,
            name: full_zone_name,
            control_vnic,
            bootstrap_vnic,
            opte_ports,
            links,
            zones_api: DebugIgnore(zones_api),
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

#[cfg(test)]
mod tests {
    use super::is_oxide_smf_service;

    #[test]
    fn test_is_oxide_smf_service() {
        assert!(is_oxide_smf_service("svc:/oxide/blah:default"));
        assert!(is_oxide_smf_service("svc:/system/illumos/blah:default"));
        assert!(!is_oxide_smf_service("svc:/system/blah:default"));
        assert!(!is_oxide_smf_service("svc:/not/oxide/blah:default"));
    }
}
