// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-local service management.
//!
//! For controlling zone-based storage services, refer to
//! [crate::storage_manager::StorageManager].
//!
//! For controlling virtual machine instances, refer to
//! [crate::instance_manager::InstanceManager].
//!
//! The [ServiceManager] provides separate mechanisms for services where the
//! "source-of-truth" is Nexus, compared with services where the
//! "source-of-truth" is the Sled Agent itself. Although we generally prefer to
//! delegate the decision of "which services run where" to Nexus, there are
//! situations where the Sled Agent must be capable of autonomously ensuring
//! that zones execute. For example, the "switch zone" contains services which
//! should automatically start when a Tofino device is detected, independently
//! of what other services Nexus wants to have executing on the sled.
//!
//! To accomplish this, the following interfaces are exposed:
//! - [ServiceManager::ensure_all_services] exposes an API to request a set of
//! services that should persist beyond reboot.
//! - [ServiceManager::activate_switch] exposes an API to specifically enable
//! or disable (via [ServiceManager::deactivate_switch]) the switch zone.

use crate::ledger::{Ledger, Ledgerable};
use crate::params::{
    DendriteAsic, ServiceEnsureBody, ServiceType, ServiceZoneRequest,
    ServiceZoneService, TimeSync, ZoneType,
};
use crate::profile::*;
use crate::smf_helper::Service;
use crate::smf_helper::SmfHelper;
use crate::storage_manager::StorageManager;
use camino::{Utf8Path, Utf8PathBuf};
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use dpd_client::{types as DpdTypes, Client as DpdClient, Error as DpdError};
use illumos_utils::addrobj::AddrObject;
use illumos_utils::addrobj::IPV6_LINK_LOCAL_NAME;
use illumos_utils::dladm::{Dladm, Etherstub, EtherstubVnic, PhysicalLink};
use illumos_utils::link::{Link, VnicAllocator};
use illumos_utils::opte::{Port, PortManager, PortTicket};
use illumos_utils::running_zone::{InstalledZone, RunningZone};
use illumos_utils::zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT;
use illumos_utils::zone::AddressRequest;
use illumos_utils::zone::Zones;
use illumos_utils::{execute, PFEXEC};
use internal_dns::resolver::Resolver;
use itertools::Itertools;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::AZ_PREFIX;
use omicron_common::address::BOOTSTRAP_ARTIFACT_PORT;
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::address::COCKROACH_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::CRUCIBLE_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::OXIMETER_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::WICKETD_PORT;
use omicron_common::api::external::Generation;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, retry_policy_local,
    BackoffError,
};
use omicron_common::nexus_config::{
    self, DeploymentConfig as NexusDeploymentConfig,
};
use once_cell::sync::OnceCell;
use sled_hardware::is_gimlet;
use sled_hardware::underlay;
use sled_hardware::underlay::BOOTSTRAP_PREFIX;
use sled_hardware::Baseboard;
use sled_hardware::SledMode;
use slog::Logger;
use std::collections::HashSet;
use std::iter;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize TOML to file {path}: {err}")]
    TomlSerialize { path: Utf8PathBuf, err: toml::ser::Error },

    #[error("Cannot deserialize TOML from file {path}: {err}")]
    TomlDeserialize { path: Utf8PathBuf, err: toml::de::Error },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to find device {device}")]
    MissingDevice { device: String },

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] crate::ledger::Error),

    #[error("Sled Agent not initialized yet")]
    SledAgentNotReady,

    #[error("Sled-local zone error: {0}")]
    SledLocalZone(anyhow::Error),

    #[error("Failed to issue SMF command: {0}")]
    SmfCommand(#[from] crate::smf_helper::Error),

    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: illumos_utils::running_zone::RunCommandError,
    },

    #[error("Failed to boot zone: {0}")]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Failed to access underlay device: {0}")]
    Underlay(#[from] underlay::Error),

    #[error("Failed to create OPTE port for service {service}: {err}")]
    ServicePortCreation {
        service: String,
        err: Box<illumos_utils::opte::Error>,
    },

    #[error("Error contacting dpd: {0}")]
    DpdError(#[from] DpdError<DpdTypes::Error>),

    #[error("Failed to create Vnic in sled-local zone: {0}")]
    SledLocalVnicCreation(illumos_utils::dladm::CreateVnicError),

    #[error("Failed to add GZ addresses: {message}: {err}")]
    GzAddress {
        message: String,
        err: illumos_utils::zone::EnsureGzAddressError,
    },

    #[error("Could not initialize service {service} as requested: {message}")]
    BadServiceRequest { service: String, message: String },

    #[error("Services already configured for this Sled Agent")]
    ServicesAlreadyConfigured,

    #[error("NTP zone not ready")]
    NtpZoneNotReady,

    #[error("Execution error: {0}")]
    ExecutionError(#[from] illumos_utils::ExecutionError),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns::resolver::ResolveError),
}

impl Error {
    fn io(message: &str, err: std::io::Error) -> Self {
        Self::Io { message: message.to_string(), err }
    }
    fn io_path(path: &Utf8Path, err: std::io::Error) -> Self {
        Self::Io { message: format!("Error accessing {path}"), err }
    }
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

/// Configuration parameters which modify the [`ServiceManager`]'s behavior.
pub struct Config {
    /// Identifies the sled being configured
    pub sled_id: Uuid,

    /// Identifies the revision of the sidecar to be used.
    pub sidecar_revision: String,
}

impl Config {
    pub fn new(sled_id: Uuid, sidecar_revision: String) -> Self {
        Self { sled_id, sidecar_revision }
    }
}

// The filename of the ledger, within the provided directory.
const SERVICES_LEDGER_FILENAME: &str = "services.toml";
const STORAGE_SERVICES_LEDGER_FILENAME: &str = "storage-services.toml";

// A wrapper around `ZoneRequest`, which allows it to be serialized
// to a toml file.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct AllZoneRequests {
    generation: Generation,
    requests: Vec<ZoneRequest>,
}

impl Default for AllZoneRequests {
    fn default() -> Self {
        Self { generation: Generation::new(), requests: vec![] }
    }
}

impl Ledgerable for AllZoneRequests {
    fn is_newer_than(&self, other: &AllZoneRequests) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation = self.generation.next();
    }
}

// This struct represents the combo of "what zone did you ask for" + "where did
// we put it".
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ZoneRequest {
    zone: ServiceZoneRequest,
    // TODO: Consider collapsing "root" into ServiceZoneRequest
    root: Utf8PathBuf,
}

struct Task {
    // A signal for the initializer task to terminate
    exit_tx: oneshot::Sender<()>,
    // A task repeatedly trying to initialize the zone
    initializer: JoinHandle<()>,
}

impl Task {
    async fn stop(self) {
        // If this succeeds, we told the background task to exit
        // successfully. If it fails, the background task already
        // exited.
        let _ = self.exit_tx.send(());
        self.initializer.await.expect("Switch initializer task panicked");
    }
}

/// Describes the state of a sled-local zone.
enum SledLocalZone {
    // The zone is not currently running.
    Disabled,
    // The zone is still initializing - it may be awaiting the initialization
    // of certain links.
    Initializing {
        // The request for the zone
        request: ServiceZoneRequest,
        // A background task which keeps looping until the zone is initialized
        worker: Option<Task>,
        // Filesystems for the switch zone to mount
        // Since SoftNPU is currently managed via a UNIX socket, we need to
        // pass those files in to the SwitchZone so Dendrite can manage SoftNPU
        filesystems: Vec<zone::Fs>,
    },
    // The Zone is currently running.
    Running {
        // The original request for the zone
        request: ServiceZoneRequest,
        // The currently running zone
        zone: RunningZone,
    },
}

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    global_zone_bootstrap_link_local_address: Ipv6Addr,
    switch_zone: Mutex<SledLocalZone>,
    sled_mode: SledMode,
    skip_timesync: Option<bool>,
    time_synced: AtomicBool,
    sidecar_revision: String,
    switch_zone_maghemite_links: Vec<PhysicalLink>,
    // Zones representing running services
    zones: Mutex<Vec<RunningZone>>,
    // Zones representing services which own datasets
    dataset_zones: Mutex<Vec<RunningZone>>,
    underlay_vnic_allocator: VnicAllocator<Etherstub>,
    underlay_vnic: EtherstubVnic,
    bootstrap_vnic_allocator: VnicAllocator<Etherstub>,
    ddmd_client: DdmAdminClient,
    advertised_prefixes: Mutex<HashSet<Ipv6Subnet<SLED_PREFIX>>>,
    sled_info: OnceCell<SledAgentInfo>,
    switch_zone_bootstrap_address: Ipv6Addr,
    // TODO(https://github.com/oxidecomputer/omicron/issues/2888): We will
    // need this interface to provision Zone filesystems on explicit U.2s,
    // rather than simply placing them on the ramdisk.
    storage: StorageManager,
    ledger_directory_override: OnceCell<Utf8PathBuf>,
}

// Late-binding information, only known once the sled agent is up and
// operational.
struct SledAgentInfo {
    config: Config,
    port_manager: PortManager,
    resolver: Resolver,
    underlay_address: Ipv6Addr,
    rack_id: Uuid,
}

#[derive(Clone)]
pub struct ServiceManager {
    inner: Arc<ServiceManagerInner>,
}

impl ServiceManager {
    /// Creates a service manager.
    ///
    /// Args:
    /// - `log`: The logger
    /// - `underlay_etherstub`: Etherstub used to allocate service vNICs.
    /// - `underlay_vnic`: The underlay's vNIC in the Global Zone.
    /// - `bootstrap_etherstub`: Etherstub used to allocate bootstrap service vNICs.
    /// - `sled_mode`: The sled's mode of operation (Gimlet vs Scrimlet).
    /// - `skip_timesync`: If true, the sled always reports synced time.
    /// - `time_synced`: If true, time sync was achieved.
    /// - `sidecar_revision`: Rev of attached sidecar, if present.
    /// - `switch_zone_bootstrap_address`: The bootstrap IP to use for the switch zone.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        log: Logger,
        global_zone_bootstrap_link_local_address: Ipv6Addr,
        underlay_etherstub: Etherstub,
        underlay_vnic: EtherstubVnic,
        bootstrap_etherstub: Etherstub,
        sled_mode: SledMode,
        skip_timesync: Option<bool>,
        sidecar_revision: String,
        switch_zone_bootstrap_address: Ipv6Addr,
        switch_zone_maghemite_links: Vec<PhysicalLink>,
        storage: StorageManager,
    ) -> Result<Self, Error> {
        debug!(log, "Creating new ServiceManager");
        let log = log.new(o!("component" => "ServiceManager"));
        let mgr = Self {
            inner: Arc::new(ServiceManagerInner {
                log: log.clone(),
                global_zone_bootstrap_link_local_address,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725):
                // Load the switch zone if it already exists?
                switch_zone: Mutex::new(SledLocalZone::Disabled),
                sled_mode,
                skip_timesync,
                time_synced: AtomicBool::new(false),
                sidecar_revision,
                switch_zone_maghemite_links,
                zones: Mutex::new(vec![]),
                dataset_zones: Mutex::new(vec![]),
                underlay_vnic_allocator: VnicAllocator::new(
                    "Service",
                    underlay_etherstub,
                ),
                underlay_vnic,
                bootstrap_vnic_allocator: VnicAllocator::new(
                    "Bootstrap",
                    bootstrap_etherstub,
                ),
                ddmd_client: DdmAdminClient::localhost(&log)?,
                advertised_prefixes: Mutex::new(HashSet::new()),
                sled_info: OnceCell::new(),
                switch_zone_bootstrap_address,
                storage,
                ledger_directory_override: OnceCell::new(),
            }),
        };
        Ok(mgr)
    }

    #[cfg(test)]
    async fn override_ledger_directory(&self, path: Utf8PathBuf) {
        self.inner.ledger_directory_override.set(path).unwrap();
    }

    pub fn switch_zone_bootstrap_address(&self) -> Ipv6Addr {
        self.inner.switch_zone_bootstrap_address
    }

    async fn all_service_ledgers(&self) -> Vec<Utf8PathBuf> {
        if let Some(dir) = self.inner.ledger_directory_override.get() {
            return vec![dir.join(SERVICES_LEDGER_FILENAME)];
        }
        self.inner
            .storage
            .resources()
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(SERVICES_LEDGER_FILENAME))
            .collect()
    }

    async fn all_storage_service_ledgers(&self) -> Vec<Utf8PathBuf> {
        if let Some(dir) = self.inner.ledger_directory_override.get() {
            return vec![dir.join(STORAGE_SERVICES_LEDGER_FILENAME)];
        }

        self.inner
            .storage
            .resources()
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(STORAGE_SERVICES_LEDGER_FILENAME))
            .collect()
    }

    pub async fn load_non_storage_services(&self) -> Result<(), Error> {
        let log = &self.inner.log;
        let mut existing_zones = self.inner.zones.lock().await;
        let ledger = Ledger::<AllZoneRequests>::new(
            log,
            self.all_service_ledgers().await,
        )
        .await?;
        let services = ledger.data();
        if services.requests.is_empty() {
            return Ok(());
        }

        // Initialize and DNS and NTP services first as they are required
        // for time synchronization, which is a pre-requisite for the other
        // services.
        self.initialize_services_locked(
            &mut existing_zones,
            &services
                .requests
                .clone()
                .into_iter()
                .filter(|svc| {
                    matches!(
                        svc.zone.zone_type,
                        ZoneType::InternalDns | ZoneType::Ntp
                    )
                })
                .collect(),
        )
        .await?;

        drop(existing_zones);

        info!(&self.inner.log, "Waiting for sled time synchronization");

        retry_notify(
            retry_policy_local(),
            || async {
                match self.timesync_get().await {
                    Ok(TimeSync { sync: true, .. }) => {
                        info!(&self.inner.log, "Time is synchronized");
                        Ok(())
                    }
                    Ok(ts) => Err(BackoffError::transient(format!(
                        "No sync {:?}",
                        ts
                    ))),
                    Err(e) => Err(BackoffError::transient(format!(
                        "Error checking for time synchronization: {}",
                        e
                    ))),
                }
            },
            |error, delay| {
                warn!(
                    self.inner.log,
                    "Time not yet synchronised (retrying in {:?})",
                    delay;
                    "error" => ?error
                );
            },
        )
        .await
        .expect("Expected an infinite retry loop syncing time");

        let mut existing_zones = self.inner.zones.lock().await;

        // Initialize all remaining serivces
        self.initialize_services_locked(
            &mut existing_zones,
            &services.requests,
        )
        .await?;
        Ok(())
    }

    pub async fn load_storage_services(&self) -> Result<(), Error> {
        let log = &self.inner.log;
        let mut existing_zones = self.inner.dataset_zones.lock().await;
        let ledger = Ledger::<AllZoneRequests>::new(
            log,
            self.all_storage_service_ledgers().await,
        )
        .await?;
        let services = ledger.data();
        if services.requests.is_empty() {
            return Ok(());
        }
        self.initialize_services_locked(
            &mut existing_zones,
            &services.requests,
        )
        .await?;
        Ok(())
    }

    /// Loads services from the services manager, and returns once all requested
    /// services have been started.
    pub async fn sled_agent_started(
        &self,
        config: Config,
        port_manager: PortManager,
        underlay_address: Ipv6Addr,
        rack_id: Uuid,
    ) -> Result<(), Error> {
        debug!(&self.inner.log, "sled agent started"; "underlay_address" => underlay_address.to_string());
        self.inner
            .sled_info
            .set(SledAgentInfo {
                config,
                port_manager,
                resolver: Resolver::new_from_ip(
                    self.inner.log.new(o!("component" => "DnsResolver")),
                    underlay_address,
                )?,
                underlay_address,
                rack_id,
            })
            .map_err(|_| "already set".to_string())
            .expect("Sled Agent should only start once");

        self.load_non_storage_services().await?;
        // TODO(https://github.com/oxidecomputer/omicron/issues/2973):
        // These will fail if the disks aren't attached.
        // Should we have a retry loop here? Kinda like we have with the switch
        // / NTP zone?
        //
        // NOTE: We could totally do the same thing with
        // "load_non_storage_services".
        self.load_storage_services().await?;

        Ok(())
    }

    /// Returns the sled's configured mode.
    pub fn sled_mode(&self) -> SledMode {
        self.inner.sled_mode
    }

    // Advertise the /64 prefix of `address`, unless we already have.
    //
    // This method only blocks long enough to check our HashSet of
    // already-advertised prefixes; the actual request to ddmd to advertise the
    // prefix is spawned onto a background task.
    async fn advertise_prefix_of_address(&self, address: Ipv6Addr) {
        let subnet = Ipv6Subnet::new(address);
        if self.inner.advertised_prefixes.lock().await.insert(subnet) {
            self.inner.ddmd_client.advertise_prefix(subnet);
        }
    }

    // Check the services intended to run in the zone to determine whether any
    // physical devices need to be mapped into the zone when it is created.
    fn devices_needed(req: &ServiceZoneRequest) -> Result<Vec<String>, Error> {
        let mut devices = vec![];
        for svc in &req.services {
            match &svc.details {
                ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic } => {
                    if let Ok(Some(n)) = tofino::get_tofino() {
                        if let Ok(device_path) = n.device_path() {
                            devices.push(device_path);
                            continue;
                        }
                    }
                    return Err(Error::MissingDevice {
                        device: "tofino".to_string(),
                    });
                }
                _ => (),
            }
        }

        for dev in &devices {
            if !Utf8Path::new(dev).exists() {
                return Err(Error::MissingDevice { device: dev.to_string() });
            }
        }
        Ok(devices)
    }

    // If we are running in the switch zone, we need a bootstrap vnic so we can
    // listen on a bootstrap address for the wicketd artifact server.
    //
    // No other zone besides the switch and global zones should have a
    // bootstrap address.
    fn bootstrap_address_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Option<(Link, Ipv6Addr)>, Error> {
        match req.zone_type {
            ZoneType::Switch => {
                let link = self
                    .inner
                    .bootstrap_vnic_allocator
                    .new_bootstrap()
                    .map_err(Error::SledLocalVnicCreation)?;
                Ok(Some((link, self.inner.switch_zone_bootstrap_address)))
            }
            _ => Ok(None),
        }
    }

    // Check the services intended to run in the zone to determine whether any
    // physical links or vnics need to be mapped into the zone when it is
    // created. Returns a list of links, plus whether or not they need link
    // local addresses in the zone.
    fn links_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Vec<(Link, bool)>, Error> {
        let mut links: Vec<(Link, bool)> = Vec::new();

        for svc in &req.services {
            match &svc.details {
                ServiceType::Tfport { pkt_source } => {
                    // The tfport service requires a MAC device to/from which sidecar
                    // packets may be multiplexed.  If the link isn't present, don't
                    // bother trying to start the zone.
                    match Dladm::verify_link(pkt_source) {
                        Ok(link) => {
                            // It's important that tfpkt does **not** receive a
                            // link local address! See: https://github.com/oxidecomputer/stlouis/issues/391
                            links.push((link, false));
                        }
                        Err(_) => {
                            return Err(Error::MissingDevice {
                                device: pkt_source.to_string(),
                            });
                        }
                    }
                }
                ServiceType::Maghemite { .. } => {
                    // If on a non-gimlet, sled-agent can be configured to map
                    // links into the switch zone. Validate those links here.
                    for link in &self.inner.switch_zone_maghemite_links {
                        match Dladm::verify_link(&link.to_string()) {
                            Ok(link) => {
                                // Link local addresses should be created in the
                                // zone so that maghemite can listen on them.
                                links.push((link, true));
                            }

                            Err(_) => {
                                return Err(Error::MissingDevice {
                                    device: link.to_string(),
                                });
                            }
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(links)
    }

    // Check the services intended to run in the zone to determine whether any
    // OPTE ports need to be created and mapped into the zone when it is created.
    async fn opte_ports_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Vec<(Port, PortTicket)>, Error> {
        // Only some services currently need OPTE ports
        if !matches!(
            req.zone_type,
            ZoneType::ExternalDns | ZoneType::Nexus | ZoneType::Ntp
        ) {
            return Ok(vec![]);
        }

        let SledAgentInfo { port_manager, resolver, underlay_address, .. } =
            &self.inner.sled_info.get().ok_or(Error::SledAgentNotReady)?;

        let dpd_addr = resolver
            .lookup_socket_v6(internal_dns::ServiceName::Dendrite)
            .await?;

        let dpd_client = DpdClient::new(
            &format!("http://[{}]:{}", dpd_addr.ip(), dpd_addr.port()),
            dpd_client::ClientState {
                tag: "sled-agent".to_string(),
                log: self.inner.log.new(o!(
                    "component" => "DpdClient"
                )),
            },
        );

        let mut ports = vec![];
        for svc in &req.services {
            let external_ip;
            let (nic, snat, external_ips) = match &svc.details {
                ServiceType::Nexus { external_ip, nic, .. } => {
                    (nic, None, std::slice::from_ref(external_ip))
                }
                ServiceType::ExternalDns { dns_address, nic, .. } => {
                    external_ip = dns_address.ip();
                    (nic, None, std::slice::from_ref(&external_ip))
                }
                ServiceType::BoundaryNtp { nic, snat_cfg, .. } => {
                    (nic, Some(*snat_cfg), &[][..])
                }
                _ => continue,
            };

            // Create the OPTE port for the service.
            // Note we don't plumb any firewall rules at this point,
            // Nexus will plumb them down later but the default OPTE
            // config allows outbound access which is enough for
            // Boundary NTP which needs to come up before Nexus.
            let port = port_manager
                .create_port(nic, snat, external_ips, &[])
                .map_err(|err| Error::ServicePortCreation {
                service: svc.details.to_string(),
                err: Box::new(err),
            })?;

            // We also need to update the switch with the NAT mappings
            let nat_target = dpd_client::types::NatTarget {
                inner_mac: dpd_client::types::MacAddr {
                    a: port.0.mac().into_array().to_vec(),
                },
                internal_ip: *underlay_address,
                vni: port.0.vni().as_u32().into(),
            };

            let (target_ip, first_port, last_port) = match snat {
                Some(s) => (s.ip, s.first_port, s.last_port),
                None => (external_ips[0], 0, u16::MAX),
            };

            // TODO-correctness(#2933): If we fail part-way we need to
            // clean up previous entries instead of leaking them.
            let nat_create = || async {
                info!(
                    self.inner.log, "creating NAT entry for service";
                    "service" => ?svc,
                );
                match &target_ip {
                    IpAddr::V4(ip) => {
                        dpd_client
                            .nat_ipv4_create(
                                ip,
                                first_port,
                                last_port,
                                &nat_target,
                            )
                            .await
                    }
                    IpAddr::V6(ip) => {
                        dpd_client
                            .nat_ipv6_create(
                                ip,
                                first_port,
                                last_port,
                                &nat_target,
                            )
                            .await
                    }
                }
                .map_err(BackoffError::transient)?;
                Ok::<(), BackoffError<DpdError<DpdTypes::Error>>>(())
            };
            let log_failure = |error, _| {
                warn!(
                    self.inner.log, "failed to create NAT entry for service";
                    "error" => ?error,
                    "service" => ?svc,
                );
            };
            retry_notify(
                retry_policy_internal_service_aggressive(),
                nat_create,
                log_failure,
            )
            .await?;

            ports.push(port);
        }

        Ok(ports)
    }

    // Check the services intended to run in the zone to determine whether any
    // additional privileges need to be enabled for the zone.
    fn privs_needed(req: &ServiceZoneRequest) -> Vec<String> {
        let mut needed = Vec::new();
        for svc in &req.services {
            match &svc.details {
                ServiceType::Tfport { .. } => {
                    needed.push("default".to_string());
                    needed.push("sys_dl_config".to_string());
                }
                ServiceType::BoundaryNtp { .. }
                | ServiceType::InternalNtp { .. } => {
                    needed.push("default".to_string());
                    needed.push("sys_time".to_string());
                    needed.push("proc_priocntl".to_string());
                }
                _ => (),
            }
        }
        needed
    }

    async fn configure_dns_client(
        &self,
        running_zone: &RunningZone,
        dns_servers: &Vec<String>,
        domain: &Option<String>,
    ) -> Result<(), Error> {
        struct DnsClient {}

        impl crate::smf_helper::Service for DnsClient {
            fn service_name(&self) -> String {
                "dns_client".to_string()
            }
            fn smf_name(&self) -> String {
                "svc:/network/dns/client".to_string()
            }
            fn should_import(&self) -> bool {
                false
            }
        }

        let service = DnsClient {};
        let smfh = SmfHelper::new(&running_zone, &service);

        let etc = running_zone.root().join("etc");
        let resolv_conf = etc.join("resolv.conf");
        let nsswitch_conf = etc.join("nsswitch.conf");
        let nsswitch_dns = etc.join("nsswitch.dns");

        if dns_servers.is_empty() {
            // Disable the dns/client service
            smfh.disable()?;
        } else {
            debug!(self.inner.log, "enabling {:?}", service.service_name());
            let mut config = String::new();
            if let Some(d) = domain {
                config.push_str(&format!("domain {d}\n"));
            }
            for s in dns_servers {
                config.push_str(&format!("nameserver {s}\n"));
            }

            debug!(self.inner.log, "creating {resolv_conf}");
            tokio::fs::write(&resolv_conf, config)
                .await
                .map_err(|err| Error::io_path(&resolv_conf, err))?;

            tokio::fs::copy(&nsswitch_dns, &nsswitch_conf)
                .await
                .map_err(|err| Error::io_path(&nsswitch_dns, err))?;

            smfh.refresh()?;
            smfh.enable()?;
        }
        Ok(())
    }

    async fn initialize_zone(
        &self,
        request: &ZoneRequest,
        filesystems: &[zone::Fs],
    ) -> Result<RunningZone, Error> {
        let device_names = Self::devices_needed(&request.zone)?;
        let (bootstrap_vnic, bootstrap_name_and_address) =
            match self.bootstrap_address_needed(&request.zone)? {
                Some((vnic, address)) => {
                    let name = vnic.name().to_string();
                    (Some(vnic), Some((name, address)))
                }
                None => (None, None),
            };
        // Unzip here, then zip later - it's important that the InstalledZone
        // owns the links, but it doesn't care about the boolean for requesting
        // link local addresses.
        let links: Vec<Link>;
        let links_need_link_local: Vec<bool>;
        (links, links_need_link_local) =
            self.links_needed(&request.zone)?.into_iter().unzip();
        let opte_ports = self.opte_ports_needed(&request.zone).await?;
        let limit_priv = Self::privs_needed(&request.zone);

        // If the zone is managing a particular dataset, plumb that
        // dataset into the zone. Additionally, construct a "unique enough" name
        // so we can create multiple zones of this type without collision.
        let unique_name = request.zone.zone_name_unique_identifier();
        let datasets = request
            .zone
            .dataset
            .iter()
            .map(|d| zone::Dataset { name: d.full() })
            .collect::<Vec<_>>();

        let devices: Vec<zone::Device> = device_names
            .iter()
            .map(|d| zone::Device { name: d.to_string() })
            .collect();

        let installed_zone = InstalledZone::install(
            &self.inner.log,
            &self.inner.underlay_vnic_allocator,
            &request.root,
            &request.zone.zone_type.to_string(),
            unique_name.as_deref(),
            datasets.as_slice(),
            &filesystems,
            &devices,
            opte_ports,
            bootstrap_vnic,
            links,
            limit_priv,
        )
        .await?;

        // TODO(https://github.com/oxidecomputer/omicron/issues/1898):
        //
        // These zones are self-assembling -- after they boot, there should
        // be no "zlogin" necessary to initialize.
        match request.zone.zone_type {
            ZoneType::Clickhouse => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };
                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                assert_eq!(request.zone.addresses.len(), 1);
                let listen_addr = &request.zone.addresses[0].to_string();
                let listen_port = &CLICKHOUSE_PORT.to_string();

                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");

                let profile = ProfileBuilder::new("omicron").add_service(
                    ServiceBuilder::new("oxide/clickhouse").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    ),
                );
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup clickhouse profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneType::CockroachDb => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };
                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                assert_eq!(request.zone.addresses.len(), 1);
                let address = SocketAddr::new(
                    IpAddr::V6(request.zone.addresses[0]),
                    COCKROACH_PORT,
                );
                let listen_addr = &address.ip().to_string();
                let listen_port = &address.port().to_string();

                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");

                let profile = ProfileBuilder::new("omicron").add_service(
                    ServiceBuilder::new("oxide/cockroachdb").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    ),
                );
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup CRDB profile", err)
                    })?;
                let running_zone = RunningZone::boot(installed_zone).await?;

                // TODO: The following lines are necessary to initialize CRDB
                // in a single-node environment. They're bad! They're wrong!
                // We definitely shouldn't be wiping the database every time
                // we want to boot this zone.
                //
                // But they're also necessary to prevent the build from
                // regressing.
                //
                // NOTE: In the (very short-term) future, this will be
                // replaced by the following:
                // 1. CRDB will simply "start", rather than "start-single-node".
                // 2. The Sled Agent will expose an explicit API to "init" the
                // Cockroach cluster, and populate it with the expected
                // contents.
                let format_crdb = || async {
                    info!(self.inner.log, "Formatting CRDB");
                    running_zone
                        .run_cmd(&[
                            "/opt/oxide/cockroachdb/bin/cockroach",
                            "sql",
                            "--insecure",
                            "--host",
                            &address.to_string(),
                            "--file",
                            "/opt/oxide/cockroachdb/sql/dbwipe.sql",
                        ])
                        .map_err(BackoffError::transient)?;
                    running_zone
                        .run_cmd(&[
                            "/opt/oxide/cockroachdb/bin/cockroach",
                            "sql",
                            "--insecure",
                            "--host",
                            &address.to_string(),
                            "--file",
                            "/opt/oxide/cockroachdb/sql/dbinit.sql",
                        ])
                        .map_err(BackoffError::transient)?;
                    info!(self.inner.log, "Formatting CRDB - Completed");
                    Ok::<
                        (),
                        BackoffError<
                            illumos_utils::running_zone::RunCommandError,
                        >,
                    >(())
                };
                let log_failure = |error, _| {
                    warn!(
                        self.inner.log, "failed to format CRDB";
                        "error" => ?error,
                    );
                };
                retry_notify(retry_policy_local(), format_crdb, log_failure)
                    .await
                    .expect("expected an infinite retry loop waiting for crdb");
                return Ok(running_zone);
            }
            ZoneType::Crucible => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };
                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                assert_eq!(request.zone.addresses.len(), 1);
                let listen_addr = &request.zone.addresses[0].to_string();
                let listen_port = &CRUCIBLE_PORT.to_string();

                let dataset = request
                    .zone
                    .dataset
                    .as_ref()
                    .expect("Crucible requires dataset");
                let uuid = &Uuid::new_v4().to_string();
                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("dataset", "astring", &dataset.full())
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("uuid", "astring", uuid)
                    .add_property("store", "astring", "/data");

                let profile = ProfileBuilder::new("omicron").add_service(
                    ServiceBuilder::new("oxide/crucible/agent").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    ),
                );
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup crucible profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneType::CruciblePantry => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                assert_eq!(request.zone.addresses.len(), 1);
                let listen_addr = &request.zone.addresses[0].to_string();
                let listen_port = &CRUCIBLE_PANTRY_PORT.to_string();

                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port);

                let profile = ProfileBuilder::new("omicron").add_service(
                    ServiceBuilder::new("oxide/crucible/pantry").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    ),
                );
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| Error::io("crucible pantry profile", err))?;
                let running_zone = RunningZone::boot(installed_zone).await?;
                return Ok(running_zone);
            }
            _ => {}
        }

        let running_zone = RunningZone::boot(installed_zone).await?;

        for (link, needs_link_local) in
            running_zone.links().iter().zip(links_need_link_local)
        {
            if needs_link_local {
                info!(
                    self.inner.log,
                    "Ensuring {}/{} exists in zone",
                    link.name(),
                    IPV6_LINK_LOCAL_NAME
                );
                Zones::ensure_has_link_local_v6_address(
                    Some(running_zone.name()),
                    &AddrObject::new(link.name(), IPV6_LINK_LOCAL_NAME)
                        .unwrap(),
                )?;
            }
        }

        if let Some((bootstrap_name, bootstrap_address)) =
            bootstrap_name_and_address.as_ref()
        {
            info!(
                self.inner.log,
                "Ensuring bootstrap address {} exists in {} zone",
                bootstrap_address.to_string(),
                request.zone.zone_type.to_string()
            );
            running_zone.ensure_bootstrap_address(*bootstrap_address).await?;
            info!(
                self.inner.log,
                "Forwarding bootstrap traffic via {} to {}",
                bootstrap_name,
                self.inner.global_zone_bootstrap_link_local_address,
            );
            running_zone
                .add_bootstrap_route(
                    BOOTSTRAP_PREFIX,
                    self.inner.global_zone_bootstrap_link_local_address,
                    bootstrap_name,
                )
                .map_err(|err| Error::ZoneCommand {
                    intent: "add bootstrap network route".to_string(),
                    err,
                })?;
        }

        for addr in &request.zone.addresses {
            if *addr == Ipv6Addr::LOCALHOST {
                continue;
            }
            info!(
                self.inner.log,
                "Ensuring address {} exists",
                addr.to_string()
            );
            let addr_request =
                AddressRequest::new_static(IpAddr::V6(*addr), None);
            running_zone.ensure_address(addr_request).await?;
            info!(
                self.inner.log,
                "Ensuring address {} exists - OK",
                addr.to_string()
            );
        }

        info!(self.inner.log, "GZ addresses: {:#?}", request.zone.gz_addresses);
        for &addr in &request.zone.gz_addresses {
            info!(
                self.inner.log,
                "Ensuring GZ address {} exists",
                addr.to_string()
            );

            let addr_name =
                request.zone.zone_type.to_string().replace(&['-', '_'][..], "");
            Zones::ensure_has_global_zone_v6_address(
                self.inner.underlay_vnic.clone(),
                addr,
                &addr_name,
            )
            .map_err(|err| Error::GzAddress {
                message: format!(
                    "adding address on behalf of service zone '{}'",
                    request.zone.zone_type
                ),
                err,
            })?;

            // If this address is in a new ipv6 prefix, notify maghemite so
            // it can advertise it to other sleds.
            self.advertise_prefix_of_address(addr).await;
        }

        let maybe_gateway = if !request.zone.gz_addresses.is_empty() {
            // If this service supplies its own GZ address, add a route.
            //
            // This is currently being used for the DNS service.
            //
            // TODO: consider limiting the number of GZ addresses which
            // can be supplied - now that we're actively using it, we
            // aren't really handling the "many GZ addresses" case, and it
            // doesn't seem necessary now.
            Some(request.zone.gz_addresses[0])
        } else if let Some(info) = self.inner.sled_info.get() {
            // If the service has not supplied a GZ address, simply add
            // a route to the sled's underlay address.
            Some(info.underlay_address)
        } else {
            // If the underlay doesn't exist, no routing occurs.
            None
        };

        if let Some(gateway) = maybe_gateway {
            running_zone.add_default_route(gateway).map_err(|err| {
                Error::ZoneCommand { intent: "Adding Route".to_string(), err }
            })?;
        }

        for service in &request.zone.services {
            // TODO: Related to
            // https://github.com/oxidecomputer/omicron/pull/1124 , should we
            // avoid importing this manifest?
            debug!(self.inner.log, "importing manifest");

            let smfh = SmfHelper::new(&running_zone, &service.details);
            smfh.import_manifest()?;

            match &service.details {
                ServiceType::Nexus { internal_ip, .. } => {
                    info!(self.inner.log, "Setting up Nexus service");

                    let sled_info = self
                        .inner
                        .sled_info
                        .get()
                        .ok_or(Error::SledAgentNotReady)?;

                    // While Nexus will be reachable via `external_ip`, it communicates
                    // atop an OPTE port which operates on a VPC private IP. OPTE will
                    // map the private IP to the external IP automatically.
                    let port_ip = running_zone
                        .ensure_address_for_port("public", 0)
                        .await?
                        .ip();

                    // Nexus takes a separate config file for parameters which
                    // cannot be known at packaging time.
                    let deployment_config = NexusDeploymentConfig {
                        id: request.zone.id,
                        rack_id: sled_info.rack_id,

                        dropshot_external: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(port_ip, 80),
                            // This has to be large enough to support:
                            // - bulk writes to disks
                            request_body_max_bytes: 8192 * 1024,
                            ..Default::default()
                        },
                        dropshot_internal: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(
                                IpAddr::V6(*internal_ip),
                                NEXUS_INTERNAL_PORT,
                            ),
                            request_body_max_bytes: 1048576,
                            ..Default::default()
                        },
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            sled_info.underlay_address,
                        ),
                        database: nexus_config::Database::FromDns,
                    };

                    // Copy the partial config file to the expected location.
                    let config_dir = Utf8PathBuf::from(format!(
                        "{}/var/svc/manifest/site/nexus",
                        running_zone.root()
                    ));
                    // The filename of a half-completed config, in need of parameters supplied at
                    // runtime.
                    const PARTIAL_LEDGER_FILENAME: &str = "config-partial.toml";
                    // The filename of a completed config, merging the partial config with
                    // additional appended parameters known at runtime.
                    const COMPLETE_LEDGER_FILENAME: &str = "config.toml";
                    let partial_config_path =
                        config_dir.join(PARTIAL_LEDGER_FILENAME);
                    let config_path = config_dir.join(COMPLETE_LEDGER_FILENAME);
                    tokio::fs::copy(partial_config_path, &config_path)
                        .await
                        .map_err(|err| Error::io_path(&config_path, err))?;

                    // Serialize the configuration and append it into the file.
                    let serialized_cfg =
                        toml::Value::try_from(&deployment_config)
                            .expect("Cannot serialize config");
                    let mut map = toml::map::Map::new();
                    map.insert("deployment".to_string(), serialized_cfg);
                    let config_str = toml::to_string(&map).map_err(|err| {
                        Error::TomlSerialize { path: config_path.clone(), err }
                    })?;
                    let mut file = tokio::fs::OpenOptions::new()
                        .append(true)
                        .open(&config_path)
                        .await
                        .map_err(|err| Error::io_path(&config_path, err))?;
                    file.write_all(config_str.as_bytes())
                        .await
                        .map_err(|err| Error::io_path(&config_path, err))?;
                }
                ServiceType::ExternalDns {
                    http_address, dns_address, ..
                } => {
                    info!(self.inner.log, "Setting up external-dns service");

                    // Like Nexus, we need to be reachable externally via
                    // `dns_address` but we don't listen on that address
                    // directly but instead on a VPC private IP. OPTE will
                    // en/decapsulate as appropriate.
                    let port_ip = running_zone
                        .ensure_address_for_port("public", 0)
                        .await?
                        .ip();
                    let dns_address =
                        SocketAddr::new(port_ip, dns_address.port());

                    smfh.setprop(
                        "config/http_address",
                        format!(
                            "[{}]:{}",
                            http_address.ip(),
                            http_address.port(),
                        ),
                    )?;
                    smfh.setprop(
                        "config/dns_address",
                        dns_address.to_string(),
                    )?;

                    // Refresh the manifest with the new properties we set, so
                    // they become "effective" properties when the service is
                    // enabled.
                    smfh.refresh()?;
                }
                ServiceType::InternalDns { http_address, dns_address } => {
                    info!(self.inner.log, "Setting up internal-dns service");
                    smfh.setprop(
                        "config/http_address",
                        format!(
                            "[{}]:{}",
                            http_address.ip(),
                            http_address.port(),
                        ),
                    )?;
                    smfh.setprop(
                        "config/dns_address",
                        &format!(
                            "[{}]:{}",
                            dns_address.ip(),
                            dns_address.port(),
                        ),
                    )?;

                    // Refresh the manifest with the new properties we set, so
                    // they become "effective" properties when the service is
                    // enabled.
                    smfh.refresh()?;
                }
                ServiceType::Oximeter => {
                    info!(self.inner.log, "Setting up oximeter service");

                    let address = request.zone.addresses[0];
                    smfh.setprop("config/id", request.zone.id)?;
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, OXIMETER_PORT),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::ManagementGatewayService => {
                    info!(self.inner.log, "Setting up MGS service");
                    smfh.setprop("config/id", request.zone.id)?;

                    // Always tell MGS to listen on localhost so wicketd can
                    // contact it even before we have an underlay network.
                    smfh.addpropvalue(
                        "config/address",
                        &format!("[::1]:{MGS_PORT}"),
                    )?;

                    if let Some(address) = request.zone.addresses.get(0) {
                        // Don't use localhost twice
                        if *address != Ipv6Addr::LOCALHOST {
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                            )?;
                        }
                    }

                    smfh.refresh()?;
                }
                ServiceType::Wicketd { baseboard } => {
                    info!(self.inner.log, "Setting up wicketd service");

                    smfh.setprop(
                        "config/address",
                        &format!("[::1]:{WICKETD_PORT}"),
                    )?;

                    // If we're launching the switch zone, we'll have a
                    // bootstrap_address based on our call to
                    // `self.bootstrap_address_needed` (which always gives us an
                    // address for the switch zone. If we _don't_ have a
                    // bootstrap address, someone has requested wicketd in a
                    // non-switch zone; return an error.
                    let Some((_, bootstrap_address))
                        = bootstrap_name_and_address
                    else {
                        return Err(Error::BadServiceRequest {
                            service: "wicketd".to_string(),
                            message: concat!(
                                "missing bootstrap address: ",
                                "wicketd can only be started in the ",
                                "switch zone",
                            ).to_string() });
                    };
                    smfh.setprop(
                        "config/artifact-address",
                        &format!(
                            "[{bootstrap_address}]:{BOOTSTRAP_ARTIFACT_PORT}"
                        ),
                    )?;

                    smfh.setprop(
                        "config/mgs-address",
                        &format!("[::1]:{MGS_PORT}"),
                    )?;
                    smfh.setprop(
                        "config/baseboard-identifier",
                        baseboard.identifier(),
                    )?;
                    smfh.setprop("config/baseboard-model", baseboard.model())?;
                    smfh.setprop(
                        "config/baseboard-revision",
                        baseboard.revision(),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::Dendrite { asic } => {
                    info!(self.inner.log, "Setting up dendrite service");

                    if let Some(info) = self.inner.sled_info.get() {
                        smfh.setprop("config/rack_id", info.rack_id)?;
                        smfh.setprop("config/sled_id", info.config.sled_id)?;
                    } else {
                        info!(
                            self.inner.log,
                            "no rack_id/sled_id available yet"
                        );
                    }

                    smfh.delpropvalue("config/address", "*")?;
                    smfh.delpropvalue("config/dns_server", "*")?;
                    for address in &request.zone.addresses {
                        smfh.addpropvalue(
                            "config/address",
                            &format!("[{}]:{}", address, DENDRITE_PORT),
                        )?;
                        if *address != Ipv6Addr::LOCALHOST {
                            let az_prefix =
                                Ipv6Subnet::<AZ_PREFIX>::new(*address);
                            for addr in Resolver::servers_from_subnet(az_prefix)
                            {
                                smfh.addpropvalue(
                                    "config/dns_server",
                                    &format!("{addr}"),
                                )?;
                            }
                        }
                    }
                    match asic {
                        DendriteAsic::TofinoAsic => {
                            // There should be exactly one device_name
                            // associated with this zone: the /dev path for
                            // the tofino ASIC.
                            let dev_cnt = device_names.len();
                            if dev_cnt == 1 {
                                smfh.setprop(
                                    "config/dev_path",
                                    device_names[0].clone(),
                                )?;
                            } else {
                                return Err(Error::SledLocalZone(
                                    anyhow::anyhow!(
                                    "{dev_cnt} devices needed for tofino asic"
                                ),
                                ));
                            }
                            smfh.setprop(
                                "config/port_config",
                                "/opt/oxide/dendrite/misc/sidecar_config.toml",
                            )?;
                            smfh.setprop(
                                "config/board_rev",
                                &self.inner.sidecar_revision,
                            )?;
                        }
                        DendriteAsic::TofinoStub => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/model_config.toml",
                        )?,
                        DendriteAsic::SoftNpu => {
                            smfh.setprop("config/mgmt", "uds")?;
                            smfh.setprop(
                                "config/uds_path",
                                "/opt/softnpu/stuff",
                            )?;
                        }
                    };
                    smfh.refresh()?;
                }
                ServiceType::Tfport { pkt_source } => {
                    info!(self.inner.log, "Setting up tfport service");

                    smfh.setprop("config/pkt_source", pkt_source)?;
                    smfh.setprop(
                        "config/host",
                        &format!("[{}]", Ipv6Addr::LOCALHOST),
                    )?;
                    smfh.setprop("config/port", &format!("{}", DENDRITE_PORT))?;
                    smfh.refresh()?;
                }
                ServiceType::BoundaryNtp {
                    ntp_servers,
                    dns_servers,
                    domain,
                    ..
                }
                | ServiceType::InternalNtp {
                    ntp_servers,
                    dns_servers,
                    domain,
                } => {
                    let boundary = matches!(
                        service.details,
                        ServiceType::BoundaryNtp { .. }
                    );
                    info!(
                        self.inner.log,
                        "Set up NTP service boundary={}, Servers={:?}",
                        boundary,
                        ntp_servers
                    );

                    let sled_info =
                        if let Some(info) = self.inner.sled_info.get() {
                            info
                        } else {
                            return Err(Error::SledAgentNotReady);
                        };

                    let rack_net = Ipv6Subnet::<RACK_PREFIX>::new(
                        sled_info.underlay_address,
                    )
                    .net();

                    smfh.setprop("config/allow", &format!("{}", rack_net))?;
                    smfh.setprop(
                        "config/boundary",
                        if boundary { "true" } else { "false" },
                    )?;

                    if boundary {
                        // Configure OPTE port for boundary NTP
                        running_zone
                            .ensure_address_for_port("public", 0)
                            .await?;
                    }

                    smfh.delpropvalue("config/server", "*")?;
                    for server in ntp_servers {
                        smfh.addpropvalue("config/server", server)?;
                    }
                    self.configure_dns_client(
                        &running_zone,
                        &dns_servers,
                        &domain,
                    )
                    .await?;

                    smfh.refresh()?;
                }
                ServiceType::Maghemite { mode } => {
                    info!(self.inner.log, "Setting up Maghemite service");

                    smfh.setprop("config/mode", &mode)?;
                    smfh.setprop("config/admin_host", "::")?;

                    let is_gimlet = is_gimlet().map_err(|e| {
                        Error::Underlay(underlay::Error::SystemDetection(e))
                    })?;

                    let maghemite_interfaces: Vec<AddrObject> = if is_gimlet {
                        (0..32)
                            .map(|i| {
                                // See the `tfport_name` function for how
                                // tfportd names the addrconf it creates.
                                // Right now, that's `tfportrear[0-31]_0`
                                // for all rear ports, which is what we're
                                // directing ddmd to listen for
                                // advertisements on.
                                //
                                // This may grow in a multi-rack future to
                                // include a subset of "front" ports too,
                                // when racks are cabled together.
                                AddrObject::new(
                                    &format!("tfportrear{}_0", i),
                                    IPV6_LINK_LOCAL_NAME,
                                )
                                .unwrap()
                            })
                            .collect()
                    } else {
                        self.inner
                            .switch_zone_maghemite_links
                            .iter()
                            .map(|i| {
                                AddrObject::new(
                                    &i.to_string(),
                                    IPV6_LINK_LOCAL_NAME,
                                )
                                .unwrap()
                            })
                            .collect()
                    };

                    smfh.setprop(
                        "config/interfaces",
                        format!(
                            "\'({})\'",
                            maghemite_interfaces
                                .iter()
                                .map(|interface| format!(r#""{}""#, interface))
                                .join(" "),
                        ),
                    )?;

                    if is_gimlet {
                        // Maghemite for a scrimlet needs to be configured to
                        // talk to dendrite
                        smfh.setprop("config/dendrite", "true")?;
                        smfh.setprop("config/dpd_host", "[::1]")?;
                        smfh.setprop("config/dpd_port", DENDRITE_PORT)?;
                    }

                    smfh.refresh()?;
                }
                ServiceType::Crucible
                | ServiceType::CruciblePantry
                | ServiceType::CockroachDb
                | ServiceType::Clickhouse => {
                    panic!(
                        "{} is a service which exists as part of a self-assembling zone",
                        service.details,
                    )
                }
            }

            debug!(self.inner.log, "enabling service");
            smfh.enable()?;
        }

        Ok(running_zone)
    }

    // Populates `existing_zones` according to the requests in `services`.
    //
    // At the point this function is invoked, IP addresses have already been
    // allocated (by either RSS or Nexus). However, this function explicitly
    // assigns such addresses to interfaces within zones.
    async fn initialize_services_locked(
        &self,
        existing_zones: &mut Vec<RunningZone>,
        requests: &Vec<ZoneRequest>,
    ) -> Result<(), Error> {
        // TODO(https://github.com/oxidecomputer/omicron/issues/726):
        // As long as we ensure the requests don't overlap, we could
        // parallelize this request.
        for req in requests {
            info!(
                self.inner.log,
                "Ensuring service zone is initialized: {:?}",
                req.zone.zone_type
            );
            // Before we bother allocating anything for this request, check if
            // this service has already been created.
            let expected_zone_name = req.zone.zone_name();
            if existing_zones.iter().any(|z| z.name() == expected_zone_name) {
                info!(
                    self.inner.log,
                    "Service zone {} already exists", req.zone.zone_type
                );
                continue;
            } else {
                info!(
                    self.inner.log,
                    "Service zone {} does not yet exist", req.zone.zone_type
                );
            }

            let running_zone = self
                .initialize_zone(
                    req,
                    // filesystems=
                    &[],
                )
                .await?;
            existing_zones.push(running_zone);
        }
        Ok(())
    }

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, and will be
    /// recorded to a local file to ensure they start automatically on next
    /// boot.
    pub async fn ensure_all_services(
        &self,
        request: ServiceEnsureBody,
    ) -> Result<(), Error> {
        let log = &self.inner.log;
        let mut existing_zones = self.inner.zones.lock().await;

        // Read the existing set of services from the ledger.
        let mut ledger = Ledger::<AllZoneRequests>::new(
            log,
            self.all_service_ledgers().await,
        )
        .await?;
        let ledger_zone_requests = ledger.data_mut();

        let new_zone_requests: Vec<ServiceZoneRequest> = {
            let known_set: HashSet<&ServiceZoneRequest> = HashSet::from_iter(
                ledger_zone_requests.requests.iter().map(|r| &r.zone),
            );
            let requested_set = HashSet::from_iter(request.services.iter());

            // TODO: We probably want to handle this case.
            if !requested_set.is_superset(&known_set) {
                // The caller may only request services additively.
                //
                // We may want to use a different mechanism for zone removal, in
                // the case of changing configurations, rather than just doing
                // that removal implicitly.
                warn!(
                    log,
                    "Cannot request services on this sled, differing configurations: {:#?}",
                    known_set.symmetric_difference(&requested_set)
                );
                return Err(Error::ServicesAlreadyConfigured);
            }
            requested_set
                .difference(&known_set)
                .map(|s| (*s).clone())
                .collect::<Vec<ServiceZoneRequest>>()
        };

        let mut zone_requests = AllZoneRequests::default();
        for zone in new_zone_requests.into_iter() {
            let root = Utf8PathBuf::from(ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT);
            zone_requests.requests.push(ZoneRequest { zone, root });
        }

        self.initialize_services_locked(
            &mut existing_zones,
            &zone_requests.requests,
        )
        .await?;

        // Update the services in the ledger and write it back to both M.2s
        ledger_zone_requests.requests.append(&mut zone_requests.requests);
        ledger.commit().await?;

        Ok(())
    }

    /// Ensures that a storage zone be initialized.
    ///
    /// These services will be instantiated by this function, and will be
    /// recorded to a local file to ensure they start automatically on next
    /// boot.
    pub async fn ensure_storage_service(
        &self,
        request: ServiceZoneRequest,
    ) -> Result<(), Error> {
        let log = &self.inner.log;
        let mut existing_zones = self.inner.dataset_zones.lock().await;

        // Read the existing set of services from the ledger.
        let mut ledger = Ledger::<AllZoneRequests>::new(
            log,
            self.all_storage_service_ledgers().await,
        )
        .await?;
        let ledger_zone_requests = ledger.data_mut();

        if !ledger_zone_requests
            .requests
            .iter()
            .any(|zone_request| zone_request.zone.id == request.id)
        {
            // If this is a new request, provision a zone filesystem on the same
            // disk as the dataset.
            let dataset = request
                .dataset
                .as_ref()
                .expect("Storage services should have a dataset");
            let root = dataset
                .pool()
                .dataset_mountpoint(sled_hardware::disk::ZONE_DATASET);
            ledger_zone_requests
                .requests
                .push(ZoneRequest { zone: request, root });
        }

        self.initialize_services_locked(
            &mut existing_zones,
            &ledger_zone_requests.requests,
        )
        .await?;

        ledger.commit().await?;

        Ok(())
    }

    pub fn boottime_rewrite(&self, zones: &Vec<RunningZone>) {
        if self
            .inner
            .time_synced
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // Already done.
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH");

        info!(self.inner.log, "Setting boot time to {:?}", now);

        let files: Vec<Utf8PathBuf> = zones
            .iter()
            .map(|z| z.root())
            .chain(iter::once(Utf8PathBuf::from("/")))
            .flat_map(|r| [r.join("var/adm/utmpx"), r.join("var/adm/wtmpx")])
            .collect();

        for file in files {
            let mut command = std::process::Command::new(PFEXEC);
            let cmd = command.args(&[
                "/usr/platform/oxide/bin/tmpx",
                &format!("{}", now.as_secs()),
                &file.as_str(),
            ]);
            match execute(cmd) {
                Err(e) => {
                    warn!(self.inner.log, "Updating {} failed: {}", &file, e);
                }
                Ok(_) => {
                    info!(self.inner.log, "Updated {}", &file);
                }
            }
        }
    }

    pub async fn timesync_get(&self) -> Result<TimeSync, Error> {
        let existing_zones = self.inner.zones.lock().await;

        if let Some(true) = self.inner.skip_timesync {
            info!(self.inner.log, "Configured to skip timesync checks");
            self.boottime_rewrite(&existing_zones);
            return Ok(TimeSync { sync: true, skew: 0.00, correction: 0.00 });
        };

        let ntp_zone_name =
            InstalledZone::get_zone_name(&ZoneType::Ntp.to_string(), None);

        let ntp_zone = existing_zones
            .iter()
            .find(|z| z.name() == ntp_zone_name)
            .ok_or_else(|| Error::NtpZoneNotReady)?;

        // XXXNTP - This could be replaced with a direct connection to the
        // daemon using a patched version of the chrony_candm crate to allow
        // a custom server socket path. From the GZ, it should be possible to
        // connect to the UNIX socket at
        // format!("{}/var/run/chrony/chronyd.sock", ntp_zone.root())

        match ntp_zone.run_cmd(&["/usr/bin/chronyc", "-c", "tracking"]) {
            Ok(stdout) => {
                let v: Vec<&str> = stdout.split(',').collect();

                if v.len() > 9 {
                    let correction = f64::from_str(v[4])
                        .map_err(|_| Error::NtpZoneNotReady)?;
                    let skew = f64::from_str(v[9])
                        .map_err(|_| Error::NtpZoneNotReady)?;

                    let sync = (skew != 0.0 || correction != 0.0)
                        && correction.abs() <= 0.05;

                    if sync {
                        self.boottime_rewrite(&existing_zones);
                    }

                    Ok(TimeSync { sync, skew, correction })
                } else {
                    Err(Error::NtpZoneNotReady)
                }
            }
            Err(e) => {
                info!(self.inner.log, "chronyc command failed: {}", e);
                Err(Error::NtpZoneNotReady)
            }
        }
    }

    /// Ensures that a switch zone exists with the provided IP adddress.
    pub async fn activate_switch(
        &self,
        switch_zone_ip: Option<Ipv6Addr>,
        baseboard: Baseboard,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ensuring scrimlet services (enabling services)");
        let mut filesystems: Vec<zone::Fs> = vec![];

        let services = match self.inner.sled_mode {
            // A pure gimlet sled should not be trying to activate a switch zone.
            SledMode::Gimlet => {
                return Err(Error::SledLocalZone(anyhow::anyhow!(
                    "attempted to activate switch zone on non-scrimlet sled"
                )))
            }

            // Sled is a scrimlet and the real tofino driver has been loaded.
            SledMode::Auto
            | SledMode::Scrimlet { asic: DendriteAsic::TofinoAsic } => {
                vec![
                    ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic },
                    ServiceType::ManagementGatewayService,
                    ServiceType::Tfport { pkt_source: "tfpkt0".to_string() },
                    ServiceType::Wicketd { baseboard },
                    ServiceType::Maghemite { mode: "transit".to_string() },
                ]
            }

            // Sled is a scrimlet but is not running the real tofino driver.
            SledMode::Scrimlet {
                asic: asic @ (DendriteAsic::TofinoStub | DendriteAsic::SoftNpu),
            } => {
                if let DendriteAsic::SoftNpu = asic {
                    let softnpu_filesystem = zone::Fs {
                        ty: "lofs".to_string(),
                        dir: "/opt/softnpu/stuff".to_string(),
                        special: "/opt/oxide/softnpu/stuff".to_string(),
                        ..Default::default()
                    };
                    filesystems.push(softnpu_filesystem);
                }

                vec![
                    ServiceType::Dendrite { asic },
                    ServiceType::ManagementGatewayService,
                    ServiceType::Wicketd { baseboard },
                    ServiceType::Maghemite { mode: "transit".to_string() },
                ]
            }
        };

        let mut addresses =
            if let Some(ip) = switch_zone_ip { vec![ip] } else { vec![] };
        addresses.push(Ipv6Addr::LOCALHOST);

        let request = ServiceZoneRequest {
            id: Uuid::new_v4(),
            zone_type: ZoneType::Switch,
            addresses,
            dataset: None,
            gz_addresses: vec![],
            services: services
                .into_iter()
                .map(|s| ServiceZoneService { id: Uuid::new_v4(), details: s })
                .collect(),
        };

        self.ensure_zone(
            ZoneType::Switch,
            // request=
            Some(request),
            // filesystems=
            filesystems,
        )
        .await
    }

    /// Ensures that no switch zone is active.
    pub async fn deactivate_switch(&self) -> Result<(), Error> {
        self.ensure_zone(
            ZoneType::Switch,
            // request=
            None,
            // filesystems=
            vec![],
        )
        .await
    }

    // Forcefully initialize a sled-local zone.
    //
    // This is a helper function for "ensure_zone".
    fn start_zone(
        self,
        zone: &mut SledLocalZone,
        request: ServiceZoneRequest,
        filesystems: Vec<zone::Fs>,
    ) {
        let (exit_tx, exit_rx) = oneshot::channel();
        let zone_type = request.zone_type.clone();
        *zone = SledLocalZone::Initializing {
            request,
            filesystems,
            worker: Some(Task {
                exit_tx,
                initializer: tokio::task::spawn(async move {
                    self.initialize_zone_loop(zone_type, exit_rx).await
                }),
            }),
        };
    }

    // Moves the current state to align with the "request".
    async fn ensure_zone(
        &self,
        zone_type: ZoneType,
        request: Option<ServiceZoneRequest>,
        filesystems: Vec<zone::Fs>,
    ) -> Result<(), Error> {
        let log = &self.inner.log;

        let mut sled_zone;
        match zone_type {
            ZoneType::Switch => {
                sled_zone = self.inner.switch_zone.lock().await;
            }
            _ => panic!("Unhandled zone type"),
        }
        let zone_typestr = zone_type.to_string();

        match (&mut *sled_zone, request) {
            (SledLocalZone::Disabled, Some(request)) => {
                info!(log, "Enabling {zone_typestr} zone (new)");
                self.clone().start_zone(&mut sled_zone, request, filesystems);
            }
            (
                SledLocalZone::Initializing { request, .. },
                Some(new_request),
            ) => {
                info!(log, "Enabling {zone_typestr} zone (already underway)");
                // The zone has not started yet -- we can simply replace
                // the next request with our new request.
                *request = new_request;
            }
            (SledLocalZone::Running { request, zone }, Some(new_request))
                if request.addresses != new_request.addresses =>
            {
                // If the switch zone is running but we have new addresses, it
                // means we're moving from the bootstrap to the underlay
                // network.  We need to add an underlay address and route in the
                // switch zone, so dendrite can communicate with nexus.
                info!(log,
                    "Re-enabling running {zone_typestr} zone (new address)";
                    "old" => format!("{:?}", request.addresses),
                    "new" => format!("{:?}", new_request.addresses),
                );
                *request = new_request;

                let address = request
                    .addresses
                    .get(0)
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|| "".to_string());

                for addr in &request.addresses {
                    if *addr == Ipv6Addr::LOCALHOST {
                        continue;
                    }
                    info!(
                        self.inner.log,
                        "Ensuring address {} exists",
                        addr.to_string()
                    );
                    let addr_request =
                        AddressRequest::new_static(IpAddr::V6(*addr), None);
                    zone.ensure_address(addr_request).await?;
                    info!(
                        self.inner.log,
                        "Ensuring address {} exists - OK",
                        addr.to_string()
                    );
                }

                if let Some(info) = self.inner.sled_info.get() {
                    zone.add_default_route(info.underlay_address).map_err(
                        |err| Error::ZoneCommand {
                            intent: "Adding Route".to_string(),
                            err,
                        },
                    )?;
                }

                for service in &request.services {
                    let smfh = SmfHelper::new(&zone, &service.details);

                    match &service.details {
                        ServiceType::ManagementGatewayService => {
                            // Remove any existing `config/address` values
                            // without deleting the property itself.
                            smfh.delpropvalue("config/address", "*")?;

                            // Restore the localhost address that we always add
                            // when setting up MGS.
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[::1]:{MGS_PORT}"),
                            )?;

                            // Add the underlay address.
                            smfh.setprop(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                            )?;

                            smfh.refresh()?;
                        }
                        ServiceType::Dendrite { .. } => {
                            info!(self.inner.log, "configuring dendrite zone");
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_id", info.rack_id)?;
                                smfh.setprop(
                                    "config/sled_id",
                                    info.config.sled_id,
                                )?;
                            } else {
                                info!(
                                    self.inner.log,
                                    "no rack_id/sled_id available yet"
                                );
                            }
                            smfh.delpropvalue("config/address", "*")?;
                            smfh.delpropvalue("config/dns_server", "*")?;
                            for address in &request.addresses {
                                smfh.addpropvalue(
                                    "config/address",
                                    &format!("[{}]:{}", address, DENDRITE_PORT),
                                )?;
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue(
                                            "config/dns_server",
                                            &format!("{addr}"),
                                        )?;
                                    }
                                }
                            }
                            smfh.refresh()?;
                        }
                        ServiceType::Tfport { .. } => {
                            // Since tfport and dpd communicate using localhost,
                            // the tfport service shouldn't need to be restarted.
                        }
                        ServiceType::Maghemite { mode } => {
                            smfh.delpropvalue("config/mode", "*")?;
                            smfh.addpropvalue("config/mode", &mode)?;
                            smfh.refresh()?;
                        }
                        _ => (),
                    }
                }
            }
            (SledLocalZone::Running { .. }, Some(_)) => {
                info!(log, "Enabling {zone_typestr} zone (already complete)");
            }
            (SledLocalZone::Disabled, None) => {
                info!(log, "Disabling {zone_typestr} zone (already complete)");
            }
            (SledLocalZone::Initializing { worker, .. }, None) => {
                info!(log, "Disabling {zone_typestr} zone (was initializing)");
                worker.take().unwrap().stop().await;
                *sled_zone = SledLocalZone::Disabled;
            }
            (SledLocalZone::Running { zone, .. }, None) => {
                info!(log, "Disabling {zone_typestr} zone (was running)");
                let _ = zone.stop().await;
                *sled_zone = SledLocalZone::Disabled;
            }
        }
        Ok(())
    }

    async fn try_initialize_sled_local_zone(
        &self,
        sled_zone: &mut SledLocalZone,
    ) -> Result<(), Error> {
        let SledLocalZone::Initializing { request, filesystems, .. } = &*sled_zone else {
            return Ok(())
        };
        let root = Utf8PathBuf::from(ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT);
        let request = ZoneRequest { zone: request.clone(), root };
        let zone = self.initialize_zone(&request, filesystems).await?;
        *sled_zone =
            SledLocalZone::Running { request: request.zone.clone(), zone };
        Ok(())
    }

    // Body of a tokio task responsible for running until the switch zone is
    // inititalized, or it has been told to stop.
    async fn initialize_zone_loop(
        &self,
        zone_type: ZoneType,
        mut exit_rx: oneshot::Receiver<()>,
    ) {
        loop {
            {
                let mut sled_zone;
                match zone_type {
                    ZoneType::Switch => {
                        sled_zone = self.inner.switch_zone.lock().await;
                    }
                    _ => panic!("Unhandled zone type"),
                }
                match self.try_initialize_sled_local_zone(&mut sled_zone).await
                {
                    Ok(()) => return,
                    Err(e) => warn!(
                        self.inner.log,
                        "Failed to initialize {zone_type}: {e}"
                    ),
                }
            }

            tokio::select! {
                // If we've been told to stop trying, bail.
                _ = &mut exit_rx => return,

                // Poll for the device every second - this timeout is somewhat
                // arbitrary, but we probably don't want to use backoff here.
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => (),
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::params::{ServiceZoneService, ZoneType};
    use illumos_utils::{
        dladm::{
            Etherstub, MockDladm, BOOTSTRAP_ETHERSTUB_NAME,
            UNDERLAY_ETHERSTUB_NAME, UNDERLAY_ETHERSTUB_VNIC_NAME,
        },
        svc,
        zone::MockZones,
    };
    use std::net::Ipv6Addr;
    use std::os::unix::process::ExitStatusExt;
    use uuid::Uuid;

    // Just placeholders. Not used.
    const GLOBAL_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;
    const SWITCH_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;

    const EXPECTED_ZONE_NAME: &str = "oxz_oximeter";

    // Returns the expectations for a new service to be created.
    fn expect_new_service() -> Vec<Box<dyn std::any::Any>> {
        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _| {
                assert_eq!(&physical_link.0, &UNDERLAY_ETHERSTUB_NAME);
                Ok(())
            },
        );
        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        install_ctx.expect().return_once(|_, _, name, _, _, _, _, _, _| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        // Boot the zone
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().return_once(|name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });

        // Ensure the address exists
        let ensure_address_ctx = MockZones::ensure_address_context();
        ensure_address_ctx.expect().return_once(|_, _, _| {
            Ok(ipnetwork::IpNetwork::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 64)
                .unwrap())
        });

        // Wait for the networking service.
        let wait_ctx = svc::wait_for_service_context();
        wait_ctx.expect().return_once(|_, _| Ok(()));
        // Import the manifest, enable the service
        let execute_ctx = illumos_utils::execute_context();
        execute_ctx.expect().times(..).returning(|_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![],
                stderr: vec![],
            })
        });

        vec![
            Box::new(create_vnic_ctx),
            Box::new(install_ctx),
            Box::new(boot_ctx),
            Box::new(ensure_address_ctx),
            Box::new(wait_ctx),
            Box::new(execute_ctx),
        ]
    }

    // Prepare to call "ensure" for a new service, then actually call "ensure".
    async fn ensure_new_service(mgr: &ServiceManager, id: Uuid) {
        let _expectations = expect_new_service();

        mgr.ensure_all_services(ServiceEnsureBody {
            services: vec![ServiceZoneRequest {
                id,
                zone_type: ZoneType::Oximeter,
                addresses: vec![Ipv6Addr::LOCALHOST],
                dataset: None,
                gz_addresses: vec![],
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::Oximeter,
                }],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to call "ensure" for a service which already exists. We should
    // return the service without actually installing a new zone.
    async fn ensure_existing_service(mgr: &ServiceManager, id: Uuid) {
        mgr.ensure_all_services(ServiceEnsureBody {
            services: vec![ServiceZoneRequest {
                id,
                zone_type: ZoneType::Oximeter,
                addresses: vec![Ipv6Addr::LOCALHOST],
                dataset: None,
                gz_addresses: vec![],
                services: vec![ServiceZoneService {
                    id,
                    details: ServiceType::Oximeter,
                }],
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to drop the service manager.
    //
    // This will shut down all allocated zones, and delete their
    // associated VNICs.
    fn drop_service_manager(mgr: ServiceManager) {
        let halt_ctx = MockZones::halt_and_remove_logged_context();
        halt_ctx.expect().returning(|_, name| {
            assert_eq!(name, EXPECTED_ZONE_NAME);
            Ok(())
        });
        let delete_vnic_ctx = MockDladm::delete_vnic_context();
        delete_vnic_ctx.expect().returning(|_| Ok(()));

        // Explicitly drop the service manager
        drop(mgr);
    }

    struct TestConfig {
        config_dir: camino_tempfile::Utf8TempDir,
    }

    impl TestConfig {
        async fn new() -> Self {
            let config_dir = camino_tempfile::Utf8TempDir::new().unwrap();
            Self { config_dir }
        }

        fn make_config(&self) -> Config {
            Config {
                sled_id: Uuid::new_v4(),
                sidecar_revision: "rev_whatever_its_a_test".to_string(),
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        let mgr = ServiceManager::new(
            log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            None, // skip_timesync
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service_which_already_exists() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_ensure_service_which_already_exists",
        );
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        let mgr = ServiceManager::new(
            log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            None, // skip_timesync
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        ensure_existing_service(&mgr, id).await;
        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_are_recreated_on_reboot() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_are_recreated_on_reboot",
        );
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            Some(true),
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        // Before we re-create the service manager - notably, using the same
        // config file! - expect that a service gets initialized.
        let _expectations = expect_new_service();
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            Some(true),
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_services_do_not_persist_without_config() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_do_not_persist_without_config",
        );
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            Some(true),
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;
        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id).await;
        drop_service_manager(mgr);

        // Next, delete the ledger. This means the service we just created will
        // not be remembered on the next initialization.
        std::fs::remove_file(
            test_config.config_dir.path().join(SERVICES_LEDGER_FILENAME),
        )
        .unwrap();

        // Observe that the old service is not re-initialized.
        let mgr = ServiceManager::new(
            logctx.log.clone(),
            GLOBAL_ZONE_BOOTSTRAP_IP,
            Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            EtherstubVnic(UNDERLAY_ETHERSTUB_VNIC_NAME.to_string()),
            Etherstub(BOOTSTRAP_ETHERSTUB_NAME.to_string()),
            SledMode::Auto,
            Some(true),
            "rev-test".to_string(),
            SWITCH_ZONE_BOOTSTRAP_IP,
            vec![],
            StorageManager::new(&log).await,
        )
        .await
        .unwrap();
        mgr.override_ledger_directory(
            test_config.config_dir.path().to_path_buf(),
        )
        .await;

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
        )
        .await
        .unwrap();

        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }
}
