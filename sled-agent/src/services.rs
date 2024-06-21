// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-local service management.
//!
//! For controlling zone-based storage services, refer to
//! [sled_storage::manager::StorageManager].
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
//! - [ServiceManager::ensure_all_omicron_zones_persistent] exposes an API to
//!   request a set of Omicron zones that should persist beyond reboot.
//! - [ServiceManager::activate_switch] exposes an API to specifically enable
//!   or disable (via [ServiceManager::deactivate_switch]) the switch zone.

use crate::bootstrap::early_networking::{
    EarlyNetworkSetup, EarlyNetworkSetupError,
};
use crate::bootstrap::BootstrapNetworking;
use crate::config::SidecarRevision;
use crate::params::{
    DendriteAsic, OmicronZoneConfig, OmicronZoneType, OmicronZonesConfig,
    TimeSync, ZoneBundleCause, ZoneBundleMetadata, ZoneType,
};
use crate::profile::*;
use crate::services_migration::{AllZoneRequests, SERVICES_LEDGER_FILENAME};
use crate::smf_helper::SmfHelper;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use anyhow::anyhow;
use camino::{Utf8Path, Utf8PathBuf};
use dpd_client::{types as DpdTypes, Client as DpdClient, Error as DpdError};
use dropshot::HandlerTaskMode;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::addrobj::IPV6_LINK_LOCAL_NAME;
use illumos_utils::dladm::{
    Dladm, Etherstub, EtherstubVnic, GetSimnetError, PhysicalLink,
};
use illumos_utils::link::{Link, VnicAllocator};
use illumos_utils::opte::{DhcpCfg, Port, PortManager, PortTicket};
use illumos_utils::running_zone::{
    EnsureAddressError, InstalledZone, RunCommandError, RunningZone,
    ZoneBuilderFactory,
};
use illumos_utils::zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT;
use illumos_utils::zone::AddressRequest;
use illumos_utils::zpool::ZpoolName;
use illumos_utils::{execute, PFEXEC};
use internal_dns::resolver::Resolver;
use itertools::Itertools;
use nexus_config::{ConfigDropshotWithTls, DeploymentConfig};
use omicron_common::address::CLICKHOUSE_KEEPER_PORT;
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::address::COCKROACH_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::CRUCIBLE_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::DNS_PORT;
use omicron_common::address::LLDP_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::WICKETD_NEXUS_PROXY_PORT;
use omicron_common::address::WICKETD_PORT;
use omicron_common::address::{Ipv6Subnet, NEXUS_TECHPORT_EXTERNAL_PORT};
use omicron_common::address::{AZ_PREFIX, OXIMETER_PORT};
use omicron_common::address::{BOOTSTRAP_ARTIFACT_PORT, COCKROACH_ADMIN_PORT};
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::{
    HostPortConfig, RackNetworkConfig,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::ledger::{self, Ledger, Ledgerable};
use omicron_ddm_admin_client::{Client as DdmAdminClient, DdmError};
use once_cell::sync::OnceCell;
use rand::prelude::SliceRandom;
use sled_hardware::is_gimlet;
use sled_hardware::underlay;
use sled_hardware::SledMode;
use sled_hardware_types::underlay::BOOTSTRAP_PREFIX;
use sled_hardware_types::Baseboard;
use sled_storage::config::MountConfig;
use sled_storage::dataset::{
    DatasetKind, DatasetName, CONFIG_DATASET, INSTALL_DATASET, ZONE_DATASET,
};
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, MutexGuard};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(test)]
use illumos_utils::zone::MockZones as Zones;
#[cfg(not(test))]
use illumos_utils::zone::Zones;

const IPV6_UNSPECIFIED: IpAddr = IpAddr::V6(Ipv6Addr::UNSPECIFIED);

#[derive(thiserror::Error, Debug, slog_error_chain::SlogInlineError)]
pub enum Error {
    #[error("Failed to initialize CockroachDb: {err}")]
    CockroachInit {
        #[source]
        err: RunCommandError,
    },

    #[error("Cannot serialize TOML to file: {path}: {err}")]
    TomlSerialize { path: Utf8PathBuf, err: toml::ser::Error },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to find device {device}")]
    MissingDevice { device: String },

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),

    #[error("Sled Agent not initialized yet")]
    SledAgentNotReady,

    #[error("No U.2 devices found with a {ZONE_DATASET} mountpoint")]
    U2NotFound,

    #[error("Sled-local zone error: {0}")]
    SledLocalZone(anyhow::Error),

    #[error("Failed to issue SMF command: {0}")]
    SmfCommand(#[from] crate::smf_helper::Error),

    #[error("{}", display_zone_init_errors(.0))]
    ZoneInitialize(Vec<(String, Box<Error>)>),

    #[error("Failed to do '{intent}' by running command in zone: {err}")]
    ZoneCommand {
        intent: String,
        #[source]
        err: illumos_utils::running_zone::RunCommandError,
    },

    #[error("Cannot list zones")]
    ZoneList(#[source] illumos_utils::zone::AdmError),

    #[error("Cannot remove zone")]
    ZoneRemoval {
        zone_name: String,
        #[source]
        err: illumos_utils::zone::AdmError,
    },

    #[error("Failed to boot zone: {0}")]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("Failed to initialize zones: {errors:?}")]
    ZoneEnsure { errors: Vec<(String, Error)> },

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

    #[error("Failed to get address: {0}")]
    GetAddressFailure(#[from] illumos_utils::zone::GetAddressError),

    #[error(
        "Failed to launch zone {zone} because ZFS value cannot be accessed"
    )]
    GetZfsValue {
        zone: String,
        #[source]
        source: illumos_utils::zfs::GetValueError,
    },

    #[error("Cannot launch {zone} with {dataset} (saw {prop_name} = {prop_value}, expected {prop_value_expected})")]
    DatasetNotReady {
        zone: String,
        dataset: String,
        prop_name: String,
        prop_value: String,
        prop_value_expected: String,
    },

    #[error("NTP zone not ready")]
    NtpZoneNotReady,

    // This isn't exactly "NtpZoneNotReady" -- it can happen when the NTP zone
    // is up, but time is still in the process of synchronizing.
    #[error("Time not yet synchronized")]
    TimeNotSynchronized,

    #[error("Execution error: {0}")]
    ExecutionError(#[from] illumos_utils::ExecutionError),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns::resolver::ResolveError),

    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Sidecar revision error")]
    SidecarRevision(#[from] anyhow::Error),

    #[error("Early networking setup error")]
    EarlyNetworkSetupError(#[from] EarlyNetworkSetupError),

    #[error("Error querying simnet devices")]
    Simnet(#[from] GetSimnetError),

    #[error(
        "Requested generation ({requested}) is older than current ({current})"
    )]
    RequestedConfigOutdated { requested: Generation, current: Generation },

    #[error("Requested generation {0} with different zones than before")]
    RequestedConfigConflicts(Generation),

    #[error("Error migrating old-format services ledger: {0:#}")]
    ServicesMigration(anyhow::Error),
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
        match err {
            Error::RequestedConfigConflicts(_) => {
                omicron_common::api::external::Error::invalid_request(
                    &err.to_string(),
                )
            }
            Error::RequestedConfigOutdated { .. } => {
                omicron_common::api::external::Error::conflict(&err.to_string())
            }
            Error::TimeNotSynchronized => {
                omicron_common::api::external::Error::unavail(&err.to_string())
            }
            Error::ZoneEnsure { errors } => {
                // As a special case, if any zones failed to timesync,
                // prioritize that error.
                //
                // This conversion to a 503 error was requested in
                // https://github.com/oxidecomputer/omicron/issues/4776 ,
                // and we preserve that behavior here, even though we may
                // launch many zones at the same time.
                if let Some(err) = errors.iter().find_map(|(_, err)| {
                    if matches!(err, Error::TimeNotSynchronized) {
                        Some(err)
                    } else {
                        None
                    }
                }) {
                    omicron_common::api::external::Error::unavail(
                        &err.to_string(),
                    )
                } else {
                    let internal_message = errors
                        .iter()
                        .map(|(name, err)| {
                            format!("failed to start {name}: {err:?}")
                        })
                        .join("\n");
                    omicron_common::api::external::Error::InternalError {
                        internal_message,
                    }
                }
            }
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
}

/// Result of [ServiceManager::load_services]
pub enum LoadServicesResult {
    /// We didn't load anything, there wasn't anything to load
    NoServicesToLoad,
    /// We successfully loaded the zones from our ledger.
    ServicesLoaded,
}

fn display_zone_init_errors(errors: &[(String, Box<Error>)]) -> String {
    if errors.len() == 1 {
        return format!(
            "Failed to initialize zone: {} errored with {}",
            errors[0].0, errors[0].1
        );
    }

    let mut output = format!("Failed to initialize {} zones:\n", errors.len());
    for (zone_name, error) in errors {
        output.push_str(&format!("  - {}: {}\n", zone_name, error));
    }
    output
}

/// Configuration parameters which modify the [`ServiceManager`]'s behavior.
pub struct Config {
    /// Identifies the sled being configured
    pub sled_id: Uuid,

    /// Identifies the revision of the sidecar to be used.
    pub sidecar_revision: SidecarRevision,
}

impl Config {
    pub fn new(sled_id: Uuid, sidecar_revision: SidecarRevision) -> Self {
        Self { sled_id, sidecar_revision }
    }
}

// The filename of the ledger, within the provided directory.
const ZONES_LEDGER_FILENAME: &str = "omicron-zones.json";

/// Combines the Nexus-provided `OmicronZonesConfig` (which describes what Nexus
/// wants for all of its zones) with the locally-determined configuration for
/// these zones.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct OmicronZonesConfigLocal {
    /// generation of the Omicron-provided part of the configuration
    ///
    /// This generation number is outside of Sled Agent's control.  We store
    /// exactly what we were given and use this number to decide when to
    /// fail requests to establish an outdated configuration.
    ///
    /// You can think of this as a major version number, with
    /// `ledger_generation` being a minor version number.  See
    /// `is_newer_than()`.
    pub omicron_generation: Generation,

    /// ledger-managed generation number
    ///
    /// This generation is managed by the ledger facility itself.  It's bumped
    /// whenever we write a new ledger.  In practice, we don't currently have
    /// any reason to bump this _for a given Omicron generation_ so it's
    /// somewhat redundant.  In principle, if we needed to modify the ledgered
    /// configuration due to some event that doesn't change the Omicron config
    /// (e.g., if we wanted to move the root filesystem to a different path), we
    /// could do that by bumping this generation.
    pub ledger_generation: Generation,
    pub zones: Vec<OmicronZoneConfigLocal>,
}

impl Ledgerable for OmicronZonesConfigLocal {
    fn is_newer_than(&self, other: &OmicronZonesConfigLocal) -> bool {
        self.omicron_generation > other.omicron_generation
            || (self.omicron_generation == other.omicron_generation
                && self.ledger_generation >= other.ledger_generation)
    }

    fn generation_bump(&mut self) {
        self.ledger_generation = self.ledger_generation.next();
    }
}

impl OmicronZonesConfigLocal {
    /// Returns the initial configuration for generation 1, which has no zones
    pub fn initial() -> OmicronZonesConfigLocal {
        OmicronZonesConfigLocal {
            omicron_generation: Generation::new(),
            ledger_generation: Generation::new(),
            zones: vec![],
        }
    }

    pub fn to_omicron_zones_config(self) -> OmicronZonesConfig {
        OmicronZonesConfig {
            generation: self.omicron_generation,
            zones: self.zones.into_iter().map(|z| z.zone).collect(),
        }
    }
}

/// Combines the Nexus-provided `OmicronZoneConfig` (which describes what Nexus
/// wants for this zone) with any locally-determined configuration (like the
/// path to the root filesystem)
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct OmicronZoneConfigLocal {
    pub zone: OmicronZoneConfig,
    #[schemars(with = "String")]
    pub root: Utf8PathBuf,
}

/// Describes how we want a switch zone to be configured
///
/// This is analogous to `OmicronZoneConfig`, but for the switch zone (which is
/// operated autonomously by the Sled Agent, not managed by Omicron).
#[derive(Clone)]
struct SwitchZoneConfig {
    id: Uuid,
    addresses: Vec<Ipv6Addr>,
    services: Vec<SwitchService>,
}

/// Describes one of several services that may be deployed in a switch zone
///
/// Some of these are only present in certain configurations (e.g., with a real
/// Tofino vs. SoftNPU) or are configured differently depending on the
/// configuration.
#[derive(Clone)]
enum SwitchService {
    ManagementGatewayService,
    Wicketd { baseboard: Baseboard },
    Dendrite { asic: DendriteAsic },
    Lldpd { baseboard: Baseboard },
    Pumpkind { asic: DendriteAsic },
    Tfport { pkt_source: String, asic: DendriteAsic },
    Uplink,
    MgDdm { mode: String },
    Mgd,
    SpSim,
}

impl crate::smf_helper::Service for SwitchService {
    fn service_name(&self) -> String {
        match self {
            SwitchService::ManagementGatewayService => "mgs",
            SwitchService::Wicketd { .. } => "wicketd",
            SwitchService::Dendrite { .. } => "dendrite",
            SwitchService::Lldpd { .. } => "lldpd",
            SwitchService::Pumpkind { .. } => "pumpkind",
            SwitchService::Tfport { .. } => "tfport",
            SwitchService::Uplink { .. } => "uplink",
            SwitchService::MgDdm { .. } => "mg-ddm",
            SwitchService::Mgd => "mgd",
            SwitchService::SpSim => "sp-sim",
        }
        .to_owned()
    }
    fn smf_name(&self) -> String {
        format!("svc:/oxide/{}", self.service_name())
    }
    fn should_import(&self) -> bool {
        true
    }
}

/// Combines the generic `SwitchZoneConfig` with other locally-determined
/// configuration
///
/// This is analogous to `OmicronZoneConfigLocal`, but for the switch zone.
struct SwitchZoneConfigLocal {
    zone: SwitchZoneConfig,
    root: Utf8PathBuf,
}

/// Describes either an Omicron-managed zone or the switch zone, used for
/// functions that operate on either one or the other
enum ZoneArgs<'a> {
    Omicron(&'a OmicronZoneConfigLocal),
    Switch(&'a SwitchZoneConfigLocal),
}

impl<'a> ZoneArgs<'a> {
    /// If this is an Omicron zone, return its type
    pub fn omicron_type(&self) -> Option<&'a OmicronZoneType> {
        match self {
            ZoneArgs::Omicron(zone_config) => Some(&zone_config.zone.zone_type),
            ZoneArgs::Switch(_) => None,
        }
    }

    /// If this is a sled-local (switch) zone, iterate over the services it's
    /// supposed to be running
    pub fn sled_local_services(
        &self,
    ) -> Box<dyn Iterator<Item = &'a SwitchService> + 'a> {
        match self {
            ZoneArgs::Omicron(_) => Box::new(std::iter::empty()),
            ZoneArgs::Switch(request) => Box::new(request.zone.services.iter()),
        }
    }

    /// Return the root filesystem path for this zone
    pub fn root(&self) -> &Utf8Path {
        match self {
            ZoneArgs::Omicron(zone_config) => &zone_config.root,
            ZoneArgs::Switch(zone_request) => &zone_request.root,
        }
    }
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
        request: SwitchZoneConfig,
        // A background task which keeps looping until the zone is initialized
        worker: Option<Task>,
        // Filesystems for the switch zone to mount
        // Since SoftNPU is currently managed via a UNIX socket, we need to
        // pass those files in to the SwitchZone so Dendrite can manage SoftNPU
        filesystems: Vec<zone::Fs>,
        // Data links that need to be plumbed into the zone.
        data_links: Vec<String>,
    },
    // The Zone is currently running.
    Running {
        // The original request for the zone
        request: SwitchZoneConfig,
        // The currently running zone
        zone: RunningZone,
    },
}

// The return type for `start_omicron_zones`.
//
// When multiple zones are started concurrently, some can fail while others
// succeed. This structure allows the function to return this nuanced
// information.
#[must_use]
struct StartZonesResult {
    // The set of zones which have successfully started.
    new_zones: Vec<OmicronZone>,

    // The set of (zone name, error) of zones that failed to start.
    errors: Vec<(String, Error)>,
}

// A running zone and the configuration which started it.
struct OmicronZone {
    runtime: RunningZone,
    config: OmicronZoneConfigLocal,
}

impl OmicronZone {
    fn name(&self) -> &str {
        self.runtime.name()
    }
}

type ZoneMap = BTreeMap<String, OmicronZone>;

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    global_zone_bootstrap_link_local_address: Ipv6Addr,
    switch_zone: Mutex<SledLocalZone>,
    sled_mode: SledMode,
    time_sync_config: TimeSyncConfig,
    time_synced: AtomicBool,
    switch_zone_maghemite_links: Vec<PhysicalLink>,
    sidecar_revision: SidecarRevision,
    // Zones representing running services
    zones: Mutex<ZoneMap>,
    underlay_vnic_allocator: VnicAllocator<Etherstub>,
    underlay_vnic: EtherstubVnic,
    bootstrap_vnic_allocator: VnicAllocator<Etherstub>,
    ddmd_client: DdmAdminClient,
    advertised_prefixes: Mutex<HashSet<Ipv6Subnet<SLED_PREFIX>>>,
    sled_info: OnceCell<SledAgentInfo>,
    switch_zone_bootstrap_address: Ipv6Addr,
    storage: StorageHandle,
    zone_bundler: ZoneBundler,
    ledger_directory_override: OnceCell<Utf8PathBuf>,
    image_directory_override: OnceCell<Utf8PathBuf>,
}

// Late-binding information, only known once the sled agent is up and
// operational.
struct SledAgentInfo {
    config: Config,
    port_manager: PortManager,
    resolver: Resolver,
    underlay_address: Ipv6Addr,
    rack_id: Uuid,
    rack_network_config: Option<RackNetworkConfig>,
}

pub(crate) enum TimeSyncConfig {
    // Waits for NTP to confirm that time has been synchronized.
    Normal,
    // Skips timesync unconditionally.
    Skip,
    // Fails timesync unconditionally.
    #[cfg(all(test, target_os = "illumos"))]
    Fail,
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
    /// - `ddm_client`: Client pointed to our localhost ddmd
    /// - `bootstrap_networking`: Collection of etherstubs/VNICs set up when
    ///    bootstrap agent begins
    /// - `sled_mode`: The sled's mode of operation (Gimlet vs Scrimlet).
    /// - `time_sync_config`: Describes how the sled awaits synced time.
    /// - `sidecar_revision`: Rev of attached sidecar, if present.
    /// - `switch_zone_maghemite_links`: List of physical links on which
    ///    maghemite should listen.
    /// - `storage`: Shared handle to get the current state of disks/zpools.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log: &Logger,
        ddmd_client: DdmAdminClient,
        bootstrap_networking: BootstrapNetworking,
        sled_mode: SledMode,
        time_sync_config: TimeSyncConfig,
        sidecar_revision: SidecarRevision,
        switch_zone_maghemite_links: Vec<PhysicalLink>,
        storage: StorageHandle,
        zone_bundler: ZoneBundler,
    ) -> Self {
        let log = log.new(o!("component" => "ServiceManager"));
        info!(log, "Creating ServiceManager");
        Self {
            inner: Arc::new(ServiceManagerInner {
                log: log.clone(),
                global_zone_bootstrap_link_local_address: bootstrap_networking
                    .global_zone_bootstrap_link_local_ip,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725):
                // Load the switch zone if it already exists?
                switch_zone: Mutex::new(SledLocalZone::Disabled),
                sled_mode,
                time_sync_config,
                time_synced: AtomicBool::new(false),
                sidecar_revision,
                switch_zone_maghemite_links,
                zones: Mutex::new(BTreeMap::new()),
                underlay_vnic_allocator: VnicAllocator::new(
                    "Service",
                    bootstrap_networking.underlay_etherstub,
                ),
                underlay_vnic: bootstrap_networking.underlay_etherstub_vnic,
                bootstrap_vnic_allocator: VnicAllocator::new(
                    "Bootstrap",
                    bootstrap_networking.bootstrap_etherstub,
                ),
                ddmd_client,
                advertised_prefixes: Mutex::new(HashSet::new()),
                sled_info: OnceCell::new(),
                switch_zone_bootstrap_address: bootstrap_networking
                    .switch_zone_bootstrap_ip,
                storage,
                zone_bundler,
                ledger_directory_override: OnceCell::new(),
                image_directory_override: OnceCell::new(),
            }),
        }
    }

    #[cfg(all(test, target_os = "illumos"))]
    fn override_ledger_directory(&self, path: Utf8PathBuf) {
        self.inner.ledger_directory_override.set(path).unwrap();
    }

    #[cfg(all(test, target_os = "illumos"))]
    fn override_image_directory(&self, path: Utf8PathBuf) {
        self.inner.image_directory_override.set(path).unwrap();
    }

    pub fn switch_zone_bootstrap_address(&self) -> Ipv6Addr {
        self.inner.switch_zone_bootstrap_address
    }

    async fn all_service_ledgers(&self) -> Vec<Utf8PathBuf> {
        if let Some(dir) = self.inner.ledger_directory_override.get() {
            return vec![dir.join(SERVICES_LEDGER_FILENAME)];
        }
        let resources = self.inner.storage.get_latest_disks().await;
        resources
            .all_m2_mountpoints(CONFIG_DATASET)
            .into_iter()
            .map(|p| p.join(SERVICES_LEDGER_FILENAME))
            .collect()
    }

    async fn all_omicron_zone_ledgers(&self) -> Vec<Utf8PathBuf> {
        if let Some(dir) = self.inner.ledger_directory_override.get() {
            return vec![dir.join(ZONES_LEDGER_FILENAME)];
        }
        let resources = self.inner.storage.get_latest_disks().await;
        resources
            .all_m2_mountpoints(CONFIG_DATASET)
            .into_iter()
            .map(|p| p.join(ZONES_LEDGER_FILENAME))
            .collect()
    }

    // Loads persistent configuration about any Omicron-managed zones that we're
    // supposed to be running.
    //
    // For historical reasons, there are two possible places this configuration
    // could live, each with its own format.  This function first checks the
    // newer one.  If no configuration was found there, it checks the older
    // one.  If only the older one was found, it is converted into the new form
    // so that future calls will only look at the new form.
    async fn load_ledgered_zones(
        &self,
        // This argument attempts to ensure that the caller holds the right
        // lock.
        _map: &MutexGuard<'_, ZoneMap>,
    ) -> Result<Option<Ledger<OmicronZonesConfigLocal>>, Error> {
        // First, try to load the current software's zone ledger.  If that
        // works, we're done.
        let log = &self.inner.log;
        let ledger_paths = self.all_omicron_zone_ledgers().await;
        info!(log, "Loading Omicron zones from: {ledger_paths:?}");
        let maybe_ledger =
            Ledger::<OmicronZonesConfigLocal>::new(log, ledger_paths.clone())
                .await;

        if let Some(ledger) = maybe_ledger {
            info!(
                log,
                "Loaded Omicron zones";
                "zones_config" => ?ledger.data()
            );
            return Ok(Some(ledger));
        }

        // Now look for the ledger used by previous versions.  If we find it,
        // we'll convert it and write out a new ledger used by the current
        // software.
        info!(
            log,
            "Loading Omicron zones - No zones detected \
            (will look for old-format services)"
        );
        let services_ledger_paths = self.all_service_ledgers().await;
        info!(
            log,
            "Loading old-format services from: {services_ledger_paths:?}"
        );

        let maybe_ledger =
            Ledger::<AllZoneRequests>::new(log, services_ledger_paths.clone())
                .await;
        let maybe_converted = match maybe_ledger {
            None => {
                // The ledger ignores all errors attempting to load files.  That
                // might be fine most of the time.  In this case, we want to
                // raise a big red flag if we find an old-format ledger that we
                // can't process.
                if services_ledger_paths.iter().any(|p| p.exists()) {
                    Err(Error::ServicesMigration(anyhow!(
                        "failed to read or parse old-format ledger, \
                        but one exists"
                    )))
                } else {
                    // There was no old-format ledger at all.
                    return Ok(None);
                }
            }
            Some(ledger) => {
                let all_services = ledger.into_inner();
                OmicronZonesConfigLocal::try_from(all_services)
                    .map_err(Error::ServicesMigration)
            }
        };

        match maybe_converted {
            Err(error) => {
                // We've tried to test thoroughly so that this should never
                // happen.  If for some reason it does happen, engineering
                // intervention is likely to be required to figure out how to
                // proceed.  The current software does not directly support
                // whatever was in the ledger, and it's not safe to just come up
                // with no zones when we're supposed to be running stuff.  We'll
                // need to figure out what's unexpected about what we found in
                // the ledger and figure out how to fix the
                // conversion.
                error!(
                    log,
                    "Loading Omicron zones - found services but failed \
                    to convert them (support intervention required): \
                    {:#}",
                    error
                );
                return Err(error);
            }
            Ok(new_config) => {
                // We've successfully converted the old ledger.  Write a new
                // one.
                info!(
                    log,
                    "Successfully migrated old-format services ledger to \
                    zones ledger"
                );
                let mut ledger = Ledger::<OmicronZonesConfigLocal>::new_with(
                    log,
                    ledger_paths.clone(),
                    new_config,
                );

                ledger.commit().await?;

                // We could consider removing the old ledger here.  That would
                // not guarantee that it would be gone, though, because we could
                // crash during `ledger.commit()` above having written at least
                // one of the new ledgers.  In that case, we won't go through
                // this code path again on restart.  If we wanted to ensure the
                // old-format ledger was gone after the migration, we could
                // consider unconditionally removing the old ledger paths in the
                // caller, after we've got a copy of the new-format ledger.
                //
                // Should we?  In principle, it shouldn't matter either way
                // because we will never look at the old-format ledger unless we
                // don't have a new-format one, and we should now have a
                // new-format one forever now.
                //
                // When might it matter?  Two cases:
                //
                // (1) If the sled agent is downgraded to a previous version
                //     that doesn't know about the new-format ledger.  Do we
                //     want that sled agent to use the old-format one?  It
                //     depends.  If that downgrade happens immediately because
                //     the upgrade to the first new-format version was a
                //     disaster, then we'd probably rather the downgraded sled
                //     agent _did_ start its zones.  If the downgrade happens
                //     months later, potentially after various additional
                //     reconfigurations, then that old-format ledger is probably
                //     out of date and shouldn't be used.  There's no way to
                //     really know which case we're in, but the latter seems
                //     quite unlikely (why would we downgrade so far back after
                //     so long?).  So that's a reason to keep the old-format
                //     ledger.
                //
                // (2) Suppose a developer or Oxide support engineer removes the
                //     new ledger for some reason, maybe thinking sled agent
                //     would come up with no zones running.  They'll be
                //     surprised to discover that it actually starts running a
                //     potentially old set of zones.  This probably only matters
                //     on a production system, and even then, it probably
                //     shouldn't happen.
                //
                // Given these cases, we're left ambivalent.  We choose to keep
                // the old ledger around.  If nothing else, if something goes
                // wrong, we'll have a copy of its last contents!
                Ok(Some(ledger))
            }
        }
    }

    // TODO(https://github.com/oxidecomputer/omicron/issues/2973):
    //
    // The sled agent retries this function indefinitely at the call-site, but
    // we could be smarter.
    //
    // - If we know that disks are missing, we could wait for them
    // - We could permanently fail if we are able to distinguish other errors
    // more clearly.
    pub async fn load_services(&self) -> Result<LoadServicesResult, Error> {
        let log = &self.inner.log;
        let mut existing_zones = self.inner.zones.lock().await;
        let Some(mut ledger) =
            self.load_ledgered_zones(&existing_zones).await?
        else {
            // Nothing found -- nothing to do.
            info!(
                log,
                "Loading Omicron zones - \
                no zones nor old-format services found"
            );
            return Ok(LoadServicesResult::NoServicesToLoad);
        };

        let zones_config = ledger.data_mut();
        info!(
            log,
            "Loaded Omicron zones";
            "zones_config" => ?zones_config
        );
        let omicron_zones_config =
            zones_config.clone().to_omicron_zones_config();

        self.ensure_all_omicron_zones(
            &mut existing_zones,
            omicron_zones_config,
            None,
        )
        .await?;
        Ok(LoadServicesResult::ServicesLoaded)
    }

    /// Sets up "Sled Agent" information, including underlay info.
    ///
    /// Any subsequent calls after the first invocation return an error.
    pub fn sled_agent_started(
        &self,
        config: Config,
        port_manager: PortManager,
        underlay_address: Ipv6Addr,
        rack_id: Uuid,
        rack_network_config: Option<RackNetworkConfig>,
    ) -> Result<(), Error> {
        info!(&self.inner.log, "sled agent started"; "underlay_address" => underlay_address.to_string());
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
                rack_network_config,
            })
            .map_err(|_| "already set".to_string())
            .expect("Sled Agent should only start once");

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
    fn devices_needed(zone_args: &ZoneArgs<'_>) -> Result<Vec<String>, Error> {
        let mut devices = vec![];
        for svc_details in zone_args.sled_local_services() {
            match svc_details {
                SwitchService::Dendrite {
                    asic: DendriteAsic::TofinoAsic,
                    ..
                } => {
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
                SwitchService::Dendrite {
                    asic: DendriteAsic::SoftNpuPropolisDevice,
                    ..
                } => {
                    devices.push("/dev/tty03".into());
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
        zone_args: &ZoneArgs<'_>,
    ) -> Result<Option<(Link, Ipv6Addr)>, Error> {
        if let ZoneArgs::Switch(_) = zone_args {
            let link = self
                .inner
                .bootstrap_vnic_allocator
                .new_bootstrap()
                .map_err(Error::SledLocalVnicCreation)?;
            Ok(Some((link, self.inner.switch_zone_bootstrap_address)))
        } else {
            Ok(None)
        }
    }

    // Derive two unique techport /64 prefixes from the bootstrap address.
    fn bootstrap_addr_to_techport_prefixes(
        addr: &Ipv6Addr,
    ) -> [Ipv6Subnet<SLED_PREFIX>; 2] {
        // Generate two unique prefixes from the bootstrap address, by
        // incrementing the second octet. This assumes that the bootstrap
        // address starts with `fdb0`, and so we end up with `fdb1` and `fdb2`.
        let mut segments = addr.segments();
        segments[0] += 1;
        let prefix0 = Ipv6Subnet::new(Ipv6Addr::from(segments));
        segments[0] += 1;
        let prefix1 = Ipv6Subnet::new(Ipv6Addr::from(segments));
        [prefix0, prefix1]
    }

    // Check the services intended to run in the zone to determine whether any
    // physical links or vnics need to be mapped into the zone when it is
    // created. Returns a list of links, plus whether or not they need link
    // local addresses in the zone.
    fn links_needed(
        &self,
        zone_args: &ZoneArgs<'_>,
    ) -> Result<Vec<(Link, bool)>, Error> {
        let mut links: Vec<(Link, bool)> = Vec::new();

        let is_gimlet = is_gimlet().map_err(|e| {
            Error::Underlay(underlay::Error::SystemDetection(e))
        })?;

        for svc_details in zone_args.sled_local_services() {
            match &svc_details {
                SwitchService::Tfport { pkt_source, asic: _ } => {
                    // The tfport service requires a MAC device to/from which
                    // sidecar packets may be multiplexed.  If the link isn't
                    // present, don't bother trying to start the zone.
                    match Dladm::verify_link(pkt_source) {
                        Ok(link) => {
                            // It's important that tfpkt does **not** receive a
                            // link local address! See: https://github.com/oxidecomputer/stlouis/issues/391
                            links.push((link, false));
                        }
                        Err(_) => {
                            if is_gimlet {
                                return Err(Error::MissingDevice {
                                    device: pkt_source.to_string(),
                                });
                            }
                        }
                    }
                }
                SwitchService::MgDdm { .. } => {
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
                                if let SidecarRevision::SoftZone(_) =
                                    self.inner.sidecar_revision
                                {
                                    return Err(Error::MissingDevice {
                                        device: link.to_string(),
                                    });
                                }
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
    // OPTE ports need to be created and mapped into the zone when it is
    // created.
    async fn opte_ports_needed(
        &self,
        zone_args: &ZoneArgs<'_>,
    ) -> Result<Vec<(Port, PortTicket)>, Error> {
        // Only some services currently need OPTE ports
        if !matches!(
            zone_args.omicron_type(),
            Some(OmicronZoneType::ExternalDns { .. })
                | Some(OmicronZoneType::Nexus { .. })
                | Some(OmicronZoneType::BoundaryNtp { .. })
        ) {
            return Ok(vec![]);
        }

        let SledAgentInfo {
            port_manager,
            underlay_address,
            resolver,
            rack_network_config,
            ..
        } = &self.inner.sled_info.get().ok_or(Error::SledAgentNotReady)?;

        let Some(rack_network_config) = rack_network_config.as_ref() else {
            // If we're in a test/dev environments with no uplinks, we have
            // nothing to do; print a warning in the (hopefully unlikely) event
            // we land here on a real rack.
            warn!(
                self.inner.log,
                "No rack network config present; skipping OPTE NAT config",
            );
            return Ok(vec![]);
        };

        let uplinked_switch_zone_addrs =
            EarlyNetworkSetup::new(&self.inner.log)
                .lookup_uplinked_switch_zone_underlay_addrs(
                    resolver,
                    rack_network_config,
                )
                .await;

        let dpd_clients: Vec<DpdClient> = uplinked_switch_zone_addrs
            .iter()
            .map(|addr| {
                DpdClient::new(
                    &format!("http://[{}]:{}", addr, DENDRITE_PORT),
                    dpd_client::ClientState {
                        tag: "sled-agent".to_string(),
                        log: self.inner.log.new(o!(
                            "component" => "DpdClient"
                        )),
                    },
                )
            })
            .collect();

        let external_ip;
        let (zone_type_str, nic, snat, floating_ips) = match &zone_args
            .omicron_type()
        {
            Some(
                zone_type @ OmicronZoneType::Nexus { external_ip, nic, .. },
            ) => (
                zone_type.zone_type_str(),
                nic,
                None,
                std::slice::from_ref(external_ip),
            ),
            Some(
                zone_type @ OmicronZoneType::ExternalDns {
                    dns_address,
                    nic,
                    ..
                },
            ) => {
                external_ip = dns_address.ip();
                (
                    zone_type.zone_type_str(),
                    nic,
                    None,
                    std::slice::from_ref(&external_ip),
                )
            }
            Some(
                zone_type @ OmicronZoneType::BoundaryNtp {
                    nic, snat_cfg, ..
                },
            ) => (zone_type.zone_type_str(), nic, Some(*snat_cfg), &[][..]),
            _ => unreachable!("unexpected zone type"),
        };

        // Create the OPTE port for the service.
        // Note we don't plumb any firewall rules at this point,
        // Nexus will plumb them down later but the default OPTE
        // config allows outbound access which is enough for
        // Boundary NTP which needs to come up before Nexus.
        let port = port_manager
            .create_port(nic, snat, None, floating_ips, &[], DhcpCfg::default())
            .map_err(|err| Error::ServicePortCreation {
                service: zone_type_str.clone(),
                err: Box::new(err),
            })?;

        // We also need to update the switch with the NAT mappings
        // XXX: need to revisit iff. any services get more than one
        //      address.
        let (target_ip, first_port, last_port) = match snat {
            Some(s) => {
                let (first_port, last_port) = s.port_range_raw();
                (s.ip, first_port, last_port)
            }
            None => (floating_ips[0], 0, u16::MAX),
        };

        for dpd_client in &dpd_clients {
            // TODO-correctness(#2933): If we fail part-way we need to
            // clean up previous entries instead of leaking them.
            let nat_create = || async {
                info!(
                    self.inner.log, "creating NAT entry for service";
                    "zone_type" => &zone_type_str,
                );

                dpd_client
                    .ensure_nat_entry(
                        &self.inner.log,
                        target_ip.into(),
                        dpd_client::types::MacAddr {
                            a: port.0.mac().into_array(),
                        },
                        first_port,
                        last_port,
                        port.0.vni().as_u32(),
                        underlay_address,
                    )
                    .await
                    .map_err(BackoffError::transient)?;

                Ok::<(), BackoffError<DpdError<DpdTypes::Error>>>(())
            };
            let log_failure = |error, _| {
                warn!(
                    self.inner.log, "failed to create NAT entry for service";
                    "error" => ?error,
                    "zone_type" => &zone_type_str,
                );
            };
            retry_notify(
                retry_policy_internal_service_aggressive(),
                nat_create,
                log_failure,
            )
            .await?;
        }
        Ok(vec![port])
    }

    // Check the services intended to run in the zone to determine whether any
    // additional privileges need to be enabled for the zone.
    fn privs_needed(zone_args: &ZoneArgs<'_>) -> Vec<String> {
        let mut needed = vec![
            "default".to_string(),
            "dtrace_user".to_string(),
            "dtrace_proc".to_string(),
        ];
        for svc_details in zone_args.sled_local_services() {
            match svc_details {
                SwitchService::Tfport { .. } => {
                    needed.push("sys_dl_config".to_string());
                }
                _ => (),
            }
        }

        if let Some(omicron_zone_type) = zone_args.omicron_type() {
            match omicron_zone_type {
                OmicronZoneType::BoundaryNtp { .. }
                | OmicronZoneType::InternalNtp { .. } => {
                    needed.push("sys_time".to_string());
                    needed.push("proc_priocntl".to_string());
                }
                _ => (),
            }
        }
        needed
    }

    async fn dns_install(
        info: &SledAgentInfo,
        ip_addrs: Option<Vec<IpAddr>>,
        domain: &Option<String>,
    ) -> Result<ServiceBuilder, Error> {
        // We want to configure the dns/install SMF service inside the
        // zone with the list of DNS nameservers.  This will cause
        // /etc/resolv.conf to be populated inside the zone.  To do
        // this, we need the full list of nameservers.  Fortunately, the
        // nameservers provide a DNS name for the full list of
        // nameservers.
        //
        // Note that when we configure the dns/install service, we're
        // supplying values for an existing property group on the SMF
        // *service*.  We're not creating a new property group, nor are
        // we configuring a property group on the instance.

        // Users may decide to provide specific addresses to set as
        // nameservers, or this information can be retrieved from
        // from SledAgentInfo.
        let nameservers = match ip_addrs {
            None => {
                let addrs = info
                    .resolver
                    .lookup_all_ipv6(internal_dns::ServiceName::InternalDns)
                    .await?;

                let mut servers: Vec<IpAddr> = vec![];
                for a in addrs {
                    let ip = IpAddr::V6(a);
                    servers.push(ip);
                }

                servers
            }
            Some(servers) => servers,
        };

        let mut dns_config_builder = PropertyGroupBuilder::new("install_props");
        for ns_addr in &nameservers {
            dns_config_builder = dns_config_builder.add_property(
                "nameserver",
                "net_address",
                &ns_addr.to_string(),
            );
        }

        match domain {
            Some(d) => {
                dns_config_builder =
                    dns_config_builder.add_property("domain", "astring", d)
            }
            None => (),
        }

        Ok(ServiceBuilder::new("network/dns/install")
            .add_property_group(dns_config_builder)
            // We do need to enable the default instance of the
            // dns/install service.  It's enough to just mention it
            // here, as the ServiceInstanceBuilder enables the
            // instance being added by default.
            .add_instance(ServiceInstanceBuilder::new("default")))
    }

    fn zone_network_setup_install(
        gw_addr: &Ipv6Addr,
        zone: &InstalledZone,
        static_addr: &Ipv6Addr,
    ) -> Result<ServiceBuilder, Error> {
        let datalink = zone.get_control_vnic_name();
        let gateway = &gw_addr.to_string();
        let static_addr = &static_addr.to_string();

        let mut config_builder = PropertyGroupBuilder::new("config");
        config_builder = config_builder
            .add_property("datalink", "astring", datalink)
            .add_property("gateway", "astring", gateway)
            .add_property("static_addr", "astring", static_addr);

        Ok(ServiceBuilder::new("oxide/zone-network-setup")
            .add_property_group(config_builder)
            .add_instance(ServiceInstanceBuilder::new("default")))
    }

    fn opte_interface_set_up_install(
        zone: &InstalledZone,
    ) -> Result<ServiceBuilder, Error> {
        let port_idx = 0;
        let port = zone.opte_ports().nth(port_idx).ok_or_else(|| {
            Error::ZoneEnsureAddress(EnsureAddressError::MissingOptePort {
                zone: String::from(zone.name()),
                port_idx,
            })
        })?;

        let opte_interface = port.vnic_name();
        let opte_gateway = port.gateway().ip().to_string();
        let opte_ip = port.ip().to_string();

        let mut config_builder = PropertyGroupBuilder::new("config");
        config_builder = config_builder
            .add_property("interface", "astring", opte_interface)
            .add_property("gateway", "astring", &opte_gateway)
            .add_property("ip", "astring", &opte_ip);

        Ok(ServiceBuilder::new("oxide/opte-interface-setup")
            .add_property_group(config_builder)
            .add_instance(ServiceInstanceBuilder::new("default")))
    }

    async fn initialize_zone(
        &self,
        request: ZoneArgs<'_>,
        filesystems: &[zone::Fs],
        data_links: &[String],
        fake_install_dir: Option<&String>,
    ) -> Result<RunningZone, Error> {
        let device_names = Self::devices_needed(&request)?;
        let (bootstrap_vnic, bootstrap_name_and_address) =
            match self.bootstrap_address_needed(&request)? {
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
            self.links_needed(&request)?.into_iter().unzip();
        let opte_ports = self.opte_ports_needed(&request).await?;
        let limit_priv = Self::privs_needed(&request);

        // If the zone is managing a particular dataset, plumb that
        // dataset into the zone. Additionally, construct a "unique enough" name
        // so we can create multiple zones of this type without collision.
        let unique_name = match &request {
            ZoneArgs::Omicron(zone_config) => Some(zone_config.zone.id),
            ZoneArgs::Switch(_) => None,
        };
        let datasets: Vec<_> = match &request {
            ZoneArgs::Omicron(zone_config) => zone_config
                .zone
                .dataset_name()
                .map(|n| zone::Dataset { name: n.full_name() })
                .into_iter()
                .collect(),
            ZoneArgs::Switch(_) => vec![],
        };

        let devices: Vec<zone::Device> = device_names
            .iter()
            .map(|d| zone::Device { name: d.to_string() })
            .collect();

        // Look for the image in the ramdisk first
        let mut zone_image_paths = vec![Utf8PathBuf::from("/opt/oxide")];
        // Inject an image path if requested by a test.
        if let Some(path) = self.inner.image_directory_override.get() {
            zone_image_paths.push(path.clone());
        };

        // If the boot disk exists, look for the image in the "install" dataset
        // there too.
        let all_disks = self.inner.storage.get_latest_disks().await;
        if let Some((_, boot_zpool)) = all_disks.boot_disk() {
            zone_image_paths.push(boot_zpool.dataset_mountpoint(
                &all_disks.mount_config.root,
                INSTALL_DATASET,
            ));
        }

        let zone_type_str = match &request {
            ZoneArgs::Omicron(zone_config) => {
                zone_config.zone.zone_type.zone_type_str()
            }
            ZoneArgs::Switch(_) => "switch".to_string(),
        };

        // We use the fake initialiser for testing
        let mut zone_builder = match fake_install_dir {
            None => ZoneBuilderFactory::default().builder(),
            Some(dir) => ZoneBuilderFactory::fake(Some(dir)).builder(),
        };
        if let Some(uuid) = unique_name {
            zone_builder = zone_builder.with_unique_name(uuid);
        }
        if let Some(vnic) = bootstrap_vnic {
            zone_builder = zone_builder.with_bootstrap_vnic(vnic);
        }
        let installed_zone = zone_builder
            .with_log(self.inner.log.clone())
            .with_underlay_vnic_allocator(&self.inner.underlay_vnic_allocator)
            .with_zone_root_path(&request.root())
            .with_zone_image_paths(zone_image_paths.as_slice())
            .with_zone_type(&zone_type_str)
            .with_datasets(datasets.as_slice())
            .with_filesystems(&filesystems)
            .with_data_links(&data_links)
            .with_devices(&devices)
            .with_opte_ports(opte_ports)
            .with_links(links)
            .with_limit_priv(limit_priv)
            .install()
            .await?;

        let disabled_ssh_service = ServiceBuilder::new("network/ssh")
            .add_instance(ServiceInstanceBuilder::new("default").disable());

        let disabled_dns_client_service =
            ServiceBuilder::new("network/dns/client")
                .add_instance(ServiceInstanceBuilder::new("default").disable());

        let enabled_dns_client_service =
            ServiceBuilder::new("network/dns/client")
                .add_instance(ServiceInstanceBuilder::new("default"));

        // TODO(https://github.com/oxidecomputer/omicron/issues/1898):
        //
        // These zones are self-assembling -- after they boot, there should
        // be no "zlogin" necessary to initialize.
        match &request {
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type: OmicronZoneType::Clickhouse { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let listen_addr = underlay_address;
                let listen_port = &CLICKHOUSE_PORT.to_string();

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    listen_addr,
                )?;

                let dns_service = Self::dns_install(info, None, &None).await?;

                let config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "listen_addr",
                        "astring",
                        listen_addr.to_string(),
                    )
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");
                let clickhouse_service =
                    ServiceBuilder::new("oxide/clickhouse").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(clickhouse_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup clickhouse profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }

            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type: OmicronZoneType::ClickhouseKeeper { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let listen_addr = underlay_address;
                let listen_port = &CLICKHOUSE_KEEPER_PORT.to_string();

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    listen_addr,
                )?;

                let dns_service = Self::dns_install(info, None, &None).await?;

                let config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "listen_addr",
                        "astring",
                        listen_addr.to_string(),
                    )
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");
                let clickhouse_keeper_service =
                    ServiceBuilder::new("oxide/clickhouse_keeper")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(config),
                        );
                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(clickhouse_keeper_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io(
                            "Failed to setup clickhouse keeper profile",
                            err,
                        )
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }

            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        id: zone_id,
                        zone_type: OmicronZoneType::CockroachDb { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let crdb_listen_ip = *underlay_address;
                let crdb_address =
                    SocketAddr::new(IpAddr::V6(crdb_listen_ip), COCKROACH_PORT)
                        .to_string();
                let admin_address = SocketAddr::new(
                    IpAddr::V6(crdb_listen_ip),
                    COCKROACH_ADMIN_PORT,
                )
                .to_string();

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    &crdb_listen_ip,
                )?;

                let dns_service = Self::dns_install(info, None, &None).await?;

                // Configure the CockroachDB service.
                let cockroachdb_config = PropertyGroupBuilder::new("config")
                    .add_property("listen_addr", "astring", &crdb_address)
                    .add_property("store", "astring", "/data");
                let cockroachdb_service =
                    ServiceBuilder::new("oxide/cockroachdb").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(cockroachdb_config),
                    );

                // Configure the Omicron cockroach-admin service.
                let cockroach_admin_config =
                    PropertyGroupBuilder::new("config")
                        .add_property("zone_id", "astring", zone_id.to_string())
                        .add_property(
                            "cockroach_address",
                            "astring",
                            crdb_address,
                        )
                        .add_property("http_address", "astring", admin_address);
                let cockroach_admin_service =
                    ServiceBuilder::new("oxide/cockroach-admin").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(cockroach_admin_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(cockroachdb_service)
                    .add_service(cockroach_admin_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup CRDB profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }

            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type: OmicronZoneType::Crucible { dataset, .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };
                let listen_addr = &underlay_address;
                let listen_port = &CRUCIBLE_PORT.to_string();

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    listen_addr,
                )?;

                let dataset_name = DatasetName::new(
                    dataset.pool_name.clone(),
                    DatasetKind::Crucible,
                )
                .full_name();
                let uuid = &Uuid::new_v4().to_string();
                let config = PropertyGroupBuilder::new("config")
                    .add_property("dataset", "astring", &dataset_name)
                    .add_property(
                        "listen_addr",
                        "astring",
                        listen_addr.to_string(),
                    )
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("uuid", "astring", uuid)
                    .add_property("store", "astring", "/data");

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(disabled_dns_client_service)
                    .add_service(
                        ServiceBuilder::new("oxide/crucible/agent")
                            .add_instance(
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

            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type: OmicronZoneType::CruciblePantry { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let listen_addr = &underlay_address;
                let listen_port = &CRUCIBLE_PANTRY_PORT.to_string();

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    listen_addr,
                )?;

                let config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "listen_addr",
                        "astring",
                        listen_addr.to_string(),
                    )
                    .add_property("listen_port", "astring", listen_port);

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(disabled_dns_client_service)
                    .add_service(
                        ServiceBuilder::new("oxide/crucible/pantry")
                            .add_instance(
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
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        id,
                        zone_type: OmicronZoneType::Oximeter { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                // Configure the Oximeter service.
                let address = SocketAddr::new(
                    IpAddr::V6(*underlay_address),
                    OXIMETER_PORT,
                );

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    underlay_address,
                )?;

                let oximeter_config = PropertyGroupBuilder::new("config")
                    .add_property("id", "astring", &id.to_string())
                    .add_property("address", "astring", &address.to_string());
                let oximeter_service = ServiceBuilder::new("oxide/oximeter")
                    .add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(oximeter_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(oximeter_service)
                    .add_service(disabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup Oximeter profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type: OmicronZoneType::ExternalDns { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    underlay_address,
                )?;

                // Like Nexus, we need to be reachable externally via
                // `dns_address` but we don't listen on that address
                // directly but instead on a VPC private IP. OPTE will
                // en/decapsulate as appropriate.
                let opte_interface_setup =
                    Self::opte_interface_set_up_install(&installed_zone)?;

                let port_idx = 0;
                let port = installed_zone
                    .opte_ports()
                    .nth(port_idx)
                    .ok_or_else(|| {
                        Error::ZoneEnsureAddress(
                            EnsureAddressError::MissingOptePort {
                                zone: String::from(installed_zone.name()),
                                port_idx,
                            },
                        )
                    })?;
                let opte_ip = port.ip();

                let http_addr =
                    format!("[{}]:{}", underlay_address, DNS_HTTP_PORT);
                let dns_addr = format!("{}:{}", opte_ip, DNS_PORT);

                let external_dns_config = PropertyGroupBuilder::new("config")
                    .add_property("http_address", "astring", &http_addr)
                    .add_property("dns_address", "astring", &dns_addr);
                let external_dns_service =
                    ServiceBuilder::new("oxide/external_dns").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(external_dns_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(opte_interface_setup)
                    .add_service(disabled_ssh_service)
                    .add_service(external_dns_service)
                    .add_service(disabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup External DNS profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type:
                            OmicronZoneType::BoundaryNtp {
                                dns_servers,
                                ntp_servers,
                                domain,
                                ..
                            },
                        underlay_address,
                        ..
                    },
                ..
            })
            | ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type:
                            OmicronZoneType::InternalNtp {
                                dns_servers,
                                ntp_servers,
                                domain,
                                ..
                            },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    underlay_address,
                )?;

                let is_boundary = matches!(
                    // It's safe to unwrap here as we already know it's one of InternalNTP or BoundaryNtp.
                    request.omicron_type().unwrap(),
                    OmicronZoneType::BoundaryNtp { .. }
                );

                let rack_net =
                    Ipv6Subnet::<RACK_PREFIX>::new(info.underlay_address)
                        .net()
                        .to_string();

                let dns_install_service =
                    Self::dns_install(info, Some(dns_servers.to_vec()), domain)
                        .await?;

                let mut chrony_config = PropertyGroupBuilder::new("config")
                    .add_property("allow", "astring", &rack_net)
                    .add_property(
                        "boundary",
                        "boolean",
                        &is_boundary.to_string(),
                    );

                for s in ntp_servers {
                    chrony_config = chrony_config.add_property(
                        "server",
                        "astring",
                        &s.to_string(),
                    );
                }

                let dns_client_service;
                if dns_servers.is_empty() {
                    dns_client_service = disabled_dns_client_service;
                } else {
                    dns_client_service = enabled_dns_client_service;
                }

                let ntp_service = ServiceBuilder::new("oxide/ntp")
                    .add_instance(ServiceInstanceBuilder::new("default"));

                let chrony_setup_service =
                    ServiceBuilder::new("oxide/chrony-setup").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(chrony_config),
                    );

                let mut profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(chrony_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(dns_install_service)
                    .add_service(dns_client_service)
                    .add_service(ntp_service);

                // Only Boundary NTP needs an OPTE interface and port configured.
                if is_boundary {
                    let opte_interface_setup =
                        Self::opte_interface_set_up_install(&installed_zone)?;
                    profile = profile.add_service(opte_interface_setup)
                }

                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to set up NTP profile", err)
                    })?;

                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type:
                            OmicronZoneType::InternalDns {
                                http_address,
                                dns_address,
                                gz_address,
                                gz_address_index,
                                ..
                            },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let nw_setup_service = Self::zone_network_setup_install(
                    gz_address,
                    &installed_zone,
                    underlay_address,
                )?;

                // Internal DNS zones require a special route through
                // the global zone, since they are not on the same part
                // of the underlay as most other services on this sled
                // (the sled's subnet).
                //
                // We create an IP address in the dedicated portion of
                // the underlay used for internal DNS servers, but we
                // *also* add a number ("which DNS server is this") to
                // ensure these addresses are given unique names. In the
                // unlikely case that two internal DNS servers end up on
                // the same machine (which is effectively a
                // developer-only environment -- we wouldn't want this
                // in prod!), they need to be given distinct names.
                let addr_name = format!("internaldns{gz_address_index}");
                Zones::ensure_has_global_zone_v6_address(
                    self.inner.underlay_vnic.clone(),
                    *gz_address,
                    &addr_name,
                )
                .map_err(|err| Error::GzAddress {
                    message: format!(
                        "Failed to create address {} for Internal DNS zone",
                        addr_name
                    ),
                    err,
                })?;

                // If this address is in a new ipv6 prefix, notify
                // maghemite so it can advertise it to other sleds.
                self.advertise_prefix_of_address(*gz_address).await;

                let http_addr =
                    format!("[{}]:{}", http_address.ip(), http_address.port());
                let dns_addr =
                    format!("[{}]:{}", dns_address.ip(), dns_address.port());

                let internal_dns_config = PropertyGroupBuilder::new("config")
                    .add_property("http_address", "astring", &http_addr)
                    .add_property("dns_address", "astring", &dns_addr);
                let internal_dns_service =
                    ServiceBuilder::new("oxide/internal_dns").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(internal_dns_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(internal_dns_service)
                    .add_service(disabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup Internal DNS profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
            }
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone:
                    OmicronZoneConfig {
                        zone_type:
                            OmicronZoneType::Nexus {
                                internal_address,
                                external_tls,
                                external_dns_servers,
                                ..
                            },
                        underlay_address,
                        id,
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    &info.underlay_address,
                    &installed_zone,
                    underlay_address,
                )?;

                // While Nexus will be reachable via `external_ip`, it
                // communicates atop an OPTE port which operates on a
                // VPC private IP. OPTE will map the private IP to the
                // external IP automatically.
                let opte_interface_setup =
                    Self::opte_interface_set_up_install(&installed_zone)?;

                let port_idx = 0;
                let port = installed_zone
                    .opte_ports()
                    .nth(port_idx)
                    .ok_or_else(|| {
                        Error::ZoneEnsureAddress(
                            EnsureAddressError::MissingOptePort {
                                zone: String::from(installed_zone.name()),
                                port_idx,
                            },
                        )
                    })?;
                let opte_ip = port.ip();

                // Nexus takes a separate config file for parameters
                // which cannot be known at packaging time.
                let nexus_port = if *external_tls { 443 } else { 80 };
                let deployment_config = DeploymentConfig {
                    id: *id,
                    rack_id: info.rack_id,
                    techport_external_server_port: NEXUS_TECHPORT_EXTERNAL_PORT,

                    dropshot_external: ConfigDropshotWithTls {
                        tls: *external_tls,
                        dropshot: dropshot::ConfigDropshot {
                            bind_address: SocketAddr::new(*opte_ip, nexus_port),
                            // This has to be large enough to support:
                            // - bulk writes to disks
                            request_body_max_bytes: 8192 * 1024,
                            default_handler_task_mode:
                                HandlerTaskMode::Detached,
                        },
                    },
                    dropshot_internal: dropshot::ConfigDropshot {
                        bind_address: (*internal_address).into(),
                        // This has to be large enough to support, among
                        // other things, the initial list of TLS
                        // certificates provided by the customer during
                        // rack setup.
                        request_body_max_bytes: 10 * 1024 * 1024,
                        default_handler_task_mode: HandlerTaskMode::Detached,
                    },
                    internal_dns: nexus_config::InternalDns::FromSubnet {
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            info.underlay_address,
                        ),
                    },
                    database: nexus_config::Database::FromDns,
                    external_dns_servers: external_dns_servers.clone(),
                };

                // Copy the partial config file to the expected
                // location.
                let config_dir = Utf8PathBuf::from(format!(
                    "{}/var/svc/manifest/site/nexus",
                    installed_zone.root()
                ));
                // The filename of a half-completed config, in need of
                // parameters supplied at runtime.
                const PARTIAL_LEDGER_FILENAME: &str = "config-partial.toml";
                // The filename of a completed config, merging the
                // partial config with additional appended parameters
                // known at runtime.
                const COMPLETE_LEDGER_FILENAME: &str = "config.toml";
                let partial_config_path =
                    config_dir.join(PARTIAL_LEDGER_FILENAME);
                let config_path = config_dir.join(COMPLETE_LEDGER_FILENAME);
                tokio::fs::copy(partial_config_path, &config_path)
                    .await
                    .map_err(|err| Error::io_path(&config_path, err))?;

                // Serialize the configuration and append it into the
                // file.
                let serialized_cfg = toml::Value::try_from(&deployment_config)
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
                file.write_all(b"\n\n")
                    .await
                    .map_err(|err| Error::io_path(&config_path, err))?;
                file.write_all(config_str.as_bytes())
                    .await
                    .map_err(|err| Error::io_path(&config_path, err))?;

                let nexus_config = PropertyGroupBuilder::new("config");
                let nexus_service = ServiceBuilder::new("oxide/nexus")
                    .add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(nexus_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(opte_interface_setup)
                    .add_service(disabled_ssh_service)
                    .add_service(nexus_service)
                    .add_service(disabled_dns_client_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup Nexus profile", err)
                    })?;
                return Ok(RunningZone::boot(installed_zone).await?);
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
                &zone_type_str,
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

        let addresses = match &request {
            ZoneArgs::Omicron(OmicronZoneConfigLocal {
                zone: OmicronZoneConfig { underlay_address, .. },
                ..
            }) => std::slice::from_ref(underlay_address),
            ZoneArgs::Switch(req) => &req.zone.addresses,
        };
        for addr in addresses {
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

        let maybe_gateway = if let Some(info) = self.inner.sled_info.get() {
            // Only consider a route to the sled's underlay address if the
            // underlay is up.
            let sled_underlay_subnet =
                Ipv6Subnet::<SLED_PREFIX>::new(info.underlay_address);

            if addresses
                .iter()
                .any(|ip| sled_underlay_subnet.net().contains(*ip))
            {
                // If the underlay is up, provide a route to it through an
                // existing address in the Zone on the same subnet.
                info!(self.inner.log, "Zone using sled underlay as gateway");
                Some(info.underlay_address)
            } else {
                // If no such address exists in the sled's subnet, don't route
                // to anything.
                info!(
                    self.inner.log,
                    "Zone not using gateway (even though underlay is up)"
                );
                None
            }
        } else {
            // If the underlay doesn't exist, no routing occurs.
            info!(
                self.inner.log,
                "Zone not using gateway (underlay is not up)"
            );
            None
        };

        let sidecar_revision = match &self.inner.sidecar_revision {
            SidecarRevision::Physical(rev) => rev.to_string(),
            SidecarRevision::SoftZone(rev)
            | SidecarRevision::SoftPropolis(rev) => format!(
                "softnpu_front_{}_rear_{}",
                rev.front_port_count, rev.rear_port_count
            ),
        };

        if let Some(gateway) = maybe_gateway {
            running_zone.add_default_route(gateway).map_err(|err| {
                Error::ZoneCommand { intent: "Adding Route".to_string(), err }
            })?;
        }

        match &request {
            ZoneArgs::Omicron(zone_config) => {
                match &zone_config.zone.zone_type {
                    OmicronZoneType::BoundaryNtp { .. }
                    | OmicronZoneType::Clickhouse { .. }
                    | OmicronZoneType::ClickhouseKeeper { .. }
                    | OmicronZoneType::CockroachDb { .. }
                    | OmicronZoneType::Crucible { .. }
                    | OmicronZoneType::CruciblePantry { .. }
                    | OmicronZoneType::ExternalDns { .. }
                    | OmicronZoneType::InternalDns { .. }
                    | OmicronZoneType::InternalNtp { .. }
                    | OmicronZoneType::Nexus { .. }
                    | OmicronZoneType::Oximeter { .. } => {
                        panic!(
                            "{} is a service which exists as part of a \
                            self-assembling zone",
                            &zone_config.zone.zone_type.zone_type_str(),
                        )
                    }
                };
            }
            ZoneArgs::Switch(request) => {
                for service in &request.zone.services {
                    // TODO: Related to
                    // https://github.com/oxidecomputer/omicron/pull/1124 , should we
                    // avoid importing this manifest?
                    debug!(self.inner.log, "importing manifest");

                    let smfh = SmfHelper::new(&running_zone, service);
                    smfh.import_manifest()?;

                    match service {
                        SwitchService::ManagementGatewayService => {
                            info!(self.inner.log, "Setting up MGS service");
                            smfh.setprop("config/id", request.zone.id)?;

                            // Always tell MGS to listen on localhost so wicketd
                            // can contact it even before we have an underlay
                            // network.
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[::1]:{MGS_PORT}"),
                            )?;

                            if let Some(address) = request.zone.addresses.get(0)
                            {
                                // Don't use localhost twice
                                if *address != Ipv6Addr::LOCALHOST {
                                    smfh.addpropvalue(
                                        "config/address",
                                        &format!("[{address}]:{MGS_PORT}"),
                                    )?;
                                }
                            }

                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_id", info.rack_id)?;
                            }

                            smfh.refresh()?;
                        }
                        SwitchService::SpSim => {
                            info!(
                                self.inner.log,
                                "Setting up Simulated SP service"
                            );
                        }
                        SwitchService::Wicketd { baseboard } => {
                            info!(self.inner.log, "Setting up wicketd service");

                            smfh.setprop(
                                "config/address",
                                &format!("[::1]:{WICKETD_PORT}"),
                            )?;

                            // If we're launching the switch zone, we'll have a
                            // bootstrap_address based on our call to
                            // `self.bootstrap_address_needed` (which always
                            // gives us an address for the switch zone. If we
                            // _don't_ have a bootstrap address, someone has
                            // requested wicketd in a non-switch zone; return an
                            // error.
                            let Some((_, bootstrap_address)) =
                                bootstrap_name_and_address
                            else {
                                return Err(Error::BadServiceRequest {
                                    service: "wicketd".to_string(),
                                    message: concat!(
                                        "missing bootstrap address: ",
                                        "wicketd can only be started in the ",
                                        "switch zone",
                                    )
                                    .to_string(),
                                });
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

                            // We intentionally bind `nexus-proxy-address` to
                            // `::` so wicketd will serve this on all
                            // interfaces, particularly the tech port
                            // interfaces, allowing external clients to connect
                            // to this Nexus proxy.
                            smfh.setprop(
                                "config/nexus-proxy-address",
                                &format!("[::]:{WICKETD_NEXUS_PROXY_PORT}"),
                            )?;
                            if let Some(underlay_address) = self
                                .inner
                                .sled_info
                                .get()
                                .map(|info| info.underlay_address)
                            {
                                let rack_subnet = Ipv6Subnet::<AZ_PREFIX>::new(
                                    underlay_address,
                                );
                                smfh.setprop(
                                    "config/rack-subnet",
                                    &rack_subnet.net().addr().to_string(),
                                )?;
                            }

                            let serialized_baseboard =
                                serde_json::to_string_pretty(&baseboard)?;
                            let serialized_baseboard_path =
                                Utf8PathBuf::from(format!(
                                    "{}/opt/oxide/baseboard.json",
                                    running_zone.root()
                                ));
                            tokio::fs::write(
                                &serialized_baseboard_path,
                                &serialized_baseboard,
                            )
                            .await
                            .map_err(|err| {
                                Error::io_path(&serialized_baseboard_path, err)
                            })?;
                            smfh.setprop(
                                "config/baseboard-file",
                                String::from("/opt/oxide/baseboard.json"),
                            )?;

                            smfh.refresh()?;
                        }
                        SwitchService::Dendrite { asic } => {
                            info!(
                                self.inner.log,
                                "Setting up dendrite service"
                            );

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
                            for address in &request.zone.addresses {
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
                            match asic {
                                DendriteAsic::TofinoAsic => {
                                    // There should be exactly one device_name
                                    // associated with this zone: the /dev path
                                    // for the tofino ASIC.
                                    let dev_cnt = device_names.len();
                                    if dev_cnt == 1 {
                                        smfh.setprop(
                                            "config/dev_path",
                                            device_names[0].clone(),
                                        )?;
                                    } else {
                                        return Err(Error::SledLocalZone(
                                            anyhow::anyhow!(
                                                "{dev_cnt} devices needed \
                                                for tofino asic"
                                            ),
                                        ));
                                    }
                                    smfh.setprop(
                                        "config/port_config",
                                        "/opt/oxide/dendrite/misc/sidecar_config.toml",
                                    )?;
                                    smfh.setprop("config/board_rev", &sidecar_revision)?;
                                }
                                DendriteAsic::TofinoStub => smfh.setprop(
                                    "config/port_config",
                                    "/opt/oxide/dendrite/misc/model_config.toml",
                                )?,
                                asic @ (DendriteAsic::SoftNpuZone
                                | DendriteAsic::SoftNpuPropolisDevice) => {
                                    if asic == &DendriteAsic::SoftNpuZone {
                                        smfh.setprop("config/mgmt", "uds")?;
                                        smfh.setprop(
                                            "config/uds_path",
                                            "/opt/softnpu/stuff",
                                        )?;
                                    }
                                    if asic == &DendriteAsic::SoftNpuPropolisDevice {
                                        smfh.setprop("config/mgmt", "uart")?;
                                    }
                                    let s = match self.inner.sidecar_revision {
                                        SidecarRevision::SoftZone(ref s) => s,
                                        SidecarRevision::SoftPropolis(ref s) => s,
                                        _ => {
                                            return Err(Error::SidecarRevision(
                                                anyhow::anyhow!(
                                                    "expected soft sidecar \
                                                    revision"
                                                ),
                                            ))
                                        }
                                    };
                                    smfh.setprop(
                                        "config/front_ports",
                                        &s.front_port_count.to_string(),
                                    )?;
                                    smfh.setprop(
                                        "config/rear_ports",
                                        &s.rear_port_count.to_string(),
                                    )?;
                                    smfh.setprop(
                                        "config/port_config",
                                        "/opt/oxide/dendrite/misc/softnpu_single_sled_config.toml",
                                    )?
                                }
                            };
                            smfh.refresh()?;
                        }
                        SwitchService::Tfport { pkt_source, asic } => {
                            info!(self.inner.log, "Setting up tfport service");

                            let is_gimlet = is_gimlet().map_err(|e| {
                                Error::Underlay(
                                    underlay::Error::SystemDetection(e),
                                )
                            })?;

                            if is_gimlet {
                                // Collect the prefixes for each techport.
                                let nameaddr =
                                    bootstrap_name_and_address.as_ref();
                                let techport_prefixes = match nameaddr {
                                    Some((_, addr)) => {
                                        Self::bootstrap_addr_to_techport_prefixes(addr)
                                    }
                                    None => {
                                        return Err(Error::BadServiceRequest {
                                            service: "tfport".into(),
                                            message: "bootstrap addr missing"
                                                .into(),
                                        });
                                    }
                                };

                                for (i, prefix) in
                                    techport_prefixes.into_iter().enumerate()
                                {
                                    // Each `prefix` is an `Ipv6Subnet`
                                    // including a netmask.  Stringify just the
                                    // network address, without the mask.
                                    smfh.setprop(
                                        format!("config/techport{i}_prefix"),
                                        prefix.net().addr(),
                                    )?;
                                }
                                smfh.setprop("config/pkt_source", pkt_source)?;
                            }
                            if asic == &DendriteAsic::SoftNpuZone {
                                smfh.setprop("config/flags", "--sync-only")?;
                            }
                            if asic == &DendriteAsic::SoftNpuPropolisDevice {
                                smfh.setprop("config/pkt_source", pkt_source)?;
                            }
                            smfh.setprop(
                                "config/host",
                                &format!("[{}]", Ipv6Addr::LOCALHOST),
                            )?;
                            smfh.setprop(
                                "config/port",
                                &format!("{}", DENDRITE_PORT),
                            )?;

                            smfh.refresh()?;
                        }
                        SwitchService::Lldpd { baseboard } => {
                            info!(self.inner.log, "Setting up lldpd service");

                            match baseboard {
                                Baseboard::Gimlet {
                                    identifier, model, ..
                                }
                                | Baseboard::Pc { identifier, model, .. } => {
                                    smfh.setprop(
                                        "config/scrimlet_id",
                                        identifier,
                                    )?;
                                    smfh.setprop(
                                        "config/scrimlet_model",
                                        model,
                                    )?;
                                }
                                Baseboard::Unknown => {}
                            }
                            smfh.setprop(
                                "config/board_rev",
                                &sidecar_revision,
                            )?;

                            smfh.delpropvalue("config/address", "*")?;
                            for address in &request.zone.addresses {
                                smfh.addpropvalue(
                                    "config/address",
                                    &format!("[{}]:{}", address, LLDP_PORT),
                                )?;
                            }
                            smfh.refresh()?;
                        }
                        SwitchService::Pumpkind { asic } => {
                            // The pumpkin daemon is only needed when running on
                            // with real sidecar.
                            if asic == &DendriteAsic::TofinoAsic {
                                info!(
                                    self.inner.log,
                                    "Setting up pumpkind service"
                                );
                                smfh.setprop("config/mode", "switch")?;
                                smfh.refresh()?;
                            }
                        }
                        SwitchService::Uplink => {
                            // Nothing to do here - this service is special and
                            // configured in
                            // `ensure_switch_zone_uplinks_configured`
                        }
                        SwitchService::Mgd => {
                            info!(self.inner.log, "Setting up mgd service");
                            smfh.delpropvalue("config/dns_servers", "*")?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_uuid", info.rack_id)?;
                                smfh.setprop(
                                    "config/sled_uuid",
                                    info.config.sled_id,
                                )?;
                            }
                            for address in &request.zone.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                        )?;
                                    }
                                    break;
                                }
                            }
                            smfh.refresh()?;
                        }
                        SwitchService::MgDdm { mode } => {
                            info!(self.inner.log, "Setting up mg-ddm service");
                            smfh.setprop("config/mode", &mode)?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_uuid", info.rack_id)?;
                                smfh.setprop(
                                    "config/sled_uuid",
                                    info.config.sled_id,
                                )?;
                            }
                            smfh.delpropvalue("config/dns_servers", "*")?;
                            for address in &request.zone.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                        )?;
                                    }
                                    break;
                                }
                            }

                            let is_gimlet = is_gimlet().map_err(|e| {
                                Error::Underlay(
                                    underlay::Error::SystemDetection(e),
                                )
                            })?;

                            let maghemite_interfaces: Vec<AddrObject> =
                                if is_gimlet {
                                    (0..32)
                                        .map(|i| {
                                            // See the `tfport_name` function
                                            // for how tfportd names the
                                            // addrconf it creates.  Right now,
                                            // that's `tfportrear[0-31]_0` for
                                            // all rear ports, which is what
                                            // we're directing ddmd to listen
                                            // for advertisements on.
                                            //
                                            // This may grow in a multi-rack
                                            // future to include a subset of
                                            // "front" ports too, when racks are
                                            // cabled together.
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
                                // `svccfg setprop` requires a list of values to
                                // be enclosed in `()`, and each string value to
                                // be enclosed in `""`. Note that we do _not_
                                // need to escape the parentheses, since this is
                                // not passed through a shell, but directly to
                                // `exec(2)` in the zone.
                                format!(
                                    "({})",
                                    maghemite_interfaces
                                        .iter()
                                        .map(|interface| format!(
                                            r#""{}""#,
                                            interface
                                        ))
                                        .join(" "),
                                ),
                            )?;

                            if is_gimlet {
                                // Ddm for a scrimlet needs to be configured to
                                // talk to dendrite
                                smfh.setprop("config/dpd_host", "[::1]")?;
                                smfh.setprop("config/dpd_port", DENDRITE_PORT)?;
                            }
                            smfh.setprop("config/dendrite", "true")?;

                            smfh.refresh()?;
                        }
                    }

                    debug!(self.inner.log, "enabling service");
                    smfh.enable()?;
                }
            }
        };

        Ok(running_zone)
    }

    // Ensures that a single Omicron zone is running.
    //
    // This method is NOT idempotent.
    //
    // - If the zone already exists, in any form, it is fully removed
    // before being initialized. This is primarily intended to remove "partially
    // stopped/started" zones with detritus from interfering with a new zone
    // being launched.
    // - If zones need time to be synchronized before they are initialized
    // (e.g., this is a hard requirement for CockroachDb) they can check the
    // `time_is_synchronized` argument.
    // - `all_u2_pools` provides a snapshot into durable storage on this sled,
    // which gives the storage manager an opportunity to validate the zone's
    // storage configuration against the reality of the current sled.
    async fn start_omicron_zone(
        &self,
        mount_config: &MountConfig,
        zone: &OmicronZoneConfig,
        time_is_synchronized: bool,
        all_u2_pools: &Vec<ZpoolName>,
        fake_install_dir: Option<&String>,
    ) -> Result<OmicronZone, Error> {
        // Ensure the zone has been fully removed before we try to boot it.
        //
        // This ensures that old "partially booted/stopped" zones do not
        // interfere with our installation.
        self.ensure_removed(&zone).await?;

        // If this zone requires timesync and we aren't ready, fail it early.
        if zone.zone_type.requires_timesync() && !time_is_synchronized {
            return Err(Error::TimeNotSynchronized);
        }

        // Ensure that this zone's storage is ready.
        let root = self
            .validate_storage_and_pick_mountpoint(
                mount_config,
                &zone,
                &all_u2_pools,
            )
            .await?;

        let config = OmicronZoneConfigLocal { zone: zone.clone(), root };

        let runtime = self
            .initialize_zone(
                ZoneArgs::Omicron(&config),
                // filesystems=
                &[],
                // data_links=
                &[],
                fake_install_dir,
            )
            .await?;

        Ok(OmicronZone { runtime, config })
    }

    // Concurrently attempts to start all zones identified by requests.
    //
    // This method is NOT idempotent.
    //
    // If we try to start ANY zones concurrently, the result is contained
    // in the `StartZonesResult` value. This will contain the set of zones which
    // were initialized successfully, as well as the set of zones which failed
    // to start.
    async fn start_omicron_zones(
        &self,
        mount_config: &MountConfig,
        requests: impl Iterator<Item = &OmicronZoneConfig> + Clone,
        time_is_synchronized: bool,
        all_u2_pools: &Vec<ZpoolName>,
        fake_install_dir: Option<&String>,
    ) -> Result<StartZonesResult, Error> {
        if let Some(name) =
            requests.clone().map(|zone| zone.zone_name()).duplicates().next()
        {
            return Err(Error::BadServiceRequest {
                service: name,
                message: "Should not initialize zone twice".to_string(),
            });
        }

        let futures = requests.map(|zone| async move {
            self.start_omicron_zone(
                mount_config,
                &zone,
                time_is_synchronized,
                all_u2_pools,
                fake_install_dir,
            )
            .await
            .map_err(|err| (zone.zone_name().to_string(), err))
        });

        let results = futures::future::join_all(futures).await;

        let mut new_zones = Vec::new();
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(zone) => {
                    info!(self.inner.log, "Zone started"; "zone" => zone.name());
                    new_zones.push(zone);
                }
                Err((name, error)) => {
                    warn!(self.inner.log, "Zone failed to start"; "zone" => &name);
                    errors.push((name, error))
                }
            }
        }
        Ok(StartZonesResult { new_zones, errors })
    }

    /// Create a zone bundle for the provided zone.
    pub async fn create_zone_bundle(
        &self,
        name: &str,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        // Search for the named zone.
        if let SledLocalZone::Running { zone, .. } =
            &*self.inner.switch_zone.lock().await
        {
            if zone.name() == name {
                return self
                    .inner
                    .zone_bundler
                    .create(zone, ZoneBundleCause::ExplicitRequest)
                    .await;
            }
        }
        if let Some(zone) = self.inner.zones.lock().await.get(name) {
            return self
                .inner
                .zone_bundler
                .create(&zone.runtime, ZoneBundleCause::ExplicitRequest)
                .await;
        }
        Err(BundleError::NoSuchZone { name: name.to_string() })
    }

    /// Returns the current Omicron zone configuration
    pub async fn omicron_zones_list(
        &self,
    ) -> Result<OmicronZonesConfig, Error> {
        let log = &self.inner.log;

        // We need to take the lock in order for the information in the ledger
        // to be up-to-date.
        let _existing_zones = self.inner.zones.lock().await;

        // Read the existing set of services from the ledger.
        let zone_ledger_paths = self.all_omicron_zone_ledgers().await;
        let ledger_data = match Ledger::<OmicronZonesConfigLocal>::new(
            log,
            zone_ledger_paths.clone(),
        )
        .await
        {
            Some(ledger) => ledger.data().clone(),
            None => OmicronZonesConfigLocal::initial(),
        };

        Ok(ledger_data.to_omicron_zones_config())
    }

    /// Ensures that particular Omicron zones are running
    ///
    /// These services will be instantiated by this function, and will be
    /// recorded to a local file to ensure they start automatically on next
    /// boot.
    pub async fn ensure_all_omicron_zones_persistent(
        &self,
        mut request: OmicronZonesConfig,
        fake_install_dir: Option<&String>,
    ) -> Result<(), Error> {
        let log = &self.inner.log;

        let mut existing_zones = self.inner.zones.lock().await;

        // Read the existing set of services from the ledger.
        let zone_ledger_paths = self.all_omicron_zone_ledgers().await;
        let mut ledger = match Ledger::<OmicronZonesConfigLocal>::new(
            log,
            zone_ledger_paths.clone(),
        )
        .await
        {
            Some(ledger) => ledger,
            None => Ledger::<OmicronZonesConfigLocal>::new_with(
                log,
                zone_ledger_paths.clone(),
                OmicronZonesConfigLocal::initial(),
            ),
        };

        let ledger_zone_config = ledger.data_mut();
        debug!(log, "ensure_all_omicron_zones_persistent";
            "request_generation" => request.generation.to_string(),
            "ledger_generation" =>
                ledger_zone_config.omicron_generation.to_string(),
        );

        // Absolutely refuse to downgrade the configuration.
        if ledger_zone_config.omicron_generation > request.generation {
            return Err(Error::RequestedConfigOutdated {
                requested: request.generation,
                current: ledger_zone_config.omicron_generation,
            });
        }

        // If the generation is the same as what we're running, but the contents
        // aren't, that's a problem, too.
        if ledger_zone_config.omicron_generation == request.generation {
            // Nexus should send us consistent zone orderings; however, we may
            // reorder the zone list inside `ensure_all_omicron_zones`. To avoid
            // equality checks failing only because the two lists are ordered
            // differently, sort them both here before comparing.
            let mut ledger_zones =
                ledger_zone_config.clone().to_omicron_zones_config().zones;

            // We sort by ID because we assume no two zones have the same ID. If
            // that assumption is wrong, we may return an error here where the
            // conflict is soley the list orders, but in such a case that's the
            // least of our problems.
            ledger_zones.sort_by_key(|z| z.id);
            request.zones.sort_by_key(|z| z.id);

            if ledger_zones != request.zones {
                return Err(Error::RequestedConfigConflicts(
                    request.generation,
                ));
            }
        }

        let omicron_generation = request.generation;
        let ledger_generation = ledger_zone_config.ledger_generation;
        self.ensure_all_omicron_zones(
            &mut existing_zones,
            request,
            fake_install_dir,
        )
        .await?;
        let zones = existing_zones
            .values()
            .map(|omicron_zone| omicron_zone.config.clone())
            .collect();

        let new_config = OmicronZonesConfigLocal {
            omicron_generation,
            ledger_generation,
            zones,
        };

        // If the contents of the ledger would be identical, we can avoid
        // performing an update and commit.
        if *ledger_zone_config == new_config {
            return Ok(());
        }

        // Update the zones in the ledger and write it back to both M.2s
        *ledger_zone_config = new_config;
        ledger.commit().await?;

        Ok(())
    }

    // Ensures that only the following Omicron zones are running.
    //
    // This method strives to be idempotent.
    //
    // - Starting and stopping zones is not an atomic operation - it's possible
    // that we cannot start a zone after a previous one has been successfully
    // created (or destroyed) intentionally. As a result, even in error cases,
    // it's possible that the set of `existing_zones` changes. However, this set
    // will only change in the direction of `new_request`: zones will only be
    // removed if they ARE NOT part of `new_request`, and zones will only be
    // added if they ARE part of `new_request`.
    // - Zones are not updated in-place: two zone configurations that differ
    // in any way are treated as entirely distinct.
    // - This method does not record any information such that these services
    // are re-instantiated on boot.
    async fn ensure_all_omicron_zones(
        &self,
        // The MutexGuard here attempts to ensure that the caller has the right
        // lock held when calling this function.
        existing_zones: &mut MutexGuard<'_, ZoneMap>,
        new_request: OmicronZonesConfig,
        fake_install_dir: Option<&String>,
    ) -> Result<(), Error> {
        // Do some data-normalization to ensure we can compare the "requested
        // set" vs the "existing set" as HashSets.
        let old_zone_configs: HashSet<OmicronZoneConfig> = existing_zones
            .values()
            .map(|omicron_zone| omicron_zone.config.zone.clone())
            .collect();
        let requested_zones_set: HashSet<OmicronZoneConfig> =
            new_request.zones.into_iter().collect();

        let zones_to_be_removed =
            old_zone_configs.difference(&requested_zones_set);
        let zones_to_be_added =
            requested_zones_set.difference(&old_zone_configs);
        // Destroy zones that should not be running
        for zone in zones_to_be_removed {
            self.zone_bundle_and_try_remove(existing_zones, &zone).await;
        }

        // Collect information that's necessary to start new zones
        let storage = self.inner.storage.get_latest_disks().await;
        let mount_config = &storage.mount_config;
        let all_u2_pools = storage.all_u2_zpools();
        let time_is_synchronized =
            match self.timesync_get_locked(&existing_zones).await {
                // Time is synchronized
                Ok(TimeSync { sync: true, .. }) => true,
                // Time is not synchronized, or we can't check
                _ => false,
            };

        // Concurrently boot all new zones
        let StartZonesResult { new_zones, errors } = self
            .start_omicron_zones(
                mount_config,
                zones_to_be_added,
                time_is_synchronized,
                &all_u2_pools,
                fake_install_dir,
            )
            .await?;

        // Add the new zones to our tracked zone set
        existing_zones.extend(
            new_zones.into_iter().map(|zone| (zone.name().to_string(), zone)),
        );

        // If any zones failed to start, exit with an error
        if !errors.is_empty() {
            return Err(Error::ZoneEnsure { errors });
        }
        Ok(())
    }

    // Attempts to take a zone bundle and remove a zone.
    //
    // Logs, but does not return an error on failure.
    async fn zone_bundle_and_try_remove(
        &self,
        existing_zones: &mut MutexGuard<'_, ZoneMap>,
        zone: &OmicronZoneConfig,
    ) {
        let log = &self.inner.log;
        let expected_zone_name = zone.zone_name();
        let Some(mut zone) = existing_zones.remove(&expected_zone_name) else {
            warn!(
                log,
                "Expected to remove zone, but could not find it";
                "zone_name" => &expected_zone_name,
            );
            return;
        };
        debug!(
            log,
            "removing an existing zone";
            "zone_name" => &expected_zone_name,
        );
        if let Err(e) = self
            .inner
            .zone_bundler
            .create(&zone.runtime, ZoneBundleCause::UnexpectedZone)
            .await
        {
            error!(
                log,
                "Failed to take bundle of unexpected zone";
                "zone_name" => &expected_zone_name,
                "reason" => ?e,
            );
        }
        if let Err(e) = zone.runtime.stop().await {
            error!(log, "Failed to stop zone {}: {e}", zone.name());
        }
    }

    // Ensures that if a zone is about to be installed, it does not exist.
    async fn ensure_removed(
        &self,
        zone: &OmicronZoneConfig,
    ) -> Result<(), Error> {
        let zone_name = zone.zone_name();
        match Zones::find(&zone_name).await {
            Ok(Some(zone)) => {
                warn!(
                    self.inner.log,
                    "removing zone";
                    "zone" => &zone_name,
                    "state" => ?zone.state(),
                );
                if let Err(e) =
                    Zones::halt_and_remove_logged(&self.inner.log, &zone_name)
                        .await
                {
                    error!(
                        self.inner.log,
                        "Failed to remove zone";
                        "zone" => &zone_name,
                        "error" => %e,
                    );
                    return Err(Error::ZoneRemoval {
                        zone_name: zone_name.to_string(),
                        err: e,
                    });
                }
                return Ok(());
            }
            Ok(None) => return Ok(()),
            Err(err) => return Err(Error::ZoneList(err)),
        }
    }

    // Returns a zone filesystem mountpoint, after ensuring that U.2 storage
    // is valid.
    async fn validate_storage_and_pick_mountpoint(
        &self,
        mount_config: &MountConfig,
        zone: &OmicronZoneConfig,
        all_u2_pools: &Vec<ZpoolName>,
    ) -> Result<Utf8PathBuf, Error> {
        let name = zone.zone_name();

        // For each new zone request, we pick a U.2 to store the zone
        // filesystem. Note: This isn't known to Nexus right now, so it's a
        // local-to-sled decision.
        //
        // Currently, the zone filesystem should be destroyed between
        // reboots, so it's fine to make this decision locally.
        let root = if let Some(dataset) = zone.dataset_name() {
            // Check that the dataset is actually ready to be used.
            let [zoned, canmount, encryption] =
                illumos_utils::zfs::Zfs::get_values(
                    &dataset.full_name(),
                    &["zoned", "canmount", "encryption"],
                )
                .map_err(|err| Error::GetZfsValue {
                    zone: zone.zone_name(),
                    source: err,
                })?;

            let check_property = |name, actual, expected| {
                if actual != expected {
                    return Err(Error::DatasetNotReady {
                        zone: zone.zone_name(),
                        dataset: dataset.full_name(),
                        prop_name: String::from(name),
                        prop_value: actual,
                        prop_value_expected: String::from(expected),
                    });
                }
                return Ok(());
            };
            check_property("zoned", zoned, "on")?;
            check_property("canmount", canmount, "on")?;
            if dataset.dataset().dataset_should_be_encrypted() {
                check_property("encryption", encryption, "aes-256-gcm")?;
            }

            // If the zone happens to already manage a dataset, then
            // we co-locate the zone dataset on the same zpool.
            //
            // This slightly reduces the underlying fault domain for the
            // service.
            let data_pool = dataset.pool();
            if !all_u2_pools.contains(&data_pool) {
                warn!(
                    self.inner.log,
                    "zone dataset requested on a zpool which doesn't exist";
                    "zone" => &name,
                    "zpool" => %data_pool
                );
                return Err(Error::MissingDevice {
                    device: format!("zpool: {data_pool}"),
                });
            }
            data_pool.dataset_mountpoint(&mount_config.root, ZONE_DATASET)
        } else {
            // If the zone it not coupled to other datsets, we pick one
            // arbitrarily.
            let mut rng = rand::thread_rng();
            all_u2_pools
                .choose(&mut rng)
                .map(|pool| {
                    pool.dataset_mountpoint(&mount_config.root, ZONE_DATASET)
                })
                .ok_or_else(|| Error::U2NotFound)?
                .clone()
        };
        Ok(root)
    }

    pub async fn cockroachdb_initialize(&self) -> Result<(), Error> {
        let log = &self.inner.log;
        let dataset_zones = self.inner.zones.lock().await;
        for zone in dataset_zones.values() {
            // TODO: We could probably store the ZoneKind in the running zone to
            // make this "comparison to existing zones by name" mechanism a bit
            // safer.
            if zone.name().contains(&ZoneType::CockroachDb.to_string()) {
                let address = Zones::get_address(
                    Some(zone.name()),
                    &zone.runtime.control_interface(),
                )?
                .ip();
                let host = &format!("[{address}]:{COCKROACH_PORT}");
                info!(
                    log,
                    "Initializing CRDB Cluster - sending request to {host}"
                );
                if let Err(err) = zone.runtime.run_cmd(&[
                    "/opt/oxide/cockroachdb/bin/cockroach",
                    "init",
                    "--insecure",
                    "--host",
                    host,
                ]) {
                    if !err
                        .to_string()
                        .contains("cluster has already been initialized")
                    {
                        return Err(Error::CockroachInit { err });
                    }
                };
                info!(log, "Formatting CRDB");
                zone.runtime
                    .run_cmd(&[
                        "/opt/oxide/cockroachdb/bin/cockroach",
                        "sql",
                        "--insecure",
                        "--host",
                        host,
                        "--file",
                        "/opt/oxide/cockroachdb/sql/dbwipe.sql",
                    ])
                    .map_err(|err| Error::CockroachInit { err })?;
                zone.runtime
                    .run_cmd(&[
                        "/opt/oxide/cockroachdb/bin/cockroach",
                        "sql",
                        "--insecure",
                        "--host",
                        host,
                        "--file",
                        "/opt/oxide/cockroachdb/sql/dbinit.sql",
                    ])
                    .map_err(|err| Error::CockroachInit { err })?;
                info!(log, "Formatting CRDB - Completed");

                // In the single-sled case, if there are multiple CRDB nodes on
                // a single device, we'd still only want to send the
                // initialization requests to a single dataset.
                return Ok(());
            }
        }

        Ok(())
    }

    pub fn boottime_rewrite(&self) {
        if self
            .inner
            .time_synced
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            // Already done.
            return;
        }

        // Call out to the 'tmpx' utility program which will rewrite the wtmpx
        // and utmpx databases in every zone, including the global zone, to
        // reflect the adjusted system boot time.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&["/usr/platform/oxide/bin/tmpx", "-Z"]);
        if let Err(e) = execute(cmd) {
            warn!(self.inner.log, "Updating [wu]tmpx databases failed: {}", e);
        }
    }

    pub async fn timesync_get(&self) -> Result<TimeSync, Error> {
        let existing_zones = self.inner.zones.lock().await;
        self.timesync_get_locked(&existing_zones).await
    }

    async fn timesync_get_locked(
        &self,
        existing_zones: &tokio::sync::MutexGuard<'_, ZoneMap>,
    ) -> Result<TimeSync, Error> {
        let skip_timesync = match &self.inner.time_sync_config {
            TimeSyncConfig::Normal => false,
            TimeSyncConfig::Skip => true,
            #[cfg(all(test, target_os = "illumos"))]
            TimeSyncConfig::Fail => {
                info!(self.inner.log, "Configured to fail timesync checks");
                return Err(Error::TimeNotSynchronized);
            }
        };

        if skip_timesync {
            info!(self.inner.log, "Configured to skip timesync checks");
            self.boottime_rewrite();
            return Ok(TimeSync {
                sync: true,
                ref_id: 0,
                ip_addr: IPV6_UNSPECIFIED,
                stratum: 0,
                ref_time: 0.0,
                correction: 0.00,
            });
        };

        let ntp_zone_name =
            InstalledZone::get_zone_name(&ZoneType::Ntp.to_string(), None);

        let ntp_zone = existing_zones
            .iter()
            .find(|(name, _)| name.starts_with(&ntp_zone_name))
            .ok_or_else(|| Error::NtpZoneNotReady)?
            .1;

        // XXXNTP - This could be replaced with a direct connection to the
        // daemon using a patched version of the chrony_candm crate to allow
        // a custom server socket path. From the GZ, it should be possible to
        // connect to the UNIX socket at
        // format!("{}/var/run/chrony/chronyd.sock", ntp_zone.root())

        match ntp_zone.runtime.run_cmd(&["/usr/bin/chronyc", "-c", "tracking"])
        {
            Ok(stdout) => {
                let v: Vec<&str> = stdout.split(',').collect();

                if v.len() > 9 {
                    let ref_id = u32::from_str_radix(v[0], 16)
                        .map_err(|_| Error::NtpZoneNotReady)?;
                    let ip_addr =
                        IpAddr::from_str(v[1]).unwrap_or(IPV6_UNSPECIFIED);
                    let stratum = u8::from_str(v[2])
                        .map_err(|_| Error::NtpZoneNotReady)?;
                    let ref_time = f64::from_str(v[3])
                        .map_err(|_| Error::NtpZoneNotReady)?;
                    let correction = f64::from_str(v[4])
                        .map_err(|_| Error::NtpZoneNotReady)?;

                    // Per `chronyc waitsync`'s implementation, if either the
                    // reference IP address is not unspecified or the reference
                    // ID is not 0 or 0x7f7f0101, we are synchronized to a peer.
                    let peer_sync = !ip_addr.is_unspecified()
                        || (ref_id != 0 && ref_id != 0x7f7f0101);

                    let sync = stratum < 10
                        && ref_time > 1234567890.0
                        && peer_sync
                        && correction.abs() <= 0.05;

                    if sync {
                        self.boottime_rewrite();
                    }

                    Ok(TimeSync {
                        sync,
                        ref_id,
                        ip_addr,
                        stratum,
                        ref_time,
                        correction,
                    })
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
        // If we're reconfiguring the switch zone with an underlay address, we
        // also need the rack network config to set tfport uplinks.
        underlay_info: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
        baseboard: Baseboard,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ensuring scrimlet services (enabling services)");
        let mut filesystems: Vec<zone::Fs> = vec![];
        let mut data_links: Vec<String> = vec![];

        let services = match self.inner.sled_mode {
            // A pure gimlet sled should not be trying to activate a switch
            // zone.
            SledMode::Gimlet => {
                return Err(Error::SledLocalZone(anyhow::anyhow!(
                    "attempted to activate switch zone on non-scrimlet sled"
                )))
            }

            // Sled is a scrimlet and the real tofino driver has been loaded.
            SledMode::Auto
            | SledMode::Scrimlet { asic: DendriteAsic::TofinoAsic } => {
                vec![
                    SwitchService::Dendrite { asic: DendriteAsic::TofinoAsic },
                    SwitchService::Lldpd { baseboard: baseboard.clone() },
                    SwitchService::ManagementGatewayService,
                    SwitchService::Pumpkind { asic: DendriteAsic::TofinoAsic },
                    SwitchService::Tfport {
                        pkt_source: "tfpkt0".to_string(),
                        asic: DendriteAsic::TofinoAsic,
                    },
                    SwitchService::Uplink,
                    SwitchService::Wicketd { baseboard: baseboard.clone() },
                    SwitchService::Mgd,
                    SwitchService::MgDdm { mode: "transit".to_string() },
                ]
            }

            SledMode::Scrimlet {
                asic: asic @ DendriteAsic::SoftNpuPropolisDevice,
            } => {
                data_links = vec!["vioif0".to_owned()];
                vec![
                    SwitchService::Dendrite { asic },
                    SwitchService::Lldpd { baseboard: baseboard.clone() },
                    SwitchService::ManagementGatewayService,
                    SwitchService::Uplink,
                    SwitchService::Wicketd { baseboard: baseboard.clone() },
                    SwitchService::Mgd,
                    SwitchService::MgDdm { mode: "transit".to_string() },
                    SwitchService::Tfport {
                        pkt_source: "vioif0".to_string(),
                        asic,
                    },
                    SwitchService::SpSim,
                ]
            }

            // Sled is a scrimlet but is not running the real tofino driver.
            SledMode::Scrimlet {
                asic:
                    asic @ (DendriteAsic::TofinoStub | DendriteAsic::SoftNpuZone),
            } => {
                if let DendriteAsic::SoftNpuZone = asic {
                    let softnpu_filesystem = zone::Fs {
                        ty: "lofs".to_string(),
                        dir: "/opt/softnpu/stuff".to_string(),
                        special: "/var/run/softnpu/sidecar".to_string(),
                        ..Default::default()
                    };
                    filesystems.push(softnpu_filesystem);
                    data_links = Dladm::get_simulated_tfports()?;
                }
                vec![
                    SwitchService::Dendrite { asic },
                    SwitchService::Lldpd { baseboard: baseboard.clone() },
                    SwitchService::ManagementGatewayService,
                    SwitchService::Uplink,
                    SwitchService::Wicketd { baseboard: baseboard.clone() },
                    SwitchService::Mgd,
                    SwitchService::MgDdm { mode: "transit".to_string() },
                    SwitchService::Tfport {
                        pkt_source: "tfpkt0".to_string(),
                        asic,
                    },
                    SwitchService::SpSim,
                ]
            }
        };

        let mut addresses =
            if let Some((ip, _)) = underlay_info { vec![ip] } else { vec![] };
        addresses.push(Ipv6Addr::LOCALHOST);

        let request =
            SwitchZoneConfig { id: Uuid::new_v4(), addresses, services };

        self.ensure_zone(
            // request=
            Some(request),
            // filesystems=
            filesystems,
            // data_links=
            data_links,
        )
        .await?;

        // If we've given the switch an underlay address, we also need to inject
        // SMF properties so that tfport uplinks can be created.
        if let Some((ip, Some(rack_network_config))) = underlay_info {
            self.ensure_switch_zone_uplinks_configured(ip, rack_network_config)
                .await?;
        }

        Ok(())
    }

    // Ensure our switch zone (at the given IP address) has its uplinks
    // configured based on `rack_network_config`. This first requires us to ask
    // MGS running in the switch zone which switch we are, so we know which
    // uplinks from `rack_network_config` to assign.
    async fn ensure_switch_zone_uplinks_configured(
        &self,
        switch_zone_ip: Ipv6Addr,
        rack_network_config: &RackNetworkConfig,
    ) -> Result<(), Error> {
        let log = &self.inner.log;

        // Configure uplinks via DPD in our switch zone.
        let our_ports = EarlyNetworkSetup::new(log)
            .init_switch_config(rack_network_config, switch_zone_ip)
            .await?
            .into_iter()
            .map(From::from)
            .collect();

        self.ensure_scrimlet_host_ports(our_ports).await
    }

    pub async fn ensure_scrimlet_host_ports(
        &self,
        our_ports: Vec<HostPortConfig>,
    ) -> Result<(), Error> {
        // We expect the switch zone to be running, as we're called immediately
        // after `ensure_zone()` above and we just successfully configured
        // uplinks via DPD running in our switch zone. If somehow we're in any
        // other state, bail out.
        let mut switch_zone = self.inner.switch_zone.lock().await;

        let zone = match &mut *switch_zone {
            SledLocalZone::Running { zone, .. } => zone,
            SledLocalZone::Disabled => {
                return Err(Error::SledLocalZone(anyhow!(
                    "Cannot configure switch zone uplinks: \
                     switch zone disabled"
                )));
            }
            SledLocalZone::Initializing { .. } => {
                return Err(Error::SledLocalZone(anyhow!(
                    "Cannot configure switch zone uplinks: \
                     switch zone still initializing"
                )));
            }
        };

        let smfh = SmfHelper::new(&zone, &SwitchService::Uplink);

        // We want to delete all the properties in the `uplinks` group, but we
        // don't know their names, so instead we'll delete and recreate the
        // group, then add all our properties.
        smfh.delpropgroup("uplinks")?;
        smfh.addpropgroup("uplinks", "application")?;

        for port_config in &our_ports {
            for addr in &port_config.addrs {
                smfh.addpropvalue_type(
                    &format!("uplinks/{}_0", port_config.port,),
                    &addr.to_string(),
                    "astring",
                )?;
            }
        }
        smfh.refresh()?;

        Ok(())
    }

    /// Ensures that no switch zone is active.
    pub async fn deactivate_switch(&self) -> Result<(), Error> {
        self.ensure_zone(
            // request=
            None,
            // filesystems=
            vec![],
            // data_links=
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
        request: SwitchZoneConfig,
        filesystems: Vec<zone::Fs>,
        data_links: Vec<String>,
    ) {
        let (exit_tx, exit_rx) = oneshot::channel();
        *zone = SledLocalZone::Initializing {
            request,
            filesystems,
            data_links,
            worker: Some(Task {
                exit_tx,
                initializer: tokio::task::spawn(async move {
                    self.initialize_zone_loop(exit_rx).await
                }),
            }),
        };
    }

    // Moves the current state to align with the "request".
    async fn ensure_zone(
        &self,
        request: Option<SwitchZoneConfig>,
        filesystems: Vec<zone::Fs>,
        data_links: Vec<String>,
    ) -> Result<(), Error> {
        let log = &self.inner.log;

        let mut sled_zone = self.inner.switch_zone.lock().await;
        let zone_typestr = "switch";

        match (&mut *sled_zone, request) {
            (SledLocalZone::Disabled, Some(request)) => {
                info!(log, "Enabling {zone_typestr} zone (new)");
                self.clone().start_zone(
                    &mut sled_zone,
                    request,
                    filesystems,
                    data_links,
                );
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

                let first_address = request.addresses.get(0);
                let address = first_address
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
                    let smfh = SmfHelper::new(&zone, service);

                    match service {
                        SwitchService::ManagementGatewayService => {
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
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                            )?;

                            // It should be impossible for the `sled_info` not
                            // to be set here, as the underlay is set at the
                            // same time.
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_id", info.rack_id)?;
                            } else {
                                error!(
                                    self.inner.log,
                                    concat!(
                                        "rack_id not present,",
                                        " even though underlay address exists"
                                    )
                                );
                            }

                            smfh.refresh()?;
                        }
                        SwitchService::Dendrite { .. } => {
                            info!(
                                self.inner.log,
                                "configuring dendrite service"
                            );
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
                        SwitchService::Wicketd { .. } => {
                            if let Some(&address) = first_address {
                                let rack_subnet =
                                    Ipv6Subnet::<AZ_PREFIX>::new(address);

                                info!(
                                    self.inner.log, "configuring wicketd";
                                    "rack_subnet" => %rack_subnet.net().addr(),
                                );

                                smfh.setprop(
                                    "config/rack-subnet",
                                    &rack_subnet.net().addr().to_string(),
                                )?;

                                smfh.refresh()?;
                            } else {
                                error!(
                                    self.inner.log,
                                    "underlay address unexpectedly missing",
                                );
                            }
                        }
                        SwitchService::Lldpd { .. } => {
                            info!(self.inner.log, "configuring lldp service");
                            smfh.delpropvalue("config/address", "*")?;
                            for address in &request.addresses {
                                smfh.addpropvalue(
                                    "config/address",
                                    &format!("[{}]:{}", address, LLDP_PORT),
                                )?;
                            }
                            smfh.refresh()?;
                        }
                        SwitchService::Tfport { .. } => {
                            // Since tfport and dpd communicate using localhost,
                            // the tfport service shouldn't need to be
                            // restarted.
                        }
                        SwitchService::Pumpkind { .. } => {
                            // Unless we want to plumb through the "only log
                            // errors, don't react" option, there are no user
                            // serviceable parts for this daemon.
                        }
                        SwitchService::Uplink { .. } => {
                            // Only configured in
                            // `ensure_switch_zone_uplinks_configured`
                        }
                        SwitchService::SpSim => {
                            // nothing to configure
                        }
                        SwitchService::Mgd => {
                            info!(self.inner.log, "configuring mgd service");
                            smfh.delpropvalue("config/dns_servers", "*")?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_uuid", info.rack_id)?;
                                smfh.setprop(
                                    "config/sled_uuid",
                                    info.config.sled_id,
                                )?;
                            }
                            for address in &request.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                        )?;
                                    }
                                    break;
                                }
                            }
                            smfh.refresh()?;
                        }
                        SwitchService::MgDdm { mode } => {
                            info!(self.inner.log, "configuring mg-ddm service");
                            smfh.delpropvalue("config/mode", "*")?;
                            smfh.addpropvalue("config/mode", &mode)?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop("config/rack_uuid", info.rack_id)?;
                                smfh.setprop(
                                    "config/sled_uuid",
                                    info.config.sled_id,
                                )?;
                            }
                            smfh.delpropvalue("config/dns_servers", "*")?;
                            for address in &request.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                        )?;
                                    }
                                    break;
                                }
                            }
                            smfh.refresh()?;
                        }
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
        let SledLocalZone::Initializing {
            request,
            filesystems,
            data_links,
            ..
        } = &*sled_zone
        else {
            return Ok(());
        };

        // The switch zone must use the ramdisk in order to receive requests
        // from RSS to initialize the rack. This enables the initialization of
        // trust quorum to derive disk encryption keys for U.2 devices. If the
        // switch zone were on a U.2 device we would not be able to run RSS, as
        // we could not create the U.2 disks due to lack of encryption. To break
        // the cycle we put the switch zone root fs on the ramdisk.
        let root = Utf8PathBuf::from(ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT);
        let zone_request =
            SwitchZoneConfigLocal { root, zone: request.clone() };
        let zone_args = ZoneArgs::Switch(&zone_request);
        let zone = self
            .initialize_zone(zone_args, filesystems, data_links, None)
            .await?;
        *sled_zone = SledLocalZone::Running { request: request.clone(), zone };
        Ok(())
    }

    // Body of a tokio task responsible for running until the switch zone is
    // inititalized, or it has been told to stop.
    async fn initialize_zone_loop(&self, mut exit_rx: oneshot::Receiver<()>) {
        loop {
            {
                let mut sled_zone = self.inner.switch_zone.lock().await;
                match self.try_initialize_sled_local_zone(&mut sled_zone).await
                {
                    Ok(()) => return,
                    Err(e) => warn!(
                        self.inner.log,
                        "Failed to initialize switch zone: {e}"
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

#[cfg(all(test, target_os = "illumos"))]
mod test {
    use super::*;
    use illumos_utils::{
        dladm::{
            Etherstub, MockDladm, BOOTSTRAP_ETHERSTUB_NAME,
            UNDERLAY_ETHERSTUB_NAME, UNDERLAY_ETHERSTUB_VNIC_NAME,
        },
        svc,
        zone::MockZones,
    };

    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::os::unix::process::ExitStatusExt;
    use uuid::Uuid;

    // Just placeholders. Not used.
    const GLOBAL_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;
    const SWITCH_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;

    const EXPECTED_ZONE_NAME_PREFIX: &str = "oxz_ntp";
    const EXPECTED_PORT: u16 = 12223;

    fn make_bootstrap_networking_config() -> BootstrapNetworking {
        BootstrapNetworking {
            bootstrap_etherstub: Etherstub(
                BOOTSTRAP_ETHERSTUB_NAME.to_string(),
            ),
            global_zone_bootstrap_ip: GLOBAL_ZONE_BOOTSTRAP_IP,
            global_zone_bootstrap_link_local_ip: GLOBAL_ZONE_BOOTSTRAP_IP,
            switch_zone_bootstrap_ip: SWITCH_ZONE_BOOTSTRAP_IP,
            underlay_etherstub: Etherstub(UNDERLAY_ETHERSTUB_NAME.to_string()),
            underlay_etherstub_vnic: EtherstubVnic(
                UNDERLAY_ETHERSTUB_VNIC_NAME.to_string(),
            ),
        }
    }

    // Returns the expectations for a new service to be created.
    fn expect_new_service(
        expected_zone_name_prefix: &str,
    ) -> Vec<Box<dyn std::any::Any>> {
        illumos_utils::USE_MOCKS.store(true, Ordering::SeqCst);

        // Ensure zone doesn't already exist
        let find_zone_ctx = MockZones::find_context();
        let prefix = expected_zone_name_prefix.to_string();
        find_zone_ctx.expect().return_once(move |zone_name| {
            assert!(zone_name.starts_with(&prefix));
            Ok(None)
        });

        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().return_once(
            |physical_link: &Etherstub, _, _, _, _| {
                assert_eq!(&physical_link.0, &UNDERLAY_ETHERSTUB_NAME);
                Ok(())
            },
        );
        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        let prefix = expected_zone_name_prefix.to_string();
        install_ctx.expect().return_once(
            move |_, _, name, _, _, _, _, _, _| {
                assert!(name.starts_with(&prefix));
                Ok(())
            },
        );

        // Boot the zone.
        let boot_ctx = MockZones::boot_context();
        let prefix = expected_zone_name_prefix.to_string();
        boot_ctx.expect().return_once(move |name| {
            assert!(name.starts_with(&prefix));
            Ok(())
        });

        // After calling `MockZones::boot`, `RunningZone::boot` will then look
        // up the zone ID for the booted zone. This goes through
        // `MockZone::id` to find the zone and get its ID.
        let id_ctx = MockZones::id_context();
        let prefix = expected_zone_name_prefix.to_string();
        id_ctx.expect().return_once(move |name| {
            assert!(name.starts_with(&prefix));
            Ok(Some(1))
        });

        // Ensure the address exists
        let ensure_address_ctx = MockZones::ensure_address_context();
        ensure_address_ctx.expect().return_once(|_, _, _| {
            Ok(ipnetwork::IpNetwork::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 64)
                .unwrap())
        });

        // Wait for the networking service.
        let wait_ctx = svc::wait_for_service_context();
        wait_ctx.expect().return_once(|_, _, _| Ok(()));

        let execute_ctx = illumos_utils::execute_helper_context();
        execute_ctx.expect().times(..).returning(|_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![],
                stderr: vec![],
            })
        });

        vec![
            Box::new(find_zone_ctx),
            Box::new(create_vnic_ctx),
            Box::new(install_ctx),
            Box::new(boot_ctx),
            Box::new(id_ctx),
            Box::new(ensure_address_ctx),
            Box::new(wait_ctx),
            Box::new(execute_ctx),
        ]
    }

    // Configures our mock implementations to work for cases where we configure
    // multiple zones in one `ensure_all_omicron_zones_persistent()` call.
    //
    // This is looser than the expectations created by ensure_new_service()
    // because these functions may return any number of times.
    fn expect_new_services() -> Vec<Box<dyn std::any::Any>> {
        illumos_utils::USE_MOCKS.store(true, Ordering::SeqCst);

        // Ensure zones don't already exist
        let find_zone_ctx = MockZones::find_context();
        find_zone_ctx.expect().returning(move |_zone_name| Ok(None));

        // Create a VNIC
        let create_vnic_ctx = MockDladm::create_vnic_context();
        create_vnic_ctx.expect().returning(
            |physical_link: &Etherstub, _, _, _, _| {
                assert_eq!(&physical_link.0, &UNDERLAY_ETHERSTUB_NAME);
                Ok(())
            },
        );

        // Install the Omicron Zone
        let install_ctx = MockZones::install_omicron_zone_context();
        install_ctx.expect().returning(|_, _, name, _, _, _, _, _, _| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
            Ok(())
        });

        // Boot the zone.
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().returning(|name| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
            Ok(())
        });

        // After calling `MockZones::boot`, `RunningZone::boot` will then look
        // up the zone ID for the booted zone. This goes through
        // `MockZone::id` to find the zone and get its ID.
        let id_ctx = MockZones::id_context();
        let id = Arc::new(std::sync::Mutex::new(1));
        id_ctx.expect().returning(move |name| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
            let mut value = id.lock().unwrap();
            let rv = *value;
            *value = rv + 1;
            Ok(Some(rv))
        });

        // Ensure the address exists
        let ensure_address_ctx = MockZones::ensure_address_context();
        ensure_address_ctx.expect().returning(|_, _, _| {
            Ok(ipnetwork::IpNetwork::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 64)
                .unwrap())
        });

        // Wait for the networking service.
        let wait_ctx = svc::wait_for_service_context();
        wait_ctx.expect().returning(|_, _, _| Ok(()));

        // Import the manifest, enable the service
        let execute_ctx = illumos_utils::execute_helper_context();
        execute_ctx.expect().times(..).returning(|_| {
            Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: vec![],
                stderr: vec![],
            })
        });

        vec![
            Box::new(find_zone_ctx),
            Box::new(create_vnic_ctx),
            Box::new(install_ctx),
            Box::new(boot_ctx),
            Box::new(id_ctx),
            Box::new(ensure_address_ctx),
            Box::new(wait_ctx),
            Box::new(execute_ctx),
        ]
    }

    // Prepare to call "ensure" for a new service, then actually call "ensure".
    async fn ensure_new_service(
        mgr: &ServiceManager,
        id: Uuid,
        generation: Generation,
        tmp_dir: String,
    ) {
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, EXPECTED_PORT, 0, 0);
        try_new_service_of_type(
            mgr,
            id,
            generation,
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers: vec![],
                dns_servers: vec![],
                domain: None,
            },
            tmp_dir,
        )
        .await
        .expect("Could not create service");
    }

    async fn try_new_service_of_type(
        mgr: &ServiceManager,
        id: Uuid,
        generation: Generation,
        zone_type: OmicronZoneType,
        tmp_dir: String,
    ) -> Result<(), Error> {
        let zone_prefix = format!("oxz_{}", zone_type.zone_type_str());
        let _expectations = expect_new_service(&zone_prefix);
        let r = mgr
            .ensure_all_omicron_zones_persistent(
                OmicronZonesConfig {
                    generation,
                    zones: vec![OmicronZoneConfig {
                        id,
                        underlay_address: Ipv6Addr::LOCALHOST,
                        zone_type,
                    }],
                },
                Some(&tmp_dir),
            )
            .await;
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        r
    }

    // Prepare to call "ensure" for a service which already exists. We should
    // return the service without actually installing a new zone.
    async fn ensure_existing_service(
        mgr: &ServiceManager,
        id: Uuid,
        generation: Generation,
        tmp_dir: String,
    ) {
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, EXPECTED_PORT, 0, 0);
        mgr.ensure_all_omicron_zones_persistent(
            OmicronZonesConfig {
                generation,
                zones: vec![OmicronZoneConfig {
                    id,
                    underlay_address: Ipv6Addr::LOCALHOST,
                    zone_type: OmicronZoneType::InternalNtp {
                        address,
                        ntp_servers: vec![],
                        dns_servers: vec![],
                        domain: None,
                    },
                }],
            },
            Some(&tmp_dir),
        )
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
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
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
                sidecar_revision: SidecarRevision::Physical(
                    "rev_whatever_its_a_test".to_string(),
                ),
            }
        }

        fn override_paths(&self, mgr: &ServiceManager) {
            let dir = self.config_dir.path();
            mgr.override_ledger_directory(dir.to_path_buf());
            mgr.override_image_directory(dir.to_path_buf());

            // We test launching "fake" versions of the zones, but the
            // logic to find paths relies on checking the existence of
            // files.
            std::fs::write(dir.join("oximeter.tar.gz"), "Not a real file")
                .unwrap();
            std::fs::write(dir.join("ntp.tar.gz"), "Not a real file").unwrap();
        }
    }

    async fn setup_storage(log: &Logger) -> StorageManagerTestHarness {
        let mut harness = StorageManagerTestHarness::new(&log).await;
        let raw_disks =
            harness.add_vdevs(&["u2_test.vdev", "m2_test.vdev"]).await;
        harness.handle().key_manager_ready().await;
        let config = harness.make_config(1, &raw_disks);
        let result = harness
            .handle()
            .omicron_physical_disks_ensure(config.clone())
            .await
            .expect("Failed to ensure disks");
        assert!(!result.has_error(), "{:?}", result);
        harness
    }

    struct LedgerTestHelper<'a> {
        log: slog::Logger,
        ddmd_client: DdmAdminClient,
        storage_test_harness: StorageManagerTestHarness,
        zone_bundler: ZoneBundler,
        test_config: &'a TestConfig,
    }

    impl<'a> LedgerTestHelper<'a> {
        async fn new(
            log: slog::Logger,
            test_config: &'a TestConfig,
        ) -> LedgerTestHelper {
            let ddmd_client = DdmAdminClient::localhost(&log).unwrap();
            let storage_test_harness = setup_storage(&log).await;
            let zone_bundler = ZoneBundler::new(
                log.clone(),
                storage_test_harness.handle().clone(),
                Default::default(),
            );

            LedgerTestHelper {
                log,
                ddmd_client,
                storage_test_harness,
                zone_bundler,
                test_config,
            }
        }

        async fn cleanup(&mut self) {
            self.storage_test_harness.cleanup().await;
        }

        fn new_service_manager(&self) -> ServiceManager {
            self.new_service_manager_with_timesync(TimeSyncConfig::Skip)
        }

        fn new_service_manager_with_timesync(
            &self,
            time_sync_config: TimeSyncConfig,
        ) -> ServiceManager {
            let log = &self.log;
            let mgr = ServiceManager::new(
                log,
                self.ddmd_client.clone(),
                make_bootstrap_networking_config(),
                SledMode::Auto,
                time_sync_config,
                SidecarRevision::Physical("rev-test".to_string()),
                vec![],
                self.storage_test_harness.handle().clone(),
                self.zone_bundler.clone(),
            );
            self.test_config.override_paths(&mgr);
            mgr
        }

        fn sled_agent_started(
            log: &slog::Logger,
            test_config: &TestConfig,
            mgr: &ServiceManager,
        ) {
            let port_manager = PortManager::new(
                log.new(o!("component" => "PortManager")),
                Ipv6Addr::new(
                    0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
                ),
            );

            mgr.sled_agent_started(
                test_config.make_config(),
                port_manager,
                Ipv6Addr::LOCALHOST,
                Uuid::new_v4(),
                None,
            )
            .unwrap();
        }
    }

    #[tokio::test]
    async fn test_ensure_service() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let v1 = Generation::new();
        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v1);
        assert!(found.zones.is_empty());

        let v2 = v1.next();
        let id = Uuid::new_v4();
        ensure_new_service(
            &mgr,
            id,
            v2,
            String::from(test_config.config_dir.path().as_str()),
        )
        .await;

        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v2);
        assert_eq!(found.zones.len(), 1);
        assert_eq!(found.zones[0].id, id);

        drop_service_manager(mgr);

        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_service_before_timesync() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_ensure_service_before_timesync",
        );
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;

        let mgr =
            helper.new_service_manager_with_timesync(TimeSyncConfig::Fail);
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let v1 = Generation::new();
        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v1);
        assert!(found.zones.is_empty());

        let v2 = v1.next();
        let id = Uuid::new_v4();

        // Should fail: time has not yet synchronized.
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, EXPECTED_PORT, 0, 0);
        let result = try_new_service_of_type(
            &mgr,
            id,
            v2,
            OmicronZoneType::Oximeter { address },
            String::from(test_config.config_dir.path().as_str()),
        )
        .await;

        // First, ensure this is the right kind of error.
        let err = result.unwrap_err();
        let errors = match &err {
            Error::ZoneEnsure { errors } => errors,
            err => panic!("unexpected result: {err:?}"),
        };
        assert_eq!(errors.len(), 1);
        assert_matches::assert_matches!(
            errors[0].1,
            Error::TimeNotSynchronized
        );

        // Next, ensure this still converts to an "unavail" common error
        let common_err = omicron_common::api::external::Error::from(err);
        assert_matches::assert_matches!(
            common_err,
            omicron_common::api::external::Error::ServiceUnavailable { .. }
        );

        // Should succeed: we don't care that time has not yet synchronized (for
        // this specific service).
        try_new_service_of_type(
            &mgr,
            id,
            v2,
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers: vec![],
                dns_servers: vec![],
                domain: None,
            },
            String::from(test_config.config_dir.path().as_str()),
        )
        .await
        .unwrap();

        drop_service_manager(mgr);
        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_service_which_already_exists() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_ensure_service_which_already_exists",
        );
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let v2 = Generation::new().next();
        let id = Uuid::new_v4();
        let dir = String::from(test_config.config_dir.path().as_str());
        ensure_new_service(&mgr, id, v2, dir.clone()).await;
        let v3 = v2.next();
        ensure_existing_service(&mgr, id, v3, dir).await;
        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v3);
        assert_eq!(found.zones.len(), 1);
        assert_eq!(found.zones[0].id, id);

        drop_service_manager(mgr);

        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_services_are_recreated_on_reboot() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_are_recreated_on_reboot",
        );
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;

        // First, spin up a ServiceManager, create a new zone, and then tear
        // down the ServiceManager.
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let v2 = Generation::new().next();
        let id = Uuid::new_v4();
        ensure_new_service(
            &mgr,
            id,
            v2,
            String::from(test_config.config_dir.path().as_str()),
        )
        .await;
        drop_service_manager(mgr);

        // Before we re-create the service manager - notably, using the same
        // config file! - expect that a service gets initialized.
        let _expectations = expect_new_service(EXPECTED_ZONE_NAME_PREFIX);
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);
        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);

        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v2);
        assert_eq!(found.zones.len(), 1);
        assert_eq!(found.zones[0].id, id);

        drop_service_manager(mgr);

        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_services_do_not_persist_without_config() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_services_do_not_persist_without_config",
        );
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;

        // First, spin up a ServiceManager, create a new zone, and then tear
        // down the ServiceManager.
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let v1 = Generation::new();
        let v2 = v1.next();
        let id = Uuid::new_v4();
        ensure_new_service(
            &mgr,
            id,
            v2,
            String::from(test_config.config_dir.path().as_str()),
        )
        .await;
        drop_service_manager(mgr);

        // Next, delete the ledger. This means the zone we just created will not
        // be remembered on the next initialization.
        std::fs::remove_file(
            test_config.config_dir.path().join(ZONES_LEDGER_FILENAME),
        )
        .unwrap();

        // Observe that the old service is not re-initialized.
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v1);
        assert!(found.zones.is_empty());

        drop_service_manager(mgr);

        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bad_generations() {
        // Start like the normal tests.
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_bad_generations");
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        // Like the normal tests, set up a generation with one zone in it.
        let v1 = Generation::new();
        let v2 = v1.next();
        let id1 = Uuid::new_v4();

        let _expectations = expect_new_services();
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, EXPECTED_PORT, 0, 0);
        let mut zones = vec![OmicronZoneConfig {
            id: id1,
            underlay_address: Ipv6Addr::LOCALHOST,
            zone_type: OmicronZoneType::InternalNtp {
                address,
                ntp_servers: vec![],
                dns_servers: vec![],
                domain: None,
            },
        }];

        let tmp_dir = String::from(test_config.config_dir.path().as_str());
        mgr.ensure_all_omicron_zones_persistent(
            OmicronZonesConfig { generation: v2, zones: zones.clone() },
            Some(&tmp_dir),
        )
        .await
        .unwrap();

        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found.generation, v2);
        assert_eq!(found.zones.len(), 1);
        assert_eq!(found.zones[0].id, id1);

        // Make a new list of zones that we're going to try with a bunch of
        // different generation numbers.
        let id2 = Uuid::new_v4();
        zones.push(OmicronZoneConfig {
            id: id2,
            underlay_address: Ipv6Addr::LOCALHOST,
            zone_type: OmicronZoneType::InternalNtp {
                address,
                ntp_servers: vec![],
                dns_servers: vec![],
                domain: None,
            },
        });

        // Now try to apply that list with an older generation number.  This
        // shouldn't work and the reported state should be unchanged.
        let tmp_dir = String::from(test_config.config_dir.path().as_str());
        let error = mgr
            .ensure_all_omicron_zones_persistent(
                OmicronZonesConfig { generation: v1, zones: zones.clone() },
                Some(&tmp_dir),
            )
            .await
            .expect_err("unexpectedly went backwards in zones generation");
        assert!(matches!(
            error,
            Error::RequestedConfigOutdated { requested, current }
            if requested == v1 && current == v2
        ));
        let found2 =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found, found2);

        // Now try to apply that list with the same generation number that we
        // used before.  This shouldn't work either.
        let error = mgr
            .ensure_all_omicron_zones_persistent(
                OmicronZonesConfig { generation: v2, zones: zones.clone() },
                Some(&tmp_dir),
            )
            .await
            .expect_err("unexpectedly changed a single zone generation");
        assert!(matches!(
            error,
            Error::RequestedConfigConflicts(vr) if vr == v2
        ));
        let found3 =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found, found3);

        // But we should be able to apply this new list of zones as long as we
        // advance the generation number.
        let v3 = v2.next();
        mgr.ensure_all_omicron_zones_persistent(
            OmicronZonesConfig { generation: v3, zones: zones.clone() },
            Some(&tmp_dir),
        )
        .await
        .expect("failed to remove all zones in a new generation");
        let found4 =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found4.generation, v3);
        let mut our_zones = zones;
        our_zones.sort_by(|a, b| a.id.cmp(&b.id));
        let mut found_zones = found4.zones;
        found_zones.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(our_zones, found_zones);

        drop_service_manager(mgr);

        illumos_utils::USE_MOCKS.store(false, Ordering::SeqCst);
        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_old_ledger_migration() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_old_ledger_migration",
        );
        let test_config = TestConfig::new().await;

        // Before we start the service manager, stuff one of our old-format
        // service ledgers into place.
        let contents =
            include_str!("../tests/old-service-ledgers/rack2-sled10.json");
        std::fs::write(
            test_config.config_dir.path().join(SERVICES_LEDGER_FILENAME),
            contents,
        )
        .expect("failed to copy example old-format services ledger into place");

        // Now start the service manager.
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        // Trigger the migration code.  (Yes, it's hokey that we create this
        // fake argument.)
        let unused = Mutex::new(BTreeMap::new());
        let migrated_ledger = mgr
            .load_ledgered_zones(&unused.lock().await)
            .await
            .expect("failed to load ledgered zones")
            .unwrap();

        // As a quick check, the migrated ledger should have some zones.
        let migrated_config = migrated_ledger.data();
        assert!(!migrated_config.zones.is_empty());

        // The ServiceManager should now report the migrated zones, meaning that
        // they've been copied into the new-format ledger.
        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found, migrated_config.clone().to_omicron_zones_config());
        // They should both match the expected converted output.
        let expected: OmicronZonesConfigLocal = serde_json::from_str(
            include_str!("../tests/output/new-zones-ledgers/rack2-sled10.json"),
        )
        .unwrap();
        let expected_config = expected.to_omicron_zones_config();
        assert_eq!(found, expected_config);

        // Just to be sure, shut down the manager and create a new one without
        // triggering migration again.  It should also report the same zones.
        drop_service_manager(mgr);

        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        let found =
            mgr.omicron_zones_list().await.expect("failed to list zones");
        assert_eq!(found, expected_config);

        drop_service_manager(mgr);
        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_old_ledger_migration_bad() {
        let logctx = omicron_test_utils::dev::test_setup_log(
            "test_old_ledger_migration_bad",
        );
        let test_config = TestConfig::new().await;
        let mut helper =
            LedgerTestHelper::new(logctx.log.clone(), &test_config).await;

        // Before we start things, stuff a broken ledger into place.  For this
        // to test what we want, it needs to be a valid ledger that we simply
        // failed to convert.
        std::fs::write(
            test_config.config_dir.path().join(SERVICES_LEDGER_FILENAME),
            "{",
        )
        .expect("failed to copy example old-format services ledger into place");

        // Start the service manager.
        let mgr = helper.new_service_manager();
        LedgerTestHelper::sled_agent_started(&logctx.log, &test_config, &mgr);

        // Trigger the migration code.
        let unused = Mutex::new(BTreeMap::new());
        let error = mgr
            .load_ledgered_zones(&unused.lock().await)
            .await
            .expect_err("succeeded in loading bogus ledgered zones");
        assert_eq!(
            "Error migrating old-format services ledger: failed to read or \
            parse old-format ledger, but one exists",
            format!("{:#}", error)
        );

        helper.cleanup().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_bootstrap_addr_to_techport_prefixes() {
        let ba: Ipv6Addr = "fdb0:1122:3344:5566::".parse().unwrap();
        let prefixes = ServiceManager::bootstrap_addr_to_techport_prefixes(&ba);
        assert!(prefixes.iter().all(|p| p.net().width() == 64));
        let prefix0 = prefixes[0].net().prefix();
        let prefix1 = prefixes[1].net().prefix();
        assert_eq!(prefix0.segments()[1..], ba.segments()[1..]);
        assert_eq!(prefix1.segments()[1..], ba.segments()[1..]);
        assert_eq!(prefix0.segments()[0], 0xfdb1);
        assert_eq!(prefix1.segments()[0], 0xfdb2);
    }

    #[test]
    fn test_zone_bundle_metadata_schema() {
        let schema = schemars::schema_for!(ZoneBundleMetadata);
        expectorate::assert_contents(
            "../schema/zone-bundle-metadata.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }

    #[test]
    fn test_all_zones_requests_schema() {
        let schema = schemars::schema_for!(OmicronZonesConfigLocal);
        expectorate::assert_contents(
            "../schema/all-zones-requests.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
