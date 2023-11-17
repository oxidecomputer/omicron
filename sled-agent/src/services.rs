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
use crate::smf_helper::Service;
use crate::smf_helper::SmfHelper;
use crate::storage_manager::StorageResources;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use anyhow::anyhow;
use camino::{Utf8Path, Utf8PathBuf};
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
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
    InstalledZone, RunCommandError, RunningZone,
};
use illumos_utils::zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT;
use illumos_utils::zone::AddressRequest;
use illumos_utils::zone::Zones;
use illumos_utils::{execute, PFEXEC};
use internal_dns::resolver::Resolver;
use itertools::Itertools;
use omicron_common::address::AZ_PREFIX;
use omicron_common::address::BOOTSTRAP_ARTIFACT_PORT;
use omicron_common::address::CLICKHOUSE_KEEPER_PORT;
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::address::COCKROACH_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::CRUCIBLE_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::WICKETD_NEXUS_PROXY_PORT;
use omicron_common::address::WICKETD_PORT;
use omicron_common::address::{Ipv6Subnet, NEXUS_TECHPORT_EXTERNAL_PORT};
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::{
    HostPortConfig, RackNetworkConfig,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, retry_policy_local,
    BackoffError,
};
use omicron_common::ledger::{self, Ledger, Ledgerable};
use omicron_common::nexus_config::{
    self, ConfigDropshotWithTls, DeploymentConfig as NexusDeploymentConfig,
};
use once_cell::sync::OnceCell;
use rand::prelude::SliceRandom;
use sled_hardware::disk::ZONE_DATASET;
use sled_hardware::is_gimlet;
use sled_hardware::underlay;
use sled_hardware::underlay::BOOTSTRAP_PREFIX;
use sled_hardware::Baseboard;
use sled_hardware::SledMode;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::iter;
use std::iter::FromIterator;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, MutexGuard};
use tokio::task::JoinHandle;
use uuid::Uuid;

const IPV6_UNSPECIFIED: IpAddr = IpAddr::V6(Ipv6Addr::UNSPECIFIED);

#[derive(thiserror::Error, Debug)]
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

    #[error("Failed to get address: {0}")]
    GetAddressFailure(#[from] illumos_utils::zone::GetAddressError),

    #[error("NTP zone not ready")]
    NtpZoneNotReady,

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

    #[error("Requested version ({0}) is older than current ({0})")]
    RequestedConfigOutdated(Generation, Generation),

    #[error("Requested version {0} with different zones than before")]
    RequestedConfigConflicts(Generation),

    #[error("Error migrating old-format services ledger")]
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
            err @ Error::RequestedConfigConflicts(_) => {
                omicron_common::api::external::Error::invalid_request(&format!(
                    "{:#}",
                    err
                ))
            }
            err @ Error::RequestedConfigOutdated(_, _) => {
                omicron_common::api::external::Error::conflict(&format!(
                    "{:#}",
                    err
                ))
            }
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
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
const ZONES_LEDGER_FILENAME: &str = "omicron_zones.json";

/// Combines the Nexus-provided `OmicronZonesConfig` (which describes what Nexus
/// wants for all of its zones) with the locally-determined configuration for
/// these zones.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct OmicronZonesConfigLocal {
    /// version of the Omicron-provided part of the configuration
    ///
    /// This version number is outside of Sled Agent's control.  We store
    /// exactly what we were given and use this number to decide when to
    /// fail requests to establish an outdated configuration.
    ///
    /// You can think of this as a major version number, with `ledger_version`
    /// being a minor version number.  See `is_newer_than()`.
    pub omicron_version: Generation,

    /// ledger-managed version
    ///
    /// This version is managed by the ledger facility itself.  It's bumped
    /// whenever we write a new ledger.  In practice, we don't currently have
    /// any reason to bump this _for a given Omicron version_ so it's somewhat
    /// redundant.  In principle, if we needed to modify the ledgered
    /// configuration due to some event that doesn't change the Omicron config
    /// (e.g., if we wanted to move the root filesystem to a different path), we
    /// could do that by bumping this version.
    pub ledger_version: Generation,
    pub zones: Vec<OmicronZoneConfigLocal>,
}

impl Ledgerable for OmicronZonesConfigLocal {
    fn is_newer_than(&self, other: &OmicronZonesConfigLocal) -> bool {
        self.omicron_version > other.omicron_version
            || (self.omicron_version == other.omicron_version
                && self.ledger_version >= other.ledger_version)
    }

    fn generation_bump(&mut self) {
        self.ledger_version = self.ledger_version.next();
    }
}

impl OmicronZonesConfigLocal {
    pub fn to_omicron_zones_config(self) -> OmicronZonesConfig {
        OmicronZonesConfig {
            version: self.omicron_version,
            zones: self.zones.into_iter().map(|z| z.zone).collect(),
        }
    }
}

/// Combines the Nexus-provided `OmicronZoneConfig` (which describes what Nexus
/// wants for this zone) with any locally-determined configuration (like the
/// path to the root filesystem)
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
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
    services: Vec<ServiceType>,
}

/// Describes one of several services that may be deployed in a switch zone
///
/// Some of these are only present in certain configurations (e.g., with a real
/// Tofino vs. SoftNPU) or are configured differently depending on the
/// configuration.
// XXX-dap towards the end, rename this to SwitchService.  Saving it for later
// to avoid having to deal with it more than necessary in merge conflicts.
#[derive(Clone)]
enum ServiceType {
    ManagementGatewayService,
    Wicketd { baseboard: Baseboard },
    Dendrite { asic: DendriteAsic },
    Tfport { pkt_source: String, asic: DendriteAsic },
    Uplink,
    MgDdm { mode: String },
    Mgd,
    SpSim,
}

impl crate::smf_helper::Service for ServiceType {
    fn service_name(&self) -> String {
        match self {
            ServiceType::ManagementGatewayService => "mgs",
            ServiceType::Wicketd { .. } => "wicketd",
            ServiceType::Dendrite { .. } => "dendrite",
            ServiceType::Tfport { .. } => "tfport",
            ServiceType::Uplink { .. } => "uplink",
            ServiceType::MgDdm { .. } => "mg-ddm",
            ServiceType::Mgd => "mgd",
            ServiceType::SpSim => "sp-sim",
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
    SledLocal(&'a SwitchZoneConfigLocal),
}

impl<'a> ZoneArgs<'a> {
    /// If this is an Omicron zone, return its type
    pub fn omicron_type(&self) -> Option<&'a OmicronZoneType> {
        match self {
            ZoneArgs::Omicron(zone_config) => Some(&zone_config.zone.zone_type),
            ZoneArgs::SledLocal(_) => None,
        }
    }

    /// If this is a sled-local (switch) zone, iterate over the services it's
    /// supposed to be running
    pub fn sled_local_services(
        &self,
    ) -> Box<dyn Iterator<Item = &'a ServiceType> + 'a> {
        match self {
            ZoneArgs::Omicron(_) => Box::new(std::iter::empty()),
            ZoneArgs::SledLocal(request) => {
                Box::new(request.zone.services.iter())
            }
        }
    }

    /// Return the root filesystem path for this zone
    pub fn root(&self) -> &Utf8Path {
        match self {
            ZoneArgs::Omicron(zone_config) => &zone_config.root,
            ZoneArgs::SledLocal(zone_request) => &zone_request.root,
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

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    global_zone_bootstrap_link_local_address: Ipv6Addr,
    switch_zone: Mutex<SledLocalZone>,
    sled_mode: SledMode,
    skip_timesync: Option<bool>,
    time_synced: AtomicBool,
    switch_zone_maghemite_links: Vec<PhysicalLink>,
    sidecar_revision: SidecarRevision,
    // Zones representing running services
    zones: Mutex<BTreeMap<String, RunningZone>>,
    underlay_vnic_allocator: VnicAllocator<Etherstub>,
    underlay_vnic: EtherstubVnic,
    bootstrap_vnic_allocator: VnicAllocator<Etherstub>,
    ddmd_client: DdmAdminClient,
    advertised_prefixes: Mutex<HashSet<Ipv6Subnet<SLED_PREFIX>>>,
    sled_info: OnceCell<SledAgentInfo>,
    switch_zone_bootstrap_address: Ipv6Addr,
    storage: StorageResources,
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
    /// - `skip_timesync`: If true, the sled always reports synced time.
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
        skip_timesync: Option<bool>,
        sidecar_revision: SidecarRevision,
        switch_zone_maghemite_links: Vec<PhysicalLink>,
        storage: StorageResources,
        zone_bundler: ZoneBundler,
    ) -> Self {
        let log = log.new(o!("component" => "ServiceManager"));
        Self {
            inner: Arc::new(ServiceManagerInner {
                log: log.clone(),
                global_zone_bootstrap_link_local_address: bootstrap_networking
                    .global_zone_bootstrap_link_local_ip,
                // TODO(https://github.com/oxidecomputer/omicron/issues/725):
                // Load the switch zone if it already exists?
                switch_zone: Mutex::new(SledLocalZone::Disabled),
                sled_mode,
                skip_timesync,
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

    #[cfg(test)]
    fn override_ledger_directory(&self, path: Utf8PathBuf) {
        self.inner.ledger_directory_override.set(path).unwrap();
    }

    #[cfg(test)]
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
        self.inner
            .storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(SERVICES_LEDGER_FILENAME))
            .collect()
    }

    async fn all_omicron_zone_ledgers(&self) -> Vec<Utf8PathBuf> {
        if let Some(dir) = self.inner.ledger_directory_override.get() {
            return vec![dir.join(ZONES_LEDGER_FILENAME)];
        }
        self.inner
            .storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
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
        _map: &MutexGuard<'_, BTreeMap<String, RunningZone>>,
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

        let Some(ledger) =
            Ledger::<AllZoneRequests>::new(log, services_ledger_paths).await
        else {
            return Ok(None);
        };

        let all_services = ledger.into_inner();
        match OmicronZonesConfigLocal::try_from(all_services) {
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
                return Err(Error::ServicesMigration(error));
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
    pub async fn load_services(&self) -> Result<(), Error> {
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
            return Ok(());
        };

        let zones_config = ledger.data_mut();
        info!(
            log,
            "Loaded Omicron zones";
            "zones_config" => ?zones_config
        );
        let omicron_zones_config =
            zones_config.clone().to_omicron_zones_config();

        // Initialize internal DNS only first: we need it to look up the
        // boundary switch addresses. This dependency is implicit: when we call
        // `ensure_all_omicron_zones` below, we eventually land in
        // `opte_ports_needed()`, which for some service types (including Ntp
        // but _not_ including InternalDns), we perform internal DNS lookups.
        let all_zones_request = self
            .ensure_all_omicron_zones(
                &mut existing_zones,
                None,
                omicron_zones_config.clone(),
                |z: &OmicronZoneConfig| {
                    matches!(
                        z.zone_type,
                        OmicronZoneType::InternalDns { .. }
                            | OmicronZoneType::BoundaryNtp { .. }
                            | OmicronZoneType::InternalNtp { .. }
                    )
                },
            )
            .await?;

        // Initialize NTP services next as they are required for time
        // synchronization, which is a pre-requisite for the other services. We
        // keep `OmicronZoneType::InternalDns` because
        // `ensure_all_omicron_zones` is additive.
        // TODO This looks like a duplicate of the block above -- why do we do
        // this?
        let all_zones_request = self
            .ensure_all_omicron_zones(
                &mut existing_zones,
                Some(&all_zones_request),
                omicron_zones_config.clone(),
                |z: &OmicronZoneConfig| {
                    matches!(
                        z.zone_type,
                        OmicronZoneType::InternalDns { .. }
                            | OmicronZoneType::BoundaryNtp { .. }
                            | OmicronZoneType::InternalNtp { .. }
                    )
                },
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

        // Initialize all remaining services
        self.ensure_all_omicron_zones(
            &mut existing_zones,
            Some(&all_zones_request),
            omicron_zones_config,
            |_| true,
        )
        .await?;
        Ok(())
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
                ServiceType::Dendrite {
                    asic: DendriteAsic::SoftNpuPropolisDevice,
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
        if let ZoneArgs::SledLocal(_) = zone_args {
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
                ServiceType::Tfport { pkt_source, asic: _ } => {
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
                            if is_gimlet {
                                return Err(Error::MissingDevice {
                                    device: pkt_source.to_string(),
                                });
                            }
                        }
                    }
                }
                ServiceType::MgDdm { .. } => {
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
    // OPTE ports need to be created and mapped into the zone when it is created.
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
        let (zone_type_str, nic, snat, external_ips) = match &zone_args
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
            _ => panic!("unexpected zone type"),
        };

        // Create the OPTE port for the service.
        // Note we don't plumb any firewall rules at this point,
        // Nexus will plumb them down later but the default OPTE
        // config allows outbound access which is enough for
        // Boundary NTP which needs to come up before Nexus.
        let port = port_manager
            .create_port(nic, snat, external_ips, &[], DhcpCfg::default())
            .map_err(|err| Error::ServicePortCreation {
                service: zone_type_str.clone(),
                err: Box::new(err),
            })?;

        // We also need to update the switch with the NAT mappings
        let (target_ip, first_port, last_port) = match snat {
            Some(s) => (s.ip, s.first_port, s.last_port),
            None => (external_ips[0], 0, u16::MAX),
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
        let mut needed = Vec::new();
        for svc_details in zone_args.sled_local_services() {
            match svc_details {
                ServiceType::Tfport { .. } => {
                    needed.push("default".to_string());
                    needed.push("sys_dl_config".to_string());
                }
                _ => (),
            }
        }

        if let Some(omicron_zone_type) = zone_args.omicron_type() {
            match omicron_zone_type {
                OmicronZoneType::BoundaryNtp { .. }
                | OmicronZoneType::InternalNtp { .. } => {
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
        dns_servers: &[IpAddr],
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

    async fn dns_install(
        info: &SledAgentInfo,
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
        let all_nameservers = info
            .resolver
            .lookup_all_ipv6(internal_dns::ServiceName::InternalDns)
            .await?;
        let mut dns_config_builder = PropertyGroupBuilder::new("install_props");
        for ns_addr in &all_nameservers {
            dns_config_builder = dns_config_builder.add_property(
                "nameserver",
                "net_address",
                &ns_addr.to_string(),
            );
        }
        Ok(ServiceBuilder::new("network/dns/install")
            .add_property_group(dns_config_builder)
            // We do need to enable the default instance of the
            // dns/install service.  It's enough to just mention it
            // here, as the ServiceInstanceBuilder always enables the
            // instance being added.
            .add_instance(ServiceInstanceBuilder::new("default")))
    }

    async fn initialize_zone(
        &self,
        request: ZoneArgs<'_>,
        filesystems: &[zone::Fs],
        data_links: &[String],
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
            ZoneArgs::SledLocal(_) => None,
        };
        let datasets: Vec<_> = match &request {
            ZoneArgs::Omicron(zone_config) => zone_config
                .zone
                .dataset_name()
                .map(|n| zone::Dataset { name: n.full() })
                .into_iter()
                .collect(),
            ZoneArgs::SledLocal(_) => vec![],
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
        if let Some((_, boot_zpool)) = self.inner.storage.boot_disk().await {
            zone_image_paths.push(
                boot_zpool
                    .dataset_mountpoint(sled_hardware::disk::INSTALL_DATASET),
            );
        }

        let zone_type_str = match &request {
            ZoneArgs::Omicron(zone_config) => {
                zone_config.zone.zone_type.zone_type_str()
            }
            ZoneArgs::SledLocal(_) => "switch".to_string(),
        };
        let installed_zone = InstalledZone::install(
            &self.inner.log,
            &self.inner.underlay_vnic_allocator,
            &request.root(),
            zone_image_paths.as_slice(),
            &zone_type_str,
            unique_name,
            datasets.as_slice(),
            &filesystems,
            &data_links,
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

                let dns_service = Self::dns_install(info).await?;

                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                let listen_addr = &underlay_address.to_string();
                let listen_port = &CLICKHOUSE_PORT.to_string();

                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");
                let clickhouse_service =
                    ServiceBuilder::new("oxide/clickhouse").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(clickhouse_service)
                    .add_service(dns_service);
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

                let dns_service = Self::dns_install(info).await?;

                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                let listen_addr = &underlay_address.to_string();
                let listen_port = &CLICKHOUSE_KEEPER_PORT.to_string();

                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");
                let clickhouse_keeper_service =
                    ServiceBuilder::new("oxide/clickhouse_keeper")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(config),
                        );
                let profile = ProfileBuilder::new("omicron")
                    .add_service(clickhouse_keeper_service)
                    .add_service(dns_service);
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
                        zone_type: OmicronZoneType::CockroachDb { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let dns_service = Self::dns_install(info).await?;

                // Configure the CockroachDB service.
                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                let address = SocketAddr::new(
                    IpAddr::V6(*underlay_address),
                    COCKROACH_PORT,
                );
                let listen_addr = &address.ip().to_string();
                let listen_port = &address.port().to_string();

                let cockroachdb_config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("listen_addr", "astring", listen_addr)
                    .add_property("listen_port", "astring", listen_port)
                    .add_property("store", "astring", "/data");
                let cockroachdb_service =
                    ServiceBuilder::new("oxide/cockroachdb").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(cockroachdb_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(cockroachdb_service)
                    .add_service(dns_service);
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
                    zone @ OmicronZoneConfig {
                        zone_type: OmicronZoneType::Crucible { .. },
                        underlay_address,
                        ..
                    },
                ..
            }) => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };
                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                let listen_addr = &underlay_address.to_string();
                let listen_port = &CRUCIBLE_PORT.to_string();

                let dataset_name = zone.dataset_name().unwrap().full();
                let uuid = &Uuid::new_v4().to_string();
                let config = PropertyGroupBuilder::new("config")
                    .add_property("datalink", "astring", datalink)
                    .add_property("gateway", "astring", gateway)
                    .add_property("dataset", "astring", &dataset_name)
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

                let datalink = installed_zone.get_control_vnic_name();
                let gateway = &info.underlay_address.to_string();
                let listen_addr = &underlay_address.to_string();
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
            }) => vec![*underlay_address],
            ZoneArgs::SledLocal(req) => req.zone.addresses.clone(),
        };
        for addr in &addresses {
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

        if let Some(gateway) = maybe_gateway {
            running_zone.add_default_route(gateway).map_err(|err| {
                Error::ZoneCommand { intent: "Adding Route".to_string(), err }
            })?;
        }

        match &request {
            ZoneArgs::Omicron(zone_config) => {
                // TODO: Related to
                // https://github.com/oxidecomputer/omicron/pull/1124 , should we
                // avoid importing this manifest?
                debug!(self.inner.log, "importing manifest");

                let smfh =
                    SmfHelper::new(&running_zone, &zone_config.zone.zone_type);
                smfh.import_manifest()?;

                match &zone_config.zone.zone_type {
                    OmicronZoneType::Nexus {
                        internal_address,
                        external_tls,
                        external_dns_servers,
                        ..
                    } => {
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
                        let nexus_port = if *external_tls { 443 } else { 80 };
                        let deployment_config = NexusDeploymentConfig {
                            id: zone_config.zone.id,
                            rack_id: sled_info.rack_id,
                            techport_external_server_port:
                                NEXUS_TECHPORT_EXTERNAL_PORT,

                            dropshot_external: ConfigDropshotWithTls {
                                tls: *external_tls,
                                dropshot: dropshot::ConfigDropshot {
                                    bind_address: SocketAddr::new(
                                        port_ip, nexus_port,
                                    ),
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
                                // certificates provided by the customer during rack
                                // setup.
                                request_body_max_bytes: 10 * 1024 * 1024,
                                default_handler_task_mode:
                                    HandlerTaskMode::Detached,
                            },
                            internal_dns:
                                nexus_config::InternalDns::FromSubnet {
                                    subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                                        sled_info.underlay_address,
                                    ),
                                },
                            database: nexus_config::Database::FromDns,
                            external_dns_servers: external_dns_servers.clone(),
                        };

                        // Copy the partial config file to the expected location.
                        let config_dir = Utf8PathBuf::from(format!(
                            "{}/var/svc/manifest/site/nexus",
                            running_zone.root()
                        ));
                        // The filename of a half-completed config, in need of parameters supplied at
                        // runtime.
                        const PARTIAL_LEDGER_FILENAME: &str =
                            "config-partial.toml";
                        // The filename of a completed config, merging the partial config with
                        // additional appended parameters known at runtime.
                        const COMPLETE_LEDGER_FILENAME: &str = "config.toml";
                        let partial_config_path =
                            config_dir.join(PARTIAL_LEDGER_FILENAME);
                        let config_path =
                            config_dir.join(COMPLETE_LEDGER_FILENAME);
                        tokio::fs::copy(partial_config_path, &config_path)
                            .await
                            .map_err(|err| Error::io_path(&config_path, err))?;

                        // Serialize the configuration and append it into the file.
                        let serialized_cfg =
                            toml::Value::try_from(&deployment_config)
                                .expect("Cannot serialize config");
                        let mut map = toml::map::Map::new();
                        map.insert("deployment".to_string(), serialized_cfg);
                        let config_str =
                            toml::to_string(&map).map_err(|err| {
                                Error::TomlSerialize {
                                    path: config_path.clone(),
                                    err,
                                }
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
                    }

                    OmicronZoneType::ExternalDns {
                        http_address,
                        dns_address,
                        ..
                    } => {
                        info!(
                            self.inner.log,
                            "Setting up external-dns service"
                        );

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

                    OmicronZoneType::InternalDns {
                        http_address,
                        dns_address,
                        gz_address,
                        gz_address_index,
                        ..
                    } => {
                        info!(
                            self.inner.log,
                            "Setting up internal-dns service"
                        );

                        // Internal DNS zones require a special route through the
                        // global zone, since they are not on the same part of the
                        // underlay as most other services on this sled (the sled's
                        // subnet).
                        //
                        // We create an IP address in the dedicated portion of the
                        // underlay used for internal DNS servers, but we *also*
                        // add a number ("which DNS server is this") to ensure
                        // these addresses are given unique names. In the unlikely
                        // case that two internal DNS servers end up on the same
                        // machine (which is effectively a developer-only
                        // environment -- we wouldn't want this in prod!), they need
                        // to be given distinct names.
                        let addr_name =
                            format!("internaldns{gz_address_index}");
                        Zones::ensure_has_global_zone_v6_address(
                            self.inner.underlay_vnic.clone(),
                            *gz_address,
                            &addr_name,
                        )
                        .map_err(|err| {
                            Error::GzAddress {
                                message: format!(
                            "Failed to create address {} for Internal DNS zone",
                            addr_name
                        ),
                                err,
                            }
                        })?;
                        // If this address is in a new ipv6 prefix, notify maghemite so
                        // it can advertise it to other sleds.
                        self.advertise_prefix_of_address(*gz_address).await;

                        running_zone.add_default_route(*gz_address).map_err(
                            |err| Error::ZoneCommand {
                                intent: "Adding Route".to_string(),
                                err,
                            },
                        )?;

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

                    OmicronZoneType::Oximeter { address } => {
                        info!(self.inner.log, "Setting up oximeter service");
                        smfh.setprop("config/id", zone_config.zone.id)?;
                        smfh.setprop("config/address", address.to_string())?;
                        smfh.refresh()?;
                    }

                    OmicronZoneType::BoundaryNtp {
                        ntp_servers,
                        dns_servers,
                        domain,
                        ..
                    }
                    | OmicronZoneType::InternalNtp {
                        ntp_servers,
                        dns_servers,
                        domain,
                        ..
                    } => {
                        let boundary = matches!(
                            &zone_config.zone.zone_type,
                            OmicronZoneType::BoundaryNtp { .. }
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
                    OmicronZoneType::Clickhouse { .. }
                    | OmicronZoneType::ClickhouseKeeper { .. }
                    | OmicronZoneType::CockroachDb { .. }
                    | OmicronZoneType::Crucible { .. }
                    | OmicronZoneType::CruciblePantry { .. } => {
                        panic!(
                            "{} is a service which exists as part of a \
                            self-assembling zone",
                            &zone_config.zone.zone_type.zone_type_str(),
                        )
                    }
                };

                debug!(self.inner.log, "enabling service");
                smfh.enable()?;
            }
            ZoneArgs::SledLocal(request) => {
                for service in &request.zone.services {
                    // TODO: Related to
                    // https://github.com/oxidecomputer/omicron/pull/1124 , should we
                    // avoid importing this manifest?
                    debug!(self.inner.log, "importing manifest");

                    let smfh = SmfHelper::new(&running_zone, service);
                    smfh.import_manifest()?;

                    match service {
                        ServiceType::ManagementGatewayService => {
                            info!(self.inner.log, "Setting up MGS service");
                            smfh.setprop("config/id", request.zone.id)?;

                            // Always tell MGS to listen on localhost so wicketd can
                            // contact it even before we have an underlay network.
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
                        ServiceType::SpSim => {
                            info!(
                                self.inner.log,
                                "Setting up Simulated SP service"
                            );
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

                            // We intentionally bind `nexus-proxy-address` to `::` so
                            // wicketd will serve this on all interfaces, particularly
                            // the tech port interfaces, allowing external clients to
                            // connect to this Nexus proxy.
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
                                    &rack_subnet.net().ip().to_string(),
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
                        ServiceType::Dendrite { asic } => {
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
                            let sidecar_revision =
                                match self.inner.sidecar_revision {
                                    SidecarRevision::Physical(ref rev) => rev,
                                    _ => {
                                        return Err(Error::SidecarRevision(
                                            anyhow::anyhow!(
                                            "expected physical sidecar revision"
                                        ),
                                        ))
                                    }
                                };
                            smfh.setprop("config/board_rev", sidecar_revision)?;
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
                                            "expected soft sidecar revision"
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
                        ServiceType::Tfport { pkt_source, asic } => {
                            info!(self.inner.log, "Setting up tfport service");

                            let is_gimlet = is_gimlet().map_err(|e| {
                                Error::Underlay(
                                    underlay::Error::SystemDetection(e),
                                )
                            })?;

                            if is_gimlet {
                                // Collect the prefixes for each techport.
                                let techport_prefixes = match bootstrap_name_and_address
                            .as_ref()
                        {
                            Some((_, addr)) => {
                                Self::bootstrap_addr_to_techport_prefixes(addr)
                            }
                            None => {
                                return Err(Error::BadServiceRequest {
                                    service: "tfport".into(),
                                    message: "bootstrap addr missing".into(),
                                });
                            }
                        };

                                for (i, prefix) in
                                    techport_prefixes.into_iter().enumerate()
                                {
                                    // Each `prefix` is an `Ipv6Subnet` including a netmask.
                                    // Stringify just the network address, without the mask.
                                    smfh.setprop(
                                        format!("config/techport{i}_prefix"),
                                        prefix.net().network().to_string(),
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
                        ServiceType::Uplink => {
                            // Nothing to do here - this service is special and
                            // configured in `ensure_switch_zone_uplinks_configured`
                        }
                        ServiceType::Mgd => {
                            info!(self.inner.log, "Setting up mgd service");
                            smfh.setprop("config/admin_host", "::")?;
                            smfh.refresh()?;
                        }
                        ServiceType::MgDdm { mode } => {
                            info!(self.inner.log, "Setting up mg-ddm service");

                            smfh.setprop("config/mode", &mode)?;
                            smfh.setprop("config/admin_host", "::")?;

                            let is_gimlet = is_gimlet().map_err(|e| {
                                Error::Underlay(
                                    underlay::Error::SystemDetection(e),
                                )
                            })?;

                            let maghemite_interfaces: Vec<AddrObject> =
                                if is_gimlet {
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
                                // `svccfg setprop` requires a list of values to be
                                // enclosed in `()`, and each string value to be
                                // enclosed in `""`. Note that we do _not_ need to
                                // escape the parentheses, since this is not passed
                                // through a shell, but directly to `exec(2)` in the
                                // zone.
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
                                // Ddm for a scrimlet needs to be configured to talk to
                                // dendrite
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

    // Populates `existing_zones` according to the requests in `services`.
    async fn initialize_omicron_zones_locked(
        &self,
        existing_zones: &mut BTreeMap<String, RunningZone>,
        requests: &Vec<OmicronZoneConfigLocal>,
    ) -> Result<(), Error> {
        if let Some(name) = requests
            .iter()
            .map(|request| request.zone.zone_name())
            .duplicates()
            .next()
        {
            return Err(Error::BadServiceRequest {
                service: name,
                message: "Should not initialize zone twice".to_string(),
            });
        }

        let futures = requests.iter().map(|request| {
            async move {
                self.initialize_zone(
                    ZoneArgs::Omicron(request),
                    // filesystems=
                    &[],
                    // data_links=
                    &[],
                )
                .await
                .map_err(|error| (request.zone.zone_name(), error))
            }
        });
        let results = futures::future::join_all(futures).await;

        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(zone) => {
                    existing_zones.insert(zone.name().to_string(), zone);
                }
                Err((zone_name, error)) => {
                    errors.push((zone_name, Box::new(error)));
                }
            }
        }

        if !errors.is_empty() {
            return Err(Error::ZoneInitialize(errors));
        }

        Ok(())
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
                .create(zone, ZoneBundleCause::ExplicitRequest)
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
        let ledger = match Ledger::<OmicronZonesConfigLocal>::new(
            log,
            zone_ledger_paths.clone(),
        )
        .await
        {
            Some(ledger) => ledger,
            None => Ledger::<OmicronZonesConfigLocal>::new_with(
                log,
                zone_ledger_paths.clone(),
                OmicronZonesConfigLocal {
                    omicron_version: Generation::new(),
                    ledger_version: Generation::new(),
                    zones: vec![],
                },
            ),
        };

        Ok(ledger.data().clone().to_omicron_zones_config())
    }

    /// Ensures that particular Omicron zones are running
    ///
    /// These services will be instantiated by this function, and will be
    /// recorded to a local file to ensure they start automatically on next
    /// boot.
    pub async fn ensure_all_omicron_zones_persistent(
        &self,
        request: OmicronZonesConfig,
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
                OmicronZonesConfigLocal {
                    omicron_version: Generation::new(),
                    ledger_version: Generation::new(),
                    zones: vec![],
                },
            ),
        };

        let ledger_zone_config = ledger.data_mut();
        debug!(log, "ensure_all_omicron_zones_persistent";
            "request_version" => request.version.to_string(),
            "ledger_version" => ledger_zone_config.omicron_version.to_string(),
        );

        // Absolutely refuse to downgrade the configuration.
        if ledger_zone_config.omicron_version > request.version {
            return Err(Error::RequestedConfigOutdated(
                request.version,
                ledger_zone_config.omicron_version,
            ));
        }

        // If the version is the same as what we're running, but the contents
        // aren't, that's a problem, too.
        if ledger_zone_config.omicron_version == request.version
            && ledger_zone_config.clone().to_omicron_zones_config().zones
                != request.zones
        {
            return Err(Error::RequestedConfigConflicts(request.version));
        }

        let new_config = self
            .ensure_all_omicron_zones(
                &mut existing_zones,
                Some(ledger_zone_config),
                request,
                |_| true,
            )
            .await?;

        // Update the zones in the ledger and write it back to both M.2s
        *ledger_zone_config = new_config;
        ledger.commit().await?;

        Ok(())
    }

    // Ensures that only the following Omicron zones are running.
    //
    // Does not record any information such that these services are
    // re-instantiated on boot.
    async fn ensure_all_omicron_zones<F>(
        &self,
        // The MutexGuard here attempts to ensure that the caller has the right
        // lock held when calling this function.
        existing_zones: &mut MutexGuard<'_, BTreeMap<String, RunningZone>>,
        old_config: Option<&OmicronZonesConfigLocal>,
        new_request: OmicronZonesConfig,
        filter: F,
    ) -> Result<OmicronZonesConfigLocal, Error>
    where
        F: Fn(&OmicronZoneConfig) -> bool,
    {
        let log = &self.inner.log;

        // Do some data-normalization to ensure we can compare the "requested
        // set" vs the "existing set" as HashSets.
        let old_zones_set: HashSet<OmicronZoneConfig> = old_config
            .map(|old_config| {
                HashSet::from_iter(
                    old_config.zones.iter().map(|z| z.zone.clone()),
                )
            })
            .unwrap_or_else(HashSet::new);
        let requested_zones_set =
            HashSet::from_iter(new_request.zones.into_iter().filter(filter));

        let zones_to_be_removed =
            old_zones_set.difference(&requested_zones_set);
        let zones_to_be_added = requested_zones_set.difference(&old_zones_set);

        // Destroy zones that should not be running
        for zone in zones_to_be_removed {
            let expected_zone_name = zone.zone_name();
            if let Some(mut zone) = existing_zones.remove(&expected_zone_name) {
                debug!(
                    log,
                    "removing an existing zone";
                    "zone_name" => &expected_zone_name,
                );
                if let Err(e) = self
                    .inner
                    .zone_bundler
                    .create(&zone, ZoneBundleCause::UnexpectedZone)
                    .await
                {
                    error!(
                        log,
                        "Failed to take bundle of unexpected zone";
                        "zone_name" => &expected_zone_name,
                        "reason" => ?e,
                    );
                }
                if let Err(e) = zone.stop().await {
                    error!(log, "Failed to stop zone {}: {e}", zone.name());
                }
            } else {
                warn!(log, "Expected to remove zone, but could not find it");
            }
        }

        // Create zones that should be running
        let all_u2_roots =
            self.inner.storage.all_u2_mountpoints(ZONE_DATASET).await;
        let mut new_zones = Vec::new();
        for zone in zones_to_be_added {
            // Check if we think the zone should already be running
            let name = zone.zone_name();
            if existing_zones.contains_key(&name) {
                // Make sure the zone actually exists in the right state too
                match Zones::find(&name).await {
                    Ok(Some(zone)) if zone.state() == zone::State::Running => {
                        info!(log, "skipping running zone"; "zone" => &name);
                        continue;
                    }
                    _ => {
                        // Mismatch between SA's view and reality, let's try to
                        // clean up any remanants and try initialize it again
                        warn!(
                            log,
                            "expected to find existing zone in running state";
                            "zone" => &name,
                        );
                        if let Err(e) =
                            existing_zones.remove(&name).unwrap().stop().await
                        {
                            error!(
                                log,
                                "Failed to stop zone";
                                "zone" => &name,
                                "error" => %e,
                            );
                        }
                    }
                }
            }

            // For each new zone request, we pick an arbitrary U.2 to store
            // the zone filesystem. Note: This isn't known to Nexus right now,
            // so it's a local-to-sled decision.
            //
            // This is (currently) intentional, as the zone filesystem should
            // be destroyed between reboots.
            let mut rng = rand::thread_rng();
            let root = all_u2_roots
                .choose(&mut rng)
                .ok_or_else(|| Error::U2NotFound)?
                .clone();

            new_zones.push(OmicronZoneConfigLocal { zone: zone.clone(), root });
        }

        self.initialize_omicron_zones_locked(existing_zones, &new_zones)
            .await?;

        if let Some(old_config) = old_config {
            for old_zone in &old_config.zones {
                if requested_zones_set.contains(&old_zone.zone) {
                    new_zones.push(old_zone.clone());
                }
            }
        }

        Ok(OmicronZonesConfigLocal {
            omicron_version: new_request.version,
            ledger_version: old_config
                .map(|c| c.ledger_version)
                .unwrap_or_else(Generation::new),
            zones: new_zones,
        })
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
                    &zone.control_interface(),
                )?
                .ip();
                let host = &format!("[{address}]:{COCKROACH_PORT}");
                info!(
                    log,
                    "Initializing CRDB Cluster - sending request to {host}"
                );
                if let Err(err) = zone.run_cmd(&[
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
                zone.run_cmd(&[
                    "/opt/oxide/cockroachdb/bin/cockroach",
                    "sql",
                    "--insecure",
                    "--host",
                    host,
                    "--file",
                    "/opt/oxide/cockroachdb/sql/dbwipe.sql",
                ])
                .map_err(|err| Error::CockroachInit { err })?;
                zone.run_cmd(&[
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

    pub fn boottime_rewrite<'a>(
        &self,
        zones: impl Iterator<Item = &'a RunningZone>,
    ) {
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
            self.boottime_rewrite(existing_zones.values());
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

        match ntp_zone.run_cmd(&["/usr/bin/chronyc", "-c", "tracking"]) {
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
                        self.boottime_rewrite(existing_zones.values());
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
                    ServiceType::Tfport {
                        pkt_source: "tfpkt0".to_string(),
                        asic: DendriteAsic::TofinoAsic,
                    },
                    ServiceType::Uplink,
                    ServiceType::Wicketd { baseboard },
                    ServiceType::Mgd,
                    ServiceType::MgDdm { mode: "transit".to_string() },
                ]
            }

            SledMode::Scrimlet {
                asic: asic @ DendriteAsic::SoftNpuPropolisDevice,
            } => {
                data_links = vec!["vioif0".to_owned()];
                vec![
                    ServiceType::Dendrite { asic },
                    ServiceType::ManagementGatewayService,
                    ServiceType::Uplink,
                    ServiceType::Wicketd { baseboard },
                    ServiceType::Mgd,
                    ServiceType::MgDdm { mode: "transit".to_string() },
                    ServiceType::Tfport {
                        pkt_source: "vioif0".to_string(),
                        asic,
                    },
                    ServiceType::SpSim,
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
                    ServiceType::Dendrite { asic },
                    ServiceType::ManagementGatewayService,
                    ServiceType::Uplink,
                    ServiceType::Wicketd { baseboard },
                    ServiceType::Mgd,
                    ServiceType::MgDdm { mode: "transit".to_string() },
                    ServiceType::Tfport {
                        pkt_source: "tfpkt0".to_string(),
                        asic,
                    },
                    ServiceType::SpSim,
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

        let smfh = SmfHelper::new(&zone, &ServiceType::Uplink);

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
                            smfh.addpropvalue(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                            )?;

                            // It should be impossible for the `sled_info` not to be set here,
                            // as the underlay is set at the same time.
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
                        ServiceType::Wicketd { .. } => {
                            if let Some(&address) = first_address {
                                let rack_subnet =
                                    Ipv6Subnet::<AZ_PREFIX>::new(address);

                                info!(
                                    self.inner.log, "configuring wicketd";
                                    "rack_subnet" => %rack_subnet.net().ip(),
                                );

                                smfh.setprop(
                                    "config/rack-subnet",
                                    &rack_subnet.net().ip().to_string(),
                                )?;

                                smfh.refresh()?;
                            } else {
                                error!(
                                    self.inner.log,
                                    "underlay address unexpectedly missing",
                                );
                            }
                        }
                        ServiceType::Tfport { .. } => {
                            // Since tfport and dpd communicate using localhost,
                            // the tfport service shouldn't need to be restarted.
                        }
                        ServiceType::Uplink { .. } => {
                            // Only configured in
                            // `ensure_switch_zone_uplinks_configured`
                        }
                        ServiceType::MgDdm { mode } => {
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
        let zone_args = ZoneArgs::SledLocal(&zone_request);
        let zone =
            self.initialize_zone(zone_args, filesystems, data_links).await?;
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

#[cfg(test)]
mod test {
    use super::*;
    use async_trait::async_trait;
    use illumos_utils::{
        dladm::{
            Etherstub, MockDladm, BOOTSTRAP_ETHERSTUB_NAME,
            UNDERLAY_ETHERSTUB_NAME, UNDERLAY_ETHERSTUB_VNIC_NAME,
        },
        svc,
        zone::MockZones,
    };
    use key_manager::{
        SecretRetriever, SecretRetrieverError, SecretState, VersionedIkm,
    };
    use omicron_common::address::OXIMETER_PORT;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::os::unix::process::ExitStatusExt;
    use uuid::Uuid;

    // Just placeholders. Not used.
    const GLOBAL_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;
    const SWITCH_ZONE_BOOTSTRAP_IP: Ipv6Addr = Ipv6Addr::LOCALHOST;

    const EXPECTED_ZONE_NAME_PREFIX: &str = "oxz_oximeter";

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
    fn expect_new_service() -> Vec<Box<dyn std::any::Any>> {
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
        install_ctx.expect().return_once(|_, _, name, _, _, _, _, _, _| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
            Ok(())
        });

        // Boot the zone.
        let boot_ctx = MockZones::boot_context();
        boot_ctx.expect().return_once(|name| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
            Ok(())
        });

        // After calling `MockZones::boot`, `RunningZone::boot` will then look
        // up the zone ID for the booted zone. This goes through
        // `MockZone::id` to find the zone and get its ID.
        let id_ctx = MockZones::id_context();
        id_ctx.expect().return_once(|name| {
            assert!(name.starts_with(EXPECTED_ZONE_NAME_PREFIX));
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
        version: Generation,
    ) {
        let _expectations = expect_new_service();
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, OXIMETER_PORT, 0, 0);
        mgr.ensure_all_omicron_zones_persistent(OmicronZonesConfig {
            version,
            zones: vec![OmicronZoneConfig {
                id,
                underlay_address: Ipv6Addr::LOCALHOST,
                zone_type: OmicronZoneType::Oximeter { address },
            }],
        })
        .await
        .unwrap();
    }

    // Prepare to call "ensure" for a service which already exists. We should
    // return the service without actually installing a new zone.
    async fn ensure_existing_service(
        mgr: &ServiceManager,
        id: Uuid,
        version: Generation,
    ) {
        let address =
            SocketAddrV6::new(Ipv6Addr::LOCALHOST, OXIMETER_PORT, 0, 0);
        mgr.ensure_all_omicron_zones_persistent(OmicronZonesConfig {
            version,
            zones: vec![OmicronZoneConfig {
                id,
                underlay_address: Ipv6Addr::LOCALHOST,
                zone_type: OmicronZoneType::Oximeter { address },
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
        }
    }

    pub struct TestSecretRetriever {}

    #[async_trait]
    impl SecretRetriever for TestSecretRetriever {
        async fn get_latest(
            &self,
        ) -> Result<VersionedIkm, SecretRetrieverError> {
            let epoch = 0;
            let salt = [0u8; 32];
            let secret = [0x1d; 32];

            Ok(VersionedIkm::new(epoch, salt, &secret))
        }

        async fn get(
            &self,
            epoch: u64,
        ) -> Result<SecretState, SecretRetrieverError> {
            if epoch != 0 {
                return Err(SecretRetrieverError::NoSuchEpoch(epoch));
            }
            Ok(SecretState::Current(self.get_latest().await?))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_ensure_service() {
        let logctx =
            omicron_test_utils::dev::test_setup_log("test_ensure_service");
        let log = logctx.log.clone();
        let test_config = TestConfig::new().await;

        let resources = StorageResources::new_for_test();
        let zone_bundler = ZoneBundler::new(
            log.clone(),
            resources.clone(),
            Default::default(),
        );
        let mgr = ServiceManager::new(
            &log,
            DdmAdminClient::localhost(&log).unwrap(),
            make_bootstrap_networking_config(),
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources,
            zone_bundler,
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id, Generation::new().next()).await;
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

        let resources = StorageResources::new_for_test();
        let zone_bundler = ZoneBundler::new(
            log.clone(),
            resources.clone(),
            Default::default(),
        );
        let mgr = ServiceManager::new(
            &log,
            DdmAdminClient::localhost(&log).unwrap(),
            make_bootstrap_networking_config(),
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources,
            zone_bundler,
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            logctx.log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
        .unwrap();

        let id = Uuid::new_v4();
        let v = Generation::new().next();
        ensure_new_service(&mgr, id, v).await;
        ensure_existing_service(&mgr, id, v).await;
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
        let ddmd_client = DdmAdminClient::localhost(&log).unwrap();
        let bootstrap_networking = make_bootstrap_networking_config();

        // First, spin up a ServiceManager, create a new service, and tear it
        // down.
        let resources = StorageResources::new_for_test();
        let zone_bundler = ZoneBundler::new(
            log.clone(),
            resources.clone(),
            Default::default(),
        );
        let mgr = ServiceManager::new(
            &log,
            ddmd_client.clone(),
            bootstrap_networking.clone(),
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources.clone(),
            zone_bundler.clone(),
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id, Generation::new().next()).await;
        drop_service_manager(mgr);

        // Before we re-create the service manager - notably, using the same
        // config file! - expect that a service gets initialized.
        let _expectations = expect_new_service();
        let mgr = ServiceManager::new(
            &log,
            ddmd_client,
            bootstrap_networking,
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources.clone(),
            zone_bundler.clone(),
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
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
        let ddmd_client = DdmAdminClient::localhost(&log).unwrap();
        let bootstrap_networking = make_bootstrap_networking_config();

        // First, spin up a ServiceManager, create a new zone, and then tear
        // down the ServiceManager.
        let resources = StorageResources::new_for_test();
        let zone_bundler = ZoneBundler::new(
            log.clone(),
            resources.clone(),
            Default::default(),
        );
        let mgr = ServiceManager::new(
            &log,
            ddmd_client.clone(),
            bootstrap_networking.clone(),
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources.clone(),
            zone_bundler.clone(),
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
        .unwrap();

        let id = Uuid::new_v4();
        ensure_new_service(&mgr, id, Generation::new().next()).await;
        drop_service_manager(mgr);

        // Next, delete the ledger. This means the zone we just created will not
        // be remembered on the next initialization.
        std::fs::remove_file(
            test_config.config_dir.path().join(ZONES_LEDGER_FILENAME),
        )
        .unwrap();

        // Observe that the old service is not re-initialized.
        let mgr = ServiceManager::new(
            &log,
            ddmd_client,
            bootstrap_networking,
            SledMode::Auto,
            Some(true),
            SidecarRevision::Physical("rev-test".to_string()),
            vec![],
            resources.clone(),
            zone_bundler.clone(),
        );
        test_config.override_paths(&mgr);

        let port_manager = PortManager::new(
            log.new(o!("component" => "PortManager")),
            Ipv6Addr::new(0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        );
        mgr.sled_agent_started(
            test_config.make_config(),
            port_manager,
            Ipv6Addr::LOCALHOST,
            Uuid::new_v4(),
            None,
        )
        .unwrap();

        drop_service_manager(mgr);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_bootstrap_addr_to_techport_prefixes() {
        let ba: Ipv6Addr = "fdb0:1122:3344:5566::".parse().unwrap();
        let prefixes = ServiceManager::bootstrap_addr_to_techport_prefixes(&ba);
        assert!(prefixes.iter().all(|p| p.net().prefix() == 64));
        let prefix0 = prefixes[0].net().network();
        let prefix1 = prefixes[1].net().network();
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
