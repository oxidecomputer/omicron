// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-local service management.
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
//! - [ServiceManager::start_omicron_zone] exposes an API to start a new Omicron
//!   zone.
//! - [ServiceManager::activate_switch] exposes an API to specifically enable
//!   or disable (via [ServiceManager::deactivate_switch]) the switch zone.

use crate::bootstrap::BootstrapNetworking;
use crate::bootstrap::early_networking::{
    EarlyNetworkSetup, EarlyNetworkSetupError,
};
use crate::config::SidecarRevision;
use crate::ddm_reconciler::DdmReconciler;
use crate::metrics::MetricsRequestQueue;
use crate::profile::*;
use anyhow::anyhow;
use camino::{Utf8Path, Utf8PathBuf};
use clickhouse_admin_types::CLICKHOUSE_KEEPER_CONFIG_DIR;
use clickhouse_admin_types::CLICKHOUSE_KEEPER_CONFIG_FILE;
use clickhouse_admin_types::CLICKHOUSE_SERVER_CONFIG_DIR;
use clickhouse_admin_types::CLICKHOUSE_SERVER_CONFIG_FILE;
use dpd_client::{Client as DpdClient, Error as DpdError, types as DpdTypes};
use dropshot::HandlerTaskMode;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::addrobj::IPV6_LINK_LOCAL_ADDROBJ_NAME;
use illumos_utils::dladm::{
    Dladm, Etherstub, EtherstubVnic, GetSimnetError, PhysicalLink,
};
use illumos_utils::link::{Link, VnicAllocator};
use illumos_utils::opte::{
    DhcpCfg, Port, PortCreateParams, PortManager, PortTicket,
};
use illumos_utils::running_zone::{
    EnsureAddressError, InstalledZone, RunCommandError, RunningZone,
    ZoneBuilderFactory,
};
use illumos_utils::smf_helper::SmfHelper;
use illumos_utils::zfs::ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT;
use illumos_utils::zone::AddressRequest;
use illumos_utils::zpool::{PathInPool, ZpoolName, ZpoolOrRamdisk};
use illumos_utils::{PFEXEC, execute};
use internal_dns_resolver::Resolver;
use internal_dns_types::names::BOUNDARY_NTP_DNS_NAME;
use internal_dns_types::names::DNS_ZONE;
use nexus_config::{ConfigDropshotWithTls, DeploymentConfig};
use nexus_sled_agent_shared::inventory::{
    OmicronZoneConfig, OmicronZoneType, ZoneKind,
};
use omicron_common::address::AZ_PREFIX;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::LLDP_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NTP_ADMIN_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::TFPORTD_PORT;
use omicron_common::address::WICKETD_NEXUS_PROXY_PORT;
use omicron_common::address::WICKETD_PORT;
use omicron_common::address::{BOOTSTRAP_ARTIFACT_PORT, COCKROACH_ADMIN_PORT};
use omicron_common::address::{
    CLICKHOUSE_ADMIN_PORT, CLICKHOUSE_TCP_PORT,
    get_internal_dns_server_addresses,
};
use omicron_common::address::{Ipv6Subnet, NEXUS_TECHPORT_EXTERNAL_PORT};
use omicron_common::api::external::Generation;
use omicron_common::api::internal::shared::{
    HostPortConfig, RackNetworkConfig, SledIdentifiers,
};
use omicron_common::backoff::{
    BackoffError, retry_notify, retry_policy_internal_service_aggressive,
};
use omicron_common::disk::{DatasetKind, DatasetName};
use omicron_ddm_admin_client::DdmError;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::sled::SWITCH_ZONE_BASEBOARD_FILE;
use sled_agent_types::zone_images::{
    MupdateOverrideReadError, PreparedOmicronZone,
};
use sled_agent_zone_images::{ZoneImageSourceResolver, ramdisk_file_source};
use sled_hardware::DendriteAsic;
use sled_hardware::SledMode;
use sled_hardware::is_oxide_sled;
use sled_hardware::underlay;
use sled_hardware_types::Baseboard;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

use illumos_utils::zone::Zones;

// These are all the same binary. They just reside at different paths.
const CLICKHOUSE_SERVER_BINARY: &str =
    "/opt/oxide/clickhouse_server/clickhouse";
const CLICKHOUSE_KEEPER_BINARY: &str =
    "/opt/oxide/clickhouse_keeper/clickhouse";
const CLICKHOUSE_BINARY: &str = "/opt/oxide/clickhouse/clickhouse";

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

    #[error("Sled Agent not initialized yet")]
    SledAgentNotReady,

    #[error("Switch zone error: {0}")]
    SwitchZone(anyhow::Error),

    #[error("Failed to issue SMF command: {0}")]
    SmfCommand(#[from] illumos_utils::smf_helper::Error),

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

    #[error("Cannot remove zone {zone_name}")]
    ZoneRemoval {
        zone_name: String,
        #[source]
        err: illumos_utils::zone::AdmError,
    },

    #[error("Cannot clean up after removal of zone {zone_name}")]
    ZoneCleanup {
        zone_name: String,
        #[source]
        err: Box<illumos_utils::zone::DeleteAddressError>,
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

    #[error("Failed to create OPTE port for service {}: {err}", service.report_str())]
    ServicePortCreation {
        service: ZoneKind,
        err: Box<illumos_utils::opte::Error>,
    },

    #[error("Error contacting dpd: {0}")]
    DpdError(#[from] DpdError<DpdTypes::Error>),

    #[error("Failed to create Vnic in the switch zone: {0}")]
    SwitchZoneVnicCreation(illumos_utils::dladm::CreateVnicError),

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
    ResolveError(#[from] internal_dns_resolver::ResolveError),

    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Sidecar revision error")]
    SidecarRevision(#[from] anyhow::Error),

    #[error("Early networking setup error")]
    EarlyNetworkSetupError(#[from] EarlyNetworkSetupError),

    #[error("Error querying simnet devices")]
    Simnet(#[from] GetSimnetError),

    #[error(
        "Requested zone generation ({requested}) \
        is older than current ({current})"
    )]
    RequestedZoneConfigOutdated { requested: Generation, current: Generation },

    #[error("Requested generation {0} with different zones than before")]
    RequestedConfigConflicts(Generation),

    #[error("Error migrating old-format services ledger: {0:#}")]
    ServicesMigration(anyhow::Error),

    #[error(
        "Invalid filesystem_pool in new zone config: \
         for zone {zone_id}, expected pool {expected_pool} but got {got_pool}"
    )]
    InvalidFilesystemPoolZoneConfig {
        zone_id: OmicronZoneUuid,
        expected_pool: ZpoolName,
        got_pool: ZpoolName,
    },

    #[error("Unexpected zone config: zone {zone_id} is running on ramdisk ?!")]
    ZoneIsRunningOnRamdisk { zone_id: OmicronZoneUuid },

    #[error("failed to read mupdate override info, not starting zones")]
    MupdateOverrideRead(#[source] MupdateOverrideReadError),

    #[error(
        "Couldn't find requested zone image ({hash}) for \
        {zone_kind:?} {id} in artifact store: {err}"
    )]
    ZoneArtifactNotFound {
        hash: ArtifactHash,
        zone_kind: &'static str,
        id: OmicronZoneUuid,
        #[source]
        err: crate::artifact_store::Error,
    },
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
            Error::RequestedConfigConflicts(_)
            | Error::InvalidFilesystemPoolZoneConfig { .. }
            | Error::ZoneIsRunningOnRamdisk { .. } => {
                omicron_common::api::external::Error::invalid_request(
                    &err.to_string(),
                )
            }
            Error::RequestedZoneConfigOutdated { .. } => {
                omicron_common::api::external::Error::conflict(&err.to_string())
            }
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
}

/// Information describing the underlay network, used when activating the switch
/// zone.
#[derive(Debug, Clone)]
pub struct UnderlayInfo {
    pub ip: Ipv6Addr,
    pub rack_network_config: Option<RackNetworkConfig>,
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

/// Helper function to add properties to a PropertyGroupBuilder for a sled
/// agent centered around rack and sled identifiers.
fn add_sled_ident_properties(
    config: PropertyGroupBuilder,
    info: &SledAgentInfo,
) -> PropertyGroupBuilder {
    config
        .add_property("rack_id", "astring", &info.rack_id.to_string())
        .add_property(
            "sled_id",
            "astring",
            &info.config.sled_identifiers.sled_id.to_string(),
        )
        .add_property(
            "sled_model",
            "astring",
            &info.config.sled_identifiers.model.to_string(),
        )
        .add_property(
            "sled_serial",
            "astring",
            &info.config.sled_identifiers.serial.to_string(),
        )
        .add_property(
            "sled_revision",
            "astring",
            &info.config.sled_identifiers.revision.to_string(),
        )
}

/// Helper function to set properties on a SmfHelper for a sled agent centered
/// around rack and sled identifiers.
fn setprop_sled_ident_properties(
    smfh: &SmfHelper,
    info: &SledAgentInfo,
) -> Result<(), Error> {
    smfh.setprop_default_instance("config/rack_id", info.rack_id)?;
    smfh.setprop_default_instance(
        "config/sled_id",
        info.config.sled_identifiers.sled_id,
    )?;
    smfh.setprop_default_instance(
        "config/sled_model",
        info.config.sled_identifiers.model.to_string(),
    )?;
    smfh.setprop_default_instance(
        "config/sled_revision",
        info.config.sled_identifiers.revision,
    )?;
    smfh.setprop_default_instance(
        "config/sled_serial",
        info.config.sled_identifiers.serial.to_string(),
    )?;

    Ok(())
}

/// Configuration parameters which modify the [`ServiceManager`]'s behavior.
pub struct Config {
    /// Identifies the sled being configured
    pub sled_identifiers: SledIdentifiers,

    /// Identifies the revision of the sidecar to be used.
    pub sidecar_revision: SidecarRevision,
}

impl Config {
    pub fn new(
        sled_identifiers: SledIdentifiers,
        sidecar_revision: SidecarRevision,
    ) -> Self {
        Self { sled_identifiers, sidecar_revision }
    }
}

/// Describes the host OS interfaces used by [ServiceManager].
///
/// Default implementations cause this trait to access the host
/// OS, but can be overwritten to intercept calls to the host
/// for testing.
pub trait SystemApi: Send + Sync {
    fn fake_install_dir(&self) -> Option<&Utf8Path> {
        None
    }

    fn dladm(&self) -> Arc<dyn illumos_utils::dladm::Api> {
        Arc::new(illumos_utils::dladm::Dladm::real_api())
    }
    fn zones(&self) -> Arc<dyn illumos_utils::zone::Api> {
        Arc::new(illumos_utils::zone::Zones::real_api())
    }
}

pub struct RealSystemApi {}

impl RealSystemApi {
    pub fn new() -> Box<dyn SystemApi> {
        Box::new(RealSystemApi {})
    }
}

impl SystemApi for RealSystemApi {}

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

impl illumos_utils::smf_helper::Service for SwitchService {
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
}

/// Describes either an Omicron-managed zone or the switch zone, used for
/// functions that operate on either one or the other
enum ZoneArgs<'a> {
    Omicron(PreparedOmicronZone<'a>),
    Switch(&'a SwitchZoneConfig),
}

impl<'a> ZoneArgs<'a> {
    /// If this is an Omicron zone, return its type
    pub fn omicron_type(&self) -> Option<&'a OmicronZoneType> {
        match self {
            ZoneArgs::Omicron(prepared_zone) => {
                Some(&prepared_zone.config().zone_type)
            }
            ZoneArgs::Switch(_) => None,
        }
    }

    /// If this is a switch zone, iterate over the services it's
    /// supposed to be running
    pub fn switch_zone_services(
        &self,
    ) -> Box<dyn Iterator<Item = &'a SwitchService> + Send + 'a> {
        match self {
            ZoneArgs::Omicron(_) => Box::new(std::iter::empty()),
            ZoneArgs::Switch(request) => Box::new(request.services.iter()),
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

/// Describes the state of a switch zone.
enum SwitchZoneState {
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
        zone: Box<RunningZone>,
        // A background task which keeps looping until the zone's uplinks are
        // configured.
        worker: Option<Task>,
    },
}

/// Manages miscellaneous Sled-local services.
pub struct ServiceManagerInner {
    log: Logger,
    global_zone_bootstrap_link_local_address: Ipv6Addr,
    switch_zone: Mutex<SwitchZoneState>,
    sled_mode: SledMode,
    time_synced: AtomicBool,
    switch_zone_maghemite_links: Vec<PhysicalLink>,
    sidecar_revision: SidecarRevision,
    underlay_vnic_allocator: VnicAllocator<Etherstub>,
    underlay_vnic: EtherstubVnic,
    bootstrap_vnic_allocator: VnicAllocator<Etherstub>,
    ddm_reconciler: DdmReconciler,
    sled_info: OnceLock<SledAgentInfo>,
    switch_zone_bootstrap_address: Ipv6Addr,
    zone_image_resolver: ZoneImageSourceResolver,
    system_api: Box<dyn SystemApi>,
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
    metrics_queue: MetricsRequestQueue,
}

#[derive(Clone)]
pub struct ServiceManager {
    inner: Arc<ServiceManagerInner>,
}

/// Ensure that a NAT entry exists, overwriting a previous conflicting entry if
/// applicable.
///
/// nat_ipv\[46\]_create are not idempotent (see oxidecomputer/dendrite#343),
/// but this wrapper function is. Call this from sagas instead.
#[allow(clippy::too_many_arguments)]
async fn dpd_ensure_nat_entry(
    client: &DpdClient,
    log: &Logger,
    target_ip: IpAddr,
    target_mac: DpdTypes::MacAddr,
    target_first_port: u16,
    target_last_port: u16,
    target_vni: u32,
    sled_ip_address: &std::net::Ipv6Addr,
) -> Result<(), Error> {
    let existing_nat = match &target_ip {
        IpAddr::V4(ip) => client.nat_ipv4_get(ip, target_first_port).await,
        IpAddr::V6(ip) => client.nat_ipv6_get(ip, target_first_port).await,
    };

    // If a NAT entry already exists, but has the wrong internal
    // IP address, delete the old entry before continuing (the
    // DPD entry-creation API won't replace an existing entry).
    // If the entry exists and has the right internal IP, there's
    // no more work to do for this external IP.
    match existing_nat {
        Ok(existing) => {
            let existing = existing.into_inner();
            if existing.internal_ip != *sled_ip_address {
                info!(log, "deleting old nat entry";
                      "target_ip" => ?target_ip);

                match &target_ip {
                    IpAddr::V4(ip) => {
                        client.nat_ipv4_delete(ip, target_first_port).await
                    }
                    IpAddr::V6(ip) => {
                        client.nat_ipv6_delete(ip, target_first_port).await
                    }
                }?;
            } else {
                info!(log,
                      "nat entry with expected internal ip exists";
                      "target_ip" => ?target_ip,
                      "existing_entry" => ?existing);

                return Ok(());
            }
        }
        Err(e) => {
            if e.status() == Some(http::StatusCode::NOT_FOUND) {
                info!(log, "no nat entry found for: {target_ip:#?}");
            } else {
                return Err(Error::DpdError(e));
            }
        }
    }

    info!(log, "creating nat entry for: {target_ip:#?}");
    let nat_target = DpdTypes::NatTarget {
        inner_mac: target_mac,
        internal_ip: *sled_ip_address,
        vni: target_vni.into(),
    };

    match &target_ip {
        IpAddr::V4(ip) => {
            client
                .nat_ipv4_create(
                    ip,
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
        }
        IpAddr::V6(ip) => {
            client
                .nat_ipv6_create(
                    ip,
                    target_first_port,
                    target_last_port,
                    &nat_target,
                )
                .await
        }
    }?;

    info!(log, "creation of nat entry successful for: {target_ip:#?}");

    Ok(())
}

impl ServiceManager {
    /// Creates a service manager.
    ///
    /// Args:
    /// - `log`: The logger
    /// - `ddm_reconciler`: Handle for configuring our localhost ddmd
    /// - `bootstrap_networking`: Collection of etherstubs/VNICs set up when
    ///    bootstrap agent begins
    /// - `sled_mode`: The sled's mode of operation (Gimlet vs Scrimlet).
    /// - `sidecar_revision`: Rev of attached sidecar, if present.
    /// - `switch_zone_maghemite_links`: List of physical links on which
    ///    maghemite should listen.
    /// - `zone_image_resolver`: how to find Omicron zone images
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log: &Logger,
        ddm_reconciler: DdmReconciler,
        bootstrap_networking: BootstrapNetworking,
        sled_mode: SledMode,
        sidecar_revision: SidecarRevision,
        switch_zone_maghemite_links: Vec<PhysicalLink>,
        zone_image_resolver: ZoneImageSourceResolver,
    ) -> Self {
        Self::new_inner(
            log,
            ddm_reconciler,
            bootstrap_networking,
            sled_mode,
            sidecar_revision,
            switch_zone_maghemite_links,
            zone_image_resolver,
            RealSystemApi::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        log: &Logger,
        ddm_reconciler: DdmReconciler,
        bootstrap_networking: BootstrapNetworking,
        sled_mode: SledMode,
        sidecar_revision: SidecarRevision,
        switch_zone_maghemite_links: Vec<PhysicalLink>,
        zone_image_resolver: ZoneImageSourceResolver,
        system_api: Box<dyn SystemApi>,
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
                switch_zone: Mutex::new(SwitchZoneState::Disabled),
                sled_mode,
                time_synced: AtomicBool::new(false),
                sidecar_revision,
                switch_zone_maghemite_links,
                underlay_vnic_allocator: VnicAllocator::new(
                    "Service",
                    bootstrap_networking.underlay_etherstub,
                    system_api.dladm(),
                ),
                underlay_vnic: bootstrap_networking.underlay_etherstub_vnic,
                bootstrap_vnic_allocator: VnicAllocator::new(
                    "Bootstrap",
                    bootstrap_networking.bootstrap_etherstub,
                    system_api.dladm(),
                ),
                ddm_reconciler,
                sled_info: OnceLock::new(),
                switch_zone_bootstrap_address: bootstrap_networking
                    .switch_zone_bootstrap_ip,
                zone_image_resolver,
                system_api,
            }),
        }
    }

    pub(crate) fn ddm_reconciler(&self) -> &DdmReconciler {
        &self.inner.ddm_reconciler
    }

    pub fn switch_zone_bootstrap_address(&self) -> Ipv6Addr {
        self.inner.switch_zone_bootstrap_address
    }

    /// Sets up "Sled Agent" information, including underlay info.
    ///
    /// Any subsequent calls after the first invocation return an error.
    pub async fn sled_agent_started(
        &self,
        config: Config,
        port_manager: PortManager,
        underlay_address: Ipv6Addr,
        rack_id: Uuid,
        rack_network_config: Option<RackNetworkConfig>,
        metrics_queue: MetricsRequestQueue,
    ) -> Result<(), Error> {
        info!(
            &self.inner.log, "sled agent started";
            "underlay_address" => underlay_address.to_string(),
        );
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
                metrics_queue: metrics_queue.clone(),
            })
            .map_err(|_| "already set".to_string())
            .expect("Sled Agent should only start once");

        // At this point, we've already started up the switch zone, but the
        // VNICs inside it cannot have been tracked by the sled-agent's metrics
        // task (the sled-agent didn't exist at the time we started the switch
        // zone!). Notify that task about the zone's VNICs now.
        if let SwitchZoneState::Running { zone, .. } =
            &*self.inner.switch_zone.lock().await
        {
            match metrics_queue.track_zone_links(zone) {
                Ok(_) => {
                    debug!(self.inner.log, "Stopped tracking zone datalinks")
                }
                Err(errors) => {
                    error!(
                        self.inner.log,
                        "Failed to track one or more data links in \
                        the switch zone, some metrics will not \
                        be produced.";
                        "errors" => ?errors,
                    );
                }
            }
        }

        Ok(())
    }

    /// Returns the sled's configured mode.
    pub fn sled_mode(&self) -> SledMode {
        self.inner.sled_mode
    }

    /// Returns the sled's identifier
    fn sled_id(&self) -> Uuid {
        self.inner
            .sled_info
            .get()
            .expect("sled agent not started")
            .config
            .sled_identifiers
            .sled_id
    }

    /// Returns the metrics queue for the sled agent if it is running.
    fn maybe_metrics_queue(&self) -> Option<&MetricsRequestQueue> {
        self.inner.sled_info.get().map(|info| &info.metrics_queue)
    }

    /// Returns the metrics queue for the sled agent.
    fn metrics_queue(&self) -> &MetricsRequestQueue {
        &self.maybe_metrics_queue().expect("Sled agent should have started")
    }

    // Check the services intended to run in the zone to determine whether any
    // physical devices need to be mapped into the zone when it is created.
    fn devices_needed(zone_args: &ZoneArgs<'_>) -> Result<Vec<String>, Error> {
        let mut devices = vec![];
        for svc_details in zone_args.switch_zone_services() {
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
    async fn bootstrap_address_needed(
        &self,
        zone_args: &ZoneArgs<'_>,
    ) -> Result<Option<(Link, Ipv6Addr)>, Error> {
        if let ZoneArgs::Switch(_) = zone_args {
            let link = self
                .inner
                .bootstrap_vnic_allocator
                .new_bootstrap()
                .await
                .map_err(Error::SwitchZoneVnicCreation)?;
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
    async fn links_needed(
        &self,
        zone_args: &ZoneArgs<'_>,
    ) -> Result<Vec<(Link, bool)>, Error> {
        let mut links: Vec<(Link, bool)> = Vec::new();

        let is_oxide_sled = is_oxide_sled().map_err(|e| {
            Error::Underlay(underlay::Error::SystemDetection(e))
        })?;

        for svc_details in zone_args.switch_zone_services() {
            match &svc_details {
                SwitchService::Tfport { pkt_source, asic: _ } => {
                    // The tfport service requires a MAC device to/from which
                    // sidecar packets may be multiplexed.  If the link isn't
                    // present, don't bother trying to start the zone.
                    match self
                        .inner
                        .system_api
                        .dladm()
                        .verify_link(pkt_source)
                        .await
                    {
                        Ok(link) => {
                            // It's important that tfpkt does **not** receive a
                            // link local address! See: https://github.com/oxidecomputer/stlouis/issues/391
                            links.push((link, false));
                        }
                        Err(_) => {
                            if is_oxide_sled {
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
                        match self
                            .inner
                            .system_api
                            .dladm()
                            .verify_link(&link.to_string())
                            .await
                        {
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
        let (zone_kind, nic, snat, floating_ips) = match &zone_args
            .omicron_type()
        {
            Some(
                zone_type @ OmicronZoneType::Nexus { external_ip, nic, .. },
            ) => {
                (zone_type.kind(), nic, None, std::slice::from_ref(external_ip))
            }
            Some(
                zone_type @ OmicronZoneType::ExternalDns {
                    dns_address,
                    nic,
                    ..
                },
            ) => {
                external_ip = dns_address.ip();
                (
                    zone_type.kind(),
                    nic,
                    None,
                    std::slice::from_ref(&external_ip),
                )
            }
            Some(
                zone_type @ OmicronZoneType::BoundaryNtp {
                    nic, snat_cfg, ..
                },
            ) => (zone_type.kind(), nic, Some(*snat_cfg), &[][..]),
            _ => unreachable!("unexpected zone type"),
        };

        // Create the OPTE port for the service.
        // Note we don't plumb any firewall rules at this point,
        // Nexus will plumb them down later but services' default OPTE
        // config allows outbound access which is enough for
        // Boundary NTP which needs to come up before Nexus.
        let port = port_manager
            .create_port(PortCreateParams {
                nic,
                source_nat: snat,
                ephemeral_ip: None,
                floating_ips,
                firewall_rules: &[],
                dhcp_config: DhcpCfg::default(),
            })
            .map_err(|err| Error::ServicePortCreation {
                service: zone_kind,
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
                    "zone_type" => zone_kind.report_str(),
                );

                dpd_ensure_nat_entry(
                    dpd_client,
                    &self.inner.log,
                    target_ip,
                    dpd_client::types::MacAddr { a: port.0.mac().into_array() },
                    first_port,
                    last_port,
                    port.0.vni().as_u32(),
                    underlay_address,
                )
                .await
                .map_err(BackoffError::transient)
            };
            let log_failure = |error, _| {
                warn!(
                    self.inner.log, "failed to create NAT entry for service";
                    "error" => ?error,
                    "zone_type" => zone_kind.report_str(),
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
        for svc_details in zone_args.switch_zone_services() {
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
        domain: Option<&str>,
    ) -> Result<ServiceBuilder, Error> {
        // We want to configure the dns/install SMF service inside the zone with
        // the list of DNS nameservers. This will cause /etc/resolv.conf to be
        // populated inside the zone. We will populate it with the small number
        // of fixed DNS addresses that should always exist. If clients want to
        // expand to a wider range of DNS servers, they can bootstrap from the
        // fixed addresses by looking up additional internal DNS servers from
        // those addresses.
        //
        // Note that when we configure the dns/install service, we're supplying
        // values for an existing property group on the SMF *service*.  We're
        // not creating a new property group, nor are we configuring a property
        // group on the instance.
        //
        // Callers may decide to provide specific addresses to set as additional
        // nameservers; e.g., boundary NTP zones need to specify upstream DNS
        // servers to resolve customer-provided NTP server names.
        let mut nameservers =
            get_internal_dns_server_addresses(info.underlay_address);
        nameservers.extend(ip_addrs.into_iter().flatten());

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
        gw_addr: Option<&Ipv6Addr>,
        zone: &InstalledZone,
        static_addrs: &[Ipv6Addr],
    ) -> Result<ServiceBuilder, Error> {
        let datalink = zone.get_control_vnic_name();

        let mut config_builder = PropertyGroupBuilder::new("config");
        config_builder =
            config_builder.add_property("datalink", "astring", datalink);

        // The switch zone is the only zone that will sometimes have an
        // unknown underlay address at zone boot on the first scrimlet.
        if let Some(gateway) = gw_addr {
            config_builder = config_builder.add_property(
                "gateway",
                "astring",
                gateway.to_string(),
            );
        }

        for s in static_addrs {
            config_builder = config_builder.add_property(
                "static_addr",
                "astring",
                &s.to_string(),
            );
        }

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

        let opte_interface = port.name();
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
        zone_root_path: PathInPool,
        filesystems: &[zone::Fs],
        data_links: &[String],
    ) -> Result<RunningZone, Error> {
        let device_names = Self::devices_needed(&request)?;
        let (bootstrap_vnic, bootstrap_name_and_address) =
            match self.bootstrap_address_needed(&request).await? {
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
            self.links_needed(&request).await?.into_iter().unzip();
        let opte_ports = self.opte_ports_needed(&request).await?;
        let limit_priv = Self::privs_needed(&request);

        // If the zone is managing a particular dataset, plumb that
        // dataset into the zone. Additionally, construct a "unique enough" name
        // so we can create multiple zones of this type without collision.
        let unique_name = match &request {
            ZoneArgs::Omicron(prepared_zone) => Some(prepared_zone.config().id),
            ZoneArgs::Switch(_) => None,
        };
        let datasets: Vec<_> = match &request {
            ZoneArgs::Omicron(prepared_zone) => prepared_zone
                .config()
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

        let zone_type_str = match &request {
            ZoneArgs::Omicron(prepared_zone) => {
                prepared_zone.config().zone_type.kind().zone_prefix()
            }
            ZoneArgs::Switch(_) => "switch",
        };

        // TODO: `InstallDataset` should be renamed to something more accurate
        // when all the major changes here have landed.
        //
        // Some zones are distributed from the host OS image and are never
        // placed in the install dataset; the Ramdisk enum variant more
        // accurately reflects that we are only search `/opt/oxide` for those
        // zones.
        //
        // (Currently, only the switch zone goes through this code path. Other
        // ramdisk zones like the probe zone construct the file source
        // directly.)

        let file_source = match &request {
            ZoneArgs::Omicron(prepared_zone) => {
                prepared_zone.file_source().file_source.clone()
            }
            ZoneArgs::Switch(_) => ramdisk_file_source(zone_type_str),
        };

        // We use the fake initialiser for testing
        let mut zone_builder = match self.inner.system_api.fake_install_dir() {
            None => ZoneBuilderFactory::new().builder(),
            Some(dir) => ZoneBuilderFactory::fake(
                Some(&dir.as_str()),
                illumos_utils::fakes::zone::Zones::new(),
            )
            .builder(),
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
            .with_zone_root_path(zone_root_path)
            .with_zone_type(zone_type_str)
            .with_file_source(&file_source)
            .with_datasets(datasets.as_slice())
            .with_filesystems(&filesystems)
            .with_data_links(&data_links)
            .with_devices(&devices)
            .with_opte_ports(opte_ports)
            .with_links(links)
            .with_limit_priv(limit_priv)
            .install()
            .await?;

        let running_zone = match &request {
            ZoneArgs::Omicron(prepared_zone) => {
                self.boot_omicron_zone(prepared_zone.config(), installed_zone)
                    .await?
            }
            ZoneArgs::Switch(config) => {
                self.boot_switch_zone(
                    config,
                    installed_zone,
                    &links_need_link_local,
                    bootstrap_name_and_address,
                    &device_names,
                )
                .await?
            }
        };

        // Now that we've booted the zone, we'll notify the sled-agent about:
        //
        // - Its control VNIC (all zones have one)
        // - Any bootstrap network VNIC (only the switch zone has one)
        // - Any OPTE ports (instance zones, or Oxide zones with external
        // connectivity).
        //
        // Note that we'll almost always have started the sled-agent at this
        // point. The only exception is the switch zone, during bootstrapping
        // but before we've either run RSS or unlocked the rack. In both those
        // cases, we have a `StartSledAgentRequest`, and so a metrics queue.
        if let Some(queue) = self.maybe_metrics_queue() {
            match queue.track_zone_links(&running_zone) {
                Ok(_) => debug!(self.inner.log, "Tracking zone datalinks"),
                Err(errors) => {
                    error!(
                        self.inner.log,
                        "Failed to track one or more links in the zone, \
                        some metrics will not be produced";
                        "zone_name" => running_zone.name(),
                        "errors" => ?errors,
                    );
                }
            }
        }
        Ok(running_zone)
    }

    async fn boot_omicron_zone(
        &self,
        config: &OmicronZoneConfig,
        installed_zone: InstalledZone,
    ) -> Result<RunningZone, Error> {
        let disabled_ssh_service = ServiceBuilder::new("network/ssh")
            .add_instance(ServiceInstanceBuilder::new("default").disable());

        let disabled_dns_client_service =
            ServiceBuilder::new("network/dns/client")
                .add_instance(ServiceInstanceBuilder::new("default").disable());

        let enabled_dns_client_service =
            ServiceBuilder::new("network/dns/client")
                .add_instance(ServiceInstanceBuilder::new("default"));

        let running_zone = match config {
            OmicronZoneConfig {
                zone_type: OmicronZoneType::Clickhouse { address, .. },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let dns_service = Self::dns_install(info, None, None).await?;

                let config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "listen_addr",
                        "astring",
                        address.ip().to_string(),
                    )
                    .add_property(
                        "listen_port",
                        "astring",
                        address.port().to_string(),
                    )
                    .add_property("store", "astring", "/data");
                let clickhouse_service =
                    ServiceBuilder::new("oxide/clickhouse").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(config),
                    );

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let admin_address = {
                    let mut addr = *address;
                    addr.set_port(CLICKHOUSE_ADMIN_PORT);
                    addr.to_string()
                };

                // The ClickHouse client connects via the TCP port
                let ch_address = {
                    let mut addr = *address;
                    addr.set_port(CLICKHOUSE_TCP_PORT);
                    addr.to_string()
                };

                let clickhouse_admin_config =
                    PropertyGroupBuilder::new("config")
                        .add_property("http_address", "astring", admin_address)
                        .add_property(
                            "ch_address",
                            "astring",
                            ch_address.to_string(),
                        )
                        .add_property(
                            "ch_binary",
                            "astring",
                            CLICKHOUSE_BINARY,
                        );
                let clickhouse_admin_service =
                    ServiceBuilder::new("oxide/clickhouse-admin-single")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(clickhouse_admin_config),
                        );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(clickhouse_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service)
                    .add_service(clickhouse_admin_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to setup clickhouse profile", err)
                    })?;
                RunningZone::boot(installed_zone).await?
            }

            OmicronZoneConfig {
                zone_type: OmicronZoneType::ClickhouseServer { address, .. },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let dns_service = Self::dns_install(info, None, None).await?;

                let clickhouse_server_config =
                    PropertyGroupBuilder::new("config")
                        .add_property(
                            "config_path",
                            "astring",
                            format!("{CLICKHOUSE_SERVER_CONFIG_DIR}/{CLICKHOUSE_SERVER_CONFIG_FILE}"),
                        );
                let disabled_clickhouse_server_service =
                    ServiceBuilder::new("oxide/clickhouse_server")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .disable()
                                .add_property_group(clickhouse_server_config),
                        );

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let admin_address = {
                    let mut addr = *address;
                    addr.set_port(CLICKHOUSE_ADMIN_PORT);
                    addr.to_string()
                };

                // The ClickHouse client connects via the TCP port
                let ch_address = {
                    let mut addr = *address;
                    addr.set_port(CLICKHOUSE_TCP_PORT);
                    addr.to_string()
                };

                let clickhouse_admin_config =
                    PropertyGroupBuilder::new("config")
                        .add_property("http_address", "astring", admin_address)
                        .add_property(
                            "ch_address",
                            "astring",
                            ch_address.to_string(),
                        )
                        .add_property(
                            "ch_binary",
                            "astring",
                            CLICKHOUSE_SERVER_BINARY,
                        );
                let clickhouse_admin_service =
                    ServiceBuilder::new("oxide/clickhouse-admin-server")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(clickhouse_admin_config),
                        );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(disabled_clickhouse_server_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service)
                    .add_service(clickhouse_admin_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io(
                            "Failed to setup clickhouse server profile",
                            err,
                        )
                    })?;
                RunningZone::boot(installed_zone).await?
            }

            OmicronZoneConfig {
                zone_type: OmicronZoneType::ClickhouseKeeper { address, .. },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let dns_service = Self::dns_install(info, None, None).await?;

                let clickhouse_keeper_config =
                    PropertyGroupBuilder::new("config")
                        .add_property(
                            "config_path",
                            "astring",
                            format!("{CLICKHOUSE_KEEPER_CONFIG_DIR}/{CLICKHOUSE_KEEPER_CONFIG_FILE}"),
                        );
                let disabled_clickhouse_keeper_service =
                    ServiceBuilder::new("oxide/clickhouse_keeper")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .disable()
                                .add_property_group(clickhouse_keeper_config),
                        );

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let admin_address = {
                    let mut addr = *address;
                    addr.set_port(CLICKHOUSE_ADMIN_PORT);
                    addr.to_string()
                };

                let clickhouse_admin_config =
                    PropertyGroupBuilder::new("config")
                        .add_property("http_address", "astring", admin_address)
                        .add_property(
                            "ch_address",
                            "astring",
                            address.to_string(),
                        )
                        .add_property(
                            "ch_binary",
                            "astring",
                            CLICKHOUSE_KEEPER_BINARY,
                        );
                let clickhouse_admin_service =
                    ServiceBuilder::new("oxide/clickhouse-admin-keeper")
                        .add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(clickhouse_admin_config),
                        );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(disabled_clickhouse_keeper_service)
                    .add_service(dns_service)
                    .add_service(enabled_dns_client_service)
                    .add_service(clickhouse_admin_service);
                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io(
                            "Failed to setup clickhouse keeper profile",
                            err,
                        )
                    })?;
                RunningZone::boot(installed_zone).await?
            }

            OmicronZoneConfig {
                id: zone_id,
                zone_type: OmicronZoneType::CockroachDb { address, .. },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let admin_address = {
                    let mut addr = *address;
                    addr.set_port(COCKROACH_ADMIN_PORT);
                    addr.to_string()
                };
                let cockroach_http_address = std::net::SocketAddrV4::new(
                    std::net::Ipv4Addr::LOCALHOST,
                    8080,
                );

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let dns_service = Self::dns_install(info, None, None).await?;

                // Configure the CockroachDB service.
                let cockroachdb_config = PropertyGroupBuilder::new("config")
                    .add_property("listen_addr", "astring", address.to_string())
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
                            address.to_string(),
                        )
                        .add_property(
                            "cockroach_http_address",
                            "astring",
                            cockroach_http_address.to_string(),
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
                RunningZone::boot(installed_zone).await?
            }

            OmicronZoneConfig {
                zone_type: OmicronZoneType::Crucible { address, dataset },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let dataset_name =
                    DatasetName::new(dataset.pool_name, DatasetKind::Crucible)
                        .full_name();
                let uuid = &Uuid::new_v4().to_string();
                let config = PropertyGroupBuilder::new("config")
                    .add_property("dataset", "astring", &dataset_name)
                    .add_property(
                        "listen_addr",
                        "astring",
                        address.ip().to_string(),
                    )
                    .add_property(
                        "listen_port",
                        "astring",
                        address.port().to_string(),
                    )
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
                RunningZone::boot(installed_zone).await?
            }

            OmicronZoneConfig {
                zone_type: OmicronZoneType::CruciblePantry { address },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "listen_addr",
                        "astring",
                        address.ip().to_string(),
                    )
                    .add_property(
                        "listen_port",
                        "astring",
                        address.port().to_string(),
                    );

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
                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                id,
                zone_type: OmicronZoneType::Oximeter { address },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let oximeter_config = PropertyGroupBuilder::new("config")
                    .add_property("id", "astring", id.to_string())
                    .add_property("address", "astring", address.to_string());
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
                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                zone_type:
                    OmicronZoneType::ExternalDns {
                        http_address,
                        dns_address,
                        nic,
                        ..
                    },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*http_address.ip()],
                )?;
                // Like Nexus, we need to be reachable externally via
                // `dns_address` but we don't listen on that address
                // directly but instead on a VPC private IP. OPTE will
                // en/decapsulate as appropriate.
                let opte_interface_setup =
                    Self::opte_interface_set_up_install(&installed_zone)?;

                // We need to tell external_dns to listen on its OPTE port IP
                // address, which comes from `nic`. Attach the port from its
                // true external DNS address (`dns_address`).
                let dns_address =
                    SocketAddr::new(nic.ip, dns_address.port()).to_string();

                let external_dns_config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "http_address",
                        "astring",
                        http_address.to_string(),
                    )
                    .add_property("dns_address", "astring", dns_address);
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
                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                zone_type:
                    OmicronZoneType::BoundaryNtp {
                        address,
                        dns_servers,
                        ntp_servers,
                        domain,
                        ..
                    },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let rack_net =
                    Ipv6Subnet::<RACK_PREFIX>::new(info.underlay_address)
                        .net()
                        .to_string();

                let dns_install_service = Self::dns_install(
                    info,
                    Some(dns_servers.to_vec()),
                    domain.as_deref(),
                )
                .await?;

                let mut chrony_config = PropertyGroupBuilder::new("config")
                    .add_property("allow", "astring", &rack_net)
                    .add_property("boundary", "boolean", "true")
                    .add_property(
                        "boundary_pool",
                        "astring",
                        format!("{BOUNDARY_NTP_DNS_NAME}.{DNS_ZONE}"),
                    );

                for s in ntp_servers {
                    chrony_config =
                        chrony_config.add_property("server", "astring", s);
                }

                let dns_client_service;
                if dns_servers.is_empty() {
                    dns_client_service = disabled_dns_client_service;
                } else {
                    dns_client_service = enabled_dns_client_service;
                }

                let ntp_service = ServiceBuilder::new("oxide/ntp")
                    .add_instance(ServiceInstanceBuilder::new("default"));

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let ntp_admin_address = {
                    let mut address = *address;
                    address.set_port(NTP_ADMIN_PORT);
                    address
                };
                let ntp_admin_config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "address",
                        "astring",
                        ntp_admin_address.to_string(),
                    );
                let ntp_admin_service = ServiceBuilder::new("oxide/ntp-admin")
                    .add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(ntp_admin_config),
                    );

                let chrony_setup_service =
                    ServiceBuilder::new("oxide/chrony-setup").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(chrony_config),
                    );

                let opte_interface_setup =
                    Self::opte_interface_set_up_install(&installed_zone)?;

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(chrony_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(dns_install_service)
                    .add_service(dns_client_service)
                    .add_service(ntp_service)
                    .add_service(ntp_admin_service)
                    .add_service(opte_interface_setup);

                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to set up NTP profile", err)
                    })?;

                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                zone_type: OmicronZoneType::InternalNtp { address },
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*address.ip()],
                )?;

                let rack_net =
                    Ipv6Subnet::<RACK_PREFIX>::new(info.underlay_address)
                        .net()
                        .to_string();

                let dns_install_service =
                    Self::dns_install(info, None, None).await?;

                let chrony_config = PropertyGroupBuilder::new("config")
                    .add_property("allow", "astring", &rack_net)
                    .add_property("boundary", "boolean", "false")
                    .add_property(
                        "boundary_pool",
                        "astring",
                        format!("{BOUNDARY_NTP_DNS_NAME}.{DNS_ZONE}"),
                    );

                let ntp_service = ServiceBuilder::new("oxide/ntp")
                    .add_instance(ServiceInstanceBuilder::new("default"));

                // We shouldn't need to hardcode a port here:
                // https://github.com/oxidecomputer/omicron/issues/6796
                let ntp_admin_address = {
                    let mut address = *address;
                    address.set_port(NTP_ADMIN_PORT);
                    address
                };
                let ntp_admin_config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "address",
                        "astring",
                        ntp_admin_address.to_string(),
                    );
                let ntp_admin_service = ServiceBuilder::new("oxide/ntp-admin")
                    .add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(ntp_admin_config),
                    );

                let chrony_setup_service =
                    ServiceBuilder::new("oxide/chrony-setup").add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(chrony_config),
                    );

                let profile = ProfileBuilder::new("omicron")
                    .add_service(nw_setup_service)
                    .add_service(chrony_setup_service)
                    .add_service(disabled_ssh_service)
                    .add_service(dns_install_service)
                    .add_service(enabled_dns_client_service)
                    .add_service(ntp_service)
                    .add_service(ntp_admin_service);

                profile
                    .add_to_zone(&self.inner.log, &installed_zone)
                    .await
                    .map_err(|err| {
                        Error::io("Failed to set up NTP profile", err)
                    })?;

                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                zone_type:
                    OmicronZoneType::InternalDns {
                        http_address,
                        dns_address,
                        gz_address,
                        gz_address_index,
                        ..
                    },
                ..
            } => {
                let underlay_ips = if http_address.ip() == dns_address.ip() {
                    vec![*http_address.ip()]
                } else {
                    vec![*http_address.ip(), *dns_address.ip()]
                };
                let nw_setup_service = Self::zone_network_setup_install(
                    Some(gz_address),
                    &installed_zone,
                    &underlay_ips,
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
                let addr_name = internal_dns_addrobj_name(*gz_address_index);
                Zones::ensure_has_global_zone_v6_address(
                    self.inner.underlay_vnic.clone(),
                    *gz_address,
                    &addr_name,
                )
                .await
                .map_err(|err| Error::GzAddress {
                    message: format!(
                        "Failed to create address {} for Internal DNS zone",
                        addr_name
                    ),
                    err,
                })?;

                // Tell DDM to start advertising our address. This may be
                // premature: we haven't yet started the internal DNS server.
                // However, the existence of the `internaldns{gz_address_index}`
                // interface we just created means processes in the gz
                // (including ourselves!) may use that as a _source_ address, so
                // we need to go ahead and advertise this prefix so other sleds
                // know how to route responses back to us. See
                // <https://github.com/oxidecomputer/omicron/issues/7782>.
                self.ddm_reconciler()
                    .add_internal_dns_subnet(Ipv6Subnet::new(*gz_address));

                let internal_dns_config = PropertyGroupBuilder::new("config")
                    .add_property(
                        "http_address",
                        "astring",
                        http_address.to_string(),
                    )
                    .add_property(
                        "dns_address",
                        "astring",
                        dns_address.to_string(),
                    );
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
                RunningZone::boot(installed_zone).await?
            }
            OmicronZoneConfig {
                zone_type:
                    OmicronZoneType::Nexus {
                        internal_address,
                        lockstep_port,
                        external_tls,
                        external_dns_servers,
                        ..
                    },
                id,
                ..
            } => {
                let Some(info) = self.inner.sled_info.get() else {
                    return Err(Error::SledAgentNotReady);
                };

                let nw_setup_service = Self::zone_network_setup_install(
                    Some(&info.underlay_address),
                    &installed_zone,
                    &[*internal_address.ip()],
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
                let opte_iface_name = port.name();

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
                            default_request_body_max_bytes: 1048576,
                            default_handler_task_mode:
                                HandlerTaskMode::Detached,
                            log_headers: vec![],
                        },
                    },
                    dropshot_internal: dropshot::ConfigDropshot {
                        bind_address: (*internal_address).into(),
                        default_request_body_max_bytes: 1048576,
                        default_handler_task_mode: HandlerTaskMode::Detached,
                        log_headers: vec![],
                    },
                    dropshot_lockstep: dropshot::ConfigDropshot {
                        bind_address: SocketAddr::new(
                            (*internal_address.ip()).into(),
                            *lockstep_port,
                        ),
                        default_request_body_max_bytes: 1048576,
                        default_handler_task_mode: HandlerTaskMode::Detached,
                        log_headers: vec![],
                    },
                    internal_dns: nexus_config::InternalDns::FromSubnet {
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            info.underlay_address,
                        ),
                    },
                    database: nexus_config::Database::FromDns,
                    external_dns_servers: external_dns_servers.clone(),
                    // TCP connections bound by HTTP clients of external services
                    // should always be bound on our OPTE interface.
                    external_http_clients:
                        nexus_config::ExternalHttpClientConfig {
                            interface: Some(opte_iface_name.to_string()),
                        },
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
                file.flush()
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
                RunningZone::boot(installed_zone).await?
            }
        };

        Ok(running_zone)
    }

    async fn boot_switch_zone(
        &self,
        config: &SwitchZoneConfig,
        installed_zone: InstalledZone,
        links_need_link_local: &[bool],
        bootstrap_name_and_address: Option<(String, Ipv6Addr)>,
        device_names: &[String],
    ) -> Result<RunningZone, Error> {
        let SwitchZoneConfig { id, services, addresses } = config;

        let disabled_dns_client_service =
            ServiceBuilder::new("network/dns/client")
                .add_instance(ServiceInstanceBuilder::new("default").disable());

        let info = self.inner.sled_info.get();

        let gw_addr = match info {
            Some(i) => Some(&i.underlay_address),
            None => None,
        };

        let nw_setup_service = Self::zone_network_setup_install(
            gw_addr,
            &installed_zone,
            addresses,
        )?;

        let sidecar_revision = match &self.inner.sidecar_revision {
            SidecarRevision::Physical(rev) => rev.to_string(),
            SidecarRevision::SoftZone(rev)
            | SidecarRevision::SoftPropolis(rev) => format!(
                "softnpu_front_{}_rear_{}",
                rev.front_port_count, rev.rear_port_count
            ),
        };

        // Define all services in the switch zone
        let mut mgs_service = ServiceBuilder::new("oxide/mgs");
        let mut wicketd_service = ServiceBuilder::new("oxide/wicketd");
        let mut switch_zone_setup_service =
            ServiceBuilder::new("oxide/switch_zone_setup");
        let mut dendrite_service = ServiceBuilder::new("oxide/dendrite");
        let mut tfport_service = ServiceBuilder::new("oxide/tfport");
        let mut lldpd_service = ServiceBuilder::new("oxide/lldpd");
        let mut pumpkind_service = ServiceBuilder::new("oxide/pumpkind");
        let mut mgd_service = ServiceBuilder::new("oxide/mgd");
        let mut mg_ddm_service = ServiceBuilder::new("oxide/mg-ddm");
        let mut uplink_service = ServiceBuilder::new("oxide/uplink");

        let mut switch_zone_setup_config = PropertyGroupBuilder::new("config")
            .add_property(
                "gz_local_link_addr",
                "astring",
                &format!(
                    "{}",
                    self.inner.global_zone_bootstrap_link_local_address
                ),
            );

        for (link, needs_link_local) in
            installed_zone.links().iter().zip(links_need_link_local)
        {
            if *needs_link_local {
                switch_zone_setup_config = switch_zone_setup_config
                    .add_property("link_local_links", "astring", link.name());
            }
        }

        if let Some((bootstrap_name, bootstrap_address)) =
            bootstrap_name_and_address.as_ref()
        {
            switch_zone_setup_config = switch_zone_setup_config
                .add_property("link_local_links", "astring", bootstrap_name)
                .add_property(
                    "bootstrap_addr",
                    "astring",
                    &format!("{bootstrap_address}"),
                )
                .add_property("bootstrap_vnic", "astring", bootstrap_name);
        }

        // Set properties for each service
        for service in services {
            match service {
                SwitchService::ManagementGatewayService => {
                    info!(self.inner.log, "Setting up MGS service");
                    let mut mgs_config = PropertyGroupBuilder::new("config")
                        // Always tell MGS to listen on localhost so wicketd
                        // can contact it even before we have an underlay
                        // network.
                        .add_property(
                            "address",
                            "astring",
                            &format!("[::1]:{MGS_PORT}"),
                        )
                        .add_property("id", "astring", &id.to_string());

                    if let Some(i) = info {
                        mgs_config = mgs_config.add_property(
                            "rack_id",
                            "astring",
                            &i.rack_id.to_string(),
                        );
                    }

                    if let Some(address) = addresses.get(0) {
                        // Don't use localhost twice
                        if *address != Ipv6Addr::LOCALHOST {
                            mgs_config = mgs_config.add_property(
                                "address",
                                "astring",
                                &format!("[{address}]:{MGS_PORT}"),
                            );
                        }
                    }
                    mgs_service = mgs_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(mgs_config),
                    );
                }
                SwitchService::SpSim => {
                    info!(self.inner.log, "Setting up Simulated SP service");
                }
                SwitchService::Wicketd { baseboard } => {
                    info!(self.inner.log, "Setting up wicketd service");
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

                    let mut wicketd_config = PropertyGroupBuilder::new(
                        "config",
                    )
                    .add_property(
                        "address",
                        "astring",
                        &format!("[::1]:{WICKETD_PORT}"),
                    )
                    .add_property(
                        "artifact-address",
                        "astring",
                        &format!(
                            "[{bootstrap_address}]:{BOOTSTRAP_ARTIFACT_PORT}"
                        ),
                    )
                    .add_property(
                        "baseboard-file",
                        "astring",
                        SWITCH_ZONE_BASEBOARD_FILE,
                    )
                    .add_property(
                        "mgs-address",
                        "astring",
                        &format!("[::1]:{MGS_PORT}"),
                    )
                    // We intentionally bind `nexus-proxy-address` to
                    // `::` so wicketd will serve this on all
                    // interfaces, particularly the tech port
                    // interfaces, allowing external clients to connect
                    // to this Nexus proxy.
                    .add_property(
                        "nexus-proxy-address",
                        "astring",
                        &format!("[::]:{WICKETD_NEXUS_PROXY_PORT}"),
                    );

                    if let Some(i) = info {
                        let rack_subnet =
                            Ipv6Subnet::<AZ_PREFIX>::new(i.underlay_address);

                        wicketd_config = wicketd_config.add_property(
                            "rack-subnet",
                            "astring",
                            &rack_subnet.net().addr().to_string(),
                        );
                    }

                    wicketd_service = wicketd_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(wicketd_config),
                    );

                    let baseboard_info = serde_json::to_string(&baseboard)?;

                    switch_zone_setup_config =
                        switch_zone_setup_config.clone().add_property(
                            "baseboard_info",
                            "astring",
                            &baseboard_info,
                        );
                }
                SwitchService::Dendrite { asic } => {
                    info!(self.inner.log, "Setting up dendrite service");
                    let mut dendrite_config =
                        PropertyGroupBuilder::new("config");

                    if let Some(i) = info {
                        dendrite_config =
                            add_sled_ident_properties(dendrite_config, i)
                    };

                    for address in addresses {
                        dendrite_config = dendrite_config.add_property(
                            "address",
                            "astring",
                            &format!("[{}]:{}", address, DENDRITE_PORT),
                        );
                        if *address != Ipv6Addr::LOCALHOST {
                            let az_prefix =
                                Ipv6Subnet::<AZ_PREFIX>::new(*address);
                            for addr in Resolver::servers_from_subnet(az_prefix)
                            {
                                dendrite_config = dendrite_config.add_property(
                                    "dns_server",
                                    "astring",
                                    &format!("{addr}"),
                                );
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
                                dendrite_config = dendrite_config.add_property(
                                    "dev_path",
                                    "astring",
                                    &device_names[0].clone(),
                                );
                            } else {
                                return Err(Error::SwitchZone(
                                    anyhow::anyhow!(
                                        "{dev_cnt} devices needed \
                                                    for tofino asic"
                                    ),
                                ));
                            }
                            dendrite_config = dendrite_config
                                        .add_property(
                                            "port_config",
                                            "astring",
                                            "/opt/oxide/dendrite/misc/sidecar_config.toml",
                                        )
                                        .add_property("board_rev", "astring", &sidecar_revision);
                        }
                        DendriteAsic::TofinoStub => {
                            dendrite_config = dendrite_config.add_property(
                                "port_config",
                                "astring",
                                "/opt/oxide/dendrite/misc/model_config.toml",
                            );
                        }
                        asic @ (DendriteAsic::SoftNpuZone
                        | DendriteAsic::SoftNpuPropolisDevice) => {
                            let s = match self.inner.sidecar_revision {
                                SidecarRevision::SoftZone(ref s) => s,
                                SidecarRevision::SoftPropolis(ref s) => s,
                                _ => {
                                    return Err(Error::SidecarRevision(
                                        anyhow::anyhow!(
                                            "expected soft sidecar \
                                                    revision"
                                        ),
                                    ));
                                }
                            };

                            dendrite_config = dendrite_config
                                        .add_property(
                                            "front_ports",
                                            "astring",
                                            &s.front_port_count.to_string(),
                                        )
                                        .add_property(
                                            "rear_ports",
                                            "astring",
                                            &s.rear_port_count.to_string(),
                                        )
                                        .add_property(
                                            "port_config",
                                            "astring",
                                            "/opt/oxide/dendrite/misc/softnpu_single_sled_config.toml",
                                        );

                            if asic == &DendriteAsic::SoftNpuZone {
                                dendrite_config = dendrite_config
                                    .add_property("mgmt", "astring", "uds")
                                    .add_property(
                                        "uds_path",
                                        "astring",
                                        "/opt/softnpu/stuff",
                                    );
                            }

                            if asic == &DendriteAsic::SoftNpuPropolisDevice {
                                dendrite_config = dendrite_config
                                    .add_property("mgmt", "astring", "uart");
                            }
                        }
                    }

                    dendrite_service = dendrite_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(dendrite_config),
                    );
                }
                SwitchService::Tfport { pkt_source, asic } => {
                    info!(self.inner.log, "Setting up tfport service");

                    let mut tfport_config = PropertyGroupBuilder::new("config");

                    tfport_config = tfport_config
                        .add_property(
                            "dpd_host",
                            "astring",
                            &format!("[{}]", Ipv6Addr::LOCALHOST),
                        )
                        .add_property(
                            "dpd_port",
                            "astring",
                            &format!("{}", DENDRITE_PORT),
                        );

                    if let Some(i) = info {
                        tfport_config =
                            add_sled_ident_properties(tfport_config, i);
                    }

                    for address in addresses {
                        tfport_config = tfport_config.add_property(
                            "listen_address",
                            "astring",
                            &format!("[{}]:{}", address, TFPORTD_PORT),
                        );
                    }

                    let is_oxide_sled = is_oxide_sled().map_err(|e| {
                        Error::Underlay(underlay::Error::SystemDetection(e))
                    })?;

                    if is_oxide_sled {
                        // Collect the prefixes for each techport.
                        let nameaddr = bootstrap_name_and_address.as_ref();
                        let techport_prefixes = match nameaddr {
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
                            // Each `prefix` is an `Ipv6Subnet`
                            // including a netmask.  Stringify just the
                            // network address, without the mask.
                            tfport_config = tfport_config.add_property(
                                &format!("techport{i}_prefix"),
                                "astring",
                                prefix.net().addr().to_string(),
                            )
                        }
                    };

                    if is_oxide_sled
                        || asic == &DendriteAsic::SoftNpuPropolisDevice
                        || asic == &DendriteAsic::TofinoAsic
                    {
                        tfport_config = tfport_config.add_property(
                            "pkt_source",
                            "astring",
                            pkt_source,
                        );
                    };

                    if asic == &DendriteAsic::SoftNpuZone {
                        tfport_config = tfport_config.add_property(
                            "flags",
                            "astring",
                            "--sync-only",
                        );
                    }

                    tfport_service = tfport_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(tfport_config),
                    );
                }
                SwitchService::Lldpd { baseboard } => {
                    info!(self.inner.log, "Setting up lldpd service");

                    let mut lldpd_config = PropertyGroupBuilder::new("config")
                        .add_property(
                            "board_rev",
                            "astring",
                            &sidecar_revision,
                        );

                    match baseboard {
                        Baseboard::Gimlet { identifier, model, .. }
                        | Baseboard::Pc { identifier, model, .. } => {
                            lldpd_config = lldpd_config
                                .add_property(
                                    "scrimlet_id",
                                    "astring",
                                    identifier,
                                )
                                .add_property(
                                    "scrimlet_model",
                                    "astring",
                                    model,
                                );
                        }
                        Baseboard::Unknown => {}
                    }

                    for address in addresses {
                        lldpd_config = lldpd_config.add_property(
                            "address",
                            "astring",
                            &format!("[{}]:{}", address, LLDP_PORT),
                        );
                    }

                    lldpd_service = lldpd_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(lldpd_config),
                    );
                }
                SwitchService::Pumpkind { asic } => {
                    // The pumpkin daemon is only needed when running on
                    // with real sidecar.
                    if asic == &DendriteAsic::TofinoAsic {
                        info!(self.inner.log, "Setting up pumpkind service");
                        let pumpkind_config =
                            PropertyGroupBuilder::new("config")
                                .add_property("mode", "astring", "switch");

                        pumpkind_service = pumpkind_service.add_instance(
                            ServiceInstanceBuilder::new("default")
                                .add_property_group(pumpkind_config),
                        );
                    } else {
                        pumpkind_service = pumpkind_service.add_instance(
                            ServiceInstanceBuilder::new("default").disable(),
                        );
                    }
                }
                SwitchService::Uplink => {
                    // Nothing to do here - this service is special and
                    // configured in
                    // `ensure_switch_zone_uplinks_configured`
                    uplink_service = uplink_service
                        .add_instance(ServiceInstanceBuilder::new("default"));
                }
                SwitchService::Mgd => {
                    info!(self.inner.log, "Setting up mgd service");

                    let mut mgd_config = PropertyGroupBuilder::new("config");

                    if let Some(i) = info {
                        mgd_config = mgd_config
                            .add_property(
                                "sled_uuid",
                                "astring",
                                &i.config.sled_identifiers.sled_id.to_string(),
                            )
                            .add_property(
                                "rack_uuid",
                                "astring",
                                &i.rack_id.to_string(),
                            );
                    }

                    for address in addresses {
                        if *address != Ipv6Addr::LOCALHOST {
                            let az_prefix =
                                Ipv6Subnet::<AZ_PREFIX>::new(*address);
                            for addr in Resolver::servers_from_subnet(az_prefix)
                            {
                                mgd_config = mgd_config.add_property(
                                    "dns_servers",
                                    "astring",
                                    &format!("{addr}"),
                                );
                            }
                            break;
                        }
                    }

                    mgd_service = mgd_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(mgd_config),
                    );
                }
                SwitchService::MgDdm { mode } => {
                    info!(self.inner.log, "Setting up mg-ddm service");

                    let mut mg_ddm_config = PropertyGroupBuilder::new("config")
                        .add_property("mode", "astring", mode)
                        .add_property("dendrite", "astring", "true");

                    if let Some(i) = info {
                        mg_ddm_config = mg_ddm_config
                            .add_property(
                                "sled_uuid",
                                "astring",
                                &i.config.sled_identifiers.sled_id.to_string(),
                            )
                            .add_property(
                                "rack_uuid",
                                "astring",
                                &i.rack_id.to_string(),
                            );
                    }

                    for address in addresses {
                        if *address != Ipv6Addr::LOCALHOST {
                            let az_prefix =
                                Ipv6Subnet::<AZ_PREFIX>::new(*address);
                            for addr in Resolver::servers_from_subnet(az_prefix)
                            {
                                mg_ddm_config = mg_ddm_config.add_property(
                                    "dns_servers",
                                    "astring",
                                    &format!("{addr}"),
                                );
                            }
                            break;
                        }
                    }

                    let is_oxide_sled = is_oxide_sled().map_err(|e| {
                        Error::Underlay(underlay::Error::SystemDetection(e))
                    })?;

                    let maghemite_interfaces: Vec<AddrObject> = if is_oxide_sled
                    {
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
                                    IPV6_LINK_LOCAL_ADDROBJ_NAME,
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
                                    IPV6_LINK_LOCAL_ADDROBJ_NAME,
                                )
                                .unwrap()
                            })
                            .collect()
                    };

                    for i in maghemite_interfaces {
                        mg_ddm_config = mg_ddm_config.add_property(
                            "interfaces",
                            "astring",
                            &i.to_string(),
                        );
                    }

                    if is_oxide_sled {
                        mg_ddm_config = mg_ddm_config
                            .add_property("dpd_host", "astring", "[::1]")
                            .add_property(
                                "dpd_port",
                                "astring",
                                &DENDRITE_PORT.to_string(),
                            )
                    }

                    mg_ddm_service = mg_ddm_service.add_instance(
                        ServiceInstanceBuilder::new("default")
                            .add_property_group(mg_ddm_config),
                    );
                }
            }
        }

        switch_zone_setup_service = switch_zone_setup_service.add_instance(
            ServiceInstanceBuilder::new("default")
                .add_property_group(switch_zone_setup_config),
        );

        let profile = ProfileBuilder::new("omicron")
            .add_service(nw_setup_service)
            .add_service(disabled_dns_client_service)
            .add_service(mgs_service)
            .add_service(wicketd_service)
            .add_service(switch_zone_setup_service)
            .add_service(dendrite_service)
            .add_service(tfport_service)
            .add_service(lldpd_service)
            .add_service(pumpkind_service)
            .add_service(mgd_service)
            .add_service(mg_ddm_service)
            .add_service(uplink_service);
        profile.add_to_zone(&self.inner.log, &installed_zone).await.map_err(
            |err| Error::io("Failed to setup Switch zone profile", err),
        )?;
        Ok(RunningZone::boot(installed_zone).await?)
    }

    // Attempt to start a single Omicron zone.
    //
    // This method is NOT idempotent.
    //
    // Callers must do any "pre-zone-start" validation, including:
    //
    // * No other zone of this same name is still running
    // * Time is synchronized, if the zone requires it
    // * Any datasets the zone depends on exist and have been configured and/or
    //   mounted appropriately
    pub(crate) async fn start_omicron_zone(
        &self,
        zone: PreparedOmicronZone<'_>,
        zone_root_path: PathInPool,
    ) -> Result<RunningZone, Error> {
        let runtime = self
            .initialize_zone(
                ZoneArgs::Omicron(zone),
                zone_root_path,
                // filesystems=
                &[],
                // data_links=
                &[],
            )
            .await?;

        Ok(runtime)
    }

    /// Adjust the system boot time to the latest boot time of all zones.
    fn boottime_rewrite(&self) {
        // Call out to the 'tmpx' utility program which will rewrite the wtmpx
        // and utmpx databases in every zone, including the global zone, to
        // reflect the adjusted system boot time.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&["/usr/platform/oxide/bin/tmpx", "-Z"]);
        if let Err(e) = execute(cmd) {
            warn!(self.inner.log, "Updating [wu]tmpx databases failed: {}", e);
        }
    }

    /// Check if the synchronization state of the sled has shifted to true and
    /// if so, execute the any out-of-band actions that need to be taken.
    ///
    /// This function only executes the out-of-band actions once, once the
    /// synchronization state has shifted to true.
    pub(crate) fn on_time_sync(&self) {
        if self
            .inner
            .time_synced
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            info!(self.inner.log, "Time is now synchronized");
            // We only want to rewrite the boot time once, so we do it here
            // when we know the time is synchronized.
            self.boottime_rewrite();

            // We expect to have a metrics queue by this point, so
            // we can safely send a message on it to say the sled has
            // been synchronized.
            //
            // We may want to retry or ensure this notification happens. See
            // https://github.com/oxidecomputer/omicron/issues/8022.
            let queue = self.metrics_queue();
            match queue.notify_time_synced_sled(self.sled_id()) {
                Ok(_) => info!(
                    self.inner.log,
                    "Notified metrics task that time is now synced",
                ),
                Err(e) => warn!(
                    self.inner.log,
                    "Failed to notify metrics task that \
                     time is now synced, metrics may not be produced.";
                     "error" => InlineErrorChain::new(&e),
                ),
            }
        } else {
            debug!(self.inner.log, "Time was already synchronized");
        }
    }

    /// Ensures that a switch zone exists with the provided IP adddress.
    pub async fn activate_switch(
        &self,
        // If we're reconfiguring the switch zone with an underlay address, we
        // also need the rack network config to set tfport uplinks.
        underlay_info: Option<UnderlayInfo>,
        baseboard: Baseboard,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ensuring scrimlet services (enabling services)");
        let mut filesystems: Vec<zone::Fs> = vec![];
        let mut data_links: Vec<String> = vec![];

        let services = match self.inner.sled_mode {
            // A sled that is not a scrimlet should not try to activate a
            // switch zone.
            SledMode::Sled => {
                return Err(Error::SwitchZone(anyhow::anyhow!(
                    "attempted to activate switch zone on non-scrimlet sled"
                )));
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
                    data_links = Dladm::get_simulated_tfports().await?;
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

        let mut addresses = if let Some(info) = &underlay_info {
            vec![info.ip]
        } else {
            vec![]
        };
        addresses.push(Ipv6Addr::LOCALHOST);

        let request =
            SwitchZoneConfig { id: Uuid::new_v4(), addresses, services };

        self.ensure_switch_zone(
            Some(request),
            filesystems,
            data_links,
            underlay_info,
        )
        .await?;

        Ok(())
    }

    // Retry ensuring switch zone uplinks until success or we're told to stop.
    //
    // TODO-correctness: This is not in great shape, and may get stuck in an
    // infinite retry loop _within_ one attempt, or may succeed even if it
    // didn't fully configure all switch zone services. See
    // <https://github.com/oxidecomputer/omicron/issues/8970> for details.
    async fn ensure_switch_zone_uplinks_configured_loop(
        &self,
        underlay_info: &UnderlayInfo,
        mut exit_rx: oneshot::Receiver<()>,
    ) {
        // We don't really expect failures trying to initialize the switch zone
        // unless something is unhealthy. This timeout is somewhat arbitrary,
        // but we probably don't want to use backoff here.
        const RETRY_DELAY: Duration = Duration::from_secs(1);

        // We can only ensure uplinks if we have a rack network config.
        //
        // It'd be surprising to have `underlay_info` containing our underlay IP
        // without a rack network config, but it is technically possible. These
        // bits of information are ledgered separately, and we could have
        // ledgered our underlay IP, then crashed before the bootstore gossip
        // happened to tell us the rack network config, and are now restarting.
        let Some(rack_network_config) = &underlay_info.rack_network_config
        else {
            return;
        };

        loop {
            match self
                .ensure_switch_zone_uplinks_configured(
                    underlay_info.ip,
                    &rack_network_config,
                )
                .await
            {
                Ok(()) => {
                    info!(self.inner.log, "configured switch zone uplinks");
                    break;
                }
                Err(e) => {
                    warn!(
                        self.inner.log,
                        "Failed to configure switch zone uplinks";
                        InlineErrorChain::new(&e),
                    );
                }
            }

            tokio::select! {
                // If we've been told to stop trying, bail.
                _ = &mut exit_rx => {
                    info!(
                        self.inner.log,
                        "instructed to give up on switch zone uplink \
                         configuration",
                    );
                    return;
                }

                _ = tokio::time::sleep(RETRY_DELAY) => {
                    info!(
                        self.inner.log,
                        "retrying switch zone uplink configuration",
                    );
                    continue;
                }
            };
        }
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
        // Helper function to add a property-value pair
        // if the config actually has a value set.
        fn apv(
            smfh: &SmfHelper,
            prop: &str,
            val: &Option<String>,
        ) -> Result<(), Error> {
            if let Some(v) = val {
                smfh.addpropvalue_type(prop, v, "astring")?
            }
            Ok(())
        }

        // We expect the switch zone to be running, as we're called immediately
        // after `ensure_zone()` above and we just successfully configured
        // uplinks via DPD running in our switch zone. If somehow we're in any
        // other state, bail out.
        let mut switch_zone = self.inner.switch_zone.lock().await;

        let zone = match &mut *switch_zone {
            SwitchZoneState::Running { zone, .. } => zone,
            SwitchZoneState::Disabled => {
                return Err(Error::SwitchZone(anyhow!(
                    "Cannot configure switch zone uplinks: \
                     switch zone disabled"
                )));
            }
            SwitchZoneState::Initializing { .. } => {
                return Err(Error::SwitchZone(anyhow!(
                    "Cannot configure switch zone uplinks: \
                     switch zone still initializing"
                )));
            }
        };

        info!(self.inner.log, "ensuring scrimlet uplinks");
        let usmfh = SmfHelper::new(&zone, &SwitchService::Uplink);
        let lsmfh = SmfHelper::new(
            &zone,
            &SwitchService::Lldpd { baseboard: Baseboard::Unknown },
        );

        // We want to delete all the properties in the `uplinks` group, but we
        // don't know their names, so instead we'll delete and recreate the
        // group, then add all our properties.
        let _ = usmfh.delpropgroup("uplinks");
        usmfh.addpropgroup("uplinks", "application")?;

        for port_config in &our_ports {
            for addr in &port_config.addrs {
                usmfh.addpropvalue_type(
                    &format!("uplinks/{}_0", port_config.port,),
                    &addr.to_string(),
                    "astring",
                )?;
            }

            if let Some(lldp_config) = &port_config.lldp {
                let group_name = format!("port_{}", port_config.port);
                info!(self.inner.log, "setting up {group_name}");
                let _ = lsmfh.delpropgroup(&group_name);
                lsmfh.addpropgroup(&group_name, "application")?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/status"),
                    &Some(lldp_config.status.to_string()),
                )?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/chassis_id"),
                    &lldp_config.chassis_id,
                )?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/system_name"),
                    &lldp_config.system_name,
                )?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/system_description"),
                    &lldp_config.system_description,
                )?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/port_description"),
                    &lldp_config.port_description,
                )?;
                apv(
                    &lsmfh,
                    &format!("{group_name}/port_id"),
                    &lldp_config.port_id,
                )?;
                if let Some(a) = &lldp_config.management_addrs {
                    for address in a {
                        apv(
                            &lsmfh,
                            &format!("{group_name}/management_addrs"),
                            &Some(address.to_string()),
                        )?;
                    }
                }
            }
        }
        usmfh.refresh()?;
        lsmfh.refresh()?;

        Ok(())
    }

    /// Ensures that no switch zone is active.
    pub async fn deactivate_switch(&self) -> Result<(), Error> {
        self.ensure_switch_zone(
            // request=
            None,
            // filesystems=
            vec![],
            // data_links=
            vec![],
            // underlay_info=
            None,
        )
        .await
    }

    /// Returns a reference to the zone image resolver.
    pub(crate) fn zone_image_resolver(&self) -> &ZoneImageSourceResolver {
        &self.inner.zone_image_resolver
    }

    // Forcefully initialize a sled-local switch zone.
    //
    // This is a helper function for "ensure_switch_zone".
    fn start_switch_zone(
        self,
        zone: &mut SwitchZoneState,
        request: SwitchZoneConfig,
        filesystems: Vec<zone::Fs>,
        data_links: Vec<String>,
        underlay_info: Option<UnderlayInfo>,
    ) {
        let (exit_tx, exit_rx) = oneshot::channel();
        *zone = SwitchZoneState::Initializing {
            request,
            filesystems,
            data_links,
            worker: Some(Task {
                exit_tx,
                initializer: tokio::task::spawn(async move {
                    self.initialize_switch_zone_loop(underlay_info, exit_rx)
                        .await
                }),
            }),
        };
    }

    // Moves the current state to align with the "request".
    async fn ensure_switch_zone(
        &self,
        request: Option<SwitchZoneConfig>,
        filesystems: Vec<zone::Fs>,
        data_links: Vec<String>,
        underlay_info: Option<UnderlayInfo>,
    ) -> Result<(), Error> {
        let log = &self.inner.log;

        let mut sled_zone = self.inner.switch_zone.lock().await;
        let zone_typestr = "switch";

        match (&mut *sled_zone, request) {
            (SwitchZoneState::Disabled, Some(request)) => {
                info!(log, "Enabling {zone_typestr} zone (new)");
                self.clone().start_switch_zone(
                    &mut sled_zone,
                    request,
                    filesystems,
                    data_links,
                    underlay_info,
                );
            }
            (
                SwitchZoneState::Initializing { request, .. },
                Some(new_request),
            ) => {
                info!(log, "Enabling {zone_typestr} zone (already underway)");
                // The zone has not started yet -- we can simply replace
                // the next request with our new request.
                *request = new_request;
            }
            (
                SwitchZoneState::Running { request, zone, worker },
                Some(new_request),
            ) if request.addresses != new_request.addresses => {
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

                // When the request addresses have changed this means the
                // underlay is available now as well.
                if let Some(info) = self.inner.sled_info.get() {
                    info!(
                        self.inner.log,
                        "Ensuring there is a default route";
                        "gateway" => ?info.underlay_address,
                    );
                    match zone.add_default_route(info.underlay_address).map_err(
                        |err| Error::ZoneCommand {
                            intent: "Adding Route".to_string(),
                            err,
                        },
                    ) {
                        Ok(_) => (),
                        Err(e) => {
                            if e.to_string().contains("entry exists") {
                                info!(
                                    self.inner.log,
                                    "Default route already exists";
                                    "gateway" => ?info.underlay_address,
                                )
                            } else {
                                return Err(e);
                            }
                        }
                    };
                }

                for service in &request.services {
                    let smfh = SmfHelper::new(&zone, service);

                    match service {
                        SwitchService::ManagementGatewayService => {
                            info!(self.inner.log, "configuring MGS service");
                            // Remove any existing `config/address` values
                            // without deleting the property itself.
                            smfh.delpropvalue_default_instance(
                                "config/address",
                                "*",
                            )?;

                            // Restore the localhost address that we always add
                            // when setting up MGS.
                            smfh.addpropvalue_type_default_instance(
                                "config/address",
                                &format!("[::1]:{MGS_PORT}"),
                                "astring",
                            )?;

                            // Add the underlay address.
                            smfh.addpropvalue_type_default_instance(
                                "config/address",
                                &format!("[{address}]:{MGS_PORT}"),
                                "astring",
                            )?;

                            // It should be impossible for the `sled_info` not
                            // to be set here, as the underlay is set at the
                            // same time.
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop_default_instance(
                                    "config/rack_id",
                                    info.rack_id,
                                )?;
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
                            info!(
                                self.inner.log,
                                "refreshed MGS service with new configuration"
                            )
                        }
                        SwitchService::Dendrite { .. } => {
                            info!(
                                self.inner.log,
                                "configuring dendrite service"
                            );
                            if let Some(info) = self.inner.sled_info.get() {
                                setprop_sled_ident_properties(&smfh, info)?;
                            } else {
                                info!(
                                    self.inner.log,
                                    "no sled info available yet"
                                );
                            }
                            smfh.delpropvalue_default_instance(
                                "config/address",
                                "*",
                            )?;
                            smfh.delpropvalue_default_instance(
                                "config/dns_server",
                                "*",
                            )?;
                            for address in &request.addresses {
                                smfh.addpropvalue_type_default_instance(
                                    "config/address",
                                    &format!("[{}]:{}", address, DENDRITE_PORT),
                                    "astring",
                                )?;
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue_type_default_instance(
                                            "config/dns_server",
                                            &format!("{addr}"),
                                            "astring",
                                        )?;
                                    }
                                }
                            }
                            smfh.refresh()?;
                            info!(
                                self.inner.log,
                                "refreshed dendrite service with new configuration"
                            )
                        }
                        SwitchService::Wicketd { .. } => {
                            if let Some(&address) = first_address {
                                let rack_subnet =
                                    Ipv6Subnet::<AZ_PREFIX>::new(address);

                                info!(
                                    self.inner.log, "configuring wicketd";
                                    "rack_subnet" => %rack_subnet.net().addr(),
                                );

                                smfh.setprop_default_instance(
                                    "config/rack-subnet",
                                    &rack_subnet.net().addr().to_string(),
                                )?;

                                smfh.refresh()?;
                                info!(
                                    self.inner.log,
                                    "refreshed wicketd service with new configuration"
                                )
                            } else {
                                error!(
                                    self.inner.log,
                                    "underlay address unexpectedly missing",
                                );
                            }
                        }
                        SwitchService::Lldpd { .. } => {
                            info!(self.inner.log, "configuring lldp service");
                            smfh.delpropvalue_default_instance(
                                "config/address",
                                "*",
                            )?;
                            for address in &request.addresses {
                                smfh.addpropvalue_type_default_instance(
                                    "config/address",
                                    &format!("[{}]:{}", address, LLDP_PORT),
                                    "astring",
                                )?;
                            }
                            smfh.refresh()?;
                            info!(
                                self.inner.log,
                                "refreshed lldpd service with new configuration"
                            )
                        }
                        SwitchService::Tfport { pkt_source, asic } => {
                            info!(self.inner.log, "configuring tfport service");
                            if let Some(info) = self.inner.sled_info.get() {
                                setprop_sled_ident_properties(&smfh, info)?;
                            } else {
                                info!(
                                    self.inner.log,
                                    "no sled info available yet"
                                );
                            }
                            smfh.delpropvalue_default_instance(
                                "config/listen_address",
                                "*",
                            )?;
                            for address in &request.addresses {
                                smfh.addpropvalue_type_default_instance(
                                    "config/listen_address",
                                    &format!("[{}]:{}", address, TFPORTD_PORT),
                                    "astring",
                                )?;
                            }

                            match asic {
                                DendriteAsic::SoftNpuPropolisDevice
                                | DendriteAsic::TofinoAsic => {
                                    smfh.setprop_default_instance(
                                        "config/pkt_source",
                                        pkt_source,
                                    )?;
                                }
                                _ => {}
                            }

                            smfh.refresh()?;
                            info!(
                                self.inner.log,
                                "refreshed tfport service with new configuration"
                            )
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
                            smfh.delpropvalue_default_instance(
                                "config/dns_servers",
                                "*",
                            )?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop_default_instance(
                                    "config/rack_uuid",
                                    info.rack_id,
                                )?;
                                smfh.setprop_default_instance(
                                    "config/sled_uuid",
                                    info.config.sled_identifiers.sled_id,
                                )?;
                            }
                            for address in &request.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue_type_default_instance(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                            "astring",
                                        )?;
                                    }
                                    break;
                                }
                            }
                            smfh.refresh()?;
                            info!(
                                self.inner.log,
                                "refreshed mgd service with new configuration"
                            )
                        }
                        SwitchService::MgDdm { mode } => {
                            info!(self.inner.log, "configuring mg-ddm service");
                            smfh.delpropvalue_default_instance(
                                "config/mode",
                                "*",
                            )?;
                            smfh.addpropvalue_type_default_instance(
                                "config/mode",
                                &mode,
                                "astring",
                            )?;
                            if let Some(info) = self.inner.sled_info.get() {
                                smfh.setprop_default_instance(
                                    "config/rack_uuid",
                                    info.rack_id,
                                )?;
                                smfh.setprop_default_instance(
                                    "config/sled_uuid",
                                    info.config.sled_identifiers.sled_id,
                                )?;
                            }
                            smfh.delpropvalue_default_instance(
                                "config/dns_servers",
                                "*",
                            )?;
                            for address in &request.addresses {
                                if *address != Ipv6Addr::LOCALHOST {
                                    let az_prefix =
                                        Ipv6Subnet::<AZ_PREFIX>::new(*address);
                                    for addr in
                                        Resolver::servers_from_subnet(az_prefix)
                                    {
                                        smfh.addpropvalue_type_default_instance(
                                            "config/dns_servers",
                                            &format!("{addr}"),
                                            "astring",
                                        )?;
                                    }
                                    break;
                                }
                            }
                            smfh.refresh()?;
                            info!(
                                self.inner.log,
                                "refreshed mg-ddm service with new configuration"
                            )
                        }
                    }
                }

                // We also need to ensure any uplinks are configured. Spawn a
                // task that goes into an infinite retry loop until it succeeds.
                if let Some(underlay_info) = underlay_info {
                    if let Some(old_worker) = worker.take() {
                        old_worker.stop().await;
                    }
                    let me = self.clone();
                    let (exit_tx, exit_rx) = oneshot::channel();
                    *worker = Some(Task {
                        exit_tx,
                        initializer: tokio::task::spawn(async move {
                            me.ensure_switch_zone_uplinks_configured_loop(
                                &underlay_info,
                                exit_rx,
                            )
                            .await;
                        }),
                    });
                }
            }
            (SwitchZoneState::Running { .. }, Some(_)) => {
                info!(log, "Enabling {zone_typestr} zone (already complete)");
            }
            (SwitchZoneState::Disabled, None) => {
                info!(log, "Disabling {zone_typestr} zone (already complete)");
            }
            (SwitchZoneState::Initializing { worker, .. }, None) => {
                info!(log, "Disabling {zone_typestr} zone (was initializing)");
                worker.take().unwrap().stop().await;
                *sled_zone = SwitchZoneState::Disabled;
            }
            (SwitchZoneState::Running { zone, .. }, None) => {
                info!(log, "Disabling {zone_typestr} zone (was running)");

                // Notify the sled-agent's metrics task to stop collecting from
                // the VNICs in the zone (if the agent exists).
                if let Some(queue) =
                    self.inner.sled_info.get().map(|sa| &sa.metrics_queue)
                {
                    match queue.untrack_zone_links(zone) {
                        Ok(_) => debug!(
                            log,
                            "stopped tracking switch zone datalinks"
                        ),
                        Err(errors) => error!(
                            log,
                            "failed to stop tracking switch zone datalinks";
                            "errors" => ?errors,
                        ),
                    }
                }

                let _ = zone.stop().await;
                *sled_zone = SwitchZoneState::Disabled;
            }
        }
        Ok(())
    }

    async fn try_initialize_switch_zone(
        &self,
        sled_zone: &mut SwitchZoneState,
    ) -> Result<(), Error> {
        let SwitchZoneState::Initializing {
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
        let zone_root_path =
            PathInPool { pool: ZpoolOrRamdisk::Ramdisk, path: root.clone() };
        let zone_args = ZoneArgs::Switch(&request);
        info!(self.inner.log, "Starting switch zone");
        let zone = self
            .initialize_zone(zone_args, zone_root_path, filesystems, data_links)
            .await?;
        *sled_zone = SwitchZoneState::Running {
            request: request.clone(),
            zone: Box::new(zone),
            worker: None,
        };
        Ok(())
    }

    // Body of a tokio task responsible for running until the switch zone is
    // inititalized, or it has been told to stop.
    async fn initialize_switch_zone_loop(
        &self,
        underlay_info: Option<UnderlayInfo>,
        mut exit_rx: oneshot::Receiver<()>,
    ) {
        // We don't really expect failures trying to initialize the switch zone
        // unless something is unhealthy. This timeout is somewhat arbitrary,
        // but we probably don't want to use backoff here.
        const RETRY_DELAY: Duration = Duration::from_secs(1);

        // First, go into a loop to bring up the switch zone; retry until we
        // succeed or are told to give up via `exit_rx`.
        loop {
            {
                let mut sled_zone = self.inner.switch_zone.lock().await;
                match self.try_initialize_switch_zone(&mut sled_zone).await {
                    Ok(()) => {
                        info!(self.inner.log, "initialized switch zone");
                        break;
                    }
                    Err(e) => {
                        warn!(
                            self.inner.log, "Failed to initialize switch zone";
                            InlineErrorChain::new(&e),
                        );
                    }
                }
            }

            tokio::select! {
                // If we've been told to stop trying, bail.
                _ = &mut exit_rx => {
                    info!(
                        self.inner.log,
                        "instructed to give up on switch zone initialization",
                    );
                    return;
                }

                _ = tokio::time::sleep(RETRY_DELAY) => {
                    info!(
                        self.inner.log,
                        "retrying switch zone initialization",
                    );
                    continue;
                }
            };
        }

        // Then, if we have underlay info, go into a loop trying to configure
        // our uplinks. As above, retry until we succeed or are told to stop.
        if let Some(underlay_info) = underlay_info {
            self.ensure_switch_zone_uplinks_configured_loop(
                &underlay_info,
                exit_rx,
            )
            .await;
        }
    }
}

fn internal_dns_addrobj_name(gz_address_index: u32) -> String {
    format!("internaldns{gz_address_index}")
}

#[cfg(test)]
mod test {
    use super::*;
    use sled_agent_types::zone_bundle::ZoneBundleMetadata;

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
}
