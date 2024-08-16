// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::boot_disk_os_writer::BootDiskOsWriter;
use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use crate::bootstrap::early_networking::EarlyNetworkSetupError;
use crate::config::Config;
use crate::instance_manager::InstanceManager;
use crate::long_running_tasks::LongRunningTaskHandles;
use crate::metrics::MetricsManager;
use crate::nexus::{
    NexusClient, NexusNotifierHandle, NexusNotifierInput, NexusNotifierTask,
};
use crate::params::OmicronZoneTypeExt;
use crate::probe_manager::ProbeManager;
use crate::services::{self, ServiceManager};
use crate::storage_monitor::StorageMonitorHandle;
use crate::updates::{ConfigUpdates, UpdateManager};
use crate::vmm_reservoir::{ReservoirMode, VmmReservoirManager};
use crate::zone_bundle;
use crate::zone_bundle::BundleError;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use derive_more::From;
use dropshot::HttpError;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use illumos_utils::opte::PortManager;
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use illumos_utils::zone::ZONE_PREFIX;
use nexus_sled_agent_shared::inventory::{
    Inventory, InventoryDisk, InventoryZpool, OmicronZonesConfig, SledRole,
};
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, SLED_PREFIX,
};
use omicron_common::api::external::{ByteCount, ByteCountRangeError, Vni};
use omicron_common::api::internal::nexus::{
    SledInstanceState, VmmRuntimeState,
};
use omicron_common::api::internal::shared::{
    HostPortConfig, RackNetworkConfig, ResolvedVpcFirewallRule,
    ResolvedVpcRouteSet, ResolvedVpcRouteState, SledIdentifiers,
    VirtualNetworkInterfaceHost,
};
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifactId,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use omicron_common::disk::{DisksManagementResult, OmicronPhysicalDisksConfig};
use omicron_ddm_admin_client::Client as DdmAdminClient;
use omicron_uuid_kinds::{InstanceUuid, PropolisUuid};
use sled_agent_api::Zpool;
use sled_agent_types::disk::DiskStateRequested;
use sled_agent_types::early_networking::EarlyNetworkConfig;
use sled_agent_types::instance::{
    InstanceExternalIpBody, InstanceHardware, InstanceMetadata,
    InstancePutStateResponse, InstanceStateRequested,
    InstanceUnregisterResponse,
};
use sled_agent_types::sled::{BaseboardId, StartSledAgentRequest};
use sled_agent_types::time_sync::TimeSync;
use sled_agent_types::zone_bundle::{
    BundleUtilization, CleanupContext, CleanupCount, CleanupPeriod,
    PriorityOrder, StorageLimit, ZoneBundleMetadata,
};
use sled_hardware::{underlay, HardwareManager};
use sled_hardware_types::underlay::BootstrapInterface;
use sled_hardware_types::Baseboard;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

use illumos_utils::running_zone::ZoneBuilderFactory;
#[cfg(not(test))]
use illumos_utils::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use illumos_utils::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not find boot disk")]
    BootDiskNotFound,

    #[error("Configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("Error setting up backing filesystems: {0}")]
    BackingFs(#[from] crate::backing_fs::BackingFsError),

    #[error("Error setting up swap device: {0}")]
    SwapDevice(#[from] crate::swap_device::SwapDeviceError),

    #[error("Failed to acquire etherstub: {0}")]
    Etherstub(illumos_utils::ExecutionError),

    #[error("Failed to acquire etherstub VNIC: {0}")]
    EtherstubVnic(illumos_utils::dladm::CreateVnicError),

    #[error("Bootstrap error: {0}")]
    Bootstrap(#[from] crate::bootstrap::BootstrapError),

    #[error("Failed to remove Omicron address: {0}")]
    DeleteAddress(#[from] illumos_utils::ExecutionError),

    #[error("Failed to operate on underlay device: {0}")]
    Underlay(#[from] underlay::Error),

    #[error("Failed to request firewall rules")]
    FirewallRequest(#[source] nexus_client::Error<nexus_client::types::Error>),

    #[error(transparent)]
    Services(#[from] crate::services::Error),

    #[error("Failed to create Sled Subnet: {err}")]
    SledSubnet { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Error managing instances: {0}")]
    Instance(#[from] crate::instance_manager::Error),

    #[error("Error managing storage: {0}")]
    Storage(#[from] sled_storage::error::Error),

    #[error("Error monitoring storage: {0}")]
    StorageMonitor(#[from] crate::storage_monitor::Error),

    #[error("Error updating: {0}")]
    Download(#[from] crate::updates::Error),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Error monitoring hardware: {0}")]
    Hardware(String),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns::resolver::ResolveError),

    #[error(transparent)]
    ZpoolList(#[from] illumos_utils::zpool::ListError),

    #[error(transparent)]
    EarlyNetworkError(#[from] EarlyNetworkSetupError),

    #[error("Bootstore Error: {0}")]
    Bootstore(#[from] bootstore::NodeRequestError),

    #[error("Failed to deserialize early network config: {0}")]
    EarlyNetworkDeserialize(serde_json::Error),

    #[error("Zone bundle error: {0}")]
    ZoneBundle(#[from] BundleError),

    #[error("Metrics error: {0}")]
    Metrics(#[from] crate::metrics::Error),

    #[error("Expected revision to fit in a u32, but found {0}")]
    UnexpectedRevision(i64),
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        match err {
            // Some errors can convert themselves into the external error
            Error::Services(err) => err.into(),
            Error::Storage(err) => err.into(),
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
}

// Provide a more specific HTTP error for some sled agent errors.
impl From<Error> for dropshot::HttpError {
    fn from(err: Error) -> Self {
        const NO_SUCH_INSTANCE: &str = "NO_SUCH_INSTANCE";
        match err {
            Error::Instance(crate::instance_manager::Error::Instance(
                instance_error,
            )) => {
                match instance_error {
                    crate::instance::Error::Propolis(propolis_error) => {
                        // Work around dropshot#693: HttpError::for_status
                        // only accepts client errors and asserts on server
                        // errors, so convert server errors by hand.
                        match propolis_error.status() {
                                None => HttpError::for_internal_error(
                                    propolis_error.to_string(),
                                ),

                                Some(status_code) if status_code.is_client_error() => {
                                    HttpError::for_status(None, status_code)
                                },

                                Some(status_code) => match status_code {
                                    http::status::StatusCode::SERVICE_UNAVAILABLE =>
                                        HttpError::for_unavail(None, propolis_error.to_string()),
                                    _ =>
                                        HttpError::for_internal_error(propolis_error.to_string()),
                                }
                            }
                    }
                    crate::instance::Error::Transition(omicron_error) => {
                        // Preserve the status associated with the wrapped
                        // Omicron error so that Nexus will see it in the
                        // Progenitor client error it gets back.
                        HttpError::from(omicron_error)
                    }
                    crate::instance::Error::Terminating => {
                        HttpError::for_client_error(
                            Some(NO_SUCH_INSTANCE.to_string()),
                            http::StatusCode::GONE,
                            instance_error.to_string(),
                        )
                    }
                    e => HttpError::for_internal_error(e.to_string()),
                }
            }
            Error::Instance(
                e @ crate::instance_manager::Error::NoSuchInstance(_),
            ) => HttpError::for_not_found(
                Some(NO_SUCH_INSTANCE.to_string()),
                e.to_string(),
            ),
            Error::ZoneBundle(ref inner) => match inner {
                BundleError::NoStorage | BundleError::Unavailable { .. } => {
                    HttpError::for_unavail(None, inner.to_string())
                }
                BundleError::NoSuchZone { .. } => {
                    HttpError::for_not_found(None, inner.to_string())
                }
                BundleError::StorageLimitCreate(_)
                | BundleError::CleanupPeriodCreate(_)
                | BundleError::PriorityOrderCreate(_) => {
                    HttpError::for_bad_request(None, inner.to_string())
                }
                BundleError::InstanceTerminating => {
                    HttpError::for_client_error(
                        Some(NO_SUCH_INSTANCE.to_string()),
                        http::StatusCode::GONE,
                        inner.to_string(),
                    )
                }
                _ => HttpError::for_internal_error(err.to_string()),
            },
            e => HttpError::for_internal_error(e.to_string()),
        }
    }
}

/// Error returned by `SledAgent::inventory()`
#[derive(thiserror::Error, Debug)]
pub enum InventoryError {
    // This error should be impossible because ByteCount supports values from
    // [0, i64::MAX] and we don't have anything with that many bytes in the
    // system.
    #[error(transparent)]
    BadByteCount(#[from] ByteCountRangeError),
}

impl From<InventoryError> for omicron_common::api::external::Error {
    fn from(inventory_error: InventoryError) -> Self {
        match inventory_error {
            e @ InventoryError::BadByteCount(..) => {
                omicron_common::api::external::Error::internal_error(&format!(
                    "{:#}",
                    e
                ))
            }
        }
    }
}

impl From<InventoryError> for dropshot::HttpError {
    fn from(error: InventoryError) -> Self {
        Self::from(omicron_common::api::external::Error::from(error))
    }
}

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
struct SledAgentInner {
    // ID of the Sled
    id: Uuid,

    // Subnet of the Sled's underlay.
    //
    // The Sled Agent's address can be derived from this value.
    subnet: Ipv6Subnet<SLED_PREFIX>,

    // The request that was used to start the sled-agent
    // This is used for idempotence checks during RSS/Add-Sled internal APIs
    start_request: StartSledAgentRequest,

    // Component of Sled Agent responsible for storage and dataset management.
    storage: StorageHandle,

    // Component of Sled Agent responsible for monitoring storage and updating
    // dump devices.
    storage_monitor: StorageMonitorHandle,

    // Component of Sled Agent responsible for managing Propolis instances.
    instances: InstanceManager,

    // Component of Sled Agent responsible for monitoring hardware.
    hardware: HardwareManager,

    // Component of Sled Agent responsible for managing updates.
    updates: UpdateManager,

    // Component of Sled Agent responsible for managing OPTE ports.
    port_manager: PortManager,

    // Other Oxide-controlled services running on this Sled.
    services: ServiceManager,

    // Connection to Nexus.
    nexus_client: NexusClient,

    // A mechanism for notifiying nexus about sled-agent updates
    nexus_notifier: NexusNotifierHandle,

    // The rack network config provided at RSS time.
    rack_network_config: Option<RackNetworkConfig>,

    // Object managing zone bundles.
    zone_bundler: zone_bundle::ZoneBundler,

    // A handle to the bootstore.
    bootstore: bootstore::NodeHandle,

    // Object handling production of metrics for oximeter.
    _metrics_manager: MetricsManager,

    // Handle to the traffic manager for writing OS updates to our boot disks.
    boot_disk_os_writer: BootDiskOsWriter,

    // Component of Sled Agent responsible for managing instrumentation probes.
    probes: ProbeManager,
}

impl SledAgentInner {
    fn sled_address(&self) -> SocketAddrV6 {
        get_sled_address(self.subnet)
    }

    fn switch_zone_ip(&self) -> Ipv6Addr {
        get_switch_zone_address(self.subnet)
    }
}

#[derive(Clone)]
pub struct SledAgent {
    inner: Arc<SledAgentInner>,
    log: Logger,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub async fn new(
        config: &Config,
        log: Logger,
        nexus_client: NexusClient,
        request: StartSledAgentRequest,
        services: ServiceManager,
        long_running_task_handles: LongRunningTaskHandles,
    ) -> Result<SledAgent, Error> {
        // Pass the "parent_log" to all subcomponents that want to set their own
        // "component" value.
        let parent_log = log.clone();

        // Use "log" for ourself.
        let log = log.new(o!(
            "component" => "SledAgent",
            "sled_id" => request.body.id.to_string(),
        ));
        info!(&log, "SledAgent::new(..) starting");

        let storage_manager = &long_running_task_handles.storage_manager;
        let boot_disk = storage_manager
            .get_latest_disks()
            .await
            .boot_disk()
            .ok_or_else(|| Error::BootDiskNotFound)?;

        // Configure a swap device of the configured size before other system setup.
        match config.swap_device_size_gb {
            Some(sz) if sz > 0 => {
                info!(log, "Requested swap device of size {} GiB", sz);
                crate::swap_device::ensure_swap_device(
                    &parent_log,
                    &boot_disk.1,
                    sz,
                )?;
            }
            Some(0) => {
                panic!("Invalid requested swap device size of 0 GiB");
            }
            None | Some(_) => {
                info!(log, "Not setting up swap device: not configured");
            }
        }

        info!(log, "Mounting backing filesystems");
        crate::backing_fs::ensure_backing_fs(&parent_log, &boot_disk.1)?;

        // TODO-correctness Bootstrap-agent already ensures the underlay
        // etherstub and etherstub VNIC exist on startup - could it pass them
        // through to us?
        let etherstub = Dladm::ensure_etherstub(
            illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .map_err(|e| Error::Etherstub(e))?;
        let etherstub_vnic = Dladm::ensure_etherstub_vnic(&etherstub)
            .map_err(|e| Error::EtherstubVnic(e))?;

        // Ensure the global zone has a functioning IPv6 address.
        let sled_address = request.sled_address();
        Zones::ensure_has_global_zone_v6_address(
            etherstub_vnic.clone(),
            *sled_address.ip(),
            "sled6",
        )
        .map_err(|err| Error::SledSubnet { err })?;

        // Initialize the xde kernel driver with the underlay devices.
        let underlay_nics = underlay::find_nics(&config.data_links)?;
        illumos_utils::opte::initialize_xde_driver(&log, &underlay_nics)?;

        // Start collecting metric data.
        let baseboard = long_running_task_handles.hardware_manager.baseboard();
        let identifiers = SledIdentifiers {
            rack_id: request.body.rack_id,
            sled_id: request.body.id,
            model: baseboard.model().to_string(),
            revision: baseboard.revision(),
            serial: baseboard.identifier().to_string(),
        };
        let metrics_manager =
            MetricsManager::new(&log, identifiers, *sled_address.ip())?;

        // Start tracking the underlay physical links.
        for link in underlay::find_chelsio_links(&config.data_links)? {
            metrics_manager
                .request_queue()
                .track_physical("global", &link.0)
                .await;
        }

        // Create the PortManager to manage all the OPTE ports on the sled.
        let port_manager = PortManager::new(
            parent_log.new(o!("component" => "PortManager")),
            *sled_address.ip(),
        );

        // Configure the VMM reservoir as either a percentage of DRAM or as an
        // exact size in MiB.
        let reservoir_mode = ReservoirMode::from_config(
            config.vmm_reservoir_percentage,
            config.vmm_reservoir_size_mb,
        );

        let vmm_reservoir_manager = VmmReservoirManager::spawn(
            &log,
            long_running_task_handles.hardware_manager.clone(),
            reservoir_mode,
        );

        let instances = InstanceManager::new(
            parent_log.clone(),
            nexus_client.clone(),
            etherstub.clone(),
            port_manager.clone(),
            storage_manager.clone(),
            long_running_task_handles.zone_bundler.clone(),
            ZoneBuilderFactory::default(),
            vmm_reservoir_manager.clone(),
            metrics_manager.request_queue(),
        )?;

        let update_config = ConfigUpdates {
            zone_artifact_path: Utf8PathBuf::from("/opt/oxide"),
        };
        let updates = UpdateManager::new(update_config);

        let svc_config = services::Config::new(
            request.body.id,
            config.sidecar_revision.clone(),
        );

        // Get our rack network config from the bootstore; we cannot proceed
        // until we have this, as we need to know which switches have uplinks to
        // correctly set up services.
        let get_network_config = || async {
            let serialized_config = long_running_task_handles
                .bootstore
                .get_network_config()
                .await
                .map_err(|err| BackoffError::transient(err.to_string()))?
                .ok_or_else(|| {
                    BackoffError::transient(
                        "Missing early network config in bootstore".to_string(),
                    )
                })?;

            let early_network_config =
                EarlyNetworkConfig::deserialize_bootstore_config(
                    &log,
                    &serialized_config,
                )
                .map_err(|err| BackoffError::transient(err.to_string()))?;

            Ok(early_network_config.body.rack_network_config)
        };
        let rack_network_config: Option<RackNetworkConfig> =
            retry_notify::<_, String, _, _, _, _>(
                retry_policy_internal_service_aggressive(),
                get_network_config,
                |error, delay| {
                    warn!(
                        log,
                        "failed to get network config from bootstore";
                        "error" => ?error,
                        "retry_after" => ?delay,
                    );
                },
            )
            .await
            .expect(
                "Expected an infinite retry loop getting \
             network config from bootstore",
            );

        services
            .sled_agent_started(
                svc_config,
                port_manager.clone(),
                *sled_address.ip(),
                request.body.rack_id,
                rack_network_config.clone(),
                metrics_manager.request_queue(),
            )
            .await?;

        // Spawn a background task for managing notifications to nexus
        // about this sled-agent.
        let nexus_notifier_input = NexusNotifierInput {
            sled_id: request.body.id,
            sled_address: get_sled_address(request.body.subnet),
            nexus_client: nexus_client.clone(),
            hardware: long_running_task_handles.hardware_manager.clone(),
            vmm_reservoir_manager: vmm_reservoir_manager.clone(),
        };
        let (nexus_notifier_task, nexus_notifier_handle) =
            NexusNotifierTask::new(nexus_notifier_input, &log);

        tokio::spawn(async move {
            nexus_notifier_task.run().await;
        });

        let probes = ProbeManager::new(
            request.body.id,
            nexus_client.clone(),
            etherstub.clone(),
            storage_manager.clone(),
            port_manager.clone(),
            metrics_manager.request_queue(),
            log.new(o!("component" => "ProbeManager")),
        );

        let sled_agent = SledAgent {
            inner: Arc::new(SledAgentInner {
                id: request.body.id,
                subnet: request.body.subnet,
                start_request: request,
                storage: long_running_task_handles.storage_manager.clone(),
                storage_monitor: long_running_task_handles
                    .storage_monitor_handle
                    .clone(),
                instances,
                probes,
                hardware: long_running_task_handles.hardware_manager.clone(),
                updates,
                port_manager,
                services,
                nexus_client,
                nexus_notifier: nexus_notifier_handle,
                rack_network_config,
                zone_bundler: long_running_task_handles.zone_bundler.clone(),
                bootstore: long_running_task_handles.bootstore.clone(),
                _metrics_manager: metrics_manager,
                boot_disk_os_writer: BootDiskOsWriter::new(&parent_log),
            }),
            log: log.clone(),
        };

        sled_agent.inner.probes.run().await;

        // We immediately add a notification to the request queue about our
        // existence. If inspection of the hardware later informs us that we're
        // actually running on a scrimlet, that's fine, the updated value will
        // be received by Nexus eventually.
        sled_agent.notify_nexus_about_self(&log).await;

        Ok(sled_agent)
    }

    /// Load services for which we're responsible.
    ///
    /// Blocks until all services have started, retrying indefinitely on
    /// failure.
    pub(crate) async fn load_services(&self) {
        info!(self.log, "Loading cold boot services");
        retry_notify(
            retry_policy_internal_service_aggressive(),
            || async {
                // Load as many services as we can, and don't exit immediately
                // upon failure.
                let load_services_result =
                    self.inner.services.load_services().await.map_err(|err| {
                        BackoffError::transient(Error::from(err))
                    });

                // If there wasn't any work to do, we're done immediately.
                if matches!(
                    load_services_result,
                    Ok(services::LoadServicesResult::NoServicesToLoad)
                ) {
                    info!(
                        self.log,
                        "load_services exiting early; no services to be loaded"
                    );
                    return Ok(());
                }

                // Otherwise, request firewall rule updates for as many services as
                // we can. Note that we still make this request even if we only
                // partially load some services.
                let firewall_result = self
                    .request_firewall_update()
                    .await
                    .map_err(|err| BackoffError::transient(err));

                // Only complete if we have loaded all services and firewall
                // rules successfully.
                load_services_result.and(firewall_result)
            },
            |err, delay| {
                warn!(
                    self.log,
                    "Failed to load services, will retry in {:?}", delay;
                    "error" => ?err,
                );
            },
        )
        .await
        .unwrap(); // we retry forever, so this can't fail
    }

    pub(crate) fn switch_zone_underlay_info(
        &self,
    ) -> (Ipv6Addr, Option<&RackNetworkConfig>) {
        (self.inner.switch_zone_ip(), self.inner.rack_network_config.as_ref())
    }

    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    pub fn logger(&self) -> &Logger {
        &self.log
    }

    pub fn start_request(&self) -> &StartSledAgentRequest {
        &self.inner.start_request
    }

    /// Requests firewall rules from Nexus.
    ///
    /// Does not retry upon failure.
    async fn request_firewall_update(&self) -> Result<(), Error> {
        let sled_id = self.inner.id;

        self.inner
            .nexus_client
            .sled_firewall_rules_request(&sled_id)
            .await
            .map_err(|err| Error::FirewallRequest(err))?;
        Ok(())
    }

    /// Trigger a request to Nexus informing it that the current sled exists,
    /// with information about the existing set of hardware.
    pub(crate) async fn notify_nexus_about_self(&self, log: &Logger) {
        self.inner.nexus_notifier.notify_nexus_about_self(log).await;
    }

    /// List all zone bundles on the system, for any zones live or dead.
    pub async fn list_all_zone_bundles(
        &self,
        filter: Option<&str>,
    ) -> Result<Vec<ZoneBundleMetadata>, Error> {
        self.inner.zone_bundler.list(filter).await.map_err(Error::from)
    }

    /// List zone bundles for the provided zone.
    pub async fn list_zone_bundles(
        &self,
        name: &str,
    ) -> Result<Vec<ZoneBundleMetadata>, Error> {
        self.inner.zone_bundler.list_for_zone(name).await.map_err(Error::from)
    }

    /// Create a zone bundle for the provided zone.
    pub async fn create_zone_bundle(
        &self,
        name: &str,
    ) -> Result<ZoneBundleMetadata, Error> {
        if name.starts_with(PROPOLIS_ZONE_PREFIX) {
            self.inner
                .instances
                .create_zone_bundle(name)
                .await
                .map_err(Error::from)
        } else if name.starts_with(ZONE_PREFIX) {
            self.inner
                .services
                .create_zone_bundle(name)
                .await
                .map_err(Error::from)
        } else {
            Err(Error::from(BundleError::NoSuchZone { name: name.to_string() }))
        }
    }

    /// Fetch the paths to all zone bundles with the provided name and ID.
    pub async fn get_zone_bundle_paths(
        &self,
        name: &str,
        id: &Uuid,
    ) -> Result<Vec<Utf8PathBuf>, Error> {
        self.inner
            .zone_bundler
            .bundle_paths(name, id)
            .await
            .map_err(Error::from)
    }

    /// List the zones that the sled agent is currently managing.
    pub async fn zones_list(&self) -> Result<Vec<String>, Error> {
        Zones::get()
            .await
            .map(|zones| {
                let mut zn: Vec<_> = zones
                    .into_iter()
                    .filter_map(|zone| {
                        if matches!(zone.state(), zone::State::Running) {
                            Some(String::from(zone.name()))
                        } else {
                            None
                        }
                    })
                    .collect();
                zn.sort();
                zn
            })
            .map_err(|e| Error::from(BundleError::from(e)))
    }

    /// Fetch the zone bundle cleanup context.
    pub async fn zone_bundle_cleanup_context(&self) -> CleanupContext {
        self.inner.zone_bundler.cleanup_context().await
    }

    /// Update the zone bundle cleanup context.
    pub async fn update_zone_bundle_cleanup_context(
        &self,
        period: Option<CleanupPeriod>,
        storage_limit: Option<StorageLimit>,
        priority: Option<PriorityOrder>,
    ) -> Result<(), Error> {
        self.inner
            .zone_bundler
            .update_cleanup_context(period, storage_limit, priority)
            .await
            .map_err(Error::from)
    }

    /// Fetch the current utilization of the relevant datasets for zone bundles.
    pub async fn zone_bundle_utilization(
        &self,
    ) -> Result<BTreeMap<Utf8PathBuf, BundleUtilization>, Error> {
        self.inner.zone_bundler.utilization().await.map_err(Error::from)
    }

    /// Trigger an explicit request to cleanup old zone bundles.
    pub async fn zone_bundle_cleanup(
        &self,
    ) -> Result<BTreeMap<Utf8PathBuf, CleanupCount>, Error> {
        self.inner.zone_bundler.cleanup().await.map_err(Error::from)
    }

    /// Requests the set of physical disks currently managed by the Sled Agent.
    ///
    /// This should be contrasted by the set of disks in the inventory, which
    /// may contain a slightly different set, if certain disks are not expected
    /// to be in-use by the broader control plane.
    pub async fn omicron_physical_disks_list(
        &self,
    ) -> Result<OmicronPhysicalDisksConfig, Error> {
        Ok(self.storage().omicron_physical_disks_list().await?)
    }

    /// Ensures that the specific set of Omicron Physical Disks are running
    /// on this sled, and that no other disks are being used by the control
    /// plane (with the exception of M.2s, which are always automatically
    /// in-use).
    pub async fn omicron_physical_disks_ensure(
        &self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, Error> {
        info!(self.log, "physical disks ensure");
        // Tell the storage subsystem which disks should be managed.
        let disk_result =
            self.storage().omicron_physical_disks_ensure(config).await?;
        info!(self.log, "physical disks ensure: Updated storage");

        // Grab a view of the latest set of disks, alongside a generation
        // number.
        //
        // This generation is at LEAST as high as our last call through
        // omicron_physical_disks_ensure. It may actually be higher, if a
        // concurrent operation occurred.
        //
        // "latest_disks" has a generation number, which is important for other
        // subcomponents of Sled Agent to consider. If multiple requests to
        // ensure disks arrive concurrently, it's important to "only advance
        // forward" as requested by Nexus.
        //
        // For example: if we receive the following requests concurrently:
        // - Use Disks {A, B, C}, generation = 1
        // - Use Disks {A, B, C, D}, generation = 2
        //
        // If we ignore generation numbers, it's possible that we start using
        // "disk D" -- e.g., for instance filesystems -- and then immediately
        // delete it when we process the request with "generation 1".
        //
        // By keeping these requests ordered, we prevent this thrashing, and
        // ensure that we always progress towards the last-requested state.
        let latest_disks = self.storage().get_latest_disks().await;
        let our_gen = latest_disks.generation();
        info!(self.log, "physical disks ensure: Propagating new generation of disks"; "generation" => ?our_gen);

        // Ensure that the StorageMonitor, and the dump devices, have committed
        // to start using new disks and stop using old ones.
        self.inner.storage_monitor.await_generation(*our_gen).await?;
        info!(self.log, "physical disks ensure: Updated storage monitor");

        // Ensure that the ZoneBundler, if it was creating a bundle referencing
        // the old U.2s, has stopped using them.
        self.inner.zone_bundler.await_completion_of_prior_bundles().await;
        info!(self.log, "physical disks ensure: Updated zone bundler");

        // Ensure that all probes, at least after our call to
        // "omicron_physical_disks_ensure", stop using any disks that
        // may have been in-service from before that request.
        self.inner.probes.use_only_these_disks(&latest_disks).await;
        info!(self.log, "physical disks ensure: Updated probes");

        // Do the same for instances - mark them failed if they were using
        // expunged disks.
        self.inner.instances.use_only_these_disks(latest_disks).await?;
        info!(self.log, "physical disks ensure: Updated instances");

        Ok(disk_result)
    }

    /// List the Omicron zone configuration that's currently running
    pub async fn omicron_zones_list(
        &self,
    ) -> Result<OmicronZonesConfig, Error> {
        Ok(self.inner.services.omicron_zones_list().await?)
    }

    /// Ensures that the specific set of Omicron zones are running as configured
    /// (and that no other zones are running)
    pub async fn omicron_zones_ensure(
        &self,
        requested_zones: OmicronZonesConfig,
    ) -> Result<(), Error> {
        // TODO:
        // - If these are the set of filesystems, we should also consider
        // removing the ones which are not listed here.
        // - It's probably worth sending a bulk request to the storage system,
        // rather than requesting individual datasets.
        for zone in &requested_zones.zones {
            let Some(dataset_name) = zone.dataset_name() else {
                continue;
            };

            // First, ensure the dataset exists
            let dataset_id = zone.id;
            self.inner
                .storage
                .upsert_filesystem(dataset_id, dataset_name)
                .await?;
        }

        self.inner
            .services
            .ensure_all_omicron_zones_persistent(requested_zones, None)
            .await?;
        Ok(())
    }

    pub async fn cockroachdb_initialize(&self) -> Result<(), Error> {
        self.inner.services.cockroachdb_initialize().await?;
        Ok(())
    }

    /// Gets the sled's current list of all zpools.
    pub async fn zpools_get(&self) -> Vec<Zpool> {
        self.inner
            .storage
            .get_latest_disks()
            .await
            .get_all_zpools()
            .into_iter()
            .map(|(name, variant)| Zpool {
                id: name.id(),
                disk_type: variant.into(),
            })
            .collect()
    }

    /// Returns whether or not the sled believes itself to be a scrimlet
    pub fn get_role(&self) -> SledRole {
        if self.inner.hardware.is_scrimlet() {
            SledRole::Scrimlet
        } else {
            SledRole::Gimlet
        }
    }

    /// Idempotently ensures that a given instance is registered with this sled,
    /// i.e., that it can be addressed by future calls to
    /// [`Self::instance_ensure_state`].
    #[allow(clippy::too_many_arguments)]
    pub async fn instance_ensure_registered(
        &self,
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
        metadata: InstanceMetadata,
    ) -> Result<SledInstanceState, Error> {
        self.inner
            .instances
            .ensure_registered(
                instance_id,
                propolis_id,
                hardware,
                instance_runtime,
                vmm_runtime,
                propolis_addr,
                self.sled_identifiers(),
                metadata,
            )
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that the specified instance is no longer registered
    /// on this sled.
    ///
    /// If the instance is registered and has a running Propolis, this operation
    /// rudely terminates the instance.
    pub async fn instance_ensure_unregistered(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        self.inner
            .instances
            .ensure_unregistered(instance_id)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently drives the specified instance into the specified target
    /// state.
    pub async fn instance_ensure_state(
        &self,
        instance_id: InstanceUuid,
        target: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        self.inner
            .instances
            .ensure_state(instance_id, target)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that an instance's OPTE/port state includes the
    /// specified external IP address.
    ///
    /// This method will return an error when trying to register an ephemeral IP which
    /// does not match the current ephemeral IP.
    pub async fn instance_put_external_ip(
        &self,
        instance_id: InstanceUuid,
        external_ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .add_external_ip(instance_id, external_ip)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that an instance's OPTE/port state does not include the
    /// specified external IP address in either its ephemeral or floating IP set.
    pub async fn instance_delete_external_ip(
        &self,
        instance_id: InstanceUuid,
        external_ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .delete_external_ip(instance_id, external_ip)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Returns the state of the instance with the provided ID.
    pub async fn instance_get_state(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<SledInstanceState, Error> {
        self.inner
            .instances
            .get_instance_state(instance_id)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that the given virtual disk is attached (or not) as
    /// specified.
    ///
    /// NOTE: Not yet implemented.
    pub async fn disk_ensure(
        &self,
        _disk_id: Uuid,
        _initial_state: DiskRuntimeState,
        _target: DiskStateRequested,
    ) -> Result<DiskRuntimeState, Error> {
        todo!("Disk attachment not yet implemented");
    }

    /// Downloads and applies an artifact.
    pub async fn update_artifact(
        &self,
        artifact: UpdateArtifactId,
    ) -> Result<(), Error> {
        self.inner
            .updates
            .download_artifact(artifact, &self.inner.nexus_client)
            .await?;
        Ok(())
    }

    /// Issue a snapshot request for a Crucible disk attached to an instance
    pub async fn instance_issue_disk_snapshot_request(
        &self,
        instance_id: InstanceUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .instance_issue_disk_snapshot_request(
                instance_id,
                disk_id,
                snapshot_id,
            )
            .await
            .map_err(Error::from)
    }

    pub async fn firewall_rules_ensure(
        &self,
        vpc_vni: Vni,
        rules: &[ResolvedVpcFirewallRule],
    ) -> Result<(), Error> {
        self.inner
            .port_manager
            .firewall_rules_ensure(vpc_vni, rules)
            .map_err(Error::from)
    }

    pub async fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        self.inner.port_manager.list_virtual_nics().map_err(Error::from)
    }

    pub async fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        self.inner
            .port_manager
            .set_virtual_nic_host(mapping)
            .map_err(Error::from)
    }

    pub async fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        self.inner
            .port_manager
            .unset_virtual_nic_host(mapping)
            .map_err(Error::from)
    }

    /// Gets the sled's current time synchronization state
    pub async fn timesync_get(&self) -> Result<TimeSync, Error> {
        self.inner.services.timesync_get().await.map_err(Error::from)
    }

    pub async fn ensure_scrimlet_host_ports(
        &self,
        uplinks: Vec<HostPortConfig>,
    ) -> Result<(), Error> {
        self.inner
            .services
            .ensure_scrimlet_host_ports(uplinks)
            .await
            .map_err(Error::from)
    }

    pub fn bootstore(&self) -> bootstore::NodeHandle {
        self.inner.bootstore.clone()
    }

    pub fn list_vpc_routes(&self) -> Vec<ResolvedVpcRouteState> {
        self.inner.port_manager.vpc_routes_list()
    }

    pub fn set_vpc_routes(
        &self,
        routes: Vec<ResolvedVpcRouteSet>,
    ) -> Result<(), Error> {
        self.inner.port_manager.vpc_routes_ensure(routes).map_err(Error::from)
    }

    pub(crate) fn storage(&self) -> &StorageHandle {
        &self.inner.storage
    }

    pub(crate) fn boot_disk_os_writer(&self) -> &BootDiskOsWriter {
        &self.inner.boot_disk_os_writer
    }

    /// Return identifiers for this sled.
    ///
    /// This is mostly used to identify timeseries data with the originating
    /// sled.
    ///
    /// NOTE: This only returns the identifiers for the _sled_ itself. If you're
    /// interested in the switch identifiers, MGS is the current best way to do
    /// that, by asking for the local switch's slot, and then that switch's SP
    /// state.
    pub(crate) fn sled_identifiers(&self) -> SledIdentifiers {
        let baseboard = self.inner.hardware.baseboard();
        SledIdentifiers {
            rack_id: self.inner.start_request.body.rack_id,
            sled_id: self.inner.id,
            model: baseboard.model().to_string(),
            revision: baseboard.revision(),
            serial: baseboard.identifier().to_string(),
        }
    }

    /// Return basic information about ourselves: identity and status
    ///
    /// This is basically a GET version of the information we push to Nexus on
    /// startup.
    pub(crate) async fn inventory(&self) -> Result<Inventory, InventoryError> {
        let sled_id = self.inner.id;
        let sled_agent_address = self.inner.sled_address();
        let is_scrimlet = self.inner.hardware.is_scrimlet();
        let baseboard = self.inner.hardware.baseboard();
        let usable_hardware_threads =
            self.inner.hardware.online_processor_count();
        let usable_physical_ram =
            self.inner.hardware.usable_physical_ram_bytes();
        let reservoir_size = self.inner.instances.reservoir_size();
        let sled_role =
            if is_scrimlet { SledRole::Scrimlet } else { SledRole::Gimlet };

        let mut disks = vec![];
        let mut zpools = vec![];
        let all_disks = self.storage().get_latest_disks().await;
        for (identity, variant, slot, _firmware) in all_disks.iter_all() {
            disks.push(InventoryDisk {
                identity: identity.clone(),
                variant,
                slot,
            });
        }
        for zpool in all_disks.all_u2_zpools() {
            let info =
                match illumos_utils::zpool::Zpool::get_info(&zpool.to_string())
                {
                    Ok(info) => info,
                    Err(err) => {
                        warn!(
                            self.log,
                            "Failed to access zpool info";
                            "zpool" => %zpool,
                            "err" => %err
                        );
                        continue;
                    }
                };

            zpools.push(InventoryZpool {
                id: zpool.id(),
                total_size: ByteCount::try_from(info.size())?,
            });
        }

        Ok(Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram: ByteCount::try_from(usable_physical_ram)?,
            reservoir_size,
            disks,
            zpools,
        })
    }
}

#[derive(From, thiserror::Error, Debug)]
pub enum AddSledError {
    #[error("Failed to learn bootstrap ip for {sled_id}")]
    BootstrapAgentClient {
        sled_id: Baseboard,
        #[source]
        err: bootstrap_agent_client::Error,
    },
    #[error("Failed to connect to DDM")]
    DdmAdminClient(#[source] omicron_ddm_admin_client::DdmError),
    #[error("Failed to learn bootstrap ip for {0:?}")]
    NotFound(BaseboardId),
    #[error("Failed to initialize {sled_id}: {err}")]
    BootstrapTcpClient {
        sled_id: Baseboard,
        err: crate::bootstrap::client::Error,
    },
}

/// Add a sled to an initialized rack.
pub async fn sled_add(
    log: Logger,
    sled_id: BaseboardId,
    request: StartSledAgentRequest,
) -> Result<(), AddSledError> {
    // Get all known bootstrap addresses via DDM
    let ddm_admin_client = DdmAdminClient::localhost(&log)?;
    let addrs = ddm_admin_client
        .derive_bootstrap_addrs_from_prefixes(&[BootstrapInterface::GlobalZone])
        .await?;

    // Create a set of futures to concurrently map the baseboard to bootstrap ip
    // for each sled
    let mut addrs_to_sleds = addrs
        .map(|ip| {
            let log = log.clone();
            async move {
                let client = bootstrap_agent_client::Client::new(
                    &format!("http://[{ip}]"),
                    log,
                );
                let result = client.baseboard_get().await;

                (ip, result)
            }
        })
        .collect::<FuturesUnordered<_>>();

    // Execute the futures until we find our matching sled or are done searching
    let mut target_ip = None;
    let mut found_baseboard = None;
    while let Some((ip, result)) = addrs_to_sleds.next().await {
        match result {
            Ok(baseboard) => {
                // Convert from progenitor type back to `sled-hardware`
                // type.
                let found: Baseboard = baseboard.into_inner().into();
                if sled_id.serial_number == found.identifier()
                    && sled_id.part_number == found.model()
                {
                    target_ip = Some(ip);
                    found_baseboard = Some(found);
                    break;
                }
            }
            Err(err) => {
                warn!(
                    log, "Failed to get baseboard for {ip}";
                    "err" => #%err,
                );
            }
        }
    }

    // Contact the sled and initialize it
    let bootstrap_addr =
        target_ip.ok_or_else(|| AddSledError::NotFound(sled_id.clone()))?;
    let bootstrap_addr =
        SocketAddrV6::new(bootstrap_addr, BOOTSTRAP_AGENT_RACK_INIT_PORT, 0, 0);
    let client = crate::bootstrap::client::Client::new(
        bootstrap_addr,
        log.new(o!("BootstrapAgentClient" => bootstrap_addr.to_string())),
    );

    // Safe to unwrap, because we would have bailed when checking target_ip
    // above otherwise. baseboard and target_ip are set together.
    let baseboard = found_baseboard.unwrap();

    client.start_sled_agent(&request).await.map_err(|err| {
        AddSledError::BootstrapTcpClient { sled_id: baseboard.clone(), err }
    })?;

    info!(log, "Peer agent initialized"; "peer_bootstrap_addr" => %bootstrap_addr, "peer_id" => %baseboard);
    Ok(())
}
