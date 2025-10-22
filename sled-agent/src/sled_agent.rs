// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::artifact_store::ArtifactStore;
use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use crate::bootstrap::early_networking::EarlyNetworkSetupError;
use crate::config::Config;
use crate::hardware_monitor::HardwareMonitorHandle;
use crate::instance_manager::InstanceManager;
use crate::long_running_tasks::LongRunningTaskHandles;
use crate::metrics::{MetricsManager, MetricsRequestQueue};
use crate::nexus::{
    NexusClient, NexusNotifierHandle, NexusNotifierInput, NexusNotifierTask,
};
use crate::probe_manager::ProbeManager;
use crate::services::{self, ServiceManager, UnderlayInfo};
use crate::support_bundle::logs::SupportBundleLogs;
use crate::support_bundle::storage::SupportBundleManager;
use crate::vmm_reservoir::{ReservoirMode, VmmReservoirManager};
use crate::zone_bundle;
use crate::zone_bundle::BundleError;
use anyhow::anyhow;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use derive_more::From;
use dropshot::HttpError;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use illumos_utils::opte::PortManager;
use illumos_utils::running_zone::RunningZone;
use illumos_utils::zpool::PathInPool;
use itertools::Itertools as _;
use nexus_sled_agent_shared::inventory::{
    Inventory, OmicronSledConfig, SledRole,
};
use omicron_common::address::{
    Ipv6Subnet, SLED_PREFIX, get_sled_address, get_switch_zone_address,
};
use omicron_common::api::external::{ByteCount, ByteCountRangeError, Vni};
use omicron_common::api::internal::nexus::{DiskRuntimeState, SledVmmState};
use omicron_common::api::internal::shared::{
    ExternalIpGatewayMap, HostPortConfig, RackNetworkConfig,
    ResolvedVpcFirewallRule, ResolvedVpcRouteSet, ResolvedVpcRouteState,
    SledIdentifiers, VirtualNetworkInterfaceHost,
};
use omicron_common::backoff::{
    BackoffError, retry_notify, retry_policy_internal_service_aggressive,
};
use omicron_ddm_admin_client::Client as DdmAdminClient;
use omicron_uuid_kinds::{
    GenericUuid, MupdateOverrideUuid, PropolisUuid, SledUuid,
};
use sled_agent_api::v5::{InstanceEnsureBody, InstanceMulticastBody};
use sled_agent_config_reconciler::{
    ConfigReconcilerHandle, ConfigReconcilerSpawnToken, InternalDisks,
    InternalDisksReceiver, LedgerNewConfigError, LedgerTaskError,
    ReconcilerInventory, SledAgentArtifactStore, SledAgentFacilities,
};
use sled_agent_types::disk::DiskStateRequested;
use sled_agent_types::early_networking::EarlyNetworkConfig;
use sled_agent_types::instance::{
    InstanceExternalIpBody, VmmPutStateResponse, VmmStateRequested,
    VmmUnregisterResponse,
};
use sled_agent_types::sled::{BaseboardId, StartSledAgentRequest};
use sled_agent_types::zone_bundle::{
    BundleUtilization, CleanupContext, CleanupCount, CleanupPeriod,
    PriorityOrder, StorageLimit, ZoneBundleMetadata,
};
use sled_agent_types::zone_images::{
    PreparedOmicronZone, RemoveMupdateOverrideResult, ResolverStatus,
};
use sled_diagnostics::SledDiagnosticsCmdError;
use sled_diagnostics::SledDiagnosticsCmdOutput;
use sled_hardware::{HardwareManager, MemoryReservations, underlay};
use sled_hardware_types::Baseboard;
use sled_hardware_types::underlay::BootstrapInterface;
use slog::Logger;
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use sprockets_tls::keys::SprocketsConfig;
use std::collections::BTreeMap;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

use illumos_utils::dladm::{Dladm, EtherstubVnic};
use illumos_utils::zone::Api;
use illumos_utils::zone::Zones;

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

    #[error(transparent)]
    Services(#[from] crate::services::Error),

    #[error("Failed to create Sled Subnet: {err}")]
    SledSubnet { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Error managing instances: {0}")]
    Instance(#[from] crate::instance_manager::Error),

    #[error("Error updating: {0}")]
    Download(#[from] crate::updates::Error),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Error monitoring hardware: {0}")]
    Hardware(String),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns_resolver::ResolveError),

    #[error(transparent)]
    ZpoolList(#[from] illumos_utils::zpool::ListError),

    #[error(transparent)]
    EarlyNetworkError(#[from] EarlyNetworkSetupError),

    #[error("Bootstore Error: {0}")]
    Bootstore(#[from] bootstore::NodeRequestError),

    #[error("Failed to deserialize early network config: {0}")]
    EarlyNetworkDeserialize(serde_json::Error),

    #[error("Support bundle error: {0}")]
    SupportBundle(String),

    #[error("Zone bundle error: {0}")]
    ZoneBundle(#[from] BundleError),

    #[error("Metrics error: {0}")]
    Metrics(#[from] crate::metrics::Error),

    #[error("Expected revision to fit in a u32, but found {0}")]
    UnexpectedRevision(i64),

    #[error(transparent)]
    RepoDepotStart(#[from] crate::artifact_store::StartError),

    #[error("Time not yet synchronized")]
    TimeNotSynchronized,
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        match err {
            // Some errors can convert themselves into the external error
            Error::Services(err) => err.into(),
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
}

// Provide a more specific HTTP error for some sled agent errors.
impl From<Error> for dropshot::HttpError {
    fn from(err: Error) -> Self {
        use dropshot::ClientErrorStatusCode;
        use dropshot::ErrorStatusCode;

        const NO_SUCH_INSTANCE: &str = "NO_SUCH_INSTANCE";
        const INSTANCE_CHANNEL_FULL: &str = "INSTANCE_CHANNEL_FULL";
        match err {
            Error::Instance(crate::instance_manager::Error::Instance(
                instance_error,
            )) => {
                match instance_error {
                    // The instance's request channel is full, so it cannot
                    // currently process this request. Shed load, but indicate
                    // to the client that it can try again later.
                    err @ crate::instance::Error::FailedSendChannelFull => {
                        HttpError::for_unavail(
                            Some(INSTANCE_CHANNEL_FULL.to_string()),
                            err.to_string(),
                        )
                    }
                    crate::instance::Error::Propolis(propolis_error) => {
                        if let Some(status_code) =
                            propolis_error.status().and_then(|status| {
                                ErrorStatusCode::try_from(status).ok()
                            })
                        {
                            if let Ok(status_code) =
                                status_code.as_client_error()
                            {
                                return HttpError::for_client_error_with_status(
                                    None,
                                    status_code,
                                );
                            }

                            if status_code
                                == ErrorStatusCode::SERVICE_UNAVAILABLE
                            {
                                return HttpError::for_unavail(
                                    None,
                                    propolis_error.to_string(),
                                );
                            }
                        }
                        HttpError::for_internal_error(
                            propolis_error.to_string(),
                        )
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
                            ClientErrorStatusCode::GONE,
                            instance_error.to_string(),
                        )
                    }
                    e => HttpError::for_internal_error(e.to_string()),
                }
            }
            Error::Instance(
                e @ crate::instance_manager::Error::NoSuchVmm(_),
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
                        ClientErrorStatusCode::GONE,
                        inner.to_string(),
                    )
                }
                _ => HttpError::for_internal_error(err.to_string()),
            },
            Error::Services(err) => {
                let err = omicron_common::api::external::Error::from(err);
                err.into()
            }
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
    #[error(transparent)]
    InventoryError(#[from] sled_agent_config_reconciler::InventoryError),
}

impl From<InventoryError> for omicron_common::api::external::Error {
    fn from(inventory_error: InventoryError) -> Self {
        match inventory_error {
            e @ (InventoryError::BadByteCount(..)
            | InventoryError::InventoryError(_)) => {
                omicron_common::api::external::Error::internal_error(
                    &InlineErrorChain::new(&e).to_string(),
                )
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
    id: SledUuid,

    // Subnet of the Sled's underlay.
    //
    // The Sled Agent's address can be derived from this value.
    subnet: Ipv6Subnet<SLED_PREFIX>,

    // The request that was used to start the sled-agent
    // This is used for idempotence checks during RSS/Add-Sled internal APIs
    start_request: StartSledAgentRequest,

    // Handle to the sled-agent-config-reconciler system.
    config_reconciler: Arc<ConfigReconcilerHandle>,

    // Component of Sled Agent responsible for managing Propolis instances.
    instances: InstanceManager,

    // Component of Sled Agent responsible for monitoring hardware.
    hardware: HardwareManager,

    // Component of Sled Agent responsible for managing OPTE ports.
    port_manager: PortManager,

    // Other Oxide-controlled services running on this Sled.
    services: ServiceManager,

    // A mechanism for notifiying nexus about sled-agent updates
    nexus_notifier: NexusNotifierHandle,

    // The rack network config provided at RSS time.
    rack_network_config: Option<RackNetworkConfig>,

    // Object managing zone bundles.
    zone_bundler: zone_bundle::ZoneBundler,

    // A handle to the bootstore.
    bootstore: bootstore::NodeHandle,

    // A handle to the hardware monitor.
    hardware_monitor: HardwareMonitorHandle,

    // Object handling production of metrics for oximeter.
    _metrics_manager: MetricsManager,

    // Component of Sled Agent responsible for managing instrumentation probes.
    probes: ProbeManager,

    // Component of Sled Agent responsible for managing the artifact store.
    repo_depot: dropshot::HttpServer<Arc<ArtifactStore<InternalDisksReceiver>>>,
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
    pub(crate) log: Logger,
    sprockets: SprocketsConfig,
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
        config_reconciler_spawn_token: ConfigReconcilerSpawnToken,
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

        // Cleanup any old sled-diagnostics ZFS snapshots
        sled_diagnostics::LogsHandle::new(
            log.new(o!("component" => "sled-diagnostics-cleanup")),
        )
        .cleanup_snapshots()
        .await;

        let config_reconciler =
            Arc::clone(&long_running_task_handles.config_reconciler);
        let boot_disk_zpool = config_reconciler
            .internal_disks_rx()
            .current()
            .boot_disk_zpool_name()
            .ok_or_else(|| Error::BootDiskNotFound)?;

        // Configure a swap device of the configured size before other system setup.
        match config.swap_device_size_gb {
            Some(sz) if sz > 0 => {
                info!(log, "Requested swap device of size {} GiB", sz);
                crate::swap_device::ensure_swap_device(
                    &parent_log,
                    &boot_disk_zpool,
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
        crate::backing_fs::ensure_backing_fs(&parent_log, &boot_disk_zpool)
            .await?;

        // TODO-correctness Bootstrap-agent already ensures the underlay
        // etherstub and etherstub VNIC exist on startup - could it pass them
        // through to us?
        let etherstub = Dladm::ensure_etherstub(
            illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .await
        .map_err(|e| Error::Etherstub(e))?;
        let etherstub_vnic = Dladm::ensure_etherstub_vnic(&etherstub)
            .await
            .map_err(|e| Error::EtherstubVnic(e))?;

        // Ensure the global zone has a functioning IPv6 address.
        let sled_address = request.sled_address();
        Zones::ensure_has_global_zone_v6_address(
            etherstub_vnic.clone(),
            *sled_address.ip(),
            "sled6",
        )
        .await
        .map_err(|err| Error::SledSubnet { err })?;

        // Initialize the xde kernel driver with the underlay devices.
        let underlay_nics = underlay::find_nics(&config.data_links).await?;
        illumos_utils::opte::initialize_xde_driver(&log, &underlay_nics)?;

        // Start collecting metric data.
        let baseboard = long_running_task_handles.hardware_manager.baseboard();
        let identifiers = SledIdentifiers {
            rack_id: request.body.rack_id,
            sled_id: request.body.id.into_untyped_uuid(),
            model: baseboard.model().to_string(),
            revision: baseboard.revision(),
            serial: baseboard.identifier().to_string(),
        };
        let metrics_manager =
            MetricsManager::new(&log, identifiers.clone(), *sled_address.ip())?;

        // Start tracking the underlay physical links.
        for link in underlay::find_chelsio_links(&config.data_links).await? {
            match metrics_manager
                .request_queue()
                .track_physical("global", &link.0)
            {
                Ok(_) => {
                    debug!(log, "started tracking global zone underlay links")
                }
                Err(e) => error!(
                    log,
                    "failed to track global zone underlay link";
                    "error" => slog_error_chain::InlineErrorChain::new(&e),
                ),
            }
        }

        // Create the PortManager to manage all the OPTE ports on the sled.
        let port_manager = PortManager::new(
            parent_log.new(o!("component" => "PortManager")),
            *sled_address.ip(),
        );

        // The VMM reservoir is configured with respect to what's left after
        // accounting for relatively fixed and predictable uses.
        // We expect certain amounts of memory to be set aside for kernel,
        // buffer, or control plane uses.
        let memory_sizes = MemoryReservations::new(
            parent_log.new(o!("component" => "MemoryReservations")),
            long_running_task_handles.hardware_manager.clone(),
            config.control_plane_memory_earmark_mb,
        );

        // Configure the VMM reservoir as either a percentage of DRAM or as an
        // exact size in MiB.
        let reservoir_mode = ReservoirMode::from_config(
            config.vmm_reservoir_percentage,
            config.vmm_reservoir_size_mb,
        );

        let vmm_reservoir_manager =
            VmmReservoirManager::spawn(&log, memory_sizes, reservoir_mode);

        let instance_vnic_allocator = illumos_utils::link::VnicAllocator::new(
            "Instance",
            etherstub.clone(),
            Arc::new(illumos_utils::dladm::Dladm::real_api()),
        );
        let instances = InstanceManager::new(
            parent_log.clone(),
            nexus_client.clone(),
            instance_vnic_allocator,
            port_manager.clone(),
            config_reconciler.currently_managed_zpools_rx().clone(),
            config_reconciler.available_datasets_rx(),
            long_running_task_handles.zone_bundler.clone(),
            vmm_reservoir_manager.clone(),
            metrics_manager.request_queue(),
        )?;

        let svc_config =
            services::Config::new(identifiers, config.sidecar_revision.clone());

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

        let artifact_store = Arc::new(
            ArtifactStore::new(
                &log,
                config_reconciler.internal_disks_rx().clone(),
                Some(Arc::clone(&config_reconciler)),
            )
            .await,
        );

        // Start reconciling against our ledgered sled config.
        config_reconciler.spawn_reconciliation_task(
            ReconcilerFacilities {
                etherstub_vnic,
                service_manager: services.clone(),
                metrics_queue: metrics_manager.request_queue(),
            },
            SledAgentArtifactStoreWrapper(Arc::clone(&artifact_store)),
            config_reconciler_spawn_token,
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

        let repo_depot =
            artifact_store.start(sled_address, &config.dropshot).await?;

        // Spawn a background task for managing notifications to nexus
        // about this sled-agent.
        let nexus_notifier_input = NexusNotifierInput {
            sled_id: request.body.id,
            sled_address: get_sled_address(request.body.subnet),
            repo_depot_port: repo_depot.local_addr().port(),
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
            request.body.id.into_untyped_uuid(),
            nexus_client.clone(),
            etherstub.clone(),
            port_manager.clone(),
            metrics_manager.request_queue(),
            config_reconciler.available_datasets_rx(),
            log.new(o!("component" => "ProbeManager")),
        );

        let currently_managed_zpools_rx =
            config_reconciler.currently_managed_zpools_rx().clone();

        let sled_agent = SledAgent {
            inner: Arc::new(SledAgentInner {
                id: request.body.id,
                subnet: request.body.subnet,
                start_request: request,
                config_reconciler,
                instances,
                probes,
                hardware: long_running_task_handles.hardware_manager.clone(),
                port_manager,
                services,
                nexus_notifier: nexus_notifier_handle,
                rack_network_config,
                zone_bundler: long_running_task_handles.zone_bundler.clone(),
                bootstore: long_running_task_handles.bootstore.clone(),
                hardware_monitor: long_running_task_handles
                    .hardware_monitor
                    .clone(),
                _metrics_manager: metrics_manager,
                repo_depot,
            }),
            log: log.clone(),
            sprockets: config.sprockets.clone(),
        };

        sled_agent.inner.probes.run(currently_managed_zpools_rx).await;

        // We immediately add a notification to the request queue about our
        // existence. If inspection of the hardware later informs us that we're
        // actually running on a scrimlet, that's fine, the updated value will
        // be received by Nexus eventually.
        sled_agent.notify_nexus_about_self(&log).await;

        Ok(sled_agent)
    }

    /// Accesses the [SupportBundleManager] API.
    pub(crate) fn as_support_bundle_storage(&self) -> SupportBundleManager<'_> {
        SupportBundleManager::new(&self.log, &*self.inner.config_reconciler)
    }

    /// Accesses the [SupportBundleLogs] API.
    pub(crate) fn as_support_bundle_logs(&self) -> SupportBundleLogs<'_> {
        SupportBundleLogs::new(
            &self.log,
            self.inner.config_reconciler.available_datasets_rx(),
        )
    }

    pub(crate) fn switch_zone_underlay_info(&self) -> UnderlayInfo {
        UnderlayInfo {
            ip: self.inner.switch_zone_ip(),
            rack_network_config: self.inner.rack_network_config.clone(),
        }
    }

    pub fn id(&self) -> SledUuid {
        self.inner.id
    }

    pub fn logger(&self) -> &Logger {
        &self.log
    }

    pub fn start_request(&self) -> &StartSledAgentRequest {
        &self.inner.start_request
    }

    pub fn sprockets(&self) -> SprocketsConfig {
        self.sprockets.clone()
    }

    pub(crate) fn hardware_monitor(&self) -> &HardwareMonitorHandle {
        &self.inner.hardware_monitor
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
        Zones::real_api()
            .get()
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

    /// Ensures that the specific sets of disks, datasets, and zones specified
    /// by `config` are running.
    pub async fn set_omicron_config(
        &self,
        config: OmicronSledConfig,
    ) -> Result<Result<(), LedgerNewConfigError>, LedgerTaskError> {
        self.inner.config_reconciler.set_sled_config(config).await
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
    pub async fn instance_ensure_registered_v1(
        &self,
        propolis_id: PropolisUuid,
        instance: sled_agent_types::instance::InstanceEnsureBody,
    ) -> Result<SledVmmState, Error> {
        // Convert v1 to v2
        let v5_instance = sled_agent_api::v5::InstanceEnsureBody {
            vmm_spec: instance.vmm_spec,
            local_config: sled_agent_api::v5::InstanceSledLocalConfig {
                hostname: instance.local_config.hostname,
                nics: instance.local_config.nics,
                source_nat: instance.local_config.source_nat,
                ephemeral_ip: instance.local_config.ephemeral_ip,
                floating_ips: instance.local_config.floating_ips,
                multicast_groups: Vec::new(), // v1 doesn't support multicast
                firewall_rules: instance.local_config.firewall_rules,
                dhcp_config: instance.local_config.dhcp_config,
            },
            vmm_runtime: instance.vmm_runtime,
            instance_id: instance.instance_id,
            migration_id: instance.migration_id,
            propolis_addr: instance.propolis_addr,
            metadata: instance.metadata,
        };
        self.instance_ensure_registered_v5(propolis_id, v5_instance).await
    }

    pub async fn instance_ensure_registered_v5(
        &self,
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
    ) -> Result<SledVmmState, Error> {
        self.instance_ensure_registered(propolis_id, instance).await
    }

    async fn instance_ensure_registered(
        &self,
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
    ) -> Result<SledVmmState, Error> {
        self.inner
            .instances
            .ensure_registered(propolis_id, instance, self.sled_identifiers())
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
        propolis_id: PropolisUuid,
    ) -> Result<VmmUnregisterResponse, Error> {
        self.inner
            .instances
            .ensure_unregistered(propolis_id)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently drives the specified instance into the specified target
    /// state.
    pub async fn instance_ensure_state(
        &self,
        propolis_id: PropolisUuid,
        target: VmmStateRequested,
    ) -> Result<VmmPutStateResponse, Error> {
        self.inner
            .instances
            .ensure_state(propolis_id, target)
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
        propolis_id: PropolisUuid,
        external_ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .add_external_ip(propolis_id, external_ip)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that an instance's OPTE/port state does not include the
    /// specified external IP address in either its ephemeral or floating IP set.
    pub async fn instance_delete_external_ip(
        &self,
        propolis_id: PropolisUuid,
        external_ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .delete_external_ip(propolis_id, external_ip)
            .await
            .map_err(|e| Error::Instance(e))
    }

    pub async fn instance_join_multicast_group(
        &self,
        propolis_id: PropolisUuid,
        multicast_body: &InstanceMulticastBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .join_multicast_group(propolis_id, multicast_body)
            .await
            .map_err(|e| Error::Instance(e))
    }

    pub async fn instance_leave_multicast_group(
        &self,
        propolis_id: PropolisUuid,
        multicast_body: &InstanceMulticastBody,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .leave_multicast_group(propolis_id, multicast_body)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Returns the state of the instance with the provided ID.
    pub async fn instance_get_state(
        &self,
        propolis_id: PropolisUuid,
    ) -> Result<SledVmmState, Error> {
        self.inner
            .instances
            .get_instance_state(propolis_id)
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

    pub fn artifact_store(&self) -> &ArtifactStore<InternalDisksReceiver> {
        &self.inner.repo_depot.app_private()
    }

    /// Issue a snapshot request for a Crucible disk attached to an instance
    pub async fn vmm_issue_disk_snapshot_request(
        &self,
        propolis_id: PropolisUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .issue_disk_snapshot_request(propolis_id, disk_id, snapshot_id)
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

    pub async fn set_eip_gateways(
        &self,
        mappings: ExternalIpGatewayMap,
    ) -> Result<(), Error> {
        info!(
            self.log,
            "IGW mapping received";
            "values" => ?mappings
        );
        let changed = self.inner.port_manager.set_eip_gateways(mappings);

        // TODO(kyle)
        // There is a substantial downside to this approach, which is that
        // we can currently only do correct Internet Gateway association for
        // *Instances* -- sled agent does not remember the ExtIPs associated
        // with Services or with Probes.
        //
        // In practice, services should not have more than one IGW. Not having
        // identical source IP selection for Probes is a little sad, though.
        // OPTE will follow the old (single-IGW) behaviour when no mappings
        // are installed.
        //
        // My gut feeling is that the correct place for External IPs to
        // live is on each NetworkInterface, which makes it far simpler for
        // nexus to administer and add/remove IPs on *all* classes of port
        // via RPW. This is how we would make this correct in general.
        // My understanding is that NetworkInterface's schema makes its way into
        // the ledger, and I'm not comfortable redoing that this close to a release.
        if changed {
            self.inner.instances.refresh_external_ips().await?;
            info!(self.log, "IGW mapping changed; external IPs refreshed");
        }

        Ok(())
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
            sled_id: self.inner.id.into_untyped_uuid(),
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
        let cpu_family = self.inner.hardware.cpu_family();
        let reservoir_size = self.inner.instances.reservoir_size();
        let sled_role =
            if is_scrimlet { SledRole::Scrimlet } else { SledRole::Gimlet };
        let zone_image_resolver =
            self.inner.services.zone_image_resolver().status().to_inventory();

        let ReconcilerInventory {
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
        } = self.inner.config_reconciler.inventory(&self.log).await?;

        Ok(Inventory {
            sled_id,
            sled_agent_address,
            sled_role,
            baseboard,
            usable_hardware_threads,
            usable_physical_ram: ByteCount::try_from(usable_physical_ram)?,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
        })
    }

    pub(crate) async fn support_zoneadm_info(
        &self,
    ) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
        sled_diagnostics::zoneadm_info().await
    }

    pub(crate) async fn support_ipadm_info(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::ipadm_info().await
    }

    pub(crate) async fn support_dladm_info(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::dladm_info().await
    }

    pub(crate) async fn support_nvmeadm_info(
        &self,
    ) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
        sled_diagnostics::nvmeadm_info().await
    }

    pub(crate) async fn support_pargs_info(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::pargs_oxide_processes(&self.log).await
    }

    pub(crate) async fn support_pstack_info(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::pstack_oxide_processes(&self.log).await
    }

    pub(crate) async fn support_pfiles_info(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::pfiles_oxide_processes(&self.log).await
    }

    pub(crate) async fn support_zfs_info(
        &self,
    ) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
        sled_diagnostics::zfs_info().await
    }

    pub(crate) async fn support_zpool_info(
        &self,
    ) -> Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError> {
        sled_diagnostics::zpool_info().await
    }

    pub(crate) async fn support_health_check(
        &self,
    ) -> Vec<Result<SledDiagnosticsCmdOutput, SledDiagnosticsCmdError>> {
        sled_diagnostics::health_check().await
    }
}

#[derive(From, thiserror::Error, Debug, SlogInlineError)]
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
        #[source]
        err: crate::bootstrap::client::Error,
    },
}

/// Add a sled to an initialized rack.
pub async fn sled_add(
    log: Logger,
    sprockets_config: SprocketsConfig,
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
        sprockets_config,
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

struct ReconcilerFacilities {
    etherstub_vnic: EtherstubVnic,
    service_manager: ServiceManager,
    metrics_queue: MetricsRequestQueue,
}

impl SledAgentFacilities for ReconcilerFacilities {
    fn underlay_vnic(&self) -> &EtherstubVnic {
        &self.etherstub_vnic
    }

    fn on_time_sync(&self) {
        self.service_manager.on_time_sync();
    }

    async fn start_omicron_zone(
        &self,
        prepared_zone: PreparedOmicronZone<'_>,
        zone_root_path: PathInPool,
    ) -> anyhow::Result<RunningZone> {
        let zone = self
            .service_manager
            .start_omicron_zone(prepared_zone, zone_root_path)
            .await?;
        Ok(zone)
    }

    fn zone_image_resolver_status(&self) -> ResolverStatus {
        self.service_manager.zone_image_resolver().status()
    }

    fn remove_mupdate_override(
        &self,
        override_id: MupdateOverrideUuid,
        internal_disks: &InternalDisks,
    ) -> RemoveMupdateOverrideResult {
        self.service_manager
            .zone_image_resolver()
            .remove_mupdate_override(override_id, internal_disks)
    }

    fn metrics_untrack_zone_links(
        &self,
        zone: &RunningZone,
    ) -> anyhow::Result<()> {
        match self.metrics_queue.untrack_zone_links(zone) {
            Ok(()) => Ok(()),
            Err(errors) => {
                let mut errors =
                    errors.iter().map(|err| InlineErrorChain::new(err));
                Err(anyhow!(
                    "{} errors untracking zone links: {}",
                    errors.len(),
                    errors.join(", ")
                ))
            }
        }
    }

    fn ddm_remove_internal_dns_prefix(&self, prefix: Ipv6Subnet<SLED_PREFIX>) {
        self.service_manager
            .ddm_reconciler()
            .remove_internal_dns_subnet(prefix);
    }
}

// Workaround wrapper for orphan rules.
#[derive(Clone)]
struct SledAgentArtifactStoreWrapper(Arc<ArtifactStore<InternalDisksReceiver>>);

impl SledAgentArtifactStore for SledAgentArtifactStoreWrapper {
    async fn get_artifact(
        &self,
        artifact: ArtifactHash,
    ) -> anyhow::Result<tokio::fs::File> {
        let file = self.0.get(artifact).await?;
        Ok(file)
    }
}
