// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::bootstrap::early_networking::{
    EarlyNetworkConfig, EarlyNetworkSetupError,
};
use crate::bootstrap::params::StartSledAgentRequest;
use crate::config::Config;
use crate::instance_manager::InstanceManager;
use crate::nexus::{NexusClientWithResolver, NexusRequestQueue};
use crate::params::{
    DiskStateRequested, InstanceHardware, InstanceMigrationSourceParams,
    InstancePutStateResponse, InstanceStateRequested,
    InstanceUnregisterResponse, ServiceEnsureBody, SledRole, TimeSync,
    VpcFirewallRule, ZoneBundleMetadata, Zpool,
};
use crate::services::{self, ServiceManager};
use crate::storage_manager::{self, StorageManager};
use crate::updates::{ConfigUpdates, UpdateManager};
use crate::zone_bundle;
use crate::zone_bundle::BundleError;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use dropshot::HttpError;
use helios_fusion::BoxedExecutor;
use illumos_utils::dladm::Dladm;
use illumos_utils::opte::params::{
    DeleteVirtualNetworkInterfaceHost, SetVirtualNetworkInterfaceHost,
};
use illumos_utils::opte::PortManager;
use illumos_utils::zone::Zones;
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use illumos_utils::zone::ZONE_PREFIX;
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, SLED_PREFIX,
};
use omicron_common::api::external::Vni;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifactId,
};
use omicron_common::backoff::{
    retry_notify, retry_notify_ext, retry_policy_internal_service_aggressive,
    BackoffError,
};
use sled_hardware::underlay;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("Error setting up swap device: {0}")]
    SwapDevice(#[from] crate::swap_device::SwapDeviceError),

    #[error("Failed to acquire etherstub: {0}")]
    Etherstub(helios_fusion::ExecutionError),

    #[error("Failed to acquire etherstub VNIC: {0}")]
    EtherstubVnic(illumos_utils::dladm::CreateVnicError),

    #[error("Bootstrap error: {0}")]
    Bootstrap(#[from] crate::bootstrap::BootstrapError),

    #[error("Failed to remove Omicron address: {0}")]
    DeleteAddress(#[from] helios_fusion::ExecutionError),

    #[error("Failed to operate on underlay device: {0}")]
    Underlay(#[from] underlay::Error),

    #[error(transparent)]
    Services(#[from] crate::services::Error),

    #[error("Failed to create Sled Subnet: {err}")]
    SledSubnet { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Error managing instances: {0}")]
    Instance(#[from] crate::instance_manager::Error),

    #[error("Error managing storage: {0}")]
    Storage(#[from] crate::storage_manager::Error),

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
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

// Provide a more specific HTTP error for some sled agent errors.
impl From<Error> for dropshot::HttpError {
    fn from(err: Error) -> Self {
        match err {
            crate::sled_agent::Error::Instance(instance_manager_error) => {
                match instance_manager_error {
                    crate::instance_manager::Error::Instance(
                        instance_error,
                    ) => match instance_error {
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
                        e => HttpError::for_internal_error(e.to_string()),
                    },
                    e => HttpError::for_internal_error(e.to_string()),
                }
            }
            crate::sled_agent::Error::ZoneBundle(ref inner) => match inner {
                BundleError::NoStorage | BundleError::Unavailable { .. } => {
                    HttpError::for_unavail(None, inner.to_string())
                }
                BundleError::NoSuchZone { .. } => {
                    HttpError::for_not_found(None, inner.to_string())
                }
                BundleError::InvalidStorageLimit
                | BundleError::InvalidCleanupPeriod => {
                    HttpError::for_bad_request(None, inner.to_string())
                }
                _ => HttpError::for_internal_error(err.to_string()),
            },
            e => HttpError::for_internal_error(e.to_string()),
        }
    }
}

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
struct SledAgentInner {
    // ID of the Sled
    id: Uuid,

    // Sled Agent's interaction with the host system
    executor: BoxedExecutor,

    // Subnet of the Sled's underlay.
    //
    // The Sled Agent's address can be derived from this value.
    subnet: Ipv6Subnet<SLED_PREFIX>,

    // Component of Sled Agent responsible for storage and dataset management.
    storage: StorageManager,

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
    nexus_client: NexusClientWithResolver,

    // A serialized request queue for operations interacting with Nexus.
    nexus_request_queue: NexusRequestQueue,

    // The rack network config provided at RSS time.
    rack_network_config: Option<RackNetworkConfig>,

    // Object managing zone bundles.
    zone_bundler: zone_bundle::ZoneBundler,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: &Config,
        log: Logger,
        executor: &BoxedExecutor,
        nexus_client: NexusClientWithResolver,
        request: StartSledAgentRequest,
        services: ServiceManager,
        storage: StorageManager,
        bootstore: bootstore::NodeHandle,
    ) -> Result<SledAgent, Error> {
        // Pass the "parent_log" to all subcomponents that want to set their own
        // "component" value.
        let parent_log = log.clone();

        // Use "log" for ourself.
        let log = log.new(o!(
            "component" => "SledAgent",
            "sled_id" => request.id.to_string(),
        ));
        info!(&log, "SledAgent::new(..) starting");

        // Configure a swap device of the configured size before other system setup.
        match config.swap_device_size_gb {
            Some(sz) if sz > 0 => {
                info!(log, "Requested swap device of size {} GiB", sz);
                let boot_disk =
                    storage.resources().boot_disk().await.ok_or_else(|| {
                        crate::swap_device::SwapDeviceError::BootDiskNotFound
                    })?;
                crate::swap_device::ensure_swap_device(
                    &parent_log,
                    executor,
                    &boot_disk.1,
                    sz,
                )?;
            }
            Some(sz) if sz == 0 => {
                panic!("Invalid requested swap device size of 0 GiB");
            }
            None | Some(_) => {
                info!(log, "Not setting up swap device: not configured");
            }
        }

        // Ensure we have a thread that automatically reaps process contracts
        // when they become empty. See the comments in
        // illumos-utils/src/running_zone.rs for more detail.
        illumos_utils::running_zone::ensure_contract_reaper(&parent_log);

        // TODO-correctness Bootstrap-agent already ensures the underlay
        // etherstub and etherstub VNIC exist on startup - could it pass them
        // through to us?
        let etherstub = Dladm::ensure_etherstub(
            executor,
            illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .map_err(|e| Error::Etherstub(e))?;
        let etherstub_vnic = Dladm::ensure_etherstub_vnic(executor, &etherstub)
            .map_err(|e| Error::EtherstubVnic(e))?;

        // Ensure the global zone has a functioning IPv6 address.
        let sled_address = request.sled_address();
        Zones::ensure_has_global_zone_v6_address(
            executor,
            etherstub_vnic.clone(),
            *sled_address.ip(),
            "sled6",
        )
        .map_err(|err| Error::SledSubnet { err })?;

        // Initialize the xde kernel driver with the underlay devices.
        let underlay_nics = underlay::find_nics(executor, &config.data_links)?;
        illumos_utils::opte::initialize_xde_driver(&log, &underlay_nics)?;

        // Create the PortManager to manage all the OPTE ports on the sled.
        let port_manager = PortManager::new(
            parent_log.new(o!("component" => "PortManager")),
            executor,
            *sled_address.ip(),
        );

        storage
            .setup_underlay_access(storage_manager::UnderlayAccess {
                nexus_client: nexus_client.clone(),
                sled_id: request.id,
            })
            .await?;

        // TODO-correctness The bootstrap agent _also_ has a `HardwareManager`.
        // We only use it for reading properties, but it's not `Clone`able
        // because it's holding an inner task handle. Could we add a way to get
        // a read-only handle to it, and have bootstrap agent give us that
        // instead of creating a new full one ourselves?
        let hardware = HardwareManager::new(&parent_log, services.sled_mode())
            .map_err(|e| Error::Hardware(e))?;

        let instances = InstanceManager::new(
            parent_log.clone(),
            executor,
            nexus_client.clone(),
            etherstub.clone(),
            port_manager.clone(),
            storage.resources().clone(),
            storage.zone_bundler().clone(),
        )?;

        match config.vmm_reservoir_percentage {
            Some(sz) if sz > 0 && sz < 100 => {
                instances.set_reservoir_size(&hardware, sz).map_err(|e| {
                    error!(log, "Failed to set VMM reservoir size: {e}");
                    e
                })?;
            }
            Some(sz) if sz == 0 => {
                warn!(log, "Not using VMM reservoir (size 0 bytes requested)");
            }
            None => {
                warn!(log, "Not using VMM reservoir");
            }
            Some(sz) => {
                panic!("invalid requested VMM reservoir percentage: {}", sz);
            }
        }

        let update_config = ConfigUpdates {
            zone_artifact_path: Utf8PathBuf::from("/opt/oxide"),
        };
        let updates = UpdateManager::new(update_config);

        let svc_config =
            services::Config::new(request.id, config.sidecar_revision.clone());

        // Get our rack network config from the bootstore; we cannot proceed
        // until we have this, as we need to know which switches have uplinks to
        // correctly set up services.
        let get_network_config = || async {
            let serialized_config = bootstore
                .get_network_config()
                .await
                .map_err(|err| BackoffError::transient(err.to_string()))?
                .ok_or_else(|| {
                    BackoffError::transient(
                        "Missing early network config in bootstore".to_string(),
                    )
                })?;

            let early_network_config =
                EarlyNetworkConfig::try_from(serialized_config)
                    .map_err(|err| BackoffError::transient(err.to_string()))?;

            Ok(early_network_config.rack_network_config)
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

        services.sled_agent_started(
            svc_config,
            port_manager.clone(),
            *sled_address.ip(),
            request.rack_id,
            rack_network_config.clone(),
        )?;

        let zone_bundler = storage.zone_bundler().clone();
        let sled_agent = SledAgent {
            inner: Arc::new(SledAgentInner {
                id: request.id,
                executor: executor.clone(),
                subnet: request.subnet,
                storage,
                instances,
                hardware,
                updates,
                port_manager,
                services,
                nexus_client,

                // TODO(https://github.com/oxidecomputer/omicron/issues/1917):
                // Propagate usage of this request queue throughout the Sled
                // Agent.
                //
                // Also, we could maybe de-dup some of the backoff code in the
                // request queue?
                nexus_request_queue: NexusRequestQueue::new(),
                rack_network_config,
                zone_bundler,
            }),
            log: log.clone(),
        };

        // We immediately add a notification to the request queue about our
        // existence. If inspection of the hardware later informs us that we're
        // actually running on a scrimlet, that's fine, the updated value will
        // be received by Nexus eventually.
        sled_agent.notify_nexus_about_self(&log);

        Ok(sled_agent)
    }

    /// Load services for which we're responsible; only meaningful to call
    /// during a cold boot.
    ///
    /// Blocks until all services have started, retrying indefinitely on
    /// failure.
    pub(crate) async fn cold_boot_load_services(&self) {
        retry_notify(
            retry_policy_internal_service_aggressive(),
            || async {
                self.inner
                    .services
                    .load_services()
                    .await
                    .map_err(|err| BackoffError::transient(err))
            },
            |err, delay| {
                warn!(
                    self.log,
                    "Failed to load services, will retry in {:?}", delay;
                    "error" => %err,
                );
            },
        )
        .await
        .unwrap(); // we retry forever, so this can't fail

        // Now that we've initialized the sled services, notify nexus again
        // at which point it'll plumb any necessary firewall rules back to us.
        self.notify_nexus_about_self(&self.log);
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

    // Sends a request to Nexus informing it that the current sled exists.
    pub(crate) fn notify_nexus_about_self(&self, log: &Logger) {
        let sled_id = self.inner.id;
        let nexus_client = self.inner.nexus_client.clone();
        let sled_address = self.inner.sled_address();
        let is_scrimlet = self.inner.hardware.is_scrimlet();
        let baseboard = nexus_client::types::Baseboard::from(
            self.inner.hardware.baseboard(),
        );
        let usable_hardware_threads =
            self.inner.hardware.online_processor_count();
        let usable_physical_ram =
            self.inner.hardware.usable_physical_ram_bytes();
        let reservoir_size = self.inner.instances.reservoir_size();

        let log = log.clone();
        let fut = async move {
            // Notify the control plane that we're up, and continue trying this
            // until it succeeds. We retry with an randomized, capped
            // exponential backoff.
            //
            // TODO-robustness if this returns a 400 error, we probably want to
            // return a permanent error from the `notify_nexus` closure.
            let notify_nexus = || async {
                info!(
                    log,
                    "contacting server nexus, registering sled";
                    "id" => ?sled_id,
                    "baseboard" => ?baseboard,
                );
                let role = if is_scrimlet {
                    nexus_client::types::SledRole::Scrimlet
                } else {
                    nexus_client::types::SledRole::Gimlet
                };

                nexus_client
                    .client()
                    .sled_agent_put(
                        &sled_id,
                        &nexus_client::types::SledAgentStartupInfo {
                            sa_address: sled_address.to_string(),
                            role,
                            baseboard: baseboard.clone(),
                            usable_hardware_threads,
                            usable_physical_ram: nexus_client::types::ByteCount(
                                usable_physical_ram,
                            ),
                            reservoir_size: nexus_client::types::ByteCount(
                                reservoir_size.to_bytes(),
                            ),
                        },
                    )
                    .await
                    .map_err(|err| BackoffError::transient(err.to_string()))
            };
            // This notification is often invoked before Nexus has started
            // running, so avoid flagging any errors as concerning until some
            // time has passed.
            let log_notification_failure = |err, call_count, total_duration| {
                if call_count == 0 {
                    info!(
                        log,
                        "failed to notify nexus about sled agent"; "error" => err,
                    );
                } else if total_duration > std::time::Duration::from_secs(30) {
                    warn!(
                        log,
                        "failed to notify nexus about sled agent"; "error" => err, "total duration" => ?total_duration,
                    );
                }
            };
            retry_notify_ext(
                retry_policy_internal_service_aggressive(),
                notify_nexus,
                log_notification_failure,
            )
            .await
            .expect("Expected an infinite retry loop contacting Nexus");
        };
        self.inner
            .nexus_request_queue
            .sender()
            .send(Box::pin(fut))
            .unwrap_or_else(|err| {
                panic!("Failed to send future to request queue: {err}");
            });
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
        Zones::get(&self.inner.executor)
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
    pub async fn zone_bundle_cleanup_context(
        &self,
    ) -> zone_bundle::CleanupContext {
        self.inner.zone_bundler.cleanup_context().await
    }

    /// Update the zone bundle cleanup context.
    pub async fn update_zone_bundle_cleanup_context(
        &self,
        period: Option<zone_bundle::CleanupPeriod>,
        storage_limit: Option<zone_bundle::StorageLimit>,
        priority: Option<zone_bundle::PriorityOrder>,
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
    ) -> Result<BTreeMap<Utf8PathBuf, zone_bundle::BundleUtilization>, Error>
    {
        self.inner.zone_bundler.utilization().await.map_err(Error::from)
    }

    /// Trigger an explicit request to cleanup old zone bundles.
    pub async fn zone_bundle_cleanup(
        &self,
    ) -> Result<BTreeMap<Utf8PathBuf, zone_bundle::CleanupCount>, Error> {
        self.inner.zone_bundler.cleanup().await.map_err(Error::from)
    }

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, will be recorded
    /// to a local file to ensure they start automatically on next boot.
    pub async fn services_ensure(
        &self,
        requested_services: ServiceEnsureBody,
    ) -> Result<(), Error> {
        let datasets: Vec<_> = requested_services
            .services
            .iter()
            .filter_map(|service| service.dataset.clone())
            .collect();

        // TODO:
        // - If these are the set of filesystems, we should also consider
        // removing the ones which are not listed here.
        // - It's probably worth sending a bulk request to the storage system,
        // rather than requesting individual datasets.
        for dataset in &datasets {
            // First, ensure the dataset exists
            self.inner
                .storage
                .upsert_filesystem(dataset.id, dataset.name.clone())
                .await?;
        }

        self.inner
            .services
            .ensure_all_services_persistent(requested_services)
            .await?;
        Ok(())
    }

    pub async fn cockroachdb_initialize(&self) -> Result<(), Error> {
        self.inner.services.cockroachdb_initialize().await?;
        Ok(())
    }

    /// Gets the sled's current list of all zpools.
    pub async fn zpools_get(&self) -> Result<Vec<Zpool>, Error> {
        let zpools = self.inner.storage.get_zpools().await?;
        Ok(zpools)
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
    /// [`instance_ensure_state`].
    pub async fn instance_ensure_registered(
        &self,
        instance_id: Uuid,
        initial: InstanceHardware,
    ) -> Result<InstanceRuntimeState, Error> {
        self.inner
            .instances
            .ensure_registered(instance_id, initial)
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
        instance_id: Uuid,
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
        instance_id: Uuid,
        target: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        self.inner
            .instances
            .ensure_state(instance_id, target)
            .await
            .map_err(|e| Error::Instance(e))
    }

    /// Idempotently ensures that the instance's runtime state contains the
    /// supplied migration IDs, provided that the caller continues to meet the
    /// conditions needed to change those IDs. See the doc comments for
    /// [`crate::params::InstancePutMigrationIdsBody`].
    pub async fn instance_put_migration_ids(
        &self,
        instance_id: Uuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        self.inner
            .instances
            .put_migration_ids(instance_id, old_runtime, migration_ids)
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
            .download_artifact(artifact, &self.inner.nexus_client.client())
            .await?;
        Ok(())
    }

    /// Issue a snapshot request for a Crucible disk attached to an instance
    pub async fn instance_issue_disk_snapshot_request(
        &self,
        instance_id: Uuid,
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
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        self.inner
            .port_manager
            .firewall_rules_ensure(vpc_vni, rules)
            .map_err(Error::from)
    }

    pub async fn set_virtual_nic_host(
        &self,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        self.inner
            .port_manager
            .set_virtual_nic_host(mapping)
            .map_err(Error::from)
    }

    pub async fn unset_virtual_nic_host(
        &self,
        mapping: &DeleteVirtualNetworkInterfaceHost,
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
}
