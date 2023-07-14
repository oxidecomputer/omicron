// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

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
use camino::Utf8PathBuf;
use dropshot::HttpError;
use illumos_utils::opte::params::SetVirtualNetworkInterfaceHost;
use illumos_utils::opte::PortManager;
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, SLED_PREFIX,
};
use omicron_common::api::external::Vni;
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifactId,
};
use omicron_common::backoff::{
    retry_notify_ext, retry_policy_internal_service_aggressive, BackoffError,
};
use sled_hardware::underlay;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(not(test))]
use illumos_utils::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use illumos_utils::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("Error setting up swap device: {0}")]
    SwapDevice(#[from] crate::swap_device::SwapDeviceError),

    #[error("Failed to acquire etherstub: {0}")]
    Etherstub(illumos_utils::ExecutionError),

    #[error("Failed to acquire etherstub VNIC: {0}")]
    EtherstubVnic(illumos_utils::dladm::CreateVnicError),

    #[error("Bootstrap error: {0}")]
    Bootstrap(#[from] crate::bootstrap::agent::BootstrapError),

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
            crate::sled_agent::Error::Services(
                crate::services::Error::Bundle(ref inner),
            ) => match inner {
                crate::services::BundleError::NoStorage => {
                    HttpError::for_unavail(None, inner.to_string())
                }
                crate::services::BundleError::NoSuchZone { .. } => {
                    HttpError::for_not_found(None, inner.to_string())
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

    /// Component of Sled Agent responsible for managing OPTE ports.
    port_manager: PortManager,

    // Other Oxide-controlled services running on this Sled.
    services: ServiceManager,

    // Connection to Nexus.
    nexus_client: NexusClientWithResolver,

    // A serialized request queue for operations interacting with Nexus.
    nexus_request_queue: NexusRequestQueue,
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
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub async fn new(
        config: &Config,
        log: Logger,
        nexus_client: NexusClientWithResolver,
        request: StartSledAgentRequest,
        services: ServiceManager,
        storage: StorageManager,
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
        let underlay_nics = underlay::find_nics()?;
        illumos_utils::opte::initialize_xde_driver(&log, &underlay_nics)?;

        // Create the PortManager to manage all the OPTE ports on the sled.
        let port_manager = PortManager::new(
            parent_log.new(o!("component" => "PortManager")),
            *sled_address.ip(),
        );

        storage
            .setup_underlay_access(storage_manager::UnderlayAccess {
                nexus_client: nexus_client.clone(),
                sled_id: request.id,
            })
            .await?;

        let hardware = HardwareManager::new(&parent_log, services.sled_mode())
            .map_err(|e| Error::Hardware(e))?;

        let instances = InstanceManager::new(
            parent_log.clone(),
            nexus_client.clone(),
            etherstub.clone(),
            port_manager.clone(),
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
        services
            .sled_agent_started(
                svc_config,
                port_manager.clone(),
                *sled_address.ip(),
                request.rack_id,
            )
            .await?;

        let sled_agent = SledAgent {
            inner: Arc::new(SledAgentInner {
                id: request.id,
                subnet: request.subnet,
                storage,
                instances,
                hardware,
                updates,
                port_manager,
                services,
                nexus_client,

                // TODO(https://github.com/oxidecomputer/omicron/issues/1917):
                // Propagate usage of this request queue throughout the Sled Agent.
                //
                // Also, we could maybe de-dup some of the backoff code in the request queue?
                nexus_request_queue: NexusRequestQueue::new(),
            }),
        };

        // We immediately add a notification to the request queue about our
        // existence. If inspection of the hardware later informs us that we're
        // actually running on a scrimlet, that's fine, the updated value will
        // be received by Nexus eventually.
        sled_agent.notify_nexus_about_self(&log);

        // Begin monitoring the underlying hardware, and reacting to changes.
        let sa = sled_agent.clone();
        tokio::spawn(async move {
            sa.hardware_monitor_task(log).await;
        });

        // Finally, load services for which we're already responsible.
        //
        // Do this *after* monitoring for harware, to enable the switch zone to
        // establish an underlay address before proceeding.
        sled_agent.inner.services.load_services().await?;
        Ok(sled_agent)
    }

    // Observe the current hardware state manually.
    //
    // We use this when we're monitoring hardware for the first
    // time, and if we miss notifications.
    async fn full_hardware_scan(&self, log: &Logger) {
        info!(log, "Performing full hardware scan");
        self.notify_nexus_about_self(log);

        let scrimlet = self.inner.hardware.is_scrimlet_driver_loaded();

        if scrimlet {
            let baseboard = self.inner.hardware.baseboard();
            let switch_zone_ip = Some(self.inner.switch_zone_ip());
            if let Err(e) = self
                .inner
                .services
                .activate_switch(switch_zone_ip, baseboard)
                .await
            {
                warn!(log, "Failed to activate switch: {e}");
            }
        } else {
            if let Err(e) = self.inner.services.deactivate_switch().await {
                warn!(log, "Failed to deactivate switch: {e}");
            }
        }

        self.inner
            .storage
            .ensure_using_exactly_these_disks(self.inner.hardware.disks())
            .await;
    }

    async fn hardware_monitor_task(&self, log: Logger) {
        // Start monitoring the hardware for changes
        let mut hardware_updates = self.inner.hardware.monitor();

        // Scan the system manually for events we have have missed
        // before we started monitoring.
        self.full_hardware_scan(&log).await;

        // Rely on monitoring for tracking all future updates.
        loop {
            use sled_hardware::HardwareUpdate;
            use tokio::sync::broadcast::error::RecvError;
            match hardware_updates.recv().await {
                Ok(update) => match update {
                    HardwareUpdate::TofinoDeviceChange => {
                        // Inform Nexus that we're now a scrimlet, instead of a Gimlet.
                        //
                        // This won't block on Nexus responding; it may take while before
                        // Nexus actually comes online.
                        self.notify_nexus_about_self(&log);
                    }
                    HardwareUpdate::TofinoLoaded => {
                        let baseboard = self.inner.hardware.baseboard();
                        let switch_zone_ip = Some(self.inner.switch_zone_ip());
                        if let Err(e) = self
                            .inner
                            .services
                            .activate_switch(switch_zone_ip, baseboard)
                            .await
                        {
                            warn!(log, "Failed to activate switch: {e}");
                        }
                    }
                    HardwareUpdate::TofinoUnloaded => {
                        if let Err(e) =
                            self.inner.services.deactivate_switch().await
                        {
                            warn!(log, "Failed to deactivate switch: {e}");
                        }
                    }
                    HardwareUpdate::DiskAdded(disk) => {
                        self.inner.storage.upsert_disk(disk).await;
                    }
                    HardwareUpdate::DiskRemoved(disk) => {
                        self.inner.storage.delete_disk(disk).await;
                    }
                },
                Err(RecvError::Lagged(count)) => {
                    warn!(log, "Hardware monitor missed {count} messages");
                    self.full_hardware_scan(&log).await;
                }
                Err(RecvError::Closed) => {
                    warn!(log, "Hardware monitor receiver closed; exiting");
                    return;
                }
            }
        }
    }

    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    // Sends a request to Nexus informing it that the current sled exists.
    fn notify_nexus_about_self(&self, log: &Logger) {
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

    /// List zone bundles for the provided zone.
    pub async fn list_zone_bundles(
        &self,
        name: &str,
    ) -> Result<Vec<ZoneBundleMetadata>, Error> {
        self.inner.services.list_zone_bundles(name).await.map_err(Error::from)
    }

    /// Create a zone bundle for the provided zone.
    pub async fn create_zone_bundle(
        &self,
        name: &str,
    ) -> Result<ZoneBundleMetadata, Error> {
        self.inner.services.create_zone_bundle(name).await.map_err(Error::from)
    }

    /// Fetch the path to a zone bundle.
    pub async fn get_zone_bundle_path(
        &self,
        name: &str,
        id: &Uuid,
    ) -> Result<Option<Utf8PathBuf>, Error> {
        self.inner
            .services
            .get_zone_bundle_path(name, id)
            .await
            .map_err(Error::from)
    }

    /// List the zones that the sled agent is currently managing.
    pub async fn zones_list(&self) -> Result<Vec<String>, Error> {
        self.inner.services.list_all_zones().await.map_err(Error::from)
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

        self.inner.services.ensure_all_services(requested_services).await?;
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
        mapping: &SetVirtualNetworkInterfaceHost,
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
