// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::bootstrap::params::SledAgentRequest;
use crate::config::Config;
use crate::instance_manager::InstanceManager;
use crate::nexus::{LazyNexusClient, NexusRequestQueue};
use crate::params::VpcFirewallRule;
use crate::params::{
    DatasetKind, DiskStateRequested, InstanceHardware, InstanceMigrateParams,
    InstanceRuntimeStateRequested, InstanceSerialConsoleData,
    ServiceEnsureBody, Zpool,
};
use crate::services::{self, ServiceManager};
use crate::storage_manager::StorageManager;
use crate::updates::{ConfigUpdates, UpdateManager};
use dropshot::HttpError;
use illumos_utils::opte::params::SetVirtualNetworkInterfaceHost;
use illumos_utils::{execute, PFEXEC};
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, SLED_PREFIX,
};
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifactId,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use sled_hardware::underlay;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use crate::serial::ByteOffset;
#[cfg(not(test))]
use illumos_utils::{dladm::Dladm, zone::Zones};
#[cfg(test)]
use illumos_utils::{dladm::MockDladm as Dladm, zone::MockZones as Zones};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("Failed to enable routing: {0}")]
    EnablingRouting(illumos_utils::ExecutionError),

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
    ResolveError(#[from] dns_service_client::multiclient::ResolveError),
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
                            match propolis_error.status() {
                                None => HttpError::for_internal_error(
                                    propolis_error.to_string(),
                                ),

                                Some(status_code) => {
                                    HttpError::for_status(None, status_code)
                                }
                            }
                        }

                        e => HttpError::for_internal_error(e.to_string()),
                    },

                    e => HttpError::for_internal_error(e.to_string()),
                }
            }

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

    // Other Oxide-controlled services running on this Sled.
    services: ServiceManager,

    // Lazily-acquired connection to Nexus.
    lazy_nexus_client: LazyNexusClient,

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
        lazy_nexus_client: LazyNexusClient,
        request: SledAgentRequest,
        services: ServiceManager,
    ) -> Result<SledAgent, Error> {
        // Pass the "parent_log" to all subcomponents that want to set their own
        // "component" value.
        let parent_log = log.clone();

        // Use "log" for ourself.
        let log = log.new(o!(
            "component" => "SledAgent",
            "sled_id" => request.id.to_string(),
        ));
        info!(&log, "created sled agent");

        let etherstub = Dladm::ensure_etherstub(
            illumos_utils::dladm::UNDERLAY_ETHERSTUB_NAME,
        )
        .map_err(|e| Error::Etherstub(e))?;
        let etherstub_vnic = Dladm::ensure_etherstub_vnic(&etherstub)
            .map_err(|e| Error::EtherstubVnic(e))?;

        // Ensure the global zone has a functioning IPv6 address.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/821): This
        // should be removed once the Sled Agent is initialized with a
        // RSS-provided IP address. In the meantime, we use one from the
        // configuration file.
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

        // Ipv6 forwarding must be enabled to route traffic between zones.
        //
        // This should be a no-op if already enabled.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "/usr/sbin/routeadm",
            // Needed to access all zones, which are on the underlay.
            "-e",
            "ipv6-forwarding",
            "-u",
        ]);
        execute(cmd).map_err(|e| Error::EnablingRouting(e))?;

        let storage = StorageManager::new(
            &parent_log,
            request.id,
            lazy_nexus_client.clone(),
            etherstub.clone(),
            *sled_address.ip(),
        )
        .await;
        if let Some(pools) = &config.zpools {
            for pool in pools {
                info!(
                    log,
                    "Sled Agent upserting zpool to Storage Manager: {}",
                    pool.to_string()
                );
                storage.upsert_zpool(pool.clone(), None).await;
            }
        }
        let instances = InstanceManager::new(
            parent_log.clone(),
            lazy_nexus_client.clone(),
            etherstub.clone(),
            *sled_address.ip(),
            request.gateway.mac,
        )?;

        let svc_config = services::Config::new(
            config.sidecar_revision.clone(),
            request.gateway.address,
        );

        let hardware = HardwareManager::new(&parent_log, config.stub_scrimlet)
            .map_err(|e| Error::Hardware(e))?;

        let update_config =
            ConfigUpdates { zone_artifact_path: PathBuf::from("/opt/oxide") };
        let updates = UpdateManager::new(update_config);

        services
            .sled_agent_started(
                svc_config,
                config.get_link()?,
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
                services,
                lazy_nexus_client,

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

        Ok(sled_agent)
    }

    // Observe the current hardware state manually.
    //
    // We use this when we're monitoring hardware for the first
    // time, and if we miss notifications.
    async fn full_hardware_scan(&self, log: &Logger) {
        info!(log, "Performing full hardware scan");
        self.notify_nexus_about_self(log);

        if self.inner.hardware.is_scrimlet_driver_loaded() {
            let switch_zone_ip = Some(self.inner.switch_zone_ip());
            if let Err(e) =
                self.inner.services.activate_switch(switch_zone_ip).await
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
                        let switch_zone_ip = Some(self.inner.switch_zone_ip());
                        if let Err(e) = self
                            .inner
                            .services
                            .activate_switch(switch_zone_ip)
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
        let lazy_nexus_client = self.inner.lazy_nexus_client.clone();
        let sled_address = self.inner.sled_address();
        let is_scrimlet = self.inner.hardware.is_scrimlet();
        let baseboard = nexus_client::types::Baseboard::from(
            self.inner.hardware.baseboard(),
        );
        let log = log.clone();
        let fut = async move {
            // Notify the control plane that we're up, and continue trying this
            // until it succeeds. We retry with an randomized, capped exponential
            // backoff.
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

                let nexus_client = lazy_nexus_client
                    .get()
                    .await
                    .map_err(|err| BackoffError::transient(err.to_string()))?;
                nexus_client
                    .sled_agent_put(
                        &sled_id,
                        &nexus_client::types::SledAgentStartupInfo {
                            sa_address: sled_address.to_string(),
                            role,
                            baseboard: baseboard.clone(),
                        },
                    )
                    .await
                    .map_err(|err| BackoffError::transient(err.to_string()))
            };
            let log_notification_failure = |err, delay| {
                warn!(
                    log,
                    "failed to notify nexus about sled agent: {}, will retry in {:?}", err, delay;
                );
            };
            retry_notify(
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

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, will be recorded
    /// to a local file to ensure they start automatically on next boot.
    pub async fn services_ensure(
        &self,
        requested_services: ServiceEnsureBody,
    ) -> Result<(), Error> {
        self.inner.services.ensure_persistent(requested_services).await?;
        Ok(())
    }

    /// Gets the sled's current list of all zpools.
    pub async fn zpools_get(&self) -> Result<Vec<Zpool>, Error> {
        let zpools = self.inner.storage.get_zpools().await?;
        Ok(zpools)
    }

    /// Ensures that a filesystem type exists within the zpool.
    pub async fn filesystem_ensure(
        &self,
        zpool_uuid: Uuid,
        dataset_kind: DatasetKind,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        self.inner
            .storage
            .upsert_filesystem(zpool_uuid, dataset_kind, address)
            .await?;
        Ok(())
    }

    /// Idempotently ensures that a given Instance is running on the sled.
    pub async fn instance_ensure(
        &self,
        instance_id: Uuid,
        initial: InstanceHardware,
        target: InstanceRuntimeStateRequested,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        self.inner
            .instances
            .ensure(instance_id, initial, target, migrate)
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
        let nexus_client = self.inner.lazy_nexus_client.get().await?;
        self.inner.updates.download_artifact(artifact, &nexus_client).await?;
        Ok(())
    }

    pub async fn instance_serial_console_data(
        &self,
        instance_id: Uuid,
        byte_offset: ByteOffset,
        max_bytes: Option<usize>,
    ) -> Result<InstanceSerialConsoleData, Error> {
        self.inner
            .instances
            .instance_serial_console_buffer_data(
                instance_id,
                byte_offset,
                max_bytes,
            )
            .await
            .map_err(Error::from)
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
        _vpc_id: Uuid,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        self.inner
            .instances
            .firewall_rules_ensure(rules)
            .await
            .map_err(Error::from)
    }

    pub async fn set_virtual_nic_host(
        &self,
        interface_id: Uuid,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .set_virtual_nic_host(interface_id, mapping)
            .await
            .map_err(Error::from)
    }

    pub async fn unset_virtual_nic_host(
        &self,
        interface_id: Uuid,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        self.inner
            .instances
            .unset_virtual_nic_host(interface_id, mapping)
            .await
            .map_err(Error::from)
    }
}
