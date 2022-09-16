// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::bootstrap::params::SledAgentRequest;
use crate::config::Config;
use crate::hardware::HardwareManager;
use crate::illumos::link::LinkKind;
use crate::illumos::zfs::{
    Mountpoint, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
};
use crate::illumos::zone::IPADM;
use crate::illumos::{execute, PFEXEC};
use crate::instance_manager::InstanceManager;
use crate::nexus::{LazyNexusClient, NexusRequestQueue};
use crate::params::{
    DatasetKind, DiskStateRequested, InstanceHardware, InstanceMigrateParams,
    InstanceRuntimeStateRequested, InstanceSerialConsoleData,
    ServiceEnsureBody, VpcFirewallRule, Zpool,
};
use crate::services::{self, ServiceManager};
use crate::storage_manager::StorageManager;
use dropshot::HttpError;
use futures::stream::{self, StreamExt, TryStreamExt};
use omicron_common::address::{
    get_sled_address, get_switch_zone_address, Ipv6Subnet, SLED_PREFIX,
};
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifact,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use slog::Logger;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::process::Command;
use std::sync::Arc;
use uuid::Uuid;

use crucible_client_types::VolumeConstructionRequest;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zfs::Zfs, zone::Zones};
#[cfg(test)]
use crate::illumos::{
    dladm::MockDladm as Dladm, zfs::MockZfs as Zfs, zone::MockZones as Zones,
};
use crate::serial::ByteOffset;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Physical link not in config, nor found automatically: {0}")]
    FindPhysicalLink(#[from] crate::illumos::dladm::FindPhysicalLinkError),

    #[error("Failed to enable routing: {0}")]
    EnablingRouting(crate::illumos::ExecutionError),

    #[error("Failed to acquire etherstub: {0}")]
    Etherstub(crate::illumos::ExecutionError),

    #[error("Failed to acquire etherstub VNIC: {0}")]
    EtherstubVnic(crate::illumos::dladm::CreateVnicError),

    #[error("Failed to lookup VNICs on boot: {0}")]
    GetVnics(#[from] crate::illumos::dladm::GetVnicError),

    #[error("Failed to delete VNIC on boot: {0}")]
    DeleteVnic(#[from] crate::illumos::dladm::DeleteVnicError),

    #[error("Failed to remove Omicron address: {0}")]
    DeleteAddress(#[from] crate::illumos::ExecutionError),

    #[error("Failed to operate on underlay device: {0}")]
    Underlay(#[from] crate::common::underlay::Error),

    #[error(transparent)]
    Services(#[from] crate::services::Error),

    #[error(transparent)]
    ZoneOperation(#[from] crate::illumos::zone::AdmError),

    #[error("Failed to create Sled Subnet: {err}")]
    SledSubnet { err: crate::illumos::zone::EnsureGzAddressError },

    #[error(transparent)]
    ZfsEnsureFilesystem(#[from] crate::illumos::zfs::EnsureFilesystemError),

    #[error("Error managing instances: {0}")]
    Instance(#[from] crate::instance_manager::Error),

    #[error("Error managing storage: {0}")]
    Storage(#[from] crate::storage_manager::Error),

    #[error("Error updating: {0}")]
    Download(#[from] crate::updates::Error),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] crate::opte::Error),

    #[error("Error monitoring hardware: {0}")]
    Hardware(String),

    #[error("Error resolving DNS name: {0}")]
    ResolveError(#[from] internal_dns_client::multiclient::ResolveError),
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

        let etherstub =
            Dladm::ensure_etherstub().map_err(|e| Error::Etherstub(e))?;
        let etherstub_vnic = Dladm::ensure_etherstub_vnic(&etherstub)
            .map_err(|e| Error::EtherstubVnic(e))?;

        // Before we start creating zones, we need to ensure that the
        // necessary ZFS and Zone resources are ready.
        Zfs::ensure_zoned_filesystem(
            ZONE_ZFS_DATASET,
            Mountpoint::Path(std::path::PathBuf::from(
                ZONE_ZFS_DATASET_MOUNTPOINT,
            )),
            // do_format=
            true,
        )?;

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
        crate::opte::initialize_xde_driver(&log)?;

        // Identify all existing zones which should be managed by the Sled
        // Agent.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/725):
        // Currently, we're removing these zones. In the future, we should
        // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
        // to leave the running Zones intact).
        let zones = Zones::get().await?;
        stream::iter(zones)
            .zip(stream::iter(std::iter::repeat(log.clone())))
            .map(Ok::<_, crate::zone::AdmError>)
            .try_for_each_concurrent(
                None,
                |(zone, log)| async move {
                    warn!(log, "Deleting existing zone"; "zone_name" => zone.name());
                    Zones::halt_and_remove_logged(&log, zone.name(), false).await
                }
            ).await?;

        // Identify all VNICs which should be managed by the Sled Agent.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/725)
        // Currently, we're removing these VNICs. In the future, we should
        // identify if they're being used by the aforementioned existing zones,
        // and track them once more.
        //
        // This should be accessible via:
        // $ dladm show-linkprop -c -p zone -o LINK,VALUE
        //
        // Note that we don't currently delete the VNICs in any particular
        // order. That should be OK, since we're definitely deleting the guest
        // VNICs before the xde devices, which is the main constraint.
        delete_omicron_vnics(&log).await?;

        // Also delete any extant xde devices. These should also eventually be
        // recovered / tracked, to avoid interruption of any guests that are
        // still running. That's currently irrelevant, since we're deleting the
        // zones anyway.
        //
        // This is also tracked by
        // https://github.com/oxidecomputer/omicron/issues/725.
        crate::opte::delete_all_xde_devices(&log)?;

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
                storage.upsert_zpool(pool).await?;
            }
        }
        let instances = InstanceManager::new(
            parent_log.clone(),
            lazy_nexus_client.clone(),
            etherstub.clone(),
            *sled_address.ip(),
            request.gateway.mac,
        );

        let svc_config = services::Config::new(
            config.sidecar_revision.clone(),
            request.gateway.address,
        );

        let hardware =
            HardwareManager::new(parent_log.clone(), config.stub_scrimlet)
                .map_err(|e| Error::Hardware(e))?;

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
            sa.monitor_task(log).await;
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
    }

    async fn monitor_task(&self, log: Logger) {
        // Start monitoring the hardware for changes
        let mut hardware_updates = self.inner.hardware.monitor();

        // Scan the system manually for events we have have missed
        // before we started monitoring.
        self.full_hardware_scan(&log).await;

        // Rely on monitoring for tracking all future updates.
        loop {
            use tokio::sync::broadcast::error::RecvError;
            match hardware_updates.recv().await {
                Ok(update) => match update {
                    crate::hardware::HardwareUpdate::TofinoDeviceChange => {
                        // Inform Nexus that we're now a scrimlet, instead of a Gimlet.
                        //
                        // This won't block on Nexus responding; it may take while before
                        // Nexus actually comes online.
                        self.notify_nexus_about_self(&log);
                    }
                    crate::hardware::HardwareUpdate::TofinoLoaded => {
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
                    crate::hardware::HardwareUpdate::TofinoUnloaded => {
                        if let Err(e) =
                            self.inner.services.deactivate_switch().await
                        {
                            warn!(log, "Failed to deactivate switch: {e}");
                        }
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
                    "contacting server nexus, registering sled: {}", sled_id
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
        artifact: UpdateArtifact,
    ) -> Result<(), Error> {
        let nexus_client = self.inner.lazy_nexus_client.get().await?;
        crate::updates::download_artifact(artifact, &nexus_client).await?;
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

    /// Issue a snapshot request for a Crucible disk not attached to an
    /// instance.
    pub async fn issue_disk_snapshot_request(
        &self,
        _disk_id: Uuid,
        _volume_construction_request: VolumeConstructionRequest,
        _snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // For a disk not attached to an instance, implementation requires
        // constructing a volume and performing a snapshot through some other
        // means. Currently unimplemented.
        todo!();
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
}

// Delete all underlay addresses created directly over the etherstub VNIC used
// for inter-zone communications.
fn delete_etherstub_addresses(log: &Logger) -> Result<(), Error> {
    let prefix = format!("{}/", crate::illumos::dladm::ETHERSTUB_VNIC_NAME);
    delete_addresses_matching_prefixes(log, &[prefix])
}

fn delete_underlay_addresses(log: &Logger) -> Result<(), Error> {
    use crate::illumos::dladm::VnicSource;
    let prefixes = crate::common::underlay::find_chelsio_links()?
        .into_iter()
        .map(|link| format!("{}/", link.name()))
        .collect::<Vec<_>>();
    delete_addresses_matching_prefixes(log, &prefixes)
}

fn delete_addresses_matching_prefixes(
    log: &Logger,
    prefixes: &[String],
) -> Result<(), Error> {
    use std::io::BufRead;
    let mut cmd = Command::new(PFEXEC);
    let cmd = cmd.args(&[IPADM, "show-addr", "-p", "-o", "ADDROBJ"]);
    let output = execute(cmd)?;

    // `ipadm show-addr` can return multiple addresses with the same name, but
    // multiple values. Collecting to a set ensures that only a single name is
    // used.
    let addrobjs = output
        .stdout
        .lines()
        .flatten()
        .collect::<std::collections::HashSet<_>>();

    for addrobj in addrobjs {
        if prefixes.iter().any(|prefix| addrobj.starts_with(prefix)) {
            warn!(
                log,
                "Deleting existing Omicron IP address";
                "addrobj" => addrobj.as_str(),
            );
            let mut cmd = Command::new(PFEXEC);
            let cmd = cmd.args(&[IPADM, "delete-addr", addrobj.as_str()]);
            execute(cmd)?;
        }
    }
    Ok(())
}

// Delete all VNICs that can be managed by the control plane.
//
// These are currently those that match the prefix `ox` or `vopte`.
async fn delete_omicron_vnics(log: &Logger) -> Result<(), Error> {
    let vnics = Dladm::get_vnics()?;
    stream::iter(vnics)
        .zip(stream::iter(std::iter::repeat(log.clone())))
        .map(Ok::<_, crate::illumos::dladm::DeleteVnicError>)
        .try_for_each_concurrent(None, |(vnic, log)| async {
            tokio::task::spawn_blocking(move || {
                warn!(
                  log,
                  "Deleting existing VNIC";
                    "vnic_name" => &vnic,
                    "vnic_kind" => ?LinkKind::from_name(&vnic).unwrap(),
                );
                Dladm::delete_vnic(&vnic)
            })
            .await
            .unwrap()
        })
        .await?;
    Ok(())
}

// Delete the etherstub and underlay VNIC used for interzone communication
fn delete_etherstub(log: &Logger) -> Result<(), Error> {
    use crate::illumos::dladm::ETHERSTUB_NAME;
    use crate::illumos::dladm::ETHERSTUB_VNIC_NAME;
    warn!(log, "Deleting Omicron underlay VNIC"; "vnic_name" => ETHERSTUB_VNIC_NAME);
    Dladm::delete_etherstub_vnic()?;
    warn!(log, "Deleting Omicron etherstub"; "stub_name" => ETHERSTUB_NAME);
    Dladm::delete_etherstub()?;
    Ok(())
}

/// Delete all networking resources installed by the sled agent, in the global
/// zone.
pub async fn cleanup_networking_resources(log: &Logger) -> Result<(), Error> {
    delete_etherstub_addresses(log)?;
    delete_underlay_addresses(log)?;
    delete_omicron_vnics(log).await?;
    delete_etherstub(log)?;
    crate::opte::delete_all_xde_devices(log)?;
    Ok(())
}
