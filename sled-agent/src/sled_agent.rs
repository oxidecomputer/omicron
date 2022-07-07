// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::config::Config;
use crate::illumos::vnic::VnicKind;
use crate::illumos::zfs::{
    Mountpoint, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
};
use crate::illumos::{execute, PFEXEC};
use crate::instance_manager::InstanceManager;
use crate::nexus::NexusClient;
use crate::params::{
    DatasetKind, DiskStateRequested, InstanceHardware, InstanceMigrateParams,
    InstanceRuntimeStateRequested, InstanceSerialConsoleData,
    ServiceEnsureBody,
};
use crate::services::{self, ServiceManager};
use crate::storage_manager::StorageManager;
use futures::stream::{self, StreamExt, TryStreamExt};
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifact,
};
use slog::Logger;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

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
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    // ID of the Sled
    id: Uuid,

    // Component of Sled Agent responsible for storage and dataset management.
    storage: StorageManager,

    // Component of Sled Agent responsible for managing Propolis instances.
    instances: InstanceManager,

    nexus_client: Arc<NexusClient>,

    // Other Oxide-controlled services running on this Sled.
    services: ServiceManager,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub async fn new(
        config: &Config,
        log: Logger,
        nexus_client: Arc<NexusClient>,
        sled_address: SocketAddrV6,
        rack_id: Uuid,
    ) -> Result<SledAgent, Error> {
        let id = &config.id;

        // Pass the "parent_log" to all subcomponents that want to set their own
        // "component" value.
        let parent_log = log.clone();

        // Use "log" for ourself.
        let log = log.new(o!(
            "component" => "SledAgent",
            "sled_id" => id.to_string(),
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
        let zones = Zones::get()?;
        stream::iter(zones)
            .zip(stream::iter(std::iter::repeat(log.clone())))
            .map(Ok::<_, crate::zone::AdmError>)
            .try_for_each_concurrent(
                None,
                |(zone, log)| async {
                    tokio::task::spawn_blocking(move || {
                        warn!(log, "Deleting existing zone"; "zone_name" => zone.name());
                        Zones::halt_and_remove_logged(&log, zone.name())
                    }).await.unwrap()
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
                        "vnic_kind" => ?VnicKind::from_name(&vnic).unwrap(),
                    );
                    Dladm::delete_vnic(&vnic)
                })
                .await
                .unwrap()
            })
            .await?;

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
            *id,
            nexus_client.clone(),
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
            nexus_client.clone(),
            etherstub.clone(),
            *sled_address.ip(),
        );

        let svc_config = services::Config {
            gateway_address: config.gateway_address,
            ..Default::default()
        };
        let services = ServiceManager::new(
            parent_log.clone(),
            etherstub.clone(),
            etherstub_vnic.clone(),
            *sled_address.ip(),
            svc_config,
            config.get_link()?,
            rack_id,
        )
        .await?;

        Ok(SledAgent {
            id: config.id,
            storage,
            instances,
            nexus_client,
            services,
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Ensures that particular services should be initialized.
    ///
    /// These services will be instantiated by this function, will be recorded
    /// to a local file to ensure they start automatically on next boot.
    pub async fn services_ensure(
        &self,
        requested_services: ServiceEnsureBody,
    ) -> Result<(), Error> {
        self.services.ensure(requested_services).await?;
        Ok(())
    }

    /// Ensures that a filesystem type exists within the zpool.
    pub async fn filesystem_ensure(
        &self,
        zpool_uuid: Uuid,
        dataset_kind: DatasetKind,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        self.storage
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
        self.instances
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
        crate::updates::download_artifact(artifact, self.nexus_client.as_ref())
            .await?;
        Ok(())
    }

    pub async fn instance_serial_console_data(
        &self,
        instance_id: Uuid,
        byte_offset: ByteOffset,
        max_bytes: Option<usize>,
    ) -> Result<InstanceSerialConsoleData, Error> {
        self.instances
            .instance_serial_console_buffer_data(
                instance_id,
                byte_offset,
                max_bytes,
            )
            .await
            .map_err(Error::from)
    }
}
