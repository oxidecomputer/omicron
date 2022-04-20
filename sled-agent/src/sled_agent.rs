// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::config::Config;
use crate::illumos::zfs::{
    Mountpoint, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
};
use crate::instance_manager::InstanceManager;
use crate::nexus::NexusClient;
use crate::params::{
    DatasetKind, DiskStateRequested, InstanceHardware, InstanceMigrateParams,
    InstanceRuntimeStateRequested, ServiceEnsureBody,
};
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use omicron_common::api::{
    internal::nexus::DiskRuntimeState, internal::nexus::InstanceRuntimeState,
    internal::nexus::UpdateArtifact,
};
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zfs::Zfs, zone::Zones};
#[cfg(test)]
use crate::illumos::{
    dladm::MockDladm as Dladm, zfs::MockZfs as Zfs, zone::MockZones as Zones,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Datalink(#[from] crate::illumos::dladm::Error),

    #[error(transparent)]
    Services(#[from] crate::services::Error),

    #[error(transparent)]
    Zone(#[from] crate::illumos::zone::Error),

    #[error(transparent)]
    Zfs(#[from] crate::illumos::zfs::Error),

    #[error("Error managing instances: {0}")]
    Instance(#[from] crate::instance_manager::Error),

    #[error("Error managing storage: {0}")]
    Storage(#[from] crate::storage_manager::Error),

    #[error("Error updating: {0}")]
    Download(#[from] crate::updates::Error),
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
    ) -> Result<SledAgent, Error> {
        let id = &config.id;
        let vlan = config.vlan;
        info!(&log, "created sled agent"; "id" => ?id);

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
            config.data_link.clone(),
            *sled_address.ip(),
            "sled6",
        )?;

        // Identify all existing zones which should be managed by the Sled
        // Agent.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/725):
        // Currently, we're removing these zones. In the future, we should
        // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
        // to leave the running Zones intact).
        let zones = Zones::get()?;
        for z in zones {
            warn!(log, "Deleting zone: {}", z.name());
            Zones::halt_and_remove_logged(&log, z.name())?;
        }

        // Identify all VNICs which should be managed by the Sled Agent.
        //
        // TODO(https://github.com/oxidecomputer/omicron/issues/725)
        // Currently, we're removing these VNICs. In the future, we should
        // identify if they're being used by the aforementioned existing zones,
        // and track them once more.
        //
        // This should be accessible via:
        // $ dladm show-linkprop -c -p zone -o LINK,VALUE
        let vnics = Dladm::get_vnics()?;
        for vnic in vnics {
            warn!(log, "Deleting VNIC: {}", vnic);
            Dladm::delete_vnic(&vnic)?;
        }

        let storage = StorageManager::new(
            &log,
            *id,
            nexus_client.clone(),
            config.data_link.clone(),
        )
        .await?;
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
            log.clone(),
            vlan,
            nexus_client.clone(),
            config.data_link.clone(),
        )?;
        let services =
            ServiceManager::new(log.clone(), config.data_link.clone(), None)
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
        address: SocketAddr,
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
}
