//! Sled agent implementation

use crate::config::Config;
use crate::illumos::zfs::{Mountpoint, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT};
use crate::instance_manager::InstanceManager;
use crate::storage_manager::StorageManager;
use omicron_common::api::{
    external::Error, internal::nexus::DiskRuntimeState,
    internal::nexus::InstanceRuntimeState,
    internal::sled_agent::DiskStateRequested,
    internal::sled_agent::InstanceHardware,
    internal::sled_agent::InstanceRuntimeStateRequested,
};
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(test)]
use {
    crate::mocks::MockNexusClient as NexusClient,
    crate::illumos::zfs::MockZfs as Zfs,
};
#[cfg(not(test))]
use {
    omicron_common::NexusClient,
    crate::illumos::zfs::Zfs,
};

// TODO: I wanna make a task that continually reports the storage status
// upward to nexus.

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    storage: StorageManager,
    instances: InstanceManager,
    nexus_client: Arc<NexusClient>,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub async fn new(
        config: &Config,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> Result<SledAgent, Error> {
        let id = &config.id;
        let vlan = config.vlan.clone();
        info!(&log, "created sled agent"; "id" => ?id);

        // Before we start creating zones, we need to ensure that the
        // necessary ZFS and Zone resources are ready.
        Zfs::ensure_filesystem(
            ZONE_ZFS_DATASET,
            Mountpoint::Path(std::path::PathBuf::from(ZONE_ZFS_DATASET_MOUNTPOINT)),
        )?;

        let storage = StorageManager::new(&log, *id, nexus_client.clone()).await?;
        if let Some(pools) = &config.zpools {
            for pool in pools {
                storage.upsert_zpool(pool).await?;
            }
        }
        // TODO-nit: Could remove nexus_client from IM?
        // basically just one less place to store it, could be passed in
        // 'ensure'. idk.
        let instances = InstanceManager::new(log, vlan, nexus_client.clone())?;

        Ok(SledAgent { storage, instances, nexus_client })
    }

    /// Idempotently ensures that a given Instance is running on the sled.
    pub async fn instance_ensure(
        &self,
        instance_id: Uuid,
        initial: InstanceHardware,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
        self.instances.ensure(instance_id, initial, target).await
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
}
