//! Sled agent implementation

use crate::config::Config;
use crate::illumos::zfs::{
    Mountpoint, ZONE_ZFS_DATASET, ZONE_ZFS_DATASET_MOUNTPOINT,
};
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
    crate::illumos::{
        dladm::MockDladm as Dladm,
        zfs::MockZfs as Zfs,
        zone::MockZones as Zones,
    },
    crate::mocks::MockNexusClient as NexusClient,
};
#[cfg(not(test))]
use {
    crate::illumos::{
        dladm::Dladm,
        zfs::Zfs,
        zone::Zones,
    },
    omicron_common::NexusClient,
};

// TODO: I wanna make a task that continually reports the storage status
// upward to nexus.

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    #[allow(dead_code)]
    storage: StorageManager,
    instances: InstanceManager,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub async fn new(
        config: &Config,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> Result<SledAgent, Error> {
        let id = &config.id;
        let vlan = config.vlan;
        info!(&log, "created sled agent"; "id" => ?id);

        // Before we start creating zones, we need to ensure that the
        // necessary ZFS and Zone resources are ready.
        Zfs::ensure_filesystem(
            ZONE_ZFS_DATASET,
            Mountpoint::Path(std::path::PathBuf::from(
                ZONE_ZFS_DATASET_MOUNTPOINT,
            )),
        )?;

        // Identify all existing zones which should be managed by the Sled
        // Agent.
        //
        // NOTE: Currently, we're removing these zones. In the future, we should
        // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
        // to leave the running Zones intact).
        let zones = Zones::get()?;
        for z in zones {
            warn!(log, "Deleting zone: {}", z.name());
            Zones::halt_and_remove(&log, z.name())?;
        }

        // Identify all VNICs which should be managed by the Sled Agent.
        //
        // NOTE: Currently, we're removing these VNICs. In the future, we should
        // identify if they're being used by the aforementioned existing zones,
        // and track them once more.
        //
        // (dladm show-vnic -p -o ZONE,LINK) might help
        let vnics = Dladm::get_vnics()?;
        for vnic in vnics {
            warn!(log, "Deleting VNIC: {}", vnic);
            Dladm::delete_vnic(&vnic)?;
        }

        let storage =
            StorageManager::new(&log, *id, nexus_client.clone()).await?;
        if let Some(pools) = &config.zpools {
            for pool in pools {
                storage.upsert_zpool(pool).await?;
            }
        }
        // TODO-nit: Could remove nexus_client from IM?
        // basically just one less place to store it, could be passed in
        // 'ensure'. idk.
        let instances = InstanceManager::new(log, vlan, nexus_client.clone())?;

        Ok(SledAgent { storage, instances })
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
