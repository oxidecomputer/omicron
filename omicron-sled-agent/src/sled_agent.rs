//! Sled agent implementation

use super::bootstrap;
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
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

use crate::common::vlan::VlanID;
use crate::instance_manager::InstanceManager;

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    pub bootstrap_agent: bootstrap::Agent,
    instances: InstanceManager,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub fn new(
        id: &Uuid,
        log: Logger,
        vlan: Option<VlanID>,
        nexus_client: Arc<NexusClient>,
    ) -> Result<SledAgent, Error> {
        info!(&log, "created sled agent"; "id" => ?id);

        let bootstrap_agent =
            bootstrap::Agent::new(log.new(o!("component" => "BootstrapAgent")));
        let instances = InstanceManager::new(log, vlan, nexus_client)?;

        Ok(SledAgent { bootstrap_agent, instances })
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

    /// Idempotently ensures that the given Disk is attached (or not) as
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
