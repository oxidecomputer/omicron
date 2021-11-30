// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent implementation

use crate::updates;
use crate::params::DiskStateRequested;
use omicron_common::api::{
    external::Error, internal::nexus::DiskRuntimeState,
    internal::nexus::InstanceRuntimeState,
    internal::sled_agent::InstanceHardware,
    internal::sled_agent::InstanceRuntimeStateRequested,
};

use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use nexus_client::Client as NexusClient;

use crate::common::vlan::VlanID;
use crate::instance_manager::InstanceManager;

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    nexus_client: Arc<NexusClient>,
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

        let instances = InstanceManager::new(log, vlan, nexus_client.clone())?;

        Ok(SledAgent { nexus_client, instances })
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

    /// Downloads and applies an artifact.
    pub async fn update_artifact(
        &self,
        artifact: &updates::UpdateArtifact,
    ) -> Result<(), Error> {
        artifact.download(self.nexus_client.as_ref())
            .await
            .map_err(|e| Error::internal_error(&format!("Failed download: {}", e)))?;
        Ok(())
    }
}
