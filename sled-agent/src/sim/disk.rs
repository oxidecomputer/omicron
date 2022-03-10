// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Simulated sled agent implementation
 */

use crate::nexus::NexusClient;
use crate::params::DiskStateRequested;
use crate::sim::simulatable::Simulatable;
use async_trait::async_trait;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use propolis_client::api::DiskAttachmentState as PropolisDiskState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::disk::{Action as DiskAction, DiskStates};

/**
 * Simulated Disk (network block device), as created by the external Oxide API
 *
 * See `Simulatable` for how this works.
 */
#[derive(Debug)]
pub struct SimDisk {
    state: DiskStates,
}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = DiskRuntimeState;
    type RequestedState = DiskStateRequested;
    type Action = DiskAction;

    fn new(current: DiskRuntimeState) -> Self {
        SimDisk { state: DiskStates::new(current) }
    }

    fn request_transition(
        &mut self,
        target: &DiskStateRequested,
    ) -> Result<Option<DiskAction>, Error> {
        self.state.request_transition(target)
    }

    fn execute_desired_transition(&mut self) -> Option<DiskAction> {
        if let Some(desired) = self.state.desired() {
            // These operations would typically be triggered via responses from
            // Propolis, but for a simulated sled agent, this does not exist.
            //
            // Instead, we make transitions to new states based entirely on the
            // value of "desired".
            let observed = match desired {
                DiskStateRequested::Attached(uuid) => {
                    PropolisDiskState::Attached(*uuid)
                }
                DiskStateRequested::Detached => PropolisDiskState::Detached,
                DiskStateRequested::Destroyed => PropolisDiskState::Destroyed,
                DiskStateRequested::Faulted => PropolisDiskState::Faulted,
            };
            self.state.observe_transition(&observed)
        } else {
            None
        }
    }

    fn generation(&self) -> Generation {
        self.state.current().gen
    }

    fn current(&self) -> &Self::CurrentState {
        self.state.current()
    }

    fn desired(&self) -> &Option<Self::RequestedState> {
        self.state.desired()
    }

    fn ready_to_destroy(&self) -> bool {
        DiskState::Destroyed == self.current().disk_state
    }

    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), Error> {
        nexus_client
            .cpapi_disks_put(
                id,
                &nexus_client::types::DiskRuntimeState::from(current),
            )
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
}
