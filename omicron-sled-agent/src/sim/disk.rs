/*!
 * Simulated sled agent implementation
 */

use crate::sim::simulatable::Simulatable;
use async_trait::async_trait;
use omicron_common::api::ApiDiskRuntimeState;
use omicron_common::api::ApiDiskState;
use omicron_common::api::ApiDiskStateRequested;
use omicron_common::api::ApiError;
use omicron_common::api::ApiGeneration;
use omicron_common::NexusClient;
use propolis_client::api::DiskAttachmentState as PropolisDiskState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::disk::{Action as DiskAction, DiskState};

/**
 * Simulated Disk (network block device), as created by the external Oxide API
 *
 * See `Simulatable` for how this works.
 */
#[derive(Debug)]
pub struct SimDisk {
    state: DiskState,
}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = ApiDiskRuntimeState;
    type RequestedState = ApiDiskStateRequested;
    type Action = DiskAction;

    fn new(current: ApiDiskRuntimeState) -> Self {
        SimDisk { state: DiskState::new(current) }
    }

    fn request_transition(
        &mut self,
        target: &ApiDiskStateRequested,
    ) -> Result<Option<DiskAction>, ApiError> {
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
                ApiDiskStateRequested::Attached(uuid) => {
                    PropolisDiskState::Attached(*uuid)
                }
                ApiDiskStateRequested::Detached => PropolisDiskState::Detached,
                ApiDiskStateRequested::Destroyed => {
                    PropolisDiskState::Destroyed
                }
                ApiDiskStateRequested::Faulted => PropolisDiskState::Faulted,
            };
            self.state.observe_transition(&observed)
        } else {
            None
        }
    }

    fn generation(&self) -> ApiGeneration {
        self.state.current().gen
    }

    fn current(&self) -> &Self::CurrentState {
        self.state.current()
    }

    fn desired(&self) -> &Option<Self::RequestedState> {
        self.state.desired()
    }

    fn ready_to_destroy(&self) -> bool {
        ApiDiskState::Destroyed == self.current().disk_state
    }

    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        nexus_client.notify_disk_updated(id, &current).await
    }
}
