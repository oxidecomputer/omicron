/*!
 * Simulated sled agent implementation
 */

use crate::sim::simulatable::Simulatable;
use async_trait::async_trait;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::NexusClient;
use propolis_client::api::DiskAttachmentState as PropolisDiskState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::disk::DiskState;

/**
 * Simulated Disk (network block device), as created by the external Oxide API
 *
 * See `Simulatable` for how this works.
 */
#[derive(Debug)]
pub struct SimDisk {}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = ApiDiskRuntimeState;
    type RequestedState = ApiDiskStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        let mut state = DiskState {
            current: current.clone(),
            pending: pending.clone(),
        };
        let _action = state.request_transition(next)?;
        Ok((state.current, state.pending))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let mut current = DiskState {
            current: current.clone(),
            pending: Some(pending.clone()),
        };

        let observed = match pending {
            ApiDiskStateRequested::Attached(uuid) => PropolisDiskState::Attached(*uuid),
            ApiDiskStateRequested::Detached => PropolisDiskState::Detached,
            ApiDiskStateRequested::Destroyed => PropolisDiskState::Destroyed,
            ApiDiskStateRequested::Faulted => PropolisDiskState::Faulted,
        };

        let _action = current.observe_transition(observed);
        (current.current.clone(), None)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        state1.gen == state2.gen
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        ApiDiskState::Destroyed == current.disk_state
    }

    async fn notify(
        csc: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        csc.notify_disk_updated(id, &current).await
    }
}
