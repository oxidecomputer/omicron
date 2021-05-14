/*!
 * Simulated sled agent implementation
 */

use super::simulatable::Simulatable;

use async_trait::async_trait;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;
use omicron_common::NexusClient;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::InstanceState;

/**
 * Simulated Instance (virtual machine), as created by the external Oxide API
 */
#[derive(Debug)]
pub struct SimInstance {}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = ApiInstanceRuntimeState;
    type RequestedState = ApiInstanceRuntimeStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        target: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        let mut state = InstanceState {
            current: current.clone(),
            pending: pending.clone(),
        };

        let _action = state.next(target)?;
        Ok((state.current, state.pending))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let current = InstanceState {
            current: current.clone(),
            pending: Some(pending.clone()),
        };

        let next = current.advance();
        (next.current, next.pending)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        state1.gen == state2.gen
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        current.run_state == ApiInstanceState::Destroyed
    }

    async fn notify(
        csc: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        /*
         * Notify Nexus that the instance state has changed.  The sled agent is
         * authoritative for the runtime state, and we use a generation number
         * here so that calls processed out of order do not settle on the wrong
         * value.
         */
        csc.notify_instance_updated(id, &current).await
    }
}
