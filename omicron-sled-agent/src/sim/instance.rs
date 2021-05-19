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
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{InstanceState, Action as InstanceAction};

/**
 * Simulated Instance (virtual machine), as created by the external Oxide API
 */
#[derive(Debug)]
pub struct SimInstance {
    state: InstanceState,
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = ApiInstanceRuntimeState;
    type RequestedState = ApiInstanceRuntimeStateRequested;
    type Action = InstanceAction;
    type ObservedState = PropolisInstanceState;

    fn new(
        current: ApiInstanceRuntimeState
    ) -> Self {
        SimInstance {
            state: InstanceState::new(current),
        }
    }

    fn request_transition(
        &mut self,
        target: ApiInstanceRuntimeStateRequested
    ) -> Result<Option<InstanceAction>, ApiError> {
        self.state.request_transition(&target)
    }

    fn observe_transition(
        &mut self,
        observed: PropolisInstanceState,
    ) -> Option<InstanceAction> {
        self.state.observe_transition(observed)
    }

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

        let _action = state.request_transition(target)?;
        Ok((state.current, state.pending))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let mut current = InstanceState {
            current: current.clone(),
            pending: Some(pending.clone()),
        };

        // These operations would typically be triggered via responses from
        // Propolis, but for a simulated sled agent, this does not exist.
        //
        // Instead, we make transitions to new states based entirely on the
        // value of "pending".
        let observed = match pending.run_state {
            ApiInstanceStateRequested::Running => PropolisInstanceState::Running,
            ApiInstanceStateRequested::Stopped => PropolisInstanceState::Stopped,
            ApiInstanceStateRequested::Destroyed => PropolisInstanceState::Destroyed,
            _ => panic!("Unexpected pending state: {}", pending.run_state),
        };

        let _action = current.observe_transition(observed);
        (current.current, current.pending)
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
