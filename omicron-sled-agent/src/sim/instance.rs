/*!
 * Simulated sled agent implementation
 */

use super::simulatable::Simulatable;

use async_trait::async_trait;
use omicron_common::error::ApiError;
use omicron_common::model::ApiGeneration;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;
use omicron_common::NexusClient;
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceState};

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

    fn new(current: ApiInstanceRuntimeState) -> Self {
        SimInstance { state: InstanceState::new(current) }
    }

    fn request_transition(
        &mut self,
        target: &ApiInstanceRuntimeStateRequested,
    ) -> Result<Option<InstanceAction>, ApiError> {
        self.state.request_transition(target)
    }

    fn observe_transition(&mut self) -> Option<InstanceAction> {
        if let Some(pending) = self.state.pending.as_ref() {
            // These operations would typically be triggered via responses from
            // Propolis, but for a simulated sled agent, this does not exist.
            //
            // Instead, we make transitions to new states based entirely on the
            // value of "pending".
            let observed = match pending.run_state {
                ApiInstanceStateRequested::Running => {
                    PropolisInstanceState::Running
                }
                ApiInstanceStateRequested::Stopped => {
                    PropolisInstanceState::Stopped
                }
                ApiInstanceStateRequested::Destroyed => {
                    PropolisInstanceState::Destroyed
                }
                _ => panic!("Unexpected pending state: {}", pending.run_state),
            };
            self.state.observe_transition(&observed)
        } else {
            None
        }
    }

    fn generation(&self) -> ApiGeneration {
        self.state.current.gen
    }

    fn current(&self) -> &Self::CurrentState {
        &self.state.current
    }

    fn pending(&self) -> Option<Self::RequestedState> {
        self.state.pending.clone()
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
