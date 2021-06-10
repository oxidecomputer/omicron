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

    fn new(current: ApiInstanceRuntimeState) -> Self {
        SimInstance { state: InstanceState::new(current) }
    }

    fn request_transition(
        &mut self,
        target: &ApiInstanceRuntimeStateRequested,
    ) -> Result<Option<InstanceAction>, ApiError> {
        self.state.request_transition(target.run_state)
    }

    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        if matches!(self.state.current().run_state, ApiInstanceState::Rebooting)
        {
            self.state.observe_transition(&PropolisInstanceState::Starting)
        } else if let Some(desired) = self.state.desired() {
            // These operations would typically be triggered via responses from
            // Propolis, but for a simulated sled agent, this does not exist.
            //
            // Instead, we make transitions to new states based entirely on the
            // value of "desired".
            let observed = match desired.run_state {
                ApiInstanceStateRequested::Running => {
                    PropolisInstanceState::Running
                }
                ApiInstanceStateRequested::Stopped => {
                    PropolisInstanceState::Stopped
                }
                ApiInstanceStateRequested::Destroyed => {
                    PropolisInstanceState::Destroyed
                }
                _ => panic!("Unexpected desired state: {}", desired.run_state),
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
        self.current().run_state == ApiInstanceState::Destroyed
    }

    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        nexus_client.notify_instance_updated(id, &current).await
    }
}
