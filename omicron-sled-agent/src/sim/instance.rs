/*!
 * Simulated sled agent implementation
 */

use super::simulatable::Simulatable;

use async_trait::async_trait;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::sled_agent::InstanceStateRequested;
use omicron_common::NexusClient;
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

/**
 * Simulated Instance (virtual machine), as created by the external Oxide API
 */
#[derive(Debug)]
pub struct SimInstance {
    state: InstanceStates,
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = InstanceRuntimeState;
    type RequestedState = InstanceRuntimeStateRequested;
    type Action = InstanceAction;

    fn new(current: InstanceRuntimeState) -> Self {
        SimInstance { state: InstanceStates::new(current) }
    }

    fn request_transition(
        &mut self,
        target: &InstanceRuntimeStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        self.state.request_transition(target.run_state)
    }

    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        if matches!(self.state.current().run_state, InstanceState::Rebooting) {
            self.state.observe_transition(&PropolisInstanceState::Starting)
        } else if let Some(desired) = self.state.desired() {
            // These operations would typically be triggered via responses from
            // Propolis, but for a simulated sled agent, this does not exist.
            //
            // Instead, we make transitions to new states based entirely on the
            // value of "desired".
            let observed = match desired.run_state {
                InstanceStateRequested::Running => {
                    PropolisInstanceState::Running
                }
                InstanceStateRequested::Stopped => {
                    PropolisInstanceState::Stopped
                }
                InstanceStateRequested::Destroyed => {
                    PropolisInstanceState::Destroyed
                }
                _ => panic!("Unexpected desired state: {}", desired.run_state),
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
        self.current().run_state == InstanceState::Destroyed
    }

    async fn notify(
        nexus_client: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), Error> {
        nexus_client.notify_instance_updated(id, &current).await
    }
}
