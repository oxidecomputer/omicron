// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use super::simulatable::Simulatable;

use crate::nexus::NexusClient;
use crate::params::{InstanceRuntimeStateRequested, InstanceStateRequested};
use async_trait::async_trait;
use nexus_client;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

/// Simulated Instance (virtual machine), as created by the external Oxide API
#[derive(Debug)]
pub struct SimInstance {
    state: InstanceStates,
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = InstanceRuntimeState;
    type RequestedState = InstanceRuntimeStateRequested;
    type ProducerArgs = ();
    type Action = InstanceAction;

    fn new(current: InstanceRuntimeState) -> Self {
        SimInstance { state: InstanceStates::new(current) }
    }

    async fn set_producer(
        &mut self,
        _args: Self::ProducerArgs,
    ) -> Result<(), Error> {
        // NOTE: Not implemented, yet.
        Ok(())
    }

    fn request_transition(
        &mut self,
        target: &InstanceRuntimeStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        self.state.request_transition(target)
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
                InstanceStateRequested::Destroyed(_) => {
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
        nexus_client
            .cpapi_instances_put(
                id,
                &nexus_client::types::InstanceRuntimeState::from(current),
            )
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
}
