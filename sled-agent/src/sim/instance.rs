// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use super::simulatable::Simulatable;

use crate::common::instance::InstanceState;
use crate::nexus::NexusClient;
use crate::params::InstanceStateRequested;
use async_trait::async_trait;
use nexus_client;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState as ApiInstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::collections::VecDeque;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

/// A simulation of an Instance created by the external Oxide API.
///
/// This simulation tries to emulate Propolis's state machine as faithfully as
/// possible within reason so that it can be used as a test double in Nexus
/// integration tests.
#[derive(Debug)]
pub struct SimInstance {
    /// The current simulated instance state.
    state: InstanceStates,

    /// A queue of Propolis instance states for the simulated state driver to
    /// observe in response to calls to `execute_desired_transition`.
    propolis_queue: VecDeque<PropolisInstanceState>,

    /// Indicates whether the instance has been logically destroyed.
    destroyed: bool,
}

impl SimInstance {
    /// Returns the state the simulated instance will reach if its queue is
    /// drained.
    fn terminal_state(&self) -> ApiInstanceState {
        match self.propolis_queue.back() {
            None => self.state.current().run_state,
            Some(p) => InstanceState::from(*p).0,
        }
    }
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = InstanceRuntimeState;
    type RequestedState = InstanceStateRequested;
    type ProducerArgs = ();
    type Action = InstanceAction;

    fn new(current: InstanceRuntimeState) -> Self {
        SimInstance {
            state: InstanceStates::new(current),
            propolis_queue: VecDeque::new(),
            destroyed: false,
        }
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
        target: &InstanceStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        match target {
            InstanceStateRequested::Running => match self.terminal_state() {
                ApiInstanceState::Creating => {
                    // The non-simulated sled agent explicitly and synchronously
                    // publishes the "Starting" state when cold-booting a new VM
                    // (so that the VM appears to be starting while its Propolis
                    // process is being launched).
                    self.state.transition(ApiInstanceState::Starting);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Running);
                }
                ApiInstanceState::Starting
                | ApiInstanceState::Running
                | ApiInstanceState::Rebooting
                | ApiInstanceState::Migrating => {}
                ApiInstanceState::Stopping
                | ApiInstanceState::Stopped
                | ApiInstanceState::Failed
                | ApiInstanceState::Destroyed => {
                    // TODO: Normally, Propolis forbids direct transitions from
                    // a stopped state back to a running state. Instead, Nexus
                    // creates a new Propolis and sends state change requests to
                    // that. This arm abstracts this behavior away and just
                    // allows a fake instance to transition right back to a
                    // running state after being stopped.
                    //
                    // This will change in the future when the sled agents (both
                    // real and simulated) split "registering" an instance with
                    // the agent and actually starting it into separate actions.
                    self.state.transition(ApiInstanceState::Starting);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Starting);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Running);
                }
                ApiInstanceState::Repairing => {
                    return Err(Error::invalid_request(&format!(
                        "can't request state Running with terminal state {}",
                        self.terminal_state()
                    )))
                }
            },
            InstanceStateRequested::Stopped => match self.terminal_state() {
                ApiInstanceState::Creating => {
                    self.state.transition(ApiInstanceState::Destroyed);
                }
                ApiInstanceState::Running => {
                    self.state.transition(ApiInstanceState::Stopping);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Stopping);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Stopped);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Destroyed);
                }
                // Idempotently allow requests to stop an instance that is
                // already stopping.
                ApiInstanceState::Stopping
                | ApiInstanceState::Stopped
                | ApiInstanceState::Destroyed => {}
                _ => {
                    return Err(Error::invalid_request(&format!(
                        "can't request state Stopped with terminal state {}",
                        self.terminal_state()
                    )))
                }
            },
            InstanceStateRequested::Reboot => match self.terminal_state() {
                ApiInstanceState::Running => {
                    self.state.transition(ApiInstanceState::Rebooting);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Rebooting);
                    self.propolis_queue
                        .push_back(PropolisInstanceState::Running);
                }
                _ => {
                    return Err(Error::invalid_request(&format!(
                        "can't request Reboot with terminal state {}",
                        self.terminal_state()
                    )))
                }
            },
            InstanceStateRequested::Migrating => {
                unimplemented!("Migration not implemented yet");
            }
            InstanceStateRequested::Destroyed => {
                self.state
                    .observe_transition(&PropolisInstanceState::Destroyed);
                self.propolis_queue.clear();
            }
        }

        Ok(None)
    }

    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        if let Some(propolis_state) = self.propolis_queue.pop_front() {
            if matches!(propolis_state, PropolisInstanceState::Destroyed) {
                self.destroyed = true;
            }
            self.state.observe_transition(&propolis_state)
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

    fn desired(&self) -> Option<Self::RequestedState> {
        self.propolis_queue.back().map(|terminal| match terminal {
            PropolisInstanceState::Creating
            | PropolisInstanceState::Starting
            | PropolisInstanceState::Migrating
            | PropolisInstanceState::Repairing
            | PropolisInstanceState::Failed => panic!(
                "terminal state {:?} doesn't map to a requested state",
                terminal
            ),
            PropolisInstanceState::Running => InstanceStateRequested::Running,
            PropolisInstanceState::Stopping
            | PropolisInstanceState::Stopped
            | PropolisInstanceState::Destroyed => {
                InstanceStateRequested::Stopped
            }
            PropolisInstanceState::Rebooting => InstanceStateRequested::Reboot,
        })
    }

    fn ready_to_destroy(&self) -> bool {
        self.destroyed
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
