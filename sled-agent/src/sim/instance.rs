// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use super::simulatable::Simulatable;

use crate::common::instance::InstanceState;
use crate::nexus::NexusClient;
use crate::params::{InstanceMigrationSourceParams, InstanceStateRequested};
use async_trait::async_trait;
use nexus_client;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState as ApiInstanceState;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceState as PropolisInstanceState;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

/// A simulation of an Instance created by the external Oxide API.
///
/// This simulation tries to emulate Propolis's state machine as faithfully as
/// possible within reason so that it can be used as a test double in Nexus
/// integration tests.
#[derive(Debug)]
struct SimInstanceInner {
    /// The current simulated instance state.
    state: InstanceStates,

    /// A queue of Propolis instance states for the simulated state driver to
    /// observe in response to calls to `execute_desired_transition`.
    propolis_queue: VecDeque<PropolisInstanceState>,

    /// Indicates whether the instance has been logically destroyed.
    destroyed: bool,
}

impl SimInstanceInner {
    fn request_transition(
        &mut self,
        target: &InstanceStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        match target {
            InstanceStateRequested::MigrationTarget(_) => {
                match self.next_resting_state() {
                    ApiInstanceState::Creating => {
                        self.propolis_queue
                            .push_back(PropolisInstanceState::Migrating);
                        self.propolis_queue
                            .push_back(PropolisInstanceState::Running);
                    }
                    _ => {
                        return Err(Error::invalid_request(&format!(
                            "can't request migration in with pending resting \
                            state {}",
                            self.next_resting_state()
                        )))
                    }
                }
            }
            InstanceStateRequested::Running => {
                match self.next_resting_state() {
                    ApiInstanceState::Creating => {
                        // The non-simulated sled agent explicitly and
                        // synchronously publishes the "Starting" state when
                        // cold-booting a new VM (so that the VM appears to be
                        // starting while its Propolis process is being
                        // launched).
                        self.state.transition(ApiInstanceState::Starting);
                        self.propolis_queue
                            .push_back(PropolisInstanceState::Running);
                    }
                    ApiInstanceState::Starting
                    | ApiInstanceState::Running
                    | ApiInstanceState::Rebooting
                    | ApiInstanceState::Migrating => {}

                    // Propolis forbids direct transitions from a stopped state
                    // back to a running state. Callers who want to restart a
                    // stopped instance must recreate it.
                    ApiInstanceState::Stopping
                    | ApiInstanceState::Stopped
                    | ApiInstanceState::Repairing
                    | ApiInstanceState::Failed
                    | ApiInstanceState::Destroyed => {
                        return Err(Error::invalid_request(&format!(
                            "can't request state Running with pending resting \
                        state {}",
                            self.next_resting_state()
                        )))
                    }
                }
            }
            InstanceStateRequested::Stopped => {
                match self.next_resting_state() {
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
                            "can't request state Stopped with pending resting \
                        state {}",
                            self.next_resting_state()
                        )))
                    }
                }
            }
            InstanceStateRequested::Reboot => match self.next_resting_state() {
                ApiInstanceState::Running => {
                    // Further requests to reboot are ignored if the instance
                    // is currently rebooting or about to reboot.
                    if self.state.current().run_state
                        != ApiInstanceState::Rebooting
                        && !self.reboot_pending()
                    {
                        self.state.transition(ApiInstanceState::Rebooting);
                        self.propolis_queue
                            .push_back(PropolisInstanceState::Rebooting);
                        self.propolis_queue
                            .push_back(PropolisInstanceState::Running);
                    }
                }
                _ => {
                    return Err(Error::invalid_request(&format!(
                        "can't request Reboot with pending resting state {}",
                        self.next_resting_state()
                    )))
                }
            },
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

    fn current(&self) -> InstanceRuntimeState {
        self.state.current().clone()
    }

    fn desired(&self) -> Option<InstanceStateRequested> {
        self.propolis_queue.back().map(|terminal| match terminal {
            // State change requests may queue these states as intermediate
            // states, but the simulation (and the tests that rely on it) is
            // currently not expected to come to rest in any of these states.
            //
            // This is an internal invariant of the current simulation; panic
            // to assert it here.
            PropolisInstanceState::Creating
            | PropolisInstanceState::Starting
            | PropolisInstanceState::Migrating
            | PropolisInstanceState::Repairing
            | PropolisInstanceState::Failed => panic!(
                "pending resting state {:?} doesn't map to a requested state",
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

    /// Returns the "resting" state the simulated instance will reach if its
    /// queue is drained.
    fn next_resting_state(&self) -> ApiInstanceState {
        match self.propolis_queue.back() {
            None => self.state.current().run_state,
            Some(p) => InstanceState::from(*p).0,
        }
    }

    /// Indicates whether there is a reboot transition pending for this
    /// instance.
    fn reboot_pending(&self) -> bool {
        self.propolis_queue
            .iter()
            .any(|s| matches!(s, PropolisInstanceState::Rebooting))
    }

    fn terminate(&mut self) -> InstanceRuntimeState {
        self.state.transition(ApiInstanceState::Destroyed);
        self.propolis_queue.clear();
        self.state.current().clone()
    }
    fn put_migration_ids(
        &mut self,
        old_runtime: &InstanceRuntimeState,
        ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        if self.state.migration_ids_already_set(old_runtime, ids) {
            return Ok(self.state.current().clone());
        }

        if self.state.current().propolis_gen != old_runtime.propolis_gen {
            return Err(Error::InvalidRequest {
                message: format!(
                    "wrong Propolis ID generation: expected {}, got {}",
                    self.state.current().propolis_gen,
                    old_runtime.propolis_gen
                ),
            });
        }

        self.state.set_migration_ids(ids);
        Ok(self.state.current().clone())
    }
}

/// A simulation of an Instance created by the external Oxide API.
///
/// Normally, to change a simulated object's state, the simulated sled agent
/// invokes a generic routine on the object's `SimCollection` that calls the
/// appropriate `Simulatable` routine on the object in question while holding
/// the collection's (private) lock. This works fine for generic functionality
/// that all `Simulatable`s share, but instances have some instance-specific
/// extensions that are not appropriate to implement in that trait.
///
/// To allow sled agent to modify instances in instance-specific ways without
/// having to hold a `SimCollection` lock, this struct wraps the instance state
/// in an `Arc` and `Mutex` and then derives `Clone`. This way, the simulated
/// sled agent proper can ask the instance collection for a clone of a specific
/// registered instance and get back a reference to the same instance the
/// `SimCollection` APIs will operate on.
#[derive(Debug, Clone)]
pub struct SimInstance {
    inner: Arc<Mutex<SimInstanceInner>>,
}

impl SimInstance {
    pub fn terminate(&self) -> InstanceRuntimeState {
        self.inner.lock().unwrap().terminate()
    }

    pub async fn put_migration_ids(
        &self,
        old_runtime: &InstanceRuntimeState,
        ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.put_migration_ids(old_runtime, ids)
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
            inner: Arc::new(Mutex::new(SimInstanceInner {
                state: InstanceStates::new(current),
                propolis_queue: VecDeque::new(),
                destroyed: false,
            })),
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
        self.inner.lock().unwrap().request_transition(target)
    }

    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        self.inner.lock().unwrap().execute_desired_transition()
    }

    fn generation(&self) -> Generation {
        self.inner.lock().unwrap().current().gen
    }

    fn current(&self) -> Self::CurrentState {
        self.inner.lock().unwrap().current()
    }

    fn desired(&self) -> Option<Self::RequestedState> {
        self.inner.lock().unwrap().desired()
    }

    fn ready_to_destroy(&self) -> bool {
        self.inner.lock().unwrap().ready_to_destroy()
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

    fn resource_type() -> ResourceType {
        ResourceType::Instance
    }
}
