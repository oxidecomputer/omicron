// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements simulated instances.

use super::simulatable::Simulatable;

use crate::common::instance::ObservedPropolisState;
use crate::nexus::NexusClient;
use crate::params::{InstanceMigrationSourceParams, InstanceStateRequested};
use async_trait::async_trait;
use nexus_client;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState as ApiInstanceState;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceMigrateStatusResponse as PropolisMigrateStatus;
use propolis_client::api::InstanceState as PropolisInstanceState;
use propolis_client::api::InstanceStateMonitorResponse;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

#[derive(Clone, Debug)]
enum MonitorChange {
    InstanceState(PropolisInstanceState),
    MigrateStatus(PropolisMigrateStatus),
}

/// A simulation of an Instance created by the external Oxide API.
///
/// This simulation tries to emulate Propolis's state machine as faithfully as
/// possible within reason so that it can be used as a test double in Nexus
/// integration tests.
///
/// The simulated instance contains a fake instance state stored as a
/// [`propolis_client::api::InstanceStateMonitorResponse`]. Transition requests
/// enqueue changes to either the instance state or the migration status fields
/// of this response. When poked, the simulated instance applies the next
/// transition, translates this to an observed Propolis state, and sends it
/// off for processing.
#[derive(Debug)]
struct SimInstanceInner {
    /// The current simulated instance state.
    state: InstanceStates,

    /// The fake Propolis state that was last used to update the instance
    /// runtime state.
    last_response: InstanceStateMonitorResponse,

    /// The queue of changes to apply to the fake Propolis state.
    queue: VecDeque<MonitorChange>,

    /// True if the instance has undergone a transition to a terminal Propolis
    /// state (i.e. one where the internal Propolis instance is destroyed).
    destroyed: bool,
}

impl SimInstanceInner {
    /// Pushes a Propolis instance state transition to the state change queue.
    fn queue_propolis_state(&mut self, propolis_state: PropolisInstanceState) {
        self.queue.push_back(MonitorChange::InstanceState(propolis_state));
    }

    /// Pushes a Propolis migration status to the state change queue.
    fn queue_migration_status(
        &mut self,
        migrate_status: PropolisMigrateStatus,
    ) {
        self.queue.push_back(MonitorChange::MigrateStatus(migrate_status))
    }

    /// Searches the queue for its last Propolis state change transition. If
    /// one exists, returns the associated Propolis state.
    fn last_queued_instance_state(&self) -> Option<PropolisInstanceState> {
        self.queue
            .iter()
            .filter_map(|entry| match entry {
                MonitorChange::InstanceState(state) => Some(state),
                _ => None,
            })
            .last()
            .copied()
    }

    /// Handles a request of the simulated sled agent to change an instance's
    /// state by queuing the appropriate state transitions and, if necessary,
    /// returning an action for the caller to simulate.
    fn request_transition(
        &mut self,
        target: &InstanceStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        match target {
            InstanceStateRequested::MigrationTarget(_) => {
                match self.next_resting_state() {
                    ApiInstanceState::Creating => {
                        self.queue_propolis_state(
                            PropolisInstanceState::Migrating,
                        );

                        let migration_id =
                            self.state.current().migration_id.expect(
                                "should have migration ID set before getting \
                                    request to migrate in",
                            );
                        self.queue_migration_status(PropolisMigrateStatus {
                            migration_id,
                            state: propolis_client::api::MigrationState::Sync,
                        });
                        self.queue_migration_status(PropolisMigrateStatus {
                            migration_id,
                            state: propolis_client::api::MigrationState::Finish,
                        });
                        self.queue_propolis_state(
                            PropolisInstanceState::Running,
                        );
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
                        self.queue_propolis_state(
                            PropolisInstanceState::Running,
                        );
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
                        self.queue_propolis_state(
                            PropolisInstanceState::Stopping,
                        );
                        self.queue_propolis_state(
                            PropolisInstanceState::Stopped,
                        );
                        self.queue_propolis_state(
                            PropolisInstanceState::Destroyed,
                        );
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
                        self.queue_propolis_state(
                            PropolisInstanceState::Rebooting,
                        );
                        self.queue_propolis_state(
                            PropolisInstanceState::Running,
                        );
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

    /// Simulates the next state transition on the queue, if one exists.
    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        if let Some(change) = self.queue.pop_front() {
            match change {
                MonitorChange::InstanceState(state) => {
                    if matches!(state, PropolisInstanceState::Destroyed) {
                        self.destroyed = true;
                    }
                    self.last_response.state = state;
                }
                MonitorChange::MigrateStatus(status) => {
                    self.last_response.migration = Some(status);
                }
            }

            self.state.apply_propolis_observation(&ObservedPropolisState::new(
                &self.current(),
                &self.last_response,
            ))
        } else {
            None
        }
    }

    /// Yields the current simulated instance runtime state.
    fn current(&self) -> InstanceRuntimeState {
        self.state.current().clone()
    }

    /// If the state change queue contains at least once instance state change,
    /// returns the requested instance state associated with the last instance
    /// state on the queue. Returns None otherwise.
    fn desired(&self) -> Option<InstanceStateRequested> {
        self.last_queued_instance_state().map(|terminal| match terminal {
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

    /// Indicates whether the instance is logically destroyed.
    fn ready_to_destroy(&self) -> bool {
        self.destroyed
    }

    /// Returns the "resting" state the simulated instance will reach if its
    /// queue is drained.
    fn next_resting_state(&self) -> ApiInstanceState {
        if self.queue.is_empty() {
            self.state.current().run_state
        } else {
            if let Some(last_state) = self.last_queued_instance_state() {
                crate::common::instance::InstanceState::from(last_state).0
            } else {
                self.state.current().run_state
            }
        }
    }

    /// Indicates whether there is a reboot transition pending for this
    /// instance.
    fn reboot_pending(&self) -> bool {
        self.queue.iter().any(|s| {
            matches!(
                s,
                MonitorChange::InstanceState(PropolisInstanceState::Rebooting)
            )
        })
    }

    /// Simulates rude termination by moving the instance to the Destroyed state
    /// immediately and clearing the queue of pending state transitions.
    fn terminate(&mut self) -> InstanceRuntimeState {
        self.state.transition(ApiInstanceState::Destroyed);
        self.queue.clear();
        self.state.current().clone()
    }

    /// Stores a set of migration IDs in the instance's runtime state.
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
                last_response: InstanceStateMonitorResponse {
                    gen: 1,
                    state: PropolisInstanceState::Creating,
                    migration: None,
                },
                queue: VecDeque::new(),
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
