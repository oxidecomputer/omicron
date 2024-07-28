// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implements simulated instances.

use super::simulatable::Simulatable;

use crate::common::instance::{ObservedPropolisState, PublishedVmmState};
use crate::nexus::NexusClient;
use async_trait::async_trait;
use chrono::Utc;
use nexus_client;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::nexus::{
    InstanceRuntimeState, MigrationRole, SledInstanceState, VmmState,
};
use propolis_client::types::{
    InstanceMigrateStatusResponse as PropolisMigrateResponse,
    InstanceMigrationStatus as PropolisMigrationStatus,
    InstanceState as PropolisInstanceState, InstanceStateMonitorResponse,
};
use sled_agent_types::instance::{
    InstanceMigrationSourceParams, InstanceStateRequested,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

#[derive(Clone, Debug)]
enum MonitorChange {
    PropolisState(PropolisInstanceState),
    MigrateStatus(PropolisMigrateResponse),
}

/// A simulation of an Instance created by the external Oxide API.
///
/// This simulation tries to emulate Propolis's state machine as faithfully as
/// possible within reason so that it can be used as a test double in Nexus
/// integration tests.
///
/// The simulated instance contains a fake instance state stored as a
/// [`propolis_client::types::InstanceStateMonitorResponse`]. Transition
/// requests enqueue changes to either the instance state or the migration
/// status fields of this response. When poked, the simulated instance applies
/// the next transition, translates this to an observed Propolis state, and
/// sends it off for processing.
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
        self.queue.push_back(MonitorChange::PropolisState(propolis_state));
    }

    /// Pushes a Propolis migration update to the state change queue.
    fn queue_migration_update(
        &mut self,
        migrate_status: PropolisMigrateResponse,
    ) {
        self.queue.push_back(MonitorChange::MigrateStatus(migrate_status))
    }

    /// Queue a successful simulated migration.
    ///
    fn queue_successful_migration(&mut self, role: MigrationRole) {
        // Propolis transitions to the Migrating state once before
        // actually starting migration.
        self.queue_propolis_state(PropolisInstanceState::Migrating);
        let migration_id =
            self.state.instance().migration_id.unwrap_or_else(|| {
                panic!(
                    "should have migration ID set before getting request to
                    migrate in (current state: {:?})",
                    self
                )
            });

        match role {
            MigrationRole::Source => {
                self.queue_migration_update(PropolisMigrateResponse {
                    migration_in: None,
                    migration_out: Some(PropolisMigrationStatus {
                        id: migration_id,
                        state: propolis_client::types::MigrationState::Sync,
                    }),
                });
                self.queue_migration_update(PropolisMigrateResponse {
                    migration_in: None,
                    migration_out: Some(PropolisMigrationStatus {
                        id: migration_id,
                        state: propolis_client::types::MigrationState::Finish,
                    }),
                });
                self.queue_graceful_stop();
            }
            MigrationRole::Target => {
                self.queue_migration_update(PropolisMigrateResponse {
                    migration_in: Some(PropolisMigrationStatus {
                        id: migration_id,
                        state: propolis_client::types::MigrationState::Sync,
                    }),
                    migration_out: None,
                });
                self.queue_migration_update(PropolisMigrateResponse {
                    migration_in: Some(PropolisMigrationStatus {
                        id: migration_id,
                        state: propolis_client::types::MigrationState::Finish,
                    }),
                    migration_out: None,
                });
                self.queue_propolis_state(PropolisInstanceState::Running)
            }
        }
    }

    fn queue_graceful_stop(&mut self) {
        self.state.transition_vmm(PublishedVmmState::Stopping, Utc::now());
        self.queue_propolis_state(PropolisInstanceState::Stopping);
        self.queue_propolis_state(PropolisInstanceState::Stopped);
        self.queue_propolis_state(PropolisInstanceState::Destroyed);
    }

    /// Searches the queue for its last Propolis state change transition. If
    /// one exists, returns the associated Propolis state.
    fn last_queued_instance_state(&self) -> Option<PropolisInstanceState> {
        self.queue
            .iter()
            .filter_map(|entry| match entry {
                MonitorChange::PropolisState(state) => Some(state),
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
            // When Nexus intends to migrate into a VMM, it should create that
            // VMM in the Migrating state and shouldn't request anything else
            // from it before asking to migrate in.
            InstanceStateRequested::MigrationTarget(_) => {
                if !self.queue.is_empty() {
                    return Err(Error::invalid_request(&format!(
                        "can't request migration in with a non-empty state
                        transition queue (current state: {:?})",
                        self
                    )));
                }
                if self.state.vmm().state != VmmState::Migrating {
                    return Err(Error::invalid_request(&format!(
                        "can't request migration in for a vmm that wasn't \
                        created in the migrating state (current state: {:?})",
                        self
                    )));
                }

                self.queue_successful_migration(MigrationRole::Target)
            }
            InstanceStateRequested::Running => {
                match self.next_resting_state() {
                    VmmState::Starting => {
                        self.queue_propolis_state(
                            PropolisInstanceState::Running,
                        );
                    }
                    VmmState::Running
                    | VmmState::Rebooting
                    | VmmState::Migrating => {}

                    // Propolis forbids direct transitions from a stopped state
                    // back to a running state. Callers who want to restart a
                    // stopped instance must recreate it.
                    VmmState::Stopping
                    | VmmState::Stopped
                    | VmmState::Failed
                    | VmmState::Destroyed => {
                        return Err(Error::invalid_request(&format!(
                            "can't request state Running with pending resting \
                            state {:?} (current state: {:?})",
                            self.next_resting_state(),
                            self
                        )))
                    }
                }
            }
            InstanceStateRequested::Stopped => {
                match self.next_resting_state() {
                    VmmState::Starting => {
                        let mark_failed = false;
                        self.state.terminate_rudely(mark_failed);
                    }
                    VmmState::Running => self.queue_graceful_stop(),
                    // Idempotently allow requests to stop an instance that is
                    // already stopping.
                    VmmState::Stopping
                    | VmmState::Stopped
                    | VmmState::Destroyed => {}
                    _ => {
                        return Err(Error::invalid_request(&format!(
                            "can't request state Stopped with pending resting \
                            state {:?} (current state: {:?})",
                            self.next_resting_state(),
                            self
                        )))
                    }
                }
            }
            InstanceStateRequested::Reboot => match self.next_resting_state() {
                VmmState::Running => {
                    // Further requests to reboot are ignored if the instance
                    // is currently rebooting or about to reboot.
                    if self.state.vmm().state != VmmState::Rebooting
                        && !self.reboot_pending()
                    {
                        self.state.transition_vmm(
                            PublishedVmmState::Rebooting,
                            Utc::now(),
                        );
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
                        "can't request Reboot with pending resting state {:?} \
                        (current state: {:?})",
                        self.next_resting_state(),
                        self
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
                MonitorChange::PropolisState(state) => {
                    if matches!(state, PropolisInstanceState::Destroyed) {
                        self.destroyed = true;
                    }
                    self.last_response.state = state;
                }
                MonitorChange::MigrateStatus(status) => {
                    self.last_response.migration = status;
                }
            }

            self.state.apply_propolis_observation(&ObservedPropolisState::new(
                self.state.instance(),
                &self.last_response,
            ))
        } else {
            None
        }
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
    fn next_resting_state(&self) -> VmmState {
        if self.queue.is_empty() {
            self.state.vmm().state
        } else {
            if let Some(last_state) = self.last_queued_instance_state() {
                use PropolisInstanceState as PropolisState;
                match last_state {
                    PropolisState::Creating | PropolisState::Starting => {
                        VmmState::Starting
                    }
                    PropolisState::Running => VmmState::Running,
                    PropolisState::Stopping => VmmState::Stopping,
                    PropolisState::Stopped => VmmState::Stopped,
                    PropolisState::Rebooting => VmmState::Rebooting,
                    PropolisState::Migrating => VmmState::Migrating,
                    PropolisState::Failed => VmmState::Failed,
                    PropolisState::Destroyed => VmmState::Destroyed,
                    PropolisState::Repairing => {
                        unreachable!("Propolis doesn't use the Repairing state")
                    }
                }
            } else {
                self.state.vmm().state
            }
        }
    }

    /// Indicates whether there is a reboot transition pending for this
    /// instance.
    fn reboot_pending(&self) -> bool {
        self.queue.iter().any(|s| {
            matches!(
                s,
                MonitorChange::PropolisState(PropolisInstanceState::Rebooting)
            )
        })
    }

    /// Simulates rude termination by moving the instance to the Destroyed state
    /// immediately and clearing the queue of pending state transitions.
    fn terminate(&mut self) -> SledInstanceState {
        let mark_failed = false;
        self.state.terminate_rudely(mark_failed);
        self.queue.clear();
        self.destroyed = true;
        self.state.sled_instance_state()
    }

    /// Stores a set of migration IDs in the instance's runtime state.
    fn put_migration_ids(
        &mut self,
        old_runtime: &InstanceRuntimeState,
        ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        if self.state.migration_ids_already_set(old_runtime, ids) {
            return Ok(self.state.sled_instance_state());
        }

        if self.state.instance().gen != old_runtime.gen {
            return Err(Error::invalid_request(format!(
                "wrong Propolis ID generation: expected {}, got {}",
                self.state.instance().gen,
                old_runtime.gen
            )));
        }

        self.state.set_migration_ids(ids, Utc::now());

        // If we set migration IDs and are the migration source, ensure that we
        // will perform the correct state transitions to simulate a successful
        // migration.
        if ids.is_some() {
            let role = self
            .state
            .migration()
            .expect(
                "we just got a `put_migration_ids` request with `Some` IDs, \
                so we should have a migration"
            )
            .role;
            if role == MigrationRole::Source {
                self.queue_successful_migration(MigrationRole::Source)
            }
        }

        Ok(self.state.sled_instance_state())
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
    pub fn terminate(&self) -> SledInstanceState {
        self.inner.lock().unwrap().terminate()
    }

    pub async fn put_migration_ids(
        &self,
        old_runtime: &InstanceRuntimeState,
        ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.put_migration_ids(old_runtime, ids)
    }
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = SledInstanceState;
    type RequestedState = InstanceStateRequested;
    type ProducerArgs = ();
    type Action = InstanceAction;

    fn new(current: SledInstanceState) -> Self {
        assert!(matches!(
            current.vmm_state.state,
            VmmState::Starting | VmmState::Migrating),
            "new VMMs should always be registered in the Starting or Migrating \
            state (supplied state: {:?})",
            current.vmm_state.state
        );

        SimInstance {
            inner: Arc::new(Mutex::new(SimInstanceInner {
                state: InstanceStates::new(
                    current.instance_state,
                    current.vmm_state,
                    current.propolis_id,
                ),
                last_response: InstanceStateMonitorResponse {
                    gen: 1,
                    state: PropolisInstanceState::Starting,
                    migration: PropolisMigrateResponse {
                        migration_in: None,
                        migration_out: None,
                    },
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
        self.inner.lock().unwrap().state.vmm().gen
    }

    fn current(&self) -> Self::CurrentState {
        self.inner.lock().unwrap().state.sled_instance_state()
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
                &nexus_client::types::SledInstanceState::from(current),
            )
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    fn resource_type() -> ResourceType {
        ResourceType::Instance
    }
}
