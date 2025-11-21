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
use omicron_common::api::internal::nexus::{SledVmmState, VmmState};
use omicron_uuid_kinds::{GenericUuid, PropolisUuid};
use propolis_client::types::{
    InstanceMigrateStatusResponse as PropolisMigrateResponse,
    InstanceMigrationStatus as PropolisMigrationStatus,
    InstanceState as PropolisInstanceState, InstanceStateMonitorResponse,
};
use sled_agent_types::instance::VmmStateRequested;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::common::instance::{Action as InstanceAction, InstanceStates};

pub use sled_agent_client::{
    SimulateMigrationSource, SimulatedMigrationResult,
};

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

    /// Queue a simulated migration out.
    fn queue_migration_out(
        &mut self,
        migration_id: Uuid,
        result: SimulatedMigrationResult,
    ) {
        let migration_update = |state| PropolisMigrateResponse {
            migration_in: None,
            migration_out: Some(PropolisMigrationStatus {
                id: migration_id,
                state,
            }),
        };
        // Propolis transitions to the Migrating state once before
        // actually starting migration.
        self.queue_propolis_state(PropolisInstanceState::Migrating);
        self.queue_migration_update(migration_update(
            propolis_client::types::MigrationState::Sync,
        ));
        match result {
            SimulatedMigrationResult::Success => {
                self.queue_migration_update(migration_update(
                    propolis_client::types::MigrationState::Finish,
                ));
                self.queue_graceful_stop();
            }
            SimulatedMigrationResult::Failure => {
                todo!("finish this part when we actuall need it...")
            }
        }
    }

    /// Queue a simulated migration in.
    fn queue_migration_in(
        &mut self,
        migration_id: Uuid,
        result: SimulatedMigrationResult,
    ) {
        let migration_update = |state| PropolisMigrateResponse {
            migration_in: Some(PropolisMigrationStatus {
                id: migration_id,
                state,
            }),
            migration_out: None,
        };
        // Propolis transitions to the Migrating state once before
        // actually starting migration.
        self.queue_propolis_state(PropolisInstanceState::Migrating);
        self.queue_migration_update(migration_update(
            propolis_client::types::MigrationState::Sync,
        ));
        match result {
            SimulatedMigrationResult::Success => {
                self.queue_migration_update(migration_update(
                    propolis_client::types::MigrationState::Finish,
                ));
                self.queue_propolis_state(PropolisInstanceState::Running)
            }
            SimulatedMigrationResult::Failure => {
                todo!("finish this part when we actually need it...")
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
            .next_back()
            .copied()
    }

    /// Handles a request of the simulated sled agent to change an instance's
    /// state by queuing the appropriate state transitions and, if necessary,
    /// returning an action for the caller to simulate.
    fn request_transition(
        &mut self,
        target: &VmmStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        match target {
            // When Nexus intends to migrate into a VMM, it should create that
            // VMM in the Migrating state and shouldn't request anything else
            // from it before asking to migrate in.
            VmmStateRequested::MigrationTarget(_) => {
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

                let migration_id = self
                    .state
                    .migration_in()
                    .ok_or_else(|| {
                        Error::invalid_request(
                            "can't request migration in for a vmm that wasn't \
                        created with a migration ID",
                        )
                    })?
                    .migration_id;
                self.queue_migration_in(
                    migration_id,
                    SimulatedMigrationResult::Success,
                );
            }
            VmmStateRequested::Running => {
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
                        )));
                    }
                }
            }
            VmmStateRequested::Stopped => {
                match self.next_resting_state() {
                    VmmState::Starting => {
                        self.state.force_state_to_destroyed();
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
                        )));
                    }
                }
            }
            VmmStateRequested::Reboot => match self.next_resting_state() {
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
                    )));
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
                &self.last_response,
            ));

            self.state.vmm_is_halted().then_some(InstanceAction::Destroy)
        } else {
            None
        }
    }

    /// If the state change queue contains at least once instance state change,
    /// returns the requested instance state associated with the last instance
    /// state on the queue. Returns None otherwise.
    fn desired(&self) -> Option<VmmStateRequested> {
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
            PropolisInstanceState::Running => VmmStateRequested::Running,
            PropolisInstanceState::Stopping
            | PropolisInstanceState::Stopped
            | PropolisInstanceState::Destroyed => VmmStateRequested::Stopped,
            PropolisInstanceState::Rebooting => VmmStateRequested::Reboot,
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

    /// Simulates rude termination by moving the instance to the Failed state
    /// immediately and clearing the queue of pending state transitions.
    fn terminate(&mut self) -> SledVmmState {
        self.state.force_state_to_failed();
        self.queue.clear();
        self.destroyed = true;
        self.state.sled_instance_state()
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
    pub fn terminate(&self) -> SledVmmState {
        self.inner.lock().unwrap().terminate()
    }

    pub(crate) fn set_simulated_migration_source(
        &self,
        migration: SimulateMigrationSource,
    ) {
        self.inner
            .lock()
            .unwrap()
            .queue_migration_out(migration.migration_id, migration.result);
    }
}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = SledVmmState;
    type RequestedState = VmmStateRequested;
    type ProducerArgs = ();
    type Action = InstanceAction;

    fn new(current: SledVmmState) -> Self {
        assert!(
            matches!(
                current.vmm_state.state,
                VmmState::Starting | VmmState::Migrating
            ),
            "new VMMs should always be registered in the Starting or Migrating \
            state (supplied state: {:?})",
            current.vmm_state.state
        );

        SimInstance {
            inner: Arc::new(Mutex::new(SimInstanceInner {
                state: InstanceStates::new(
                    current.vmm_state,
                    current.migration_in.map(|m| m.migration_id),
                ),
                last_response: InstanceStateMonitorResponse {
                    r#gen: 1,
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
        target: &VmmStateRequested,
    ) -> Result<Option<InstanceAction>, Error> {
        self.inner.lock().unwrap().request_transition(target)
    }

    fn execute_desired_transition(&mut self) -> Option<InstanceAction> {
        self.inner.lock().unwrap().execute_desired_transition()
    }

    fn generation(&self) -> Generation {
        self.inner.lock().unwrap().state.vmm().generation
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
                &PropolisUuid::from_untyped_uuid(*id),
                &nexus_client::types::SledVmmState::from(current),
            )
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    fn resource_type() -> ResourceType {
        ResourceType::Instance
    }
}
