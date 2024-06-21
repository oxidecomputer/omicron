// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use chrono::{DateTime, Utc};
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::{
    MigrationRuntimeState, MigrationState, SledInstanceState, VmmRuntimeState,
    VmmState,
};
use omicron_uuid_kinds::PropolisUuid;
use propolis_client::types::{
    InstanceMigrationStatus, InstanceState as PropolisApiState,
    InstanceStateMonitorResponse, MigrationState as PropolisMigrationState,
};
use uuid::Uuid;

/// The instance and VMM state that sled agent maintains on a per-VMM basis.
#[derive(Clone, Debug)]
pub struct InstanceStates {
    vmm: VmmRuntimeState,
    propolis_id: PropolisUuid,
    migration_in: Option<MigrationRuntimeState>,
    migration_out: Option<MigrationRuntimeState>,
}

/// Newtype to allow conversion from Propolis API states (returned by the
/// Propolis state monitor) to Nexus VMM states.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropolisInstanceState(PropolisApiState);

impl From<PropolisApiState> for PropolisInstanceState {
    fn from(value: PropolisApiState) -> Self {
        Self(value)
    }
}

impl From<PropolisInstanceState> for VmmState {
    fn from(value: PropolisInstanceState) -> Self {
        use propolis_client::types::InstanceState as State;
        match value.0 {
            // Nexus uses the VMM state as the externally-visible instance state
            // when an instance has an active VMM. A Propolis that is "creating"
            // its virtual machine objects is "starting" from the external API's
            // perspective.
            State::Creating | State::Starting => VmmState::Starting,
            State::Running => VmmState::Running,
            State::Stopping => VmmState::Stopping,
            // A Propolis that is stopped but not yet destroyed should still
            // appear to be Stopping from an external API perspective, since
            // they cannot be restarted yet. Instances become logically Stopped
            // once Propolis reports that the VM is Destroyed (see below).
            State::Stopped => VmmState::Stopping,
            State::Rebooting => VmmState::Rebooting,
            State::Migrating => VmmState::Migrating,
            State::Failed => VmmState::Failed,
            // Nexus needs to learn when a VM has entered the "destroyed" state
            // so that it can release its resource reservation. When this
            // happens, this module also clears the active VMM ID from the
            // instance record, which will accordingly set the Nexus-owned
            // instance state to Stopped, preventing this state from being used
            // as an externally-visible instance state.
            State::Destroyed => VmmState::Destroyed,
            // Propolis never actually uses the Repairing state, so this should
            // be unreachable, but since these states come from another process,
            // program defensively and convert Repairing to Running.
            State::Repairing => VmmState::Running,
        }
    }
}

/// Describes the status of the migration identified in an instance's runtime
/// state as it relates to any migration status information reported by the
/// instance's Propolis.
#[derive(Clone, Copy, Debug)]
pub enum ObservedMigrationStatus {
    /// The instance's runtime state and Propolis agree that no migration is in
    /// progress.
    NoMigration,

    /// The instance has a migration ID, but Propolis either has no migration ID
    /// or a different ID from this one (possible if the Propolis was
    /// initialized via migration in).
    Pending,

    /// Propolis reported that migration isn't done yet.
    InProgress,

    /// Propolis reported that the migration completed successfully.
    Succeeded,

    /// Propolis reported that the migration failed.
    Failed,
}

/// The information observed by the instance's Propolis state monitor.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedPropolisState {
    /// The state reported by Propolis's instance state monitor API.
    pub vmm_state: PropolisInstanceState,

    pub migration_in: Option<ObservedMigrationState>,
    pub migration_out: Option<ObservedMigrationState>,

    /// The approximate time at which this observation was made.
    pub time: DateTime<Utc>,
}

impl ObservedPropolisState {
    /// Constructs a Propolis state observation from an instance's current
    /// state and an instance state monitor response received from
    /// Propolis.
    pub fn new(propolis_state: &InstanceStateMonitorResponse) -> Self {
        Self {
            vmm_state: PropolisInstanceState(propolis_state.state),
            migration_in: propolis_state
                .migration
                .migration_in
                .as_ref()
                .map(ObservedMigrationState::from),
            migration_out: propolis_state
                .migration
                .migration_out
                .as_ref()
                .map(ObservedMigrationState::from),
            time: Utc::now(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ObservedMigrationState {
    state: MigrationState,
    id: Uuid,
}

impl From<&'_ InstanceMigrationStatus> for ObservedMigrationState {
    fn from(observed: &InstanceMigrationStatus) -> Self {
        let state = match observed.state {
            PropolisMigrationState::Error => MigrationState::Failed,
            PropolisMigrationState::Finish => MigrationState::Completed,
            _ => MigrationState::InProgress,
        };
        Self { state, id: observed.id }
    }
}

/// The set of instance states that sled agent can publish to Nexus. This is
/// a subset of the instance states Nexus knows about: the Creating and
/// Destroyed states are reserved for Nexus to use for instances that are being
/// created for the very first time or have been explicitly deleted.
pub enum PublishedVmmState {
    Stopping,
    Rebooting,
}

impl From<PublishedVmmState> for VmmState {
    fn from(value: PublishedVmmState) -> Self {
        match value {
            PublishedVmmState::Stopping => VmmState::Stopping,
            PublishedVmmState::Rebooting => VmmState::Rebooting,
        }
    }
}

/// Action to be taken on behalf of state transition.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    /// Terminate the VM and associated service.
    Destroy,
}

impl InstanceStates {
    pub fn new(
        vmm: VmmRuntimeState,
        propolis_id: PropolisUuid,
        migration_id: Option<Uuid>,
    ) -> Self {
        // If this instance is created with a migration ID, we are the intended
        // target of a migration in. Set that up now.
        let migration_in =
            migration_id.map(|migration_id| MigrationRuntimeState {
                migration_id,
                state: MigrationState::Pending,
                gen: Generation::new(),
                time_updated: Utc::now(),
            });
        InstanceStates { vmm, propolis_id, migration_in, migration_out: None }
    }

    pub fn vmm(&self) -> &VmmRuntimeState {
        &self.vmm
    }

    pub fn propolis_id(&self) -> PropolisUuid {
        self.propolis_id
    }

    pub fn migration_in(&self) -> Option<&MigrationRuntimeState> {
        self.migration_in.as_ref()
    }

    pub fn migration_out(&self) -> Option<&MigrationRuntimeState> {
        self.migration_out.as_ref()
    }

    /// Creates a `SledInstanceState` structure containing the entirety of this
    /// structure's runtime state. This requires cloning; for simple read access
    /// use the `instance` or `vmm` accessors instead.
    pub fn sled_instance_state(&self) -> SledInstanceState {
        SledInstanceState {
            vmm_state: self.vmm.clone(),
            propolis_id: self.propolis_id,
            // migration_state: self.migration.clone(),
            migration_in: self.migration_in.clone(),
            migration_out: self.migration_out.clone(),
        }
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub(crate) fn apply_propolis_observation(
        &mut self,
        observed: &ObservedPropolisState,
    ) -> Option<Action> {
        fn transition_migration(
            current: &mut Option<MigrationRuntimeState>,
            ObservedMigrationState { id, state }: ObservedMigrationState,
            now: DateTime<Utc>,
        ) {
            if let Some(ref mut m) = current {
                // Don't generate spurious state updates if the migration is already in
                // the state we're transitioning to.
                if m.migration_id == id && m.state == state {
                    return;
                }
                m.state = state;
                if m.migration_id == id {
                    m.gen = m.gen.next();
                } else {
                    m.migration_id = id;
                    m.gen = Generation::new();
                }
                m.time_updated = now;
            } else {
                *current = Some(MigrationRuntimeState {
                    migration_id: id,
                    gen: Generation::new(),
                    state,
                    time_updated: now,
                });
            }
        }

        fn destroy_migration(
            migration: &mut MigrationRuntimeState,
            now: DateTime<Utc>,
        ) {
            if matches!(
                migration.state,
                MigrationState::InProgress | MigrationState::Pending
            ) {
                migration.gen = migration.gen.next();
                migration.time_updated = now;
                migration.state = MigrationState::Failed;
            }
        }

        let vmm_gone = matches!(
            observed.vmm_state.0,
            PropolisApiState::Destroyed | PropolisApiState::Failed
        );

        // Apply this observation to the VMM record. It is safe to apply the
        // Destroyed state directly here because this routine ensures that if
        // this VMM is active, it will be retired and an appropriate
        // non-Destroyed state applied to the instance itself.
        self.vmm.state = observed.vmm_state.into();
        self.vmm.gen = self.vmm.gen.next();
        self.vmm.time_updated = observed.time;

        // Update the instance record to reflect the result of any completed
        // migration.
        if let Some(m) = observed.migration_in {
            transition_migration(&mut self.migration_in, m, observed.time);
        }
        if let Some(m) = observed.migration_out {
            transition_migration(&mut self.migration_out, m, observed.time);
        }

        // If this Propolis has exited, tear down its zone. If it was in the
        // active position, immediately retire any migration that might have
        // been pending and clear the active Propolis ID so that the instance
        // can start somewhere else.
        //
        // N.B. It is important to refetch the current Propolis role here,
        //      because it might have changed in the course of dealing with a
        //      completed migration. (In particular, if this VMM is gone because
        //      it was the source of a successful migration out, control has
        //      been transferred to the target, and what was once an active VMM
        //      is now retired.)
        if vmm_gone {
            // If there's an active migration and the VMM is suddenly gone,
            // that should constitute a migration failure!
            if let Some(ref mut m) = self.migration_in {
                destroy_migration(m, observed.time);
            }
            if let Some(ref mut m) = self.migration_out {
                destroy_migration(m, observed.time);
            }
            Some(Action::Destroy)
        } else {
            None
        }
    }

    /// Forcibly transitions this instance's VMM into the specified `next`
    /// state and updates its generation number.
    pub(crate) fn transition_vmm(
        &mut self,
        next: PublishedVmmState,
        now: DateTime<Utc>,
    ) {
        self.vmm.state = next.into();
        self.vmm.gen = self.vmm.gen.next();
        self.vmm.time_updated = now;
    }

    /// Updates the state of this instance in response to a rude termination of
    /// its Propolis zone, marking the VMM as destroyed and applying any
    /// consequent state updates.
    ///
    /// # Synchronization
    ///
    /// A caller who is rudely terminating a Propolis zone must hold locks
    /// sufficient to ensure that no other Propolis observations arrive in the
    /// transaction that terminates the zone and then calls this function.
    ///
    /// TODO(#4004): This routine works by synthesizing a Propolis state change
    /// that says "this Propolis is destroyed and its active migration failed."
    /// If this conflicts with the actual Propolis state--e.g., if the
    /// underlying Propolis was destroyed but migration *succeeded*--the
    /// instance's state in Nexus may become inconsistent. This routine should
    /// therefore only be invoked by callers who know that an instance is not
    /// migrating.
    pub(crate) fn terminate_rudely(&mut self, mark_failed: bool) {
        let vmm_state = if mark_failed {
            PropolisInstanceState(PropolisApiState::Failed)
        } else {
            PropolisInstanceState(PropolisApiState::Destroyed)
        };

        let fake_observed = ObservedPropolisState {
            vmm_state,
            // We don't actually need to populate these, because observing a
            // `Destroyed` instance state will fail any in progress migrations anyway.
            migration_in: None,
            migration_out: None,
            time: Utc::now(),
        };

        self.apply_propolis_observation(&fake_observed);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use chrono::Utc;
    use omicron_common::api::external::Generation;
    use propolis_client::types::InstanceState as Observed;
    use uuid::Uuid;

    fn make_instance() -> InstanceStates {
        let propolis_id = PropolisUuid::new_v4();
        let now = Utc::now();

        let vmm = VmmRuntimeState {
            state: VmmState::Starting,
            gen: Generation::new(),
            time_updated: now,
        };

        InstanceStates::new(vmm, propolis_id, None)
    }

    fn make_migration_source_instance() -> InstanceStates {
        let mut state = make_instance();
        state.vmm.state = VmmState::Migrating;
        let migration_id = Uuid::new_v4();
        state.migration_out = Some(MigrationRuntimeState {
            migration_id,
            state: MigrationState::InProgress,
            // advance the generation once, since we are starting out in the
            // `InProgress` state.
            gen: Generation::new().next(),
            time_updated: Utc::now(),
        });

        state
    }

    fn make_migration_target_instance() -> InstanceStates {
        let propolis_id = PropolisUuid::new_v4();
        let now = Utc::now();

        let vmm = VmmRuntimeState {
            state: VmmState::Migrating,
            gen: Generation::new(),
            time_updated: now,
        };

        InstanceStates::new(vmm, propolis_id, Some(Uuid::new_v4()))
    }

    fn make_observed_state(
        propolis_state: PropolisInstanceState,
    ) -> ObservedPropolisState {
        ObservedPropolisState {
            vmm_state: propolis_state,
            migration_in: None,
            migration_out: None,
            time: Utc::now(),
        }
    }

    /// Checks to see if the instance state structures `prev` and `next` have a
    /// difference that should produce a change in generation and asserts that
    /// such a change occurred.
    fn assert_state_change_has_gen_change(
        prev: &InstanceStates,
        next: &InstanceStates,
    ) {
        // Propolis is free to publish no-op VMM state updates (e.g. when an
        // in-progress migration's state changes but the migration is not yet
        // complete), so don't test the converse here.
        if prev.vmm.gen == next.vmm.gen {
            assert_eq!(prev.vmm.state, next.vmm.state);
        }
    }

    #[test]
    fn propolis_terminal_states_request_destroy_action() {
        for state in [Observed::Destroyed, Observed::Failed] {
            let mut instance_state = make_instance();
            let requested_action = instance_state
                .apply_propolis_observation(&make_observed_state(state.into()));

            assert!(matches!(requested_action, Some(Action::Destroy)));
        }
    }

    #[test]
    fn source_termination_fails_in_progress_migration() {
        for state in [Observed::Destroyed, Observed::Failed] {
            let mut instance_state = make_migration_source_instance();
            let original_migration =
                instance_state.clone().migration_out.unwrap();
            let requested_action = instance_state
                .apply_propolis_observation(&make_observed_state(state.into()));

            let migration = instance_state
                .migration_out
                .expect("state must have a migration");
            assert_eq!(migration.state, MigrationState::Failed);
            assert!(migration.gen > original_migration.gen);
            assert!(matches!(requested_action, Some(Action::Destroy)));
        }
    }

    #[test]
    fn target_termination_fails_in_progress_migration() {
        for state in [Observed::Destroyed, Observed::Failed] {
            let mut instance_state = make_migration_target_instance();
            let original_migration =
                instance_state.clone().migration_in.unwrap();
            let requested_action = instance_state
                .apply_propolis_observation(&make_observed_state(state.into()));

            let migration = instance_state
                .migration_in
                .expect("state must have a migration");
            assert_eq!(migration.state, MigrationState::Failed);
            assert!(migration.gen > original_migration.gen);
            assert!(matches!(requested_action, Some(Action::Destroy)));
        }
    }

    #[test]
    fn destruction_after_migration_out_does_not_transition() {
        let mut state = make_migration_source_instance();
        let migration_id = state.migration_out.as_ref().unwrap().migration_id;

        // After a migration succeeds, the source VM appears to stop but reports
        // that the migration has succeeded.
        let mut observed = ObservedPropolisState {
            vmm_state: PropolisInstanceState(Observed::Stopping),
            migration_out: Some(ObservedMigrationState {
                state: MigrationState::Completed,
                id: migration_id,
            }),
            migration_in: None,
            time: Utc::now(),
        };

        // This transition should transfer control to the target VMM without
        // actually marking the migration as completed. This advances the
        // instance's state generation.
        let prev = state.clone();
        assert!(state.apply_propolis_observation(&observed).is_none());
        assert_state_change_has_gen_change(&prev, &state);

        // The migration state should transition to "completed"
        let migration = state
            .migration_out
            .clone()
            .expect("instance must have a migration state");
        let prev_migration =
            prev.migration_out.expect("previous state must have a migration");
        assert_eq!(migration.state, MigrationState::Completed);
        assert!(migration.gen > prev_migration.gen);
        let prev_migration = migration;

        // Once a successful migration is observed, the VMM's state should
        // continue to update, but the instance's state shouldn't change
        // anymore.
        let prev = state.clone();
        observed.vmm_state = PropolisInstanceState(Observed::Stopped);
        assert!(state.apply_propolis_observation(&observed).is_none());
        assert_state_change_has_gen_change(&prev, &state);

        // The Stopped state is translated internally to Stopping to prevent
        // external viewers from perceiving that the instance is stopped before
        // the VMM is fully retired.
        assert_eq!(state.vmm.state, VmmState::Stopping);
        assert!(state.vmm.gen > prev.vmm.gen);

        // Now that the migration has completed, it should not transition again.
        let migration = state
            .migration_out
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::Completed);
        assert_eq!(migration.gen, prev_migration.gen);
        let prev_migration = migration;

        let prev = state.clone();
        observed.vmm_state = PropolisInstanceState(Observed::Destroyed);
        assert!(matches!(
            state.apply_propolis_observation(&observed),
            Some(Action::Destroy)
        ));
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.state, VmmState::Destroyed);
        assert!(state.vmm.gen > prev.vmm.gen);

        let migration = state
            .migration_out
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::Completed);
        assert_eq!(migration.gen, prev_migration.gen);
    }

    #[test]
    fn failure_after_migration_in_does_not_transition() {
        let mut state = make_migration_target_instance();
        let migration_id = state.migration_in.as_ref().unwrap().migration_id;

        // Failure to migrate into an instance should mark the VMM as destroyed
        // but should not change the instance's migration IDs.
        let observed = ObservedPropolisState {
            vmm_state: PropolisInstanceState(Observed::Failed),
            migration_in: Some(ObservedMigrationState {
                state: MigrationState::Failed,
                id: migration_id,
            }),
            migration_out: None,
            time: Utc::now(),
        };

        let prev = state.clone();
        assert!(matches!(
            state.apply_propolis_observation(&observed),
            Some(Action::Destroy)
        ));
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.state, VmmState::Failed);
        assert!(state.vmm.gen > prev.vmm.gen);

        // The migration state should transition.
        let migration =
            state.migration_in.expect("instance must have a migration state");
        let prev_migration =
            prev.migration_in.expect("previous state must have a migration");
        assert_eq!(migration.state, MigrationState::Failed);
        assert!(migration.gen > prev_migration.gen);
    }

    // Verifies that the rude-termination state change doesn't update the
    // instance record if the VMM under consideration is a migration target.
    //
    // The live migration saga relies on this property for correctness (it needs
    // to know that unwinding its "create destination VMM" step will not produce
    // an updated instance record).
    #[test]
    fn rude_terminate_of_migration_target_does_not_transition_instance() {
        let mut state = make_migration_target_instance();

        let prev = state.clone();
        let mark_failed = false;
        state.terminate_rudely(mark_failed);

        assert_state_change_has_gen_change(&prev, &state);

        // The migration state should transition.
        let migration =
            state.migration_in.expect("instance must have a migration state");
        let prev_migration =
            prev.migration_in.expect("previous state must have a migration");
        assert_eq!(migration.state, MigrationState::Failed);
        assert!(migration.gen > prev_migration.gen);
    }

    #[test]
    #[ignore = "this logic is basically trivial now, maybe just get rid of the test?"]
    fn migration_out_after_migration_in() {
        todo!("eliza")
        // let mut state = make_migration_target_instance();
        // let mut observed = ObservedPropolisState {
        //     vmm_state: PropolisInstanceState(Observed::Running),
        //     migration_in: ObservedMigrationStatus::Succeeded,
        //     time: Utc::now(),
        // };

        // // The transition into the Running state on the migration target should
        // // take over for the source, updating the Propolis generation.
        // let prev = state.clone();
        // assert!(state.apply_propolis_observation(&observed).is_none());
        // assert_state_change_has_gen_change(&prev, &state);
        // assert_eq!(state.vmm.state, VmmState::Running);
        // assert!(state.vmm.gen > prev.vmm.gen);

        // // The migration state should transition to completed.
        // let migration = state
        //     .migration
        //     .clone()
        //     .expect("instance must have a migration state");
        // let prev_migration =
        //     prev.migration.expect("previous state must have a migration");
        // assert_eq!(migration.state, MigrationState::Completed);
        // assert!(migration.gen > prev_migration.gen);

        // // Pretend Nexus set some new migration IDs.
        // let migration_id = Uuid::new_v4();
        // let prev = state.clone();
        // state.set_migration_ids(
        //     &Some(InstanceMigrationSourceParams {
        //         migration_id,
        //         dst_propolis_id: PropolisUuid::new_v4(),
        //     }),
        //     Utc::now(),
        // );
        // assert_state_change_has_gen_change(&prev, &state);
        // assert_eq!(state.vmm.gen, prev.vmm.gen);

        // // There should be a new, pending migration state.
        // let migration = state
        //     .migration
        //     .clone()
        //     .expect("instance must have a migration state");
        // assert_eq!(migration.state, MigrationState::Pending);
        // assert_eq!(migration.migration_id, migration_id);
        // let prev_migration = migration;

        // // Mark that the new migration out is in progress. This doesn't change
        // // anything in the instance runtime state, but does update the VMM state
        // // generation.
        // let prev = state.clone();
        // observed.vmm_state = PropolisInstanceState(Observed::Migrating);
        // assert!(state.apply_propolis_observation(&observed).is_none());
        // assert_state_change_has_gen_change(&prev, &state);
        // assert_eq!(state.vmm.state, VmmState::Migrating);
        // assert!(state.vmm.gen > prev.vmm.gen);

        // // The migration state should transition to in progress.
        // let migration = state
        //     .migration
        //     .clone()
        //     .expect("instance must have a migration state");
        // assert_eq!(migration.state, MigrationState::InProgress);
        // assert!(migration.gen > prev_migration.gen);
        // let prev_migration = migration;

        // // Propolis will publish that the migration succeeds before changing any
        // // state. This should transfer control to the target but should not
        // // touch the migration ID (that is the new target's job).
        // let prev = state.clone();
        // observed.vmm_state = PropolisInstanceState(Observed::Migrating);
        // assert!(state.apply_propolis_observation(&observed).is_none());
        // assert_state_change_has_gen_change(&prev, &state);
        // assert_eq!(state.vmm.state, VmmState::Migrating);
        // assert!(state.vmm.gen > prev.vmm.gen);

        // // The migration state should transition to completed.
        // let migration = state
        //     .migration
        //     .clone()
        //     .expect("instance must have a migration state");
        // assert_eq!(migration.state, MigrationState::Completed);
        // assert!(migration.gen > prev_migration.gen);

        // // The rest of the destruction sequence is covered by other tests.
    }
}
