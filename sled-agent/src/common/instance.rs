// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use crate::params::InstanceMigrationSourceParams;
use chrono::{DateTime, Utc};
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::{
    MigrationRole, MigrationRuntimeState, MigrationState, SledInstanceState,
    VmmRuntimeState, VmmState,
};
use omicron_uuid_kinds::PropolisUuid;
use propolis_client::types::{
    InstanceState as PropolisApiState, InstanceStateMonitorResponse,
    MigrationState as PropolisMigrationState,
};

/// The instance and VMM state that sled agent maintains on a per-VMM basis.
#[derive(Clone, Debug)]
pub struct InstanceStates {
    vmm: VmmRuntimeState,
    propolis_id: PropolisUuid,
    migration: Option<MigrationRuntimeState>,
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

    /// Information about whether the state observer queried migration status at
    /// all and, if so, what response it got from Propolis.
    pub migration_status: ObservedMigrationStatus,

    /// The approximate time at which this observation was made.
    pub time: DateTime<Utc>,
}

impl ObservedPropolisState {
    /// Constructs a Propolis state observation from an instance's current
    /// state and an instance state monitor response received from
    /// Propolis.
    pub fn new(
        state: &InstanceStates,
        propolis_state: &InstanceStateMonitorResponse,
    ) -> Self {
        // If there's no migration currently registered with this sled, report
        // the current state and that no migration is currently in progress,
        // even if Propolis has some migration data to share. (This case arises
        // when Propolis returns state from a previous migration that sled agent
        // has already retired.)
        //
        // N.B. This needs to be read from the instance runtime state and not
        //      the migration runtime state to ensure that, once a migration in
        //      completes, the "completed" observation is reported to
        //      `InstanceStates::apply_propolis_observation` exactly once.
        //      Otherwise that routine will try to apply the "inbound migration
        //      complete" instance state transition twice.
        let Some(migration_id) = instance_runtime.migration_id else {
            return Self {
                vmm_state: PropolisInstanceState(propolis_state.state),
                migration_status: ObservedMigrationStatus::NoMigration,
                time: Utc::now(),
            };
        };

        // Sled agent believes a live migration may be in progress. See if
        // either of the Propolis migrations corresponds to it.
        let propolis_migration = match (
            &propolis_state.migration.migration_in,
            &propolis_state.migration.migration_out,
        ) {
            (Some(inbound), _) if inbound.id == migration_id => inbound,
            (_, Some(outbound)) if outbound.id == migration_id => outbound,
            _ => {
                // Sled agent believes this instance should be migrating, but
                // Propolis isn't reporting a matching migration yet, so assume
                // the migration is still pending.
                return Self {
                    vmm_state: PropolisInstanceState(propolis_state.state),
                    migration_status: ObservedMigrationStatus::Pending,
                    time: Utc::now(),
                };
            }
        };

        Self {
            vmm_state: PropolisInstanceState(propolis_state.state),
            migration_status: match propolis_migration.state {
                PropolisMigrationState::Finish => {
                    ObservedMigrationStatus::Succeeded
                }
                PropolisMigrationState::Error => {
                    ObservedMigrationStatus::Failed
                }
                _ => ObservedMigrationStatus::InProgress,
            },
            time: Utc::now(),
        }
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

/// The possible roles a VMM can have vis-a-vis an instance.
#[derive(Clone, Copy, Debug, PartialEq)]
enum PropolisRole {
    /// The VMM is its instance's current active VMM.
    Active,

    /// The VMM is its instance's migration target VMM.
    MigrationTarget,

    /// The instance does not refer to this VMM (but it may have done so in the
    /// past).
    Retired,
}

/// Action to be taken on behalf of state transition.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    /// Terminate the VM and associated service.
    Destroy,
}

impl InstanceStates {
    pub fn new(vmm: VmmRuntimeState, propolis_id: Uuid) -> Self {
        InstanceStates { vmm, propolis_id, migration: None }
    }

    pub fn vmm(&self) -> &VmmRuntimeState {
        &self.vmm
    }

    pub fn propolis_id(&self) -> PropolisUuid {
        self.propolis_id
    }

    pub(crate) fn migration(&self) -> Option<&MigrationRuntimeState> {
        self.migration.as_ref()
    }

    /// Creates a `SledInstanceState` structure containing the entirety of this
    /// structure's runtime state. This requires cloning; for simple read access
    /// use the `instance` or `vmm` accessors instead.
    pub fn sled_instance_state(&self) -> SledInstanceState {
        SledInstanceState {
            vmm_state: self.vmm.clone(),
            propolis_id: self.propolis_id,
            migration_state: self.migration.clone(),
        }
    }

    fn transition_migration(
        &mut self,
        state: MigrationState,
        time_updated: DateTime<Utc>,
    ) {
        let migration = self.migration.as_mut().expect(
            "an ObservedMigrationState should only be constructed when the \
            VMM has an active migration",
        );
        // Don't generate spurious state updates if the migration is already in
        // the state we're transitioning to.
        if migration.state != state {
            migration.state = state;
            migration.time_updated = time_updated;
            migration.gen = migration.gen.next();
        }
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub(crate) fn apply_propolis_observation(
        &mut self,
        observed: &ObservedPropolisState,
    ) -> Option<Action> {
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
        match observed.migration_status {
            ObservedMigrationStatus::Succeeded => {
                self.transition_migration(
                    MigrationState::Completed,
                    observed.time,
                );
            }
            ObservedMigrationStatus::Failed => {
                self.transition_migration(
                    MigrationState::Failed,
                    observed.time,
                );
            }
            ObservedMigrationStatus::InProgress => {
                self.transition_migration(
                    MigrationState::InProgress,
                    observed.time,
                );
            }
            ObservedMigrationStatus::NoMigration
            | ObservedMigrationStatus::Pending => {}
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
            if let Some(MigrationState::Pending | MigrationState::InProgress) =
                self.migration.as_ref().map(|m| m.state)
            {
                self.transition_migration(
                    MigrationState::Failed,
                    observed.time,
                );
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
            migration_status: if self.migration.is_some() {
                ObservedMigrationStatus::Failed
            } else {
                ObservedMigrationStatus::NoMigration
            },
            time: Utc::now(),
        };

        self.apply_propolis_observation(&fake_observed);
    }

    /// Sets or clears this instance's migration IDs and advances its Propolis
    /// generation number.
    pub(crate) fn set_migration_ids(
        &mut self,
        ids: &Option<InstanceMigrationSourceParams>,
        now: DateTime<Utc>,
    ) {
        if let Some(InstanceMigrationSourceParams {
            migration_id,
            dst_propolis_id,
        }) = *ids
        {
            let role = if dst_propolis_id == self.propolis_id {
                MigrationRole::Target
            } else {
                MigrationRole::Source
            };
            self.migration = Some(MigrationRuntimeState {
                migration_id,
                state: MigrationState::Pending,
                role,
                gen: Generation::new(),
                time_updated: now,
            })
        } else {
            self.migration = None;
        }
    }

    /// Returns true if the migration IDs in this instance are already set as they
    /// would be on a successful transition from the migration IDs in
    /// `old_runtime` to the ones in `migration_ids`.
    pub(crate) fn migration_ids_already_set(
        &self,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> bool {
        match (self.migration, migration_ids) {
            // If the migration ID is already set, and this is a request to set
            // IDs, the records match if the relevant IDs match.
            (Some(migration), Some(ids)) => {
                migration.migration_id == ids.migration_id
            }
            // If the migration ID is already cleared, and this is a request to
            // clear IDs, the records match.
            (None, None) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use chrono::Utc;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use propolis_client::types::InstanceState as Observed;
    use uuid::Uuid;

    fn make_instance() -> InstanceStates {
        let propolis_id = PropolisUuid::new_v4();
        let now = Utc::now();
        let instance = InstanceRuntimeState {
            propolis_id: Some(propolis_id),
            dst_propolis_id: None,
            migration_id: None,
            gen: Generation::new(),
            time_updated: now,
        };

        let vmm = VmmRuntimeState {
            state: VmmState::Starting,
            gen: Generation::new(),
            time_updated: now,
        };

        InstanceStates::new(vmm, propolis_id)
    }

    fn make_migration_source_instance() -> InstanceStates {
        let mut state = make_instance();
        state.vmm.state = VmmState::Migrating;
        let migration_id = Uuid::new_v4();
        state.migration = Some(MigrationRuntimeState {
            migration_id,
            state: MigrationState::InProgress,
            role: MigrationRole::Source,
            // advance the generation once, since we are starting out in the
            // `InProgress` state.
            gen: Generation::new().next(),
            time_updated: Utc::now(),
        });

        state
    }

    fn make_migration_target_instance() -> InstanceStates {
        let mut state = make_instance();
        state.vmm.state = VmmState::Migrating;
        let migration_id = Uuid::new_v4();
        state.propolis_id = Uuid::new_v4();
        state.migration = Some(MigrationRuntimeState {
            migration_id,
            state: MigrationState::InProgress,
            role: MigrationRole::Target,
            // advance the generation once, since we are starting out in the
            // `InProgress` state.
            gen: Generation::new().next(),
            time_updated: Utc::now(),
        });
        state
    }

    fn make_observed_state(
        propolis_state: PropolisInstanceState,
    ) -> ObservedPropolisState {
        ObservedPropolisState {
            vmm_state: propolis_state,
            migration_status: ObservedMigrationStatus::NoMigration,
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
            let original_instance_state = instance_state.clone();
            let requested_action = instance_state
                .apply_propolis_observation(&make_observed_state(state.into()));

            assert!(matches!(requested_action, Some(Action::Destroy)));
        }
    }

    fn test_termination_fails_in_progress_migration(
        mk_instance: impl Fn() -> InstanceStates,
    ) {
        for state in [Observed::Destroyed, Observed::Failed] {
            let mut instance_state = mk_instance();
            let original_migration = instance_state.clone().migration.unwrap();
            let requested_action = instance_state
                .apply_propolis_observation(&make_observed_state(state.into()));

            let migration =
                instance_state.migration.expect("state must have a migration");
            assert_eq!(migration.state, MigrationState::Failed);
            assert!(migration.gen > original_migration.gen);
            assert!(matches!(requested_action, Some(Action::Destroy)));
        }
    }

    #[test]
    fn source_termination_fails_in_progress_migration() {
        test_termination_fails_in_progress_migration(
            make_migration_source_instance,
        )
    }

    #[test]
    fn target_termination_fails_in_progress_migration() {
        test_termination_fails_in_progress_migration(
            make_migration_target_instance,
        )
    }

    #[test]
    fn destruction_after_migration_out_does_not_transition() {
        let mut state = make_migration_source_instance();

        // After a migration succeeds, the source VM appears to stop but reports
        // that the migration has succeeded.
        let mut observed = ObservedPropolisState {
            vmm_state: PropolisInstanceState(Observed::Stopping),
            migration_status: ObservedMigrationStatus::Succeeded,
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
            .migration
            .clone()
            .expect("instance must have a migration state");
        let prev_migration =
            prev.migration.expect("previous state must have a migration");
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
            .migration
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
            .migration
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::Completed);
        assert_eq!(migration.gen, prev_migration.gen);
    }

    #[test]
    fn failure_after_migration_in_does_not_transition() {
        let mut state = make_migration_target_instance();

        // Failure to migrate into an instance should mark the VMM as destroyed
        // but should not change the instance's migration IDs.
        let observed = ObservedPropolisState {
            vmm_state: PropolisInstanceState(Observed::Failed),
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
            state.migration.expect("instance must have a migration state");
        let prev_migration =
            prev.migration.expect("previous state must have a migration");
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
            state.migration.expect("instance must have a migration state");
        let prev_migration =
            prev.migration.expect("previous state must have a migration");
        assert_eq!(migration.state, MigrationState::Failed);
        assert!(migration.gen > prev_migration.gen);
    }

    #[test]
    fn migration_out_after_migration_in() {
        let mut state = make_migration_target_instance();
        let mut observed = ObservedPropolisState {
            vmm_state: PropolisInstanceState(Observed::Running),
            migration_status: ObservedMigrationStatus::Succeeded,
            time: Utc::now(),
        };

        // The transition into the Running state on the migration target should
        // take over for the source, updating the Propolis generation.
        let prev = state.clone();
        assert!(state.apply_propolis_observation(&observed).is_none());
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.state, VmmState::Running);
        assert!(state.vmm.gen > prev.vmm.gen);

        // The migration state should transition to completed.
        let migration = state
            .migration
            .clone()
            .expect("instance must have a migration state");
        let prev_migration =
            prev.migration.expect("previous state must have a migration");
        assert_eq!(migration.state, MigrationState::Completed);
        assert!(migration.gen > prev_migration.gen);

        // Pretend Nexus set some new migration IDs.
        let migration_id = Uuid::new_v4();
        let prev = state.clone();
        state.set_migration_ids(
            &Some(InstanceMigrationSourceParams {
                migration_id,
                dst_propolis_id: PropolisUuid::new_v4(),
            }),
            Utc::now(),
        );
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.gen, prev.vmm.gen);

        // There should be a new, pending migration state.
        let migration = state
            .migration
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::Pending);
        assert_eq!(migration.migration_id, migration_id);
        let prev_migration = migration;

        // Mark that the new migration out is in progress. This doesn't change
        // anything in the instance runtime state, but does update the VMM state
        // generation.
        let prev = state.clone();
        observed.vmm_state = PropolisInstanceState(Observed::Migrating);
        assert!(state.apply_propolis_observation(&observed).is_none());
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.state, VmmState::Migrating);
        assert!(state.vmm.gen > prev.vmm.gen);

        // The migration state should transition to in progress.
        let migration = state
            .migration
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::InProgress);
        assert!(migration.gen > prev_migration.gen);
        let prev_migration = migration;

        // Propolis will publish that the migration succeeds before changing any
        // state. This should transfer control to the target but should not
        // touch the migration ID (that is the new target's job).
        let prev = state.clone();
        observed.vmm_state = PropolisInstanceState(Observed::Migrating);
        assert!(state.apply_propolis_observation(&observed).is_none());
        assert_state_change_has_gen_change(&prev, &state);
        assert_eq!(state.vmm.state, VmmState::Migrating);
        assert!(state.vmm.gen > prev.vmm.gen);

        // The migration state should transition to completed.
        let migration = state
            .migration
            .clone()
            .expect("instance must have a migration state");
        assert_eq!(migration.state, MigrationState::Completed);
        assert!(migration.gen > prev_migration.gen);

        // The rest of the destruction sequence is covered by other tests.
    }
}
