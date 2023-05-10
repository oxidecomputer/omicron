// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use crate::params::InstanceMigrationSourceParams;
use chrono::Utc;
use omicron_common::api::external::InstanceState as ApiInstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::{
    InstanceState as PropolisInstanceState, InstanceStateMonitorResponse,
};

/// Describes the status of the migration identified in an instance's runtime
/// state as it relates to any migration status information reported by the
/// instance's Propolis.
#[derive(Clone, Copy, Debug)]
pub enum ObservedMigrationStatus {
    /// The instance's runtime state and Propolis agree that no migration is in
    /// progress.
    NoMigration,

    /// Propolis thinks a migration is in progress, but its migration ID does
    /// not agree with the instance's current runtime state: either the current
    /// runtime state has no ID, or Propolis has an older ID than sled agent
    /// does because a newer migration has begun (see below).
    ///
    /// This is expected in the following scenarios:
    ///
    /// - Propolis was initialized via migration in, after which Nexus cleared
    ///   the instance's migration IDs.
    /// - Propolis was initialized via migration in, and the instance is about
    ///   to migrate again. Propolis will have the old ID (from the migration
    ///   in) while the instance runtime state has the new ID (from the pending
    ///   migration out).
    MismatchedId,

    /// Either:
    ///
    /// - The instance's runtime state contains a migration ID, but Propolis did
    ///   not report any migration was in progress, or
    /// - Propolis reported that the active migration is not done yet.
    ///
    /// The first case occurs when the current instance is queued to be a
    /// migration source, but its Propolis changed state before any migration
    /// request reached that Propolis.
    InProgress,

    /// Propolis reported that the migration completed successfully.
    Succeeded,

    /// Propolis reported that the migration failed.
    Failed,
}

/// The information observed by the instance's Propolis state monitor.
#[derive(Clone, Copy, Debug)]
pub struct ObservedPropolisState {
    /// The state reported by Propolis's instance state monitor API.
    ///
    /// Note that this API allows transitions to be missed (if multiple
    /// transitions occur between calls to the monitor, only the most recent
    /// state is reported).
    pub instance_state: PropolisInstanceState,

    /// Information about whether the state observer queried migration status at
    /// all and, if so, what response it got from Propolis.
    pub migration_status: ObservedMigrationStatus,
}

impl ObservedPropolisState {
    /// Constructs a Propolis state observation from an instance's current
    /// runtime state and an instance state monitor response received from
    /// Propolis.
    pub fn new(
        runtime_state: &InstanceRuntimeState,
        propolis_state: &InstanceStateMonitorResponse,
    ) -> Self {
        let migration_status =
            match (runtime_state.migration_id, &propolis_state.migration) {
                // If the runtime state and Propolis state agree that there's
                // a migration in progress, and they agree on its ID, the
                // Propolis migration state determines the migration status.
                (Some(this_id), Some(propolis_migration))
                    if this_id == propolis_migration.migration_id =>
                {
                    use propolis_client::api::MigrationState;
                    match propolis_migration.state {
                        MigrationState::Finish => {
                            ObservedMigrationStatus::Succeeded
                        }
                        MigrationState::Error => {
                            ObservedMigrationStatus::Failed
                        }
                        _ => ObservedMigrationStatus::InProgress,
                    }
                }

                // If the migration IDs don't match, or Propolis thinks a
                // migration is in progress but the instance's runtime state
                // does not, report the mismatch.
                (_, Some(_)) => ObservedMigrationStatus::MismatchedId,

                // A migration source's migration IDs get set before its
                // Propolis actually gets asked to migrate, so it's possible for
                // the runtime state to contain an ID while the Propolis has
                // none, in which case the migration is pending.
                (Some(_), None) => ObservedMigrationStatus::InProgress,

                // If neither side has a migration ID, then there's clearly no
                // migration.
                (None, None) => ObservedMigrationStatus::NoMigration,
            };
        Self { instance_state: propolis_state.state, migration_status }
    }
}

/// Newtype to facilitate conversions from Propolis states to external API
/// states.
pub struct InstanceState(pub ApiInstanceState);

impl From<PropolisInstanceState> for InstanceState {
    fn from(value: PropolisInstanceState) -> Self {
        InstanceState(match value {
            // From an external perspective, the instance has already been
            // created. Creating the propolis instance is an internal detail and
            // happens every time we start the instance, so we map it to
            // "Starting" here.
            PropolisInstanceState::Creating
            | PropolisInstanceState::Starting => ApiInstanceState::Starting,
            PropolisInstanceState::Running => ApiInstanceState::Running,
            PropolisInstanceState::Stopping => ApiInstanceState::Stopping,
            PropolisInstanceState::Stopped => ApiInstanceState::Stopped,
            PropolisInstanceState::Rebooting => ApiInstanceState::Rebooting,
            PropolisInstanceState::Migrating => ApiInstanceState::Migrating,
            PropolisInstanceState::Repairing => ApiInstanceState::Repairing,
            PropolisInstanceState::Failed => ApiInstanceState::Failed,
            // NOTE: This is a bit of an odd one - we intentionally do *not*
            // translate the "destroyed" propolis state to the destroyed instance
            // API state.
            //
            // When a propolis instance reports that it has been destroyed,
            // this does not necessarily mean the customer-visible instance
            // should be torn down. Instead, it implies that the Propolis service
            // should be stopped, but the VM could be allocated to a different
            // machine.
            PropolisInstanceState::Destroyed => ApiInstanceState::Stopped,
        })
    }
}

/// Action to be taken on behalf of state transition.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Update the VM state to cause it to run.
    Run,
    /// Update the VM state to cause it to stop.
    Stop,
    /// Invoke a reboot of the VM.
    Reboot,
    /// Terminate the VM and associated service.
    Destroy,
}

/// A wrapper around an instance's current state, represented as a Nexus
/// `InstanceRuntimeState`. The externally-visible instance state in this
/// structure is mostly changed when the instance's Propolis state changes.
#[derive(Clone, Debug)]
pub struct InstanceStates {
    current: InstanceRuntimeState,
}

impl InstanceStates {
    pub fn new(current: InstanceRuntimeState) -> Self {
        InstanceStates { current }
    }

    /// Returns the current instance state.
    pub fn current(&self) -> &InstanceRuntimeState {
        &self.current
    }

    /// Returns the current instance state.
    pub fn current_mut(&mut self) -> &mut InstanceRuntimeState {
        &mut self.current
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub fn apply_propolis_observation(
        &mut self,
        observed: &ObservedPropolisState,
    ) -> Option<Action> {
        // The state after this transition will be published to Nexus, so some
        // care is required around migration to ensure that Nexus's instance
        // state remains consistent even in the face of racing updates from a
        // migration source and a migration target. The possibilities are as
        // follows:
        //
        // 1. The current migration succeeded.
        //   1a. Migration source: ideally this case would pass control to the
        //       target explicitly, but there currently isn't enough information
        //       to do that (the source doesn't know the target's sled ID or
        //       Propolis IP), so just let the target deal with updating
        //       everything.
        //
        //       This is the one case in which this routine explicitly *should
        //       not* transition the current state (to avoid having a "stopped"
        //       state reach Nexus before the target takes control of the state
        //       machine).
        //   1b. Migration target: Signal that migration is done by bumping the
        //       Propolis generation number and clearing the migration ID and
        //       destination Propolis ID from the instance record.
        // 2. The current migration failed.
        //   2a. Migration source: The source is running now. Clear the
        //       migration IDs, bump the Propolis generation number, and publish
        //       the updated state, ending the migration.
        //   2b. Migration target: The target has failed and control of the
        //       instance remains with the source. Don't update the Propolis
        //       generation number. Updating state is OK here because migration
        //       targets can't update Nexus instance states without changing the
        //       Propolis generation.
        // 3. No migration is ongoing, or the migration ID in the instance
        //    record doesn't line up with the putatively ongoing migration. Just
        //    update state normally in this case; whichever sled has the current
        //    Propolis generation will have its update applied.
        //
        // There is an additional exceptional case here: when an instance stops,
        // its migration IDs should be cleared so that it can migrate when it is
        // started again. If the VM is in a terminal state, and this is *not*
        // case 1a above (i.e. the Propolis is stopping because the instance
        // migrated out), clear any leftover migration IDs.
        //
        // TODO(#2315): Terminal-state cleanup should also clear an instance's
        // sled assignment and Propolis ID, but that requires Nexus work to
        // repopulate these when the instance starts again.
        let action = if matches!(
            observed.instance_state,
            PropolisInstanceState::Destroyed | PropolisInstanceState::Failed
        ) {
            Some(Action::Destroy)
        } else {
            None
        };

        let next_state = InstanceState::from(observed.instance_state).0;
        match observed.migration_status {
            // Case 3: Update normally if there is no migration in progress or
            // the current migration is unrecognized or in flight.
            ObservedMigrationStatus::NoMigration
            | ObservedMigrationStatus::MismatchedId
            | ObservedMigrationStatus::InProgress => {
                self.transition(next_state);
            }

            // Case 1: Migration succeeded. Only update the instance record if
            // this is a migration target.
            //
            // Calling `is_migration_target` is safe here because the instance
            // must have had a migration ID in its record to have inferred that
            // an ongoing migration succeeded.
            ObservedMigrationStatus::Succeeded => {
                if self.is_migration_target() {
                    self.transition(next_state);
                    self.clear_migration_ids();
                } else {
                    // Case 1a: Short-circuit without touching the instance
                    // record.
                    return action;
                }
            }

            // Case 2: Migration failed. Only update the instance record if this
            // is a migration source. (Updating the target record is allowed,
            // but still has to short-circuit so that the call to
            // `clear_migration_ids` below is not reached.)
            ObservedMigrationStatus::Failed => {
                if self.is_migration_target() {
                    return action;
                } else {
                    self.transition(next_state);
                    self.clear_migration_ids();
                }
            }
        }

        if matches!(action, Some(Action::Destroy)) {
            self.clear_migration_ids();
        }

        action
    }

    // Transitions to a new InstanceState value, updating the timestamp and
    // generation number.
    pub(crate) fn transition(&mut self, next: ApiInstanceState) {
        self.current.run_state = next;
        self.current.gen = self.current.gen.next();
        self.current.time_updated = Utc::now();
    }

    /// Sets or clears this instance's migration IDs and advances its Propolis
    /// generation number.
    pub(crate) fn set_migration_ids(
        &mut self,
        ids: &Option<InstanceMigrationSourceParams>,
    ) {
        if let Some(ids) = ids {
            self.current.migration_id = Some(ids.migration_id);
            self.current.dst_propolis_id = Some(ids.dst_propolis_id);
        } else {
            self.current.migration_id = None;
            self.current.dst_propolis_id = None;
        }

        self.current.propolis_gen = self.current.propolis_gen.next();
    }

    /// Unconditionally clears the instance's migration IDs and advances its
    /// Propolis generation. Not public; used internally to conclude migrations.
    fn clear_migration_ids(&mut self) {
        self.current.migration_id = None;
        self.current.dst_propolis_id = None;
        self.current.propolis_gen = self.current.propolis_gen.next();
    }

    /// Returns true if the migration IDs in this instance are already set as they
    /// would be on a successful transition from the migration IDs in
    /// `old_runtime` to the ones in `migration_ids`.
    pub(crate) fn migration_ids_already_set(
        &self,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> bool {
        // For the old and new records to match, the new record's Propolis
        // generation must immediately succeed the old record's.
        //
        // This is an equality check to try to avoid the following A-B-A
        // problem:
        //
        // 1. Instance starts on sled 1.
        // 2. Parallel sagas start, one to migrate the instance to sled 2
        //    and one to migrate the instance to sled 3.
        // 3. The "migrate to sled 2" saga completes.
        // 4. A new migration starts that migrates the instance back to sled 1.
        // 5. The "migrate to sled 3" saga attempts to set its migration
        //    ID.
        //
        // A simple less-than check allows the migration to sled 3 to proceed
        // even though the most-recently-expressed intent to migrate put the
        // instance on sled 1.
        if old_runtime.propolis_gen.next() != self.current.propolis_gen {
            return false;
        }

        match (self.current.migration_id, migration_ids) {
            // If the migration ID is already set, and this is a request to set
            // IDs, the records match if the relevant IDs match.
            (Some(current_migration_id), Some(ids)) => {
                let current_dst_id = self.current.dst_propolis_id.expect(
                    "migration ID and destination ID must be set together",
                );

                current_migration_id == ids.migration_id
                    && current_dst_id == ids.dst_propolis_id
            }
            // If the migration ID is already cleared, and this is a request to
            // clear IDs, the records match.
            (None, None) => {
                assert!(self.current.dst_propolis_id.is_none());
                true
            }
            _ => false,
        }
    }

    /// Indicates whether this instance incarnation is a migration source or
    /// target by comparing the instance's current active Propolis ID with its
    /// migration destination ID.
    ///
    /// # Panics
    ///
    /// Panics if the instance has no destination Propolis ID set.
    fn is_migration_target(&self) -> bool {
        self.current.propolis_id == self.current.dst_propolis_id.unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::params::InstanceMigrationSourceParams;

    use chrono::Utc;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState as State,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use propolis_client::api::InstanceState as Observed;
    use uuid::Uuid;

    fn make_instance() -> InstanceStates {
        InstanceStates::new(InstanceRuntimeState {
            run_state: State::Creating,
            sled_id: Uuid::new_v4(),
            propolis_id: Uuid::new_v4(),
            dst_propolis_id: None,
            propolis_addr: None,
            migration_id: None,
            propolis_gen: Generation::new(),
            ncpus: InstanceCpuCount(2),
            memory: ByteCount::from_mebibytes_u32(512),
            hostname: "myvm".to_string(),
            gen: Generation::new(),
            time_updated: Utc::now(),
        })
    }

    fn make_migration_source_instance() -> InstanceStates {
        let mut state = make_instance();
        state.current.run_state = State::Migrating;
        state.current.migration_id = Some(Uuid::new_v4());
        state.current.dst_propolis_id = Some(Uuid::new_v4());
        state
    }

    fn make_migration_target_instance() -> InstanceStates {
        let mut state = make_instance();
        state.current.run_state = State::Migrating;
        state.current.migration_id = Some(Uuid::new_v4());
        state.current.dst_propolis_id = Some(state.current.propolis_id);
        state
    }

    fn make_observed_state(
        propolis_state: PropolisInstanceState,
    ) -> ObservedPropolisState {
        ObservedPropolisState {
            instance_state: propolis_state,
            migration_status: ObservedMigrationStatus::NoMigration,
        }
    }

    #[test]
    fn propolis_terminal_states_request_destroy_action() {
        for state in [Observed::Destroyed, Observed::Failed] {
            let mut instance = make_instance();
            let original_instance = instance.clone();
            let requested_action = instance
                .apply_propolis_observation(&make_observed_state(state));

            assert!(matches!(requested_action, Some(Action::Destroy)));
            assert!(instance.current.gen > original_instance.current.gen);
        }
    }

    #[test]
    fn destruction_after_migration_out_does_not_transition() {
        let mut instance = make_migration_source_instance();
        let mut observed = ObservedPropolisState {
            instance_state: Observed::Stopping,
            migration_status: ObservedMigrationStatus::Succeeded,
        };

        let original = instance.clone();
        assert!(instance.apply_propolis_observation(&observed).is_none());
        assert_eq!(instance.current.gen, original.current.gen);

        observed.instance_state = Observed::Stopped;
        assert!(instance.apply_propolis_observation(&observed).is_none());
        assert_eq!(instance.current.gen, original.current.gen);

        observed.instance_state = Observed::Destroyed;
        assert!(matches!(
            instance.apply_propolis_observation(&observed),
            Some(Action::Destroy)
        ));
        assert_eq!(instance.current.gen, original.current.gen);
    }

    #[test]
    fn failure_after_migration_in_does_not_transition() {
        let mut instance = make_migration_target_instance();
        let observed = ObservedPropolisState {
            instance_state: Observed::Failed,
            migration_status: ObservedMigrationStatus::Failed,
        };

        let original = instance.clone();
        assert!(matches!(
            instance.apply_propolis_observation(&observed),
            Some(Action::Destroy)
        ));
        assert_eq!(instance.current.gen, original.current.gen);
    }

    #[test]
    fn migration_out_after_migration_in() {
        let mut instance = make_migration_target_instance();
        let mut observed = ObservedPropolisState {
            instance_state: Observed::Running,
            migration_status: ObservedMigrationStatus::Succeeded,
        };

        // The transition into the Running state on the migration target should
        // take over for the source, updating the Propolis generation.
        let prev = instance.clone();
        assert!(instance.apply_propolis_observation(&observed).is_none());
        assert!(instance.current.migration_id.is_none());
        assert!(instance.current.dst_propolis_id.is_none());
        assert!(instance.current.gen > prev.current.gen);
        assert!(instance.current.propolis_gen > prev.current.propolis_gen);

        // Pretend Nexus set some new migration IDs.
        let prev = instance.clone();
        instance.set_migration_ids(&Some(InstanceMigrationSourceParams {
            migration_id: Uuid::new_v4(),
            dst_propolis_id: Uuid::new_v4(),
        }));
        assert!(instance.current.propolis_gen > prev.current.propolis_gen);

        // Mark that the new migration out is in progress.
        let prev = instance.clone();
        observed.instance_state = Observed::Migrating;
        observed.migration_status = ObservedMigrationStatus::InProgress;
        assert!(instance.apply_propolis_observation(&observed).is_none());
        assert_eq!(
            instance.current.migration_id.unwrap(),
            prev.current.migration_id.unwrap()
        );
        assert_eq!(
            instance.current.dst_propolis_id.unwrap(),
            prev.current.dst_propolis_id.unwrap()
        );
        assert_eq!(instance.current.run_state, State::Migrating);
        assert!(instance.current.gen > prev.current.gen);
        assert_eq!(instance.current.propolis_gen, prev.current.propolis_gen);

        // Propolis will publish that the migration succeeds before changing any
        // state. Because this is now a successful migration source, the
        // instance record is not updated.
        observed.instance_state = Observed::Migrating;
        observed.migration_status = ObservedMigrationStatus::Succeeded;
        let prev = instance.clone();
        assert!(instance.apply_propolis_observation(&observed).is_none());
        assert_eq!(instance.current.run_state, State::Migrating);
        assert_eq!(
            instance.current.migration_id.unwrap(),
            prev.current.migration_id.unwrap()
        );
        assert_eq!(
            instance.current.dst_propolis_id.unwrap(),
            prev.current.dst_propolis_id.unwrap()
        );
        assert_eq!(instance.current.gen, prev.current.gen);
        assert_eq!(instance.current.propolis_gen, prev.current.propolis_gen);

        // The rest of the destruction sequence is covered by other tests.
    }

    #[test]
    fn test_migration_ids_already_set() {
        let orig_instance = make_instance();
        let mut old_instance = orig_instance.clone();
        let mut new_instance = old_instance.clone();

        // Advancing the old instance's migration IDs and then asking if the
        // new IDs are present should indicate that they are indeed present.
        let migration_ids = InstanceMigrationSourceParams {
            migration_id: Uuid::new_v4(),
            dst_propolis_id: Uuid::new_v4(),
        };

        new_instance.set_migration_ids(&Some(migration_ids));
        assert!(new_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));

        // The IDs aren't already set if the new record has an ID that's
        // advanced from the old record by more than one generation.
        let mut newer_instance = new_instance.clone();
        newer_instance.current.propolis_gen =
            newer_instance.current.propolis_gen.next();
        assert!(!newer_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));

        // They also aren't set if the old generation has somehow equaled or
        // surpassed the current generation.
        old_instance.current.propolis_gen =
            old_instance.current.propolis_gen.next();
        assert!(!new_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));

        // If the generation numbers are right, but either requested ID is not
        // present in the current instance, the requested IDs aren't set.
        old_instance = orig_instance;
        new_instance.current.migration_id = Some(Uuid::new_v4());
        assert!(!new_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));

        new_instance.current.migration_id = Some(migration_ids.migration_id);
        new_instance.current.dst_propolis_id = Some(Uuid::new_v4());
        assert!(!new_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));

        new_instance.current.migration_id = None;
        new_instance.current.dst_propolis_id = None;
        assert!(!new_instance.migration_ids_already_set(
            old_instance.current(),
            &Some(migration_ids)
        ));
    }
}
