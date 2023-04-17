// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use crate::params::InstanceMigrationSourceParams;
use chrono::Utc;
use omicron_common::api::external::InstanceState as ApiInstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceState as PropolisInstanceState;

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
    pub fn observe_transition(
        &mut self,
        observed: &PropolisInstanceState,
    ) -> Option<Action> {
        let current = InstanceState::from(*observed).0;
        self.transition(current);

        // Most commands to update Propolis are triggered via requests (from
        // Nexus), but if the instance reports that it has been destroyed,
        // we should clean it up.
        if matches!(observed, PropolisInstanceState::Destroyed) {
            Some(Action::Destroy)
        } else {
            None
        }
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

    /// Returns true if the migration IDs in this instance are already set as they
    /// would be on a successful transition from the migration IDs in
    /// `old_runtime` to the ones in `migration_ids`.
    pub(crate) fn migration_ids_already_set(
        &self,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> bool {
        // For the old and new records to match, the new record's Propolis
        // generation must succeed the old record's.
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
}

#[cfg(test)]
mod test {
    use super::{Action, InstanceStates};

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

    #[test]
    fn propolis_destroy_requests_destroy_action() {
        let mut instance = make_instance();
        let original_instance = instance.clone();
        let requested_action =
            instance.observe_transition(&Observed::Destroyed);

        assert!(matches!(requested_action, Some(Action::Destroy)));
        assert!(instance.current.gen > original_instance.current.gen);
        assert_eq!(instance.current.run_state, State::Stopped);
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
