// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

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
}

#[cfg(test)]
mod test {
    use super::{Action, InstanceStates};

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
}
