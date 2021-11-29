// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use chrono::Utc;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use omicron_common::api::internal::sled_agent::InstanceStateRequested;
use propolis_client::api::InstanceState as PropolisInstanceState;

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

fn get_next_desired_state(
    observed: &PropolisInstanceState,
    requested: InstanceStateRequested,
) -> Option<InstanceStateRequested> {
    use InstanceStateRequested as Requested;
    use PropolisInstanceState as Observed;

    match (requested, observed) {
        (Requested::Running, Observed::Running) => None,
        (Requested::Stopped, Observed::Stopped) => None,
        (Requested::Destroyed, Observed::Destroyed) => None,

        // Reboot requests.
        // - "Stopping" is an expected state on the way to rebooting.
        // - "Starting" expects "Running" to occur next.
        // - Other observed states stop the expected transitions.
        (Requested::Reboot, Observed::Stopping) => Some(Requested::Reboot),
        (Requested::Reboot, Observed::Starting) => Some(Requested::Running),
        (Requested::Reboot, _) => None,

        (_, _) => Some(requested),
    }
}

/// The instance state is a combination of the last-known state, as well as an
/// "objective" state which the sled agent will work towards achieving.
#[derive(Clone, Debug)]
pub struct InstanceStates {
    // Last known state reported from Propolis.
    current: InstanceRuntimeState,

    // Desired state, which we will attempt to poke the VM towards.
    desired: Option<InstanceRuntimeStateRequested>,
}

impl InstanceStates {
    pub fn new(current: InstanceRuntimeState) -> Self {
        InstanceStates { current, desired: None }
    }

    /// Returns the current instance state.
    pub fn current(&self) -> &InstanceRuntimeState {
        &self.current
    }

    /// Returns the current instance state.
    pub fn current_mut(&mut self) -> &mut InstanceRuntimeState {
        &mut self.current
    }

    /// Returns the desired instance state, if any exists.
    pub fn desired(&self) -> &Option<InstanceRuntimeStateRequested> {
        &self.desired
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub fn observe_transition(
        &mut self,
        observed: &PropolisInstanceState,
    ) -> Option<Action> {
        use InstanceState as State;
        use PropolisInstanceState as Observed;

        let current = match observed {
            Observed::Creating => State::Creating,
            Observed::Starting => State::Starting,
            Observed::Running => State::Running,
            Observed::Stopping => State::Stopping,
            Observed::Stopped => State::Stopped,
            Observed::Rebooting => State::Rebooting,
            Observed::Repairing => State::Repairing,
            Observed::Failed => State::Failed,
            // NOTE: This is a bit of an odd one - we intentionally do *not*
            // translate the "destroyed" propolis state to the destroyed instance
            // API state.
            //
            // When a propolis instance reports that it has been destroyed,
            // this does not necessarily mean the customer-visible instance
            // should be torn down. Instead, it implies that the Propolis service
            // should be stopped, but the VM could be allocated to a different
            // machine.
            Observed::Destroyed => State::Stopped,
        };

        let desired = self
            .desired
            .as_ref()
            .and_then(|s| get_next_desired_state(&observed, s.run_state));

        self.transition(current, desired);

        // Most commands to update Propolis are triggered via requests (from
        // Nexus), but if the instance reports that it has been destroyed,
        // we should clean it up.
        if matches!(observed, Observed::Destroyed) {
            Some(Action::Destroy)
        } else {
            None
        }
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: InstanceStateRequested,
    ) -> Result<Option<Action>, Error> {
        match target {
            InstanceStateRequested::Running => self.request_running(),
            InstanceStateRequested::Stopped => self.request_stopped(),
            InstanceStateRequested::Reboot => self.request_reboot(),
            InstanceStateRequested::Destroyed => self.request_destroyed(),
        }
    }

    // Transitions to a new InstanceState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    fn transition(
        &mut self,
        next: InstanceState,
        desired: Option<InstanceStateRequested>,
    ) {
        self.current.run_state = next;
        self.current.gen = self.current.gen.next();
        self.current.time_updated = Utc::now();
        self.desired = desired
            .map(|run_state| InstanceRuntimeStateRequested { run_state });
    }

    fn request_running(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Running request is no-op
            InstanceState::Running
            | InstanceState::Starting
            | InstanceState::Rebooting => return Ok(None),
            // Valid states for a running request
            InstanceState::Creating
            | InstanceState::Stopping
            | InstanceState::Stopped => {
                self.transition(
                    InstanceState::Starting,
                    Some(InstanceStateRequested::Running),
                );
                return Ok(Some(Action::Run));
            }
            // Invalid states for a running request
            InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot run instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_stopped(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Stop request is a no-op
            InstanceState::Stopped | InstanceState::Stopping => {
                return Ok(None)
            }
            // Valid states for a stop request
            InstanceState::Creating => {
                // Already stopped, no action necessary.
                self.transition(InstanceState::Stopped, None);
                return Ok(None);
            }
            InstanceState::Starting
            | InstanceState::Running
            | InstanceState::Rebooting => {
                // The VM is running, explicitly tell it to stop.
                self.transition(
                    InstanceState::Stopping,
                    Some(InstanceStateRequested::Stopped),
                );
                return Ok(Some(Action::Stop));
            }
            // Invalid states for a stop request
            InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot stop instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_reboot(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Reboot request is a no-op
            InstanceState::Rebooting => return Ok(None),
            // Valid states for a reboot request
            InstanceState::Starting | InstanceState::Running => {
                self.transition(
                    InstanceState::Rebooting,
                    Some(InstanceStateRequested::Reboot),
                );
                return Ok(Some(Action::Reboot));
            }
            // Invalid states for a reboot request
            _ => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot reboot instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_destroyed(&mut self) -> Result<Option<Action>, Error> {
        if self.current.run_state.is_stopped() {
            self.transition(InstanceState::Destroyed, None);
            return Ok(Some(Action::Destroy));
        } else {
            self.transition(
                InstanceState::Stopping,
                Some(InstanceStateRequested::Destroyed),
            );
            return Ok(Some(Action::Stop));
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Action, InstanceStates};
    use chrono::Utc;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState as State,
    };
    use omicron_common::api::internal::{
        nexus::InstanceRuntimeState,
        sled_agent::InstanceStateRequested as Requested,
    };
    use propolis_client::api::InstanceState as Observed;

    fn make_instance() -> InstanceStates {
        InstanceStates::new(InstanceRuntimeState {
            run_state: State::Creating,
            sled_uuid: uuid::Uuid::new_v4(),
            propolis_uuid: uuid::Uuid::new_v4(),
            propolis_addr: None,
            ncpus: InstanceCpuCount(2),
            memory: ByteCount::from_mebibytes_u32(512),
            hostname: "myvm".to_string(),
            gen: Generation::new(),
            time_updated: Utc::now(),
        })
    }

    fn verify_state(
        instance: &InstanceStates,
        expected_current: State,
        expected_desired: Option<Requested>,
    ) {
        assert_eq!(expected_current, instance.current().run_state);
        match expected_desired {
            Some(desired) => {
                assert_eq!(
                    desired,
                    instance.desired().as_ref().unwrap().run_state
                );
            }
            None => assert!(instance.desired().is_none()),
        }
    }

    #[test]
    fn test_running_from_creating() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance.request_transition(Requested::Running).unwrap().unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
    }

    #[test]
    fn test_reboot() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance.request_transition(Requested::Running).unwrap().unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Normal reboot behavior:
        // - Request Reboot
        // - Observe Stopping, Starting, Running
        assert_eq!(
            Action::Reboot,
            instance.request_transition(Requested::Reboot).unwrap().unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Stopping));
        verify_state(&instance, State::Stopping, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Starting));
        verify_state(&instance, State::Starting, Some(Requested::Running));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_reboot_skip_starting_converges_to_running() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance.request_transition(Requested::Running).unwrap().unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Unexpected reboot behavior:
        // - Request Reboot
        // - Observe Stopping, jump immediately to Running.
        // - Ultimately, we should still end up "running".
        assert_eq!(
            Action::Reboot,
            instance.request_transition(Requested::Reboot).unwrap().unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Stopping));
        verify_state(&instance, State::Stopping, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_reboot_skip_stopping_converges_to_running() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance.request_transition(Requested::Running).unwrap().unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Unexpected reboot behavior:
        // - Request Reboot
        // - Observe Starting then Running.
        // - Ultimately, we should still end up "running".
        assert_eq!(
            Action::Reboot,
            instance.request_transition(Requested::Reboot).unwrap().unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Starting));
        verify_state(&instance, State::Starting, Some(Requested::Running));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_destroy_from_running_stops_first() {
        let mut instance = make_instance();
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        assert_eq!(
            Action::Stop,
            instance.request_transition(Requested::Destroyed).unwrap().unwrap()
        );
        verify_state(&instance, State::Stopping, Some(Requested::Destroyed));
        assert_eq!(None, instance.observe_transition(&Observed::Stopped));
        verify_state(&instance, State::Stopped, Some(Requested::Destroyed));
        assert_eq!(
            Action::Destroy,
            instance.observe_transition(&Observed::Destroyed).unwrap()
        );
        verify_state(&instance, State::Stopped, None);
    }

    #[test]
    fn test_destroy_from_stopped_destroys_immediately() {
        let mut instance = make_instance();
        assert_eq!(None, instance.observe_transition(&Observed::Stopped));
        assert_eq!(
            Action::Destroy,
            instance.request_transition(Requested::Destroyed).unwrap().unwrap()
        );
        verify_state(&instance, State::Destroyed, None);
    }
}
