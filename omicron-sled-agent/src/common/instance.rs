//! Describes the states of VM instances.

use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;
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

fn propolis_to_omicron_state(
    state: &PropolisInstanceState,
) -> ApiInstanceState {
    match state {
        PropolisInstanceState::Creating => ApiInstanceState::Creating,
        PropolisInstanceState::Starting => ApiInstanceState::Starting,
        PropolisInstanceState::Running => ApiInstanceState::Running,
        PropolisInstanceState::Stopping => ApiInstanceState::Stopping,
        PropolisInstanceState::Stopped => ApiInstanceState::Stopped,
        PropolisInstanceState::Rebooting => ApiInstanceState::Rebooting,
        PropolisInstanceState::Repairing => ApiInstanceState::Repairing,
        PropolisInstanceState::Failed => ApiInstanceState::Failed,
        PropolisInstanceState::Destroyed => ApiInstanceState::Destroyed,
    }
}

fn get_next_desired_state(
    observed: &PropolisInstanceState,
    requested: ApiInstanceStateRequested,
) -> Option<ApiInstanceStateRequested> {
    use ApiInstanceStateRequested as Requested;
    use PropolisInstanceState as Observed;

    match (requested, observed) {
        (Requested::Running, Observed::Running) => None,
        (Requested::Stopped, Observed::Stopped) => None,
        (Requested::Destroyed, Observed::Destroyed) => None,

        // Reboot requests.
        // - "Stopping" is an expected state on the way to rebooting.
        // - "Starting" expects "Running" to occur next.
        // - Other observed states stop the expected transitions.
        (Requested::Reboot, Observed::Stopping) => Some(requested),
        (Requested::Reboot, Observed::Starting) => Some(Requested::Running),
        (Requested::Reboot, _) => None,

        (_, _) => Some(requested),
    }
}

/// The instance state is a combination of the last-known state, as well as an
/// "objective" state which the sled agent will work towards achieving.
#[derive(Clone, Debug)]
pub struct InstanceState {
    // Last known state reported from Propolis.
    current: ApiInstanceRuntimeState,

    // Desired state, which we will attempt to poke the VM towards.
    desired: Option<ApiInstanceRuntimeStateRequested>,
}

impl InstanceState {
    pub fn new(current: ApiInstanceRuntimeState) -> Self {
        InstanceState { current, desired: None }
    }

    /// Returns the current instance state.
    pub fn current(&self) -> &ApiInstanceRuntimeState {
        &self.current
    }

    /// Returns the desired instance state, if any exists.
    pub fn desired(&self) -> &Option<ApiInstanceRuntimeStateRequested> {
        &self.desired
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub fn observe_transition(
        &mut self,
        observed: &PropolisInstanceState,
    ) -> Option<Action> {
        use ApiInstanceState as State;
        use ApiInstanceStateRequested as Requested;

        let current = propolis_to_omicron_state(observed);
        let desired = self
            .desired
            .as_ref()
            .and_then(|s| get_next_desired_state(&observed, s.run_state));

        self.transition(current, desired);

        // Most commands to update Propolis are triggered via requests (from
        // Nexus), but if the instance reports that it has been destroyed,
        // we should clean it up.
        match (current, desired) {
            (State::Destroyed, _)
            | (State::Stopped, Some(Requested::Destroyed)) => {
                Some(Action::Destroy)
            }
            _ => None,
        }
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: ApiInstanceStateRequested,
    ) -> Result<Option<Action>, ApiError> {
        match target {
            ApiInstanceStateRequested::Running => self.request_running(),
            ApiInstanceStateRequested::Stopped => self.request_stopped(),
            ApiInstanceStateRequested::Reboot => self.request_reboot(),
            ApiInstanceStateRequested::Destroyed => self.request_destroyed(),
        }
    }

    // Transitions to a new InstanceState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    fn transition(
        &mut self,
        next: ApiInstanceState,
        desired: Option<ApiInstanceStateRequested>,
    ) {
        self.current = ApiInstanceRuntimeState {
            run_state: next,
            sled_uuid: self.current.sled_uuid,
            gen: self.current.gen.next(),
            time_updated: Utc::now(),
        };
        self.desired = desired
            .map(|run_state| ApiInstanceRuntimeStateRequested { run_state });
    }

    fn request_running(&mut self) -> Result<Option<Action>, ApiError> {
        match self.current.run_state {
            // Early exit: Running request is no-op
            ApiInstanceState::Running
            | ApiInstanceState::Starting
            | ApiInstanceState::Rebooting => return Ok(None),
            // Valid states for a running request
            ApiInstanceState::Creating
            | ApiInstanceState::Stopping
            | ApiInstanceState::Stopped => {
                self.transition(
                    ApiInstanceState::Starting,
                    Some(ApiInstanceStateRequested::Running),
                );
                return Ok(Some(Action::Run));
            }
            // Invalid states for a running request
            ApiInstanceState::Repairing
            | ApiInstanceState::Failed
            | ApiInstanceState::Destroyed => {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot run instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_stopped(&mut self) -> Result<Option<Action>, ApiError> {
        match self.current.run_state {
            // Early exit: Stop request is a no-op
            ApiInstanceState::Stopped | ApiInstanceState::Stopping => {
                return Ok(None)
            }
            // Valid states for a stop request
            ApiInstanceState::Creating => {
                // Already stopped, no action necessary.
                self.transition(ApiInstanceState::Stopped, None);
                return Ok(None);
            }
            ApiInstanceState::Starting
            | ApiInstanceState::Running
            | ApiInstanceState::Rebooting => {
                // The VM is running, explicitly tell it to stop.
                self.transition(
                    ApiInstanceState::Stopping,
                    Some(ApiInstanceStateRequested::Stopped),
                );
                return Ok(Some(Action::Stop));
            }
            // Invalid states for a stop request
            ApiInstanceState::Repairing
            | ApiInstanceState::Failed
            | ApiInstanceState::Destroyed => {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot stop instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_reboot(&mut self) -> Result<Option<Action>, ApiError> {
        match self.current.run_state {
            // Early exit: Reboot request is a no-op
            ApiInstanceState::Rebooting => return Ok(None),
            // Valid states for a reboot request
            ApiInstanceState::Starting | ApiInstanceState::Running => {
                self.transition(
                    ApiInstanceState::Rebooting,
                    Some(ApiInstanceStateRequested::Reboot),
                );
                return Ok(Some(Action::Reboot));
            }
            // Invalid states for a reboot request
            _ => {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot reboot instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_destroyed(&mut self) -> Result<Option<Action>, ApiError> {
        if self.current.run_state.is_stopped() {
            self.transition(ApiInstanceState::Destroyed, None);
            return Ok(Some(Action::Destroy));
        } else {
            self.transition(
                ApiInstanceState::Stopping,
                Some(ApiInstanceStateRequested::Destroyed),
            );
            return Ok(Some(Action::Stop));
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Action, InstanceState};
    use chrono::Utc;
    use omicron_common::model::{
        ApiGeneration, ApiInstanceRuntimeState, ApiInstanceState as State,
        ApiInstanceStateRequested as Requested,
    };
    use propolis_client::api::InstanceState as Observed;

    fn make_instance() -> InstanceState {
        InstanceState::new(ApiInstanceRuntimeState {
            run_state: State::Creating,
            sled_uuid: uuid::Uuid::new_v4(),
            gen: ApiGeneration::new(),
            time_updated: Utc::now(),
        })
    }

    fn verify_state(
        instance: &InstanceState,
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
        assert_eq!(
            Action::Destroy,
            instance.observe_transition(&Observed::Stopped).unwrap()
        );
        verify_state(&instance, State::Stopped, Some(Requested::Destroyed));
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
