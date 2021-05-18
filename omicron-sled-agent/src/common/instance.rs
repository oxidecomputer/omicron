use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;
use propolis_client::api::InstanceState as PropolisInstanceState;

/// Action to be taken on behalf of state transition.
pub enum Action {
    Run,
    Stop,
    Reboot,
    Destroy,
}

pub fn propolis_to_omicron_state(state: PropolisInstanceState) -> ApiInstanceState {
    match state {
        PropolisInstanceState::Creating => ApiInstanceState::Creating,
        PropolisInstanceState::Starting => ApiInstanceState::Starting,
        PropolisInstanceState::Running => ApiInstanceState::Running,
        PropolisInstanceState::Stopping => ApiInstanceState::Stopping { rebooting: false },
        PropolisInstanceState::Stopped => ApiInstanceState::Stopped { rebooting: false },
        PropolisInstanceState::Repairing => ApiInstanceState::Repairing,
        PropolisInstanceState::Failed => ApiInstanceState::Failed,
        PropolisInstanceState::Destroyed => ApiInstanceState::Destroyed
    }
}

/// The instance state is a combination of the last-known state, as well as an
/// "objective" state which the sled agent will work towards achieving.
// TODO: Use this instead of "current + pending" in APIs.
#[derive(Clone)]
pub struct InstanceState {
    // TODO: Not pub!
    pub current: ApiInstanceRuntimeState,
    pub pending: Option<ApiInstanceRuntimeStateRequested>,
}

impl InstanceState {
    pub fn new(current: ApiInstanceRuntimeState) -> Self {
        InstanceState {
            current,
            pending: None,
        }
    }

    // Transitions to a new InstanceState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    fn transition(&mut self, next: ApiInstanceState, pending: Option<ApiInstanceStateRequested>) {
        // TODO: Deal with no-op transition?
        self.current =
            ApiInstanceRuntimeState {
                run_state: next,
                sled_uuid: self.current.sled_uuid,
                gen: self.current.gen.next(),
                time_updated: Utc::now(),
            };
        self.pending = pending.map(|run_state| ApiInstanceRuntimeStateRequested { run_state });
    }

    /// Update the known state of an instance based on a response from Propolis.
    pub fn observe_transition(
        &mut self,
        observed: PropolisInstanceState,
    ) -> Option<Action> {
        let next_pending = match observed {
            PropolisInstanceState::Stopped => {
                if matches!(self.current.run_state, ApiInstanceState::Stopping { rebooting } if rebooting) {
                    Some(ApiInstanceStateRequested::Running)
                } else {
                    None
                }
            },
            _ => None,
        };
        self.transition(propolis_to_omicron_state(observed), next_pending);
        None
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: &ApiInstanceRuntimeStateRequested,
    ) -> Result<Option<Action>, ApiError> {
        // Validate the state transition and return the next state.
        // This may differ from the "final" state if the transition requires
        // multiple stages (i.e., running -> stopping -> stopped).
        match target.run_state {
            ApiInstanceStateRequested::Running => {
                match self.current.run_state {
                    // Early exit: Running request is no-op
                    ApiInstanceState::Running
                    | ApiInstanceState::Starting
                    | ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        return Ok(None)
                    }
                    // Valid states for a running request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Stopping { rebooting: false }
                    | ApiInstanceState::Stopped { rebooting: false } => {
                        self.transition(
                            ApiInstanceState::Starting,
                            Some(ApiInstanceStateRequested::Running),
                        );
                        return Ok(Some(Action::Run))
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
            ApiInstanceStateRequested::Stopped => {
                match self.current.run_state {
                    // Early exit: Stop request is a no-op
                    ApiInstanceState::Stopped { rebooting: false }
                    | ApiInstanceState::Stopping { rebooting: false } => {
                        return Ok(None)
                    }
                    // Valid states for a stop request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        // Already stopped, no action necessary.
                        self.transition(ApiInstanceState::Stopped { rebooting: false }, None);
                        return Ok(None)
                    },
                    ApiInstanceState::Stopping { rebooting: true } => {
                        // The VM is stopping, so no new action is necessary to
                        // make it stop, but we can avoid rebooting.
                        self.transition(
                            ApiInstanceState::Stopping { rebooting: false },
                            Some(ApiInstanceStateRequested::Stopped),
                        );
                        return Ok(None)
                    },
                    ApiInstanceState::Starting
                    | ApiInstanceState::Running => {
                        // The VM is running, explicitly tell it to stop.
                        self.transition(
                            ApiInstanceState::Stopping { rebooting: false },
                            Some(ApiInstanceStateRequested::Stopped),
                        );
                        return Ok(Some(Action::Stop))
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
            ApiInstanceStateRequested::Reboot => {
                match self.current.run_state {
                    // Early exit: Reboot request is a no-op
                    ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        return Ok(None);
                    }
                    // Valid states for a reboot request
                    ApiInstanceState::Starting | ApiInstanceState::Running => {
                         self.transition(
                            ApiInstanceState::Stopping { rebooting: true },
                            Some(ApiInstanceStateRequested::Stopped),
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
            // All states may be destroyed.
            ApiInstanceStateRequested::Destroyed => {
                if self.current.run_state.is_stopped() {
                    self.transition(ApiInstanceState::Destroyed, None);
                    return Ok(Some(Action::Destroy));
                } else {
                    self.transition(
                        ApiInstanceState::Stopping { rebooting: false },
                        Some(ApiInstanceStateRequested::Destroyed),
                    );
                    return Ok(Some(Action::Stop));
                }
            }
        };
    }
}
