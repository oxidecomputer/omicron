//! code shared between the "real" and "sim" Sled Agents.

use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;

// TODO: Refactor to "instance" module.
// TODO: Add "disk" module.

/// The instance state is a combination of the last-known state,
/// as well as an "objective" state which the sled agent will
/// work towards achieving.
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

    fn update(&self, next: ApiInstanceState, pending: Option<ApiInstanceStateRequested>) -> Self {
        InstanceState {
            current: ApiInstanceRuntimeState {
                run_state: next,
                sled_uuid: self.current.sled_uuid,
                gen: self.current.gen.next(),
                time_updated: Utc::now(),
            },
            pending: pending.map(|run_state| ApiInstanceRuntimeStateRequested { run_state }),
        }
    }


    // TODO: Act on &mut self? Perform update?
    pub fn next(
        &self,
        target: &ApiInstanceRuntimeStateRequested,
    ) -> Result<InstanceState, ApiError> {
        // Validate the state transition and return the next state.
        // This may differ from the "final" state if the transition requires
        // multiple stages (i.e., running -> stopping -> stopped).
        let (next_state, final_state) = match target.run_state {
            ApiInstanceStateRequested::Running => {
                match self.current.run_state {
                    // Early exit: Running request is no-op
                    ApiInstanceState::Running
                    | ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        return Ok(self.clone());
                    }
                    // Valid states for a running request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Starting
                    | ApiInstanceState::Stopping { rebooting: false }
                    | ApiInstanceState::Stopped { rebooting: false } => {
                        if self.current.run_state.is_stopped() {
                            (
                                ApiInstanceState::Starting,
                                Some(ApiInstanceStateRequested::Running),
                            )
                        } else {
                            (ApiInstanceState::Running, None)
                        }
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
                        return Ok(self.clone());
                    }
                    // Valid states for a stop request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Starting
                    | ApiInstanceState::Running
                    | ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        // Note that if we were rebooting, a request to enter
                        // the stopped state effectively cancels the reboot.
                        if self.current.run_state.is_stopped() {
                            (
                                ApiInstanceState::Stopped { rebooting: false },
                                None,
                            )
                        } else {
                            (
                                ApiInstanceState::Stopping { rebooting: false },
                                Some(ApiInstanceStateRequested::Stopped),
                            )
                        }
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
                        return Ok(self.clone());
                    }
                    // Valid states for a reboot request
                    ApiInstanceState::Starting | ApiInstanceState::Running => {
                        if self.current.run_state.is_stopped() {
                            (
                                ApiInstanceState::Stopped { rebooting: true },
                                Some(ApiInstanceStateRequested::Stopped),
                            )
                        } else {
                            (
                                ApiInstanceState::Stopping { rebooting: true },
                                Some(ApiInstanceStateRequested::Stopped),
                            )
                        }
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
                    (ApiInstanceState::Destroyed, None)
                } else {
                    (
                        ApiInstanceState::Stopping { rebooting: false },
                        Some(ApiInstanceStateRequested::Destroyed),
                    )
                }
            }
        };

        Ok(self.update(next_state, final_state))
    }

    // TODO: So...... the fact that we only have a *single* match
    // for the "pending" state is a sign that maaaayyyyybe pending is actually
    // implicit????
    //
    // Or at least can be constrained significantly
    pub fn advance(
        &self
    ) -> InstanceState {
        if let Some(pending) = self.pending.as_ref() {
            match self.current.run_state {
                ApiInstanceState::Creating | ApiInstanceState::Starting => {
                    match pending.run_state {
                        ApiInstanceStateRequested::Running => {
                            self.update(ApiInstanceState::Running, None)
                        }
                        _ => panic!("unexpected transition: Creating/Starting only allowed to become Running"),
                    }
                },
                ApiInstanceState::Stopping { rebooting } => {
                    match pending.run_state {
                        ApiInstanceStateRequested::Stopped => {
                            let next = if rebooting {
                                Some(ApiInstanceStateRequested::Running)
                            } else {
                                None
                            };
                            self.update( ApiInstanceState::Stopped { rebooting: false }, next)

                        }
                        ApiInstanceStateRequested::Destroyed => {
                            self.update(ApiInstanceState::Destroyed, None)
                        }
                        _ => panic!("unexpected transition: Stopping only allowed to become Stopped; was trying to become {}", pending.run_state),
                    }
                },
                _ => panic!("unexpected transition: Pending transition from non-transitory state {}", self.current.run_state),
            }
        } else {
            // No pending state means that we have no objective state.
            self.clone()
        }
    }
}
