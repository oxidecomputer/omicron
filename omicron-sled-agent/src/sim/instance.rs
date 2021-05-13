/*!
 * Simulated sled agent implementation
 */

use super::simulatable::Simulatable;

use async_trait::async_trait;
use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceStateRequested;
use omicron_common::NexusClient;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Simulated Instance (virtual machine), as created by the external Oxide API
 */
#[derive(Debug)]
pub struct SimInstance {}

#[async_trait]
impl Simulatable for SimInstance {
    type CurrentState = ApiInstanceRuntimeState;
    type RequestedState = ApiInstanceRuntimeStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        target: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        // Validate the state transition and return the next state.
        // This may differ from the "final" state if the transition requires
        // multiple stages (i.e., running -> stopping -> stopped).
        let (next_state, final_state) = match target.run_state {
            ApiInstanceStateRequested::Running => {
                match current.run_state {
                    // Early exit: Running request is no-op
                    ApiInstanceState::Running
                    | ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        return Ok((current.clone(), pending.clone()));
                    }
                    // Valid states for a running request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Starting
                    | ApiInstanceState::Stopping { rebooting: false }
                    | ApiInstanceState::Stopped { rebooting: false } => {
                        if current.run_state.is_stopped() {
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
                                current.run_state,
                            ),
                        });
                    }
                }
            }
            ApiInstanceStateRequested::Stopped => {
                match current.run_state {
                    // Early exit: Stop request is a no-op
                    ApiInstanceState::Stopped { rebooting: false }
                    | ApiInstanceState::Stopping { rebooting: false } => {
                        return Ok((current.clone(), pending.clone()));
                    }
                    // Valid states for a stop request
                    ApiInstanceState::Creating
                    | ApiInstanceState::Starting
                    | ApiInstanceState::Running
                    | ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        // Note that if we were rebooting, a request to enter
                        // the stopped state effectively cancels the reboot.
                        if current.run_state.is_stopped() {
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
                                current.run_state,
                            ),
                        });
                    }
                }
            }
            ApiInstanceStateRequested::Reboot => {
                match current.run_state {
                    // Early exit: Reboot request is a no-op
                    ApiInstanceState::Stopping { rebooting: true }
                    | ApiInstanceState::Stopped { rebooting: true } => {
                        return Ok((current.clone(), pending.clone()));
                    }
                    // Valid states for a reboot request
                    ApiInstanceState::Starting | ApiInstanceState::Running => {
                        if current.run_state.is_stopped() {
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
                                current.run_state,
                            ),
                        });
                    }
                }
            }
            // All states may be destroyed.
            ApiInstanceStateRequested::Destroyed => {
                if current.run_state.is_stopped() {
                    (ApiInstanceState::Destroyed, None)
                } else {
                    (
                        ApiInstanceState::Stopping { rebooting: false },
                        Some(ApiInstanceStateRequested::Destroyed),
                    )
                }
            }
        };

        let next_state = ApiInstanceRuntimeState {
            run_state: next_state,
            sled_uuid: current.sled_uuid,
            gen: current.gen.next(),
            time_updated: Utc::now(),
        };

        let next_target = if let Some(final_state) = final_state {
            Some(ApiInstanceRuntimeStateRequested { run_state: final_state })
        } else {
            None
        };

        Ok((next_state, next_target))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        /*
         * As documented above, `self.requested_state` is only non-None when
         * there's an asynchronous (simulated) transition in progress, and the
         * only such transitions start at "Starting" or "Stopping" and go to
         * "Running" or one of several stopped states, respectively.  Since we
         * checked `self.requested_state` above, we know we're in one of
         * these two transitions and assert that here.
         */
        let run_state_before = current.run_state;
        let run_state_after = pending.run_state;
        match run_state_before {
            ApiInstanceState::Stopped { rebooting: _ }
            | ApiInstanceState::Stopping { rebooting: _ } => {
                assert!(run_state_after.is_stopped());
            }
            ApiInstanceState::Starting => {
                assert!(!run_state_after.is_stopped());
            }
            _ => panic!("async transition started for unexpected state"),
        };

        let run_state = match run_state_after {
            ApiInstanceStateRequested::Running => ApiInstanceState::Running,
            ApiInstanceStateRequested::Stopped => {
                ApiInstanceState::Stopped { rebooting: false }
            }
            ApiInstanceStateRequested::Destroyed => ApiInstanceState::Destroyed,
            _ => panic!("unexpected async transition: {}", run_state_after),
        };
        /*
         * Having verified all that, we can update the Instance's state.
         */
        let next_state = ApiInstanceRuntimeState {
            run_state,
            sled_uuid: current.sled_uuid,
            gen: current.gen.next(),
            time_updated: Utc::now(),
        };

        let next_async = if run_state_before.is_rebooting() {
            assert_eq!(run_state_after, ApiInstanceStateRequested::Stopped);
            Some(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceStateRequested::Running,
            })
        } else {
            None
        };

        (next_state, next_async)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        state1.gen == state2.gen
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        current.run_state == ApiInstanceState::Destroyed
    }

    async fn notify(
        csc: &Arc<NexusClient>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        /*
         * Notify Nexus that the instance state has changed.  The sled agent is
         * authoritative for the runtime state, and we use a generation number
         * here so that calls processed out of order do not settle on the wrong
         * value.
         */
        csc.notify_instance_updated(id, &current).await
    }
}
