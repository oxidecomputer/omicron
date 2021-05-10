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

        /*
        match current.run_state {
            ApiInstanceState::Creating,
            ApiInstanceState::Starting,
            ApiInstanceState::Running,
            ApiInstanceState::Stopping,
            ApiInstanceState::Stopped,
            ApiInstanceState::Repairing,
            ApiInstanceState::Failed,
            ApiInstanceState::Destroyed,
        }
        */

        // - Can only reboot TO running.
        // - Can only reboot FROM starting, running, stopping.
        // - If current state == target...
        //      - ... AND no reboots are pending+requested
        //      - ... OR a reboot is pending+requested but our current state is
        //      stopping...
        //   -> Then exit early with "OK".
        // - If a reboot is requested, set the target state to stopped.
        //

        /*
         * TODO-cleanup it would be better if the type used to represent a
         * requested instance state did not allow you to even express this
         * value.
         */
        if target.reboot_wanted
            && target.run_state != ApiInstanceStateRequested::Running
        {
            return Err(ApiError::InvalidRequest {
                message: String::from(
                    "cannot reboot to a state other than \"running\"",
                ),
            });
        }

        let state_before = current.run_state.clone();

        /*
         * TODO-cleanup would it be possible to eliminate this possibility by
         * modifying the type used to represent the requested instance state?
         */
        if target.reboot_wanted
            && state_before != ApiInstanceState::Starting
            && state_before != ApiInstanceState::Running
            && (state_before != ApiInstanceState::Stopping
                || !current.reboot_in_progress)
        {
            return Err(ApiError::InvalidRequest {
                message: format!(
                    "cannot reboot instance in state \"{}\"",
                    state_before
                ),
            });
        }

        let mut state_after = &target.run_state;

        /*
         * There's nothing to do if the current and target states are the same
         * AND either:
         *
         * - there's neither a reboot pending nor a reboot requested
         * - there's both a reboot pending and a reboot requested and
         *   the current reboot is still in the "Stopping" phase
         *
         * Otherwise, even if the states match, we may need to take action to
         * begin or cancel a reboot.
         *
         * Reboot can only be requested with a target state of "Running".
         * It doesn't make sense to reboot to any other state.
         * TODO-debug log a warning in this case or make it impossible to
         * represent?  Or validate it sooner?  TODO-cleanup if we create a
         * separate ApiInstanceStateRequested as discussed elsewhere, the
         * `Running` state could have a boolean indicating whether a reboot
         * is requested first.
         */
        let reb_pending = current.reboot_in_progress;
        if state_before == state_after.clone().into()
            && ((!reb_pending && !target.reboot_wanted)
                || (reb_pending
                    && target.reboot_wanted
                    && state_before == ApiInstanceState::Stopping))
        {
            let next_state = current.clone();
            let next_async = pending.clone();
            return Ok((next_state, next_async));
        }

        /*
         * If we're doing a reboot, then we've already verified that the target
         * run state is "Running", but for the rest of this function we'll treat
         * it like a transition to "Stopped" (with an extra bit telling us later
         * to transition again to Running).
         */
        if target.reboot_wanted {
            state_after = &ApiInstanceStateRequested::Stopped;
        }

        /*
         * Depending on what state we're in and what state we're going to, we
         * may need to transition to an intermediate state before we can get to
         * the requested state.  In that case, we'll asynchronously simulate the
         * transition.
         */
        let (immed_next_state, need_async) =
            if state_before.is_stopped() && !state_after.is_stopped() {
                (ApiInstanceState::Starting, true)
            } else if !state_before.is_stopped() && state_after.is_stopped() {
                (ApiInstanceState::Stopping, true)
            } else {
                (state_after.clone().into(), false)
            };

        let next_state = ApiInstanceRuntimeState {
            run_state: immed_next_state,
            reboot_in_progress: target.reboot_wanted,
            sled_uuid: current.sled_uuid,
            gen: current.gen.next(),
            time_updated: Utc::now(),
        };

        let next_async = if need_async {
            Some(ApiInstanceRuntimeStateRequested {
                run_state: state_after.clone(),
                reboot_wanted: target.reboot_wanted,
            })
        } else {
            None
        };

        Ok((next_state, next_async))
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
        let run_state_before = current.run_state.clone();
        let run_state_after = &pending.run_state;
        match run_state_before {
            ApiInstanceState::Starting => {
                assert_eq!(
                    *run_state_after,
                    ApiInstanceStateRequested::Running
                );
                assert!(!pending.reboot_wanted);
            }
            ApiInstanceState::Stopping => {
                assert!(run_state_after.is_stopped());
                assert_eq!(pending.reboot_wanted, current.reboot_in_progress);
                assert!(
                    !pending.reboot_wanted
                        || *run_state_after
                            == ApiInstanceStateRequested::Stopped
                );
            }
            _ => panic!("async transition started for unexpected state"),
        };

        /*
         * Having verified all that, we can update the Instance's state.
         */
        let next_state = ApiInstanceRuntimeState {
            run_state: run_state_after.clone().into(),
            reboot_in_progress: pending.reboot_wanted,
            sled_uuid: current.sled_uuid,
            gen: current.gen.next(),
            time_updated: Utc::now(),
        };

        let next_async = if next_state.reboot_in_progress {
            assert_eq!(*run_state_after, ApiInstanceStateRequested::Stopped);
            Some(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceStateRequested::Running,
                reboot_wanted: false,
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
