/*!
 * Simulated sled agent implementation
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiDiskStateRequested;
use crate::sled_agent::sim::simulatable::Simulatable;
use crate::controller;
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Simulated Disk (network block device), as created by the external Oxide API
 *
 * See `Simulatable` for how this works.
 */
#[derive(Debug)]
pub struct SimDisk {}

#[async_trait]
impl Simulatable for SimDisk {
    type CurrentState = ApiDiskRuntimeState;
    type RequestedState = ApiDiskStateRequested;

    fn next_state_for_new_target(
        current: &Self::CurrentState,
        pending: &Option<Self::RequestedState>,
        next: &Self::RequestedState,
    ) -> Result<(Self::CurrentState, Option<Self::RequestedState>), ApiError>
    {
        let state_before = &current.disk_state;
        let state_after = next;

        let to_do = match (state_before, state_after) {
            /*
             * It's conceivable that we'd be asked to transition from a state to
             * itself, in which case we also don't need to do anything.
             */
            (
                ApiDiskState::Attached(id1),
                ApiDiskStateRequested::Attached(id2),
            ) => {
                /*
                 * It's not legal to ask us to go from attached to one instance
                 * to attached to another.
                 */
                assert!(pending.is_none());
                if id1 != id2 {
                    return Err(ApiError::InvalidRequest {
                        message: String::from("disk is already attached"),
                    });
                }

                None
            }
            (ApiDiskState::Detached, ApiDiskStateRequested::Detached) => {
                assert!(pending.is_none());
                None
            }
            (ApiDiskState::Destroyed, ApiDiskStateRequested::Destroyed) => {
                assert!(pending.is_none());
                None
            }
            (ApiDiskState::Faulted, ApiDiskStateRequested::Faulted) => {
                assert!(pending.is_none());
                None
            }

            /*
             * If we're going from any unattached state to "Attached" (the only
             * requestable attached state), the appropriate next state is
             * "Attaching", and it will be an asynchronous transition to
             * "Attached".  This is allowed even for "Destroyed" -- the caller
             * is responsible for disallowing this if that's what's intended.
             */
            (ApiDiskState::Creating, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(*id), Some(state_after)))
            }
            (ApiDiskState::Detached, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(*id), Some(state_after)))
            }
            (ApiDiskState::Destroyed, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(*id), Some(state_after)))
            }
            (ApiDiskState::Faulted, ApiDiskStateRequested::Attached(id)) => {
                assert!(pending.is_none());
                Some((ApiDiskState::Attaching(*id), Some(state_after)))
            }

            /*
             * If we're currently attaching, it's only legal to try to attach to
             * the same thing (in which case it's a noop).
             * TODO-cleanup would it be more consistent with our intended
             * interface (which is to let the controller just say what it wants
             * and have us do the work) to have this work and go through
             * detaching first?
             */
            (
                ApiDiskState::Attaching(id1),
                ApiDiskStateRequested::Attached(id2),
            ) => {
                if id1 != id2 {
                    return Err(ApiError::InvalidRequest {
                        message: String::from("disk is already attached"),
                    });
                }

                Some((ApiDiskState::Attaching(*id2), Some(state_after)))
            }

            (
                ApiDiskState::Detaching(_),
                ApiDiskStateRequested::Attached(_),
            ) => {
                return Err(ApiError::InvalidRequest {
                    message: String::from("cannot attach while detaching"),
                });
            }

            /*
             * If we're going from any attached state to any detached state,
             * then we'll go straight to "Detaching" en route to the new state.
             */
            (from_state, to_state)
                if from_state.is_attached() && !to_state.is_attached() =>
            {
                let id = from_state.attached_instance_id().unwrap();
                Some((ApiDiskState::Detaching(*id), Some(to_state)))
            }

            /*
             * The only remaining options are transitioning from one detached
             * state to a different one, in which case we can go straight there
             * with no need for an asynchronous transition.
             */
            (from_state, ApiDiskStateRequested::Destroyed) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Destroyed, None))
            }

            (from_state, ApiDiskStateRequested::Detached) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Detached, None))
            }

            (from_state, ApiDiskStateRequested::Faulted) => {
                assert!(!from_state.is_attached());
                Some((ApiDiskState::Faulted, None))
            }
        };

        if to_do.is_none() {
            return Ok((current.clone(), pending.clone()));
        }

        let (immed_next_state, next_async) = to_do.unwrap();
        let next_state = ApiDiskRuntimeState {
            disk_state: immed_next_state,
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };

        Ok((next_state, next_async.cloned()))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let state_before = &current.disk_state;
        let is_detaching = match &current.disk_state {
            ApiDiskState::Attaching(_) => false,
            ApiDiskState::Detaching(_) => true,
            _ => panic!("async transition was not in progress"),
        };
        let next_state = match pending {
            ApiDiskStateRequested::Attached(id) => {
                assert_eq!(state_before, &ApiDiskState::Attaching(*id));
                ApiDiskState::Attached(*id)
            }
            ApiDiskStateRequested::Faulted => {
                assert!(is_detaching);
                ApiDiskState::Faulted
            }
            ApiDiskStateRequested::Destroyed => {
                assert!(is_detaching);
                ApiDiskState::Destroyed
            }
            ApiDiskStateRequested::Detached => {
                assert!(is_detaching);
                ApiDiskState::Detached
            }
        };
        let next_runtime = ApiDiskRuntimeState {
            disk_state: next_state,
            gen: current.gen + 1,
            time_updated: Utc::now(),
        };
        (next_runtime, None)
    }

    fn state_unchanged(
        state1: &Self::CurrentState,
        state2: &Self::CurrentState,
    ) -> bool {
        state1.gen == state2.gen
    }

    fn ready_to_destroy(current: &Self::CurrentState) -> bool {
        ApiDiskState::Destroyed == current.disk_state
    }

    async fn notify(
        csc: &Arc<controller::Client>,
        id: &Uuid,
        current: Self::CurrentState,
    ) -> Result<(), ApiError> {
        csc.notify_disk_updated(id, &current).await
    }
}
