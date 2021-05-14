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

use crate::common::InstanceState;

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
        let old_state = InstanceState {
            current: current.clone(),
            pending: pending.clone(),
        };

        let new_state = old_state.next(target)?;

        Ok((new_state.current, new_state.pending))
    }

    fn next_state_for_async_transition_finish(
        current: &Self::CurrentState,
        pending: &Self::RequestedState,
    ) -> (Self::CurrentState, Option<Self::RequestedState>) {
        let current = InstanceState {
            current: current.clone(),
            pending: Some(pending.clone()),
        };

        let next = current.advance();
        (next.current, next.pending)

        /*
         * As documented above, `self.requested_state` is only non-None when
         * there's an asynchronous (simulated) transition in progress, and the
         * only such transitions start at "Starting" or "Stopping" and go to
         * "Running" or one of several stopped states, respectively.  Since we
         * checked `self.requested_state` above, we know we're in one of
         * these two transitions and assert that here.
         */
        /*
        match current.run_state {
            ApiInstanceState::Stopped { rebooting: _ }
            | ApiInstanceState::Stopping { rebooting: _ } => {
                assert!(pending.run_state.is_stopped());
            }
            ApiInstanceState::Starting => {
                assert!(!pending.run_state.is_stopped());
            }
            _ => panic!("async transition started for unexpected state"),
        };

        let run_state = match pending.run_state {
            ApiInstanceStateRequested::Running => ApiInstanceState::Running,
            ApiInstanceStateRequested::Stopped => {
                ApiInstanceState::Stopped { rebooting: false }
            }
            ApiInstanceStateRequested::Destroyed => ApiInstanceState::Destroyed,
            _ => panic!("unexpected async transition: {}", pending.run_state),
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

        let next_async = if current.run_state.is_rebooting() {
            assert_eq!(pending.run_state, ApiInstanceStateRequested::Stopped);
            Some(ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceStateRequested::Running,
            })
        } else {
            None
        };

        (next_state, next_async)
            */
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
