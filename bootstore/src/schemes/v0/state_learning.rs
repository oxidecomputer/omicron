// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Uninitialized`

use std::collections::BTreeMap;

use crate::schemes::v0::fsm_output::ApiError;
use crate::schemes::v0::state_learned::LearnedState;
use crate::trust_quorum::SharePkgV0;

use super::fsm::{next_peer, StateHandler};
use super::fsm_output::Output;
use super::messages::{Request, RequestType, Response, ResponseType};
use super::state::{FsmCommonData, InitialMemberState, State};
use sled_hardware::Baseboard;
use uuid::Uuid;

/// An attempt by *this* peer to learn a key share
///
/// When an attempt is started, a peer is selected from those known to the FSM,
/// and a `RequestType::Learn` message is sent to that peer. This peer is recorded
/// along with the current clock as `start`. If `Config.learn_timeout` ticks
/// have fired since `start` based on the current FSM clock, without this
/// FSM having received a `ResponseType::Pkg`, then the `LearnAttempt` will be
/// cancelled, and the next peer in order known to the FSM will be contacted in
/// a new attempt.
#[derive(Debug, Clone)]
pub struct LearnAttempt {
    pub peer: Baseboard,
    pub start: Ticks,
}

impl LearnAttempt {
    pub fn expired(&self, now: Ticks, timeout: Ticks) -> bool {
        now.saturating_sub(self.start) >= timeout
    }
}

#[derive(Debug)]
pub struct LearningState {
    attempt: Option<LearnAttempt>,
}

impl LearningState {
    fn name(&self) -> &'static str {
        "learning"
    }
}

impl StateHandler for LearningState {
    fn handle_request(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (state, Output) {
        use RequestType::*;
        let output = match request {
            Init(_) => {
                Output::respond(from, request_id, Error::AlreadyLearning.into())
            }
            InitLearner => {
                // Idempotent, since we are already learning
                // TODO: Should we send a rack_uuid with this messsage
                // and check it ?
                Output::respond(from, request_id, ResponseType::InitAck)
            }
            GetShare { .. } => {
                Output::respond(from, request_id, Error::StillLearning.into())
            }
            Learn => {
                // Learners can't distribute new shares to other learners
                Output::respond(
                    from,
                    request_id,
                    Error::CannotSpareAShare.into(),
                )
            }
        };
        (state.into(), output)
    }

    fn handle_response(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output) {
        use ResponseType::*;
        match response {
            InitAck | Share(_) => {
                let state = self.name();
                (
                    self.into(),
                    ApiError::UnexpectedResponse {
                        from,
                        state,
                        request_id,
                        msg: response.name(),
                    }
                    .into(),
                )
            }
            Pkg(pkg) => {
                // Tansition to `State::Learned`
                //
                // It doesn't matter who we received the response from, as it
                // must have been a peer we asked.
                //
                // TODO: We should check the rack_uuid if we add it to
                // `RequestType::InitLearner`  and save it.
                (LearnedState::new(pkg), Output::none())
            }
            Error(error) => {
                let state = self.name();
                (
                    self.into(),
                    ApiError::ErrorResponseReceived {
                        from,
                        state,
                        request_id,
                        error,
                    }
                    .into(),
                )
            }
        }
    }

    /// Check for expired learn attempts
    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output) {
        match &mut self.attempt {
            Some(attempt) => {
                if attempt.expired(common.clock, common.config.learn_timeout) {
                    if let Some(peer) = next_peer(&attempt.peer, &self.peers) {
                        attempt.peer = peer.clone();
                        attempt.start = self.clock;
                        (self.into(), Output::request(peer, RequestType::Learn))
                    } else {
                        // No peers to learn from
                        (self.into(), Output::none())
                    }
                }
            }
            None => {
                if let Some(peer) = self.peers.first() {
                    let state = LearningState {
                        attempt: Some(LearnAttempt {
                            peer: peer.clone(),
                            start: self.clock,
                        }),
                    }
                    .into();
                    (state, Output::request(peer.clone(), RequestType::Learn))
                } else {
                    // No peers to learn from
                    (self.into(), Output::none())
                }
            }
        }
    }
}
