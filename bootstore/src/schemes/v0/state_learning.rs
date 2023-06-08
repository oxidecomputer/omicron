// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Uninitialized`

use std::collections::BTreeMap;

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
                    *state = State::Learning(Some(LearnAttempt {
                        peer: peer.clone(),
                        start: self.clock,
                    }));
                    (
                        self.into(),
                        Output::request(peer.clone(), RequestType::Learn),
                    )
                } else {
                    (self.into(), Output::none())
                }
                // No peers to learn from
            }
        }
    }
}
