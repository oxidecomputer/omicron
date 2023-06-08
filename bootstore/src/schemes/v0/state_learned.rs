// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Learned`

use std::collections::BTreeMap;

use crate::trust_quorum::SharePkgV0;

use super::fsm::{next_peer, StateHandler};
use super::fsm_output::Output;
use super::messages::{Request, RequestType, Response, ResponseType};
use super::state::{FsmCommonData, InitialMemberState, State};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug)]
pub struct LearnedState {
    pub pkg: LearnedSharePkgV0,
    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl LearnedState {
    pub fn new(pkg: LearnedSharePkgV0) -> Self {
        LearnedState { pkg, rack_secret_state: None }
    }
}

impl StateHandler for LearnedState {
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
                let rack_uuid = pkg.rack_uuid;
                Output::respond(
                    from,
                    request_id,
                    Error::AlreadyLearned { rack_uuid }.into(),
                )
            }
            InitLearner => {
                // Idempotent, since we already learned
                // TODO: Should we send a rack_uuid with this messsage
                // and check it ?
                Output::respond(from, request_id, ResponseType::InitAck)
            }
            GetShare { rack_uuid } => {
                if rack_uuid != self.pkg.rack_uuid {
                    Output::respond(
                        from,
                        request_id,
                        Error::RackUuidMismatch {
                            expected: self.pkg.rack_uuid,
                            got: rack_uuid,
                        },
                    )
                } else {
                    Output::respond(
                        from,
                        request_id,
                        ResponseType::Share(self.pkg.share.clone()),
                    )
                }
            }
            Learn => Output::respond(
                from,
                request_id,
                Error::CannotSpareAShare.into(),
            ),
        };

        // This is a terminal state
        (self.into(), output)
    }
}
