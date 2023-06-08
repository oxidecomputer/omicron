// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Learned`

use std::collections::BTreeMap;

use crate::schemes::v0::fsm_output::ApiError;
use crate::trust_quorum::SharePkgV0;

use super::fsm::StateHandler;
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

    fn handle_response(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output) {
        use ResponseType::*;
        let output = match response {
            InitAck => ApiError::UnexpectedResponse {
                from,
                state: self.name(),
                request_id,
                msg: response.name(),
            }
            .into(),
            Share(Share) => {
                let rack_secret_result =
                    match RackSecretState::combine_shares_if_necessary(
                        &mut self.rack_secret_state,
                        from,
                        share,
                        &self.pkg.share_digests,
                    ) {
                        Ok(rack_secret_result) => rack_secret_result,
                        Err(output) => return output,
                    };

                // Did computation of the rack secret succeed or fail?
                //
                // If we got to this point it means we at least had enough shares to try
                // to reconstruct the rack secret.

                // If we have a pending API request for the rack secret we can
                // resolve it now.
                common
                    .resolve_pending_api_request(
                        rack_secret_result.as_ref().ok(),
                    )
                    .into()
            }
            Pkg(_) => ApiError::UnexpectedResponse {
                from,
                state: self.name(),
                request_id,
                msg: response.name(),
            }
            .into(),
            Error(error) => ApiError::ErrorResponseReceived {
                from,
                state: self.name(),
                request_id,
                error,
            }
            .into(),
        };

        // This is a terminal state
        (self.into(), output)
    }

    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output) {
        // Check for rack secret request expiry
        if let Some(start) = common.pending_api_rack_secret_request {
            if common.clock.saturating_sub(start)
                > common.config.rack_secret_request_timeout
            {
                common.pending_api_rack_secret_request = None;
                return (self.into(), ApiError::RackSecretLoadTimeout.into());
            }
        }

        (self.into(), Output::none())
    }
}
