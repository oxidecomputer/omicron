// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Learned`

use crate::schemes::v0::fsm_output::ApiError;
use crate::trust_quorum::LearnedSharePkgV0;

use super::fsm::StateHandler;
use super::fsm_output::Output;
use super::messages::{Error, RequestType, ResponseType};
use super::state::{FsmCommonData, RackSecretState, State};
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

    pub fn name(&self) -> &'static str {
        "learned"
    }
}

impl StateHandler for LearnedState {
    fn handle_request(
        self,
        _common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (State, Output) {
        use RequestType::*;
        let output = match request {
            Init(_) => {
                let rack_uuid = self.pkg.rack_uuid;
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
                        }
                        .into(),
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
        self,
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
            Share(share) => {
                // The RackSecret state needs this deadline to know how long to
                // keep the secret
                // TODO: Use a separate timeout for this purpose?
                let rack_secret_expiry =
                    common.clock + common.config.rack_secret_request_timeout;

                common.rack_secret_state.on_share(
                    from,
                    request_id,
                    share,
                    rack_secret_expiry,
                )
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

    fn tick(self, common: &mut FsmCommonData) -> (State, Output) {
        // Check for rack secret request expiry
        (self.into(), common.rack_secret_state.on_tick(common.clock))
    }

    fn on_connect(
        &mut self,
        common: &mut FsmCommonData,
        peer: Baseboard,
    ) -> Output {
        // TODO: If we are collecting shares, then try to get one from this peer
        // as well.
        Output::none()
    }

    fn on_disconnect(
        &mut self,
        common: &mut FsmCommonData,
        peer: Baseboard,
    ) -> Output {
        // Nothing to do here
        Output::none()
    }
}
