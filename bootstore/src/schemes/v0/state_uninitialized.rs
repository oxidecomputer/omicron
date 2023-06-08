// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Uninitialized`

use std::collections::BTreeMap;

use crate::trust_quorum::SharePkgV0;

use super::fsm::StateHandler;
use super::fsm_output::Output;
use super::messages::{Request, RequestType, Response, ResponseType};
use super::state::{FsmCommonData, InitialMemberState, State};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug)]
pub struct UninitializedState {}

impl UninitializedState {
    fn name(&self) -> &'static str {
        "uninitialized"
    }
}

impl StateHandler for UninitializedState {
    fn handle_request(
        mut self,
        common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (State, Output) {
        use RequestType::*;
        match request {
            Init(SharePkgV0) => (
                InitialMemberState::new(pkg, BTreeMap::new()).into(),
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                ),
            ),
            InitLearner => (
                State::Learning(None).into(),
                Output::persist_and_respond(
                    from,
                    request_id,
                    ResponseType::InitAck,
                ),
            ),
            GetShare { .. } => (
                self.into(),
                Output::respond(from, request_id, Error::NotInitialized.into()),
            ),
            Learn => (
                self.into(),
                Output::respond(from, request_id, Error::NotInitialized.into()),
            ),
        }
    }

    fn handle_response(
        mut self,
        _common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> Output {
        // We don't handle any responses in `UninitializedState`
        // Inform the user of the FSM about unexpected messages so this can be
        // logged if desired
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

    fn tick(&mut self, common: &mut FsmCommonData) -> Output {
        (self.into(), Output::none())
    }
}
