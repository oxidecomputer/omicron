// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Uninitialized`

use std::collections::BTreeMap;

use super::fsm::StateHandler;
use super::fsm_output::{ApiError, Output};
use super::messages::{Error, RequestType, ResponseType};
use super::state::{FsmCommonData, State};
use super::state_initial_member::InitialMemberState;
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
        self,
        _common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        request: RequestType,
    ) -> (State, Output) {
        use RequestType::*;
        match request {
            Init(pkg) => (
                InitialMemberState::new(pkg, BTreeMap::new()).into(),
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
        self,
        _common: &mut FsmCommonData,
        from: Baseboard,
        request_id: Uuid,
        response: ResponseType,
    ) -> (State, Output) {
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

    fn tick(self, _common: &mut FsmCommonData) -> (State, Output) {
        (self.into(), Output::none())
    }

    fn on_connect(
        &mut self,
        _common: &mut FsmCommonData,
        _peer: Baseboard,
    ) -> Output {
        Output::none()
    }

    fn on_disconnect(
        &mut self,
        _common: &mut FsmCommonData,
        _peer: Baseboard,
    ) -> Output {
        Output::none()
    }
}
