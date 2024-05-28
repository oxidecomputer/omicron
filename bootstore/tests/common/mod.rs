// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code shared among integration tests
pub mod generators;

use self::generators::TICK_TIMEOUT;
use crate::Uuid;
use assert_matches::assert_matches;
use bootstore::schemes::v0::{
    ApiError, Envelope, Fsm, FsmConfig, Msg, MsgError, RackUuid, Request,
    RequestType, Response, ResponseType,
};
use sled_hardware_types::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

// A tracked request issued to the SUT
pub struct TestRequest {
    pub start: Instant,
    pub acks: BTreeSet<Baseboard>,
}

impl TestRequest {
    pub fn new(start: Instant) -> TestRequest {
        TestRequest { start, acks: BTreeSet::new() }
    }
}

// Test state shared among learner and rack-coordinator proptests
pub struct CommonTestState {
    // The Fsm under test
    pub sut: Fsm,

    // The unique id of the initialized rack
    pub rack_uuid: RackUuid,

    // The generated configuration
    pub config: FsmConfig,

    // IDs of all initial members
    pub initial_members: BTreeSet<Baseboard>,

    // Any peers connected to the SUT Fsm
    pub connected_peers: BTreeSet<Baseboard>,

    // The current time at the SUT Fsm
    pub now: Instant,

    // `Fsm::load_rack_secret` requests triggered by an `Action::LoadRackSecret`
    pub load_rack_secret_requests: BTreeMap<Uuid, TestRequest>,

    // Rack secret threshold
    pub threshold: usize,
}

impl CommonTestState {
    pub fn new(
        sut: Fsm,
        initial_members: BTreeSet<Baseboard>,
        config: FsmConfig,
        rack_uuid: RackUuid,
    ) -> CommonTestState {
        let threshold = initial_members.len() / 2 + 1;
        CommonTestState {
            sut,
            rack_uuid,
            config,
            initial_members,
            connected_peers: BTreeSet::new(),
            now: Instant::now(),
            load_rack_secret_requests: BTreeMap::new(),
            threshold,
        }
    }
    pub fn load_rack_secret(&mut self) -> Vec<Envelope> {
        let request_id = self.sut.load_rack_secret(self.now).unwrap();
        self.load_rack_secret_requests
            .insert(request_id, TestRequest::new(self.now));
        let envelopes = self.sut.drain_envelopes().collect();
        self.expect_get_share_broadcast(&envelopes);
        envelopes
    }

    pub fn expect_get_share_broadcast(&self, envelopes: &Vec<Envelope>) {
        assert_eq!(self.connected_peers.len(), envelopes.len());
        for envelope in envelopes {
            assert!(self.connected_peers.contains(&envelope.to));
            assert_matches!(
                &envelope.msg,
                &Msg::Req(Request {
                    type_: RequestType::GetShare { rack_uuid },
                    ..
                }) if rack_uuid == self.rack_uuid
            );
        }
    }

    pub fn connect(
        &mut self,
        peer_id: Baseboard,
    ) -> (Result<(), ApiError>, Vec<Envelope>) {
        let result = self.sut.on_connected(self.now, peer_id.clone());
        let envelopes = self.sut.drain_envelopes().collect();
        self.connected_peers.insert(peer_id);
        (result, envelopes)
    }

    pub fn disconnect(&mut self, peer_id: Baseboard) {
        self.sut.on_disconnected(&peer_id);
        self.connected_peers.remove(&peer_id);

        // There should be no envelopes sent on a disconnect
        assert_eq!(None, self.sut.drain_envelopes().next());
    }

    pub fn tick(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.now += TICK_TIMEOUT;
            if let Err(errors) = self.sut.tick(self.now) {
                // The only possible error is a timeout
                // Ensure the request exists and it should have timed out
                for (request_id, error) in errors {
                    assert_eq!(error, ApiError::RackSecretLoadTimeout);
                    let test_req = &self.load_rack_secret_requests[&request_id];
                    let expiry = test_req.start
                        + self.config.rack_secret_request_timeout;
                    assert!(expiry < self.now);
                    // Remove the tracking req from test state
                    self.load_rack_secret_requests.remove(&request_id);
                }
            }
        }
    }

    pub fn get_share(&mut self, peer_id: Baseboard) {
        let id = Uuid::new_v4();
        let req = Request {
            id,
            type_: RequestType::GetShare { rack_uuid: self.rack_uuid },
        }
        .into();
        let res = self.sut.handle_msg(self.now, peer_id.clone(), req);
        assert_eq!(res, Ok(None));
        let mut iter = self.sut.drain_envelopes();
        let envelope = iter.next().unwrap();
        assert_matches!(envelope, Envelope {
            to,
            msg: Msg::Rsp(Response {
                request_id,
                type_: ResponseType::Share(_)
            })
        } if to == peer_id && id == request_id);

        // There's only one envelope
        assert!(iter.next().is_none());
    }

    pub fn get_share_fail(&mut self, peer_id: Baseboard) {
        let id = Uuid::new_v4();
        let bad_rack_uuid = Uuid::new_v4().into();
        let req = Request {
            id,
            type_: RequestType::GetShare { rack_uuid: bad_rack_uuid },
        }
        .into();
        let res = self.sut.handle_msg(self.now, peer_id.clone(), req);
        assert_eq!(res, Ok(None));
        let mut iter = self.sut.drain_envelopes();
        let envelope = iter.next().unwrap();
        assert_matches!(envelope, Envelope {
            to,
            msg: Msg::Rsp(Response {
                request_id,
                type_: ResponseType::Error(
                    MsgError::RackUuidMismatch { expected , got  }
                )
            })
        } if to == peer_id &&
             id == request_id &&
             expected == self.rack_uuid &&
             got == bad_rack_uuid
        );

        // There's only one envelope
        assert!(iter.next().is_none());
    }

    pub fn handle_error_response(&mut self, peer_id: Baseboard, err: MsgError) {
        let rsp =
            Response { request_id: Uuid::new_v4(), type_: err.into() }.into();
        let output = self.sut.handle_msg(self.now, peer_id, rsp);
        assert_matches!(output, Err(ApiError::ErrorResponseReceived { .. }));
        assert!(self.sut.drain_envelopes().next().is_none());
    }
}
