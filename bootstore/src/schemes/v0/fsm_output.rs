// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Output related types for V0 protocol state machine methods

use super::messages::{
    Envelope, Error, Request, RequestType, Response, ResponseType,
};
use crate::trust_quorum::{RackSecret, TrustQuorumError};
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

/// Output from a a call to [`Fsm::Handle`]
pub struct Output {
    /// Possible state that needs persisting before any messages are sent
    pub persist: bool,

    /// Messages wrapped with a destination
    pub envelopes: Vec<Envelope>,

    // A reply to the user of the FSM API
    pub api_output: Option<Result<ApiOutput, ApiError>>,
}

impl Output {
    pub fn none() -> Output {
        Output { persist: false, envelopes: vec![], api_output: None }
    }

    // Send a request directly to a peer
    pub fn request(to: Baseboard, type_: RequestType) -> Output {
        let request = Request { id: Uuid::new_v4(), type_ };
        Output {
            persist: false,
            envelopes: vec![Envelope { to, msg: request.into() }],
            api_output: None,
        }
    }

    // Return a response directly to a peer that doesn't require persistence
    pub fn respond(
        to: Baseboard,
        request_id: Uuid,
        type_: ResponseType,
    ) -> Output {
        let response = Response { request_id, type_ };
        Output {
            persist: false,
            envelopes: vec![Envelope { to, msg: response.into() }],
            api_output: None,
        }
    }

    // Indicate to the caller that state must be perisisted and then a response
    // returned to the peer.
    pub fn persist_and_respond(
        to: Baseboard,
        request_id: Uuid,
        type_: ResponseType,
    ) -> Output {
        let response = Response { request_id, type_ };
        Output {
            persist: true,
            envelopes: vec![Envelope { to, msg: response.into() }],
            api_output: None,
        }
    }
}

impl From<ApiOutput> for Output {
    fn from(value: ApiOutput) -> Self {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(Ok(value)),
        }
    }
}

impl From<ApiError> for Output {
    fn from(err: ApiError) -> Self {
        Output { persist: false, envelopes: vec![], api_output: Some(Err(err)) }
    }
}

impl From<Option<Result<ApiOutput, ApiError>>> for Output {
    fn from(value: Option<Result<ApiOutput, ApiError>>) -> Self {
        Output { persist: false, envelopes: vec![], api_output: value }
    }
}

/// Errors returned to the FSM caller not to a peer FSM in a message
// TODO: Use thiserror
#[derive(Debug)]
pub enum ApiError {
    RackInitTimeout {
        unacked_peers: BTreeSet<Baseboard>,
    },
    /// Rack initialization was already run once.
    RackAlreadyInitialized,

    RackInitFailed(TrustQuorumError),

    /// Peer can only be initialized when in `State::Uninitialized`
    PeerAlreadyInitialized,

    /// The rack must be initialized before the rack secret can be loaded
    RackNotInitialized,

    /// A timeout when retreiving the rack secret
    RackSecretLoadTimeout,

    // The API user tried to load a rack secret before the learner was done
    // getting its share
    StillLearning,

    /// Share digest does not match what's in our package
    InvalidShare {
        from: Baseboard,
    },

    /// We could not reconstruct the rack secret even after retrieving enough
    /// valid shares.
    FailedToReconstructRackSecret,

    /// Unexpected response received
    UnexpectedRequest {
        from: Baseboard,
        state: &'static str,
        request_id: Uuid,
        msg: &'static str,
    },

    /// Unexpected response received
    UnexpectedResponse {
        from: Baseboard,
        state: &'static str,
        request_id: Uuid,
        msg: &'static str,
    },

    // Error response received from a peer request
    ErrorResponseReceived {
        from: Baseboard,
        state: &'static str,
        request_id: Uuid,
        error: Error,
    },
}

/// The caller of the API (aka the peer/network layer will sometimes need to get
/// messages delivered to it, such as when rack initialization has completed. We
/// provide this information in `Output::api_output`.
pub enum ApiOutput {
    RackInitComplete,
    RackSecret(RackSecret),
}
