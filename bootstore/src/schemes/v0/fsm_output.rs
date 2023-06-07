// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Output related types for V0 protocol state machine methods

use super::messages::{
    Envelope, Error, Msg, Request, RequestType, Response, ResponseType,
};
use crate::trust_quorum::{RackSecret, TrustQuorumError};
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// Output from a a call to [`Fsm::Handle`]
pub struct Output {
    /// Possible state that needs persisting before any messages are sent
    pub persist: bool,

    /// Messages wrapped with a destination
    pub envelopes: Vec<Envelope>,

    // A reply to the user of the FSM API
    pub api_output: Option<ApiOutput>,
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

    pub fn rack_init_timeout(unacked_peers: BTreeSet<Baseboard>) -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackInitTimeout { unacked_peers }),
        }
    }

    pub fn rack_init_complete() -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackInitComplete),
        }
    }

    pub fn rack_already_initialized() -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackAlreadyInitialized),
        }
    }

    pub fn rack_not_initialized() -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackNotInitialized),
        }
    }

    pub fn rack_init_failed(err: TrustQuorumError) -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::RackInitFailed(err)),
        }
    }

    pub fn log<S: Into<String>>(level: LogLevel, msg: S) -> Output {
        Output {
            persist: false,
            envelopes: vec![],
            api_output: Some(ApiOutput::Log(level, msg.into())),
        }
    }
}

/// The caller of the API (aka the peer/network layer will sometimes need to get
/// messages delivered to it, such as when rack initialization has completed. We
/// provide this information in `Output::api_output`.
pub enum ApiOutput {
    RackInitComplete,
    RackInitTimeout {
        unacked_peers: BTreeSet<Baseboard>,
    },

    // Return the rack secret
    RackSecret(RackSecret),

    /// Rack initialization was already run once.
    RackAlreadyInitialized,

    RackInitFailed(TrustQuorumError),

    /// The rack must be initialized before the rack secret can be loaded
    RackNotInitialized,

    // A timeout when retreiving the rack secret
    RackSecretLoadTimeout,

    // We don't log inside the FSM, we return logs to the caller as part of our
    // "No IO in the FSM" paradigm
    Log(LogLevel, String),
}
