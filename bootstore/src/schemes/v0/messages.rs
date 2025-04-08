// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messages sent between peers

use super::{LearnedSharePkg, RackUuid, Share, SharePkg};
use derive_more::From;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;
use std::net::SocketAddrV6;
use thiserror::Error;
use uuid::Uuid;

/// The first thing a peer does after connecting or accepting is to identify
/// themselves to the connected peer.
///
/// This message is interpreted at the peer (network) level, and not at the FSM level,
/// because it is used to associate IP addresses with [`Baseboard`]s.
///
/// Note that we include the address, which is totally spoofable here, so we can
/// test on localhost with multiple ports instead of different IPs.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Identify {
    pub id: Baseboard,
    pub addr: SocketAddrV6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub to: Baseboard,
    pub msg: Msg,
}

#[derive(From, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    Req(Request),
    Rsp(Response),
}

impl Msg {
    pub fn request_id(&self) -> Uuid {
        match self {
            Msg::Req(req) => req.id,
            Msg::Rsp(rsp) => rsp.request_id,
        }
    }
}

/// A request sent to a peer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    // A counter to uniquely match a request to a response for a given peer
    pub id: Uuid,
    pub type_: RequestType,
}

/// A response sent from a peer that matches a request with the same sequence
/// number
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub request_id: Uuid,
    pub type_: ResponseType,
}

/// A request from a peer to another peer over TCP
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestType {
    /// A rack initialization request informing the peer that it is a member of
    /// the initial trust quorum.
    Init(SharePkg),

    /// Request a share from a remote peer
    GetShare { rack_uuid: RackUuid },

    /// Get a [`LearnedSharePkg`] from a peer that was part of the rack
    /// initialization group
    Learn,
}

impl RequestType {
    pub fn name(&self) -> &'static str {
        match self {
            RequestType::Init(_) => "init",
            RequestType::GetShare { .. } => "get_share",
            RequestType::Learn => "learn",
        }
    }
}

/// A response to a request from a peer over TCP
#[derive(From, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseType {
    /// Response to [`RequestType::Init`]
    InitAck,

    /// Response to [`RequestType::GetShare`]
    Share(Share),

    /// Response to [`RequestType::Learn`]
    LearnPkg(LearnedSharePkg),

    /// An error response
    Error(MsgError),
}

impl ResponseType {
    pub fn name(&self) -> &'static str {
        match self {
            ResponseType::InitAck => "init_ack",
            ResponseType::Share(_) => "share",
            ResponseType::LearnPkg(_) => "learn_pkg",
            ResponseType::Error(_) => "error",
        }
    }
}

/// An error returned from a peer over TCP
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MsgError {
    #[error("already initialized")]
    AlreadyInitialized,

    #[error("not yet initialized")]
    NotInitialized,

    #[error("peer still trying to learn its share")]
    StillLearning,

    #[error("no shares available for learners")]
    CannotSpareAShare,

    #[error("rack uuid mismatch: expected: {expected}, got: {got}")]
    RackUuidMismatch { expected: RackUuid, got: RackUuid },
}
