// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messages sent between peers

use crate::trust_quorum::{LearnedSharePkgV0, SharePkgV0};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    to: Baseboard,
    msg: Msg,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Msg {
    Req(Request),
    Rsp(Response),
}

/// A request from a peer to another peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// The first thing a peer does is identify themselves to the connected peer
    ///
    /// The Baseboard is mapped to the connection after this.
    Identify(Baseboard),

    /// A rack initialization request informing the peer that it is a member of
    /// the initial trust quorum.
    Init(SharePkgV0),

    /// Request a share from a remote peer
    GetShare { rack_uuid: Uuid },

    /// Get a [`LearnedSharePkgV0`] from a peer that was part of the rack
    /// initialization group
    ///
    /// `Baseboard` uniquely identifies the requesting sled.
    Learn(Baseboard),
}

/// A response to a request from a peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// Response to [`Request::Identify`]
    IdentifyAck(Baseboard),

    /// Response to [`Request::Init`]
    InitAck,

    /// Response to [`Request::GetShare`]
    Share(Vec<u8>),

    /// Response to [`Request::Learn`]
    Pkg(LearnedSharePkgV0),

    /// An error response
    Error(Error),
}

/// An error returned from a peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// The peer is already initialized
    AlreadyInitialized { rack_uuid: Uuid },

    /// The peer is not initialized yet
    NotInitialized,

    /// The peer does not have any shares to hand out
    /// to learners
    NoShares,
}
