// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messages sent between peers

use crate::trust_quorum::{LearnedSharePkgV0, SharePkgV0};
use derive_more::From;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use uuid::Uuid;

/// The first thing a peer does after connecting or accepting is to identify
/// themselves to the connected peer.
///
/// This message is interpreted at the peer (network) level, and not at the FSM level,
/// because it is used to associate IP addresses with [`Baseboard`]s.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Identify(Baseboard);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub to: Baseboard,
    pub msg: Msg,
}

#[derive(From, Debug, PartialEq, Serialize, Deserialize)]
pub enum Msg {
    Req(Request),
    Rsp(Response),
}

/// A request sent to a peer
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    // A counter to uniquely match a request to a response for a given peer
    pub id: Uuid,
    pub type_: RequestType,
}

/// A response sent from a peer that matches a request with the same sequence
/// number
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub request_id: Uuid,
    pub type_: ResponseType,
}

/// A request from a peer to another peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum RequestType {
    /// A rack initialization request informing the peer that it is a member of
    /// the initial trust quorum.
    Init(SharePkgV0),

    /// Initialize a peer as a Learner
    InitLearner,

    /// Request a share from a remote peer
    GetShare { rack_uuid: Uuid },

    /// Get a [`LearnedSharePkgV0`] from a peer that was part of the rack
    /// initialization group
    Learn,
}

/// A response to a request from a peer over TCP
#[derive(From, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResponseType {
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
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// The peer is already initialized as a member of the original group
    AlreadyInitialized { rack_uuid: Uuid },

    /// The peer has already learned it is a shared member of the group
    AlreadyLearned { rack_uuid: Uuid },

    /// The peer is already in the process of learning
    AlreadyLearning,

    /// The peer is not initialized yet
    NotInitialized,

    /// The peer is trying to learn its share
    StillLearning,

    /// The peer does not have any shares to hand out
    /// to learners
    CannotSpareAShare,

    /// Shares to hand to learners cannot be decrypted
    FailedToDecryptShares,

    /// A timeout that occurred while trying to recompute the rack secret
    TimeoutWaitingForShares,

    /// We could not reconstruct the rack secret even after retrieving enough
    /// valid shares.
    FailedToReconstructRackSecret,
}
