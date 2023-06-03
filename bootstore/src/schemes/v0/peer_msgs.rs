// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Messages sent between peers

/// A header for messages sent between peers over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MsgHeader {
    // The version of the bootstore protocol
    scheme: u32,
    // The version of the protocol for given bootstore scheme
    // Each scheme has independently numbered protocol versions
    // It's probably easiest to actually run the schemes themselves
    // on different ports and switch over to the new one when ready.
    version: u32,

    // The size of the Msg to follow
    size: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Msg {}

/// A request from a peer to another peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    /// A rack initialization request informing the peer that it is a member of
    /// the initial trust quorum.
    Init(SharePkgV0),

    /// Request a share from a remote peer
    GetShare { rack_uuid: Uuid, epoch: u32 },

    /// Get a [`LearnedSharePkgV0`] from a peer that was part of the rack
    /// initialization group
    ///
    /// `Baseboard` uniquely identifies the requesting sled.
    Learn(Baseboard),
}

/// A response to a request from a peer over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
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
