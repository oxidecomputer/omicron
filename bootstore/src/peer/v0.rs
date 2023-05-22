// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Scheme V0 of a bootstore peer

use crate::trust_quorum::{LearnedSharePkgV0, SharePkgV0};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::net::SocketAddrV6;
use std::{collections::HashSet, time::Duration};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

enum SharePkg {
    Initial(SharePkgV0),
    Learned(LearnedSharePkgV0),
}

impl SharePkg {
    pub fn rack_uuid(&self) -> Uuid {
        match self {
            SharePkg::Initial(pkg) => pkg.rack_uuid,
            SharePkg::Learned(pkg) => pkg.rack_uuid,
        }
    }
}

/// An error when RSS tries to initialize the rack
pub enum RackInitError {
    AlreadyInitialized { rack_uuid: Uuid },
}

/// A message for interacting with the local [`Peer`] via a [`PeerHandle`]
enum LocalPeerRequest {
    /// Generate a rack secret and distribute [`SharePkgV0`] to `peers`
    InitializeRack {
        rack_uuid: Uuid,
        peers: HashSet<SocketAddrV6>,

        /// How long we wait for peers to accept initialization.
        /// If all peers do not accept initialization we send back a `RackInitError`.
        ///
        /// If any peer sends back an error we report that immediately.
        timeout: Duration,
        responder: oneshot::Sender<Result<(), RackInitError>>,
    },

    /// Update the set of known peers
    UpdatePeers { peers: HashSet<SocketAddrV6> },

    /// This peer is being added to an existing trust quorum. It should try to
    /// retrieve a `LearnedSharePkgV0` from an existing peer.
    Learn { rack_uuid: Uuid, peers: HashSet<SocketAddrV6> },
}

/// A mechanism for interacting with a local sled peer
pub struct PeerHandle {
    tx: mpsc::Sender<LocalPeerRequest>,
}

/// A participant in trust quorum
pub struct Peer {
    listen_addr: SocketAddrV6,
    peers: HashSet<SocketAddrV6>,
    rx: mpsc::Receiver<LocalPeerRequest>,
    pkg: Option<SharePkg>,
}

impl Peer {
    pub fn new(listen_addr: SocketAddrV6) -> (PeerHandle, Peer) {
        // Only one request at a time.
        let (tx, rx) = mpsc::channel(1);

        let handle = PeerHandle { tx };
        let peer = Peer { listen_addr, peers: HashSet::new(), rx, pkg: None };

        (handle, peer)
    }

    // RSS on the scrimlet is telling this peer to initialize the trust quorum
    async fn init(
        &mut self,
        rack_uuid: Uuid,
        peers: HashSet<SocketAddrV6>,
        timeout: Duration,
    ) -> Result<(), RackInitError> {
        if let Some(pkg) = &self.pkg {
            return Err(RackInitError::AlreadyInitialized {
                rack_uuid: pkg.rack_uuid(),
            });
        }

        // TODO: Connect to all peers and initialize them

        Ok(())
    }

    /// This peer is not part of the original trust quorum group
    /// Try to learn from an existing peer
    async fn learn(&mut self, rack_uuid: Uuid, peers: HashSet<SocketAddrV6>) {}
}

/// A header for messages sent between peers over TCP
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct MsgHeader {
    // The version of the protocol for the bootstore v0 scheme
    // We don't intend to evolve the protocol, but we may end up having
    // to add new messages before we upgrade to the bootstore v1 scheme
    version: u32,

    // The size of the Msg to follow
    size: u32,
}

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
