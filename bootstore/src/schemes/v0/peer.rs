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
