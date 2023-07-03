// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::share_pkg::{LearnedSharePkg, SharePkg};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Configuration of the FSM
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub learn_timeout: Duration,
    pub rack_init_timeout: Duration,
    pub rack_secret_request_timeout: Duration,
}

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(pub usize);

pub enum State {
    Uninitialized,
    InitialMember { pkg: SharePkg },
    Learning,
    Learned { pkg: LearnedSharePkg },
}

pub struct Fsm2 {
    /// The current state of this peer
    state: State,
    /// Unique IDs of this peer
    id: Baseboard,

    config: Config,

    /// Unique IDs of connected peers
    connected_peers: BTreeSet<Baseboard>,

    /// The approximate wall-clock time
    ///
    /// This is updated via API calls, and not read directly.
    /// Doing it this way allows deterministic tests.
    clock: Instant,
}

impl Fsm2 {}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Share(Vec<u8>);

// Manually implemented to redact info
impl Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share").finish()
    }
}

/// Acknowledgement tracking for `RequestType::InitRack`.
#[derive(Debug, Default)]
pub struct InitAcks {
    expected: BTreeSet<Baseboard>,
    received: BTreeSet<Baseboard>,
}

/// Acknowledgement tracking for `RequestType::LoadRackSecret` and
/// `RequestType::Learn`
#[derive(Debug)]
pub struct ShareAcks {
    threshold: u8,
    received: BTreeMap<Baseboard, Share>,
}

impl ShareAcks {
    pub fn new(threshold: u8) -> ShareAcks {
        ShareAcks { threshold, received: BTreeMap::new() }
    }
}

#[derive(Debug)]
pub enum Request {
    /// A request from the caller of the Fsm API to initialize a rack
    ///
    /// This must only be called at one peer.
    InitRack { rack_uuid: Uuid, acks: InitAcks },

    /// A request from the caller of the Fsm API to load a rack secret
    LoadRackSecret { acks: ShareAcks },

    /// A request from a peer to learn a new share
    /// This peer was not part of the initial membership group
    Learn { from: Baseboard, acks: ShareAcks },
}

/// A mechanism to manage all in flight requests
///
/// We expect very few requests at a time - on the order of one or two requests.
pub struct RequestManager {
    config: Config,
    requests: BTreeMap<Uuid, Request>,
    expiry_to_id: BTreeMap<Instant, Uuid>,
}

impl RequestManager {
    pub fn new_init_rack(&mut self, now: Instant, rack_uuid: Uuid) -> Uuid {
        let req = Request::InitRack { rack_uuid, acks: InitAcks::default() };
        self.new_request(now, req)
    }

    pub fn new_load_rack_secret(
        &mut self,
        now: Instant,
        threshold: u8,
    ) -> Uuid {
        self.new_request(
            now,
            Request::LoadRackSecret { acks: ShareAcks::new(threshold) },
        )
    }

    pub fn new_learn(
        &mut self,
        now: Instant,
        threshold: u8,
        from: Baseboard,
    ) -> Uuid {
        self.new_request(
            now,
            Request::Learn { from, acks: ShareAcks::new(threshold) },
        )
    }

    /// Return any expired requests
    pub fn expired(&mut self, now: Instant) -> Vec<Request> {
        let mut expired = vec![];
        while let Some((expiry, request_id)) = self.expiry_to_id.pop_last() {
            if expiry > now {
                expired.push(self.requests.remove(&request_id).unwrap());
            } else {
                // Put the last request back. We are done.
                self.expiry_to_id.insert(expiry, request_id);
                break;
            }
        }
        expired
    }

    fn new_request(&mut self, now: Instant, request: Request) -> Uuid {
        let expiry = now + self.config.rack_init_timeout;
        let id = Uuid::new_v4();
        self.requests.insert(id, request);
        self.expiry_to_id.insert(expiry, id);
        id
    }

    /// Return true if initialization completed, false otherwise
    ///
    /// If initialization completed, the request will be deleted.
    ///
    /// We drop the ack if the request_id is not found. This could be a lingering
    /// old ack from when the rack was reset to clean up after a prior failed rack
    /// init.
    pub fn on_init_ack(&mut self, from: Baseboard, request_id: Uuid) -> bool {
        if let Some(Request::InitRack { acks, .. }) =
            self.requests.get_mut(&request_id)
        {
            acks.received.insert(from);
            if acks.received == acks.expected {
                self.requests.remove(&request_id);
                return true;
            }
        }

        false
    }

    /// Return the `Some(request)` if a threshold of acks has been received.
    /// Otherwise return `None`
    pub fn on_share(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        share: Share,
    ) -> Option<Request> {
        let acks = match self.requests.get_mut(&request_id) {
            Some(Request::LoadRackSecret { acks }) => acks,
            Some(Request::Learn { acks, .. }) => acks,
            _ => return None,
        };
        acks.received.insert(from, share);
        if acks.received.len() == acks.threshold as usize {
            self.requests.remove(&request_id)
        } else {
            None
        }
    }
}
