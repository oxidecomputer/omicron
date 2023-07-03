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

pub struct Request {
    id: Uuid,
    ty: RequestType,
    expiry: Instant,
}

/// Acknowledgement tracking for `RequestType::InitRack`.
pub struct InitAcks {
    expected: BTreeSet<Baseboard>,
    received: BTreeSet<Baseboard>,
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Share(Vec<u8>);

// Manually implemented to redact info
impl Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share").finish()
    }
}

/// Acknowledgement tracking for `RequestType::LoadRackSecret` and
/// `RequestType::Learn`
#[derive(Debug)]
pub struct ShareAcks {
    threshold: u8,
    received: BTreeMap<Baseboard, Share>,
}

#[derive(Debug)]
pub enum RequestType {
    /// A request from the caller of the Fsm API to initialize a rack
    ///
    /// This must only be called at one peer.
    InitRack { rack_uuid: Uuid },

    /// A request from the caller of the Fsm API to load a rack secret
    LoadRackSecret,

    /// A request from a peer to learn a new share
    /// This peer was not part of the initial membership group
    Learn { from: Baseboard },
}

/// A mechanism to manage all in flight requests
///
/// We expect very few requests at a time - on the order of one or two requests,
/// and so we just store them in a Vec.
pub struct RequestManager {
    config: Config,
    requests: Vec<Request>,
}

impl RequestManager {
    /// Create a new init_rack request and return the request id
    pub fn new_init_rack(&mut self, now: Instant, rack_uuid: Uuid) -> Uuid {
        let ty = RequestType::InitRack { rack_uuid };
        self.new_request(now, ty)
    }

    pub fn new_rack_secret_load(&mut self, now: Instant) -> Uuid {
        self.new_request(now, RequestType::LoadRackSecret)
    }

    pub fn new_learn(&mut self, now: Instant, from: Baseboard) -> Uuid {
        self.new_request(now, RequestType::Learn { from })
    }

    fn new_request(&mut self, now: Instant, ty: RequestType) -> Uuid {
        let expiry = now + self.config.rack_init_timeout;
        let id = Uuid::new_v4();
        let request = Request { id, ty, expiry };
        self.requests.push(request);
        id
    }
}
