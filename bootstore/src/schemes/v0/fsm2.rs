// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::{
    create_pkgs, ApiError, ApiOutput, Config2, Envelope, LearnedSharePkg, Msg,
    Request, RequestManager, RequestType, Share, SharePkg, TrackableRequest,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::time::{Duration, Instant};
use uuid::Uuid;

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

    config: Config2,

    /// Unique IDs of connected peers
    connected_peers: BTreeSet<Baseboard>,

    /// Manage all trackable broadcasts
    request_manager: RequestManager,
}

impl Fsm2 {
    /// Create a new FSM in `State::Uninitialized`
    pub fn new_uninitialized(id: Baseboard, config: Config2) -> Fsm2 {
        Fsm2 {
            state: State::Uninitialized,
            id,
            config,
            connected_peers: BTreeSet::new(),
            request_manager: RequestManager::new(config),
        }
    }

    /// Return any envelopes that need sending
    ///
    /// This must be called after any API callback
    pub fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> + '_ {
        self.request_manager.drain_elements()
    }

    /// This call is triggered locally on a single sled as a result of RSS
    /// running. It may only be called once, which is enforced by checking to see
    /// if we already are in `State::Uninitialized`.
    ///
    /// Persistence is required after a successful call to `init_rack`
    pub fn init_rack(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Result<(), ApiError> {
        let State::Uninitialized = self.state else {
            return Err(ApiError::RackAlreadyInitialized);
        };
        let total_members = initial_membership.len();
        let pkgs = create_pkgs(rack_uuid, initial_membership.clone())
            .map_err(|e| ApiError::RackInitFailed(e))?;
        let mut iter = pkgs.expose_secret().into_iter();
        let our_pkg = iter.next().unwrap().clone();

        // Move into an initialized state
        self.state = State::InitialMember { pkg: our_pkg };

        let packages: BTreeMap<Baseboard, SharePkg> = initial_membership
            .into_iter()
            .filter(|peer| *peer != self.id)
            .zip(iter.cloned())
            .collect();

        let _ = self.request_manager.new_init_rack_req(
            now,
            rack_uuid,
            packages.clone(),
            &self.connected_peers,
        );

        Ok(())
    }

    /// Initialize a node added after rack initialization
    ///
    /// Persistence is required after a successful call to `init_learner`
    pub fn init_learner(&mut self, now: Instant) -> Result<(), ApiError> {
        let State::Uninitialized = self.state else {
            return Err(ApiError::PeerAlreadyInitialized);
        };

        if let Some(to) = self.connected_peers.first() {
            let _ = self.request_manager.new_learn_sent_req(now, to.clone());
        }
        self.state = State::Learning;
        Ok(())
    }

    /// This call is triggered locally after RSS runs, in order to retrieve the
    /// `RackSecret` so that it can be used as input key material.
    ///
    /// if the rack secret has not already been loaded, then share retrieval
    /// will begin.
    pub fn load_rack_secret(&mut self, now: Instant) -> Result<(), ApiError> {
        match &self.state {
            State::Uninitialized => return Err(ApiError::RackNotInitialized),
            State::Learning { .. } => return Err(ApiError::StillLearning),
            State::InitialMember { pkg } => {
                let _ = self.request_manager.new_load_rack_secret_req(
                    now,
                    pkg.rack_uuid,
                    pkg.threshold,
                    &self.connected_peers,
                );
            }
            State::Learned { pkg } => {
                let _ = self.request_manager.new_load_rack_secret_req(
                    now,
                    pkg.rack_uuid,
                    pkg.threshold,
                    &self.connected_peers,
                );
            }
        }

        Ok(())
    }

    /// Periodic tick to check for request expiration
    ///
    /// Return any expired request errors mapped to their request id
    pub fn tick(
        &mut self,
        now: Instant,
    ) -> Result<(), BTreeMap<Uuid, ApiError>> {
        if let State::Uninitialized = &self.state {
            return Ok(());
        }
        let mut errors = BTreeMap::new();
        for (req_id, req) in self.request_manager.expired(now) {
            match req {
                TrackableRequest::InitRack { rack_uuid, acks, .. } => {
                    let unacked_peers = acks
                        .expected
                        .difference(&acks.received)
                        .cloned()
                        .collect();
                    errors.insert(
                        req_id,
                        ApiError::RackInitTimeout { unacked_peers },
                    );
                }
                TrackableRequest::LoadRackSecret { rack_uuid, acks } => {
                    errors.insert(req_id, ApiError::RackSecretLoadTimeout);
                }
                TrackableRequest::LearnReceived { .. } => {
                    // Nothing to do here, as these are requests from messages
                    // and not api requests
                }
                TrackableRequest::LearnSent { to } => {
                    if let State::Learning = &self.state {
                        // Issue the next request
                        if let Some(next_peer) = self.next_peer(&to) {
                            let _ = self
                                .request_manager
                                .new_learn_sent_req(now, next_peer);
                        }
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// A peer has been connected.
    ///
    /// Send any necessary messages required by pending requesets.
    pub fn on_connected(&mut self, now: Instant, peer_id: Baseboard) {
        if let State::Learning = &self.state {
            if !self.request_manager.has_learn_sent_req() {
                // This is the first peer we've seen in the learning state, so try
                // to learn from it.
                let request_id = self
                    .request_manager
                    .new_learn_sent_req(now, peer_id.clone());
            }
        }
        self.request_manager.on_connected(&peer_id);
        self.connected_peers.insert(peer_id);
    }

    /// Select the next peer in a round-robin fashion
    pub fn next_peer(&self, current: &Baseboard) -> Option<Baseboard> {
        let mut iter = self.connected_peers.range(current..);
        let matched = iter.next() == Some(current);
        iter.next()
            .filter(|_| matched)
            .or_else(|| self.connected_peers.first())
            .cloned()
    }
}
