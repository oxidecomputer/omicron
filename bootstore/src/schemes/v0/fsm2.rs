// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::share_pkg::{create_pkgs, LearnedSharePkg, SharePkg};
use super::{ApiError, ApiOutput, Envelope, Msg, Request, RequestType};
use secrecy::ExposeSecret;
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

    /// Manage all trackable broadcasts
    request_manager: RequestManager,
}

impl Fsm2 {
    /// Create a new FSM in `State::Uninitialized`
    pub fn new_uninitialized(id: Baseboard, config: Config) -> Fsm2 {
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
    pub fn envelopes(&mut self) -> impl Iterator<Item = Envelope> + '_ {
        self.request_manager.envelopes.drain(..)
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

/// A mechanism to track in flight requests
///
/// Manages expiry, acknowledgment, and retries
#[derive(Debug)]
pub enum TrackableRequest {
    /// A request from the caller of the Fsm API to initialize a rack
    ///
    /// This must only be called at one peer, exactly once. That peer
    /// will be in `InitialMember` state.
    InitRack {
        rack_uuid: Uuid,
        packages: BTreeMap<Baseboard, SharePkg>,
        acks: InitAcks,
    },

    /// A request from the caller of the Fsm API to load a rack secret
    ///
    /// Only peers in `InitialMember` or `Learned` state can load rack secrets
    LoadRackSecret { rack_uuid: Uuid, acks: ShareAcks },

    /// A request received from a peer to learn a new share
    ///
    /// This request results in share gathering so "extra" shares can be
    /// decrypted via a key derived from the rack secret and one of them
    /// distributed to the learner.
    ///
    /// Only peers in `InitialMember` state can respond successfully
    LearnReceived { rack_uuid: Uuid, from: Baseboard, acks: ShareAcks },

    /// A request sent from a peer in `Learning` state to another peer
    /// to learn a key share.
    LearnSent { to: Baseboard },
}

/// A mechanism to manage all in flight requests
///
/// We expect very few requests at a time - on the order of one or two requests.
pub struct RequestManager {
    config: Config,
    requests: BTreeMap<Uuid, TrackableRequest>,
    expiry_to_id: BTreeMap<Instant, Uuid>,

    /// Messages that need sending to other peers.
    ///
    /// These should be drained on each API call.
    envelopes: Vec<Envelope>,
}

impl RequestManager {
    pub fn new(config: Config) -> RequestManager {
        RequestManager {
            config,
            requests: BTreeMap::new(),
            expiry_to_id: BTreeMap::new(),
            envelopes: vec![],
        }
    }

    /// Track a new `Fsm::init_rack` api request and broadcast a
    /// `RequestType::Init` to connected peers.
    pub fn new_init_rack_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        packages: BTreeMap<Baseboard, SharePkg>,
        connected_peers: &BTreeSet<Baseboard>,
    ) -> Uuid {
        let expiry = now + self.config.rack_init_timeout;
        let req = TrackableRequest::InitRack {
            rack_uuid,
            packages: packages.clone(),
            acks: InitAcks::default(),
        };
        let request_id = self.new_request(expiry, req);

        // Send a `Request::Init` to each connected peer in the initial group
        let iter = packages.into_iter().filter_map(|(to, pkg)| {
            if connected_peers.contains(&to) {
                Some(Envelope {
                    to,
                    msg: Request {
                        id: request_id,
                        type_: RequestType::Init(pkg),
                    }
                    .into(),
                })
            } else {
                None
            }
        });
        self.envelopes.extend(iter);
        request_id
    }

    /// Track a new `Fsm::load_rack_secret` api request and broadcast a
    /// `RequestType::GetShare` to connected peers.
    pub fn new_load_rack_secret_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        threshold: u8,
        connected_peers: &BTreeSet<Baseboard>,
    ) -> Uuid {
        let expiry = now + self.config.rack_secret_request_timeout;
        let request_id = self.new_request(
            expiry,
            TrackableRequest::LoadRackSecret {
                rack_uuid,
                acks: ShareAcks::new(threshold),
            },
        );

        // Send a `GetShare` request to all connected peers
        let iter = connected_peers.iter().cloned().map(|to| Envelope {
            to,
            msg: Request {
                id: request_id,
                type_: RequestType::GetShare { rack_uuid },
            }
            .into(),
        });
        self.envelopes.extend(iter);
        request_id
    }

    pub fn new_learn_received_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        threshold: u8,
        from: Baseboard,
    ) -> Uuid {
        let expiry = now + self.config.learn_timeout;
        let request_id = self.new_request(
            expiry,
            TrackableRequest::LearnReceived {
                rack_uuid,
                from,
                acks: ShareAcks::new(threshold),
            },
        );

        request_id
    }

    /// Track and send a `RequestType::Learn` as a result of an
    /// `Fsm::init_learner` api request.
    pub fn new_learn_sent_req(&mut self, now: Instant, to: Baseboard) -> Uuid {
        let expiry = now + self.config.learn_timeout;
        let request_id = self.new_request(
            expiry,
            TrackableRequest::LearnSent { to: to.clone() },
        );

        self.envelopes.push(Envelope {
            to,
            msg: Msg::Req(Request {
                id: request_id,
                type_: RequestType::Learn,
            }),
        });

        request_id
    }

    // Track a new request
    fn new_request(
        &mut self,
        expiry: Instant,
        request: TrackableRequest,
    ) -> Uuid {
        let id = Uuid::new_v4();
        self.requests.insert(id, request);
        self.expiry_to_id.insert(expiry, id);
        id
    }

    /// Is there an outstanding `LearnSent` request
    pub fn has_learn_sent_req(&self) -> bool {
        self.requests.values().any(|req| {
            if let TrackableRequest::LearnSent { .. } = req {
                true
            } else {
                false
            }
        })
    }

    /// Return any expired requests mapped to their request id
    ///
    /// This is typically called during `tick` callbacks.
    pub fn expired(
        &mut self,
        now: Instant,
    ) -> BTreeMap<Uuid, TrackableRequest> {
        let mut expired = BTreeMap::new();
        while let Some((expiry, request_id)) = self.expiry_to_id.pop_last() {
            if expiry > now {
                expired.insert(
                    request_id,
                    self.requests.remove(&request_id).unwrap(),
                );
            } else {
                // Put the last request back. We are done.
                self.expiry_to_id.insert(expiry, request_id);
                break;
            }
        }
        expired
    }

    /// Return true if initialization completed, false otherwise
    ///
    /// If initialization completed, the request will be deleted.
    ///
    /// We drop the ack if the request_id is not found. This could be a lingering
    /// old ack from when the rack was reset to clean up after a prior failed rack
    /// init.
    pub fn on_init_ack(&mut self, from: Baseboard, request_id: Uuid) -> bool {
        if let Some(TrackableRequest::InitRack { acks, .. }) =
            self.requests.get_mut(&request_id)
        {
            acks.received.insert(from);
            if acks.received == acks.expected {
                self.expiry_to_id.retain(|_, id| *id != request_id);
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
    ) -> Option<TrackableRequest> {
        let acks = match self.requests.get_mut(&request_id) {
            Some(TrackableRequest::LoadRackSecret { acks, .. }) => acks,
            Some(TrackableRequest::LearnReceived { acks, .. }) => acks,
            _ => return None,
        };
        acks.received.insert(from, share);
        // We already have our own share to be used to reconstruct the secret
        if acks.received.len() == (acks.threshold - 1) as usize {
            self.expiry_to_id.retain(|_, id| *id != request_id);
            self.requests.remove(&request_id)
        } else {
            None
        }
    }

    /// Return the pkg if there is a matching request for it.
    ///Otherwise return `None`.
    pub fn on_pkg(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        pkg: LearnedSharePkg,
    ) -> Option<LearnedSharePkg> {
        None
    }

    /// If there are outstanding requests and this peer has not acknowledged
    /// the given request then send the request to the peer.
    pub fn on_connected(&mut self, peer_id: &Baseboard) {
        for (request_id, request) in &self.requests {
            match request {
                TrackableRequest::InitRack { rack_uuid, packages, acks } => {
                    if acks.received.contains(peer_id) {
                        continue;
                    }
                    if let Some(pkg) = packages.get(peer_id) {
                        self.envelopes.push(Envelope {
                            to: peer_id.clone(),
                            msg: Msg::Req(Request {
                                id: *request_id,
                                type_: RequestType::Init(pkg.clone()),
                            }),
                        });
                    }
                }
                TrackableRequest::LoadRackSecret { rack_uuid, acks } => {
                    if acks.received.contains_key(peer_id) {
                        continue;
                    }
                    self.envelopes.push(Envelope {
                        to: peer_id.clone(),
                        msg: Msg::Req(Request {
                            id: *request_id,
                            type_: RequestType::GetShare {
                                rack_uuid: *rack_uuid,
                            },
                        }),
                    });
                }
                TrackableRequest::LearnReceived { rack_uuid, acks, .. } => {
                    if acks.received.contains_key(peer_id) {
                        continue;
                    }
                    self.envelopes.push(Envelope {
                        to: peer_id.clone(),
                        msg: Msg::Req(Request {
                            id: *request_id,
                            type_: RequestType::GetShare {
                                rack_uuid: *rack_uuid,
                            },
                        }),
                    });
                }
                TrackableRequest::LearnSent { .. } => {
                    // If we have an existing `LearnSender` request there is no
                    // need to send another one currently.
                    continue;
                }
            }
        }
    }
}
