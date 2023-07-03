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

/// An attempt by *this* peer to learn a key share
///
/// When received it triggers a `TrackableRequest::Learn` at the receiver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearnAttempt {
    pub peer: Baseboard,
    pub expiry: Instant,
}

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(pub usize);

pub enum State {
    Uninitialized,
    InitialMember { pkg: SharePkg },
    Learning { attempt: Option<LearnAttempt> },
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

    /// Messages that need sending to other peers.
    ///
    /// These should be drained on each API call.
    envelopes: Vec<Envelope>,
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
            envelopes: vec![],
        }
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

        let request_id = self.request_manager.new_init_rack_req(
            now,
            rack_uuid,
            packages.clone(),
        );

        // Send a `Request::Init` to each connected peer in the initial group
        let envelope_iter = packages.into_iter().filter_map(|(to, pkg)| {
            if self.connected_peers.contains(&to) {
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
        self.envelopes.extend(envelope_iter);

        Ok(())
    }

    /// Initialize a node added after rack initialization
    ///
    /// Persistence is required after a successful call to `init_learner`
    pub fn init_learner(&mut self, now: Instant) -> Result<(), ApiError> {
        let State::Uninitialized = self.state else {
            return Err(ApiError::PeerAlreadyInitialized);
        };

        let attempt =
            self.connected_peers.first().map(|peer_id| LearnAttempt {
                peer: peer_id.clone(),
                expiry: now + self.config.learn_timeout,
            });

        self.state = State::Learning { attempt };
        Ok(())
    }

    /// This call is triggered locally after RSS runs, in order to retrieve the
    /// `RackSecret` so that it can be used as input key material.
    ///
    /// if the rack secret has not already been loaded, then share retrieval
    /// will begin.
    pub fn load_rack_secret(&mut self, now: Instant) -> Result<(), ApiError> {
        let (request_id, rack_uuid) = match &self.state {
            State::Uninitialized => return Err(ApiError::RackNotInitialized),
            State::Learning { .. } => return Err(ApiError::StillLearning),
            State::InitialMember { pkg } => {
                let request_id = self.request_manager.new_load_rack_secret_req(
                    now,
                    pkg.rack_uuid,
                    pkg.threshold,
                );
                (request_id, pkg.rack_uuid)
            }
            State::Learned { pkg } => {
                let request_id = self.request_manager.new_load_rack_secret_req(
                    now,
                    pkg.rack_uuid,
                    pkg.threshold,
                );
                (request_id, pkg.rack_uuid)
            }
        };

        // Send a `GetShare` request to all connected peers
        let iter = self.connected_peers.iter().cloned().map(|to| Envelope {
            to,
            msg: Request {
                id: request_id,
                type_: RequestType::GetShare { rack_uuid },
            }
            .into(),
        });
        self.envelopes.extend(iter);
        Ok(())
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
    /// This must only be called at one peer, exactly once.
    InitRack {
        rack_uuid: Uuid,
        packages: BTreeMap<Baseboard, SharePkg>,
        acks: InitAcks,
    },

    /// A request from the caller of the Fsm API to load a rack secret
    LoadRackSecret { rack_uuid: Uuid, acks: ShareAcks },

    /// A request from a peer to learn a new share
    //
    /// This peer was not part of the initial membership group.
    Learn { rack_uuid: Uuid, from: Baseboard, acks: ShareAcks },
}

/// A mechanism to manage all in flight requests
///
/// We expect very few requests at a time - on the order of one or two requests.
pub struct RequestManager {
    config: Config,
    requests: BTreeMap<Uuid, TrackableRequest>,
    expiry_to_id: BTreeMap<Instant, Uuid>,
}

impl RequestManager {
    pub fn new(config: Config) -> RequestManager {
        RequestManager {
            config,
            requests: BTreeMap::new(),
            expiry_to_id: BTreeMap::new(),
        }
    }

    pub fn new_init_rack_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        packages: BTreeMap<Baseboard, SharePkg>,
    ) -> Uuid {
        let expiry = now + self.config.rack_init_timeout;
        let req = TrackableRequest::InitRack {
            rack_uuid,
            packages,
            acks: InitAcks::default(),
        };
        self.new_request(expiry, req)
    }

    pub fn new_load_rack_secret_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        threshold: u8,
    ) -> Uuid {
        let expiry = now + self.config.rack_secret_request_timeout;
        self.new_request(
            expiry,
            TrackableRequest::LoadRackSecret {
                rack_uuid,
                acks: ShareAcks::new(threshold),
            },
        )
    }

    pub fn new_learn_req(
        &mut self,
        now: Instant,
        rack_uuid: Uuid,
        threshold: u8,
        from: Baseboard,
    ) -> Uuid {
        let expiry = now + self.config.learn_timeout;
        self.new_request(
            expiry,
            TrackableRequest::Learn {
                rack_uuid,
                from,
                acks: ShareAcks::new(threshold),
            },
        )
    }

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

    /// Return any expired requests
    ///
    /// This is typically called during `on_tick` callbacks.
    pub fn expired(&mut self, now: Instant) -> Vec<TrackableRequest> {
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
            Some(TrackableRequest::Learn { acks, .. }) => acks,
            _ => return None,
        };
        acks.received.insert(from, share);
        if acks.received.len() == acks.threshold as usize {
            self.expiry_to_id.retain(|_, id| *id != request_id);
            self.requests.remove(&request_id)
        } else {
            None
        }
    }

    /// If there are outstanding requests and this peer has not acknowledged
    /// the given request then send the request to the peer.
    pub fn on_connected(&self, peer_id: &Baseboard) -> Vec<Envelope> {
        let mut envelopes = vec![];
        for (request_id, request) in &self.requests {
            match request {
                TrackableRequest::InitRack { rack_uuid, packages, acks } => {
                    if acks.received.contains(peer_id) {
                        continue;
                    }
                    if let Some(pkg) = packages.get(peer_id) {
                        envelopes.push(Envelope {
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
                    envelopes.push(Envelope {
                        to: peer_id.clone(),
                        msg: Msg::Req(Request {
                            id: *request_id,
                            type_: RequestType::GetShare {
                                rack_uuid: *rack_uuid,
                            },
                        }),
                    });
                }
                TrackableRequest::Learn { rack_uuid, acks, .. } => {
                    if acks.received.contains_key(peer_id) {
                        continue;
                    }
                    envelopes.push(Envelope {
                        to: peer_id.clone(),
                        msg: Msg::Req(Request {
                            id: *request_id,
                            type_: RequestType::GetShare {
                                rack_uuid: *rack_uuid,
                            },
                        }),
                    });
                }
            }
        }
        envelopes
    }
}
