// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::request_manager::ShareAcks;
use super::{
    create_pkgs, Config, Envelope, LearnedSharePkg, Msg, MsgError, Request,
    RequestManager, RequestType, Response, ResponseType, Share, SharePkg,
    Shares, TrackableRequest,
};
use crate::trust_quorum::{RackSecret, TrustQuorumError};
use crate::Sha3_256Digest;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;

// An index into an encrypted share
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ShareIdx(pub usize);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    Uninitialized,
    InitialMember {
        pkg: SharePkg,

        /// Shares given to other sleds. We mark them as used so that we don't
        /// hand them out twice. If the same sled asks us for a share, because
        /// it crashes or there is a network blip, we will return the same
        /// share each time.
        ///
        /// Note that this is a fairly optimistic strategy as the requesting
        /// sled can always go ask another sled after a network blip. However,
        /// this guarantees that a single sled never hands out more than one of
        /// its shares to any given sled.
        ///
        /// We can't do much better than this without some sort of centralized
        /// distributor which is part of the reconfiguration mechanism in later
        /// versions of the trust quourum protocol.
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    },
    Learning,
    Learned {
        pkg: LearnedSharePkg,
    },
}

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            State::Uninitialized => "uninitialized",
            State::InitialMember { .. } => "initial_member",
            State::Learning => "learning",
            State::Learned { .. } => "learned",
        }
    }
}

/// A response to an Fsm API request
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiOutput {
    /// This peer has been initialized
    ///
    /// The caller *must* persist the state
    PeerInitialized,

    /// Rack initialization has completed. This node was the coordinator.
    RackInitComplete,

    /// A `RackSecret` was reconstructed
    RackSecret { request_id: Uuid, secret: RackSecret },

    /// An extra share has been distributed to a learning peer
    ///
    /// The caller *must* persist the state
    ShareDistributedToLearner,

    /// This peer Learned its share
    ///
    /// The caller must persist the state
    LearningCompleted,
}

/// An error returned from an Fsm API request
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("already initialized")]
    AlreadyInitialized,

    #[error("not yet initialized")]
    NotInitialized,

    #[error("cannot retrieve or distribute shares while learning")]
    StillLearning,

    #[error("rack init timeout: unacked_peers: {unacked_peers:?}")]
    RackInitTimeout { unacked_peers: BTreeSet<Baseboard> },

    #[error("rack init falied: trust quorum error: {0:?}")]
    RackInitFailed(TrustQuorumError),

    #[error("rack secret load timeout")]
    RackSecretLoadTimeout,

    #[error("share from {from} has invalid sha3_256 digest")]
    InvalidShare { from: Baseboard },

    #[error("critical: failed to reconstruct rack secret with valid shares")]
    FailedToReconstructRackSecret,

    #[error("critical: failed to decrypt extra shares")]
    FailedToDecryptExtraShares,

    #[error("unexpected response ({msg}) from ({from}) in state ({state}) with request_id ({request_id})")]
    UnexpectedResponse {
        from: Baseboard,
        state: &'static str,
        request_id: Uuid,
        msg: &'static str,
    },

    #[error("error response received from ({from}) in state ({state}) with request_id ({request_id}): {error:?}")]
    ErrorResponseReceived {
        from: Baseboard,
        state: &'static str,
        request_id: Uuid,
        error: MsgError,
    },
}

pub struct Fsm {
    /// The current state of this peer
    state: State,
    /// Unique IDs of this peer
    id: Baseboard,

    config: Config,

    /// Unique IDs of connected peers
    connected_peers: BTreeSet<Baseboard>,

    /// Manage all trackable broadcasts
    request_manager: RequestManager,

    /// Envelopes not managed by the `RequestManager`
    /// These are all envelopes containing `Response` messages
    envelopes: Vec<Envelope>,
}

impl Fsm {
    /// Create a new FSM in `State::Uninitialized`
    pub fn new_uninitialized(id: Baseboard, config: Config) -> Fsm {
        Fsm {
            state: State::Uninitialized,
            id,
            config,
            connected_peers: BTreeSet::new(),
            request_manager: RequestManager::new(config),
            envelopes: vec![],
        }
    }

    /// Return any envelopes that need sending
    ///
    /// This must be called after any API callback
    pub fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> + '_ {
        self.envelopes.drain(..).chain(self.request_manager.drain_elements())
    }

    /// Put a request into an envelope and add it to `self.envelopes`
    pub fn push_response(
        &mut self,
        to: Baseboard,
        request_id: Uuid,
        type_: ResponseType,
    ) {
        self.envelopes
            .push(Envelope { to, msg: Response { request_id, type_ }.into() });
    }

    pub fn state(&self) -> &State {
        &self.state
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
            return Err(ApiError::AlreadyInitialized);
        };
        let total_members = initial_membership.len();
        let pkgs = create_pkgs(rack_uuid, initial_membership.clone())
            .map_err(|e| ApiError::RackInitFailed(e))?;
        let mut iter = pkgs.expose_secret().into_iter();
        let our_pkg = iter.next().unwrap().clone();

        // Move into an initialized state
        self.state = State::InitialMember {
            pkg: our_pkg,
            distributed_shares: BTreeMap::new(),
        };

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
            return Err(ApiError::AlreadyInitialized);
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
            State::Uninitialized => return Err(ApiError::NotInitialized),
            State::Learning { .. } => return Err(ApiError::StillLearning),
            State::InitialMember { pkg, .. } => {
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
                TrackableRequest::LoadRackSecret { .. } => {
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

    /// Handle messages from other peers
    pub fn handle_msg(
        &mut self,
        now: Instant,
        from: Baseboard,
        msg: Msg,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match msg {
            Msg::Req(req) => self.handle_request(now, from, req),
            Msg::Rsp(rsp) => self.handle_response(from, rsp),
        }
    }

    // Handle a `Request` from a peer
    fn handle_request(
        &mut self,
        now: Instant,
        from: Baseboard,
        req: Request,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match req.type_ {
            RequestType::Init(pkg) => self.on_init(from, req.id, pkg),
            RequestType::GetShare { rack_uuid } => {
                self.on_get_share(from, req.id, rack_uuid);
                Ok(None)
            }
            RequestType::Learn => {
                self.on_learn(now, from, req.id);
                Ok(None)
            }
        }
    }

    // Handle a `RequestType::Init` from a peer
    fn on_init(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        new_pkg: SharePkg,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match &self.state {
            State::Uninitialized => {
                // Initialize ourselves and ack
                self.state = State::InitialMember {
                    pkg: new_pkg,
                    distributed_shares: BTreeMap::new(),
                };
                self.push_response(from, request_id, ResponseType::InitAck);
                Ok(Some(ApiOutput::PeerInitialized))
            }
            State::InitialMember { pkg, .. } => {
                // Return sucess on idempotence
                if pkg == &new_pkg {
                    self.push_response(from, request_id, ResponseType::InitAck);
                } else {
                    // Send an error response
                    self.push_response(
                        from,
                        request_id,
                        MsgError::AlreadyInitialized.into(),
                    );
                }
                // We already persisted that we are initialized
                Ok(None)
            }
            _ => {
                // Send an error response
                self.push_response(
                    from,
                    request_id,
                    MsgError::AlreadyInitialized.into(),
                );
                Ok(None)
            }
        }
    }

    // Handle a `RequestType::GetShare` from a peer
    fn on_get_share(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        rack_uuid: Uuid,
    ) {
        let response = match &self.state {
            State::Uninitialized => MsgError::NotInitialized.into(),
            State::Learning => MsgError::StillLearning.into(),
            State::Learned { pkg } => {
                if rack_uuid != pkg.rack_uuid {
                    MsgError::RackUuidMismatch {
                        expected: pkg.rack_uuid,
                        got: rack_uuid,
                    }
                    .into()
                } else {
                    ResponseType::Share(Share(pkg.share.clone()))
                }
            }
            State::InitialMember { pkg, .. } => {
                if rack_uuid != pkg.rack_uuid {
                    MsgError::RackUuidMismatch {
                        expected: pkg.rack_uuid,
                        got: rack_uuid,
                    }
                    .into()
                } else {
                    ResponseType::Share(Share(pkg.share.clone()))
                }
            }
        };

        self.push_response(from, request_id, response);
    }

    // Handle a `RequestType::Learn` from a peer
    fn on_learn(&mut self, now: Instant, from: Baseboard, request_id: Uuid) {
        let err = match &self.state {
            State::Uninitialized => Some(MsgError::NotInitialized),
            State::Learning => Some(MsgError::StillLearning),
            State::Learned { .. } => Some(MsgError::CannotSpareAShare),
            State::InitialMember { pkg, .. } => {
                let _ = self.request_manager.new_learn_received_req(
                    now,
                    pkg.rack_uuid,
                    pkg.threshold,
                    from.clone(),
                    &self.connected_peers,
                );
                // We need to gather shares and reconstruct the rack secret.
                // Therefore we don't send a response immediately.
                None
            }
        };
        if let Some(err) = err {
            self.push_response(from, request_id, err.into());
        }
    }

    // Handle a `Response` from a peer
    fn handle_response(
        &mut self,
        from: Baseboard,
        rsp: Response,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match rsp.type_ {
            ResponseType::InitAck => self.on_init_ack(from, rsp.request_id),
            ResponseType::Share(share) => {
                self.on_share(from, rsp.request_id, share)
            }
            ResponseType::Pkg(pkg) => self.on_pkg(from, rsp.request_id, pkg),
            ResponseType::Error(error) => {
                Err(ApiError::ErrorResponseReceived {
                    from,
                    state: self.state.name(),
                    request_id: rsp.request_id,
                    error,
                })
            }
        }
    }

    // Handle a `ResposneType::InitAck` from a peer
    fn on_init_ack(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match self.request_manager.on_init_ack(from.clone(), request_id) {
            Some(true) => Ok(Some(ApiOutput::RackInitComplete)),
            Some(false) => Ok(None),
            None => Err(ApiError::UnexpectedResponse {
                from,
                state: self.state.name(),
                request_id,
                msg: "InitAck",
            }),
        }
    }

    // Handle a `ResponseType::Pkg` from a peer
    fn on_pkg(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        pkg: LearnedSharePkg,
    ) -> Result<Option<ApiOutput>, ApiError> {
        if self.request_manager.on_pkg(request_id) {
            // This pkg matched our outstanding request. Let's transition from
            // `State::Learning` to `State::Learned`.
            assert_eq!(self.state, State::Learning);
            self.state = State::Learned { pkg };
            Ok(Some(ApiOutput::LearningCompleted))
        } else {
            if self.state == State::Learning {
                // This is a stale response. We could choose to accept it, but
                // for consistency with the rest of the `TrackableRequests`
                // we  only accept responses that have currently outstanding
                // requests.
                Ok(None)
            } else {
                Err(ApiError::UnexpectedResponse {
                    from,
                    state: self.state.name(),
                    request_id,
                    msg: "Pkg",
                })
            }
        }
    }

    // Handle a `ResponseType::Share` from a peer
    fn on_share(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        share: Share,
    ) -> Result<Option<ApiOutput>, ApiError> {
        match &mut self.state {
            // We don't send `GetShare` requests in these states
            State::Uninitialized | State::Learning => {
                return Err(ApiError::UnexpectedResponse {
                    from,
                    state: self.state.name(),
                    request_id,
                    msg: "Share",
                });
            }
            State::InitialMember { pkg, distributed_shares } => {
                validate_share(&from, &share, &pkg.share_digests)?;
                match self.request_manager.on_share(from, request_id, share) {
                    Some(TrackableRequest::LoadRackSecret { acks, .. }) => {
                        let secret = combine_shares(&pkg.share, acks)?;
                        Ok(Some(ApiOutput::RackSecret { request_id, secret }))
                    }
                    Some(TrackableRequest::LearnReceived {
                        rack_uuid,
                        from,
                        acks,
                    }) => {
                        let rack_secret = combine_shares(&pkg.share, acks)?;
                        // We now have the rack secret and can decrypt extra shares
                        decrypt_and_send_share_response(
                            from,
                            request_id,
                            pkg,
                            distributed_shares,
                            &rack_secret,
                            &mut self.envelopes,
                        )
                    }
                    // Only LoadRackSecret and LearnReceived track shares so we
                    // cannot get another variant back.
                    //
                    // If we get a `None` back we either haven't received enough
                    // shares or we have a late response to a prior request. A
                    // late response is very common, as we terminate the request
                    // once a threshold is received but we may still receive
                    // shares because we asked more than a threshold of peers
                    // for a share. Logging this as an unexpected response would
                    // be noisy and misleading.
                    _ => Ok(None),
                }
            }
            State::Learned { pkg } => {
                validate_share(&from, &share, &pkg.share_digests)?;
                match self.request_manager.on_share(from, request_id, share) {
                    Some(TrackableRequest::LoadRackSecret { acks, .. }) => {
                        let secret = combine_shares(&pkg.share, acks)?;
                        Ok(Some(ApiOutput::RackSecret { request_id, secret }))
                    }
                    Some(TrackableRequest::LearnReceived { .. }) => {
                        panic!(
                            "Invariant violation: Learned members must not 
                            accept 'Learn' requests"
                        )
                    }
                    // Only LoadRackSecret and LearnReceived track shares so we
                    // cannot get another variant back.
                    //
                    // If we get a `None` back we either haven't received enough
                    // shares or we have a late response to a prior request. A
                    // late response is very common, as we terminate the request
                    // once a threshold is received but we may still receive
                    // shares because we asked more than a threshold of peers
                    // for a share. Logging this as an unexpected response would
                    // be noisy and misleading.
                    _ => Ok(None),
                }
            }
        }
    }

    // Select the next peer in a round-robin fashion
    fn next_peer(&self, current: &Baseboard) -> Option<Baseboard> {
        let mut iter = self.connected_peers.range(current..);
        let matched = iter.next() == Some(current);
        iter.next()
            .filter(|_| matched)
            .or_else(|| self.connected_peers.first())
            .cloned()
    }
}

fn decrypt_and_send_share_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkg,
    distributed_shares: &mut BTreeMap<Baseboard, ShareIdx>,
    rack_secret: &RackSecret,
    envelopes: &mut Vec<Envelope>,
) -> Result<Option<ApiOutput>, ApiError> {
    // If decryption failse, we log it locally The peer will timeout and move to
    // the next one
    let shares = pkg
        .decrypt_shares(&rack_secret)
        .map_err(|_| ApiError::FailedToDecryptExtraShares)?;

    if let Some(idx) = distributed_shares.get(&from) {
        // The share was already handed out to this peer. Give back the same
        // one.
        let share = &shares.expose_secret()[idx.0];
        queue_pkg_response(from, request_id, pkg, &share, envelopes);
        // No state was updated, so no need to persist
        Ok(None)
    } else {
        // We need to pick a share to hand out and persist that fact. We find
        // the highest currently used index and add 1 or we select index 0.
        let idx = distributed_shares
            .values()
            .max()
            .cloned()
            .map(|idx| idx.0 + 1)
            .unwrap_or(0);

        match shares.expose_secret().get(idx) {
            Some(share) => {
                distributed_shares.insert(from.clone(), ShareIdx(idx));
                queue_pkg_response(from, request_id, pkg, share, envelopes);
                // This is a new distribution so we must inform the caller to
                // persist `Fsm::state`
                Ok(Some(ApiOutput::ShareDistributedToLearner))
            }
            None => {
                // Inform the learner that we don't have any shares
                envelopes.push(Envelope {
                    to: from,
                    msg: Msg::Rsp(Response {
                        request_id,
                        type_: MsgError::CannotSpareAShare.into(),
                    }),
                });
                Ok(None)
            }
        }
    }
}

// We have a share to hand back to a learner. Enqueue it in a response envelope.
fn queue_pkg_response(
    from: Baseboard,
    request_id: Uuid,
    pkg: &SharePkg,
    share: &Vec<u8>,
    envelopes: &mut Vec<Envelope>,
) {
    let learned_pkg = LearnedSharePkg {
        rack_uuid: pkg.rack_uuid,
        epoch: pkg.epoch,
        threshold: pkg.threshold,
        share: share.clone(),
        share_digests: pkg.share_digests.clone(),
    };
    // Queue up a response to the learner
    envelopes.push(Envelope {
        to: from,
        msg: Msg::Rsp(Response {
            request_id,
            type_: ResponseType::Pkg(learned_pkg),
        }),
    });
}

// Combine a threshold of shares and return the `RackSecret` or an error.
fn combine_shares(
    my_share: &Vec<u8>,
    acks: ShareAcks,
) -> Result<RackSecret, ApiError> {
    let shares = acks.received.into_values().fold(
        Shares(vec![my_share.clone()]),
        |mut acc, s| {
            acc.0.push(s.0.clone());
            acc
        },
    );
    RackSecret::combine_shares(&shares.0)
        .map_err(|_| ApiError::FailedToReconstructRackSecret)
}

// Validate a received share against known share digests
fn validate_share(
    from: &Baseboard,
    share: &Share,
    share_digests: &BTreeSet<Sha3_256Digest>,
) -> Result<(), ApiError> {
    let computed_hash = Sha3_256Digest(
        Sha3_256::digest(&share.0).as_slice().try_into().unwrap(),
    );

    if !share_digests.contains(&computed_hash) {
        Err(ApiError::InvalidShare { from: from.clone() })
    } else {
        Ok(())
    }
}
