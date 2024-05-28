// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's written
//! this way to enable easy testing and auditing.

#![allow(clippy::result_large_err)]

use super::request_manager::ShareAcks;
use super::{
    create_pkgs, Envelope, FsmConfig, LearnedSharePkg, Msg, MsgError, RackUuid,
    Request, RequestManager, RequestType, Response, ResponseType, Share,
    SharePkg, Shares, TrackableRequest,
};
use crate::schemes::v0::share_pkg::SharePkgCommon;
use crate::trust_quorum::{RackSecret, TrustQuorumError};
use crate::Sha3_256Digest;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sha3::{Digest, Sha3_256};
use sled_hardware_types::Baseboard;
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

#[serde_as]
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
        /// trust quourum protocol schemes.
        #[serde_as(as = "Vec<(_, _)>")]
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
    /// The caller *must* persist `Fsm::State`
    PeerInitialized,

    /// Rack initialization has completed. This node was the coordinator.
    RackInitComplete,

    /// A `RackSecret` was reconstructed
    RackSecret { request_id: Uuid, secret: RackSecret },

    /// An extra share has been distributed to a learning peer
    ///
    /// The caller *must* persist `Fsm::State`
    ShareDistributedToLearner,

    /// This peer learned its share
    ///
    /// The caller *must* persist `Fsm::State`
    LearningCompleted,
}

/// An error returned from an Fsm API request
#[derive(Error, Debug, Clone, PartialEq, Eq)]
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
    /// Unique ID of this peer
    id: Baseboard,

    /// User provided configuration
    config: FsmConfig,

    /// Unique IDs of connected peers
    connected_peers: BTreeSet<Baseboard>,

    /// Manage all trackable requests
    request_manager: RequestManager,

    /// Envelopes not managed by the `RequestManager`
    /// These are all envelopes containing `Response` messages
    responses: Vec<Envelope>,

    /// We keep track of whether the rack failed to initialize. If this happens
    /// the coordinator should return this error on every new API request.
    rack_init_error: Option<(Uuid, ApiError)>,
}

impl Fsm {
    /// Create a new FSM in `State::Uninitialized`
    pub fn new_uninitialized(id: Baseboard, config: FsmConfig) -> Fsm {
        Fsm::new(id, config, State::Uninitialized)
    }

    /// Create an Fsm with a saved state
    pub fn new(id: Baseboard, config: FsmConfig, state: State) -> Fsm {
        Fsm {
            state,
            id: id.clone(),
            config,
            connected_peers: BTreeSet::new(),
            request_manager: RequestManager::new(id, config),
            responses: vec![],
            rack_init_error: None,
        }
    }

    pub fn config(&self) -> &FsmConfig {
        &self.config
    }

    /// Return any envelopes that need sending
    ///
    /// This must be called after any API callback
    pub fn drain_envelopes(&mut self) -> impl Iterator<Item = Envelope> + '_ {
        self.responses.drain(..).chain(self.request_manager.drain_elements())
    }

    /// Put a request into an envelope and add it to `self.envelopes`
    pub fn push_response(
        &mut self,
        to: Baseboard,
        request_id: Uuid,
        type_: ResponseType,
    ) {
        self.responses
            .push(Envelope { to, msg: Response { request_id, type_ }.into() });
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn rack_init_failed(&self) -> bool {
        self.rack_init_error.is_some()
    }

    pub fn check_init_err(&self) -> Result<(), ApiError> {
        match &self.rack_init_error {
            Some((_, err)) => Err(err.clone()),
            None => Ok(()),
        }
    }

    /// This call is triggered locally on a single sled as a result of RSS
    /// running. It may only be called once, which is enforced by checking to
    /// see if we already are in `State::Uninitialized`.
    ///
    /// Persistence is required after a successful call to `init_rack`.
    pub fn init_rack(
        &mut self,
        now: Instant,
        rack_uuid: RackUuid,
        initial_membership: BTreeSet<Baseboard>,
    ) -> Result<(), ApiError> {
        self.check_init_err()?;
        let State::Uninitialized = self.state else {
            return Err(ApiError::AlreadyInitialized);
        };
        let pkgs = create_pkgs(rack_uuid.0, initial_membership.clone())
            .map_err(ApiError::RackInitFailed)?;
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
            packages,
            &self.connected_peers,
        );

        Ok(())
    }

    /// Are we still waiting for `InitAck` responses from peers?
    pub fn is_rack_initializing(&self) -> bool {
        self.request_manager.has_init_rack_req()
    }

    /// Initialize a node added after rack initialization
    ///
    /// Persistence is required after a successful call to `init_learner`
    pub fn init_learner(&mut self, now: Instant) -> Result<(), ApiError> {
        self.check_init_err()?;
        let State::Uninitialized = self.state else {
            return Err(ApiError::AlreadyInitialized);
        };

        if let Some(to) = self.connected_peers.first() {
            let _uuid =
                self.request_manager.new_learn_sent_req(now, to.clone());
        }
        self.state = State::Learning;
        Ok(())
    }

    /// This call is triggered locally after RSS runs, in order to retrieve
    /// the `RackSecret` so that it can be used as input key material. It
    /// starts a key share retrieval process so that the `RackSecret` can
    /// be reconstructed.
    pub fn load_rack_secret(&mut self, now: Instant) -> Result<Uuid, ApiError> {
        self.check_init_err()?;
        let pkg = match &self.state {
            State::Uninitialized => return Err(ApiError::NotInitialized),
            State::Learning { .. } => return Err(ApiError::StillLearning),
            State::InitialMember { pkg, .. } => &pkg.common,
            State::Learned { pkg } => &pkg.common,
        };
        let request_id = self.request_manager.new_load_rack_secret_req(
            now,
            pkg.rack_uuid.into(),
            pkg.threshold,
            &self.connected_peers,
        );

        Ok(request_id)
    }

    /// Periodic tick to check for request expiration and trigger learner
    /// attempt peer rotation.
    ///
    /// Return any expired request errors mapped to their request id
    pub fn tick(
        &mut self,
        now: Instant,
    ) -> Result<(), BTreeMap<Uuid, ApiError>> {
        if let State::Uninitialized = &self.state {
            return Ok(());
        }
        if let Some((request_id, err)) = &self.rack_init_error {
            return Err(BTreeMap::from([(*request_id, err.clone())]));
        }
        let mut errors = BTreeMap::new();
        for (req_id, req) in self.request_manager.expired(now) {
            match req {
                TrackableRequest::InitRack { acks, .. } => {
                    let unacked_peers = acks
                        .expected
                        .difference(&acks.received)
                        .cloned()
                        .collect();
                    let err = ApiError::RackInitTimeout { unacked_peers };
                    errors.insert(req_id, err.clone());
                    self.rack_init_error = Some((req_id, err));
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
    /// Send any necessary messages required by pending requests.
    pub fn on_connected(
        &mut self,
        now: Instant,
        peer_id: Baseboard,
    ) -> Result<(), ApiError> {
        self.check_init_err()?;
        if let State::Learning = &self.state {
            if !self.request_manager.has_learn_sent_req() {
                // This is the first peer we've seen in the learning state, so try
                // to learn from it.
                let _uuid = self
                    .request_manager
                    .new_learn_sent_req(now, peer_id.clone());
            }
        }
        self.request_manager.on_connected(&peer_id);
        self.connected_peers.insert(peer_id);
        Ok(())
    }

    /// A peer has been disconnected
    ///
    /// If this node is a learner and it was talking to the disconnected peer,
    /// the `RequestManager` will eventually time out the request and we'll move
    /// onto the next peer.
    pub fn on_disconnected(&mut self, peer_id: &Baseboard) {
        self.connected_peers.remove(peer_id);
    }

    /// Handle messages from other peers
    pub fn handle_msg(
        &mut self,
        now: Instant,
        from: Baseboard,
        msg: Msg,
    ) -> Result<Option<ApiOutput>, ApiError> {
        self.check_init_err()?;
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
        rack_uuid: RackUuid,
    ) {
        let response = match &self.state {
            State::Uninitialized => MsgError::NotInitialized.into(),
            State::Learning => MsgError::StillLearning.into(),
            State::Learned { pkg: LearnedSharePkg { common } }
            | State::InitialMember { pkg: SharePkg { common, .. }, .. } => {
                if rack_uuid.0 != common.rack_uuid {
                    MsgError::RackUuidMismatch {
                        expected: common.rack_uuid.into(),
                        got: rack_uuid,
                    }
                    .into()
                } else {
                    ResponseType::Share(Share(common.share.clone()))
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
                self.request_manager.new_learn_received_req(
                    request_id,
                    now,
                    pkg.common.rack_uuid.into(),
                    pkg.common.threshold,
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
            ResponseType::LearnPkg(pkg) => {
                self.on_learn_pkg(from, rsp.request_id, pkg)
            }
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

    // Handle a `ResponseType::InitAck` from a peer
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
    fn on_learn_pkg(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
        pkg: LearnedSharePkg,
    ) -> Result<Option<ApiOutput>, ApiError> {
        if self.request_manager.on_learn_pkg(request_id) {
            // This pkg matched our outstanding request. Let's transition from
            // `State::Learning` to `State::Learned`.
            assert_eq!(self.state, State::Learning);
            self.state = State::Learned { pkg };
            Ok(Some(ApiOutput::LearningCompleted))
        } else if self.state == State::Learning {
            // This is a stale response. We could choose to accept it, but
            // for consistency with the rest of the `TrackableRequests`
            // we only accept responses that have currently outstanding
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
                validate_share(&from, &share, &pkg.common.share_digests)?;
                match self.request_manager.on_share(from, request_id, share) {
                    Some(TrackableRequest::LoadRackSecret { acks, .. }) => {
                        let secret = combine_shares(&pkg.common.share, acks)?;
                        Ok(Some(ApiOutput::RackSecret { request_id, secret }))
                    }
                    Some(TrackableRequest::LearnReceived {
                        from,
                        acks,
                        ..
                    }) => {
                        let rack_secret =
                            combine_shares(&pkg.common.share, acks)?;
                        // We now have the rack secret and can decrypt extra shares
                        decrypt_and_send_share_response(
                            from,
                            request_id,
                            pkg,
                            distributed_shares,
                            &rack_secret,
                            &mut self.responses,
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
                validate_share(&from, &share, &pkg.common.share_digests)?;
                match self.request_manager.on_share(from, request_id, share) {
                    Some(TrackableRequest::LoadRackSecret { acks, .. }) => {
                        let secret = combine_shares(&pkg.common.share, acks)?;
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
    // If decryption fails, we log it locally. The peer will timeout and move to
    // the next one. This is really bad and should be impossible.
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
        common: SharePkgCommon {
            rack_uuid: pkg.common.rack_uuid,
            epoch: pkg.common.epoch,
            threshold: pkg.common.threshold,
            share: share.clone(),
            share_digests: pkg.common.share_digests.clone(),
        },
    };
    // Queue up a response to the learner
    envelopes.push(Envelope {
        to: from,
        msg: Msg::Rsp(Response {
            request_id,
            type_: ResponseType::LearnPkg(learned_pkg),
        }),
    });
}

// Combine a threshold of shares and return the `RackSecret` or an error.
fn combine_shares(
    my_share: &Vec<u8>,
    acks: ShareAcks,
) -> Result<RackSecret, ApiError> {
    let shares = Shares(
        acks.received
            .into_values()
            .map(|mut s| std::mem::take(&mut s.0))
            .chain(std::iter::once(my_share.clone()))
            .collect(),
    );

    // If this fails, it's really bad. This means valid shares can't reconstruct
    // the rack secret. This should be impossible, as reconstruction is
    // determinisitic. It could only possibly happen with an incompatible
    // upgrade that we should test before shipping.
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
