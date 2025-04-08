// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for tracking in flight requests

use super::{
    Envelope, FsmConfig, Msg, RackUuid, Request, RequestType, Share, SharePkg,
};
use sled_hardware_types::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use uuid::Uuid;

/// Acknowledgement tracking for `RequestType::InitRack`.
#[derive(Debug, Default)]
pub struct InitAcks {
    pub expected: BTreeSet<Baseboard>,
    pub received: BTreeSet<Baseboard>,
}

/// Acknowledgement tracking for `RequestType::LoadRackSecret` and
/// `RequestType::Learn`
#[derive(Debug)]
pub struct ShareAcks {
    pub threshold: u8,
    pub received: BTreeMap<Baseboard, Share>,
}

impl ShareAcks {
    pub fn new(threshold: u8) -> ShareAcks {
        ShareAcks { threshold, received: BTreeMap::new() }
    }
}

/// A mechanism to track in flight requests
#[derive(Debug)]
pub enum TrackableRequest {
    /// A request from the caller of the Fsm API to initialize a rack
    ///
    /// This must only be called at one peer, exactly once. That peer
    /// will be in `InitialMember` state.
    InitRack {
        rack_uuid: RackUuid,
        packages: BTreeMap<Baseboard, SharePkg>,
        acks: InitAcks,
    },

    /// A request from the caller of the Fsm API to load a rack secret
    ///
    /// Only peers in `InitialMember` or `Learned` state can load rack secrets
    LoadRackSecret { rack_uuid: RackUuid, acks: ShareAcks },

    /// A request received from a peer to learn a new share
    ///
    /// This request results in share gathering so "extra" shares can be
    /// decrypted via a key derived from the rack secret and one of them
    /// distributed to the learner.
    ///
    /// Only peers in `InitialMember` state can respond successfully
    LearnReceived { rack_uuid: RackUuid, from: Baseboard, acks: ShareAcks },

    /// A request sent from a peer in `Learning` state to another peer
    /// to learn a key share.
    LearnSent { to: Baseboard },
}

/// A mechanism to manage all in flight requests
///
/// We expect very few requests at a time - on the order of one or two requests.
pub struct RequestManager {
    id: Baseboard,
    config: FsmConfig,
    requests: BTreeMap<Uuid, TrackableRequest>,
    expiry_to_id: BTreeMap<Instant, Uuid>,

    /// Messages that need sending to other peers.
    ///
    /// These should be drained on each API call.
    envelopes: Vec<Envelope>,
}

impl RequestManager {
    /// Create a new RequestManager
    pub fn new(id: Baseboard, config: FsmConfig) -> RequestManager {
        RequestManager {
            id,
            config,
            requests: BTreeMap::new(),
            expiry_to_id: BTreeMap::new(),
            envelopes: vec![],
        }
    }

    /// Return an iterator of drained envelopes
    pub fn drain_elements(&mut self) -> impl Iterator<Item = Envelope> + '_ {
        self.envelopes.drain(..)
    }

    /// Track a new `Fsm::init_rack` api request and broadcast a
    /// `RequestType::Init` to connected peers.
    pub fn new_init_rack_req(
        &mut self,
        now: Instant,
        rack_uuid: RackUuid,
        packages: BTreeMap<Baseboard, SharePkg>,
        connected_peers: &BTreeSet<Baseboard>,
    ) -> Uuid {
        let expiry = now + self.config.rack_init_timeout;
        let mut acks = InitAcks::default();
        acks.expected =
            packages.keys().filter(|&id| id != &self.id).cloned().collect();
        let req = TrackableRequest::InitRack {
            rack_uuid,
            packages: packages.clone(),
            acks,
        };
        let request_id = self.new_request(expiry, req);

        // Send a `Request::Init` to each connected peer in the initial group
        let iter = packages
            .into_iter()
            .filter(|(to, _pkg)| connected_peers.contains(to))
            .map(|(to, pkg)| Envelope {
                to,
                msg: Request { id: request_id, type_: RequestType::Init(pkg) }
                    .into(),
            });
        self.envelopes.extend(iter);
        request_id
    }

    /// Track a new `Fsm::load_rack_secret` api request and broadcast a
    /// `RequestType::GetShare` to connected peers.
    pub fn new_load_rack_secret_req(
        &mut self,
        now: Instant,
        rack_uuid: RackUuid,
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
        self.broadcast_get_share(request_id, rack_uuid, connected_peers);
        request_id
    }

    /// Track a new `RequestType::Learn` request received from a peer and
    /// broadcast to all connected peers to get shares to unlock the rack
    /// secret, so the "extra" shares can be decrypted and on can be handed out
    /// to the learning peer.
    pub fn new_learn_received_req(
        &mut self,
        request_id: Uuid,
        now: Instant,
        rack_uuid: RackUuid,
        threshold: u8,
        from: Baseboard,
        connected_peers: &BTreeSet<Baseboard>,
    ) {
        let request = TrackableRequest::LearnReceived {
            rack_uuid,
            from,
            acks: ShareAcks::new(threshold),
        };
        let expiry = now + self.config.learn_timeout;
        self.requests.insert(request_id, request);
        self.expiry_to_id.insert(expiry, request_id);
        self.broadcast_get_share(request_id, rack_uuid, connected_peers);
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

    fn remove_request(&mut self, request_id: Uuid) -> Option<TrackableRequest> {
        self.expiry_to_id.retain(|_, id| *id != request_id);
        self.requests.remove(&request_id)
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

    // Send a `GetShare` request to all connected peers
    fn broadcast_get_share(
        &mut self,
        request_id: Uuid,
        rack_uuid: RackUuid,
        connected_peers: &BTreeSet<Baseboard>,
    ) {
        let iter = connected_peers.iter().cloned().map(|to| Envelope {
            to,
            msg: Request {
                id: request_id,
                type_: RequestType::GetShare { rack_uuid },
            }
            .into(),
        });
        self.envelopes.extend(iter);
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

    /// Is there an outstanding `InitRack` request
    pub fn has_init_rack_req(&self) -> bool {
        self.requests.values().any(|req| {
            if let TrackableRequest::InitRack { .. } = req {
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
            if expiry < now {
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

    /// Return `Some(true)` if initialization completed, `Some(false)` if it
    // did not.
    ///
    /// Return `None` if there is no active rack initialization ongoing.
    pub fn on_init_ack(
        &mut self,
        from: Baseboard,
        request_id: Uuid,
    ) -> Option<bool> {
        if let Some(TrackableRequest::InitRack { acks, .. }) =
            self.requests.get_mut(&request_id)
        {
            if !acks.expected.contains(&from) {
                // We don't want to allow nodes outside of our initial trust
                // quorum membership to ack.
                return None;
            }
            acks.received.insert(from);
            if acks.received == acks.expected {
                let _req = self.remove_request(request_id);
                Some(true)
            } else {
                Some(false)
            }
        } else {
            None
        }
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
            self.remove_request(request_id)
        } else {
            None
        }
    }

    /// Return true if there is a `LearnSent` for the given `request_id`, false
    /// otherwise.
    pub fn on_learn_pkg(&mut self, request_id: Uuid) -> bool {
        if let Some(TrackableRequest::LearnSent { .. }) =
            self.requests.get_mut(&request_id)
        {
            let _req = self.remove_request(request_id);
            true
        } else {
            false
        }
    }

    /// If there are outstanding requests and this peer has not acknowledged
    /// the given request then send the request to the peer.
    pub fn on_connected(&mut self, peer_id: &Baseboard) {
        for (request_id, request) in &self.requests {
            match request {
                TrackableRequest::InitRack { packages, acks, .. } => {
                    if acks.received.contains(peer_id)
                        || !acks.expected.contains(peer_id)
                    {
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
                TrackableRequest::LoadRackSecret {
                    rack_uuid, acks, ..
                } => {
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
