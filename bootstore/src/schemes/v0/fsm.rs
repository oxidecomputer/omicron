// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use super::messages::{Envelope, Error, Msg, Request, Response};
use crate::trust_quorum::{LearnedSharePkgV0, SharePkgV0};
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

type Persist = bool;

// An index intjo an encrypted share
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareIdx(usize);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharePkg {
    Initial {
        pkg: SharePkgV0,
        // Shares given to other sleds. We mark them as used so that we don't
        // hand them out twice. If the same sled asks us for a share, because
        // it crashes or there is a network blip, we will return the same share
        // each time.
        //
        // Note that this is a fairly optimistic strategy as the requesting
        // sled can always go ask another sled after a network blip. However,
        // this guarantees that a single sled never hands out more than one of
        // its shares to any given sled.
        //
        // We can't do much better than this without some sort of centralized
        // distributor which is part of the reconfiguration mechanism in later
        // versions of the trust quourum protocol.
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    },

    Learned(LearnedSharePkgV0),
}

impl SharePkg {
    pub fn new_initial(pkg: SharePkgV0) -> SharePkg {
        SharePkg::Initial { pkg, distributed_shares: BTreeMap::new() }
    }

    pub fn new_learned(pkg: LearnedSharePkgV0) -> SharePkg {
        SharePkg::Learned(pkg)
    }

    pub fn rack_uuid(&self) -> Uuid {
        match self {
            SharePkg::Initial { pkg, .. } => pkg.rack_uuid,
            SharePkg::Learned(pkg) => pkg.rack_uuid,
        }
    }
}

/// State stored on the M.2s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    // The generation number for ledger writing purposes on both M.2s
    pub version: u32,
    pub pkg: Option<SharePkg>,
}

/// The state machine for a [`$crate::Peer`]
pub struct Fsm {
    id: Baseboard,
    config: Config,
    state: PersistentState,
    peers: BTreeSet<Baseboard>,
    // The current time in ticks
    clock: usize,

    // Is this node a learner or member of the initial group?
    // This is determe
    is_learner: Option<bool>,
}

/// Configuration of the FSM
pub struct Config {
    retry_timeout_in_ticks: usize,
}

impl Fsm {
    pub fn new(id: Baseboard, config: Config, state: PersistentState) -> Fsm {
        Fsm {
            id,
            config,
            state,
            peers: BTreeSet::new(),
            clock: 0,
            is_learner: None,
        }
    }

    /// Return true if the FSM is initialized as a member of the initial group
    /// or a learner, false otherwise.
    pub fn is_initialized(&self) -> bool {
        self.state.pkg.is_some()
    }

    /// Return the current persistent state
    pub fn persistent_state(&self) -> &PersistentState {
        &self.state
    }

    /// Handle a message from a peer.
    ///
    /// Return whether persistent state needs syncing to disk and a set of
    /// messages to send to other peers. Persistant state must be saved by
    /// the caller and safely persisted before messages are sent, or the next
    /// message is handled here.
    pub fn handle(
        &mut self,
        from: Baseboard,
        msg: Msg,
    ) -> (Persist, Vec<Envelope>) {
        match msg {
            Msg::Req(req) => self.handle_request(from, req),
            Msg::Rsp(rsp) => self.handle_response(from, rsp),
        }
    }

    /// An abstraction of a timer tick.
    ///
    /// Ticks mutate state and can result in message retries.
    ///
    /// Each tick represents some abstract duration of time.
    /// Timeouts are represented by number of ticks in this module, which
    /// allows for deterministic property based tests in a straightforward manner.
    /// On each tick, the current duration since start is passed in. We only
    /// deal in relative time needed for timeouts, and not absolute time. This
    /// strategy allows for deterministic property based tests.
    pub fn tick(&mut self) -> Vec<Envelope> {
        self.clock += 1;
        vec![]
    }

    fn handle_request(
        &mut self,
        from: Baseboard,
        request: Request,
    ) -> (Persist, Vec<Envelope>) {
        match request {
            Request::Identify(peer) => {
                self.peers.insert(peer.clone());
                self.respond(from, Response::IdentifyAck(self.id.clone()))
            }
            Request::Init(new_pkg) => {
                match &self.state.pkg {
                    Some(SharePkg::Initial { pkg, .. }) => {
                        if new_pkg == *pkg {
                            // Idempotent response given same pkg
                            self.respond(from, Response::InitAck)
                        } else {
                            self.respond(
                                from,
                                Error::AlreadyInitialized {
                                    rack_uuid: pkg.rack_uuid,
                                }
                                .into(),
                            )
                        }
                    }
                    Some(SharePkg::Learned(pkg)) => self.respond(
                        from,
                        Error::AlreadyInitialized { rack_uuid: pkg.rack_uuid }
                            .into(),
                    ),
                    None => {
                        self.state.pkg = Some(SharePkg::new_initial(new_pkg));
                        self.state.version += 1;
                        self.persist_and_respond(from, Response::InitAck)
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    fn handle_response(
        &mut self,
        from: Baseboard,
        response: Response,
    ) -> (Persist, Vec<Envelope>) {
        unimplemented!()
    }

    // Return a response directly to a peer that doesn't require persistence
    fn respond(
        &self,
        to: Baseboard,
        response: Response,
    ) -> (Persist, Vec<Envelope>) {
        (false, vec![Envelope { to, msg: response.into() }])
    }

    // Indicate to the caller that state must be perisisted and then a response
    // returned to the peer.
    fn persist_and_respond(
        &self,
        to: Baseboard,
        response: Response,
    ) -> (Persist, Vec<Envelope>) {
        (true, vec![Envelope { to, msg: response.into() }])
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn blah() {}
}
