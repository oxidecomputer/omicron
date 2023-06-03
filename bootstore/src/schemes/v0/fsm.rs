// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use std::collections::BTreeSet;

use super::messages::{Envelope, Msg, Request, Response};
use crate::trust_quorum::{LearnedSharePkgV0, SharePkgV0};
use sled_hardware::Baseboard;

// An index into an encrypted share
#[derive(Debug, Serialize, Deserialize)]
struct ShareIdx(usize);

#[derive(Debug, Serialize, Deserialize)]
enum SharePkg {
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
    pub fn rack_uuid(&self) -> Uuid {
        match self {
            SharePkg::Initial(pkg) => pkg.rack_uuid,
            SharePkg::Learned(pkg) => pkg.rack_uuid,
        }
    }
}

/// State stored on the M.2s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    // The generation number for ledger writing purposes on both M.2s
    pub ledger_generation: u32,
    pub pkg: Option<SharePkg>,
}

/// The state machine for a [`$crate::Peer`]
pub struct Fsm {
    config: Config,
    state: PersistentState,
    peers: BTreeSet<Baseboard>,
    // The current time in ticks
    clock: usize,
}

/// Configuration of the FSM
pub struct Config {
    retry_timeout_in_ticks: usize,
}

impl Fsm {
    pub fn new(config: Config, state: PersistentState) {
        Fsm { config, state, peers: BTreeSet::new(), clock: 0 }
    }

    /// Handle a message from a peer.
    ///
    /// Return any persistent state to sync to disk and a set of messages to
    /// send to other peers. Persistant state must be saved by the caller and
    /// safely persisted before messages should be sent, or the next message
    /// processed.
    pub fn handle(
        &mut self,
        from: Baseboard,
        msg: Msg,
    ) -> (Option<PersistentState>, Vec<Envelope>) {
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
    }

    fn handle_request(
        &mut self,
        from: Baseboard,
        request: Request,
    ) -> (Option<PersistentState>, Vec<Envelope>) {
        unimplemented!()
    }

    fn handle_response(
        &mut self,
        from: Baseboard,
        response: Response,
    ) -> (Option<PersistentState>, Vec<Envelope>) {
        unimplemented!()
    }
}
