// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! V0 protocol state machine
//!
//! This state machine is entirely synchronous. It performs actions and returns
//! results. This is where the bulk of the protocol logic lives. It's
//! written this way to enable easy testing and auditing.

use crate::peer_msgs::{Msg, Request};
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
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    // The generation number for ledger writing purposes on both M.2s
    pub ledger_generation: u32,
    pub pkg: Option<SharePkg>,
}

/// The state machine for a [`$crate::Peer`]
pub struct Fsm {
    state: PersistentState,
}

impl Fsm {
    pub fn new(state: PersistentState) {
        Fsm { state }
    }

    pub fn handle_req(&mut self, request: Request) -> Msg {}
}
