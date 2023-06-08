// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::InitialMember`

use std::collections::BTreeMap;

use crate::trust_quorum::SharePkgV0;

use super::fsm::{next_peer, StateHandler};
use super::fsm_output::Output;
use super::messages::{Request, RequestType, Response, ResponseType};
use super::state::{
    FsmCommonData, InitialMemberState, RackInitState, RackSecretState, State,
};
use sled_hardware::Baseboard;
use uuid::Uuid;

#[derive(Debug)]
pub struct InitialMemberState {
    pub pkg: SharePkgV0,

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
    pub distributed_shares: BTreeMap<Baseboard, ShareIdx>,

    // Acknowledgements tracked during rack initialization if this node is
    // the one acting as coordinator (local to RSS).
    pub rack_init_state: Option<RackInitState>,

    // Pending learn requests from other peers mapped to their start time.
    //
    // When a peer attempts to learn a share, we may not be able to give it
    // one because we cannot yet recompute the rack secret. We queue requests
    // here and respond when we can recompute the rack secret. If we timeout before
    // we can recompute the rack secret, we respond with a timeout Error.
    //
    // Note that if we get a new `RequestType::Learn` from a peer that is already
    // pending, we will reset the start time.
    pub pending_learn_requests: BTreeMap<Baseboard, RequestMetadata>,

    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl InitialMemberState {
    pub fn new(
        pkg: SharePkgV0,
        distributed_shares: BTreeMap<Baseboard, ShareIdx>,
    ) -> Self {
        InitialMemberState {
            pkg,
            distributed_shares,
            rack_init_state: None,
            pending_learn_requests: BTreeMap::new(),
            rack_secret_state: None,
        }
    }

    pub fn name(&self) -> &'static str {
        "initial_member"
    }
}

impl StateHandler for InitialMemberState {
    /// Check for rack initialization timeout
    ///
    /// TODO: Also need to check for:
    ///  * pending learn request timeouts
    ///  * rack secret expiry - so we can zero it or shares
    fn tick(mut self, common: &mut FsmCommonData) -> (State, Output) {
        if let Some(rack_init_state) = &mut self.rack_init_state {
            if rack_init_state
                .timer_expired(common.clock, common.config.rack_init_timeout)
            {
                let unacked_peers = pkg
                    .initial_membership
                    .difference(&rack_init_state.acks)
                    .cloned()
                    .collect();
                self.rack_init_state = None;
                (
                    self.into(),
                    ApiError::RackInitTimeout { unacked_peers }.into(),
                )
            }
        }

        (self.into(), Output::none())
    }
}
