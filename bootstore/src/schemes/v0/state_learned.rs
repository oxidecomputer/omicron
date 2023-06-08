// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! FSM API for `State::Learned`

#[derive(Debug)]
pub struct LearnedState {
    pub pkg: LearnedSharePkgV0,
    // In `InitialMember` or `Learned` states, it is sometimes necessary to
    // reconstruct the rack secret.
    //
    // This is needed to both unlock local storage or decrypt our extra shares
    // to hand out to learners.
    pub rack_secret_state: Option<RackSecretState>,
}

impl LearnedState {
    pub fn new(pkg: LearnedSharePkgV0) -> Self {
        LearnedState { pkg, rack_secret_state: None }
    }
}
