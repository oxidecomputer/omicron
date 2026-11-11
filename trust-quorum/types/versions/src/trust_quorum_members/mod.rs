// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `TRUST_QUORUM_MEMBERS` of trust-quorum types.
//!
//! This version adds [`membership::TrustQuorumMembers`], a set of trust quorum
//! members whose size is bounded by [`membership::TRUST_QUORUM_MIN_MEMBERS`]
//! and [`membership::TRUST_QUORUM_MAX_MEMBERS`], and uses it for the members of
//! [`messages::ReconfigureMsg`] and [`messages::LrtqUpgradeMsg`].

pub mod membership;
pub mod messages;
