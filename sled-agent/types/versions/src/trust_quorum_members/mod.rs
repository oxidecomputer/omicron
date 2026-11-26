// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `TRUST_QUORUM_MEMBERS` of the Sled Agent API.
//!
//! This version makes two changes:
//!
//! 1. Add a newtype for the trust quorum member set (in
//!    trust-quorum-types-versions).
//! 2. Add `latest_committed_epoch` to
//!   `TrustQuorumCoordinatorStatusResponse::NotCoordinating`.

pub mod trust_quorum;
