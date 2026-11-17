// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `TRUST_QUORUM_MEMBERS` of the wicketd commissioning API.
//!
//! This version changes the `trust_quorum_peers` field of
//! [`rack_setup::MultirackJoinRequest`] to a `TrustQuorumMembers`, so the
//! trust quorum size bounds are part of the request's schema.

pub mod rack_setup;
