// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module doc: trust quorum membership bounds shared by the protocol, Nexus, wicketd and multirack-join; not a wire type, so it lives here rather than in trust-quorum-types-versions.

/// The minimum number of members in a trust quorum.
pub const TRUST_QUORUM_MIN_MEMBERS: usize = 3;

/// The maximum number of members in a trust quorum.
pub const TRUST_QUORUM_MAX_MEMBERS: usize = 32;
