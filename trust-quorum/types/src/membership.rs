// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum membership bounds.

/// The minimum number of members in a trust quorum.
pub const TRUST_QUORUM_MIN_MEMBERS: usize = 3;

/// The maximum number of members in a trust quorum.
pub const TRUST_QUORUM_MAX_MEMBERS: usize = 32;
