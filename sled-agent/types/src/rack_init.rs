// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Constants related to rack initialization.

/// The contents of the bootstore go through three initial values during rack
/// initialization:
///
/// 1. `RSS_INITIAL`: The first generation written to the bootstore. This
///    contains the rack network config, but does not contain any service zone
///    NAT entries.
/// 2. `RSS_FINAL`: The second generation written to the bootstore. This
///    contains both the rack network config and all service zone NAT entries
///    (for services placed by RSS).
/// 3. `NEXUS_INITIAL`: The first generation written by Nexus after handoff from
///    RSS.
pub mod rack_init_bootstore_generation {
    pub const RSS_INITIAL: u64 = 1;
    pub const RSS_FINAL: u64 = 2;

    // This is an i64 instead of a u64 to match where this is written to the
    // database.
    pub const NEXUS_INITIAL: i64 = 3;
}
