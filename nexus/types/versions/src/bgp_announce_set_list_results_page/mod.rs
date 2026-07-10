// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BGP_ANNOUNCE_SET_LIST_RESULTS_PAGE` of the external Nexus API.
//!
//! Changes in this version:
//!
//! * `networking_bgp_announce_set_list` now returns a `ResultsPage` (with
//!   `next_page` token) instead of a bare array, making it consistent with
//!   the other paginated list endpoints. `BgpAnnounceSet` now derives
//!   `ObjectIdentity` so it can be used with the standard pagination
//!   machinery.

pub mod networking;
