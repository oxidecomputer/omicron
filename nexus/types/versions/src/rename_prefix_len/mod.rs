// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `RENAME_PREFIX_LEN` of the Nexus external API.
//!
//! Renames `prefix_len` to `prefix_length` in
//! `ExternalSubnetAllocator::Auto` for consistency with
//! `min_prefix_length` / `max_prefix_length` on subnet pool members.

pub mod external_subnet;
