// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version `MULTICAST_IMPLICIT_LIFECYCLE_UPDATES` that changed
//! in version `MULTICAST_DROP_MVLAN`.
//!
//! Removes the `mvlan` field from `MulticastGroup`, `MulticastGroupCreate`,
//! and `MulticastGroupUpdate`. Adds `has_any_source_member` to
//! `MulticastGroup`.

pub mod multicast;
