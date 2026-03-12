// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MULTICAST_IMPLICIT_LIFECYCLE_UPDATES` of the Nexus external API.
//!
//! This version changes multicast group paths and member operations to use
//! `MulticastGroupIdentifier` (supporting name, UUID, or IP address) instead
//! of `NameOrId`. It also introduces `MulticastGroupJoinSpec` for inline
//! multicast group membership in instance create/update, and adds per-member
//! source IP filtering.

pub mod instance;
pub mod multicast;
