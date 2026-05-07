// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `PROBE_MULTICAST` of the Nexus external API.
//!
//! This module adds `multicast_groups` to `ProbeCreate` so that probes can be
//! enrolled as multicast group members at creation time, using the same
//! `MulticastGroupJoinSpec` shape as instance joins. Membership is fixed
//! at probe creation.

pub mod multicast;
pub mod probe;
