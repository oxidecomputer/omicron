// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_MULTIRACK_JOIN_IN_PROGRESS_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds the `multirack_join_in_progress` field to inventory. This
//! field reports whether this sled is currently running the multirack join
//! service.
//!
//! Nexus uses this to identify which sled-agent of a joining rack to send the
//! `/multirack-join-complete` request to, rather than having to guess or try
//! every sled-agent.

pub mod inventory;
