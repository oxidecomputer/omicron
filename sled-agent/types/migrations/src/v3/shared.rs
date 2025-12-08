// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared types for Sled Agent API v3.
//!
//! This module contains types used in both requests and responses.
//!
//! Per RFD 619, high-level types are defined in the earliest version they
//! appear in. These types are used directly by the API crate with fixed
//! identifiers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Policy allowing an operator (via `omdb`) to control whether the switch zone
/// is started or stopped.
///
/// This is an _extremely_ dicey operation in general; a stopped switch zone
/// leaves the rack inoperable! We are only adding this as a workaround and test
/// tool for handling sidecar resets; see
/// <https://github.com/oxidecomputer/omicron/issues/8480> for background.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum OperatorSwitchZonePolicy {
    /// Start the switch zone if a switch is present.
    ///
    /// This is the default policy.
    StartIfSwitchPresent,

    /// Even if a switch zone is present, stop the switch zone.
    StopDespiteSwitchPresence,
}
