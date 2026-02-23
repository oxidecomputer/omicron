// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! BFD (Bidirectional Forwarding Detection) types for the Nexus external API.

use std::net::IpAddr;

use omicron_common::api::external::Name;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum BfdState {
    /// A stable down state. Non-responsive to incoming messages.
    AdminDown = 0,

    /// The initial state.
    Down = 1,

    /// The peer has detected a remote peer in the down state.
    Init = 2,

    /// The peer has detected a remote peer in the up or init state while in the
    /// init state.
    Up = 3,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct BfdStatus {
    pub peer: IpAddr,
    pub state: BfdState,
    pub switch: Name,
    pub local: Option<IpAddr>,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: ExternalBfdMode,
}

/// BFD connection mode.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
#[serde(rename_all = "snake_case")]
#[schemars(rename = "BfdMode")] // don't include "External..." in OpenAPI spec
pub enum ExternalBfdMode {
    SingleHop,
    MultiHop,
}
