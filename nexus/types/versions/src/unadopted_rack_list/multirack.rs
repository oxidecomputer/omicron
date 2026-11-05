// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus types for external APIs related to multirack.

use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;
use uuid::Uuid;

/// View of a rack that's joined the underlay network but hasn't been adopted
/// into the regional cluster yet
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnadoptedRack {
    pub rack_id: Uuid,
    pub rack_subnet: Ipv6Net,
    pub unadopted_sleds: Vec<UnadoptedSled>,
}

/// View of an unadopted sled in an unadopted rack
///
/// This is distinct from an `UninitializedSled`, which represents an
/// uninitialized sled in an *adopted* rack; by contrast, this represents a sled
/// in a rack that hasn't been adopted yet.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct UnadoptedSled {
    pub sled_id: Uuid,
    pub baseboard_id: BaseboardId,

    pub sled_agent_started: bool,
    pub last_start_err: Option<String>,
}
