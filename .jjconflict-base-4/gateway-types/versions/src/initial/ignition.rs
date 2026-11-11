// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::component::SpIdentifier;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct SpIgnitionInfo {
    pub id: SpIdentifier,
    pub details: SpIgnition,
}

/// State of an ignition target.
///
/// TODO: Ignition returns much more information than we're reporting here: do
/// we want to expand this?
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "present")]
pub enum SpIgnition {
    #[serde(rename = "no")]
    Absent,
    #[serde(rename = "yes")]
    Present {
        id: SpIgnitionSystemType,
        power: bool,
        ctrl_detect_0: bool,
        ctrl_detect_1: bool,
        flt_a3: bool,
        flt_a2: bool,
        flt_rot: bool,
        flt_sp: bool,
    },
}

/// TODO: Do we want to bake in specific board names, or use raw u16 ID numbers?
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "system_type", rename_all = "snake_case")]
pub enum SpIgnitionSystemType {
    Gimlet,
    Sidecar,
    Psc,
    Unknown { id: u16 },
}

/// Ignition command.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IgnitionCommand {
    PowerOn,
    PowerOff,
    PowerReset,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpIgnitionCommand {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// Ignition command to perform on the targeted SP.
    pub command: IgnitionCommand,
}
