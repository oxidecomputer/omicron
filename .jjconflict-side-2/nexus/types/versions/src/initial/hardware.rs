// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Hardware types for the Nexus external API.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Properties that uniquely identify an Oxide hardware component
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
pub struct Baseboard {
    pub serial: String,
    pub part: String,
    pub revision: u32,
}

/// A sled that has not been added to an initialized rack yet
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
pub struct UninitializedSled {
    pub baseboard: Baseboard,
    pub rack_id: Uuid,
    pub cubby: u16,
}

/// The unique hardware ID for a sled
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
pub struct UninitializedSledId {
    pub serial: String,
    pub part: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateableComponentType {
    BootloaderForRot,
    BootloaderForSp,
    BootloaderForHostProc,
    HubrisForPscRot,
    HubrisForPscSp,
    HubrisForSidecarRot,
    HubrisForSidecarSp,
    HubrisForGimletRot,
    HubrisForGimletSp,
    HeliosHostPhase1,
    HeliosHostPhase2,
    HostOmicron,
}
