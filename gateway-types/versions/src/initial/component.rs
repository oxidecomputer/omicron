// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::rot::RotState;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
    Diffable,
)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum SpType {
    Sled,
    Power,
    Switch,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SpIdentifier {
    #[serde(rename = "type")]
    pub typ: SpType,
    #[serde(deserialize_with = "deserializer_u16_from_string")]
    pub slot: u16,
}

// We can't use the default `Deserialize` derivation for `SpIdentifier::slot`
// because it's embedded in other structs via `serde(flatten)`, which does not
// play well with the way dropshot parses HTTP queries/paths. serde ends up
// trying to deserialize the flattened struct as a map of strings to strings,
// which breaks on `slot` (but not on `typ` for reasons I don't entirely
// understand). We can work around by using an enum that allows either `String`
// or `u16` (which gets us past the serde map of strings), and then parsing the
// string into a u16 ourselves (which gets us to the `slot` we want). More
// background: https://github.com/serde-rs/serde/issues/1346
fn deserializer_u16_from_string<'de, D>(
    deserializer: D,
) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Unexpected};

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum StringOrU16 {
        String(String),
        U16(u16),
    }

    match StringOrU16::deserialize(deserializer)? {
        StringOrU16::String(s) => s
            .parse()
            .map_err(|_| de::Error::invalid_type(Unexpected::Str(&s), &"u16")),
        StringOrU16::U16(n) => Ok(n),
    }
}

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
#[serde(rename_all = "snake_case")]
pub struct SpState {
    pub serial_number: String,
    pub model: String,
    pub revision: u32,
    pub hubris_archive_id: String,
    pub base_mac_address: [u8; 6],
    pub power_state: PowerState,
    pub rot: RotState,
}

/// See RFD 81.
///
/// This enum only lists power states the SP is able to control; higher power
/// states are controlled by ignition.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub enum PowerState {
    A0,
    A1,
    A2,
}

/// List of components from a single SP.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpComponentList {
    pub components: Vec<SpComponentInfo>,
}

/// Overview of a single SP component.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpComponentInfo {
    /// The unique identifier for this component.
    pub component: String,
    /// The name of the physical device.
    pub device: String,
    /// The component's serial number, if it has one.
    pub serial_number: Option<String>,
    /// A human-readable description of the component.
    pub description: String,
    /// `capabilities` is a bitmask; interpret it via
    /// [`gateway_messages::DeviceCapabilities`].
    pub capabilities: u32,
    /// Whether or not the component is present, to the best of the SP's ability
    /// to judge.
    pub presence: SpComponentPresence,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Description of the presence or absence of a component.
///
/// The presence of some components may vary based on the power state of the
/// sled (e.g., components that time out or appear unavailable if the sled is in
/// A2 may become present when the sled moves to A0).
pub enum SpComponentPresence {
    /// The component is present.
    Present,
    /// The component is not present.
    NotPresent,
    /// The component is present but in a failed or faulty state.
    Failed,
    /// The SP is unable to determine the presence of the component.
    Unavailable,
    /// The SP's attempt to determine the presence of the component timed out.
    Timeout,
    /// The SP's attempt to determine the presence of the component failed.
    Error,
}

/// Identifier for an SP's component's firmware slot; e.g., slots 0 and 1 for
/// the host boot flash.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SpComponentFirmwareSlot {
    pub slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponent {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct PathSpComponentFirmwareSlot {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub component: String,
    /// Firmware slot of the component.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct SetComponentActiveSlotParams {
    /// Persist this choice of active slot.
    pub persist: bool,
}
