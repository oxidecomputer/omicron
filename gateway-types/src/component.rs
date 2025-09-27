// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    fmt,
    str::{self, FromStr},
};

use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::rot::RotState;

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

impl fmt::Display for SpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpType::Sled => write!(f, "sled"),
            SpType::Power => write!(f, "power"),
            SpType::Switch => write!(f, "switch"),
        }
    }
}

impl FromStr for SpType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sled" => Ok(SpType::Sled),
            "power" => Ok(SpType::Power),
            "switch" => Ok(SpType::Switch),
            _ => Err(format!("invalid SpType: {}", s)),
        }
    }
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

impl fmt::Display for SpIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {}", self.typ, self.slot)
    }
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

// We expect serial and model numbers to be ASCII and 0-padded: find the first 0
// byte and convert to a string. If that fails, hexlify the entire slice.
fn stringify_byte_string(bytes: &[u8]) -> String {
    let first_zero = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());

    str::from_utf8(&bytes[..first_zero])
        .map(|s| s.to_string())
        .unwrap_or_else(|_err| hex::encode(bytes))
}

impl From<(gateway_messages::SpStateV1, RotState)> for SpState {
    fn from(all: (gateway_messages::SpStateV1, RotState)) -> Self {
        let (state, rot) = all;
        Self {
            serial_number: stringify_byte_string(&state.serial_number),
            model: stringify_byte_string(&state.model),
            revision: state.revision,
            hubris_archive_id: hex::encode(state.hubris_archive_id),
            base_mac_address: state.base_mac_address,
            power_state: PowerState::from(state.power_state),
            rot,
        }
    }
}

impl From<gateway_messages::SpStateV1> for SpState {
    fn from(state: gateway_messages::SpStateV1) -> Self {
        Self::from((state, RotState::from(state.rot)))
    }
}

impl From<(gateway_messages::SpStateV2, RotState)> for SpState {
    fn from(all: (gateway_messages::SpStateV2, RotState)) -> Self {
        let (state, rot) = all;
        Self {
            serial_number: stringify_byte_string(&state.serial_number),
            model: stringify_byte_string(&state.model),
            revision: state.revision,
            hubris_archive_id: hex::encode(state.hubris_archive_id),
            base_mac_address: state.base_mac_address,
            power_state: PowerState::from(state.power_state),
            rot,
        }
    }
}

impl From<gateway_messages::SpStateV2> for SpState {
    fn from(state: gateway_messages::SpStateV2) -> Self {
        Self::from((state, RotState::from(state.rot)))
    }
}

impl From<(gateway_messages::SpStateV3, RotState)> for SpState {
    fn from(all: (gateway_messages::SpStateV3, RotState)) -> Self {
        let (state, rot) = all;
        Self {
            serial_number: stringify_byte_string(&state.serial_number),
            model: stringify_byte_string(&state.model),
            revision: state.revision,
            hubris_archive_id: hex::encode(state.hubris_archive_id),
            base_mac_address: state.base_mac_address,
            power_state: PowerState::from(state.power_state),
            rot,
        }
    }
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

impl From<gateway_messages::PowerState> for PowerState {
    fn from(power_state: gateway_messages::PowerState) -> Self {
        match power_state {
            gateway_messages::PowerState::A0 => Self::A0,
            gateway_messages::PowerState::A1 => Self::A1,
            gateway_messages::PowerState::A2 => Self::A2,
        }
    }
}

impl From<PowerState> for gateway_messages::PowerState {
    fn from(power_state: PowerState) -> Self {
        match power_state {
            PowerState::A0 => Self::A0,
            PowerState::A1 => Self::A1,
            PowerState::A2 => Self::A2,
        }
    }
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

impl From<gateway_messages::DevicePresence> for SpComponentPresence {
    fn from(p: gateway_messages::DevicePresence) -> Self {
        match p {
            gateway_messages::DevicePresence::Present => Self::Present,
            gateway_messages::DevicePresence::NotPresent => Self::NotPresent,
            gateway_messages::DevicePresence::Failed => Self::Failed,
            gateway_messages::DevicePresence::Unavailable => Self::Unavailable,
            gateway_messages::DevicePresence::Timeout => Self::Timeout,
            gateway_messages::DevicePresence::Error => Self::Error,
        }
    }
}

/// Identifier for an SP's component's firmware slot; e.g., slots 0 and 1 for
/// the host boot flash.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SpComponentFirmwareSlot {
    pub slot: u16,
}
