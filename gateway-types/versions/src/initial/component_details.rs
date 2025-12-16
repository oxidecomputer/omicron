// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

// --------------------------------------
// Monorail port status-related types
// --------------------------------------

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SpComponentDetails {
    PortStatus(PortStatus),
    PortStatusError(PortStatusError),
    Measurement(Measurement),
    MeasurementError(MeasurementError),
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct PortStatusError {
    pub port: u32,
    pub code: PortStatusErrorCode,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum PortStatusErrorCode {
    Unconfigured,
    Other { raw: u32 },
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PortStatus {
    pub port: u32,
    pub cfg: PortConfig,
    pub link_status: LinkStatus,
    pub phy_status: Option<PhyStatus>,
    pub counters: PortCounters,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PortConfig {
    pub mode: PortMode,
    pub dev_type: PortDev,
    pub dev_num: u8,
    pub serdes_type: PortSerdes,
    pub serdes_num: u8,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PortDev {
    Dev1g,
    Dev2g5,
    Dev10g,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PortSerdes {
    Serdes1g,
    Serdes6g,
    Serdes10g,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "speed", rename_all = "snake_case")]
pub enum Speed {
    Speed100M,
    Speed1G,
    Speed10G,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PortMode {
    Sfi,
    BaseKr,
    Sgmii { speed: Speed },
    Qsgmii { speed: Speed },
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PacketCount {
    pub multicast: u32,
    pub unicast: u32,
    pub broadcast: u32,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PortCounters {
    pub rx: PacketCount,
    pub tx: PacketCount,
    pub link_down_sticky: bool,
    pub phy_link_down_sticky: bool,
}

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LinkStatus {
    Error,
    Down,
    Up,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PhyStatus {
    pub ty: PhyType,
    pub mac_link_up: LinkStatus,
    pub media_link_up: LinkStatus,
}

#[derive(
    Copy, Clone, Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PhyType {
    Vsc8504,
    Vsc8522,
    Vsc8552,
    Vsc8562,
}

/// Error type for `gateway_messages::ComponentDetails` that are not supported
/// by MGS proper and are only available via `faux-mgs`.
#[derive(Debug, thiserror::Error)]
#[error("unsupported component details: {description}")]
pub struct UnsupportedComponentDetails {
    pub description: String,
}

// --------------------------------------
// Measurement-related types
// --------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Measurement {
    pub name: String,
    pub kind: MeasurementKind,
    pub value: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct MeasurementError {
    pub name: String,
    pub kind: MeasurementKind,
    pub error: MeasurementErrorCode,
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum MeasurementErrorCode {
    InvalidSensor,
    NoReading,
    NotPresent,
    DeviceError,
    DeviceUnavailable,
    DeviceTimeout,
    DeviceOff,
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MeasurementKind {
    Temperature,
    Power,
    Current,
    Voltage,
    InputCurrent,
    InputVoltage,
    Speed,
    CpuTctl,
}
