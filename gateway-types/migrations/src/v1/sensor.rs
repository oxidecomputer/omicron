// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Result of reading an SP sensor.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SpSensorReading {
    /// SP-centric timestamp of when `result` was recorded from this sensor.
    ///
    /// Currently this value represents "milliseconds since the last SP boot"
    /// and is primarily useful as a delta between sensors on this SP (assuming
    /// no reboot in between). The meaning could change with future SP releases.
    pub timestamp: u64,
    /// Value (or error) from the sensor.
    pub result: SpSensorReadingResult,
}

impl From<gateway_messages::SensorReading> for SpSensorReading {
    fn from(value: gateway_messages::SensorReading) -> Self {
        Self {
            timestamp: value.timestamp,
            result: match value.value {
                Ok(value) => SpSensorReadingResult::Success { value },
                Err(err) => err.into(),
            },
        }
    }
}

/// Single reading (or error) from an SP sensor.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpSensorReadingResult {
    Success { value: f32 },
    DeviceOff,
    DeviceError,
    DeviceNotPresent,
    DeviceUnavailable,
    DeviceTimeout,
}

impl From<gateway_messages::SensorDataMissing> for SpSensorReadingResult {
    fn from(value: gateway_messages::SensorDataMissing) -> Self {
        use gateway_messages::SensorDataMissing;
        match value {
            SensorDataMissing::DeviceOff => Self::DeviceOff,
            SensorDataMissing::DeviceError => Self::DeviceError,
            SensorDataMissing::DeviceNotPresent => Self::DeviceNotPresent,
            SensorDataMissing::DeviceUnavailable => Self::DeviceUnavailable,
            SensorDataMissing::DeviceTimeout => Self::DeviceTimeout,
        }
    }
}
