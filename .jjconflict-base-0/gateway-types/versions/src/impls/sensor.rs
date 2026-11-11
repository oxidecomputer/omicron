// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::sensor::{SpSensorReading, SpSensorReadingResult};

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
