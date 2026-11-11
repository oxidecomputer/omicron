// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    fmt,
    str::{self, FromStr},
};

use crate::latest::component::{
    PowerState, SpComponentPresence, SpIdentifier, SpState, SpType,
};
use crate::latest::rot::RotState;

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

impl fmt::Display for SpIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {}", self.typ, self.slot)
    }
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
