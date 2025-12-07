// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::component::SpIdentifier;

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

impl From<gateway_messages::IgnitionState> for SpIgnition {
    fn from(state: gateway_messages::IgnitionState) -> Self {
        use gateway_messages::ignition::SystemPowerState;

        if let Some(target_state) = state.target {
            Self::Present {
                id: target_state.system_type.into(),
                power: matches!(
                    target_state.power_state,
                    SystemPowerState::On | SystemPowerState::PoweringOn
                ),
                ctrl_detect_0: target_state.controller0_present,
                ctrl_detect_1: target_state.controller1_present,
                flt_a3: target_state.faults.power_a3,
                flt_a2: target_state.faults.power_a2,
                flt_rot: target_state.faults.rot,
                flt_sp: target_state.faults.sp,
            }
        } else {
            Self::Absent
        }
    }
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

impl From<gateway_messages::ignition::SystemType> for SpIgnitionSystemType {
    fn from(st: gateway_messages::ignition::SystemType) -> Self {
        use gateway_messages::ignition::SystemType;
        match st {
            SystemType::Gimlet => Self::Gimlet,
            SystemType::Sidecar => Self::Sidecar,
            SystemType::Psc => Self::Psc,
            SystemType::Unknown(id) => Self::Unknown { id },
            // `0x4` is the ignition value per RFD 142
            SystemType::Cosmo => Self::Unknown { id: 0x4 },
        }
    }
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

impl From<IgnitionCommand> for gateway_messages::IgnitionCommand {
    fn from(cmd: IgnitionCommand) -> Self {
        match cmd {
            IgnitionCommand::PowerOn => {
                gateway_messages::IgnitionCommand::PowerOn
            }
            IgnitionCommand::PowerOff => {
                gateway_messages::IgnitionCommand::PowerOff
            }
            IgnitionCommand::PowerReset => {
                gateway_messages::IgnitionCommand::PowerReset
            }
        }
    }
}
