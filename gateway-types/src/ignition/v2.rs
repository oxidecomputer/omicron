// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::component::SpIdentifier;

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

impl From<SpIgnitionInfo> for crate::ignition::v1::SpIgnitionInfo {
    fn from(s: SpIgnitionInfo) -> Self {
        Self { id: s.id, details: s.details.into() }
    }
}

/// State of an ignition target.
//
// TODO: Ignition returns much more information than we're reporting here: do
// we want to expand this?
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
        /// Fault from the A3 power domain
        flt_a3: bool,
        /// Fault from the A2 power domain
        flt_a2: bool,
        /// Fault from the RoT
        flt_rot: bool,
        /// Fault from the SP
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

impl From<SpIgnition> for crate::ignition::v1::SpIgnition {
    fn from(state: SpIgnition) -> Self {
        match state {
            SpIgnition::Absent => Self::Absent,
            SpIgnition::Present {
                id,
                power,
                ctrl_detect_0,
                ctrl_detect_1,
                flt_a3,
                flt_a2,
                flt_rot,
                flt_sp,
            } => Self::Present {
                id: id.into(),
                power,
                ctrl_detect_0,
                ctrl_detect_1,
                flt_a3,
                flt_a2,
                flt_rot,
                flt_sp,
            },
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
    Cosmo,
}

impl From<gateway_messages::ignition::SystemType> for SpIgnitionSystemType {
    fn from(st: gateway_messages::ignition::SystemType) -> Self {
        use gateway_messages::ignition::SystemType;
        match st {
            SystemType::Gimlet => Self::Gimlet,
            SystemType::Sidecar => Self::Sidecar,
            SystemType::Psc => Self::Psc,
            SystemType::Unknown(id) => Self::Unknown { id },
            SystemType::Cosmo => Self::Cosmo,
        }
    }
}

impl From<SpIgnitionSystemType> for crate::ignition::v1::SpIgnitionSystemType {
    fn from(st: SpIgnitionSystemType) -> Self {
        match st {
            SpIgnitionSystemType::Gimlet => Self::Gimlet,
            SpIgnitionSystemType::Sidecar => Self::Sidecar,
            SpIgnitionSystemType::Psc => Self::Psc,
            // Cosmo system id is 0x4
            SpIgnitionSystemType::Cosmo => Self::Unknown { id: 0x4 },
            SpIgnitionSystemType::Unknown { id } => Self::Unknown { id },
        }
    }
}
