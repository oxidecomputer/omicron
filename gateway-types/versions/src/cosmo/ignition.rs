// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1;
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

impl From<SpIgnitionInfo> for v1::ignition::SpIgnitionInfo {
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

impl From<SpIgnition> for v1::ignition::SpIgnition {
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

impl From<SpIgnitionSystemType> for v1::ignition::SpIgnitionSystemType {
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
