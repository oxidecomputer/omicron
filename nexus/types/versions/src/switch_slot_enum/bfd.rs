// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2025_11_20_00;
use crate::v2025_11_20_00::bfd::BfdState;
use crate::v2026_03_06_01::format_switch_slot_as_name;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::SwitchSlot;
use std::net::IpAddr;

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
pub struct BfdStatus {
    pub peer: IpAddr,
    pub state: BfdState,
    pub switch_slot: SwitchSlot,
    pub local: Option<IpAddr>,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: BfdMode,
}

impl From<BfdStatus> for v2025_11_20_00::bfd::BfdStatus {
    fn from(value: BfdStatus) -> Self {
        let switch = format_switch_slot_as_name(value.switch_slot);
        Self {
            peer: value.peer,
            state: value.state,
            switch,
            local: value.local,
            detection_threshold: value.detection_threshold,
            required_rx: value.required_rx,
            mode: value.mode,
        }
    }
}
