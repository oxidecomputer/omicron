// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Conversions between externally-defined types and HTTP / JsonSchema types.

use super::PowerState;
use super::SpComponentInfo;
use super::SpComponentList;
use super::SpComponentPresence;
use super::SpIdentifier;
use super::SpIgnition;
use super::SpState;
use super::SpType;
use super::SpUpdateStatus;
use super::UpdatePreparationProgress;
use dropshot::HttpError;
use gateway_messages::IgnitionFlags;
use gateway_messages::SpComponent;
use gateway_messages::UpdateStatus;

// wrap `SpComponent::try_from(&str)` into a usable form for dropshot endpoints
pub(super) fn component_from_str(s: &str) -> Result<SpComponent, HttpError> {
    SpComponent::try_from(s).map_err(|_| {
        HttpError::for_bad_request(
            Some("InvalidSpComponent".to_string()),
            "invalid SP component name".to_string(),
        )
    })
}

impl From<UpdateStatus> for SpUpdateStatus {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::None => Self::None,
            UpdateStatus::Preparing(status) => Self::Preparing {
                id: status.id.into(),
                progress: status.progress.map(Into::into),
            },
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id, total_size, ..
            } => Self::InProgress {
                id: id.into(),
                bytes_received: 0,
                total_bytes: total_size,
            },
            UpdateStatus::InProgress(status) => Self::InProgress {
                id: status.id.into(),
                bytes_received: status.bytes_received,
                total_bytes: status.total_size,
            },
            UpdateStatus::Complete(id) => Self::Complete { id: id.into() },
            UpdateStatus::Aborted(id) => Self::Aborted { id: id.into() },
            UpdateStatus::Failed { id, code } => {
                Self::Failed { id: id.into(), code }
            }
        }
    }
}

impl From<gateway_messages::UpdatePreparationProgress>
    for UpdatePreparationProgress
{
    fn from(progress: gateway_messages::UpdatePreparationProgress) -> Self {
        Self { current: progress.current, total: progress.total }
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
    fn from(power_state: PowerState) -> gateway_messages::PowerState {
        match power_state {
            PowerState::A0 => gateway_messages::PowerState::A0,
            PowerState::A1 => gateway_messages::PowerState::A1,
            PowerState::A2 => gateway_messages::PowerState::A2,
        }
    }
}

impl From<gateway_messages::SpState> for SpState {
    fn from(state: gateway_messages::SpState) -> Self {
        Self::Enabled { serial_number: hex::encode(&state.serial_number[..]) }
    }
}

impl From<gateway_messages::IgnitionState> for SpIgnition {
    fn from(state: gateway_messages::IgnitionState) -> Self {
        // if we have a state, the SP was present
        Self::Present {
            id: state.id,
            power: state.flags.intersects(IgnitionFlags::POWER),
            ctrl_detect_0: state.flags.intersects(IgnitionFlags::CTRL_DETECT_0),
            ctrl_detect_1: state.flags.intersects(IgnitionFlags::CTRL_DETECT_1),
            flt_a3: state.flags.intersects(IgnitionFlags::FLT_A3),
            flt_a2: state.flags.intersects(IgnitionFlags::FLT_A2),
            flt_rot: state.flags.intersects(IgnitionFlags::FLT_ROT),
            flt_sp: state.flags.intersects(IgnitionFlags::FLT_SP),
        }
    }
}

impl From<SpType> for gateway_sp_comms::SpType {
    fn from(typ: SpType) -> Self {
        match typ {
            SpType::Sled => Self::Sled,
            SpType::Power => Self::Power,
            SpType::Switch => Self::Switch,
        }
    }
}

impl From<gateway_sp_comms::SpType> for SpType {
    fn from(typ: gateway_sp_comms::SpType) -> Self {
        match typ {
            gateway_sp_comms::SpType::Sled => Self::Sled,
            gateway_sp_comms::SpType::Power => Self::Power,
            gateway_sp_comms::SpType::Switch => Self::Switch,
        }
    }
}

impl From<SpIdentifier> for gateway_sp_comms::SpIdentifier {
    fn from(id: SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot may come from an untrusted source, but usize >= 32 bits
            // on any platform that will run this code, so unwrap is fine
            slot: usize::try_from(id.slot).unwrap(),
        }
    }
}

impl From<gateway_sp_comms::SpIdentifier> for SpIdentifier {
    fn from(id: gateway_sp_comms::SpIdentifier) -> Self {
        Self {
            typ: id.typ.into(),
            // id.slot comes from a trusted source (gateway_sp_comms) and will
            // not exceed u32::MAX
            slot: u32::try_from(id.slot).unwrap(),
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

impl From<gateway_sp_comms::SpDevice> for SpComponentInfo {
    fn from(d: gateway_sp_comms::SpDevice) -> Self {
        Self {
            component: d.component.as_str().unwrap_or("???").to_string(),
            device: d.device,
            serial_number: None, // TODO populate when SP provides it
            description: d.description,
            capabilities: d.capabilities.bits(),
            presence: d.presence.into(),
        }
    }
}

impl From<gateway_sp_comms::SpInventory> for SpComponentList {
    fn from(inv: gateway_sp_comms::SpInventory) -> Self {
        Self { components: inv.devices.into_iter().map(Into::into).collect() }
    }
}
