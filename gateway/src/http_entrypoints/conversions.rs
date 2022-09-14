// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Conversions between externally-defined types and HTTP / JsonSchema types.

use super::Progress;
use super::SpIdentifier;
use super::SpIgnition;
use super::SpState;
use super::SpType;
use super::SpUpdateStatus;
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
        use super::SpUpdateState;
        match status {
            UpdateStatus::Preparing(status) => Self {
                id: status.id.into(),
                state: SpUpdateState::Preparing {
                    progress: status.progress.map(Into::into),
                },
            },
            UpdateStatus::InProgress(status) => Self {
                id: status.id.into(),
                state: SpUpdateState::InProgress { progress: status.into() },
            },
            UpdateStatus::Complete(id) => {
                Self { id: id.into(), state: SpUpdateState::Complete }
            }
            UpdateStatus::Aborted(id) => {
                Self { id: id.into(), state: SpUpdateState::Aborted }
            }
        }
    }
}

impl From<gateway_messages::UpdatePreparationProgress> for Progress {
    fn from(progress: gateway_messages::UpdatePreparationProgress) -> Self {
        Self { current: progress.current, total: progress.total }
    }
}

impl From<gateway_messages::UpdateInProgressStatus> for Progress {
    fn from(progress: gateway_messages::UpdateInProgressStatus) -> Self {
        Self { current: progress.bytes_received, total: progress.total_size }
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
