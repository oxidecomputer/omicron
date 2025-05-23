// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RotState {
    V2 {
        active: RotSlot,
        persistent_boot_preference: RotSlot,
        pending_persistent_boot_preference: Option<RotSlot>,
        transient_boot_preference: Option<RotSlot>,
        slot_a_sha3_256_digest: Option<String>,
        slot_b_sha3_256_digest: Option<String>,
    },
    CommunicationFailed {
        message: String,
    },
    V3 {
        active: RotSlot,
        persistent_boot_preference: RotSlot,
        pending_persistent_boot_preference: Option<RotSlot>,
        transient_boot_preference: Option<RotSlot>,

        slot_a_fwid: String,
        slot_b_fwid: String,
        stage0_fwid: String,
        stage0next_fwid: String,

        slot_a_error: Option<RotImageError>,
        slot_b_error: Option<RotImageError>,
        stage0_error: Option<RotImageError>,
        stage0next_error: Option<RotImageError>,
    },
}

impl RotState {
    fn fwid_to_string(fwid: gateway_messages::Fwid) -> String {
        match fwid {
            gateway_messages::Fwid::Sha3_256(digest) => hex::encode(digest),
        }
    }
}

impl From<Result<gateway_messages::RotState, gateway_messages::RotError>>
    for RotState
{
    fn from(
        result: Result<gateway_messages::RotState, gateway_messages::RotError>,
    ) -> Self {
        match result {
            Ok(state) => Self::from(state),
            Err(err) => Self::CommunicationFailed { message: err.to_string() },
        }
    }
}

impl From<Result<gateway_messages::RotStateV2, gateway_messages::RotError>>
    for RotState
{
    fn from(
        result: Result<
            gateway_messages::RotStateV2,
            gateway_messages::RotError,
        >,
    ) -> Self {
        match result {
            Ok(state) => Self::from(state),
            Err(err) => Self::CommunicationFailed { message: err.to_string() },
        }
    }
}

impl From<gateway_messages::RotState> for RotState {
    fn from(state: gateway_messages::RotState) -> Self {
        let boot_state = state.rot_updates.boot_state;
        Self::V2 {
            active: boot_state.active.into(),
            slot_a_sha3_256_digest: boot_state
                .slot_a
                .map(|details| hex::encode(details.digest)),
            slot_b_sha3_256_digest: boot_state
                .slot_b
                .map(|details| hex::encode(details.digest)),
            // RotState(V1) didn't have the following fields, so we make
            // it up as best we can. This RoT version is pre-shipping
            // and should only exist on (not updated recently) test
            // systems.
            persistent_boot_preference: boot_state.active.into(),
            pending_persistent_boot_preference: None,
            transient_boot_preference: None,
        }
    }
}

impl From<gateway_messages::RotStateV2> for RotState {
    fn from(state: gateway_messages::RotStateV2) -> Self {
        Self::V2 {
            active: state.active.into(),
            persistent_boot_preference: state.persistent_boot_preference.into(),
            pending_persistent_boot_preference: state
                .pending_persistent_boot_preference
                .map(Into::into),
            transient_boot_preference: state
                .transient_boot_preference
                .map(Into::into),
            slot_a_sha3_256_digest: state
                .slot_a_sha3_256_digest
                .map(hex::encode),
            slot_b_sha3_256_digest: state
                .slot_b_sha3_256_digest
                .map(hex::encode),
        }
    }
}

impl From<gateway_messages::RotStateV3> for RotState {
    fn from(state: gateway_messages::RotStateV3) -> Self {
        Self::V3 {
            active: state.active.into(),
            persistent_boot_preference: state.persistent_boot_preference.into(),
            pending_persistent_boot_preference: state
                .pending_persistent_boot_preference
                .map(Into::into),
            transient_boot_preference: state
                .transient_boot_preference
                .map(Into::into),
            slot_a_fwid: Self::fwid_to_string(state.slot_a_fwid),
            slot_b_fwid: Self::fwid_to_string(state.slot_b_fwid),

            stage0_fwid: Self::fwid_to_string(state.stage0_fwid),
            stage0next_fwid: Self::fwid_to_string(state.stage0next_fwid),

            slot_a_error: state.slot_a_status.err().map(From::from),
            slot_b_error: state.slot_b_status.err().map(From::from),

            stage0_error: state.stage0_status.err().map(From::from),
            stage0next_error: state.stage0next_status.err().map(From::from),
        }
    }
}

impl From<gateway_messages::RotBootInfo> for RotState {
    fn from(value: gateway_messages::RotBootInfo) -> Self {
        match value {
            gateway_messages::RotBootInfo::V1(s) => Self::from(s),
            gateway_messages::RotBootInfo::V2(s) => Self::from(s),
            gateway_messages::RotBootInfo::V3(s) => Self::from(s),
        }
    }
}

#[derive(
    Debug,
    Diffable,
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
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum RotSlot {
    A,
    B,
}

impl RotSlot {
    pub fn to_u16(&self) -> u16 {
        match self {
            RotSlot::A => 0,
            RotSlot::B => 1,
        }
    }

    pub fn toggled(&self) -> Self {
        match self {
            RotSlot::A => RotSlot::B,
            RotSlot::B => RotSlot::A,
        }
    }
}

impl FromStr for RotSlot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "a" | "A" => Ok(RotSlot::A),
            "b" | "B" => Ok(RotSlot::B),
            _ => Err(format!(
                "unrecognized value {} for RoT slot. \
                Must be one of `a`, `A`, `b`, or `B`",
                s
            )),
        }
    }
}

impl From<gateway_messages::RotSlotId> for RotSlot {
    fn from(slot: gateway_messages::RotSlotId) -> Self {
        match slot {
            gateway_messages::RotSlotId::A => Self::A,
            gateway_messages::RotSlotId::B => Self::B,
        }
    }
}

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
pub struct RotImageDetails {
    pub digest: String,
    pub version: ImageVersion,
}

impl From<gateway_messages::RotImageDetails> for RotImageDetails {
    fn from(details: gateway_messages::RotImageDetails) -> Self {
        Self {
            digest: hex::encode(details.digest),
            version: details.version.into(),
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
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct ImageVersion {
    pub epoch: u32,
    pub version: u32,
}

impl From<gateway_messages::ImageVersion> for ImageVersion {
    fn from(v: gateway_messages::ImageVersion) -> Self {
        Self { epoch: v.epoch, version: v.version }
    }
}

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
#[serde(rename_all = "snake_case")]
pub enum RotImageError {
    Unchecked,
    FirstPageErased,
    PartiallyProgrammed,
    InvalidLength,
    HeaderNotProgrammed,
    BootloaderTooSmall,
    BadMagic,
    HeaderImageSize,
    UnalignedLength,
    UnsupportedType,
    ResetVectorNotThumb2,
    ResetVector,
    Signature,
}

impl From<gateway_messages::ImageError> for RotImageError {
    fn from(error: gateway_messages::ImageError) -> Self {
        match error {
            gateway_messages::ImageError::Unchecked => RotImageError::Unchecked,
            gateway_messages::ImageError::FirstPageErased => {
                RotImageError::FirstPageErased
            }
            gateway_messages::ImageError::PartiallyProgrammed => {
                RotImageError::PartiallyProgrammed
            }
            gateway_messages::ImageError::InvalidLength => {
                RotImageError::InvalidLength
            }
            gateway_messages::ImageError::HeaderNotProgrammed => {
                RotImageError::HeaderNotProgrammed
            }
            gateway_messages::ImageError::BootloaderTooSmall => {
                RotImageError::BootloaderTooSmall
            }
            gateway_messages::ImageError::BadMagic => RotImageError::BadMagic,
            gateway_messages::ImageError::HeaderImageSize => {
                RotImageError::HeaderImageSize
            }
            gateway_messages::ImageError::UnalignedLength => {
                RotImageError::UnalignedLength
            }
            gateway_messages::ImageError::UnsupportedType => {
                RotImageError::UnsupportedType
            }
            gateway_messages::ImageError::ResetVectorNotThumb2 => {
                RotImageError::ResetVectorNotThumb2
            }
            gateway_messages::ImageError::ResetVector => {
                RotImageError::ResetVector
            }
            gateway_messages::ImageError::Signature => RotImageError::Signature,
        }
    }
}

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
pub struct RotCmpa {
    pub base64_data: String,
}

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
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum RotCfpaSlot {
    Active,
    Inactive,
    Scratch,
}

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
pub struct RotCfpa {
    pub base64_data: String,
    pub slot: RotCfpaSlot,
}
