// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
#[cfg_attr(any(test, feature = "testing"), derive(test_strategy::Arbitrary))]
pub enum RotSlot {
    A,
    B,
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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetCfpaParams {
    pub slot: RotCfpaSlot,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema,
)]
pub struct GetRotBootInfoParams {
    pub version: u8,
}
