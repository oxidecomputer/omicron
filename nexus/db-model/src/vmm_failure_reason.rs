// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes why a VMM record is in the `Failed` state.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    VmmFailureReasonEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::Display,
        strum::IntoStaticStr,
    )]
    #[strum(serialize_all = "snake_case")]
    pub enum VmmFailureReason;

    // The reason for this VMM's failure is unknown, because the VMM failed
    // prior to the recording of failure reasons.
    Prehistoric => b"prehistoric"
    // The sled-agent reported that this VMM failed.
    FromSledAgent => b"from_sled_agent"
    // A request to the sled-agent received a response indicating that this
    // VMM is no longer present on the sled.
    NoSuchVmm => b"no_such_vmm"
    // The sled on which this VMM was running has been expunged.
    SledExpunged => b"sled_expunged"
    // The sled on which this VMM was running has powered off.
    SledOff => b"sled_off"
);

impl VmmFailureReason {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Prehistoric => "<VMM failed prior to recorded history>",
            Self::FromSledAgent => "failed VMM state received from sled-agent",
            Self::NoSuchVmm => "VMM no longer present on sled",
            Self::SledExpunged => {
                "the sled this VMM was running on has been expunged"
            }
            Self::SledOff => "the sled this VMM was running on powered off",
        }
    }
}
