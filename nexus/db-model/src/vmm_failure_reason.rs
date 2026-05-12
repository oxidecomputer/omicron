// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes why a VMM record is in the `Failed` state.

use super::impl_enum_type;
use nexus_types::instance as types;
use serde::{Deserialize, Serialize};
use std::fmt;

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
    )]
    pub enum VmmFailureReason;

    // The reason for this VMM's failure is unknown, because the VMM failed
    // prior to the recording of failure reasons.
    Prehistoric => b"prehistoric"
    // The sled-agent reported that this VMM failed.
    FromSledAgent => b"from_sled_agent"
    // A request to the sled-agent received a response indicating that this
    // VMM is no longer present on the sled.
    NoSuchInstance => b"no_such_instance"
    // The sled on which this VMM was running has been expunged.
    SledExpunged => b"sled_expunged"
    // The sled on which this VMM was running has powered off.
    SledOff => b"sled_off"
);

impl From<types::VmmFailureReason> for VmmFailureReason {
    fn from(reason: types::VmmFailureReason) -> Self {
        match reason {
            types::VmmFailureReason::Prehistoric => Self::Prehistoric,
            types::VmmFailureReason::FromSledAgent => Self::FromSledAgent,
            types::VmmFailureReason::NoSuchInstance => Self::NoSuchInstance,
            types::VmmFailureReason::SledExpunged => Self::SledExpunged,
            types::VmmFailureReason::SledOff => Self::SledOff,
        }
    }
}

impl From<VmmFailureReason> for types::VmmFailureReason {
    fn from(reason: VmmFailureReason) -> Self {
        match reason {
            VmmFailureReason::Prehistoric => Self::Prehistoric,
            VmmFailureReason::FromSledAgent => Self::FromSledAgent,
            VmmFailureReason::NoSuchInstance => Self::NoSuchInstance,
            VmmFailureReason::SledExpunged => Self::SledExpunged,
            VmmFailureReason::SledOff => Self::SledOff,
        }
    }
}

impl VmmFailureReason {
    pub fn from_vmm_state(state: types::VmmState) -> Option<Self> {
        match state {
            types::VmmState::Failed(reason) => Some(reason.into()),
            _ => None,
        }
    }

    pub fn description(&self) -> &'static str {
        types::VmmFailureReason::from(*self).description()
    }
}

impl fmt::Display for VmmFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        types::VmmFailureReason::from(*self).fmt(f)
    }
}
