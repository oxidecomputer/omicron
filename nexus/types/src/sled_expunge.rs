// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use std::fmt;
use trust_quorum_types::types::Epoch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeRequest {
    pub id: SledExpungeRequestUuid,
    pub rack_id: RackUuid,
    pub sled_id: SledUuid,
    pub baseboard_id: BaseboardId,
    pub tq_epoch: Epoch,
    pub state: SledExpungeRequestState,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestState {
    WaitingForTrustQuorum {
        cubby: Option<SledExpungeRecordedCubby>,
        last_error: Option<String>,
    },
    PoweringOff {
        cubby: Option<SledExpungeRecordedCubby>,
        last_error: Option<String>,
    },
    Expunging {
        power_off: SledExpungePowerOff,
        last_error: Option<String>,
    },
    Done {
        power_off: SledExpungePowerOff,
        time_completed: DateTime<Utc>,
    },
    Failed {
        power_off: SledExpungePowerOffProgress,
        failure_reason: String,
        time_completed: DateTime<Utc>,
    },
}

impl SledExpungeRequestState {
    pub fn kind(&self) -> SledExpungeRequestStateKind {
        match self {
            Self::WaitingForTrustQuorum { .. } => {
                SledExpungeRequestStateKind::WaitingForTrustQuorum
            }
            Self::PoweringOff { .. } => {
                SledExpungeRequestStateKind::PoweringOff
            }
            Self::Expunging { .. } => SledExpungeRequestStateKind::Expunging,
            Self::Done { .. } => SledExpungeRequestStateKind::Done,
            Self::Failed { .. } => SledExpungeRequestStateKind::Failed,
        }
    }

    pub fn recorded_cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::WaitingForTrustQuorum { cubby, last_error: _ }
            | Self::PoweringOff { cubby, last_error: _ } => *cubby,
            Self::Expunging { power_off, last_error: _ }
            | Self::Done { power_off, time_completed: _ } => power_off.cubby(),
            Self::Failed {
                power_off,
                failure_reason: _,
                time_completed: _,
            } => power_off.cubby(),
        }
    }
}

#[derive(
    Copy, Clone, Debug, PartialEq, Eq, strum::VariantArray, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum SledExpungeRequestStateKind {
    WaitingForTrustQuorum,
    PoweringOff,
    Expunging,
    Done,
    Failed,
}

impl SledExpungeRequestStateKind {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub const UNFINISHED: &'static [Self] =
        &[Self::WaitingForTrustQuorum, Self::PoweringOff, Self::Expunging];

    pub fn is_finished(&self) -> bool {
        match self {
            Self::WaitingForTrustQuorum
            | Self::PoweringOff
            | Self::Expunging => false,
            Self::Done | Self::Failed => true,
        }
    }

    pub fn label(&self) -> &'static str {
        self.into()
    }
}

impl fmt::Display for SledExpungeRequestStateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SledExpungeRecordedCubby {
    pub slot: u16,
    pub source: SledExpungeSpSlotSource,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeSpSlotSource {
    Inventory,
    LiveScan,
    FoundAfterAbsent,
    Relocated,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungePowerOff {
    Cubby {
        cubby: SledExpungeRecordedCubby,
        outcome: SledExpungeCubbyPowerOffOutcome,
    },
    AbsentAtCreation,
}

impl SledExpungePowerOff {
    pub fn cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::Cubby { cubby, outcome: _ } => Some(*cubby),
            Self::AbsentAtCreation => None,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCubbyPowerOffOutcome {
    PoweredOff,
    AlreadyOff,
    OtherBoardPresent,
    NotPresent,
}

// TODO-RAINCLAUDE: How far the power-off step got, for a request that failed.
//
// A request can fail before the power-off step settles. For example, its
// removal can be aborted while it is still `waiting_for_trust_quorum`. That
// request has no outcome, but it still has whatever cubby was recorded for it
// by then (possibly none). `Pending` carries that cubby, so that it isn't lost
// when the row is decoded.
//
// A request that fails while it is `expunging` already has an outcome, and
// `Settled` carries it. The outcome includes its own cubby.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungePowerOffProgress {
    Pending { cubby: Option<SledExpungeRecordedCubby> },
    Settled(SledExpungePowerOff),
}

impl SledExpungePowerOffProgress {
    pub fn cubby(&self) -> Option<SledExpungeRecordedCubby> {
        match self {
            Self::Pending { cubby } => *cubby,
            Self::Settled(power_off) => power_off.cubby(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unfinished_states_consistent() {
        for &kind in SledExpungeRequestStateKind::ALL {
            assert_eq!(
                SledExpungeRequestStateKind::UNFINISHED.contains(&kind),
                !kind.is_finished(),
                "SledExpungeRequestStateKind::{kind:?}: UNFINISHED and \
                 is_finished() disagree",
            );
        }
    }
}
