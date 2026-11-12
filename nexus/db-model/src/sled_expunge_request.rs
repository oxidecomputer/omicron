// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::SpMgsSlot;
use crate::typed_uuid::DbTypedUuid;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::sled_expunge_request;
use nexus_types::sled_expunge::FinishedSledExpungeRequestState;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestKind;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use std::fmt;

impl_enum_type!(
    SledExpungeRequestStateEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        AsExpression,
        FromSqlRow,
        strum::VariantArray,
    )]
    pub enum SledExpungeRequestState;

    WaitingForTrustQuorum => b"waiting_for_trust_quorum"
    PoweringOff => b"powering_off"
    Expunging => b"expunging"
    Done => b"done"
    Failed => b"failed"
);

impl SledExpungeRequestState {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub const UNFINISHED: &'static [Self] =
        &[Self::WaitingForTrustQuorum, Self::PoweringOff, Self::Expunging];

    pub fn is_finished(&self) -> bool {
        self.as_unfinished().is_none()
    }

    pub fn as_unfinished(&self) -> Option<SledExpungeRequestUnfinishedState> {
        match self {
            Self::WaitingForTrustQuorum => {
                Some(SledExpungeRequestUnfinishedState::WaitingForTrustQuorum)
            }
            Self::PoweringOff => {
                Some(SledExpungeRequestUnfinishedState::PoweringOff)
            }
            Self::Expunging => {
                Some(SledExpungeRequestUnfinishedState::Expunging)
            }
            Self::Done | Self::Failed => None,
        }
    }

    pub fn as_finished(&self) -> Option<FinishedSledExpungeRequestState> {
        match self {
            Self::Done => Some(FinishedSledExpungeRequestState::Done),
            Self::Failed => Some(FinishedSledExpungeRequestState::Failed),
            Self::WaitingForTrustQuorum
            | Self::PoweringOff
            | Self::Expunging => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::WaitingForTrustQuorum => "waiting_for_trust_quorum",
            Self::PoweringOff => "powering_off",
            Self::Expunging => "expunging",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, strum::VariantArray)]
pub enum SledExpungeRequestUnfinishedState {
    WaitingForTrustQuorum,
    PoweringOff,
    Expunging,
}

impl SledExpungeRequestUnfinishedState {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn state(&self) -> SledExpungeRequestState {
        match self {
            Self::WaitingForTrustQuorum => {
                SledExpungeRequestState::WaitingForTrustQuorum
            }
            Self::PoweringOff => SledExpungeRequestState::PoweringOff,
            Self::Expunging => SledExpungeRequestState::Expunging,
        }
    }
}

impl fmt::Display for SledExpungeRequestUnfinishedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.state().fmt(f)
    }
}

impl fmt::Display for SledExpungeRequestState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl_enum_type!(
    SledExpungePowerOffOutcomeEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        AsExpression,
        FromSqlRow,
        strum::VariantArray,
    )]
    pub enum SledExpungePowerOffOutcome;

    PoweredOff => b"powered_off"
    AlreadyOff => b"already_off"
    OtherBoardPresent => b"other_board_present"
    NotPresent => b"not_present"
    AbsentAtCreation => b"absent_at_creation"
);

impl SledExpungePowerOffOutcome {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn label(&self) -> &'static str {
        match self {
            Self::PoweredOff => "powered_off",
            Self::AlreadyOff => "already_off",
            Self::OtherBoardPresent => "other_board_present",
            Self::NotPresent => "not_present",
            Self::AbsentAtCreation => "absent_at_creation",
        }
    }
}

impl fmt::Display for SledExpungePowerOffOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

// TODO-RAINCLAUDE: what request creation found about where the sled is, mapped to the sp_slot column: Present records the cubby its SP was found in, and Gone (neither its SP nor its sled agent in the latest collection, confirmed by ignition) records no cubby.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCreationPresence {
    Present { sp_slot: SpMgsSlot },
    Gone,
}

impl SledExpungeCreationPresence {
    pub fn sp_slot(&self) -> Option<SpMgsSlot> {
        match self {
            Self::Present { sp_slot } => Some(*sp_slot),
            Self::Gone => None,
        }
    }
}

// TODO-RAINCLAUDE: the outcomes that come from checking a recorded cubby; each needs the request's sp_slot to be set, which SledExpungePowerOff::Cubby carries.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeCubbyPowerOffOutcome {
    PoweredOff,
    AlreadyOff,
    OtherBoardPresent,
    NotPresent,
}

impl From<SledExpungeCubbyPowerOffOutcome> for SledExpungePowerOffOutcome {
    fn from(outcome: SledExpungeCubbyPowerOffOutcome) -> Self {
        match outcome {
            SledExpungeCubbyPowerOffOutcome::PoweredOff => Self::PoweredOff,
            SledExpungeCubbyPowerOffOutcome::AlreadyOff => Self::AlreadyOff,
            SledExpungeCubbyPowerOffOutcome::OtherBoardPresent => {
                Self::OtherBoardPresent
            }
            SledExpungeCubbyPowerOffOutcome::NotPresent => Self::NotPresent,
        }
    }
}

// TODO-RAINCLAUDE: how the powering_off step ended, as the (sp_slot, power_off_outcome) pair the sp_slot_matches_power_off_outcome CHECK allows: a cubby outcome names the cubby it checked, and AbsentAtCreation has none. The advance to expunging compare-and-swaps on that sp_slot too, so a pair that no longer matches the row is NotInExpectedState rather than a CHECK violation.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungePowerOff {
    Cubby { sp_slot: SpMgsSlot, outcome: SledExpungeCubbyPowerOffOutcome },
    AbsentAtCreation,
}

impl SledExpungePowerOff {
    pub fn sp_slot(&self) -> Option<SpMgsSlot> {
        match self {
            Self::Cubby { sp_slot, outcome: _ } => Some(*sp_slot),
            Self::AbsentAtCreation => None,
        }
    }

    pub fn outcome(&self) -> SledExpungePowerOffOutcome {
        match self {
            Self::Cubby { sp_slot: _, outcome } => (*outcome).into(),
            Self::AbsentAtCreation => {
                SledExpungePowerOffOutcome::AbsentAtCreation
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestAdvance {
    ToPoweringOff,
    ToExpunging { power_off: SledExpungePowerOff },
}

impl SledExpungeRequestAdvance {
    pub fn from_state(&self) -> SledExpungeRequestUnfinishedState {
        match self {
            Self::ToPoweringOff => {
                SledExpungeRequestUnfinishedState::WaitingForTrustQuorum
            }
            Self::ToExpunging { .. } => {
                SledExpungeRequestUnfinishedState::PoweringOff
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeRequestTransition {
    Advance(SledExpungeRequestAdvance),
    Done,
    Failed { from: SledExpungeRequestUnfinishedState, failure_reason: String },
}

impl SledExpungeRequestTransition {
    pub fn from_state(&self) -> SledExpungeRequestUnfinishedState {
        match self {
            Self::Advance(advance) => advance.from_state(),
            Self::Done => SledExpungeRequestUnfinishedState::Expunging,
            Self::Failed { from, .. } => *from,
        }
    }

    pub fn sp_slot_precondition(&self) -> SledExpungeSpSlotPrecondition {
        match self {
            Self::Advance(SledExpungeRequestAdvance::ToExpunging {
                power_off,
            }) => SledExpungeSpSlotPrecondition::Equals(power_off.sp_slot()),
            Self::Advance(SledExpungeRequestAdvance::ToPoweringOff)
            | Self::Done
            | Self::Failed { .. } => SledExpungeSpSlotPrecondition::Any,
        }
    }

    // TODO-RAINCLAUDE: the one place that maps a transition to the columns it writes, shared by the datastore transitions and the test insert helper; every transition clears last_error, and Done and Failed keep the power-off outcome already on the row.
    pub fn columns(
        self,
        now: DateTime<Utc>,
    ) -> SledExpungeRequestTransitionColumns {
        let (state, power_off_outcome, failure_reason, time_completed) =
            match self {
                Self::Advance(SledExpungeRequestAdvance::ToPoweringOff) => {
                    (SledExpungeRequestState::PoweringOff, None, None, None)
                }
                Self::Advance(SledExpungeRequestAdvance::ToExpunging {
                    power_off,
                }) => (
                    SledExpungeRequestState::Expunging,
                    Some(power_off.outcome()),
                    None,
                    None,
                ),
                Self::Done => {
                    (SledExpungeRequestState::Done, None, None, Some(now))
                }
                Self::Failed { from: _, failure_reason } => (
                    SledExpungeRequestState::Failed,
                    None,
                    Some(failure_reason),
                    Some(now),
                ),
            };
        SledExpungeRequestTransitionColumns {
            state,
            power_off_outcome,
            failure_reason,
            last_error: None,
            time_completed,
            time_modified: now,
        }
    }
}

// TODO-RAINCLAUDE: the sp_slot a transition's compare-and-swap requires besides the from state; Equals(None) means sp_slot IS NULL.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SledExpungeSpSlotPrecondition {
    Any,
    Equals(Option<SpMgsSlot>),
}

// TODO-RAINCLAUDE: built only by SledExpungeRequestTransition::columns.
#[derive(AsChangeset, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = sled_expunge_request, treat_none_as_null = true)]
pub struct SledExpungeRequestTransitionColumns {
    state: SledExpungeRequestState,
    #[diesel(treat_none_as_null = false)]
    power_off_outcome: Option<SledExpungePowerOffOutcome>,
    failure_reason: Option<String>,
    last_error: Option<String>,
    time_completed: Option<DateTime<Utc>>,
    time_modified: DateTime<Utc>,
}

impl SledExpungeRequestTransitionColumns {
    pub fn apply_to(self, request: SledExpungeRequest) -> SledExpungeRequest {
        let Self {
            state,
            power_off_outcome,
            failure_reason,
            last_error,
            time_completed,
            time_modified,
        } = self;
        SledExpungeRequest {
            state,
            power_off_outcome: power_off_outcome.or(request.power_off_outcome),
            failure_reason,
            last_error,
            time_completed,
            time_modified,
            ..request
        }
    }
}

// TODO-RAINCLAUDE: sled_id, rack_id, the baseboard (part and serial number) and tq_epoch are immutable after insert.
#[derive(Queryable, Insertable, Selectable, Clone, Debug, PartialEq, Eq)]
#[diesel(table_name = sled_expunge_request)]
pub struct SledExpungeRequest {
    pub id: DbTypedUuid<SledExpungeRequestKind>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub rack_id: DbTypedUuid<RackKind>,
    pub sled_id: DbTypedUuid<SledKind>,
    pub part_number: String,
    pub serial_number: String,
    pub sp_slot: Option<SpMgsSlot>,
    pub tq_epoch: i64,
    pub state: SledExpungeRequestState,
    pub power_off_outcome: Option<SledExpungePowerOffOutcome>,
    pub last_error: Option<String>,
    pub failure_reason: Option<String>,
    pub time_completed: Option<DateTime<Utc>>,
}

impl SledExpungeRequest {
    pub fn new(
        rack_id: RackUuid,
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        sp_slot: Option<SpMgsSlot>,
        tq_epoch: i64,
    ) -> Self {
        let BaseboardId { part_number, serial_number } = baseboard_id;
        let now = now_db_precision();
        Self {
            id: SledExpungeRequestUuid::new_v4().into(),
            time_created: now,
            time_modified: now,
            rack_id: rack_id.into(),
            sled_id: sled_id.into(),
            part_number,
            serial_number,
            sp_slot,
            tq_epoch,
            state: SledExpungeRequestState::WaitingForTrustQuorum,
            power_off_outcome: None,
            last_error: None,
            failure_reason: None,
            time_completed: None,
        }
    }

    pub fn id(&self) -> SledExpungeRequestUuid {
        self.id.into()
    }

    pub fn baseboard_id(&self) -> BaseboardId {
        BaseboardId {
            part_number: self.part_number.clone(),
            serial_number: self.serial_number.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unfinished_state_enum_matches_unfinished() {
        let converted: Vec<SledExpungeRequestState> =
            SledExpungeRequestUnfinishedState::ALL
                .iter()
                .map(|state| state.state())
                .collect();
        assert_eq!(converted, SledExpungeRequestState::UNFINISHED);
    }
}
