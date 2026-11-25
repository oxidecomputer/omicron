// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::SpMgsSlot;
use crate::typed_uuid::DbTypedUuid;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::sled_expunge_request;
use nexus_types::sled_expunge::SledExpungeCubbyPowerOffOutcome;
use nexus_types::sled_expunge::SledExpungePowerOff;
use nexus_types::sled_expunge::SledExpungePowerOffProgress;
use nexus_types::sled_expunge::SledExpungeRecordedCubby;
use nexus_types::sled_expunge::SledExpungeRequestState;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use nexus_types::sled_expunge::SledExpungeSpSlotSource;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestKind;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use trust_quorum_types::types::Epoch;

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
    pub enum DbSledExpungeRequestState;

    WaitingForTrustQuorum => b"waiting_for_trust_quorum"
    PoweringOff => b"powering_off"
    Expunging => b"expunging"
    Done => b"done"
    Failed => b"failed"
);

impl DbSledExpungeRequestState {
    pub const UNFINISHED: &'static [Self] =
        &[Self::WaitingForTrustQuorum, Self::PoweringOff, Self::Expunging];
}

impl From<SledExpungeRequestStateKind> for DbSledExpungeRequestState {
    fn from(kind: SledExpungeRequestStateKind) -> Self {
        match kind {
            SledExpungeRequestStateKind::WaitingForTrustQuorum => {
                Self::WaitingForTrustQuorum
            }
            SledExpungeRequestStateKind::PoweringOff => Self::PoweringOff,
            SledExpungeRequestStateKind::Expunging => Self::Expunging,
            SledExpungeRequestStateKind::Done => Self::Done,
            SledExpungeRequestStateKind::Failed => Self::Failed,
        }
    }
}

impl From<DbSledExpungeRequestState> for SledExpungeRequestStateKind {
    fn from(state: DbSledExpungeRequestState) -> Self {
        match state {
            DbSledExpungeRequestState::WaitingForTrustQuorum => {
                Self::WaitingForTrustQuorum
            }
            DbSledExpungeRequestState::PoweringOff => Self::PoweringOff,
            DbSledExpungeRequestState::Expunging => Self::Expunging,
            DbSledExpungeRequestState::Done => Self::Done,
            DbSledExpungeRequestState::Failed => Self::Failed,
        }
    }
}

impl_enum_type!(
    SledExpungeSpSlotSourceEnum:

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
    pub enum DbSledExpungeSpSlotSource;

    Inventory => b"inventory"
    LiveScan => b"live_scan"
    FoundAfterAbsent => b"found_after_absent"
    Relocated => b"relocated"
);

impl From<SledExpungeSpSlotSource> for DbSledExpungeSpSlotSource {
    fn from(source: SledExpungeSpSlotSource) -> Self {
        match source {
            SledExpungeSpSlotSource::Inventory => Self::Inventory,
            SledExpungeSpSlotSource::LiveScan => Self::LiveScan,
            SledExpungeSpSlotSource::FoundAfterAbsent => Self::FoundAfterAbsent,
            SledExpungeSpSlotSource::Relocated => Self::Relocated,
        }
    }
}

impl From<DbSledExpungeSpSlotSource> for SledExpungeSpSlotSource {
    fn from(source: DbSledExpungeSpSlotSource) -> Self {
        match source {
            DbSledExpungeSpSlotSource::Inventory => Self::Inventory,
            DbSledExpungeSpSlotSource::LiveScan => Self::LiveScan,
            DbSledExpungeSpSlotSource::FoundAfterAbsent => {
                Self::FoundAfterAbsent
            }
            DbSledExpungeSpSlotSource::Relocated => Self::Relocated,
        }
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
    pub enum DbSledExpungePowerOffOutcome;

    PoweredOff => b"powered_off"
    AlreadyOff => b"already_off"
    OtherBoardPresent => b"other_board_present"
    NotPresent => b"not_present"
    AbsentAtCreation => b"absent_at_creation"
);

impl DbSledExpungePowerOffOutcome {
    fn cubby_outcome(&self) -> Option<SledExpungeCubbyPowerOffOutcome> {
        match self {
            Self::PoweredOff => {
                Some(SledExpungeCubbyPowerOffOutcome::PoweredOff)
            }
            Self::AlreadyOff => {
                Some(SledExpungeCubbyPowerOffOutcome::AlreadyOff)
            }
            Self::OtherBoardPresent => {
                Some(SledExpungeCubbyPowerOffOutcome::OtherBoardPresent)
            }
            Self::NotPresent => {
                Some(SledExpungeCubbyPowerOffOutcome::NotPresent)
            }
            Self::AbsentAtCreation => None,
        }
    }
}

impl From<SledExpungeCubbyPowerOffOutcome> for DbSledExpungePowerOffOutcome {
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

impl From<SledExpungePowerOff> for DbSledExpungePowerOffOutcome {
    fn from(power_off: SledExpungePowerOff) -> Self {
        match power_off {
            SledExpungePowerOff::Cubby { cubby: _, outcome } => outcome.into(),
            SledExpungePowerOff::AbsentAtCreation => Self::AbsentAtCreation,
        }
    }
}

/// A row of the `sled_expunge_request` table.
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
    pub sp_slot_source: Option<DbSledExpungeSpSlotSource>,
    pub tq_epoch: i64,
    pub state: DbSledExpungeRequestState,
    pub power_off_outcome: Option<DbSledExpungePowerOffOutcome>,
    pub last_error: Option<String>,
    pub failure_reason: Option<String>,
    pub time_completed: Option<DateTime<Utc>>,
}

impl SledExpungeRequest {
    pub fn new(
        rack_id: RackUuid,
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        cubby: Option<SledExpungeRecordedCubby>,
        tq_epoch: Epoch,
    ) -> anyhow::Result<Self> {
        let tq_epoch = i64::try_from(tq_epoch.0).map_err(|_| {
            anyhow!(
                "trust quorum epoch {tq_epoch} is too large to store in the \
                 database (the largest it can store is {})",
                i64::MAX,
            )
        })?;
        let BaseboardId { part_number, serial_number } = baseboard_id;
        let now = now_db_precision();
        Ok(Self {
            id: SledExpungeRequestUuid::new_v4().into(),
            time_created: now,
            time_modified: now,
            rack_id: rack_id.into(),
            sled_id: sled_id.into(),
            part_number,
            serial_number,
            sp_slot: cubby.map(|cubby| SpMgsSlot::from(cubby.slot)),
            sp_slot_source: cubby.map(|cubby| cubby.source.into()),
            tq_epoch,
            state: DbSledExpungeRequestState::WaitingForTrustQuorum,
            power_off_outcome: None,
            last_error: None,
            failure_reason: None,
            time_completed: None,
        })
    }

    pub fn id(&self) -> SledExpungeRequestUuid {
        self.id.into()
    }
}

/// Columns for sled expunge request state.
///
/// Gathered here so all the CHECK constraints can be enforced together.
#[derive(Debug)]
struct SledExpungeRequestStateColumns {
    state: DbSledExpungeRequestState,
    sp_slot: Option<SpMgsSlot>,
    sp_slot_source: Option<DbSledExpungeSpSlotSource>,
    power_off_outcome: Option<DbSledExpungePowerOffOutcome>,
    last_error: Option<String>,
    failure_reason: Option<String>,
    time_completed: Option<DateTime<Utc>>,
}

impl SledExpungeRequestStateColumns {
    /// Turns the columns into the domain type, [`SledExpungeRequestState`].
    ///
    /// The checks here correspond exactly to the CHECK contsraints in the
    /// database. This can only return `None` if the database is corrupt and one
    /// or more CHECK constraints have been violated. (The actual error is
    /// returned via the `TryFrom` conversion.)
    ///
    /// Note that this doesn't check `sp_slot_is_sled_cubby` (slots 0 to 31)..
    fn decode(&self) -> Option<SledExpungeRequestState> {
        let cubby = match (self.sp_slot, self.sp_slot_source) {
            (Some(slot), Some(source)) => Some(SledExpungeRecordedCubby {
                slot: **slot,
                source: source.into(),
            }),
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => return None,
        };
        let power_off = match self.power_off_outcome {
            None => None,
            Some(outcome) => match (outcome.cubby_outcome(), cubby) {
                (Some(outcome), Some(cubby)) => {
                    Some(SledExpungePowerOff::Cubby { cubby, outcome })
                }
                (None, None) => Some(SledExpungePowerOff::AbsentAtCreation),
                (Some(_), None) | (None, Some(_)) => return None,
            },
        };
        match self.state {
            DbSledExpungeRequestState::WaitingForTrustQuorum => {
                let (None, None, None) =
                    (power_off, &self.failure_reason, self.time_completed)
                else {
                    return None;
                };
                Some(SledExpungeRequestState::WaitingForTrustQuorum {
                    cubby,
                    last_error: self.last_error.clone(),
                })
            }
            DbSledExpungeRequestState::PoweringOff => {
                let (None, None, None) =
                    (power_off, &self.failure_reason, self.time_completed)
                else {
                    return None;
                };
                Some(SledExpungeRequestState::PoweringOff {
                    cubby,
                    last_error: self.last_error.clone(),
                })
            }
            DbSledExpungeRequestState::Expunging => {
                let (Some(power_off), None, None) =
                    (power_off, &self.failure_reason, self.time_completed)
                else {
                    return None;
                };
                Some(SledExpungeRequestState::Expunging {
                    power_off,
                    last_error: self.last_error.clone(),
                })
            }
            DbSledExpungeRequestState::Done => {
                let (Some(power_off), None, None, Some(time_completed)) = (
                    power_off,
                    &self.last_error,
                    &self.failure_reason,
                    self.time_completed,
                ) else {
                    return None;
                };
                Some(SledExpungeRequestState::Done {
                    power_off,
                    time_completed,
                })
            }
            DbSledExpungeRequestState::Failed => {
                let (None, Some(failure_reason), Some(time_completed)) = (
                    &self.last_error,
                    &self.failure_reason,
                    self.time_completed,
                ) else {
                    return None;
                };
                let power_off = match power_off {
                    Some(power_off) => {
                        SledExpungePowerOffProgress::Settled(power_off)
                    }
                    None => SledExpungePowerOffProgress::Pending { cubby },
                };
                Some(SledExpungeRequestState::Failed {
                    power_off,
                    failure_reason: failure_reason.clone(),
                    time_completed,
                })
            }
        }
    }
}

impl TryFrom<SledExpungeRequest>
    for nexus_types::sled_expunge::SledExpungeRequest
{
    type Error = anyhow::Error;

    fn try_from(request: SledExpungeRequest) -> Result<Self, Self::Error> {
        let SledExpungeRequest {
            id,
            time_created,
            time_modified,
            rack_id,
            sled_id,
            part_number,
            serial_number,
            sp_slot,
            sp_slot_source,
            tq_epoch,
            state,
            power_off_outcome,
            last_error,
            failure_reason,
            time_completed,
        } = request;
        let id = SledExpungeRequestUuid::from(id);
        let tq_epoch = u64::try_from(tq_epoch).map(Epoch).map_err(|_| {
            anyhow!(
                "sled expunge request {id} has tq_epoch {tq_epoch}, which is \
                 negative and so is not a valid trust quorum epoch"
            )
        })?;
        let columns = SledExpungeRequestStateColumns {
            state,
            sp_slot,
            sp_slot_source,
            power_off_outcome,
            last_error,
            failure_reason,
            time_completed,
        };
        let state = columns.decode().ok_or_else(|| {
            anyhow!(
                "illegal database state (CHECK constraint broken?!): sled \
                 expunge request {id} has {columns:?}"
            )
        })?;
        Ok(Self {
            id,
            rack_id: rack_id.into(),
            sled_id: sled_id.into(),
            baseboard_id: BaseboardId { part_number, serial_number },
            tq_epoch,
            state,
            time_created,
            time_modified,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unfinished_matches_kind() {
        let converted: Vec<DbSledExpungeRequestState> =
            SledExpungeRequestStateKind::UNFINISHED
                .iter()
                .map(|&kind| DbSledExpungeRequestState::from(kind))
                .collect();
        assert_eq!(converted, DbSledExpungeRequestState::UNFINISHED);
    }
}
