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

// TODO-RAINCLAUDE: These are the columns that the table's CHECK constraints tie
// together. Between them, they decide which `SledExpungeRequestState` a row
// decodes to.
//
// They are grouped into their own struct for two reasons:
//
// * When a row breaks the CHECK constraints, the `TryFrom` conversion below
//   reports it with a single `{columns:?}`, so the error shows every column
//   that could be involved.
// * The test at the bottom of this file can build each combination of these
//   columns without filling in the rest of the row.
#[derive(Debug, PartialEq, Eq)]
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
    // TODO-RAINCLAUDE: Turns the columns into the domain state, and returns
    // `None` if no state fits them.
    //
    // A row that satisfies the table's CHECK constraints always decodes, so
    // `None` means that a CHECK is broken. For example, a `done` row with no
    // `power_off_outcome` breaks `power_off_outcome_matches_state`, and there
    // is no way to build `SledExpungeRequestState::Done` without an outcome.
    //
    // This returns an `Option` rather than an error because the caller (the
    // `TryFrom` conversion below) builds the error. The caller has the request
    // ID, and it prints all of the columns.
    //
    // Note that this doesn't check `sp_slot_is_sled_cubby` (slots 0 to 31). A
    // row with a slot outside that range still decodes.
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
    use itertools::iproduct;
    use strum::VariantArray;

    const TEST_SLOT: u16 = 3;

    #[test]
    fn test_unfinished_matches_kind() {
        let converted: Vec<DbSledExpungeRequestState> =
            SledExpungeRequestStateKind::UNFINISHED
                .iter()
                .map(|&kind| DbSledExpungeRequestState::from(kind))
                .collect();
        assert_eq!(converted, DbSledExpungeRequestState::UNFINISHED);
    }

    // TODO-RAINCLAUDE: This is the table's CHECK constraints from `dbinit.sql`,
    // written out again by hand in Rust. `decode` doesn't use it. The test
    // below compares `decode` against it, so the two have to agree about which
    // combinations of columns are legal.
    //
    // One constraint, `sp_slot_is_sled_cubby` (slots 0 to 31), is left out.
    // `decode` doesn't check the slot's range either, and the test only uses
    // slot 3.
    //
    // (Note: the previous marker called this a transcription of the table's
    // CHECK constraints, without saying that one of the seven is left out.)
    fn satisfies_checks(columns: &SledExpungeRequestStateColumns) -> bool {
        let unfinished = match columns.state {
            DbSledExpungeRequestState::WaitingForTrustQuorum
            | DbSledExpungeRequestState::PoweringOff
            | DbSledExpungeRequestState::Expunging => true,
            DbSledExpungeRequestState::Done
            | DbSledExpungeRequestState::Failed => false,
        };
        let failed = match columns.state {
            DbSledExpungeRequestState::Failed => true,
            DbSledExpungeRequestState::WaitingForTrustQuorum
            | DbSledExpungeRequestState::PoweringOff
            | DbSledExpungeRequestState::Expunging
            | DbSledExpungeRequestState::Done => false,
        };
        let outcome_matches_state = match columns.state {
            DbSledExpungeRequestState::WaitingForTrustQuorum
            | DbSledExpungeRequestState::PoweringOff => {
                columns.power_off_outcome.is_none()
            }
            DbSledExpungeRequestState::Expunging
            | DbSledExpungeRequestState::Done => {
                columns.power_off_outcome.is_some()
            }
            DbSledExpungeRequestState::Failed => true,
        };
        let outcome_matches_sp_slot = match columns.power_off_outcome {
            None => true,
            Some(outcome) => {
                let absent_at_creation = match outcome {
                    DbSledExpungePowerOffOutcome::AbsentAtCreation => true,
                    DbSledExpungePowerOffOutcome::PoweredOff
                    | DbSledExpungePowerOffOutcome::AlreadyOff
                    | DbSledExpungePowerOffOutcome::OtherBoardPresent
                    | DbSledExpungePowerOffOutcome::NotPresent => false,
                };
                absent_at_creation == columns.sp_slot.is_none()
            }
        };
        columns.time_completed.is_some() == !unfinished
            && columns.failure_reason.is_some() == failed
            && outcome_matches_state
            && (unfinished || columns.last_error.is_none())
            && columns.sp_slot.is_some() == columns.sp_slot_source.is_some()
            && outcome_matches_sp_slot
    }

    fn encode(
        state: &SledExpungeRequestState,
    ) -> SledExpungeRequestStateColumns {
        let cubby = state.recorded_cubby();
        let (
            state_kind,
            power_off_outcome,
            last_error,
            failure_reason,
            time_completed,
        ) = match state {
            SledExpungeRequestState::WaitingForTrustQuorum {
                cubby: _,
                last_error,
            } => (
                SledExpungeRequestStateKind::WaitingForTrustQuorum,
                None,
                last_error.clone(),
                None,
                None,
            ),
            SledExpungeRequestState::PoweringOff { cubby: _, last_error } => (
                SledExpungeRequestStateKind::PoweringOff,
                None,
                last_error.clone(),
                None,
                None,
            ),
            SledExpungeRequestState::Expunging { power_off, last_error } => (
                SledExpungeRequestStateKind::Expunging,
                Some(DbSledExpungePowerOffOutcome::from(*power_off)),
                last_error.clone(),
                None,
                None,
            ),
            SledExpungeRequestState::Done { power_off, time_completed } => (
                SledExpungeRequestStateKind::Done,
                Some(DbSledExpungePowerOffOutcome::from(*power_off)),
                None,
                None,
                Some(*time_completed),
            ),
            SledExpungeRequestState::Failed {
                power_off,
                failure_reason,
                time_completed,
            } => {
                let power_off_outcome = match power_off {
                    SledExpungePowerOffProgress::Pending { cubby: _ } => None,
                    SledExpungePowerOffProgress::Settled(power_off) => {
                        Some(DbSledExpungePowerOffOutcome::from(*power_off))
                    }
                };
                (
                    SledExpungeRequestStateKind::Failed,
                    power_off_outcome,
                    None,
                    Some(failure_reason.clone()),
                    Some(*time_completed),
                )
            }
        };
        SledExpungeRequestStateColumns {
            state: state_kind.into(),
            sp_slot: cubby.map(|cubby| SpMgsSlot::from(cubby.slot)),
            sp_slot_source: cubby.map(|cubby| cubby.source.into()),
            power_off_outcome,
            last_error,
            failure_reason,
            time_completed,
        }
    }

    fn row_with(
        base: &SledExpungeRequest,
        columns: &SledExpungeRequestStateColumns,
    ) -> SledExpungeRequest {
        SledExpungeRequest {
            state: columns.state,
            sp_slot: columns.sp_slot,
            sp_slot_source: columns.sp_slot_source,
            power_off_outcome: columns.power_off_outcome,
            last_error: columns.last_error.clone(),
            failure_reason: columns.failure_reason.clone(),
            time_completed: columns.time_completed,
            ..base.clone()
        }
    }

    // TODO-RAINCLAUDE: This shows that `decode` accepts exactly the rows that
    // the CHECK constraints allow, as `satisfies_checks` describes them.
    //
    // The test tries every combination of the state columns:
    //
    // * `state` is tried with each of its 5 variants.
    // * The cubby is tried 4 ways: neither `sp_slot` nor `sp_slot_source`,
    //   both, and each one alone.
    // * `power_off_outcome` is tried as NULL and with each of its 5 variants.
    // * `last_error`, `failure_reason` and `time_completed` are each tried as
    //   NULL and with one value.
    //
    // That makes 5 * 4 * 6 * 2 * 2 * 2 = 960 combinations. For each one, one of
    // two things has to hold:
    //
    // * If the combination satisfies the CHECK constraints, it decodes, and
    //   encoding the decoded state gives back the same columns. This means the
    //   decode doesn't drop or change anything.
    // * Otherwise, the conversion fails with the "illegal database state"
    //   error, which names the request and prints the columns.
    #[test]
    fn test_decode_matches_check_constraints() {
        let base = SledExpungeRequest::new(
            RackUuid::from_u128(0x6b1c5a3e_2f4d_4e8a_9c7b_1d2e3f4a5b6c),
            SledUuid::from_u128(0x0f9e8d7c_6b5a_4c3d_8e1f_2a3b4c5d6e7f),
            BaseboardId {
                part_number: "913-0000019".to_string(),
                serial_number: "test-serial".to_string(),
            },
            None,
            Epoch(2),
        )
        .expect("epoch 2 fits in the database");
        let cubby_columns = [
            (None, None),
            (
                Some(SpMgsSlot::from(TEST_SLOT)),
                Some(DbSledExpungeSpSlotSource::Inventory),
            ),
            (Some(SpMgsSlot::from(TEST_SLOT)), None),
            (None, Some(DbSledExpungeSpSlotSource::Inventory)),
        ];
        let outcomes = std::iter::once(None).chain(
            DbSledExpungePowerOffOutcome::VARIANTS.iter().copied().map(Some),
        );
        let mut valid_count = 0;
        for (
            &state,
            &(sp_slot, sp_slot_source),
            power_off_outcome,
            last_error,
            failure_reason,
            time_completed,
        ) in iproduct!(
            DbSledExpungeRequestState::VARIANTS,
            &cubby_columns,
            outcomes,
            [None, Some("test last error".to_string())],
            [None, Some("test failure".to_string())],
            [None, Some(base.time_created)]
        ) {
            let columns = SledExpungeRequestStateColumns {
                state,
                sp_slot,
                sp_slot_source,
                power_off_outcome,
                last_error,
                failure_reason,
                time_completed,
            };
            let decoded =
                nexus_types::sled_expunge::SledExpungeRequest::try_from(
                    row_with(&base, &columns),
                );
            if satisfies_checks(&columns) {
                valid_count += 1;
                let decoded = decoded.unwrap_or_else(|error| {
                    panic!("{columns:?} decodes: {error:#}")
                });
                assert_eq!(
                    encode(&decoded.state),
                    columns,
                    "{columns:?} encodes back to the same columns",
                );
            } else {
                let error =
                    decoded.expect_err(&format!("{columns:?} fails to decode"));
                assert_eq!(
                    error.to_string(),
                    format!(
                        "illegal database state (CHECK constraint broken?!): \
                         sled expunge request {} has {columns:?}",
                        base.id(),
                    ),
                );
            }
        }
        // TODO-RAINCLAUDE: The loop above only shows that `decode` and
        // `satisfies_checks` agree with each other. If both were wrong in the
        // same way (for example, if both accepted nothing), the loop would
        // still pass. So we also count the legal combinations, and compare the
        // count with a number worked out by hand from the CHECK constraints in
        // `dbinit.sql`.
        //
        // The 30 breaks down by state. In every state, the cubby is legal in 2
        // of its 4 forms (both columns set, or neither), and `failure_reason`
        // and `time_completed` are fixed by the state.
        //
        // * `waiting_for_trust_quorum` and `powering_off` allow 4 each. There
        //   is no outcome, the cubby has 2 forms, and `last_error` is set or
        //   not.
        // * `expunging` allows 10. There are 5 outcomes, and each one fixes the
        //   cubby (`absent_at_creation` needs none, and the other four need
        //   one). `last_error` is set or not.
        // * `done` allows 5. These are the same 5 outcomes, with no
        //   `last_error`.
        // * `failed` allows 7. With no outcome, the cubby has 2 forms. With an
        //   outcome, there are the same 5 as above. There is no `last_error`.
        assert_eq!(valid_count, 30);
    }
}
