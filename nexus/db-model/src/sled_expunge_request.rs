// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::typed_uuid::DbTypedUuid;
use chrono::DateTime;
use chrono::Utc;
use nexus_db_schema::schema::sled_expunge_request;
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
        match self {
            Self::WaitingForTrustQuorum
            | Self::PoweringOff
            | Self::Expunging => false,
            Self::Done | Self::Failed => true,
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
);

impl SledExpungePowerOffOutcome {
    pub const ALL: &'static [Self] = <Self as strum::VariantArray>::VARIANTS;

    pub fn label(&self) -> &'static str {
        match self {
            Self::PoweredOff => "powered_off",
        }
    }
}

impl fmt::Display for SledExpungePowerOffOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
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
    pub tq_epoch: i64,
    pub state: SledExpungeRequestState,
    pub power_off_outcome: Option<SledExpungePowerOffOutcome>,
    pub send_uncertain: bool,
    pub last_error: Option<String>,
    pub failure_reason: Option<String>,
    pub time_completed: Option<DateTime<Utc>>,
}

impl SledExpungeRequest {
    pub fn new(
        rack_id: RackUuid,
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
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
            tq_epoch,
            state: SledExpungeRequestState::WaitingForTrustQuorum,
            power_off_outcome: None,
            send_uncertain: true,
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
    fn test_unfinished_states_consistent() {
        for &state in SledExpungeRequestState::ALL {
            assert_eq!(
                SledExpungeRequestState::UNFINISHED.contains(&state),
                !state.is_finished(),
                "SledExpungeRequestState::{state:?}: UNFINISHED and \
                 is_finished() disagree",
            );
        }
    }
}
