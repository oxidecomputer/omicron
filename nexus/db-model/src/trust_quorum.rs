// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representations for trust quorum types

use super::impl_enum_type;
use crate::SqlU8;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{
    trust_quorum_configuration, trust_quorum_member,
};
use nexus_types::trust_quorum::{
    TrustQuorumConfigState, TrustQuorumMemberState,
};
use omicron_uuid_kinds::RackKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    TrustQuorumConfigurationStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum DbTrustQuorumConfigurationState;

    // Enum values
    Preparing => b"preparing"
    PreparingLrtqUpgrade => b"preparing-lrtq-upgrade"
    Committing => b"committing"
    Committed => b"committed"
    CommittedPartially => b"committed-partially"
    Aborted => b"aborted"
);

impl From<DbTrustQuorumConfigurationState> for TrustQuorumConfigState {
    fn from(value: DbTrustQuorumConfigurationState) -> Self {
        match value {
            DbTrustQuorumConfigurationState::Preparing => Self::Preparing,
            DbTrustQuorumConfigurationState::PreparingLrtqUpgrade => {
                Self::PreparingLrtqUpgrade
            }
            DbTrustQuorumConfigurationState::Committing => Self::Committing,
            DbTrustQuorumConfigurationState::Committed => Self::Committed,
            DbTrustQuorumConfigurationState::CommittedPartially => {
                Self::CommittedPartially
            }
            DbTrustQuorumConfigurationState::Aborted => Self::Aborted,
        }
    }
}

impl From<TrustQuorumConfigState> for DbTrustQuorumConfigurationState {
    fn from(value: TrustQuorumConfigState) -> Self {
        match value {
            TrustQuorumConfigState::Preparing => Self::Preparing,
            TrustQuorumConfigState::PreparingLrtqUpgrade => {
                Self::PreparingLrtqUpgrade
            }
            TrustQuorumConfigState::Committing => Self::Committing,
            TrustQuorumConfigState::Committed => Self::Committed,
            TrustQuorumConfigState::CommittedPartially => {
                Self::CommittedPartially
            }
            TrustQuorumConfigState::Aborted => Self::Aborted,
        }
    }
}

impl_enum_type!(
    TrustQuorumMemberStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum DbTrustQuorumMemberState;

    // Enum values
    Unacked => b"unacked"
    Prepared => b"prepared"
    Committed => b"committed"
);

impl From<DbTrustQuorumMemberState> for TrustQuorumMemberState {
    fn from(value: DbTrustQuorumMemberState) -> Self {
        match value {
            DbTrustQuorumMemberState::Unacked => Self::Unacked,
            DbTrustQuorumMemberState::Prepared => Self::Prepared,
            DbTrustQuorumMemberState::Committed => Self::Committed,
        }
    }
}

impl From<TrustQuorumMemberState> for DbTrustQuorumMemberState {
    fn from(value: TrustQuorumMemberState) -> Self {
        match value {
            TrustQuorumMemberState::Unacked => Self::Unacked,
            TrustQuorumMemberState::Prepared => Self::Prepared,
            TrustQuorumMemberState::Committed => Self::Committed,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_configuration)]
pub struct TrustQuorumConfiguration {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub last_committed_epoch: Option<i64>,
    pub state: DbTrustQuorumConfigurationState,
    pub threshold: SqlU8,
    pub commit_crash_tolerance: SqlU8,
    pub coordinator: Uuid,
    pub encrypted_rack_secrets_salt: Option<String>,
    pub encrypted_rack_secrets: Option<Vec<u8>>,
    pub time_created: DateTime<Utc>,
    pub time_committing: Option<DateTime<Utc>>,
    pub time_committed: Option<DateTime<Utc>>,
    pub time_aborted: Option<DateTime<Utc>>,
    pub abort_reason: Option<String>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_member)]
pub struct TrustQuorumMember {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub hw_baseboard_id: Uuid,
    pub state: DbTrustQuorumMemberState,
    pub share_digest: Option<String>,
    pub time_prepared: Option<DateTime<Utc>>,
    pub time_committed: Option<DateTime<Utc>>,
}
