// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representations for trust quorum types

use super::impl_enum_type;
use crate::SqlU8;
use crate::typed_uuid::DbTypedUuid;
use nexus_db_schema::schema::{
    lrtq_member, trust_quorum_acked_commit, trust_quorum_acked_prepare,
    trust_quorum_configuration, trust_quorum_member,
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
    Committed => b"committed"
    Aborted => b"aborted"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = lrtq_member)]
pub struct LrtqMember {
    pub rack_id: DbTypedUuid<RackKind>,
    pub hw_baseboard_id: Uuid,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_configuration)]
pub struct TrustQuorumConfiguration {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub state: DbTrustQuorumConfigurationState,
    pub threshold: SqlU8,
    pub commit_crash_tolerance: SqlU8,
    pub coordinator: Uuid,
    pub encrypted_rack_secrets_salt: String,
    pub encrypted_rack_secrets: Vec<u8>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_member)]
pub struct TrustQuorumMember {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub hw_baseboard_id: Uuid,
    pub share_digest: Option<String>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_acked_prepare)]
pub struct TrustQuorumAckedPrepare {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub hw_baseboard_id: Uuid,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_acked_commit)]
pub struct TrustQuorumAckedCommit {
    pub rack_id: DbTypedUuid<RackKind>,
    pub epoch: i64,
    pub hw_baseboard_id: Uuid,
}
