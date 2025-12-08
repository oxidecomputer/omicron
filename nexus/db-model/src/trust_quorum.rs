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
    rack_id: DbTypedUuid<RackKind>,
    hw_baseboard_id: Uuid,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_configuration)]
pub struct TrustQuorumConfiguration {
    rack_id: DbTypedUuid<RackKind>,
    epoch: i64,
    state: DbTrustQuorumConfigurationState,
    threshold: SqlU8,
    commit_crash_tolerance: SqlU8,
    coordinator: Uuid,
    encrypted_rack_secrets_salt: String,
    encrypted_rack_secrets: Vec<u8>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_member)]
pub struct TrustQuorumMember {
    rack_id: DbTypedUuid<RackKind>,
    epoch: i64,
    hw_baseboard_id: Uuid,
    share_digest: Option<String>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_acked_prepare)]
pub struct TrustQuorumAckedPrepare {
    rack_id: DbTypedUuid<RackKind>,
    epoch: i64,
    hw_baseboard_id: Uuid,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = trust_quorum_acked_commit)]
pub struct TrustQuorumAckedCommit {
    rack_id: DbTypedUuid<RackKind>,
    epoch: i64,
    hw_baseboard_id: Uuid,
}
