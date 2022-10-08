// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_wrapper;
use crate::schema::update_available_artifact;
use chrono::{DateTime, Utc};
use omicron_common::api::internal;
use parse_display::Display;
use std::io::Write;

impl_enum_wrapper!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "update_artifact_kind"))]
    pub struct UpdateArtifactKindEnum;

    #[derive(Clone, Copy, Debug, Display, AsExpression, FromSqlRow, PartialEq, Eq)]
    #[display("{0}")]
    #[diesel(sql_type = UpdateArtifactKindEnum)]
    pub struct UpdateArtifactKind(pub internal::nexus::UpdateArtifactKind);

    // Enum values
    Zone => b"zone"
);

#[derive(
    Queryable, Insertable, Clone, Debug, Display, Selectable, AsChangeset,
)]
#[diesel(table_name = update_available_artifact)]
#[display("{kind} \"{name}\" v{version}")]
pub struct UpdateAvailableArtifact {
    pub name: String,
    /// Version of the artifact itself
    pub version: i64,
    pub kind: UpdateArtifactKind,
    /// `version` field of targets.json from the repository
    // FIXME this *should* be a NonZeroU64
    pub targets_role_version: i64,
    pub valid_until: DateTime<Utc>,
    pub target_name: String,
    // FIXME should this be [u8; 32]?
    pub target_sha256: String,
    // FIXME this *should* be a u64
    pub target_length: i64,
}

impl UpdateAvailableArtifact {
    pub fn id(&self) -> (String, i64, UpdateArtifactKind) {
        (self.name.clone(), self.version, self.kind)
    }
}
