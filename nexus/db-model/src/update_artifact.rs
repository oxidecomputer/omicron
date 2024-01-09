// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_wrapper;
use crate::schema::update_artifact;
use crate::SemverVersion;
use chrono::{DateTime, Utc};
use omicron_common::api::internal;
use parse_display::Display;
use serde::Deserialize;
use serde::Serialize;
use std::io::Write;

impl_enum_wrapper!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "update_artifact_kind", schema = "public"))]
    pub struct KnownArtifactKindEnum;

    #[derive(Clone, Copy, Debug, Display, AsExpression, FromSqlRow, PartialEq, Eq, Serialize, Deserialize)]
    #[display("{0}")]
    #[diesel(sql_type = KnownArtifactKindEnum)]
    pub struct KnownArtifactKind(pub internal::nexus::KnownArtifactKind);

    // Enum values
    GimletSp => b"gimlet_sp"
    GimletRot => b"gimlet_rot"
    Host => b"host"
    Trampoline => b"trampoline"
    ControlPlane => b"control_plane"
    PscSp => b"psc_sp"
    PscRot => b"psc_rot"
    SwitchSp => b"switch_sp"
    SwitchRot => b"switch_rot"
);

#[derive(
    Queryable, Insertable, Clone, Debug, Display, Selectable, AsChangeset,
)]
#[diesel(table_name = update_artifact)]
#[display("{kind} \"{name}\" v{version}")]
pub struct UpdateArtifact {
    pub name: String,
    /// Version of the artifact itself
    pub version: SemverVersion,
    pub kind: KnownArtifactKind,
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

impl UpdateArtifact {
    pub fn id(&self) -> (String, SemverVersion, KnownArtifactKind) {
        (self.name.clone(), self.version.clone(), self.kind)
    }
}
