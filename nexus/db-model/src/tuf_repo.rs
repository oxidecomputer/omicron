// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::str::FromStr;

use crate::{Generation, SemverVersion, typed_uuid::DbTypedUuid};
use chrono::{DateTime, Utc};
use diesel::sql_types::{Jsonb, Text};
use diesel::{deserialize::FromSql, serialize::ToSql};
use nexus_db_schema::schema::{
    tuf_artifact, tuf_repo, tuf_repo_artifact, tuf_trust_root,
};
use nexus_types::external_api::shared::TufSignedRootRole;
use nexus_types::external_api::views;
use omicron_common::{api::external, update::ArtifactId};
use omicron_uuid_kinds::TufArtifactKind;
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TufTrustRootKind;
use omicron_uuid_kinds::TufTrustRootUuid;
use omicron_uuid_kinds::TypedUuid;
use parse_display::Display;
use serde::{Deserialize, Serialize};
use std::fmt;
use tufaceous_artifact::{
    ArtifactHash as ExternalArtifactHash, ArtifactKind, ArtifactVersion,
};
use uuid::Uuid;

/// A description of a TUF update: a repo, along with the artifacts it
/// contains.
///
/// This is the internal variant of [`external::TufRepoDescription`].
#[derive(Debug, Clone)]
pub struct TufRepoDescription {
    /// The repository.
    pub repo: TufRepo,

    /// The artifacts.
    pub artifacts: Vec<TufArtifact>,
}

impl TufRepoDescription {
    /// Creates a new `TufRepoDescription` from an
    /// [`external::TufRepoDescription`].
    ///
    /// This is not implemented as a `From` impl because we insert new fields
    /// as part of the process, which `From` doesn't necessarily communicate
    /// and can be surprising.
    pub fn from_external(
        description: external::TufRepoDescription,
        generation_added: external::Generation,
    ) -> Self {
        Self {
            repo: TufRepo::from_external(description.repo),
            artifacts: description
                .artifacts
                .into_iter()
                .map(|artifact| {
                    TufArtifact::from_external(artifact, generation_added)
                })
                .collect(),
        }
    }

    /// Converts self into [`external::TufRepoDescription`].
    pub fn into_external(self) -> external::TufRepoDescription {
        external::TufRepoDescription {
            repo: self.repo.into_external(),
            artifacts: self
                .artifacts
                .into_iter()
                .map(TufArtifact::into_external)
                .collect(),
        }
    }
}

/// A record representing an uploaded TUF repository.
///
/// This is the internal variant of [`external::TufRepoMeta`].
#[derive(
    Queryable, Identifiable, Insertable, Clone, Debug, Selectable, AsChangeset,
)]
#[diesel(table_name = tuf_repo)]
pub struct TufRepo {
    pub id: DbTypedUuid<TufRepoKind>,
    pub time_created: DateTime<Utc>,
    // XXX: We're overloading ArtifactHash here to also mean the hash of the
    // repository zip itself.
    pub sha256: ArtifactHash,
    pub targets_role_version: i64,
    pub valid_until: DateTime<Utc>,
    pub system_version: SemverVersion,
    pub file_name: String,
}

impl TufRepo {
    /// Creates a new `TufRepo` ready for insertion.
    pub fn new(
        sha256: ArtifactHash,
        targets_role_version: u64,
        valid_until: DateTime<Utc>,
        system_version: SemverVersion,
        file_name: String,
    ) -> Self {
        Self {
            id: TypedUuid::new_v4().into(),
            time_created: Utc::now(),
            sha256,
            targets_role_version: targets_role_version as i64,
            valid_until,
            system_version,
            file_name,
        }
    }

    /// Creates a new `TufRepo` ready for insertion from an external
    /// `TufRepoMeta`.
    ///
    /// This is not implemented as a `From` impl because we insert new fields
    /// as part of the process, which `From` doesn't necessarily communicate
    /// and can be surprising.
    pub fn from_external(repo: external::TufRepoMeta) -> Self {
        Self::new(
            repo.hash.into(),
            repo.targets_role_version,
            repo.valid_until,
            repo.system_version.into(),
            repo.file_name,
        )
    }

    /// Converts self into [`external::TufRepoMeta`].
    pub fn into_external(self) -> external::TufRepoMeta {
        external::TufRepoMeta {
            hash: self.sha256.into(),
            targets_role_version: self.targets_role_version as u64,
            valid_until: self.valid_until,
            system_version: self.system_version.into(),
            file_name: self.file_name,
        }
    }

    /// Returns the repository's ID.
    pub fn id(&self) -> TypedUuid<TufRepoKind> {
        self.id.into()
    }

    /// Returns the targets role version.
    pub fn targets_role_version(&self) -> u64 {
        self.targets_role_version as u64
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, AsChangeset)]
#[diesel(table_name = tuf_artifact)]
pub struct TufArtifact {
    pub id: DbTypedUuid<TufArtifactKind>,
    pub name: String,
    pub version: DbArtifactVersion,
    pub kind: String,
    pub time_created: DateTime<Utc>,
    pub sha256: ArtifactHash,
    artifact_size: i64,
    pub generation_added: Generation,
}

impl TufArtifact {
    /// Creates a new `TufArtifact` ready for insertion.
    pub fn new(
        artifact_id: ArtifactId,
        sha256: ArtifactHash,
        artifact_size: u64,
        generation_added: external::Generation,
    ) -> Self {
        Self {
            id: TypedUuid::new_v4().into(),
            name: artifact_id.name,
            version: artifact_id.version.into(),
            kind: artifact_id.kind.as_str().to_owned(),
            time_created: Utc::now(),
            sha256,
            artifact_size: artifact_size as i64,
            generation_added: generation_added.into(),
        }
    }

    /// Creates a new `TufArtifact` ready for insertion from an external
    /// `TufArtifactMeta`.
    ///
    /// This is not implemented as a `From` impl because we insert new fields
    /// as part of the process, which `From` doesn't necessarily communicate
    /// and can be surprising.
    pub fn from_external(
        artifact: external::TufArtifactMeta,
        generation_added: external::Generation,
    ) -> Self {
        Self::new(
            artifact.id,
            artifact.hash.into(),
            artifact.size,
            generation_added,
        )
    }

    /// Converts self into [`external::TufArtifactMeta`].
    pub fn into_external(self) -> external::TufArtifactMeta {
        external::TufArtifactMeta {
            id: ArtifactId {
                name: self.name,
                version: self.version.into(),
                kind: ArtifactKind::new(self.kind),
            },
            hash: self.sha256.into(),
            size: self.artifact_size as u64,
        }
    }

    /// Returns the artifact's ID.
    pub fn id(&self) -> TypedUuid<TufArtifactKind> {
        self.id.into()
    }

    /// Returns the artifact's name, version, and kind, which is unique across
    /// all artifacts.
    pub fn nvk(&self) -> (&str, &ArtifactVersion, &str) {
        (&self.name, &self.version, &self.kind)
    }

    /// Returns the artifact length in bytes.
    pub fn artifact_size(&self) -> u64 {
        self.artifact_size as u64
    }
}

/// Artifact version in the database: a freeform identifier.
#[derive(
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    Display,
)]
#[diesel(sql_type = Text)]
#[serde(transparent)]
#[display("{0}")]
pub struct DbArtifactVersion(pub ArtifactVersion);

NewtypeFrom! { () pub struct DbArtifactVersion(ArtifactVersion); }
NewtypeDeref! { () pub struct DbArtifactVersion(ArtifactVersion); }

impl ToSql<Text, diesel::pg::Pg> for DbArtifactVersion {
    fn to_sql<'a>(
        &'a self,
        out: &mut diesel::serialize::Output<'a, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <String as ToSql<Text, diesel::pg::Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl FromSql<Text, diesel::pg::Pg> for DbArtifactVersion {
    fn from_sql(
        bytes: diesel::pg::PgValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, diesel::pg::Pg>>::from_sql(bytes)?;
        s.parse::<ArtifactVersion>()
            .map(DbArtifactVersion)
            .map_err(|e| e.into())
    }
}

/// A many-to-many relationship between [`TufRepo`] and [`TufArtifact`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = tuf_repo_artifact)]
pub struct TufRepoArtifact {
    pub tuf_repo_id: Uuid,
    pub tuf_artifact_id: Uuid,
}

/// A wrapper around omicron-common's [`ArtifactHash`](ExternalArtifactHash),
/// supported by Diesel.
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = Text)]
#[serde(transparent)]
pub struct ArtifactHash(pub ExternalArtifactHash);

NewtypeFrom! { () pub struct ArtifactHash(ExternalArtifactHash); }
NewtypeDeref! { () pub struct ArtifactHash(ExternalArtifactHash); }

impl fmt::Display for ArtifactHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ToSql<Text, diesel::pg::Pg> for ArtifactHash {
    fn to_sql<'a>(
        &'a self,
        out: &mut diesel::serialize::Output<'a, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <String as ToSql<Text, diesel::pg::Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl FromSql<Text, diesel::pg::Pg> for ArtifactHash {
    fn from_sql(
        bytes: diesel::pg::PgValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, diesel::pg::Pg>>::from_sql(bytes)?;
        ExternalArtifactHash::from_str(&s)
            .map(ArtifactHash)
            .map_err(|e| e.into())
    }
}

/// A trusted TUF root roles, used to verify TUF repo signatures.
#[derive(Clone, Debug, Queryable, Insertable, Selectable)]
#[diesel(table_name = tuf_trust_root)]
pub struct TufTrustRoot {
    pub id: DbTypedUuid<TufTrustRootKind>,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub root_role: DbTufSignedRootRole,
}

impl TufTrustRoot {
    pub fn new(root_role: TufSignedRootRole) -> TufTrustRoot {
        TufTrustRoot {
            id: TufTrustRootUuid::new_v4().into(),
            time_created: Utc::now(),
            time_deleted: None,
            root_role: DbTufSignedRootRole(root_role),
        }
    }

    pub fn id(&self) -> TufTrustRootUuid {
        self.id.into()
    }
}

impl From<TufTrustRoot> for views::UpdatesTrustRoot {
    fn from(trust_root: TufTrustRoot) -> views::UpdatesTrustRoot {
        views::UpdatesTrustRoot {
            id: trust_root.id.into(),
            time_created: trust_root.time_created,
            root_role: trust_root.root_role.0,
        }
    }
}

#[derive(Debug, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
pub struct DbTufSignedRootRole(pub TufSignedRootRole);

impl ToSql<Jsonb, diesel::pg::Pg> for DbTufSignedRootRole {
    fn to_sql<'a>(
        &'a self,
        out: &mut diesel::serialize::Output<'a, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <serde_json::Value as ToSql<Jsonb, diesel::pg::Pg>>::to_sql(
            &serde_json::to_value(self.0.clone())?,
            &mut out.reborrow(),
        )
    }
}

impl FromSql<Jsonb, diesel::pg::Pg> for DbTufSignedRootRole {
    fn from_sql(
        bytes: diesel::pg::PgValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let value =
            <serde_json::Value as FromSql<Jsonb, diesel::pg::Pg>>::from_sql(
                bytes,
            )?;
        serde_json::from_value(value)
            .map(DbTufSignedRootRole)
            .map_err(|e| e.into())
    }
}
