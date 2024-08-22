// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::str::FromStr;

use crate::{
    schema::{tuf_artifact, tuf_repo, tuf_repo_artifact},
    typed_uuid::DbTypedUuid,
    SemverVersion,
};
use chrono::{DateTime, Utc};
use diesel::{deserialize::FromSql, serialize::ToSql, sql_types::Text};
use omicron_common::{
    api::external,
    update::{
        ArtifactHash as ExternalArtifactHash, ArtifactId as ExternalArtifactId,
        ArtifactKind,
    },
};
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TypedUuid;
use serde::{Deserialize, Serialize};
use std::fmt;
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
    pub fn from_external(description: external::TufRepoDescription) -> Self {
        Self {
            repo: TufRepo::from_external(description.repo),
            artifacts: description
                .artifacts
                .into_iter()
                .map(TufArtifact::from_external)
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
    #[diesel(embed)]
    pub id: ArtifactId,
    pub time_created: DateTime<Utc>,
    pub sha256: ArtifactHash,
    artifact_size: i64,
}

impl TufArtifact {
    /// Creates a new `TufArtifact` ready for insertion.
    pub fn new(
        id: ArtifactId,
        sha256: ArtifactHash,
        artifact_size: u64,
    ) -> Self {
        Self {
            id,
            time_created: Utc::now(),
            sha256,
            artifact_size: artifact_size as i64,
        }
    }

    /// Creates a new `TufArtifact` ready for insertion from an external
    /// `TufArtifactMeta`.
    ///
    /// This is not implemented as a `From` impl because we insert new fields
    /// as part of the process, which `From` doesn't necessarily communicate
    /// and can be surprising.
    pub fn from_external(artifact: external::TufArtifactMeta) -> Self {
        Self::new(artifact.id.into(), artifact.hash.into(), artifact.size)
    }

    /// Converts self into [`external::TufArtifactMeta`].
    pub fn into_external(self) -> external::TufArtifactMeta {
        external::TufArtifactMeta {
            id: self.id.into(),
            hash: self.sha256.into(),
            size: self.artifact_size as u64,
        }
    }

    /// Returns the artifact's ID.
    pub fn id(&self) -> (String, SemverVersion, String) {
        (self.id.name.clone(), self.id.version.clone(), self.id.kind.clone())
    }

    /// Returns the artifact length in bytes.
    pub fn artifact_size(&self) -> u64 {
        self.artifact_size as u64
    }
}

/// The ID (primary key) of a [`TufArtifact`].
///
/// This is the internal variant of a [`ExternalArtifactId`].
#[derive(
    Queryable,
    Insertable,
    Clone,
    Debug,
    Selectable,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
)]
#[diesel(table_name = tuf_artifact)]
pub struct ArtifactId {
    pub name: String,
    pub version: SemverVersion,
    pub kind: String,
}

impl From<ExternalArtifactId> for ArtifactId {
    fn from(id: ExternalArtifactId) -> Self {
        Self {
            name: id.name,
            version: id.version.into(),
            kind: id.kind.as_str().to_owned(),
        }
    }
}

impl From<ArtifactId> for ExternalArtifactId {
    fn from(id: ArtifactId) -> Self {
        Self {
            name: id.name,
            version: id.version.into(),
            kind: ArtifactKind::new(id.kind),
        }
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This is the same as ExternalArtifactId's Display impl.
        write!(f, "{} v{} ({})", self.name, self.version, self.kind)
    }
}

/// Required by the authz_resource macro.
impl From<ArtifactId> for (String, SemverVersion, String) {
    fn from(id: ArtifactId) -> Self {
        (id.name, id.version, id.kind)
    }
}

/// A many-to-many relationship between [`TufRepo`] and [`TufArtifact`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = tuf_repo_artifact)]
pub struct TufRepoArtifact {
    pub tuf_repo_id: Uuid,
    pub tuf_artifact_name: String,
    pub tuf_artifact_version: SemverVersion,
    pub tuf_artifact_kind: String,
}

/// A wrapper around omicron-common's [`ArtifactHash`](ExternalArtifactHash),
/// supported by Diesel.
#[derive(
    Copy,
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
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

impl ToSql<diesel::sql_types::Text, diesel::pg::Pg> for ArtifactHash {
    fn to_sql<'a>(
        &'a self,
        out: &mut diesel::serialize::Output<'a, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <String as ToSql<diesel::sql_types::Text, diesel::pg::Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl FromSql<diesel::sql_types::Text, diesel::pg::Pg> for ArtifactHash {
    fn from_sql(
        bytes: diesel::pg::PgValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let s =
            <String as FromSql<diesel::sql_types::Text, diesel::pg::Pg>>::from_sql(
                bytes,
            )?;
        ExternalArtifactHash::from_str(&s)
            .map(ArtifactHash)
            .map_err(|e| e.into())
    }
}
