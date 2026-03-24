// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::str::FromStr;

use crate::{ByteCount, Generation, SemverVersion, typed_uuid::DbTypedUuid};
use chrono::{DateTime, Utc};
use diesel::sql_types::{Jsonb, Text};
use diesel::{deserialize::FromSql, serialize::ToSql};
use iddqd::{IdHashItem, id_upcast};
use nexus_db_schema::schema::{
    tuf_artifact, tuf_artifact_tag, tuf_repo, tuf_repo_artifact, tuf_trust_root,
};
use nexus_types::external_api::update as update_types;
use omicron_common::api::external;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::TufArtifactKind;
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TufTrustRootKind;
use omicron_uuid_kinds::TufTrustRootUuid;
use omicron_uuid_kinds::TypedUuid;
use parse_display::Display;
use serde::{Deserialize, Serialize};
use std::fmt;
use tufaceous_artifact::{
    Artifact, ArtifactHash as ExternalArtifactHash, ArtifactVersion,
};

/// A description of a TUF update: a repo, along with the artifacts it
/// contains.
#[derive(Debug, Clone)]
pub struct TufRepoDescription {
    /// The repository.
    pub repo: TufRepo,

    /// The artifacts.
    pub artifacts: Vec<TufArtifactDescription>,
}

impl TufRepoDescription {
    /// Creates a new `TufRepoDescription` from an
    /// [`omicron_common::update::TufRepoDescription`].
    pub fn from_common(
        description: omicron_common::update::TufRepoDescription,
        generation_added: external::Generation,
    ) -> Result<Self, external::ByteCountRangeError> {
        Ok(Self {
            repo: TufRepo {
                id: TypedUuid::new_v4().into(),
                time_created: Utc::now(),
                time_pruned: None,
                sha256: description.hash.map(Into::into),
                system_version: description.system_version.into(),
                file_name: description.file_name,
            },
            artifacts: description
                .artifacts
                .into_iter()
                .map(|artifact| {
                    TufArtifactDescription::from_artifact(
                        artifact,
                        generation_added,
                    )
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

impl From<TufRepoDescription> for omicron_common::update::TufRepoDescription {
    fn from(description: TufRepoDescription) -> Self {
        Self {
            artifacts: description
                .artifacts
                .into_iter()
                .map(Into::into)
                .collect(),
            system_version: description.repo.system_version.into(),
            hash: description.repo.sha256.map(Into::into),
            file_name: description.repo.file_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TufArtifactDescription {
    pub artifact: TufArtifact,
    pub tags: Vec<TufArtifactTag>,
}

impl TufArtifactDescription {
    pub fn from_artifact(
        artifact: Artifact,
        generation_added: external::Generation,
    ) -> Result<Self, external::ByteCountRangeError> {
        let id = TypedUuid::new_v4().into();
        Ok(Self {
            artifact: TufArtifact {
                id,
                version: artifact.version.into(),
                time_created: Utc::now(),
                sha256: artifact.hash.into(),
                artifact_size: ByteCount(artifact.length.try_into()?),
                generation_added: generation_added.into(),
            },
            tags: artifact
                .tags
                .into_iter()
                .map(|(key, value)| TufArtifactTag {
                    tuf_artifact_id: id,
                    key,
                    value,
                })
                .collect(),
        })
    }
}

impl From<TufArtifactDescription> for Artifact {
    fn from(description: TufArtifactDescription) -> Self {
        Artifact {
            // `target_name` is only used in conjunction with a loaded
            // `tufaceous::Repository`.
            target_name: String::new(),

            version: description.artifact.version.into(),
            tags: description
                .tags
                .into_iter()
                .map(|tag| (tag.key, tag.value))
                .collect(),
            hash: description.artifact.sha256.0,
            length: description.artifact.artifact_size.0.to_bytes(),
        }
    }
}

impl IdHashItem for TufArtifactDescription {
    type Key<'a> = &'a DbTypedUuid<TufArtifactKind>;

    fn key(&self) -> Self::Key<'_> {
        &self.artifact.id
    }

    id_upcast!();
}

/// A record representing an uploaded TUF repository.
#[derive(
    Queryable, Identifiable, Insertable, Clone, Debug, Selectable, AsChangeset,
)]
#[diesel(table_name = tuf_repo)]
pub struct TufRepo {
    pub id: DbTypedUuid<TufRepoKind>,
    pub time_created: DateTime<Utc>,
    pub time_pruned: Option<DateTime<Utc>>,
    // XXX: We're overloading ArtifactHash here to also mean the hash of the
    // repository zip itself.
    pub sha256: Option<ArtifactHash>,
    pub system_version: SemverVersion,
    pub file_name: Option<String>,
}

impl TufRepo {
    /// Returns the repository's ID.
    pub fn id(&self) -> TypedUuid<TufRepoKind> {
        self.id.into()
    }
}

impl From<TufRepo> for update_types::TufRepo {
    fn from(repo: TufRepo) -> update_types::TufRepo {
        update_types::TufRepo {
            hash: repo.sha256.map(|sha256| sha256.into()),
            system_version: repo.system_version.into(),
            file_name: repo.file_name,
            time_created: repo.time_created,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, AsChangeset)]
#[diesel(table_name = tuf_artifact)]
pub struct TufArtifact {
    pub id: DbTypedUuid<TufArtifactKind>,
    pub version: DbArtifactVersion,
    pub time_created: DateTime<Utc>,
    pub sha256: ArtifactHash,
    pub artifact_size: ByteCount,
    pub generation_added: Generation,
}

impl TufArtifact {
    /// Returns the artifact's ID.
    pub fn id(&self) -> TypedUuid<TufArtifactKind> {
        self.id.into()
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
    pub tuf_repo_id: DbTypedUuid<TufRepoKind>,
    pub tuf_artifact_id: DbTypedUuid<TufArtifactKind>,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = tuf_artifact_tag)]
pub struct TufArtifactTag {
    pub tuf_artifact_id: DbTypedUuid<TufArtifactKind>,
    pub key: String,
    pub value: String,
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
    pub fn new(root_role: update_types::TufSignedRootRole) -> TufTrustRoot {
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

impl From<TufTrustRoot> for update_types::UpdatesTrustRoot {
    fn from(trust_root: TufTrustRoot) -> update_types::UpdatesTrustRoot {
        update_types::UpdatesTrustRoot {
            id: trust_root.id.into_untyped_uuid(),
            time_created: trust_root.time_created,
            root_role: trust_root.root_role.0,
        }
    }
}

#[derive(Debug, Clone, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
pub struct DbTufSignedRootRole(pub update_types::TufSignedRootRole);

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

// The following isn't a real model in the sense that it represents DB data,
// but it is the return type of a datastore function. The main reason we can't
// just use the view for this like we do with TufRepoUploadStatus is that
// TufRepoDescription has a bit more info in it that we rely on in code outside
// of the external API, like tests and internal APIs

/// The return value of the tuf repo insert function
pub struct TufRepoUpload {
    pub recorded: TufRepoDescription,
    pub status: update_types::TufRepoUploadStatus,
}

impl From<TufRepoUpload> for update_types::TufRepoUpload {
    fn from(upload: TufRepoUpload) -> Self {
        update_types::TufRepoUpload {
            repo: upload.recorded.repo.into(),
            status: upload.status,
        }
    }
}
