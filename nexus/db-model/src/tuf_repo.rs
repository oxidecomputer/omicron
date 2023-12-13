// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::str::FromStr;

use crate::{
    schema::{tuf_artifact, tuf_repo, tuf_repo_artifact},
    SemverVersion,
};
use chrono::{DateTime, Utc};
use diesel::{deserialize::FromSql, serialize::ToSql, sql_types::Text};
use omicron_common::update::ArtifactHash as ExternalArtifactHash;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// A description of a TUF update: a repo, along with the artifacts it
/// contains.
#[derive(Debug, Clone)]
pub struct TufRepoDescription {
    /// The repository.
    pub repo: TufRepo,
    /// The artifacts.
    pub artifacts: Vec<TufArtifact>,
}

/// A record representing an uploaded TUF repository.
#[derive(
    Queryable, Identifiable, Insertable, Clone, Debug, Selectable, AsChangeset,
)]
#[diesel(table_name = tuf_repo)]
pub struct TufRepo {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    // XXX: We're overloading ArtifactHash here to also mean the hash of the
    // repository zip itself.
    pub sha256: ArtifactHash,
    pub targets_role_version: i64,
    pub valid_until: DateTime<Utc>,
    pub system_version: SemverVersion,
    pub source_file: String,
}

impl TufRepo {
    /// Creates a new `TufRepo` ready for insertion.
    pub fn new(
        sha256: ArtifactHash,
        targets_role_version: u64,
        valid_until: DateTime<Utc>,
        system_version: SemverVersion,
        source_file: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            time_created: Utc::now(),
            sha256,
            targets_role_version: targets_role_version as i64,
            valid_until,
            system_version,
            source_file,
        }
    }

    /// Returns the repository's ID.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the targets role version.
    pub fn targets_role_version(&self) -> u64 {
        self.targets_role_version as u64
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, AsChangeset)]
#[diesel(table_name = tuf_artifact)]
pub struct TufArtifact {
    pub name: String,
    pub version: SemverVersion,
    pub kind: String,
    pub time_created: DateTime<Utc>,
    pub sha256: ArtifactHash,
    artifact_length: i64,
}

impl TufArtifact {
    /// Creates a new `TufArtifact` ready for insertion.
    pub fn new(
        name: String,
        version: SemverVersion,
        kind: String,
        sha256: ArtifactHash,
        artifact_length: u64,
    ) -> Self {
        Self {
            name,
            version,
            kind,
            time_created: Utc::now(),
            sha256,
            artifact_length: artifact_length as i64,
        }
    }

    /// Returns the artifact's ID.
    pub fn id(&self) -> (String, SemverVersion, String) {
        (self.name.clone(), self.version.clone(), self.kind.clone())
    }

    /// Returns the artifact length in bytes.
    pub fn artifact_length(&self) -> u64 {
        self.artifact_length as u64
    }

    /// Returns a way to display this object's (name, version, kind) triple (its
    /// primary key).
    pub fn display_id(&self) -> TufArtifactDisplayId<'_> {
        TufArtifactDisplayId(self)
    }
}

/// A wrapper around [`TufArtifact`] that implements [`std::fmt::Display`] to display
/// the artifact's (name, version, kind) triple (its primary key).
pub struct TufArtifactDisplayId<'a>(&'a TufArtifact);

impl fmt::Display for TufArtifactDisplayId<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{} ({})", self.0.name, self.0.version, self.0.kind)
    }
}

/// A many-to-many relationship between [`TufRepo`] and [`TufArtifact`].
#[derive(
    Queryable, Identifiable, Insertable, Clone, Debug, Selectable, Associations,
)]
#[diesel(belongs_to(TufRepo))]
// Diesel doesn't support belongs_to with a composite primary key, sadly, so
// we'll have to construct queries by hand.
#[diesel(table_name = tuf_repo_artifact)]
#[diesel(primary_key(
    tuf_repo_id,
    tuf_artifact_name,
    tuf_artifact_version,
    tuf_artifact_kind
))]
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
        let s = String::from_sql(bytes)?;
        ExternalArtifactHash::from_str(&s)
            .map(ArtifactHash)
            .map_err(|e| e.into())
    }
}
