// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Generation;
use crate::typed_uuid::DbTypedUuid;
use anyhow::Context;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::target_release;
use omicron_uuid_kinds::{TufRepoKind, TufRepoUuid};

use crate::impl_enum_type;

impl_enum_type!(
    TargetReleaseSourceEnum:

    /// The source of the software release that should be deployed to the
    /// rack.
    #[derive(
        Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Hash,
    )]
    pub enum DbTargetReleaseSource;

    Unspecified => b"unspecified"
    SystemVersion => b"system_version"
);

/// Specify the target system software release.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = target_release)]
pub struct TargetRelease {
    /// Each change to the target release is recorded as a row with
    /// a monotonically increasing generation number. The row with
    /// the largest generation is the current target release.
    pub generation: Generation,

    /// The time at which this target release was requested.
    pub time_requested: DateTime<Utc>,

    /// The source of the target release.
    ///
    /// Strongly consider using `TargetRelease::release_source()` instead of
    /// accessing this field; this is `pub` to allow database queries.
    pub release_source: DbTargetReleaseSource,

    /// The TUF repo containing the target release.
    ///
    /// Strongly consider using `TargetRelease::release_source()` instead of
    /// accessing this field; this is `pub` to allow database queries.
    pub tuf_repo_id: Option<DbTypedUuid<TufRepoKind>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetReleaseSource {
    Unspecified,
    SystemVersion(TufRepoUuid),
}

impl TargetRelease {
    pub fn new_unspecified(prev: &TargetRelease) -> Self {
        Self {
            generation: Generation(prev.generation.next()),
            time_requested: Utc::now(),
            release_source: DbTargetReleaseSource::Unspecified,
            tuf_repo_id: None,
        }
    }

    pub fn new_system_version(
        prev: &TargetRelease,
        tuf_repo_id: DbTypedUuid<TufRepoKind>,
    ) -> Self {
        Self {
            generation: Generation(prev.generation.next()),
            time_requested: Utc::now(),
            release_source: DbTargetReleaseSource::SystemVersion,
            tuf_repo_id: Some(tuf_repo_id),
        }
    }

    pub fn release_source(&self) -> anyhow::Result<TargetReleaseSource> {
        match self.release_source {
            DbTargetReleaseSource::Unspecified => {
                Ok(TargetReleaseSource::Unspecified)
            }
            DbTargetReleaseSource::SystemVersion => {
                let repo_id = self.tuf_repo_id.with_context(|| {
                    format!(
                        "invalid database contents: target release generation \
                         {} has release source `system_version` with no \
                         `tuf_repo_id`",
                        *self.generation
                    )
                })?;
                Ok(TargetReleaseSource::SystemVersion(repo_id.into()))
            }
        }
    }
}
