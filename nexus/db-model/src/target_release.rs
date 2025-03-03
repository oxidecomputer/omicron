// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation};
use crate::schema::target_release;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::TufRepoKind;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "target_release_source", schema = "public"))]
    pub struct TargetReleaseSourceEnum;

    /// The source of the software release that should be deployed to the rack.
    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Hash)]
    #[diesel(sql_type = TargetReleaseSourceEnum)]
    pub enum TargetReleaseSource;

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
    pub release_source: TargetReleaseSource,

    /// The TUF repo containing the target release.
    pub tuf_repo_id: Option<DbTypedUuid<TufRepoKind>>,
}

impl TargetRelease {
    pub fn new(
        generation: Generation,
        release_source: TargetReleaseSource,
        tuf_repo_id: Option<DbTypedUuid<TufRepoKind>>,
    ) -> Self {
        Self {
            generation,
            time_requested: Utc::now(),
            release_source,
            tuf_repo_id,
        }
    }

    pub fn new_from_prev(
        prev: TargetRelease,
        release_source: TargetReleaseSource,
        tuf_repo_id: Option<DbTypedUuid<TufRepoKind>>,
    ) -> Self {
        Self::new(
            Generation(prev.generation.next()),
            release_source,
            tuf_repo_id,
        )
    }
}
