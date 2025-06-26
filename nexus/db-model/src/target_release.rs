// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, impl_enum_type};
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::target_release;
use nexus_types::external_api::views;
use omicron_uuid_kinds::TufRepoKind;

impl_enum_type!(
    TargetReleaseSourceEnum:

    /// The source of the software release that should be deployed to the rack.
    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Hash)]
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
    pub fn new_unspecified(prev: &TargetRelease) -> Self {
        Self {
            generation: Generation(prev.generation.next()),
            time_requested: Utc::now(),
            release_source: TargetReleaseSource::Unspecified,
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
            release_source: TargetReleaseSource::SystemVersion,
            tuf_repo_id: Some(tuf_repo_id),
        }
    }

    pub fn into_external(
        &self,
        release_source: views::TargetReleaseSource,
        mupdate_override: Option<views::TargetReleaseMupdateOverride>,
    ) -> views::TargetRelease {
        views::TargetRelease {
            generation: (&self.generation.0).into(),
            time_requested: self.time_requested,
            release_source,
            mupdate_override,
        }
    }
}
