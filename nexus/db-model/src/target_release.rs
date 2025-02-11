// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation};
use crate::schema::target_release;
use crate::SemverVersion;
use chrono::{DateTime, Utc};
use nexus_types::external_api::shared;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "target_release_source", schema = "public"))]
    pub struct TargetReleaseSourceEnum;

    /// The source of the software release that should be deployed to the rack.
    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Hash)]
    #[diesel(sql_type = TargetReleaseSourceEnum)]
    pub enum TargetReleaseSource;

    InstallDataset => b"install_dataset"
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

    /// The semantic version of the target release.
    pub system_version: Option<SemverVersion>,
}

impl TargetRelease {
    pub fn new(
        generation: Generation,
        release_source: TargetReleaseSource,
        system_version: Option<SemverVersion>,
    ) -> Self {
        Self {
            generation,
            time_requested: Utc::now(),
            release_source,
            system_version,
        }
    }

    pub fn new_from_prev(
        prev: TargetRelease,
        release_source: TargetReleaseSource,
        system_version: Option<SemverVersion>,
    ) -> Self {
        Self::new(
            Generation(prev.generation.next()),
            release_source,
            system_version,
        )
    }

    pub fn into_external(self) -> shared::TargetRelease {
        shared::TargetRelease {
            generation: (&self.generation.0).into(),
            time_requested: self.time_requested,
            release_source: match self.release_source {
                TargetReleaseSource::InstallDataset => {
                    shared::TargetReleaseSource::InstallDataset
                }
                TargetReleaseSource::SystemVersion => {
                    shared::TargetReleaseSource::SystemVersion(
                        self.system_version
                            .expect("CONSTRAINT system_version_for_release")
                            .into(),
                    )
                }
            },
        }
    }
}
