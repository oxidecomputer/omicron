// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SemverVersion;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::db_metadata;
use serde::{Deserialize, Serialize};

/// These fields of "db_metadata" have been stable since the initial
/// release of the database.
///
/// For backwards-compatibility purposes, they can be loaded separately
/// from DbMetadata.
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadataBase {
    singleton: bool,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
    version: SemverVersion,
    target_version: Option<SemverVersion>,
}

impl DbMetadataBase {
    pub fn version(&self) -> &SemverVersion {
        &self.version
    }
}

/// Internal database metadata
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadata {
    #[diesel(embed)]
    base: DbMetadataBase,
    quiesce_started: bool,
    quiesce_completed: bool,
}

impl DbMetadata {
    pub fn from_base(base: DbMetadataBase, quiesced: bool) -> Self {
        Self { base, quiesce_started: quiesced, quiesce_completed: quiesced }
    }

    pub fn time_created(&self) -> &DateTime<Utc> {
        &self.base.time_created
    }

    pub fn time_modified(&self) -> &DateTime<Utc> {
        &self.base.time_modified
    }

    pub fn version(&self) -> &SemverVersion {
        &self.base.version
    }

    pub fn target_version(&self) -> Option<&SemverVersion> {
        self.base.target_version.as_ref()
    }

    pub fn quiesce_started(&self) -> bool {
        self.quiesce_started
    }

    pub fn quiesce_completed(&self) -> bool {
        self.quiesce_completed
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadataUpdate {
    time_modified: DateTime<Utc>,
    version: String,
    #[diesel(treat_none_as_null = true)]
    target_version: Option<String>,
    quiesce_started: Option<bool>,
    quiesce_completed: Option<bool>,
}

impl DbMetadataUpdate {
    pub fn update_to_version(version: semver::Version) -> Self {
        Self {
            time_modified: Utc::now(),
            version: version.to_string(),
            target_version: None,
            quiesce_started: None,
            quiesce_completed: None,
        }
    }

    pub fn clear_quiesce(&mut self) {
        self.quiesce_started = Some(false);
        self.quiesce_completed = Some(false);
    }
}
