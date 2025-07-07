// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SemverVersion;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::db_metadata;
use serde::{Deserialize, Serialize};

/// Internal database metadata
#[derive(
    Queryable, Insertable, Debug, Clone, Selectable, Serialize, Deserialize,
)]
#[diesel(table_name = db_metadata)]
pub struct DbMetadata {
    singleton: bool,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
    version: SemverVersion,
    target_version: Option<SemverVersion>,
    quiesce_started: bool,
    quiesce_completed: bool,
}

impl DbMetadata {
    pub fn time_created(&self) -> &DateTime<Utc> {
        &self.time_created
    }

    pub fn time_modified(&self) -> &DateTime<Utc> {
        &self.time_modified
    }

    pub fn version(&self) -> &SemverVersion {
        &self.version
    }

    pub fn quiesce_started(&self) -> bool {
        self.quiesce_started
    }

    pub fn quiesce_completed(&self) -> bool {
        self.quiesce_completed
    }
}
