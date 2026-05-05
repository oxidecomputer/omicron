// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// The resource usage of a database table.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct TableUsage {
    /// The name of the database table.
    pub name: String,
    /// The number of bytes consumed on disk.
    pub n_bytes: u64,
    /// The number of rows in the table.
    pub n_rows: u64,
}

impl IdOrdItem for TableUsage {
    type Key<'a> = &'a String;

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    id_upcast!();
}

/// The resource usage of all database tables.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseUsage {
    /// Usage for each table.
    pub tables: IdOrdMap<TableUsage>,
    /// The time at which the usage calculation was started.
    pub started_at: DateTime<Utc>,
    /// The time at which the usage calculation was completed.
    pub completed_at: DateTime<Utc>,
}

/// An error when computing the usage of the database.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseUsageError {
    pub timestamp: DateTime<Utc>,
    pub error: String,
}

/// The result of ongoing attempts to compute database usage.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseUsageResult {
    /// The last successful result computing the usage.
    pub last_success: Option<DatabaseUsage>,
    /// The last error when computing the usage.
    pub last_error: Option<DatabaseUsageError>,
}
