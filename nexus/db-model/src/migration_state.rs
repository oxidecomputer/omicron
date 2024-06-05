// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a migration's state as understood by Nexus.

use super::impl_enum_type;
use serde::{Deserialize, Serialize};
use std::fmt;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "migration_state", schema = "public"))]
    pub struct MigrationStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq)]
    #[diesel(sql_type = MigrationStateEnum)]
     // match the database representation when serializing
    #[serde(rename_all = "snake_case")]
    pub enum MigrationState;

    // Enum values
    InProgress => b"in_progress"
    Completed => b"completed"
    Failed => b"failed"
);

impl MigrationState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}
