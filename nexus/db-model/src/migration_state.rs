// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a migration's state as understood by Nexus.

use super::impl_enum_wrapper;
use omicron_common::api::internal::nexus;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::io::Write;

impl_enum_wrapper!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "migration_state", schema = "public"))]
    pub struct MigrationStateEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq, Eq)]
    #[diesel(sql_type = MigrationStateEnum)]
    pub struct MigrationState(pub nexus::MigrationState);

    // Enum values
    Pending => b"pending"
    InProgress => b"in_progress"
    Completed => b"completed"
    Failed => b"failed"
);

impl MigrationState {
    pub const COMPLETED: MigrationState =
        MigrationState(nexus::MigrationState::Completed);
    pub const FAILED: MigrationState =
        MigrationState(nexus::MigrationState::Failed);

    /// Returns `true` if this migration state means that the migration is no
    /// longer in progress (it has either succeeded or failed).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.0.is_terminal()
    }
}

impl fmt::Display for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<nexus::MigrationState> for MigrationState {
    fn from(s: nexus::MigrationState) -> Self {
        Self(s)
    }
}
