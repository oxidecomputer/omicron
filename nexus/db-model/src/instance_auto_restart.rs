// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_auto_restart", schema = "public"))]
    pub struct InstanceAutoRestartEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = InstanceAutoRestartEnum)]
    pub enum InstanceAutoRestart;

    // Enum values
    Never => b"never"
    SledFailuresOnly => b"sled_failures_only"
    AllFailures => b"all_failures"
);

impl InstanceAutoRestart {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::SledFailuresOnly => "sled_failures_only",
            Self::AllFailures => "all_failures",
        }
    }
}

impl Default for InstanceAutoRestart {
    fn default() -> Self {
        Self::SledFailuresOnly
    }
}

impl fmt::Display for InstanceAutoRestart {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl diesel::query_builder::QueryId for InstanceAutoRestart {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
