// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::params;
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

impl fmt::Display for InstanceAutoRestart {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.label().fmt(f)
    }
}

impl From<InstanceAutoRestart> for params::InstanceAutoRestart {
    fn from(value: InstanceAutoRestart) -> Self {
        match value {
            InstanceAutoRestart::Never => Self::Never,
            InstanceAutoRestart::SledFailuresOnly => Self::SledFailuresOnly,
            InstanceAutoRestart::AllFailures => Self::AllFailures,
        }
    }
}

impl From<params::InstanceAutoRestart> for InstanceAutoRestart {
    fn from(value: params::InstanceAutoRestart) -> Self {
        match value {
            params::InstanceAutoRestart::Never => Self::Never,
            params::InstanceAutoRestart::SledFailuresOnly => {
                Self::SledFailuresOnly
            }
            params::InstanceAutoRestart::AllFailures => Self::AllFailures,
        }
    }
}

impl diesel::query_builder::QueryId for InstanceAutoRestartEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
