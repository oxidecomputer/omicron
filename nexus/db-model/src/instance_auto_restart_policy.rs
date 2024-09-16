// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_auto_restart_v2", schema = "public"))]
    pub struct InstanceAutoRestartPolicyEnum;

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
    #[diesel(sql_type = InstanceAutoRestartPolicyEnum)]
    pub enum InstanceAutoRestartPolicy;

    // Enum values
    Never => b"never"
    BestEffort => b"best_effort"
);

impl InstanceAutoRestartPolicy {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::BestEffort => "best_effort",
        }
    }
}

impl fmt::Display for InstanceAutoRestartPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.label().fmt(f)
    }
}

impl From<InstanceAutoRestartPolicy> for external::InstanceAutoRestartPolicy {
    fn from(value: InstanceAutoRestartPolicy) -> Self {
        match value {
            InstanceAutoRestartPolicy::Never => Self::Never,
            InstanceAutoRestartPolicy::BestEffort => Self::BestEffort,
        }
    }
}

impl From<external::InstanceAutoRestartPolicy> for InstanceAutoRestartPolicy {
    fn from(value: external::InstanceAutoRestartPolicy) -> Self {
        match value {
            external::InstanceAutoRestartPolicy::Never => Self::Never,
            external::InstanceAutoRestartPolicy::BestEffort => Self::BestEffort,
        }
    }
}

impl diesel::query_builder::QueryId for InstanceAutoRestartPolicyEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
