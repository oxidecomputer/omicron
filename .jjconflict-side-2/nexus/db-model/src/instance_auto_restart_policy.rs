// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::external_api::instance;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    InstanceAutoRestartPolicyEnum:

    #[derive(Copy, Clone, Debug, PartialEq, AsExpression, FromSqlRow, Serialize, Deserialize)]
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

impl From<InstanceAutoRestartPolicy> for instance::InstanceAutoRestartPolicy {
    fn from(value: InstanceAutoRestartPolicy) -> Self {
        match value {
            InstanceAutoRestartPolicy::Never => Self::Never,
            InstanceAutoRestartPolicy::BestEffort => Self::BestEffort,
        }
    }
}

impl From<instance::InstanceAutoRestartPolicy> for InstanceAutoRestartPolicy {
    fn from(value: instance::InstanceAutoRestartPolicy) -> Self {
        match value {
            instance::InstanceAutoRestartPolicy::Never => Self::Never,
            instance::InstanceAutoRestartPolicy::BestEffort => Self::BestEffort,
        }
    }
}
