// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2::retention::Days;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// A request for setting a retention policy.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RetentionPolicyRequest {
    /// The requested retention period, in days.
    pub days: Days,
}

impl From<crate::v2::retention::RetentionPolicy> for RetentionPolicyRequest {
    fn from(value: crate::v2::retention::RetentionPolicy) -> Self {
        Self { days: value.days }
    }
}

/// Policy for retaining telemetry data for a database table.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RetentionPolicy {
    /// The table the policy applies to.
    pub table: String,
    /// The retention period, in days.
    pub days: Days,
}

impl From<RetentionPolicy> for crate::v2::retention::RetentionPolicy {
    fn from(value: RetentionPolicy) -> Self {
        Self { days: value.days }
    }
}

impl IdOrdItem for RetentionPolicy {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        self.table.as_str()
    }

    id_upcast!();
}

/// Policy for retaining telemetry data for all tables.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct DatabaseRetentionPolicy {
    pub tables: IdOrdMap<RetentionPolicy>,
}
