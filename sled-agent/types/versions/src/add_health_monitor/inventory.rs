// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use omicron_common::snake_case_result;
use omicron_common::snake_case_result::SnakeCaseResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub use crate::v11::inventory::ConfigReconcilerInventory;
pub use crate::v11::inventory::ConfigReconcilerInventoryStatus;
pub use crate::v11::inventory::OmicronSledConfig;

/// Fields of sled-agent inventory reported by the health monitor subsystem.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct HealthMonitorInventory {
    #[serde(with = "snake_case_result")]
    #[schemars(
        schema_with = "SnakeCaseResult::<SvcsInMaintenanceResult, String>::json_schema"
    )]
    pub smf_services_in_maintenance: Result<SvcsInMaintenanceResult, String>,
    // TODO: Other health check results will live here as well
}

impl HealthMonitorInventory {
    pub fn new() -> Self {
        Self { smf_services_in_maintenance: Ok(SvcsInMaintenanceResult::new()) }
    }

    pub fn is_empty(&self) -> bool {
        let Self { smf_services_in_maintenance } = self;

        if let Ok(svcs) = smf_services_in_maintenance {
            svcs.is_empty()
        } else {
            false
        }
    }
}

/// Lists services in maintenance status if any, and the time the health check
/// for SMF services ran
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SvcsInMaintenanceResult {
    pub services: Vec<SvcInMaintenance>,
    pub errors: Vec<String>,
    pub time_of_status: Option<DateTime<Utc>>,
}

impl SvcsInMaintenanceResult {
    pub fn new() -> Self {
        Self { services: vec![], errors: vec![], time_of_status: None }
    }

    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
            && self.errors.is_empty()
            && self.time_of_status == None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct SvcInMaintenance {
    fmri: String,
    zone: String,
}

impl Display for SvcInMaintenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let SvcInMaintenance { fmri, zone } = self;

        writeln!(f, "FMRI: {} zone: {}", fmri, zone)
    }
}
