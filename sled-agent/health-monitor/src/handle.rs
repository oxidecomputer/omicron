// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::svcs::SvcsInMaintenanceResult;
use nexus_sled_agent_shared::inventory::HealthMonitorInventory;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    pub smf_services_in_maintenance_tx:
        watch::Sender<Result<SvcsInMaintenanceResult, String>>,
}

impl HealthMonitorHandle {
    pub fn new() -> Self {
        let (smf_services_in_maintenance_tx, _rx) =
            watch::channel(Ok(SvcsInMaintenanceResult::new()));
        Self { smf_services_in_maintenance_tx }
    }

    pub fn to_inventory(&self) -> HealthMonitorInventory {
        HealthMonitorInventory {
            smf_services_in_maintenance: self
                .smf_services_in_maintenance_tx
                .borrow()
                .clone(),
        }
    }
}

// /// Fields of sled-agent inventory reported by the health monitor subsystem.
// #[derive(Debug, Clone)]
// pub struct HealthMonitorInventory {
//     pub smf_services_in_maintenance: Result<SvcsInMaintenanceResult, String>,
//
//     // TODO: Other health check results will live here as well
// }
