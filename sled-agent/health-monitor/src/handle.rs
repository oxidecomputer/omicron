// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::health_checks::poll_smf_services_in_maintenance;

use illumos_utils::svcs::SvcsInMaintenanceResult;
use sled_agent_types::inventory::HealthMonitorInventory;
use slog::Logger;
use slog::info;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // Return a String instead of a custom error type as inventory requires
    // all types to be cloneable. The only error that could happen here is
    // the failure to execute `svcs`, which is a `illumos_utils::ExecutionError`
    // and this error cannot be cloned.
    pub smf_services_in_maintenance_rx:
        watch::Receiver<Result<SvcsInMaintenanceResult, String>>,
}

impl HealthMonitorHandle {
    pub fn stub() -> Self {
        let (_tx, smf_services_in_maintenance_rx) =
            watch::channel(Ok(SvcsInMaintenanceResult::new()));
        Self { smf_services_in_maintenance_rx }
    }

    pub fn spawn(log: Logger) -> Self {
        // Spawn a task to retrieve information about services in maintenance
        info!(log, "Starting SMF service health poller");

        let (smf_services_in_maintenance_tx, smf_services_in_maintenance_rx) =
            watch::channel(Ok(SvcsInMaintenanceResult::new()));

        tokio::spawn(async move {
            poll_smf_services_in_maintenance(
                log,
                smf_services_in_maintenance_tx,
            )
            .await
        });

        Self { smf_services_in_maintenance_rx }
    }

    pub fn to_inventory(&self) -> HealthMonitorInventory {
        HealthMonitorInventory {
            smf_services_in_maintenance: self
                .smf_services_in_maintenance_rx
                .borrow()
                .clone(),
        }
    }
}
