// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::health_checks::poll_smf_services_in_maintenance;

use illumos_utils::svcs::SvcsInMaintenanceResult;
use nexus_sled_agent_shared::inventory::HealthMonitorInventory;
use slog::Logger;
use slog::info;
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

    pub fn spawn(log: &Logger) -> Self {
        let health_handle = HealthMonitorHandle::new();

        // Spawn a task to retrieve information about services in maintenance
        info!(log, "Starting SMF service health poller");
        let health_handle2 = health_handle.clone();
        let log = log.clone();
        tokio::spawn(async move {
            poll_smf_services_in_maintenance(
                log,
                health_handle2.smf_services_in_maintenance_tx,
            )
            .await
        });

        health_handle
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
