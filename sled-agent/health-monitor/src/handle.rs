// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::health_checks::poll_smf_services_in_maintenance;
use crate::health_checks::poll_unhealthy_zpools;

use illumos_utils::svcs::SvcsInMaintenanceResult;
use illumos_utils::zpool::UnhealthyZpoolsResult;
use sled_agent_types::inventory::HealthMonitorInventory;
use slog::Logger;
use slog::info;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // Return a String instead of a custom error type as inventory requires
    // all types to be cloneable. The only error that could happen here is
    // the failure to execute the command, which is a
    // `illumos_utils::ExecutionError` and this error cannot be cloned.
    pub smf_services_in_maintenance_rx:
        watch::Receiver<Option<Result<SvcsInMaintenanceResult, String>>>,
    pub unhealthy_zpools_rx:
        watch::Receiver<Option<Result<UnhealthyZpoolsResult, String>>>,
}

impl HealthMonitorHandle {
    /// Returns a `HealthMonitorHandle` that doesn't monitor health and always
    /// reports no problems
    pub fn stub() -> Self {
        let (_tx, smf_services_in_maintenance_rx) = watch::channel(None);
        let (_tx, unhealthy_zpools_rx) = watch::channel(None);
        Self { smf_services_in_maintenance_rx, unhealthy_zpools_rx }
    }

    pub fn spawn(log: Logger) -> Self {
        // Spawn a task to retrieve information about services in maintenance
        info!(log, "Starting SMF service health poller");

        let (smf_services_in_maintenance_tx, smf_services_in_maintenance_rx) =
            watch::channel(None);

        let zpool_log = log.clone();
        tokio::spawn(async move {
            poll_smf_services_in_maintenance(
                log,
                smf_services_in_maintenance_tx,
            )
            .await
        });

        // Spawn a task to retrieve information about unhealthy zpools
        info!(zpool_log, "Starting Zpool health poller");

        let (unhealthy_zpools_tx, unhealthy_zpools_rx) = watch::channel(None);

        tokio::spawn(async move {
            poll_unhealthy_zpools(zpool_log, unhealthy_zpools_tx).await
        });

        Self { smf_services_in_maintenance_rx, unhealthy_zpools_rx }
    }

    pub fn to_inventory(&self) -> HealthMonitorInventory {
        HealthMonitorInventory {
            smf_services_in_maintenance: self
                .smf_services_in_maintenance_rx
                .borrow()
                .clone(),
            unhealthy_zpools: self.unhealthy_zpools_rx.borrow().clone(),
        }
    }
}
