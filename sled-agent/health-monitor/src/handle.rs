// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::health_checks::poll_smf_services_enabled_not_online;
use crate::health_checks::poll_smf_services_in_maintenance;

use illumos_utils::svcs::SvcsInMaintenanceResult;
use illumos_utils::svcs::SvcsResult;
use sled_agent_types::inventory::HealthMonitorInventory;
use sled_agent_types::inventory::SvcsEnabledNotOnline;
use slog::Logger;
use slog::info;
use tokio::sync::watch;

// TODO-K: Pending feedback from omicron/#9876. Remove all of this and make the
// call in-line during inventory collection.
#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // Return a String instead of a custom error type as inventory requires
    // all types to be cloneable. The only error that could happen here is
    // the failure to execute `svcs`, which is a `illumos_utils::ExecutionError`
    // and this error cannot be cloned.
    pub smf_services_in_maintenance_rx:
        watch::Receiver<Result<SvcsInMaintenanceResult, String>>,
    pub smf_services_enabled_not_online_rx:
        watch::Receiver<Result<SvcsResult, String>>,
}

impl HealthMonitorHandle {
    /// Returns a `HealthMonitorHandle` that doesn't monitor health and always
    /// reports no problems
    pub fn stub() -> Self {
        let (_tx, smf_services_in_maintenance_rx) =
            watch::channel(Ok(SvcsInMaintenanceResult::new()));
        let (_tx, smf_services_enabled_not_online_rx) =
            watch::channel(Ok(SvcsResult::new()));
        Self {
            smf_services_in_maintenance_rx,
            smf_services_enabled_not_online_rx,
        }
    }

    pub fn spawn(log: Logger) -> Self {
        // Spawn a task to retrieve information about services in maintenance
        info!(log, "Starting SMF service health poller");

        let (smf_services_in_maintenance_tx, smf_services_in_maintenance_rx) =
            watch::channel(Ok(SvcsInMaintenanceResult::new()));

        let log_2 = log.clone();
        tokio::spawn(async move {
            poll_smf_services_in_maintenance(
                log_2,
                smf_services_in_maintenance_tx,
            )
            .await
        });

        let (
            smf_services_enabled_not_online_tx,
            smf_services_enabled_not_online_rx,
        ) = watch::channel(Ok(SvcsResult::new()));

        // TODO-K: remove this clone
        let log_3 = log.clone();
        tokio::spawn(async move {
            poll_smf_services_enabled_not_online(
                log_3,
                smf_services_enabled_not_online_tx,
            )
            .await
        });

        Self {
            smf_services_in_maintenance_rx,
            smf_services_enabled_not_online_rx,
        }
    }

    pub fn to_inventory(&self) -> HealthMonitorInventory {
        HealthMonitorInventory {
            smf_services_in_maintenance: self
                .smf_services_in_maintenance_rx
                .borrow()
                .clone(),
        }
    }

    // TODO-K: The more I see this the more I think this should be encapsulated
    // in a type
    // TODO-K: change name to to_inventory?
    pub fn to_svcs_inventory(&self) -> Result<SvcsEnabledNotOnline, String> {
        match self.smf_services_enabled_not_online_rx.borrow().clone() {
            Ok(svcs) => {
                let SvcsResult { services, errors, time_of_status } = svcs;
                Ok(SvcsEnabledNotOnline { services, errors, time_of_status })
            }
            Err(e) => Err(e),
        }
    }
}
