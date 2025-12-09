// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::svcs::SvcInMaintenance;
use illumos_utils::svcs::Svcs;
use slog::Logger;
use slog::error;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // TODO-K: Do I actually need the logger here?
    //pub log: Logger,
    pub smf_services_in_maintenance_tx: watch::Sender<Vec<SvcInMaintenance>>,
}

impl HealthMonitorHandle {
    pub fn new() -> Self {
        let (smf_services_in_maintenance_tx, _rx) =
            watch::channel(vec![SvcInMaintenance::new()]);
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

/// Fields of sled-agent inventory reported by the health monitor subsystem.
#[derive(Debug, Clone)]
pub struct HealthMonitorInventory {
    pub smf_services_in_maintenance: Vec<SvcInMaintenance>,
}

// TODO-K: Put this in another file?
pub async fn poll_smf_services_in_maintenance(
    log: Logger,
    smf_services_in_maintenance_tx: watch::Sender<Vec<SvcInMaintenance>>,
) {
    // We poll every minute to verify the health of all services. This interval
    // is arbitrary.
    const SVCS_POLL_INTERVAL: tokio::time::Duration =
        tokio::time::Duration::from_secs(60);

    loop {
        match Svcs::in_maintenance(&log).await {
            Err(e) => error!(log, "failed to check SMF services' health"; &e),
            Ok(svcs) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = svcs;
            }),
        };
        tokio::time::sleep(SVCS_POLL_INTERVAL).await;
    }
}
