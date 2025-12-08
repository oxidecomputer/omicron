// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use illumos_utils::svcs::{SvcNotRunning, Svcs};
use slog::Logger;
use slog::error;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // TODO-K: Do I actually need the logger here?
    pub log: Logger,
    pub smf_services_in_maintenance_tx: watch::Sender<Vec<SvcNotRunning>>,
}

impl HealthMonitorHandle {
    pub fn new(log: &Logger) -> Self {
        // TODO-K: Does this new() make sense? Or just spawn_health
        let (smf_services_in_maintenance_tx, _rx) =
            watch::channel(vec![SvcNotRunning::new()]);
        Self { log: log.clone(), smf_services_in_maintenance_tx }
    }

    // TODO-K: better return type with more health check fields?
    pub fn to_inventory(&self) -> Vec<SvcNotRunning> {
        // TODO-K: Add some logging?
        self.smf_services_in_maintenance_tx.borrow().clone()
    }
}

// TODO-K: Put this in another file?
pub async fn poll_smf_services_in_maintenance(
    log: Logger,
    smf_services_in_maintenance_tx: watch::Sender<Vec<SvcNotRunning>>,
) {
    // We poll every minute to verify the health of all services. This interval
    // is arbitrary.
    const SVCS_POLL_INTERVAL: tokio::time::Duration =
        tokio::time::Duration::from_secs(60);

    loop {
        match Svcs::enabled_not_running(&log).await {
            Err(e) => error!(log, "failed to check SMF services' health"; &e),
            Ok(svcs) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = svcs;
            }),
        };
        tokio::time::sleep(SVCS_POLL_INTERVAL).await;
    }
}
