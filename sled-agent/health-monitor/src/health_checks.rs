// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for running health checks from the sled agent

use illumos_utils::svcs::Svcs;
use illumos_utils::svcs::SvcsInMaintenanceResult;
use slog::Logger;
use tokio::sync::watch;

pub(crate) async fn poll_smf_services_in_maintenance(
    log: Logger,
    smf_services_in_maintenance_tx: watch::Sender<
        Result<SvcsInMaintenanceResult, String>,
    >,
) {
    // We poll every minute to verify the health of all services. This interval
    // is arbitrary.
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(60));

    loop {
        interval.tick().await;
        match Svcs::in_maintenance(&log).await {
            Err(e) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = Err(e.to_string());
            }),
            Ok(svcs) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = Ok(svcs);
            }),
        };
    }
}
