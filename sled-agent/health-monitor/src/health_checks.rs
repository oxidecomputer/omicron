// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for running health checks from the sled agent

use chrono::Utc;
use illumos_utils::svcs::SvcInMaintenance;
use illumos_utils::svcs::Svcs;
use illumos_utils::svcs::SvcsInMaintenanceResult;
use slog::Logger;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio::time::interval;

pub(crate) async fn poll_smf_services_in_maintenance(
    log: Logger,
    smf_services_in_maintenance_tx: watch::Sender<
        Result<SvcsInMaintenanceResult, String>,
    >,
) {
    // We poll every minute to verify the health of all services. This interval
    // is arbitrary.
    let mut interval = interval(Duration::from_secs(60));

    // If one of these calls to `svcs` takes longer than a minute,
    // `MissedTickBehavior::Skip` ensures that the health check happens every
    // interval, rather than bursting.
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        match Svcs::in_maintenance(&log).await {
            // There isn't anything waiting for changes because we only look at
            // the health check status when an inventory request comes in. This
            // means we can safely use `send_modify` instead of
            // `send_if_modified()`.
            Err(e) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = Err(e.to_string());
            }),
            Ok(svcs) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = Ok(svcs);
            }),
        };
    }
}

pub(crate) async fn sim_poll_smf_services_in_maintenance(
    // TODO-K: Add sim config here?
    smf_services_in_maintenance_tx: watch::Sender<
        Result<SvcsInMaintenanceResult, String>,
    >,
) {
    // We poll every minute to mimic what the actual health monitor does
    let mut interval = interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        smf_services_in_maintenance_tx.send_modify(|status| {
            // TODO-K: Set the config here instead
            *status = Ok(SvcsInMaintenanceResult {
                services: vec![SvcInMaintenance {
                    fmri: "fake".to_string(),
                    zone: "fake-global".to_string(),
                }],
                errors: vec![],
                time_of_status: Some(Utc::now()),
            });
        })
    }
}
