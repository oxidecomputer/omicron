// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for running health checks from the sled agent

use illumos_utils::svcs::Svcs;
use illumos_utils::svcs::SvcsInMaintenanceResult;
use illumos_utils::zpool::UnhealthyZpoolsResult;
use illumos_utils::zpool::Zpool;
use slog::Logger;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio::time::interval;

pub(crate) async fn poll_smf_services_in_maintenance(
    log: Logger,
    smf_services_in_maintenance_tx: watch::Sender<
        Option<Result<SvcsInMaintenanceResult, String>>,
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
                *status = Some(Err(e.to_string()));
            }),
            Ok(svcs) => smf_services_in_maintenance_tx.send_modify(|status| {
                *status = Some(Ok(svcs));
            }),
        };
    }
}

pub(crate) async fn poll_unhealthy_zpools(
    log: Logger,
    unhealthy_zpools_tx: watch::Sender<
        Option<Result<UnhealthyZpoolsResult, String>>,
    >,
) {
    // We poll every minute to verify the health of all zpools. This interval
    // is arbitrary.
    let mut interval = interval(Duration::from_secs(60));

    // If one of these calls to `zpool` takes longer than a minute,
    // `MissedTickBehavior::Skip` ensures that the health check happens every
    // interval, rather than bursting.
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        match Zpool::status_unhealthy(&log).await {
            // As above, there isn't anything waiting for changes because we
            // only look at the health check status when an inventory request
            // comes in.
            Err(e) => unhealthy_zpools_tx.send_modify(|status| {
                *status = Some(Err(e.to_string()));
            }),
            Ok(zpools) => unhealthy_zpools_tx.send_modify(|status| {
                *status = Some(Ok(zpools));
            }),
        };
    }
}
