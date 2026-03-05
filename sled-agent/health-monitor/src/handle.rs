// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::health_checks::poll_smf_services_enabled_not_online;

use illumos_utils::svcs::SvcsResult;
use sled_agent_types::inventory::SvcsEnabledNotOnline;
use slog::Logger;
use slog::info;
use tokio::sync::watch;

// TODO-K: Pending omicron/#9935. Remove all of this and make the call in-line
// during inventory collection using the new mechanism.
#[derive(Debug, Clone)]
pub struct HealthMonitorHandle {
    // Return a String instead of a custom error type as inventory requires
    // all types to be cloneable. The only error that could happen here is
    // the failure to execute `svcs`, which is a `illumos_utils::ExecutionError`
    // and this error cannot be cloned.
    pub smf_services_enabled_not_online_rx:
        watch::Receiver<Option<Result<SvcsResult, String>>>,
}

impl HealthMonitorHandle {
    /// Returns a `HealthMonitorHandle` that doesn't monitor health and always
    /// reports no problems
    pub fn stub() -> Self {
        let (_tx, smf_services_enabled_not_online_rx) = watch::channel(None);
        Self { smf_services_enabled_not_online_rx }
    }

    pub fn spawn(log: Logger) -> Self {
        // Spawn a task to retrieve information about services in maintenance
        info!(log, "Starting SMF service health poller");

        let (
            smf_services_enabled_not_online_tx,
            smf_services_enabled_not_online_rx,
        ) = watch::channel(None);

        tokio::spawn(async move {
            poll_smf_services_enabled_not_online(
                log,
                smf_services_enabled_not_online_tx,
            )
            .await
        });

        Self { smf_services_enabled_not_online_rx }
    }

    pub fn to_inventory(&self) -> Option<Result<SvcsEnabledNotOnline, String>> {
        match self.smf_services_enabled_not_online_rx.borrow().clone() {
            Some(result) => match result {
                Ok(svcs) => {
                    let SvcsResult { services, errors, time_of_status } = svcs;
                    Some(Ok(SvcsEnabledNotOnline {
                        services,
                        errors,
                        time_of_status,
                    }))
                }
                Err(e) => Some(Err(e)),
            },
            None => None,
        }
    }
}
