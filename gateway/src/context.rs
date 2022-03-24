// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::sp_comms::SpCommunicator;
use crate::Config;
use gateway_sp_comms::error::StartupError;
use slog::Logger;
use std::{sync::Arc, time::Duration};

/// Shared state used by API request handlers
pub struct ServerContext {
    pub sp_comms: Arc<SpCommunicator>,
    pub timeouts: Timeouts,
}

pub struct Timeouts {
    pub ignition_controller: Duration,
    pub sp_request: Duration,
    pub bulk_request_default: Duration,
    pub bulk_request_max: Duration,
    pub bulk_request_page: Duration,
    pub bulk_request_retain_grace_period: Duration,
}

impl From<&'_ crate::config::Timeouts> for Timeouts {
    fn from(timeouts: &'_ crate::config::Timeouts) -> Self {
        Self {
            ignition_controller: Duration::from_millis(
                timeouts.ignition_controller_millis,
            ),
            sp_request: Duration::from_millis(timeouts.sp_request_millis),
            bulk_request_default: Duration::from_millis(
                timeouts.bulk_request_default_millis,
            ),
            bulk_request_max: Duration::from_millis(
                timeouts.bulk_request_max_millis,
            ),
            bulk_request_page: Duration::from_millis(
                timeouts.bulk_request_page_millis,
            ),
            bulk_request_retain_grace_period: Duration::from_millis(
                timeouts.bulk_request_retain_grace_period_millis,
            ),
        }
    }
}

impl ServerContext {
    pub async fn new(
        config: &Config,
        log: &Logger,
    ) -> Result<Arc<Self>, StartupError> {
        let sp_comms =
            Arc::new(SpCommunicator::new(config.known_sps.clone(), log).await?);
        Ok(Arc::new(ServerContext {
            sp_comms,
            timeouts: Timeouts::from(&config.timeouts),
        }))
    }
}
