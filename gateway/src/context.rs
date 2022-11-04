// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_sp_comms::Communicator;
use gateway_sp_comms::{error::StartupError, SwitchConfig};
use slog::Logger;
use std::{sync::Arc, time::Duration};

/// Shared state used by API request handlers
pub struct ServerContext {
    pub sp_comms: Arc<Communicator>,
    pub timeouts: Timeouts,
    pub log: Logger,
}

pub struct Timeouts {
    pub bulk_request_default: Duration,
    pub bulk_request_max: Duration,
    pub bulk_request_page: Duration,
    pub bulk_request_retain_grace_period: Duration,
}

impl From<&'_ crate::config::Timeouts> for Timeouts {
    fn from(timeouts: &'_ crate::config::Timeouts) -> Self {
        Self {
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
        switch_config: SwitchConfig,
        timeouts: crate::config::Timeouts,
        log: &Logger,
    ) -> Result<Arc<Self>, StartupError> {
        let comms = Arc::new(Communicator::new(switch_config, log).await?);
        Ok(Arc::new(ServerContext {
            sp_comms: Arc::clone(&comms),
            timeouts: Timeouts::from(&timeouts),
            log: log.clone(),
        }))
    }
}
