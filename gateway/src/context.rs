// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    sp_comms::{SpCommunicator, StartupError},
    Config,
};
use slog::Logger;
use std::{sync::Arc, time::Duration};

/// Shared state used by API request handlers
pub struct ServerContext {
    pub sp_comms: SpCommunicator,
    pub ignition_controller_timeout: Duration,
    pub sp_request_timeout: Duration,
}

impl ServerContext {
    pub async fn new(
        config: &Config,
        log: &Logger,
    ) -> Result<Arc<Self>, StartupError> {
        let sp_comms = SpCommunicator::new(
            config.udp_bind_address,
            config.known_sps.clone(),
            log,
        )
        .await?;
        Ok(Arc::new(ServerContext {
            sp_comms,
            ignition_controller_timeout: Duration::from_millis(
                config.timeouts.ignition_controller_milliseconds,
            ),
            sp_request_timeout: Duration::from_millis(
                config.timeouts.sp_request_milliseconds,
            ),
        }))
    }
}
