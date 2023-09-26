// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::StartupError;
use crate::management_switch::ManagementSwitch;
use crate::management_switch::SwitchConfig;
use gateway_sp_comms::InMemoryHostPhase2Provider;
use slog::{error, info, Logger};
use std::sync::Arc;
use std::sync::OnceLock;
use uuid::Uuid;

/// Shared state used by API request handlers
pub struct ServerContext {
    pub mgmt_switch: ManagementSwitch,
    pub host_phase2_provider: Arc<InMemoryHostPhase2Provider>,
    pub rack_id: OnceLock<Uuid>,
    pub log: Logger,
}

impl ServerContext {
    pub async fn new(
        host_phase2_provider: Arc<InMemoryHostPhase2Provider>,
        switch_config: SwitchConfig,
        rack_id_config: Option<Uuid>,
        log: &Logger,
    ) -> Result<Arc<Self>, StartupError> {
        let mgmt_switch =
            ManagementSwitch::new(switch_config, &host_phase2_provider, log)
                .await?;

        let rack_id = OnceLock::new();
        if let Some(id) = rack_id_config {
            info!(log, "Setting rack_id: {id} for MGS");
            match rack_id.set(id) {
                Ok(()) => info!(log, "Setting rack_id: {id} for MGS"),
                Err(existing) => error!(
                    log,
                    "Failed to set rack_id: {id}. Already set to {existing}"
                ),
            }
        }

        Ok(Arc::new(ServerContext {
            mgmt_switch,
            host_phase2_provider,
            rack_id,
            log: log.clone(),
        }))
    }
}
