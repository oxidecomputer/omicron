// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::StartupError;
use crate::management_switch::ManagementSwitch;
use crate::management_switch::SwitchConfig;
use gateway_sp_comms::InMemoryHostPhase2Provider;
use slog::{info, Logger};
use std::sync::Arc;
use std::sync::OnceLock;
use uuid::Uuid;

/// Shared state used by API request handlers
pub struct ServerContext {
    pub mgmt_switch: ManagementSwitch,
    pub host_phase2_provider: Arc<InMemoryHostPhase2Provider>,
    pub rack_id: OnceLock<Uuid>,
    pub latencies: oximeter_instruments::http::LatencyTracker,
    pub log: Logger,
}

impl ServerContext {
    pub async fn new(
        id: Uuid,
        host_phase2_provider: Arc<InMemoryHostPhase2Provider>,
        switch_config: SwitchConfig,
        rack_id_config: Option<Uuid>,
        log: &Logger,
    ) -> Result<Arc<Self>, StartupError> {
        let mgmt_switch =
            ManagementSwitch::new(switch_config, &host_phase2_provider, log)
                .await?;

        let rack_id = if let Some(id) = rack_id_config {
            info!(log, "Setting rack_id"; "rack_id" => %id);
            OnceLock::from(id)
        } else {
            OnceLock::new()
        };

        // Track from 1 microsecond == 1e3 nanoseconds
        const START_LATENCY_DECADE: u16 = 3;
        // To 1000s == 1e9 * 1e3 == 1e12 nanoseconds
        const END_LATENCY_DECADE: u16 = 12;
        let latencies =
            oximeter_instruments::http::LatencyTracker::with_log_linear_bins(
                oximeter_instruments::http::HttpService {
                    name: "management-gateway-service".into(),
                    id,
                },
                START_LATENCY_DECADE,
                END_LATENCY_DECADE,
            )
            .expect("start and end decades are hardcoded and should be valid");

        Ok(Arc::new(ServerContext {
            latencies,
            mgmt_switch,
            host_phase2_provider,
            rack_id,
            log: log.clone(),
        }))
    }
}
