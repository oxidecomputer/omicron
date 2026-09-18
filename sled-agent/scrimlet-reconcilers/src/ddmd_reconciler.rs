// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for the set of interfaces `ddmd` runs DDM on within a
//! scrimlet's switch zone.
//!
//! Rear ports always carry DDM; front ports only do so when the rack network
//! config marks them `allow_ddm_traffic`, which is how two racks peer over a
//! sidecar interconnect.

use crate::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ddmd::DdmdReconcilerStatus;
use ddm_admin_client::Client;
use ddm_api_types::external_peers::ExternalPeers;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct DdmdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for DdmdReconciler {
    type Status = DdmdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "DdmdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        Self { client: mode.ddmd_client(parent_log), switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let address_objects = system_networking_config
            .rack_network_config
            .ports
            .iter()
            .filter(|port| {
                port.switch == self.switch_slot && port.allow_ddm_traffic
            })
            .map(|port| format!("tfport{}_0/ll", port.port))
            .collect();

        // Unconditional: the endpoint is idempotent, and reapplying every pass
        // means we recover on our own if ddmd restarts and loses its FSMs.
        let request = ExternalPeers { address_objects };
        match self.client.set_external_peers(&request).await {
            Ok(_) => {
                info!(
                    log, "set external peers for DDM interfaces";
                    "external_peers" => ?request,
                );
                DdmdReconcilerStatus::Reconciled {
                    external_peers_address_objects: request.address_objects,
                }
            }
            Err(err) => DdmdReconcilerStatus::Failed(format!(
                "failed to apply DDM interfaces to ddmd: {}",
                InlineErrorChain::new(&err)
            )),
        }
    }
}

#[cfg(test)]
mod tests;
