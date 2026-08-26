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
use ddm_admin_client::types::ApplyRequest;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct DdmdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
    base_interfaces: BTreeSet<String>,
}

impl Reconciler for DdmdReconciler {
    type Status = DdmdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "DdmdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        base_ddm_interfaces: BTreeSet<String>,
        parent_log: &Logger,
    ) -> Self {
        Self {
            client: mode.ddmd_client(parent_log),
            switch_slot,
            base_interfaces: base_ddm_interfaces,
        }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let interfaces = desired_interfaces(
            &self.base_interfaces,
            &system_networking_config.rack_network_config,
            self.switch_slot,
        );

        // Unconditional: the endpoint is idempotent, and reapplying every pass
        // means we recover on our own if ddmd restarts and loses its FSMs.
        let request = ApplyRequest {
            ddm_interfaces: interfaces.iter().cloned().collect(),
        };
        match self.client.ddm_apply(&request).await {
            Ok(_) => {
                info!(
                    log, "applied DDM interfaces";
                    "interfaces" => ?interfaces,
                );
                DdmdReconcilerStatus::Reconciled { interfaces }
            }
            Err(err) => DdmdReconcilerStatus::Failed(format!(
                "failed to apply DDM interfaces to ddmd: {}",
                InlineErrorChain::new(&err)
            )),
        }
    }
}

/// The full set of interfaces ddmd should be running DDM on.
///
/// ddmd's apply endpoint is declarative over every FSM it has running, so this
/// must be a superset of `base`: omitting those would tear down the rear-port
/// sessions SMF started at switch zone boot and collapse underlay routing.
///
/// Names are returned bare (no `/ll` addrobj suffix); ddmd appends that itself.
fn desired_interfaces(
    base: &BTreeSet<String>,
    rack_network_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> BTreeSet<String> {
    let mut interfaces = base.clone();
    for port in rack_network_config
        .ports
        .iter()
        .filter(|port| port.switch == our_switch_slot && port.allow_ddm_traffic)
    {
        interfaces.insert(format!("tfport{}_0", port.port));
    }
    interfaces
}

#[cfg(test)]
mod tests;
