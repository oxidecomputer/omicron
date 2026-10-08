// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for informing ddmd in the scrimlet's switch zone
//! about which external front ports should run DDM.
//!
//! Rear ports always carry DDM traffic. This reconciler only syncs front port
//! information based on user configuration.

use crate::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ddmd::DdmdReconcilerStatus;
use ddm_admin_client::Client;
use ddm_api_types::external_peers::ExternalPeers;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::addrobj::ParseError;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
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
        let mut address_objects = BTreeSet::new();
        for port in
            system_networking_config.rack_network_config.ports.iter().filter(
                |port| {
                    port.switch == self.switch_slot && port.allow_ddm_traffic
                },
            )
        {
            match ddmd_specific_addrobj(&port.port) {
                Ok(addrobj) => {
                    address_objects.insert(addrobj);
                }

                Err(err) => {
                    error!(
                        log,
                        "Invalid port name: {}. Ports cannot have slashes.",
                         port.port;
                        "err" => %err
                    );
                }
            }
        }

        // Set which external ports should carry DDM traffic unconditionally.
        // The endpoint is idempotent, and reapplying every pass allows recovery
        // if ddmd restarts and loses its FSMs.
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

/// Create an illumos addrobj as a string to identify the given port.
///
/// Put the port in the addrobj format that dendrite expects as a string. This
/// string passes through maghemite into dendrite without being interpreted by
/// maghemite. Since the string is user specified and maghemite should not know
/// about the format of addrobjs, but should just use them directly, it cannot
/// create this string on its own.
///
/// A better solution would be to pass the `AddrObject` down directly and pass
/// it through to dendrite. This requires changes to dendrite and maghemite
/// APIs.
fn ddmd_specific_addrobj(port: &str) -> Result<String, ParseError> {
    AddrObject::link_local(&format!("tfport{port}_0")).map(|a| a.to_string())
}

#[cfg(test)]
mod tests;
