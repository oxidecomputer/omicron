// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `mgd` within a scrimlet's switch
//! zone.

use crate::ScrimletReconcilersMode;
use crate::handle::BgpSocketConfig;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdReconcilerStatus;
use mg_admin_client::Client;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use std::time::Duration;

mod bfd_reconciler;
mod bgp_reconciler;
mod static_route_reconciler;

#[derive(Debug)]
pub(crate) struct MgdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
    bgp_socket_config: BgpSocketConfig,
}

impl Reconciler for MgdReconciler {
    type Status = MgdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "MgdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        let bgp_socket_config = match mode {
            ScrimletReconcilersMode::SwitchZone(_) => {
                BgpSocketConfig::default()
            }
            ScrimletReconcilersMode::Test { bgp_socket_config, .. } => {
                bgp_socket_config
            }
        };
        Self {
            client: mode.mgd_client(parent_log),
            switch_slot,
            bgp_socket_config,
        }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let static_routes_status = static_route_reconciler::reconcile(
            &self.client,
            &system_networking_config.rack_network_config,
            self.switch_slot,
            log,
        )
        .await;

        let bgp_status = bgp_reconciler::reconcile(
            &self.client,
            &system_networking_config.rack_network_config,
            self.switch_slot,
            self.bgp_socket_config,
            log,
        )
        .await;

        let bfd_status = bfd_reconciler::reconcile(
            &self.client,
            &system_networking_config.rack_network_config,
            self.switch_slot,
            log,
        )
        .await;

        MgdReconcilerStatus { static_routes_status, bgp_status, bfd_status }
    }
}
