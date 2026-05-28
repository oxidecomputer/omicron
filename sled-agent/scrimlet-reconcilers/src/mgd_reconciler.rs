// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `mgd` within a scrimlet's switch
//! zone.

use crate::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use mg_admin_client::Client;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use std::time::Duration;

mod bgp_reconciler;
mod static_route_reconciler;

pub use bgp_reconciler::MgdBgpReconcilerStatus;
pub use bgp_reconciler::MgdBgpReconcilerStatusOpCount;
pub use static_route_reconciler::MgdStaticRouteReconcilerStatus;

#[derive(Debug, Clone)]
pub struct MgdReconcilerStatus {
    pub bgp_status: MgdBgpReconcilerStatus,
    pub static_routes_status: MgdStaticRouteReconcilerStatus,
}

impl slog::KV for MgdReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { bgp_status, static_routes_status } = self;
        bgp_status.serialize(record, serializer)?;
        static_routes_status.serialize(record, serializer)?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct MgdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
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
        Self { client: mode.mgd_client(parent_log), switch_slot }
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
            log,
        )
        .await;

        MgdReconcilerStatus { bgp_status, static_routes_status }
    }
}
