// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `mgd` within a scrimlet's switch
//! zone.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use mg_admin_client::Client;
use omicron_common::address::MGD_PORT;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use std::time::Duration;

mod static_route_reconciler;

pub use static_route_reconciler::MgdStaticRouteReconcilerStatus;
pub use static_route_reconciler::MgdStaticRouteBulkOperationResult;

#[derive(Debug, Clone)]
pub struct MgdReconcilerStatus {
    pub static_routes_status: MgdStaticRouteReconcilerStatus,
}

pub(crate) struct MgdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for MgdReconciler {
    type Status = MgdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "MgdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        // Build a custom reqwest client, primarly to set a lower
        // `pool_idle_timeout`. Our `RE_RECONCILE_INTERVAL` interval of 30
        // seconds happens to coincide exactly with dropshot's default
        // connection timeout of 30 seconds. In early testing, this caused us to
        // hit <https://github.com/hyperium/hyper/issues/2136> surprisingly
        // frequently: mgd would close a connection right as we were trying to
        // use it, resulting in spurious "connection closed before message
        // completed" or "connection reset by peer" errors.
        //
        // We choose a much lower `pool_idle_timeout`: 10 seconds is long enough
        // to reuse a connection for all the requests made during one
        // reconciliation pass, but is short enough we should discard it before
        // the server wants to time us out.
        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest parameters are valid");

        let baseurl = format!("http://[{switch_zone_underlay_ip}]:{MGD_PORT}");

        let client = Client::new_with_client(
            &baseurl,
            reqwest_client,
            parent_log.new(slog::o!("component" => "MgdReconcilerClient")),
        );

        Self { client, switch_slot }
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

        info!(
            log, "mgd reconciliation completed";
            &static_routes_status,
        );
        MgdReconcilerStatus { static_routes_status }
    }
}
