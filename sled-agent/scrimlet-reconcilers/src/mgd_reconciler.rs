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
use sled_agent_types::early_networking::RackNetworkConfig;
use slog::Logger;
use slog::info;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum MgdReconcilerStatus {
    /// The reconciler does nothing because it's currently a stub.
    NotYetImplemented,
}

pub(crate) struct MgdReconciler {
    _client: Client,
    _switch_slot: ThisSledSwitchSlot,
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
        let baseurl =
            format!("http://[{switch_zone_underlay_ip}]:{MGD_PORT}");
        let client = Client::new(
            &baseurl,
            parent_log.new(slog::o!("component" => "MgdReconcilerClient")),
        );
        Self { _client: client, _switch_slot: switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        _rack_network_config: &RackNetworkConfig,
        log: &Logger,
    ) -> Self::Status {
        info!(log, "TODO: implement mgd reconciler");
        MgdReconcilerStatus::NotYetImplemented
    }
}
