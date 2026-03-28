// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use dpd_client::Client;
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::address::DENDRITE_PORT;
use sled_agent_types::early_networking::RackNetworkConfig;
use slog::Logger;
use slog::info;
use std::time::Duration;

pub(crate) struct DpdReconciler {
    _client: Client,
}

impl Reconciler for DpdReconciler {
    type Status = ();

    const LOGGER_COMPONENT_NAME: &'static str = "DpdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        _switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        let baseurl =
            format!("http://[{switch_zone_underlay_ip}]:{DENDRITE_PORT}");
        let client = Client::new(
            &baseurl,
            dpd_client::ClientState {
                tag: OMICRON_DPD_TAG.to_owned(),
                log: parent_log
                    .new(slog::o!("component" => "DpdReconcilerClient")),
            },
        );
        Self { _client: client }
    }

    async fn do_reconciliation(
        &mut self,
        _rack_network_config: &RackNetworkConfig,
        log: &Logger,
    ) -> Self::Status {
        info!(log, "TODO: implement dpd reconciler");
    }
}
