// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `dpd` within a scrimlet's switch
//! zone.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use dpd_client::Client;
use omicron_common::OMICRON_DPD_TAG;
use omicron_common::address::DENDRITE_PORT;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use std::time::Duration;

mod nat;

pub use nat::DpdNatReconcilerStatus;

#[derive(Debug, Clone)]
pub struct DpdReconcilerStatus {
    /// Result of reconciling service zone NAT entries
    pub nat_status: DpdNatReconcilerStatus,
}

pub(crate) struct DpdReconciler {
    client: Client,
    _switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for DpdReconciler {
    type Status = DpdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "DpdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot: ThisSledSwitchSlot,
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
        Self { client, _switch_slot: switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        // TODO implement port reconciliation

        let nat_status = if let Some(nat_entries) =
            system_networking_config.service_zone_nat_entries.as_ref()
        {
            nat::reconcile(&self.client, nat_entries, log).await
        } else {
            DpdNatReconcilerStatus::NoNatEntriesConfig
        };

        info!(
            log,
            "dpd reconciliation completed";
            &nat_status,
        );

        DpdReconcilerStatus { nat_status }
    }
}
