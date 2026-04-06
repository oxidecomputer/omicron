// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `dpd` within a scrimlet's switch
//! zone.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::dpd_reconciler::port_reconciler::PortReconciler;
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
mod port_reconciler;

pub use nat::DpdNatReconcilerStatus;
pub use nat::DpdNatReconcilerStatusNatEntry;
pub use nat::DpdNatReconcilerStatusNatEntryFailure;
pub use port_reconciler::DpdPortOperationFailure;
pub use port_reconciler::DpdPortReconcilerStatus;

#[derive(Debug, Clone)]
pub struct DpdReconcilerStatus {
    /// Result of reconciling port settings
    pub port_settings_status: DpdPortReconcilerStatus,
    /// Result of reconciling service zone NAT entries
    pub nat_status: DpdNatReconcilerStatus,
}

pub(crate) struct DpdReconciler {
    client: Client,
    switch_slot: ThisSledSwitchSlot,
    port_reconciler: PortReconciler,
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
        // Build a custom reqwest client, primarly to set a lower
        // `pool_idle_timeout`. Our `RE_RECONCILE_INTERVAL` interval of 30
        // seconds happens to coincide exactly with dropshot's default
        // connection timeout of 30 seconds. In early testing, this caused us to
        // hit <https://github.com/hyperium/hyper/issues/2136> surprisingly
        // frequently: dpd would close a connection right as we were trying to
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

        let baseurl =
            format!("http://[{switch_zone_underlay_ip}]:{DENDRITE_PORT}");

        let client = Client::new_with_client(
            &baseurl,
            reqwest_client,
            dpd_client::ClientState {
                tag: OMICRON_DPD_TAG.to_owned(),
                log: parent_log
                    .new(slog::o!("component" => "DpdReconcilerClient")),
            },
        );

        let port_reconciler = PortReconciler::default();

        Self { client, switch_slot, port_reconciler }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let port_settings_status = self
            .port_reconciler
            .reconcile(
                &self.client,
                &system_networking_config.rack_network_config,
                self.switch_slot,
                log,
            )
            .await;

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
            &port_settings_status,
            &nat_status,
        );

        DpdReconcilerStatus { port_settings_status, nat_status }
    }
}
