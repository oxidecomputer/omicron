// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler responsible for configuration of `dpd` within a scrimlet's switch
//! zone.

use crate::handle::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use dpd_client::Client;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use std::time::Duration;

mod nat;
mod port_reconciler;

pub use nat::DpdNatReconcilerStatus;
pub use nat::DpdNatReconcilerStatusNatEntry;
pub use nat::DpdNatReconcilerStatusNatEntryFailure;
pub use port_reconciler::DpdPortOperationFailure;
pub use port_reconciler::DpdPortReconcilerStatus;
use port_reconciler::PortReconciler;

#[derive(Debug, Clone)]
pub struct DpdReconcilerStatus {
    /// Result of reconciling port settings
    pub port_settings_status: DpdPortReconcilerStatus,
    /// Result of reconciling service zone NAT entries
    pub nat_status: DpdNatReconcilerStatus,
}

impl slog::KV for DpdReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { port_settings_status, nat_status } = self;
        port_settings_status.serialize(record, serializer)?;
        nat_status.serialize(record, serializer)?;
        Ok(())
    }
}

#[derive(Debug)]
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
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        parent_log: &Logger,
    ) -> Self {
        Self {
            client: mode.dpd_client(parent_log),
            switch_slot,
            port_reconciler: PortReconciler::default(),
        }
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

        let nat_status = if let Some(nat_entries) = system_networking_config
            .blueprint_external_networking_config
            .as_ref()
            .map(|config| &config.service_zone_nat_entries)
        {
            nat::reconcile(&self.client, nat_entries, log).await
        } else {
            DpdNatReconcilerStatus::NoNatEntriesConfig
        };

        DpdReconcilerStatus { port_settings_status, nat_status }
    }
}
