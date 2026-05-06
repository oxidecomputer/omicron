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
use slog::info;
use std::time::Duration;

mod nat;

pub use nat::DpdNatReconcilerStatus;
pub use nat::DpdNatReconcilerStatusNatEntry;
pub use nat::DpdNatReconcilerStatusNatEntryFailure;

#[derive(Debug, Clone)]
pub struct DpdReconcilerStatus {
    /// Result of reconciling service zone NAT entries
    pub nat_status: DpdNatReconcilerStatus,
}

impl slog::KV for DpdReconcilerStatus {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let Self { nat_status } = self;
        nat_status.serialize(record, serializer)
    }
}

#[derive(Debug)]
pub(crate) struct DpdReconciler {
    client: Client,
    _switch_slot: ThisSledSwitchSlot,
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
        Self { client: mode.dpd_client(parent_log), _switch_slot: switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let nat_status = if let Some(nat_entries) = system_networking_config
            .blueprint_external_networking_config
            .as_ref()
            .map(|config| &config.service_zone_nat_entries)
        {
            nat::reconcile(&self.client, nat_entries, log).await
        } else {
            DpdNatReconcilerStatus::NoNatEntriesConfig
        };

        info!(
            log, "dpd reconciliation completed";
            &nat_status,
        );

        DpdReconcilerStatus { nat_status }
    }
}
