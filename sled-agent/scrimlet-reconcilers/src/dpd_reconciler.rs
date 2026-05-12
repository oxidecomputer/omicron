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

#[derive(Debug, Clone)]
pub struct DpdReconcilerStatus {
    pub todo_status: (),
}

impl slog::KV for DpdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str("dpd-reconciler".into(), "not yet implemented")
    }
}

#[derive(Debug)]
pub(crate) struct DpdReconciler {
    _client: Client,
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
        Self { _client: mode.dpd_client(parent_log), _switch_slot: switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        _system_networking_config: &SystemNetworkingConfig,
        _log: &Logger,
    ) -> Self::Status {
        DpdReconcilerStatus { todo_status: () }
    }
}
