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

#[derive(Debug, Clone)]
pub struct MgdReconcilerStatus {
    pub todo_status: (),
}

impl slog::KV for MgdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str("mgd-reconciler".into(), "not yet implemented")
    }
}

#[derive(Debug)]
pub(crate) struct MgdReconciler {
    _client: Client,
}

impl Reconciler for MgdReconciler {
    type Status = MgdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "MgdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(mode: ScrimletReconcilersMode, parent_log: &Logger) -> Self {
        Self { _client: mode.mgd_client(parent_log) }
    }

    async fn do_reconciliation(
        &mut self,
        _system_networking_config: &SystemNetworkingConfig,
        _switch_slot: ThisSledSwitchSlot,
        _log: &Logger,
    ) -> Self::Status {
        MgdReconcilerStatus { todo_status: () }
    }
}
