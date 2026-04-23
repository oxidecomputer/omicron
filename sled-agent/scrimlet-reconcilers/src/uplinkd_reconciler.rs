// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for configuration of `uplinkd` within a scrimlet's switch zone.
//!
//! Unlike most reconcilers in this crate, `uplinkd`'s configuration is managed
//! via SMF, not a dropshot server.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct UplinkdReconcilerStatus {
    pub todo_status: (),
}

#[derive(Debug)]
pub(crate) struct UplinkdReconciler {
    _switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for UplinkdReconciler {
    type Status = UplinkdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "UplinkdReconciler";
    const RE_RECONCILE_INTERVAL: std::time::Duration = Duration::from_secs(30);

    fn new(
        _switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot: ThisSledSwitchSlot,
        _parent_log: &Logger,
    ) -> Self {
        Self { _switch_slot: switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        _system_networking_config: &SystemNetworkingConfig,
        _log: &Logger,
    ) -> Self::Status {
        UplinkdReconcilerStatus { todo_status: () }
    }
}
