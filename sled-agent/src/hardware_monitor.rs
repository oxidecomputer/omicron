// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for hardware events from the
//! [`sled_hardware::HardwareManager`] and dispatches them to other parts
//! of the bootstrap agent and sled-agent code.

use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use sled_agent_config_reconciler::RawDisksSender;
use sled_hardware::{HardwareManager, HardwareUpdate};
use sled_hardware_types::Baseboard;
use sled_storage::disk::RawDisk;
use slog::Logger;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, oneshot, watch};

/// Policy allowing an operator (via `omdb`) to control whether the switch zone
/// is started or stopped.
///
/// This is an _extremely_ dicey operation in general; a stopped switch zone
/// leaves the rack inoperable! We are only adding this as a workaround and test
/// tool for handling sidecar resets; see
/// https://github.com/oxidecomputer/omicron/issues/8480 for background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorSwitchZonePolicy {
    /// Start the switch zone if a switch is present.
    ///
    /// This is the default policy.
    StartIfSwitchPresent,

    /// Stop the switch zone despite a switch being present.
    StopDespiteSwitchPresence,
}

/// A handle controlling the behavior of a [`HardwareMonitor`]
#[derive(Debug, Clone)]
pub struct HardwareMonitorHandle {
    switch_zone_policy_tx: watch::Sender<OperatorSwitchZonePolicy>,
}

impl HardwareMonitorHandle {
    pub fn set_switch_zone_policy(&self, policy: OperatorSwitchZonePolicy) {
        self.switch_zone_policy_tx.send_if_modified(|p| {
            if *p != policy {
                *p = policy;
                true
            } else {
                false
            }
        });
    }
}

/// A monitor for hardware events
pub struct HardwareMonitor {
    log: Logger,

    baseboard: Baseboard,

    // Receive a onetime notification that the SledAgent has started
    sled_agent_started_rx: oneshot::Receiver<SledAgent>,

    // Receive a onetime notification that the ServiceManager is ready
    service_manager_ready_rx: oneshot::Receiver<ServiceManager>,

    // Receive messages from the [`HardwareManager`]
    hardware_rx: broadcast::Receiver<HardwareUpdate>,

    // Receive the operator's policy controlling the switch zone
    switch_zone_policy_rx: watch::Receiver<OperatorSwitchZonePolicy>,

    // A reference to the hardware manager
    hardware_manager: HardwareManager,

    // A handle to send raw disk updates to the config-reconciler system.
    raw_disks_tx: RawDisksSender,

    // A handle to the sled-agent
    //
    // This will go away once Nexus updates are polled:
    // See:
    //  * https://github.com/oxidecomputer/omicron/issues/1917
    //  * https://rfd.shared.oxide.computer/rfd/0433
    sled_agent: Option<SledAgent>,

    /// The [`ServiceManager`] is instantiated after [`HardwareMonitor`]. We
    /// need a handle to it to start and stop the switch zone when our hardware
    /// or policy changes.
    service_manager: Option<ServiceManager>,

    /// Whether or not the tofino is loaded.
    tofino_loaded: bool,
}

impl HardwareMonitor {
    pub fn spawn(
        log: &Logger,
        hardware_manager: &HardwareManager,
        raw_disks_tx: RawDisksSender,
    ) -> (
        HardwareMonitorHandle,
        oneshot::Sender<SledAgent>,
        oneshot::Sender<ServiceManager>,
    ) {
        let (sled_agent_started_tx, sled_agent_started_rx) = oneshot::channel();
        let (service_manager_ready_tx, service_manager_ready_rx) =
            oneshot::channel();
        let baseboard = hardware_manager.baseboard();
        let hardware_rx = hardware_manager.monitor();
        let log = log.new(o!("component" => "HardwareMonitor"));
        let (switch_zone_policy_tx, switch_zone_policy_rx) =
            watch::channel(OperatorSwitchZonePolicy::StartIfSwitchPresent);
        let monitor = HardwareMonitor {
            log,
            baseboard,
            sled_agent_started_rx,
            service_manager_ready_rx,
            hardware_rx,
            switch_zone_policy_rx,
            hardware_manager: hardware_manager.clone(),
            raw_disks_tx,
            sled_agent: None,
            service_manager: None,
            tofino_loaded: false,
        };
        tokio::spawn(monitor.run());
        let handle = HardwareMonitorHandle { switch_zone_policy_tx };
        (handle, sled_agent_started_tx, service_manager_ready_tx)
    }

    /// Run the main receive loop of the `HardwareMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        // Check the latest hardware snapshot; we could have missed events
        // between the creation of the hardware manager and our subscription of
        // its monitor.
        self.check_latest_hardware_snapshot().await;

        loop {
            tokio::select! {
                Ok(sled_agent) = &mut self.sled_agent_started_rx,
                    if self.sled_agent.is_none() =>
                {
                    info!(self.log, "Sled Agent Started");
                    self.sled_agent = Some(sled_agent);
                    self.check_latest_hardware_snapshot().await;
                }
                Ok(service_manager) = &mut self.service_manager_ready_rx,
                    if self.service_manager.is_none() =>
                {
                    self.service_manager = Some(service_manager);
                    self.ensure_switch_zone_activated_or_deactivated().await;
                }
                update = self.hardware_rx.recv() => {
                    info!(
                        self.log,
                        "Received hardware update message";
                        "update" => ?update,
                    );
                    self.handle_hardware_update(update).await;
                }
                Ok(()) = self.switch_zone_policy_rx.changed() => {
                    info!(self.log, "Switch zone policy changed; reevaluating");
                    self.ensure_switch_zone_activated_or_deactivated().await;
                }
            }
        }
    }

    // Handle an update from the [`HardwareMonitor`]
    async fn handle_hardware_update(
        &mut self,
        update: Result<HardwareUpdate, RecvError>,
    ) {
        match update {
            Ok(update) => match update {
                HardwareUpdate::TofinoLoaded => {
                    self.set_tofino_loaded(true).await
                }
                HardwareUpdate::TofinoUnloaded => {
                    self.set_tofino_loaded(false).await
                }
                HardwareUpdate::TofinoDeviceChange => {
                    if let Some(sled_agent) = &mut self.sled_agent {
                        sled_agent.notify_nexus_about_self(&self.log).await;
                    }
                }
                HardwareUpdate::DiskAdded(disk) => {
                    self.raw_disks_tx
                        .add_or_update_raw_disk(disk.into(), &self.log);
                }
                HardwareUpdate::DiskRemoved(disk) => {
                    self.raw_disks_tx
                        .remove_raw_disk(disk.identity(), &self.log);
                }
                HardwareUpdate::DiskUpdated(disk) => {
                    self.raw_disks_tx
                        .add_or_update_raw_disk(disk.into(), &self.log);
                }
            },
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!(self.log, "Hardware monitor missed {count} messages");
                self.check_latest_hardware_snapshot().await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                // The `HardwareManager` monitoring task is an infinite loop -
                // the only way for us to get `Closed` here is if it panicked,
                // so we will propagate such a panic.
                panic!("Hardware manager monitor task panicked");
            }
        }
    }

    async fn set_tofino_loaded(&mut self, tofino_loaded: bool) {
        self.tofino_loaded = tofino_loaded;
        self.ensure_switch_zone_activated_or_deactivated().await;
    }

    async fn ensure_switch_zone_activated_or_deactivated(&mut self) {
        // If we don't have the service manager yet, we can't do anything.
        let Some(service_manager) = &self.service_manager else {
            return;
        };

        // Decide whether to activate or deactivate based on the combination of
        // `tofino_loaded` and the operator policy.
        let tofino_loaded = self.tofino_loaded;
        let policy = *self.switch_zone_policy_rx.borrow_and_update();
        let should_activate = match (tofino_loaded, policy) {
            // We have a tofino and policy says to start the switch zone
            (true, OperatorSwitchZonePolicy::StartIfSwitchPresent) => true,
            // We have a tofino but policy says to stop the switch zone
            (true, OperatorSwitchZonePolicy::StopDespiteSwitchPresence) => {
                false
            }
            // If we don't have a tofino, stop the switch zone regardless of
            // policy.
            (false, _) => false,
        };

        if should_activate {
            if let Err(e) = service_manager
                .activate_switch(
                    self.sled_agent
                        .as_ref()
                        .map(|sa| sa.switch_zone_underlay_info()),
                    self.baseboard.clone(),
                )
                .await
            {
                error!(self.log, "Failed to activate switch"; e);
            }
        } else {
            if let Err(e) = service_manager.deactivate_switch().await {
                warn!(self.log, "Failed to deactivate switch: {e}");
            }
        }
    }

    // Observe the current hardware state manually.
    //
    // We use this when we're monitoring hardware for the first
    // time, and if we miss notifications.
    async fn check_latest_hardware_snapshot(&mut self) {
        let underlay_network = if let Some(sled_agent) = &self.sled_agent {
            sled_agent.notify_nexus_about_self(&self.log).await;
            Some(sled_agent.switch_zone_underlay_info())
        } else {
            None
        };

        info!(
            self.log, "Checking current full hardware snapshot";
            "underlay_network_info" => ?underlay_network,
            "disks" => ?self.hardware_manager.disks(),
        );

        self.set_tofino_loaded(
            self.hardware_manager.is_scrimlet_driver_loaded(),
        )
        .await;

        self.raw_disks_tx.set_raw_disks(
            self.hardware_manager.disks().into_values().map(RawDisk::from),
            &self.log,
        );
    }
}
