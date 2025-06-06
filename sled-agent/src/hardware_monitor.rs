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
use tokio::sync::{broadcast, oneshot};

// A thin wrapper around the the [`ServiceManager`] that caches the state
// whether or not the tofino is loaded if the [`ServiceManager`] doesn't exist
// yet.
enum TofinoManager {
    Ready(ServiceManager),
    NotReady { tofino_loaded: bool },
}

impl TofinoManager {
    pub fn new() -> TofinoManager {
        TofinoManager::NotReady { tofino_loaded: false }
    }

    // Must only be called once on the transition from `NotReady` to `Ready`.
    // Panics otherwise.
    //
    // Returns whether the tofino was loaded or not
    pub fn become_ready(&mut self, service_manager: ServiceManager) -> bool {
        let tofino_loaded = match self {
            Self::Ready(_) => panic!("ServiceManager is already available"),
            Self::NotReady { tofino_loaded } => *tofino_loaded,
        };
        *self = Self::Ready(service_manager);
        tofino_loaded
    }

    pub fn is_ready(&self) -> bool {
        match self {
            TofinoManager::Ready(_) => true,
            _ => false,
        }
    }
}

// A monitor for hardware events
pub struct HardwareMonitor {
    log: Logger,

    baseboard: Baseboard,

    // Receive a onetime notification that the SledAgent has started
    sled_agent_started_rx: oneshot::Receiver<SledAgent>,

    // Receive a onetime notification that the ServiceManager is ready
    service_manager_ready_rx: oneshot::Receiver<ServiceManager>,

    // Receive messages from the [`HardwareManager`]
    hardware_rx: broadcast::Receiver<HardwareUpdate>,

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

    // The [`ServiceManager`] is instantiated after we start the [`HardwareMonitor`]
    // task. However, it is only used to load and unload the switch zone when thes
    // state of the tofino changes. We keep track of the tofino state so that we
    // can properly load the tofino when the [`ServiceManager`] becomes available
    // available.
    tofino_manager: TofinoManager,
}

impl HardwareMonitor {
    pub fn new(
        log: &Logger,
        hardware_manager: &HardwareManager,
        raw_disks_tx: RawDisksSender,
    ) -> (
        HardwareMonitor,
        oneshot::Sender<SledAgent>,
        oneshot::Sender<ServiceManager>,
    ) {
        let (sled_agent_started_tx, sled_agent_started_rx) = oneshot::channel();
        let (service_manager_ready_tx, service_manager_ready_rx) =
            oneshot::channel();
        let baseboard = hardware_manager.baseboard();
        let hardware_rx = hardware_manager.monitor();
        let log = log.new(o!("component" => "HardwareMonitor"));
        let tofino_manager = TofinoManager::new();
        (
            HardwareMonitor {
                log,
                baseboard,
                sled_agent_started_rx,
                service_manager_ready_rx,
                hardware_rx,
                hardware_manager: hardware_manager.clone(),
                raw_disks_tx,
                sled_agent: None,
                tofino_manager,
            },
            sled_agent_started_tx,
            service_manager_ready_tx,
        )
    }

    /// Run the main receive loop of the `HardwareMonitor`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(&mut self) {
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
                    if !self.tofino_manager.is_ready() =>
                {
                    let tofino_loaded =
                        self.tofino_manager.become_ready(service_manager);
                    if tofino_loaded {
                        self.activate_switch().await;
                    }
                }
                update = self.hardware_rx.recv() => {
                    info!(
                        self.log,
                        "Received hardware update message";
                        "update" => ?update,
                    );
                    self.handle_hardware_update(update).await;
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
                HardwareUpdate::TofinoLoaded => self.activate_switch().await,
                HardwareUpdate::TofinoUnloaded => {
                    self.deactivate_switch().await
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

    async fn activate_switch(&mut self) {
        match &mut self.tofino_manager {
            TofinoManager::Ready(service_manager) => {
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
            }
            TofinoManager::NotReady { tofino_loaded } => {
                *tofino_loaded = true;
            }
        }
    }

    async fn deactivate_switch(&mut self) {
        match &mut self.tofino_manager {
            TofinoManager::Ready(service_manager) => {
                if let Err(e) = service_manager.deactivate_switch().await {
                    warn!(self.log, "Failed to deactivate switch: {e}");
                }
            }
            TofinoManager::NotReady { tofino_loaded } => {
                *tofino_loaded = false;
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

        if self.hardware_manager.is_scrimlet_driver_loaded() {
            self.activate_switch().await;
        } else {
            self.deactivate_switch().await;
        }

        self.raw_disks_tx.set_raw_disks(
            self.hardware_manager.disks().into_values().map(RawDisk::from),
            &self.log,
        );
    }
}
