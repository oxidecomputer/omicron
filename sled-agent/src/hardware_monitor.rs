// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for hardware events from the
//! [`sled_hardware::HardwareManager`] and dispatches them to other parts
//! of the bootstrap agent and sled-agent code.

use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use sled_hardware::{Baseboard, HardwareManager, HardwareUpdate};
use sled_storage::disk::RawDisk;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::fmt::Debug;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

const QUEUE_SIZE: usize = 10;

pub enum HardwareMonitorMsg {
    SledAgentStarted(SledAgent),
    ServiceManagerCreated(ServiceManager),
}

impl Debug for HardwareMonitorMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HardwareMonitorMsg::SledAgentStarted(_) => {
                f.debug_struct("SledAgentStarted").finish()
            }
            HardwareMonitorMsg::ServiceManagerCreated(_) => {
                f.debug_struct("ServiceManagerCreated").finish()
            }
        }
    }
}

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
}

#[derive(Clone)]
pub struct HardwareMonitorHandle {
    tx: mpsc::Sender<HardwareMonitorMsg>,
}

impl HardwareMonitorHandle {
    pub async fn service_manager_ready(&self, service_manager: ServiceManager) {
        self.tx
            .send(HardwareMonitorMsg::ServiceManagerCreated(service_manager))
            .await
            .unwrap();
    }

    pub async fn sled_agent_started(&self, sled_agent: SledAgent) {
        self.tx
            .send(HardwareMonitorMsg::SledAgentStarted(sled_agent))
            .await
            .unwrap();
    }
}

pub struct HardwareMonitor {
    log: Logger,

    baseboard: Baseboard,

    // Receive messages from the [`HardwareMonitorHandle`]
    handle_rx: mpsc::Receiver<HardwareMonitorMsg>,

    // Receive messages from the [`HardwareManager`]
    hardware_rx: broadcast::Receiver<HardwareUpdate>,

    // A reference to the hardware manager
    hardware_manager: HardwareManager,

    // A handle to [`sled_hardware::manager::StorageManger`]
    storage_manager: StorageHandle,

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
        storage_manager: &StorageHandle,
    ) -> (HardwareMonitor, HardwareMonitorHandle) {
        let baseboard = hardware_manager.baseboard();
        let (handle_tx, handle_rx) = mpsc::channel(QUEUE_SIZE);
        let hardware_rx = hardware_manager.monitor();
        let log = log.new(o!("component" => "HardwareMonitor"));
        let tofino_manager = TofinoManager::new();
        (
            HardwareMonitor {
                log,
                baseboard,
                handle_rx,
                hardware_rx,
                hardware_manager: hardware_manager.clone(),
                storage_manager: storage_manager.clone(),
                sled_agent: None,
                tofino_manager,
            },
            HardwareMonitorHandle { tx: handle_tx },
        )
    }

    /// Run the main receive loop of the `StorageManager`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(&mut self) {
        // Check the latest hardware snapshot; we could have missed events
        // between the creation of the hardware manager and our subscription of
        // its monitor.
        self.check_latest_hardware_snapshot().await;

        loop {
            tokio::select! {
                Some(msg) = self.handle_rx.recv() => {
                    info!(
                        self.log,
                        "Received hardware monitor message";
                        "msg" => ?msg
                    );
                    self.handle_monitor_msg(msg).await;
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

    // Handle a message from the [`HardwareMonitorHandle`]
    async fn handle_monitor_msg(&mut self, msg: HardwareMonitorMsg) {
        match msg {
            HardwareMonitorMsg::SledAgentStarted(sled_agent) => {
                self.sled_agent = Some(sled_agent);
                self.check_latest_hardware_snapshot().await;
            }
            HardwareMonitorMsg::ServiceManagerCreated(service_manager) => {
                let tofino_loaded =
                    self.tofino_manager.become_ready(service_manager);
                if tofino_loaded {
                    self.activate_switch().await;
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
                        sled_agent.notify_nexus_about_self(&self.log);
                    }
                }
                HardwareUpdate::DiskAdded(disk) => {
                    self.storage_manager.upsert_disk(disk.into()).await;
                }
                HardwareUpdate::DiskRemoved(disk) => {
                    self.storage_manager.delete_disk(disk.into()).await;
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
                    warn!(self.log, "Failed to activate switch: {e}");
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
        let underlay_network = self.sled_agent.as_ref().map(|sled_agent| {
            sled_agent.notify_nexus_about_self(&self.log);
            sled_agent.switch_zone_underlay_info()
        });
        info!(
            self.log, "Checking current full hardware snapshot";
            "underlay_network_info" => ?underlay_network,
        );
        if self.hardware_manager.is_scrimlet_driver_loaded() {
            self.activate_switch().await;
        } else {
            self.deactivate_switch().await;
        }

        self.storage_manager
            .ensure_using_exactly_these_disks(
                self.hardware_manager.disks().into_iter().map(RawDisk::from),
            )
            .await;
    }
}
