// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The bootstrap agent's view into hardware

use crate::config::{Config as SledConfig, SledMode as SledModeConfig};
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use illumos_utils::dladm::{Etherstub, EtherstubVnic};
use sled_hardware::{DendriteAsic, HardwareManager, SledMode};
use slog::Logger;
use std::net::Ipv6Addr;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Error monitoring hardware: {0}")]
    Hardware(String),

    #[error("Error from service manager: {0}")]
    ServiceManager(#[from] crate::services::Error),
}

// Represents the bootstrap agent's view into hardware.
//
// The bootstrap agent is responsible for launching the switch zone, which it
// can do once it has detected that the corresponding driver has loaded.
struct HardwareMonitorWorker {
    log: Logger,
    exit_rx: oneshot::Receiver<()>,
    hardware: HardwareManager,
    services: ServiceManager,
    storage: StorageManager,
}

impl HardwareMonitorWorker {
    fn new(
        log: Logger,
        exit_rx: oneshot::Receiver<()>,
        hardware: HardwareManager,
        services: ServiceManager,
        storage: StorageManager,
    ) -> Self {
        Self { log, exit_rx, hardware, services, storage }
    }

    async fn run(
        mut self,
    ) -> Result<(HardwareManager, ServiceManager, StorageManager), Error> {
        // Start monitoring the hardware for changes
        let mut hardware_updates = self.hardware.monitor();

        // Scan the system manually for events we have have missed
        // before we started monitoring.
        self.full_hardware_scan().await;

        // Rely on monitoring for tracking all future updates.
        loop {
            use tokio::sync::broadcast::error::RecvError;

            tokio::select! {
                _ = &mut self.exit_rx => return Ok((self.hardware, self.services, self.storage)),
                update = hardware_updates.recv() => {
                    match update {
                        Ok(update) => match update {
                            sled_hardware::HardwareUpdate::TofinoLoaded => {
                                let switch_zone_ip = None;
                                if let Err(e) = self.services.activate_switch(switch_zone_ip).await {
                                    warn!(self.log, "Failed to activate switch: {e}");
                                }
                            }
                            sled_hardware::HardwareUpdate::TofinoUnloaded => {
                                if let Err(e) = self.services.deactivate_switch().await {
                                    warn!(self.log, "Failed to deactivate switch: {e}");
                                }
                            }
                            sled_hardware::HardwareUpdate::DiskAdded(disk) => {
                                self.storage.upsert_disk(disk).await;
                            }
                            sled_hardware::HardwareUpdate::DiskRemoved(disk) => {
                                self.storage.delete_disk(disk).await;
                            }
                            _ => continue,
                        },
                        Err(RecvError::Lagged(count)) => {
                            warn!(self.log, "Hardware monitor missed {count} messages");
                            self.full_hardware_scan().await;
                        }
                        Err(RecvError::Closed) => {
                            warn!(self.log, "Hardware monitor receiver closed; exiting");
                            return Err(Error::Hardware("Hardware monitor closed".to_string()));
                        }
                    }
                }
            }
        }
    }

    // Observe the current hardware state manually.
    //
    // We use this when we're monitoring hardware for the first
    // time, and if we miss notifications.
    async fn full_hardware_scan(&self) {
        info!(self.log, "Performing full hardware scan");
        if self.hardware.is_scrimlet_driver_loaded() {
            let switch_zone_ip = None;
            if let Err(e) = self.services.activate_switch(switch_zone_ip).await
            {
                warn!(self.log, "Failed to activate switch: {e}");
            }
        } else {
            if let Err(e) = self.services.deactivate_switch().await {
                warn!(self.log, "Failed to deactivate switch: {e}");
            }
        }
    }
}

// The client-side of a connection to a HardwareMonitorWorker which runs in a
// tokio task.
//
// Once created, the bootstrap agent monitors hardware to initialize (or
// terminate) the switch zone when requested.
pub(crate) struct HardwareMonitor {
    exit_tx: oneshot::Sender<()>,
    handle: JoinHandle<
        Result<(HardwareManager, ServiceManager, StorageManager), Error>,
    >,
}

impl HardwareMonitor {
    // Spawns a new task which monitors for hardware and launches the switch
    // zone if the necessary Tofino drivers are detected.
    pub async fn new(
        log: &Logger,
        sled_config: &SledConfig,
        global_zone_bootstrap_link_local_address: Ipv6Addr,
        underlay_etherstub: Etherstub,
        underlay_etherstub_vnic: EtherstubVnic,
        bootstrap_etherstub: Etherstub,
        switch_zone_bootstrap_address: Ipv6Addr,
    ) -> Result<Self, Error> {
        // Combine the `sled_mode` config with the build-time
        // switch type to determine the actual sled mode.
        let sled_mode = match sled_config.sled_mode {
            SledModeConfig::Auto => {
                if !cfg!(feature = "switch-asic") {
                    return Err(Error::Hardware(
                        "sled-agent was not packaged with `switch-asic`"
                            .to_string(),
                    ));
                }
                SledMode::Auto
            }
            SledModeConfig::Gimlet => SledMode::Gimlet,
            SledModeConfig::Scrimlet => {
                let asic = if cfg!(feature = "switch-asic") {
                    DendriteAsic::TofinoAsic
                } else if cfg!(feature = "switch-stub") {
                    DendriteAsic::TofinoStub
                } else if cfg!(feature = "switch-softnpu") {
                    DendriteAsic::SoftNpu
                } else {
                    return Err(Error::Hardware(
                        "sled-agent configured to run on scrimlet but wasn't \
                        packaged with switch zone"
                            .to_string(),
                    ));
                };
                SledMode::Scrimlet { asic }
            }
        };
        info!(log, "Starting hardware monitor"; "sled_mode" => ?sled_mode);
        let hardware = HardwareManager::new(log, sled_mode)
            .map_err(|e| Error::Hardware(e))?;

        // TODO: The coupling between the storage and service manager is growing
        // pretty tight; we should consider merging them together.
        let storage_manager =
            StorageManager::new(&log, underlay_etherstub.clone()).await;

        let service_manager = ServiceManager::new(
            log.clone(),
            global_zone_bootstrap_link_local_address,
            underlay_etherstub.clone(),
            underlay_etherstub_vnic.clone(),
            bootstrap_etherstub,
            sled_mode,
            sled_config.skip_timesync,
            sled_config.sidecar_revision.clone(),
            switch_zone_bootstrap_address,
            sled_config.switch_zone_maghemite_links.clone(),
            storage_manager.clone(),
        )
        .await?;

        Ok(Self::start(log, hardware, service_manager, storage_manager))
    }

    // Starts a hardware monitoring task
    pub fn start(
        log: &Logger,
        hardware: HardwareManager,
        services: ServiceManager,
        storage: StorageManager,
    ) -> Self {
        let (exit_tx, exit_rx) = oneshot::channel();
        let worker = HardwareMonitorWorker::new(
            log.clone(),
            exit_rx,
            hardware,
            services,
            storage,
        );
        let handle = tokio::spawn(async move { worker.run().await });

        Self { exit_tx, handle }
    }

    // Stops the task from executing
    pub async fn stop(
        self,
    ) -> Result<(HardwareManager, ServiceManager, StorageManager), Error> {
        let _ = self.exit_tx.send(());
        self.handle.await.expect("Hardware monitor panicked")
    }
}
