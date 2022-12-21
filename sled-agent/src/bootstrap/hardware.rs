// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The bootstrap agent's view into hardware

use crate::config::Config as SledConfig;
use crate::services::ServiceManager;
use illumos_utils::dladm::{Etherstub, EtherstubVnic};
use sled_hardware::HardwareManager;
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
}

impl HardwareMonitorWorker {
    fn new(
        log: Logger,
        exit_rx: oneshot::Receiver<()>,
        hardware: HardwareManager,
        services: ServiceManager,
    ) -> Self {
        Self { log, exit_rx, hardware, services }
    }

    async fn run(mut self) -> Result<(HardwareManager, ServiceManager), Error> {
        // Start monitoring the hardware for changes
        let mut hardware_updates = self.hardware.monitor();

        // Scan the system manually for events we have have missed
        // before we started monitoring.
        self.full_hardware_scan().await;

        // Rely on monitoring for tracking all future updates.
        loop {
            use tokio::sync::broadcast::error::RecvError;

            tokio::select! {
                _ = &mut self.exit_rx => return Ok((self.hardware, self.services)),
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
    handle: JoinHandle<Result<(HardwareManager, ServiceManager), Error>>,
}

impl HardwareMonitor {
    // Spawns a new task which monitors for hardware and launches the switch
    // zone if the necessary Tofino drivers are detected.
    pub async fn new(
        log: &Logger,
        sled_config: &SledConfig,
        underlay_etherstub: Etherstub,
        underlay_etherstub_vnic: EtherstubVnic,
        bootstrap_etherstub: Etherstub,
        switch_zone_bootstrap_address: Ipv6Addr,
    ) -> Result<Self, Error> {
        let hardware =
            HardwareManager::new(log.clone(), sled_config.scrimlet_override)
                .map_err(|e| Error::Hardware(e))?;

        let service_manager = ServiceManager::new(
            log.clone(),
            underlay_etherstub.clone(),
            underlay_etherstub_vnic.clone(),
            bootstrap_etherstub,
            sled_config.scrimlet_override,
            sled_config.sidecar_revision.clone(),
            switch_zone_bootstrap_address,
        )
        .await?;

        Ok(Self::start(log, hardware, service_manager))
    }

    // Starts a hardware monitoring task
    pub fn start(
        log: &Logger,
        hardware: HardwareManager,
        services: ServiceManager,
    ) -> Self {
        let (exit_tx, exit_rx) = oneshot::channel();
        let worker = HardwareMonitorWorker::new(
            log.clone(),
            exit_rx,
            hardware,
            services,
        );
        let handle = tokio::spawn(async move { worker.run().await });

        Self { exit_tx, handle }
    }

    // Stops the task from executing
    pub async fn stop(
        self,
    ) -> Result<(HardwareManager, ServiceManager), Error> {
        let _ = self.exit_tx.send(());
        self.handle.await.expect("Hardware monitor panicked")
    }
}
