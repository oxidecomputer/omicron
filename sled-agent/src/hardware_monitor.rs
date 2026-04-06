// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A task that listens for changes in the [`HardwareView`] from the
//! [`sled_hardware::HardwareManager`] and dispatches them to other parts
//! of the bootstrap agent and sled-agent code.

use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use sled_agent_config_reconciler::RawDisksSender;
use sled_agent_types::debug::OperatorSwitchZonePolicy;
use sled_hardware::{HardwareManager, HardwareView};
use sled_hardware_types::Baseboard;
use sled_storage::disk::RawDisk;
use slog::Logger;
use tokio::sync::oneshot;
use tokio::sync::watch;

/// A handle controlling the behavior of a [`HardwareMonitor`]
#[derive(Debug, Clone)]
pub struct HardwareMonitorHandle {
    switch_zone_policy_tx: watch::Sender<OperatorSwitchZonePolicy>,
}

impl HardwareMonitorHandle {
    pub fn current_switch_zone_policy(&self) -> OperatorSwitchZonePolicy {
        *self.switch_zone_policy_tx.borrow()
    }

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

    // Receive current view of hardware from the [`HardwareManager`]
    hardware_view_rx: watch::Receiver<HardwareView>,

    // Receive the operator's policy controlling the switch zone
    switch_zone_policy_rx: watch::Receiver<OperatorSwitchZonePolicy>,

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

    /// Whether or not the tofino is available.  This implies that the ASIC is
    /// present, the driver has been loaded, and that we are able to use the
    /// driver to interact with the ASIC.
    is_tofino_available: bool,
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
        let hardware_view_rx = hardware_manager.subscribe();
        let baseboard = hardware_view_rx.borrow().baseboard();
        let log = log.new(o!("component" => "HardwareMonitor"));
        let (switch_zone_policy_tx, switch_zone_policy_rx) =
            watch::channel(OperatorSwitchZonePolicy::StartIfSwitchPresent);
        let monitor = HardwareMonitor {
            log,
            baseboard,
            sled_agent_started_rx,
            service_manager_ready_rx,
            hardware_view_rx,
            switch_zone_policy_rx,
            raw_disks_tx,
            sled_agent: None,
            service_manager: None,
            is_tofino_available: false,
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

                    // We may have already loaded the tofino and were waiting on
                    // the service manager to start the switch zone; do so now.
                    let policy = self.current_switch_zone_policy();
                    self.ensure_switch_zone_activated_or_deactivated(
                        self.is_tofino_available,
                        policy,
                    ).await;
                }
                result = self.hardware_view_rx.changed() => {
                    match result {
                        Ok(()) => {
                            info!(
                                self.log,
                                "Received notification hardware \
                                 view has changed"
                            );
                            self.check_latest_hardware_snapshot().await;
                        }
                        Err(_recv_error) => {
                            // The `HardwareManager` monitoring task is an
                            // infinite loop - the only way for us to get
                            // an error from `changed()` here is if it panicked,
                            // so we will propagate such a panic.
                            panic!("Hardware manager monitor task panicked");
                        }
                    }
                }
                Ok(()) = self.switch_zone_policy_rx.changed() => {
                    let policy = self.current_switch_zone_policy();
                    info!(
                        self.log, "Switch zone policy changed; reevaluating";
                        "policy" => ?policy,
                    );
                    self.ensure_switch_zone_activated_or_deactivated(
                        self.is_tofino_available,
                        policy,
                    ).await;
                }
            }
        }
    }

    // Read the current switch zone policy (and update the watch channel to note
    // that we've read the current value).
    fn current_switch_zone_policy(&mut self) -> OperatorSwitchZonePolicy {
        *self.switch_zone_policy_rx.borrow_and_update()
    }

    async fn ensure_switch_zone_activated_or_deactivated(
        &mut self,
        is_tofino_available: bool,
        policy: OperatorSwitchZonePolicy,
    ) {
        // Remember whether the tofino is loaded regardless of the action we
        // take (or don't take) below.
        self.is_tofino_available = is_tofino_available;

        // If we don't have the service manager yet, we can't do anything.
        let Some(service_manager) = &self.service_manager else {
            return;
        };

        // Decide whether to activate or deactivate based on the combination of
        // `tofino_available` and the operator policy.
        let should_activate = match (is_tofino_available, policy) {
            // We have a tofino and policy says to start the switch zone
            (true, OperatorSwitchZonePolicy::StartIfSwitchPresent) => {
                info!(
                    self.log,
                    "tofino present and policy allows switch zone; \
                     will activate it"
                );
                true
            }
            // We have a tofino but policy says to stop the switch zone
            (true, OperatorSwitchZonePolicy::StopDespiteSwitchPresence) => {
                info!(
                    self.log,
                    "tofino present but policy disables switch zone; \
                     will deactivate it"
                );
                false
            }
            // If we don't have a tofino, stop the switch zone regardless of
            // policy.
            (false, _) => false,
        };

        if should_activate {
            if let Err(e) =
                service_manager.activate_switch(self.baseboard.clone()).await
            {
                error!(self.log, "Failed to activate switch"; e);
            }
        } else {
            if let Err(e) = service_manager.deactivate_switch().await {
                warn!(self.log, "Failed to deactivate switch"; e);
            }
        }
    }

    // Act on the current hardware snapshot.
    //
    // We use this on startup and any time the snapshot changes.
    async fn check_latest_hardware_snapshot(&mut self) {
        if let Some(sled_agent) = &self.sled_agent {
            sled_agent.notify_nexus_about_self(&self.log).await;
        }

        let snapshot = self.hardware_view_rx.borrow_and_update().clone();
        info!(
            self.log, "Checking current full hardware snapshot";
            "snapshot" => ?snapshot,
        );

        let policy = self.current_switch_zone_policy();
        self.ensure_switch_zone_activated_or_deactivated(
            snapshot.is_scrimlet_asic_available(),
            policy,
        )
        .await;

        self.raw_disks_tx.set_raw_disks(
            snapshot.into_disks().into_values().map(RawDisk::from),
            &self.log,
        );
    }
}
