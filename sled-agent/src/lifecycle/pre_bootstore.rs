// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*
//! TODO explanatory comment

use super::startup::SledAgentSetup;
use super::startup::StartError;
use crate::bootstrap::config::BOOTSTORE_PORT;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use crate::storage_manager::StorageResources;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use sled_hardware::Baseboard;
use sled_hardware::HardwareManager;
use sled_hardware::HardwareUpdate;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;

const BOOTSTORE_FSM_STATE_FILE: &str = "bootstore-fsm-state.json";
const BOOTSTORE_NETWORK_CONFIG_FILE: &str = "bootstore-network-config.json";

pub(super) struct PreBootstore<'a> {
    pub(super) global_zone_bootstrap_ip: Ipv6Addr,
    pub(super) hardware_monitor: &'a mut broadcast::Receiver<HardwareUpdate>,
    pub(super) hardware_manager: &'a HardwareManager,
    pub(super) service_manager: &'a ServiceManager,
    pub(super) storage_manager: &'a StorageManager,
    pub(super) log: Logger,
    pub(super) base_log: &'a Logger,
}

impl PreBootstore<'_> {
    pub(super) async fn run(&mut self) -> Result<(), StartError> {
        // Wait for at least the M.2 we booted from to show up.
        let mut check_boot_disk_interval =
            tokio::time::interval(Duration::from_millis(250));
        check_boot_disk_interval
            .set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
                hardware_update = self.hardware_monitor.recv() => {
                    self.handle_hardware_update(hardware_update).await;
                }

                // Cancel-safe per the docs on `Interval::tick()`.
                _instant = check_boot_disk_interval.tick() => {
                    match self.storage_manager.resources().boot_disk().await {
                        Some(disk) => {
                            info!(self.log, "Found boot disk M.2: {disk:?}");
                            break;
                        }
                        None => {
                            info!(self.log, "Waiting for boot disk M.2...");
                        }
                    }
                }
            }
        }

        // Configure and start the bootstore. We do this on a background task so
        // we can continue to monitor for hardware updates.
        let mut setup_bootstore_task = tokio::spawn(setup_bootstore(
            self.hardware_manager.baseboard(),
            self.global_zone_bootstrap_ip,
            self.storage_manager.resources().clone(),
            self.base_log.clone(),
        ));

        loop {
            tokio::select! {
                // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
                hardware_update = self.hardware_monitor.recv() => {
                    self.handle_hardware_update(hardware_update).await;
                }

                // Cancel-safe per the docs on `JoinHandle`.
                task_result = &mut setup_bootstore_task => {
                    // `task_result` can only be `Err` if the task panicked, so
                    // we will propagate such a panic.
                    let result = task_result.unwrap();
                    return result;
                }
            }
        }
    }

    async fn handle_hardware_update(
        &self,
        update: Result<HardwareUpdate, broadcast::error::RecvError>,
    ) {
        // We are pre-trust-quorum and therefore do not yet want to
        // set up the underlay network.
        let underlay_network = None;
        super::handle_hardware_update(
            update,
            &self.hardware_manager,
            &self.service_manager,
            &self.storage_manager,
            underlay_network,
            &self.log,
        )
        .await;
    }
}

async fn setup_bootstore(
    baseboard: Baseboard,
    global_zone_bootstrap_ip: Ipv6Addr,
    storage_resources: StorageResources,
    base_log: Logger,
) -> Result<(), StartError> {
    let bootstore_config = bootstore::Config {
        id: baseboard,
        addr: SocketAddrV6::new(global_zone_bootstrap_ip, BOOTSTORE_PORT, 0, 0),
        time_per_tick: Duration::from_millis(250),
        learn_timeout: Duration::from_secs(5),
        rack_init_timeout: Duration::from_secs(300),
        rack_secret_request_timeout: Duration::from_secs(5),
        fsm_state_ledger_paths: bootstore_fsm_state_paths(&storage_resources)
            .await?,
        network_config_ledger_paths: bootstore_network_config_paths(
            &storage_resources,
        )
        .await?,
    };

    let (mut bootstore_node, bootstore_node_handle) =
        bootstore::Node::new(bootstore_config, &base_log).await;

    let bootstore_join_handle =
        tokio::spawn(async move { bootstore_node.run().await });

    // Spawn a task for polling DDMD and updating bootstore
    let bootstore_peer_update_handle =
        Self::poll_ddmd_for_bootstore_peer_update(
            &ba_log,
            bootstore_node_handle.clone(),
        )
        .await;

    Ok(())
}

async fn bootstore_fsm_state_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, StartError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_FSM_STATE_FILE))
        .collect();

    if paths.is_empty() {
        return Err(StartError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn bootstore_network_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, StartError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_NETWORK_CONFIG_FILE))
        .collect();

    if paths.is_empty() {
        return Err(StartError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}
*/
