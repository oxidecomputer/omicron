// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module is responsible for spawning, starting, and managing long running
//! tasks and task driven subsystems. These tasks run for the remainder of the
//! sled-agent process from the moment they begin. Primarily they include the
//! "managers", like `InstanceManager`, and are used by both the bootstrap agent
//! and the sled-agent.
//!
//! We don't bother keeping track of the spawned tasks handles because we know
//! these tasks are supposed to run forever, and they can shutdown if their
//! handles are dropped.

use crate::bootstrap::bootstore_setup::{
    new_bootstore_config, poll_ddmd_for_bootstore_peer_update,
};
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::config::Config;
use crate::config_reconciler::{
    ConfigReconcilerHandle, DatasetTask, DatasetTaskSupportBundleHandle,
    InternalDisksReceiver, RawDisksSender,
};
use crate::hardware_monitor::HardwareMonitor;
use crate::services::{ServiceManager, TimeSyncConfig};
use crate::sled_agent::SledAgent;
use crate::storage_monitor::StorageMonitor;
use crate::zone_bundle::ZoneBundler;
use bootstore::schemes::v0 as bootstore;
use key_manager::{KeyManager, StorageKeyRequester};
use sled_agent_types::zone_bundle::CleanupContext;
use sled_hardware::{HardwareManager, SledMode, UnparsedDisk};
use sled_storage::config::MountConfig;
use sled_storage::disk::RawSyntheticDisk;
use slog::{Logger, info};
use std::net::Ipv6Addr;
use std::sync::Arc;
use tokio::sync::oneshot;

/// A mechanism for interacting with all long running tasks that can be shared
/// between the bootstrap-agent and sled-agent code.
#[derive(Clone)]
pub struct LongRunningTaskHandles {
    /// TODO-john comments
    pub config_reconciler: Arc<ConfigReconcilerHandle>,
    pub support_bundle_dataset_task_handle: DatasetTaskSupportBundleHandle,

    /// A mechanism for interacting with the hardware device tree
    pub hardware_manager: HardwareManager,

    // A handle for interacting with the bootstore
    pub bootstore: bootstore::NodeHandle,

    // A reference to the object used to manage zone bundles
    pub zone_bundler: ZoneBundler,
}

/// Spawn all long running tasks
pub async fn spawn_all_longrunning_tasks(
    log: &Logger,
    sled_mode: SledMode,
    global_zone_bootstrap_ip: Ipv6Addr,
    config: &Config,
) -> (
    LongRunningTaskHandles,
    oneshot::Sender<SledAgent>,
    oneshot::Sender<ServiceManager>,
) {
    let storage_key_requester = spawn_key_manager(log);

    let (reconciler_dataset_task_handle, support_bundle_dataset_task_handle) =
        DatasetTask::spawn(MountConfig::default(), log);

    let time_sync_config = if let Some(true) = config.skip_timesync {
        TimeSyncConfig::Skip
    } else {
        TimeSyncConfig::Normal
    };

    let (config_reconciler, raw_disks_tx) = ConfigReconcilerHandle::new(
        storage_key_requester,
        reconciler_dataset_task_handle,
        time_sync_config,
        log,
    );
    /*
    let mut storage_manager =
        spawn_storage_manager(log, storage_key_requester.clone());

    let storage_monitor_handle =
        spawn_storage_monitor(log, storage_manager.clone());
        */
    spawn_storage_monitor(log, &config_reconciler);

    let nongimlet_observed_disks =
        config.nongimlet_observed_disks.clone().unwrap_or(vec![]);

    let hardware_manager =
        spawn_hardware_manager(log, sled_mode, nongimlet_observed_disks).await;

    // Add some synthetic disks if necessary.
    upsert_synthetic_disks_if_needed(&log, &raw_disks_tx, &config);

    // Start monitoring for hardware changes
    let (sled_agent_started_tx, service_manager_ready_tx) =
        spawn_hardware_monitor(log, &hardware_manager, raw_disks_tx);

    // Wait for the boot disk so that we can work with any ledgers,
    // such as those needed by the bootstore and sled-agent
    let mut internal_disks_rx = config_reconciler.internal_disks_rx().clone();
    info!(log, "Waiting for boot disk");
    let disk_id = internal_disks_rx.wait_for_boot_disk().await;
    info!(log, "Found boot disk {:?}", disk_id);

    let bootstore = spawn_bootstore_tasks(
        log,
        &mut internal_disks_rx,
        &hardware_manager,
        global_zone_bootstrap_ip,
    )
    .await;

    let zone_bundler = spawn_zone_bundler_tasks(log, &config_reconciler);

    (
        LongRunningTaskHandles {
            config_reconciler: Arc::new(config_reconciler),
            support_bundle_dataset_task_handle,
            hardware_manager,
            bootstore,
            zone_bundler,
        },
        sled_agent_started_tx,
        service_manager_ready_tx,
    )
}

fn spawn_key_manager(log: &Logger) -> StorageKeyRequester {
    info!(log, "Starting KeyManager");
    let secret_retriever = LrtqOrHardcodedSecretRetriever::new();
    let (mut key_manager, storage_key_requester) =
        KeyManager::new(log, secret_retriever);
    tokio::spawn(async move { key_manager.run().await });
    storage_key_requester
}

fn spawn_storage_monitor(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
) {
    info!(log, "Starting StorageMonitor");
    let storage_monitor = StorageMonitor::new(
        log,
        config_reconciler.internal_disks_rx().clone(),
        config_reconciler.reconciler_state_rx(),
    );
    tokio::spawn(async move {
        storage_monitor.run().await;
    });
}

async fn spawn_hardware_manager(
    log: &Logger,
    sled_mode: SledMode,
    nongimlet_observed_disks: Vec<UnparsedDisk>,
) -> HardwareManager {
    // The `HardwareManager` does not use the the "task/handle" pattern
    // and spawns its worker task inside `HardwareManager::new`. Instead of returning
    // a handle to send messages to that task, the "Inner/Mutex" pattern is used
    // which shares data between the task, the manager itself, and the users of the manager
    // since the manager can be freely cloned and passed around.
    //
    // There are pros and cons to both methods, but the reason to mention it here is that
    // the handle in this case is the `HardwareManager` itself.
    info!(log, "Starting HardwareManager"; "sled_mode" => ?sled_mode, "nongimlet_observed_disks" => ?nongimlet_observed_disks);
    let log = log.clone();
    tokio::task::spawn_blocking(move || {
        HardwareManager::new(&log, sled_mode, nongimlet_observed_disks).unwrap()
    })
    .await
    .unwrap()
}

fn spawn_hardware_monitor(
    log: &Logger,
    hardware_manager: &HardwareManager,
    raw_disks_tx: RawDisksSender,
) -> (oneshot::Sender<SledAgent>, oneshot::Sender<ServiceManager>) {
    info!(log, "Starting HardwareMonitor");
    let (mut monitor, sled_agent_started_tx, service_manager_ready_tx) =
        HardwareMonitor::new(log, hardware_manager, raw_disks_tx);
    tokio::spawn(async move {
        monitor.run().await;
    });
    (sled_agent_started_tx, service_manager_ready_tx)
}

async fn spawn_bootstore_tasks(
    log: &Logger,
    internal_disks_rx: &mut InternalDisksReceiver,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
) -> bootstore::NodeHandle {
    let config = new_bootstore_config(
        internal_disks_rx,
        hardware_manager.baseboard(),
        global_zone_bootstrap_ip,
    )
    .unwrap();

    // Create and spawn the bootstore
    info!(log, "Starting Bootstore");
    let (mut node, node_handle) = bootstore::Node::new(config, log).await;
    tokio::spawn(async move { node.run().await });

    // Spawn a task for polling DDMD and updating bootstore with peer addresses
    info!(log, "Starting Bootstore DDMD poller");
    let log = log.new(o!("component" => "bootstore_ddmd_poller"));
    let node_handle2 = node_handle.clone();
    tokio::spawn(async move {
        poll_ddmd_for_bootstore_peer_update(log, node_handle2).await
    });

    node_handle
}

// `ZoneBundler::new` spawns a periodic cleanup task that runs indefinitely
fn spawn_zone_bundler_tasks(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
) -> ZoneBundler {
    info!(log, "Starting ZoneBundler related tasks");
    let log = log.new(o!("component" => "ZoneBundler"));
    ZoneBundler::new(
        log,
        config_reconciler.internal_disks_rx().clone(),
        config_reconciler.reconciler_state_rx(),
        CleanupContext::default(),
    )
}

fn upsert_synthetic_disks_if_needed(
    log: &Logger,
    raw_disks_tx: &RawDisksSender,
    config: &Config,
) {
    if let Some(vdevs) = &config.vdevs {
        for (i, vdev) in vdevs.iter().enumerate() {
            info!(
                log,
                "Upserting synthetic device to Storage Manager";
                "vdev" => vdev.to_string(),
            );
            let disk = RawSyntheticDisk::load(vdev, i.try_into().unwrap())
                .expect("parsed synthetic disk");
            raw_disks_tx.add_or_update_raw_disk(disk.into());
        }
    }
}
