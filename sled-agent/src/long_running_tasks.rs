// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module is responsible for spawning, starting, and managing long running
//! tasks and task driven subsystems. These tasks run for the remainder of the
//! sled-agent process from the moment they begin. Primarily they include the
//! "managers", like `StorageManager`, `InstanceManager`, etc..., and are used
//! by both the bootstrap agent and the sled-agent.
//!
//! We don't bother keeping track of the spawned tasks handles because we know
//! these tasks are supposed to run forever, and they can shutdown if their
//! handles are dropped.

use crate::bootstrap::bootstore_setup::{
    new_bootstore_config, poll_ddmd_for_bootstore_peer_update,
};
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::config::Config;
use crate::hardware_monitor::HardwareMonitor;
use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use crate::storage_monitor::{StorageMonitor, StorageMonitorHandle};
use crate::zone_bundle::ZoneBundler;
use bootstore::schemes::v0 as bootstore;
use illumos_utils::zpool::ZpoolName;
use key_manager::{KeyManager, StorageKeyRequester};
use sled_agent_types::zone_bundle::CleanupContext;
use sled_agent_zone_images::{ZoneImageSourceResolver, ZoneImageZpools};
use sled_hardware::{HardwareManager, SledMode, UnparsedDisk};
use sled_storage::config::MountConfig;
use sled_storage::disk::RawSyntheticDisk;
use sled_storage::manager::{StorageHandle, StorageManager};
use sled_storage::resources::AllDisks;
use slog::{Logger, info};
use std::net::Ipv6Addr;
use tokio::sync::oneshot;

/// A mechanism for interacting with all long running tasks that can be shared
/// between the bootstrap-agent and sled-agent code.
#[derive(Clone)]
pub struct LongRunningTaskHandles {
    /// A mechanism for retrieving storage keys. This interacts with the
    /// [`KeyManager`] task. In the future, there may be other handles for
    /// retrieving different types of keys. Separating the handles limits the
    /// access for a given key type to the code that holds the handle.
    pub storage_key_requester: StorageKeyRequester,

    /// A mechanism for talking to the [`StorageManager`] which is responsible
    /// for establishing zpools on disks and managing their datasets.
    pub storage_manager: StorageHandle,

    /// A mechanism for talking to the [`StorageMonitor`], which reacts to disk
    /// changes and updates the dump devices.
    pub storage_monitor_handle: StorageMonitorHandle,

    /// A mechanism for interacting with the hardware device tree
    pub hardware_manager: HardwareManager,

    // A handle for interacting with the bootstore
    pub bootstore: bootstore::NodeHandle,

    // A reference to the object used to manage zone bundles
    pub zone_bundler: ZoneBundler,

    /// Resolver for zone image sources.
    ///
    /// This isn't really implemented in the backend as a task per se, but it
    /// looks like one from the outside, and is convenient to put here. (If it
    /// had any async involved within it, it would be a task.)
    pub zone_image_resolver: ZoneImageSourceResolver,
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
    let mut storage_manager =
        spawn_storage_manager(log, storage_key_requester.clone());

    let storage_monitor_handle =
        spawn_storage_monitor(log, storage_manager.clone());

    let nongimlet_observed_disks =
        config.nongimlet_observed_disks.clone().unwrap_or(vec![]);

    let hardware_manager =
        spawn_hardware_manager(log, sled_mode, nongimlet_observed_disks).await;

    // Start monitoring for hardware changes
    let (sled_agent_started_tx, service_manager_ready_tx) =
        spawn_hardware_monitor(log, &hardware_manager, &storage_manager);

    // Add some synthetic disks if necessary.
    upsert_synthetic_disks_if_needed(&log, &storage_manager, &config).await;

    // Wait for the boot disk so that we can work with any ledgers,
    // such as those needed by the bootstore and sled-agent
    info!(log, "Waiting for boot disk");
    let (disk_id, boot_zpool) = storage_manager.wait_for_boot_disk().await;
    info!(log, "Found boot disk {:?}", disk_id);

    let all_disks = storage_manager.get_latest_disks().await;
    let bootstore = spawn_bootstore_tasks(
        log,
        &all_disks,
        &hardware_manager,
        global_zone_bootstrap_ip,
    )
    .await;

    let zone_bundler =
        spawn_zone_bundler_tasks(log, &mut storage_manager).await;
    let zone_image_resolver =
        make_zone_image_resolver(log, &all_disks, &boot_zpool);

    (
        LongRunningTaskHandles {
            storage_key_requester,
            storage_manager,
            storage_monitor_handle,
            hardware_manager,
            bootstore,
            zone_bundler,
            zone_image_resolver,
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

fn spawn_storage_manager(
    log: &Logger,
    key_requester: StorageKeyRequester,
) -> StorageHandle {
    info!(log, "Starting StorageManager");
    let (manager, handle) =
        StorageManager::new(log, MountConfig::default(), key_requester);
    tokio::spawn(async move {
        manager.run().await;
    });
    handle
}

fn spawn_storage_monitor(
    log: &Logger,
    storage_handle: StorageHandle,
) -> StorageMonitorHandle {
    info!(log, "Starting StorageMonitor");
    let (storage_monitor, handle) =
        StorageMonitor::new(log, MountConfig::default(), storage_handle);
    tokio::spawn(async move {
        storage_monitor.run().await;
    });
    handle
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
    storage_handle: &StorageHandle,
) -> (oneshot::Sender<SledAgent>, oneshot::Sender<ServiceManager>) {
    info!(log, "Starting HardwareMonitor");
    let (mut monitor, sled_agent_started_tx, service_manager_ready_tx) =
        HardwareMonitor::new(log, hardware_manager, storage_handle);
    tokio::spawn(async move {
        monitor.run().await;
    });
    (sled_agent_started_tx, service_manager_ready_tx)
}

async fn spawn_bootstore_tasks(
    log: &Logger,
    all_disks: &AllDisks,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
) -> bootstore::NodeHandle {
    let config = new_bootstore_config(
        all_disks,
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
async fn spawn_zone_bundler_tasks(
    log: &Logger,
    storage_handle: &mut StorageHandle,
) -> ZoneBundler {
    info!(log, "Starting ZoneBundler related tasks");
    let log = log.new(o!("component" => "ZoneBundler"));
    ZoneBundler::new(log, storage_handle.clone(), CleanupContext::default())
        .await
}

fn make_zone_image_resolver(
    log: &Logger,
    all_disks: &AllDisks,
    boot_zpool: &ZpoolName,
) -> ZoneImageSourceResolver {
    let zpools = ZoneImageZpools {
        root: &all_disks.mount_config().root,
        all_m2_zpools: all_disks.all_m2_zpools(),
    };
    ZoneImageSourceResolver::new(log, &zpools, boot_zpool)
}

async fn upsert_synthetic_disks_if_needed(
    log: &Logger,
    storage_manager: &StorageHandle,
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
                .expect("Failed to parse synthetic disk")
                .into();
            storage_manager.detected_raw_disk(disk).await.await.unwrap();
        }
    }
}
