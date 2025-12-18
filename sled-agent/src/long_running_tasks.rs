// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This module is responsible for spawning, starting, and managing long running
//! tasks and task driven subsystems. These tasks run for the remainder of the
//! sled-agent process from the moment they begin. Primarily they include the
//! "managers", like `KeyManager`, `ServiceManager`, etc., and are used by both
//! the bootstrap agent and the sled-agent.
//!
//! We don't bother keeping track of the spawned tasks handles because we know
//! these tasks are supposed to run forever, and they can shutdown if their
//! handles are dropped.

use crate::bootstrap::bootstore_setup::{
    new_bootstore_config, poll_ddmd_for_bootstore_and_tq_peer_update,
};
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::bootstrap::trust_quorum_setup::new_trust_quorum_config;
use crate::config::Config;
use crate::hardware_monitor::{HardwareMonitor, HardwareMonitorHandle};
use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use crate::zone_bundle::ZoneBundler;
use bootstore::schemes::v0 as bootstore;
use key_manager::{KeyManager, StorageKeyRequester};
use sled_agent_config_reconciler::{
    ConfigReconcilerHandle, ConfigReconcilerSpawnToken, RawDisksSender,
    TimeSyncConfig,
};
use sled_agent_types::zone_bundle::CleanupContext;
use sled_agent_zone_images::ZoneImageSourceResolver;
use sled_hardware::{HardwareManager, SledMode, UnparsedDisk};
use sled_storage::config::MountConfig;
use sled_storage::disk::RawSyntheticDisk;
use slog::{Logger, info};
use sprockets_tls::keys::SprocketsConfig;
use std::net::Ipv6Addr;
use std::sync::Arc;
use tokio::sync::oneshot;
use trust_quorum;

/// A mechanism for interacting with all long running tasks that can be shared
/// between the bootstrap-agent and sled-agent code.
#[derive(Clone)]
pub struct LongRunningTaskHandles {
    /// A handle to the set of tasks managed by the sled-agent-config-reconciler
    /// system.
    pub config_reconciler: Arc<ConfigReconcilerHandle>,

    /// A mechanism for interacting with the hardware device tree
    pub hardware_manager: HardwareManager,

    /// A handle for controlling the hardware monitor's behavior
    pub hardware_monitor: HardwareMonitorHandle,

    /// A handle for interacting with the bootstore
    pub bootstore: bootstore::NodeHandle,

    /// A reference to the object used to manage zone bundles
    pub zone_bundler: ZoneBundler,

    /// Resolver for zone image sources.
    ///
    /// This isn't really implemented in the backend as a task per se, but it
    /// looks like one from the outside, and is convenient to put here. (If it
    /// had any async involved within it, it would be a task.)
    pub zone_image_resolver: ZoneImageSourceResolver,

    /// A handle for interacting with the trust quorum
    pub trust_quorum: trust_quorum::NodeTaskHandle,
}

/// Spawn all long running tasks
pub async fn spawn_all_longrunning_tasks(
    log: &Logger,
    sled_mode: SledMode,
    global_zone_bootstrap_ip: Ipv6Addr,
    config: &Config,
) -> (
    LongRunningTaskHandles,
    ConfigReconcilerSpawnToken,
    oneshot::Sender<SledAgent>,
    oneshot::Sender<ServiceManager>,
) {
    let storage_key_requester = spawn_key_manager(log);

    let time_sync_config = if let Some(true) = config.skip_timesync {
        TimeSyncConfig::Skip
    } else {
        TimeSyncConfig::Normal
    };
    let (mut config_reconciler, config_reconciler_spawn_token) =
        ConfigReconcilerHandle::new(
            MountConfig::default(),
            storage_key_requester,
            time_sync_config,
            log,
        );

    let nongimlet_observed_disks =
        config.nongimlet_observed_disks.clone().unwrap_or(vec![]);

    let hardware_manager =
        spawn_hardware_manager(log, sled_mode, nongimlet_observed_disks).await;

    // Start monitoring for hardware changes, adding some synthetic disks if
    // necessary.
    let raw_disks_tx = config_reconciler.raw_disks_tx();
    upsert_synthetic_disks_if_needed(&log, &raw_disks_tx, &config).await;
    let (hardware_monitor, sled_agent_started_tx, service_manager_ready_tx) =
        spawn_hardware_monitor(log, &hardware_manager, raw_disks_tx);

    // Wait for the boot disk so that we can work with any ledgers,
    // such as those needed by the bootstore and sled-agent
    info!(log, "Waiting for boot disk");
    let internal_disks = config_reconciler.wait_for_boot_disk().await;
    info!(log, "Found boot disk {:?}", internal_disks.boot_disk_id());

    let trust_quorum = spawn_trust_quorum_task(
        log,
        &config_reconciler,
        &hardware_manager,
        global_zone_bootstrap_ip,
        config.sprockets.clone(),
    )
    .await;

    let bootstore = spawn_bootstore_tasks(
        log,
        &config_reconciler,
        &hardware_manager,
        global_zone_bootstrap_ip,
        trust_quorum.clone(),
    )
    .await;

    let zone_bundler = spawn_zone_bundler_tasks(log, &config_reconciler).await;
    let zone_image_resolver = ZoneImageSourceResolver::new(log, internal_disks);

    (
        LongRunningTaskHandles {
            config_reconciler: Arc::new(config_reconciler),
            hardware_manager,
            hardware_monitor,
            bootstore,
            zone_bundler,
            zone_image_resolver,
            trust_quorum,
        },
        config_reconciler_spawn_token,
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
) -> (
    HardwareMonitorHandle,
    oneshot::Sender<SledAgent>,
    oneshot::Sender<ServiceManager>,
) {
    info!(log, "Starting HardwareMonitor");
    let (monitor, sled_agent_started_tx, service_manager_ready_tx) =
        HardwareMonitor::spawn(log, hardware_manager, raw_disks_tx);
    (monitor, sled_agent_started_tx, service_manager_ready_tx)
}

async fn spawn_trust_quorum_task(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
    sprockets_config: SprocketsConfig,
) -> trust_quorum::NodeTaskHandle {
    info!(
        log,
        "Using sprockets config for trust-quorum: {sprockets_config:#?}"
    );
    let cluster_dataset_paths = config_reconciler
        .internal_disks_rx()
        .current()
        .all_cluster_datasets()
        .collect::<Vec<_>>();
    let baseboard_id =
        hardware_manager.baseboard().try_into().expect("known baseboard type");
    let config = new_trust_quorum_config(
        &cluster_dataset_paths,
        baseboard_id,
        global_zone_bootstrap_ip,
        sprockets_config,
    )
    .expect("valid trust quorum config");

    info!(log, "Starting trust quorum node task");

    let (mut node, handle) = trust_quorum::NodeTask::new(config, log).await;
    tokio::spawn(async move { node.run().await });
    handle
}

async fn spawn_bootstore_tasks(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
    tq_handle: trust_quorum::NodeTaskHandle,
) -> bootstore::NodeHandle {
    let config = new_bootstore_config(
        &config_reconciler
            .internal_disks_rx()
            .current()
            .all_cluster_datasets()
            .collect::<Vec<_>>(),
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
        poll_ddmd_for_bootstore_and_tq_peer_update(log, node_handle2, tq_handle)
            .await
    });

    node_handle
}

// `ZoneBundler::new` spawns a periodic cleanup task that runs indefinitely
async fn spawn_zone_bundler_tasks(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
) -> ZoneBundler {
    info!(log, "Starting ZoneBundler related tasks");
    let log = log.new(o!("component" => "ZoneBundler"));
    ZoneBundler::new(
        log,
        config_reconciler.internal_disks_rx().clone(),
        config_reconciler.available_datasets_rx(),
        CleanupContext::default(),
    )
    .await
}

async fn upsert_synthetic_disks_if_needed(
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
                .expect("Failed to parse synthetic disk")
                .into();
            raw_disks_tx.add_or_update_raw_disk(disk, log);
        }
    }
}
