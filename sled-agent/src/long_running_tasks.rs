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

use crate::artifact_store::{ArtifactStore, SledAgentArtifactStoreWrapper};
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
use camino::Utf8PathBuf;
use key_manager::{KeyManager, StorageKeyRequester};
use sled_agent_config_reconciler::{
    ConfigReconcilerHandle, ConfigReconcilerSpawnToken, InternalDisksReceiver,
    RawDisksSender, TimeSyncConfig,
};
use sled_agent_health_monitor::HealthMonitorHandle;
use sled_agent_resolvable_files::ZoneImageSourceResolver;
use sled_agent_types::zone_bundle::CleanupContext;
use sled_hardware::{HardwareManager, SledMode, UnparsedDisk};
use sled_storage::config::MountConfig;
use sled_storage::disk::RawSyntheticDisk;
use slog::{Logger, error, info};
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

    /// A handle to the set of health checks managed by the health-check-monitor
    /// system.
    pub health_monitor: HealthMonitorHandle,

    /// A handle for interacting with the trust quorum
    pub trust_quorum: trust_quorum::NodeTaskHandle,
    /// Handle to the artifact store
    pub artifact_store: Arc<ArtifactStore<InternalDisksReceiver>>,
}

pub struct LongRunningTaskResult {
    /// Handles to the long running tasks
    pub long_running_task_handles: LongRunningTaskHandles,

    /// Token spawning the config reconciler
    pub config_reconciler_spawn_token: ConfigReconcilerSpawnToken,

    /// sled agent started channel
    pub sled_agent_started_tx: oneshot::Sender<SledAgent>,

    /// service manager ready channel
    pub service_manager_ready_tx: oneshot::Sender<ServiceManager>,

    /// measurements needed for early bootup
    pub cold_boot_measurements: Vec<Utf8PathBuf>,
}

/// Spawn all long running tasks
pub async fn spawn_all_longrunning_tasks(
    log: &Logger,
    sled_mode: SledMode,
    global_zone_bootstrap_ip: Ipv6Addr,
    config: &Config,
) -> LongRunningTaskResult {
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

    let zone_bundler = spawn_zone_bundler_tasks(log, &config_reconciler).await;
    let zone_image_resolver = ZoneImageSourceResolver::new(log, internal_disks);

    let config_reconciler = Arc::new(config_reconciler);

    let artifact_store = Arc::new(
        ArtifactStore::new(
            &log,
            config_reconciler.internal_disks_rx().clone(),
            Some(Arc::clone(&config_reconciler)),
        )
        .await,
    );

    let config_reconciler_spawn_token = config_reconciler
        .pre_spawn_reconciliation_task(
            SledAgentArtifactStoreWrapper(Arc::clone(&artifact_store)),
            config_reconciler_spawn_token,
        );

    // This must come after we've spawned the ledger task
    let cold_boot_measurements = get_cold_boot_measurements(
        log,
        &config_reconciler,
        &zone_image_resolver,
    )
    .await;

    let trust_quorum = spawn_trust_quorum_task(
        log,
        &config_reconciler,
        &hardware_manager,
        global_zone_bootstrap_ip,
        config.sprockets.clone(),
        cold_boot_measurements.clone(),
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

    let health_monitor = spawn_health_monitor_tasks(log).await;

    LongRunningTaskResult {
        long_running_task_handles: LongRunningTaskHandles {
            config_reconciler,
            hardware_manager,
            hardware_monitor,
            bootstore,
            zone_bundler,
            zone_image_resolver,
            health_monitor,
            trust_quorum,
            artifact_store,
        },
        config_reconciler_spawn_token,
        sled_agent_started_tx,
        service_manager_ready_tx,
        cold_boot_measurements,
    }
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

/// on sled-agent cold boot we need a set of measurements before the
/// config reconciler actually runs
async fn get_cold_boot_measurements(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
    zone_image_resolver: &ZoneImageSourceResolver,
) -> Vec<Utf8PathBuf> {
    while let Err(
        sled_agent_config_reconciler::InventoryError::WaitingOnLedger,
    ) = config_reconciler.ledgered_sled_config()
    {
        // waiting for our ledger task to run. This could arguably loop
        // forever but if the ledger task isn't running we can't do
        // anything anyway
    }

    // Get our pre-boot measurements, first we check the ledger
    match config_reconciler.ledgered_sled_config() {
        Err(e) => {
            // Not much we can do!
            error!(log, "Error reading sled config from ledger: {e}");
            vec![]
        }
        // We haven't run RSS, we'll take what we get from the measurement manifest
        Ok(None) => match zone_image_resolver
            .status()
            .to_inventory()
            .measurement_manifest
            .boot_inventory
        {
            Err(e) => {
                // Not much we can do!
                error!(log, "Error reading boot inventory manifest: {e}");
                vec![]
            }
            Ok(s) => s
                .artifacts
                .iter()
                .filter_map(|entry| match entry.status {
                    Ok(_) => Some(entry.path.clone()),
                    Err(_) => None,
                })
                .collect(),
        },
        // Do an early resolution
        Ok(Some(s)) => {
            config_reconciler
                .bootstrap_measurement_reconciler(
                    &zone_image_resolver.status(),
                    &config_reconciler.internal_disks_rx().current(),
                    &s.measurements,
                    &log,
                )
                .await
        }
    }
}

async fn spawn_trust_quorum_task(
    log: &Logger,
    config_reconciler: &ConfigReconcilerHandle,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
    sprockets_config: SprocketsConfig,
    cold_boot_measurements: Vec<Utf8PathBuf>,
) -> trust_quorum::NodeTaskHandle {
    info!(
        log,
        "Using sprockets config for trust-quorum: {sprockets_config:#?}"
    );

    let measurements_rx = config_reconciler
        .measurement_corpus_rx(cold_boot_measurements)
        .await
        .clone();

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

    let (mut node, handle) =
        trust_quorum::NodeTask::new(config, log, measurements_rx).await;
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

async fn spawn_health_monitor_tasks(log: &Logger) -> HealthMonitorHandle {
    info!(log, "Starting health monitor");
    let log = log.new(o!("component" => "HealthMonitor"));
    HealthMonitorHandle::spawn(log)
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
