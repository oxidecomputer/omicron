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

use crate::bootstrap::bootstore::{
    new_bootstore_config, poll_ddmd_for_bootstore_peer_update,
};
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::zone_bundle::{CleanupContext, ZoneBundler};
use bootstore::schemes::v0 as bootstore;
use key_manager::{KeyManager, StorageKeyRequester};
use sled_hardware::{HardwareManager, SledMode};
use sled_storage::manager::{StorageHandle, StorageManager};
use slog::{info, Logger};
use std::net::Ipv6Addr;

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
) -> LongRunningTaskHandles {
    let storage_key_requester = spawn_key_manager(log);
    let mut storage_manager =
        spawn_storage_manager(log, storage_key_requester.clone());

    // TODO: Does this need to run inside tokio::task::spawn_blocking?
    let hardware_manager = spawn_hardware_manager(log, sled_mode);

    // Wait for the boot disk so that we can work with any ledgers,
    // such as those needed by the bootstore and sled-agent
    let _ = storage_manager.wait_for_boot_disk().await;

    let bootstore = spawn_bootstore_tasks(
        log,
        &mut storage_manager,
        &hardware_manager,
        global_zone_bootstrap_ip,
    )
    .await;

    let zone_bundler = spawn_zone_bundler_tasks(log, &mut storage_manager);

    LongRunningTaskHandles {
        storage_key_requester,
        storage_manager,
        hardware_manager,
        bootstore,
        zone_bundler,
    }
}

fn spawn_key_manager(log: &Logger) -> StorageKeyRequester {
    info!(log, "Starting KeyManager");
    let secret_retriever = LrtqOrHardcodedSecretRetriever::new();
    let (mut key_manager, storage_key_requester) =
        KeyManager::new(log, secret_retriever);
    let key_manager_handle =
        tokio::spawn(async move { key_manager.run().await });
    storage_key_requester
}

fn spawn_storage_manager(
    log: &Logger,
    key_requester: StorageKeyRequester,
) -> StorageHandle {
    info!(log, "Starting StorageManager");
    let (mut manager, handle) = StorageManager::new(log, key_requester);
    tokio::spawn(async move {
        manager.run().await;
    });
    handle
}

fn spawn_hardware_manager(
    log: &Logger,
    sled_mode: SledMode,
) -> HardwareManager {
    // The `HardwareManager` does not use the the "task/handle" pattern
    // and spawns its worker task inside `HardwareManager::new`. Instead of returning
    // a handle to send messages to that task, the "Inner/Mutex" pattern is used
    // which shares data between the task, the manager itself, and the users of the manager
    // since the manager can be freely cloned and passed around.
    //
    // There are pros and cons to both methods, but the reason to mention it here is that
    // the handle in this case is the `HardwareManager` itself.
    info!(log, "Starting HardwareManager"; "sled_mode" => ?sled_mode);
    HardwareManager::new(log, sled_mode).unwrap()
}

async fn spawn_bootstore_tasks(
    log: &Logger,
    storage_handle: &mut StorageHandle,
    hardware_manager: &HardwareManager,
    global_zone_bootstrap_ip: Ipv6Addr,
) -> bootstore::NodeHandle {
    let storage_resources = storage_handle.get_latest_resources().await;
    let config = new_bootstore_config(
        &storage_resources,
        hardware_manager.baseboard(),
        global_zone_bootstrap_ip,
    )
    .unwrap();

    // Create and spawn the bootstore
    let (mut node, node_handle) = bootstore::Node::new(config, log).await;
    tokio::spawn(async move { node.run().await });

    // Spawn a task for polling DDMD and updating bootstore with peer addresses
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
    storage_handle: &mut StorageHandle,
) -> ZoneBundler {
    let log = log.new(o!("component" => "ZoneBundler"));
    ZoneBundler::new(log, storage_handle.clone(), CleanupContext::default())
}
