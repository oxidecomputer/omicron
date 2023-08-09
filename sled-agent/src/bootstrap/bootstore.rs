// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for configuring and starting the bootstore during bootstrap agent
//! startup.

use super::agent::BootstrapError;
use super::config::BOOTSTORE_PORT;
use crate::storage_manager::StorageResources;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use ddm_admin_client::Client as DdmAdminClient;
use sled_hardware::underlay::BootstrapInterface;
use sled_hardware::Baseboard;
use slog::Logger;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::task::JoinHandle;

const BOOTSTORE_FSM_STATE_FILE: &str = "bootstore-fsm-state.json";
const BOOTSTORE_NETWORK_CONFIG_FILE: &str = "bootstore-network-config.json";

pub(super) struct BootstoreHandles {
    pub(super) node_handle: bootstore::NodeHandle,

    // These two are never used; we keep them to show ownership of the spawned
    // tasks.
    _node_task_handle: JoinHandle<()>,
    _peer_update_task_handle: JoinHandle<()>,
}

impl BootstoreHandles {
    pub(super) async fn spawn(
        storage_resources: &StorageResources,
        ddm_admin_client: DdmAdminClient,
        baseboard: Baseboard,
        global_zone_bootstrap_ip: Ipv6Addr,
        base_log: &Logger,
    ) -> Result<Self, BootstrapError> {
        let config = bootstore::Config {
            id: baseboard,
            addr: SocketAddrV6::new(
                global_zone_bootstrap_ip,
                BOOTSTORE_PORT,
                0,
                0,
            ),
            time_per_tick: Duration::from_millis(250),
            learn_timeout: Duration::from_secs(5),
            rack_init_timeout: Duration::from_secs(300),
            rack_secret_request_timeout: Duration::from_secs(5),
            fsm_state_ledger_paths: bootstore_fsm_state_paths(
                &storage_resources,
            )
            .await?,
            network_config_ledger_paths: bootstore_network_config_paths(
                &storage_resources,
            )
            .await?,
        };

        let (mut node, node_handle) =
            bootstore::Node::new(config, base_log).await;

        let join_handle = tokio::spawn(async move { node.run().await });

        // Spawn a task for polling DDMD and updating bootstore
        let peer_update_handle =
            tokio::spawn(poll_ddmd_for_bootstore_peer_update(
                base_log.new(o!("component" => "bootstore_ddmd_poller")),
                node_handle.clone(),
                ddm_admin_client,
            ));

        Ok(Self {
            node_handle,
            _node_task_handle: join_handle,
            _peer_update_task_handle: peer_update_handle,
        })
    }
}

async fn bootstore_fsm_state_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, BootstrapError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_FSM_STATE_FILE))
        .collect();

    if paths.is_empty() {
        return Err(BootstrapError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn bootstore_network_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, BootstrapError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_NETWORK_CONFIG_FILE))
        .collect();

    if paths.is_empty() {
        return Err(BootstrapError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn poll_ddmd_for_bootstore_peer_update(
    log: Logger,
    bootstore_node_handle: bootstore::NodeHandle,
    ddmd_client: DdmAdminClient,
) {
    let mut current_peers: BTreeSet<SocketAddrV6> = BTreeSet::new();
    // We're talking to a service's admin interface on localhost and
    // we're only asking for its current state. We use a retry in a loop
    // instead of `backoff`.
    //
    // We also use this timeout in the case of spurious ddmd failures
    // that require a reconnection from the ddmd_client.
    const RETRY: tokio::time::Duration = tokio::time::Duration::from_secs(5);

    loop {
        match ddmd_client
            .derive_bootstrap_addrs_from_prefixes(&[
                BootstrapInterface::GlobalZone,
            ])
            .await
        {
            Ok(addrs) => {
                let peers: BTreeSet<_> = addrs
                    .map(|ip| SocketAddrV6::new(ip, BOOTSTORE_PORT, 0, 0))
                    .collect();
                if peers != current_peers {
                    current_peers = peers;
                    if let Err(e) = bootstore_node_handle
                        .load_peer_addresses(current_peers.clone())
                        .await
                    {
                        error!(
                            log,
                            concat!(
                                "Bootstore comms error: {}. ",
                                "bootstore::Node task must have paniced",
                            ),
                            e
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                warn!(
                    log, "Failed to get prefixes from ddmd";
                    "err" => #%err,
                );
                break;
            }
        }
        tokio::time::sleep(RETRY).await;
    }
}
