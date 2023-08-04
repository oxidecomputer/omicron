// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

#![allow(dead_code)]
// TODO remove

// `usdt::Error` is larger than clippy's "large err" threshold. Remove this
// allow if that changes (https://github.com/oxidecomputer/usdt/issues/133).
#![allow(clippy::result_large_err)]

use crate::bootstrap::agent::RssAccess;
use crate::config::Config;
use crate::lifecycle::bootstrap::BootstrapServerContext;
use crate::server::Server;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use crate::storage_manager::StorageResources;
use camino::Utf8PathBuf;
use omicron_common::api::internal::shared::RackNetworkConfig;
use sled_hardware::HardwareManager;
use sled_hardware::HardwareUpdate;
use slog::Logger;
use std::net::Ipv6Addr;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

mod bootstrap;
mod sled_agent;
mod startup;

//use pre_bootstore::PreBootstore;
use self::startup::BootstoreHandles;
use self::startup::SledAgentSetup;
use self::startup::StartError;

// Re-export this so `ServiceManager` can use it.
pub(crate) use self::startup::StartupNetworking;

const SLED_AGENT_REQUEST_FILE: &str = "sled-agent-request.json";

pub struct SledAgent {
    sled_agent_main_task: JoinHandle<()>,
}

impl SledAgent {
    pub async fn start(config: Config) -> Result<Self, StartError> {
        // Do all initial setup - if any step of this fails, we fail to start.
        let SledAgentSetup {
            config,
            global_zone_bootstrap_ip,
            ddm_admin_localhost_client,
            base_log,
            startup_log,
            hardware_manager,
            storage_manager,
            service_manager,
            key_manager_handle,
        } = SledAgentSetup::run(config).await?;

        // From this point on we will listen for hardware notifications and
        // potentially start the switch zone and be notified of new disks.
        let mut hardware_monitor = hardware_manager.monitor();

        // Put the rest of startup in a future that will run concurrently with
        // us monitoring for hardware events.
        let rest_of_start_fut = {
            let service_manager = service_manager.clone();
            let storage_manager = storage_manager.clone();
            let baseboard = hardware_manager.baseboard();
            let startup_log = startup_log.clone();
            tokio::spawn(async move {
                let storage_resources = storage_manager.resources();

                // Wait for our boot M.2 to show up.
                startup::wait_for_boot_m2(&storage_resources, &startup_log)
                    .await;

                // Wait for the bootstore to start. If this fails, we fail to
                // start.
                let bootstore = startup::spawn_bootstore_tasks(
                    &storage_resources,
                    ddm_admin_localhost_client.clone(),
                    baseboard.clone(),
                    global_zone_bootstrap_ip,
                    &base_log,
                )
                .await?;

                // Do we have a StartSledAgentRequest stored in the ledger? If
                // we fail to read the ledger at all, we fail to start.
                let maybe_ledger =
                    startup::read_persistent_sled_agent_request_from_ledger(
                        &storage_resources,
                        &startup_log,
                    )
                    .await?;

                // If we have a request stored in the ledger, start the
                // sled-agent server. If this fails, we fail to start.
                //
                // If we have no request in the ledger, we remain in the
                // `Bootstrapping` state and are finished starting.
                let state = match maybe_ledger {
                    Some(ledger) => {
                        let sled_request = ledger.data();
                        let server = sled_agent::start(
                            &config,
                            &sled_request.request,
                            &bootstore.node_handle,
                            &service_manager,
                            &storage_manager,
                            &ddm_admin_localhost_client,
                            &base_log,
                            &startup_log,
                        )
                        .await?;
                        SledAgentState::ServerStarted(server)
                    }
                    None => SledAgentState::Bootstrapping,
                };

                // Construct an RssAccess based on whether or not we're already
                // initialized.
                let rss_access = match &state {
                    SledAgentState::Bootstrapping => RssAccess::new(false),
                    SledAgentState::ServerStarted(_) => RssAccess::new(true),
                };

                // Start the bootstrap dropshot server.
                let bootstrap_context = BootstrapServerContext {
                    base_log,
                    global_zone_bootstrap_ip,
                    storage_resources: storage_resources.clone(),
                    bootstore_node_handle: bootstore.node_handle.clone(),
                    baseboard,
                    rss_access,
                    updates: config.updates,
                };
                let bootstrap_http_server =
                    bootstrap::start_dropshot_server(bootstrap_context);

                Ok::<_, StartError>((state, bootstore))
            })
        };
        tokio::pin!(rest_of_start_fut);

        let (state, bootstore) = loop {
            tokio::select! {
                // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
                hardware_update = hardware_monitor.recv() => {
                    info!(
                        startup_log,
                        "Handling hardware update message";
                        "update" => ?hardware_update,
                    );

                    // We are pre-trust-quorum and therefore do not yet want to
                    // set up the underlay network.
                    let underlay_network = None;
                    handle_hardware_update(hardware_update,
                        &hardware_manager,
                        &service_manager,
                        &storage_manager,
                        underlay_network,
                        &startup_log,
                    ).await;
                }

                // Cancel-safe: we're using a `&mut Future`; dropping the
                // reference does not cancel the underlying future.
                task_result = &mut rest_of_start_fut => {
                    let result = task_result.unwrap()?;
                    break result;
                }
            }
        };

        // Spawn our version of `main()`; it runs until told to exit.
        // TODO-FIXME how do we tell it to exit?
        // TODO-FIXME wait until this task tell us to return to catch other
        // startup errors?
        let sled_agent_main_task =
            tokio::spawn(sled_agent_main(state, bootstore, key_manager_handle));

        Ok(Self { sled_agent_main_task })
    }
}

// Describes the view of the sled agent from the perspective of the bootstrap
// agent.
enum SledAgentState {
    // We're still in the bootstrapping phase, waiting for a sled-agent request.
    Bootstrapping,
    // ... or the sled agent server is running.
    ServerStarted(Server),
}

async fn sled_agent_main(
    _state: SledAgentState,
    _bootstore: BootstoreHandles,
    _key_manager_handle: JoinHandle<()>,
) {
    /*
    // Unpack all the handles and managers we already set up.
    let SledAgentSetup {
        config,
        ddm_admin_localhost_client,
        base_log,
        hardware_manager,
        storage_manager,
        service_manager,
        key_manager_handle,
    } = &setup;
    let mut hardware_monitor = hardware_manager.monitor();

    // Early lifecycle: We have not yet started the bootstore, and are waiting
    // for `hardware_manager` to tell us it has found our boot M.2. It may also
    // tell us we have a switch attached, in which case we start the switch
    // zone.
    let mut pre_bootstore = PreBootstore {
        hardware_monitor: &mut hardware_monitor,
        hardware_manager,
        service_manager,
        storage_manager,
        log: base_log.new(o!("component" => "PreBootstore")),
    };
    pre_bootstore.run().await;
    */
}

async fn handle_hardware_update(
    update: Result<HardwareUpdate, broadcast::error::RecvError>,
    hardware_manager: &HardwareManager,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    underlay_network: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
    log: &Logger,
) {
    match update {
        Ok(update) => match update {
            HardwareUpdate::TofinoLoaded => {
                let baseboard = hardware_manager.baseboard();
                if let Err(e) = service_manager
                    .activate_switch(underlay_network, baseboard)
                    .await
                {
                    warn!(log, "Failed to activate switch: {e}");
                }
            }
            HardwareUpdate::TofinoUnloaded => {
                if let Err(e) = service_manager.deactivate_switch().await {
                    warn!(log, "Failed to deactivate switch: {e}");
                }
            }
            HardwareUpdate::TofinoDeviceChange => {
                // TODO-correctness What should we do here?
            }
            HardwareUpdate::DiskAdded(disk) => {
                storage_manager.upsert_disk(disk).await;
            }
            HardwareUpdate::DiskRemoved(disk) => {
                storage_manager.delete_disk(disk).await;
            }
        },
        Err(broadcast::error::RecvError::Lagged(count)) => {
            warn!(log, "Hardware monitor missed {count} messages");
            update_from_current_hardware_snapshot(
                hardware_manager,
                service_manager,
                storage_manager,
                underlay_network,
                log,
            )
            .await;
        }
        Err(broadcast::error::RecvError::Closed) => {
            // The `HardwareManager` monitoring task is an infinite loop - the
            // only way for us to get `Closed` here is if it panicked, so we
            // will propagate such a panic.
            panic!("Hardware manager monitor task panicked");
        }
    }
}

async fn update_from_current_hardware_snapshot(
    hardware_manager: &HardwareManager,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    underlay_network: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
    log: &Logger,
) {
    info!(log, "Checking current full hardware snapshot");
    if hardware_manager.is_scrimlet_driver_loaded() {
        let baseboard = hardware_manager.baseboard();
        if let Err(e) =
            service_manager.activate_switch(underlay_network, baseboard).await
        {
            warn!(log, "Failed to activate switch: {e}");
        }
    } else {
        if let Err(e) = service_manager.deactivate_switch().await {
            warn!(log, "Failed to deactivate switch: {e}");
        }
    }

    storage_manager
        .ensure_using_exactly_these_disks(hardware_manager.disks())
        .await;
}

struct MissingM2Paths(&'static str);

async fn sled_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, MissingM2Paths> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(SLED_AGENT_REQUEST_FILE))
        .collect();

    if paths.is_empty() {
        return Err(MissingM2Paths(sled_hardware::disk::CONFIG_DATASET));
    }
    Ok(paths)
}
