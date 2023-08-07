// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

#![allow(dead_code)]
// TODO remove

// `usdt::Error` is larger than clippy's "large err" threshold. Remove this
// allow if that changes (https://github.com/oxidecomputer/usdt/issues/133).
#![allow(clippy::result_large_err)]

use crate::bootstrap::agent::BootstrapError;
use crate::bootstrap::agent::RackInitId;
use crate::bootstrap::agent::RssAccessError;
use crate::bootstrap::params::RackInitializeRequest;
use crate::bootstrap::params::StartSledAgentRequest;
use crate::bootstrap::views::SledAgentResponse;
use crate::config::Config;
use crate::server::Server;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use crate::storage_manager::StorageResources;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use ddm_admin_client::Client as DdmAdminClient;
use dropshot::HttpServer;
use futures::Future;
use omicron_common::api::internal::shared::RackNetworkConfig;
use sled_hardware::HardwareManager;
use sled_hardware::HardwareUpdate;
use slog::Logger;
use std::net::Ipv6Addr;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod bootstrap;
mod sled_agent;
mod startup;

use self::bootstrap::BootstrapServerContext;
use self::bootstrap::SledAgentBootstrap;
use self::startup::SledAgentStartup;
use self::startup::StartError;

// Re-export this so `ServiceManager` can use it.
pub(crate) use self::startup::StartupNetworking;

const SLED_AGENT_REQUEST_FILE: &str = "sled-agent-request.json";

pub struct SledAgent {
    sled_agent_main_task: JoinHandle<()>,
    bootstrap_http_server: HttpServer<BootstrapServerContext>,
}

impl SledAgent {
    pub async fn start(config: Config) -> Result<Self, StartError> {
        // Do all initial setup - if any step of this fails, we fail to start.
        let SledAgentStartup {
            config,
            global_zone_bootstrap_ip,
            ddm_admin_localhost_client,
            base_log,
            startup_log,
            hardware_manager,
            storage_manager,
            service_manager,
            key_manager_handle,
        } = SledAgentStartup::run(config).await?;

        // From this point on we will listen for hardware notifications and
        // potentially start the switch zone and be notified of new disks.
        let mut hardware_monitor = hardware_manager.monitor();

        // Run bootstrapping in a concurrent future with monitoring for hardware
        // updates.
        let bootstrap_fut = SledAgentBootstrap::run(
            storage_manager.resources(),
            hardware_manager.baseboard(),
            global_zone_bootstrap_ip,
            config.updates.clone(),
            ddm_admin_localhost_client.clone(),
            &base_log,
        );

        // Wait for bootstrapping while handling hardware updates. We are
        // pre-trust-quorum and therefore do not yet want to set up the underlay
        // network.
        let bootstrap = wait_while_handling_hardware_updates(
            bootstrap_fut,
            &mut hardware_monitor,
            &hardware_manager,
            &service_manager,
            &storage_manager,
            None, // no underlay network
            &startup_log,
            "bootstrapping",
        )
        .await?;

        // Bootstrapping complete: we're now running the bootstrap servers, but
        // not yet receiving on the channels that those use to init or reset our
        // sled.
        let SledAgentBootstrap {
            maybe_ledger,
            bootstrap_http_server,
            bootstore_node_handle,
            sprockets_server_handle,
            sled_reset_rx,
            sled_init_rx,
        } = bootstrap;

        // Do we have a persistent sled-agent request that we need to restore?
        let state = if let Some(ledger) = maybe_ledger {
            let sled_request = ledger.data();
            let start_sled_agent_fut = sled_agent::start(
                &config,
                &sled_request.request,
                &bootstore_node_handle,
                &service_manager,
                &storage_manager,
                &ddm_admin_localhost_client,
                &base_log,
                &startup_log,
            );
            let sled_agent_server = wait_while_handling_hardware_updates(
                start_sled_agent_fut,
                &mut hardware_monitor,
                &hardware_manager,
                &service_manager,
                &storage_manager,
                None, // no underlay network
                &startup_log,
                "restoring sled-agent",
            )
            .await?;

            // We've created sled-agent; we need to (possibly) configure the
            // switch zone, if we're a scrimlet, to give it our underlay
            // network information.
            let sled_agent = sled_agent_server.sled_agent();
            let switch_zone_underlay_info =
                Some(sled_agent.switch_zone_underlay_info());
            update_from_current_hardware_snapshot(
                &hardware_manager,
                &service_manager,
                &storage_manager,
                switch_zone_underlay_info,
                &startup_log,
            )
            .await;

            // Finally, we need to load the services we're responsible for,
            // while continuing to handle hardware notifications (for which we
            // now have underlay info to provide). This cannot fail: we retry
            // indefinitely until we're done loading services.
            let load_services_fut = sled_agent.cold_boot_load_services();
            wait_while_handling_hardware_updates(
                load_services_fut,
                &mut hardware_monitor,
                &hardware_manager,
                &service_manager,
                &storage_manager,
                switch_zone_underlay_info,
                &startup_log,
                "restoring sled-agent",
            )
            .await;

            SledAgentState::ServerStarted(sled_agent_server)
        } else {
            SledAgentState::Bootstrapping
        };

        // Spawn our version of `main()`; it runs until told to exit.
        // TODO-FIXME how do we tell it to exit?
        let sled_agent_main_task = tokio::spawn(sled_agent_main(
            config,
            hardware_monitor,
            state,
            sled_init_rx,
            sled_reset_rx,
            hardware_manager,
            service_manager,
            storage_manager,
            ddm_admin_localhost_client,
            bootstore_node_handle,
            sprockets_server_handle,
            key_manager_handle,
            base_log,
        ));

        Ok(Self { sled_agent_main_task, bootstrap_http_server })
    }

    pub fn start_rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, RssAccessError> {
        self.bootstrap_http_server.app_private().start_rack_initialize(request)
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        match self.sled_agent_main_task.await {
            Ok(()) => Ok(()),
            Err(err) => Err(format!("sled-agent-main panicked: {err}")),
        }
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

impl SledAgentState {
    fn switch_zone_underlay_info(
        &self,
    ) -> Option<(Ipv6Addr, Option<&RackNetworkConfig>)> {
        match self {
            SledAgentState::Bootstrapping => None,
            SledAgentState::ServerStarted(server) => {
                Some(server.sled_agent().switch_zone_underlay_info())
            }
        }
    }
}

#[allow(clippy::too_many_arguments)] // TODO-john FIXME
async fn sled_agent_main(
    config: Config,
    mut hardware_monitor: broadcast::Receiver<HardwareUpdate>,
    mut state: SledAgentState,
    mut sled_init_rx: mpsc::Receiver<(
        StartSledAgentRequest,
        oneshot::Sender<Result<SledAgentResponse, String>>,
    )>,
    mut sled_reset_rx: mpsc::Receiver<
        oneshot::Sender<Result<(), BootstrapError>>,
    >,
    hardware_manager: HardwareManager,
    service_manager: ServiceManager,
    storage_manager: StorageManager,
    ddm_admin_localhost_client: DdmAdminClient,
    bootstore_node_handle: bootstore::NodeHandle,
    _sprockets_server_handle: JoinHandle<()>,
    _key_manager_handle: JoinHandle<()>,
    base_log: Logger,
) {
    let log = base_log.new(o!("component" => "SledAgentMain"));
    loop {
        // TODO-correctness We pause handling hardware update messages while we
        // handle sled init/reset requests - is that okay?
        tokio::select! {
            // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
            hardware_update = hardware_monitor.recv() => {
                info!(
                    log,
                    "Handling hardware update message";
                    "phase" => "sled-agent-main",
                    "update" => ?hardware_update,
                );

                // TODO are these choices correct? This tries to match the
                // previous behavior when SledAgent monitored hardware itself.
                let should_notify_nexus = match &hardware_update {
                    Ok(HardwareUpdate::TofinoDeviceChange)
                    | Err(broadcast::error::RecvError::Lagged(_)) => true,
                    _ => false,
                };

                handle_hardware_update(
                    hardware_update,
                    &hardware_manager,
                    &service_manager,
                    &storage_manager,
                    state.switch_zone_underlay_info(),
                    &log,
                ).await;

                if should_notify_nexus {
                    match &state {
                        SledAgentState::Bootstrapping => (),
                        SledAgentState::ServerStarted(server) => {
                            server.sled_agent().notify_nexus_about_self(&log);
                        }
                    }
                }
            }

            // Cancel-safe per the docs on `mpsc::Receiver::recv()`.
            Some((request, response_tx)) = sled_init_rx.recv() => {
                match &state {
                    SledAgentState::Bootstrapping => {
                        let response = match sled_agent::start(
                            &config,
                            &request,
                            &bootstore_node_handle,
                            &service_manager,
                            &storage_manager,
                            &ddm_admin_localhost_client,
                            &base_log,
                            &log,
                        ).await {
                            Ok(server) => {
                                state = SledAgentState::ServerStarted(server);
                                Ok(SledAgentResponse { id: request.id })
                            }
                            Err(err) => {
                                Err(format!("{err:#}"))
                            }
                        };
                        _ = response_tx.send(response);
                    }
                    SledAgentState::ServerStarted(server) => {
                        info!(log, "Sled Agent already loaded");

                        let sled_address = request.sled_address();
                        let response = if server.id() != request.id {
                            Err(format!(
                                "Sled Agent already running with UUID {}, \
                                 but {} was requested",
                                server.id(),
                                request.id,
                            ))
                        } else if &server.address().ip() != sled_address.ip() {
                            Err(format!(
                                "Sled Agent already running on address {}, \
                                 but {} was requested",
                                server.address().ip(),
                                sled_address.ip(),
                            ))
                        } else {
                            Ok(SledAgentResponse { id: server.id() })
                        };

                        _ = response_tx.send(response);
                    }
                }
            }

            // Cancel-safe per the docs on `mpsc::Receiver::recv()`.
            Some(response_tx) = sled_reset_rx.recv() => {
                // Try to reset the sled, but do not exit early on error.
                let result = async {
                    bootstrap::uninstall_zones().await?;
                    bootstrap::uninstall_sled_local_config(
                        storage_manager.resources(),
                    ).await?;
                    bootstrap::uninstall_networking(&log).await?;
                    bootstrap::uninstall_storage(&log).await?;
                    Ok::<(), BootstrapError>(())
                }
                .await;

                _ = response_tx.send(result);
            }
        }
    }
}

// Helper function to wait for `fut` while handling any updates about hardware.
#[allow(clippy::too_many_arguments)] // TODO maybe combine some into a struct?
async fn wait_while_handling_hardware_updates<F: Future<Output = T>, T>(
    fut: F,
    hardware_monitor: &mut broadcast::Receiver<HardwareUpdate>,
    hardware_manager: &HardwareManager,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    underlay_network: Option<(Ipv6Addr, Option<&RackNetworkConfig>)>,
    log: &Logger,
    log_phase: &str,
) -> T {
    tokio::pin!(fut);
    loop {
        tokio::select! {
            // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
            hardware_update = hardware_monitor.recv() => {
                info!(
                    log,
                    "Handling hardware update message";
                    "phase" => log_phase,
                    "update" => ?hardware_update,
                );

                handle_hardware_update(
                    hardware_update,
                    hardware_manager,
                    service_manager,
                    storage_manager,
                    underlay_network,
                    log,
                ).await;
            }

            // Cancel-safe: we're using a `&mut Future`; dropping the
            // reference does not cancel the underlying future.
            result = &mut fut => return result,
        }
    }
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
