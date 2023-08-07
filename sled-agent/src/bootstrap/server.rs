// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::BOOTSTRAP_AGENT_HTTP_PORT;
use super::http_entrypoints;
use super::params::StartSledAgentRequest;
use super::pre_server::BootstrapManagers;
use crate::bootstrap::bootstore::BootstoreHandles;
use crate::bootstrap::http_entrypoints::api as http_api;
use crate::bootstrap::http_entrypoints::BootstrapServerContext;
use crate::bootstrap::maghemite;
use crate::bootstrap::pre_server::BootstrapAgentStartup;
use crate::bootstrap::rack_ops::RssAccess;
use crate::bootstrap::sprockets_server::SprocketsServer;
use crate::config::Config as SledConfig;
use crate::config::ConfigError;
use crate::storage_manager::StorageResources;
use camino::Utf8PathBuf;
use ddm_admin_client::DdmError;
use dropshot::HttpServer;
use futures::Future;
use illumos_utils::dladm;
use illumos_utils::zfs;
use illumos_utils::zone;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::ledger;
use omicron_common::ledger::Ledger;
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware::underlay;
use sled_hardware::HardwareUpdate;
use slog::Logger;
use std::borrow::Cow;
use std::io;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const SLED_AGENT_REQUEST_FILE: &str = "sled-agent-request.json";

/// Describes errors which may occur while starting the bootstrap server.
///
/// All of these errors are fatal.
#[derive(thiserror::Error, Debug)]
pub enum StartError {
    #[error("Failed to initialize logger")]
    InitLogger(#[source] io::Error),

    #[error("Failed to register DTrace probes")]
    RegisterDTraceProbes(#[source] usdt::Error),

    #[error("Failed to find address objects for maghemite")]
    FindMaghemiteAddrObjs(#[source] underlay::Error),

    #[error("underlay::find_nics() returned 0 address objects")]
    NoUnderlayAddrObjs,

    #[error("Failed to enable mg-ddm")]
    EnableMgDdm(#[from] maghemite::Error),

    #[error("Failed to create zfs key directory {dir:?}")]
    CreateZfsKeyDirectory {
        dir: &'static str,
        #[source]
        err: io::Error,
    },

    // TODO-completeness This error variant should go away (or change) when we
    // start using the IPCC-provided MAC address for the bootstrap network
    // (https://github.com/oxidecomputer/omicron/issues/2301), at least on real
    // gimlets. Maybe it stays around for non-gimlets?
    #[error("Failed to find link for bootstrap address generation")]
    ConfigLink(#[source] ConfigError),

    #[error("Failed to get MAC address of bootstrap link")]
    BootstrapLinkMac(#[source] dladm::GetMacError),

    #[error("Failed to ensure existence of etherstub {name:?}")]
    EnsureEtherstubError {
        name: &'static str,
        #[source]
        err: illumos_utils::ExecutionError,
    },

    #[error(transparent)]
    CreateVnicError(#[from] dladm::CreateVnicError),

    #[error(transparent)]
    EnsureGzAddressError(#[from] zone::EnsureGzAddressError),

    #[error(transparent)]
    GetAddressError(#[from] zone::GetAddressError),

    #[error("Failed to create DDM admin localhost client")]
    CreateDdmAdminLocalhostClient(#[source] DdmError),

    #[error("Failed to create ZFS ramdisk dataset")]
    EnsureZfsRamdiskDataset(#[source] zfs::EnsureFilesystemError),

    #[error("Failed to list zones")]
    ListZones(#[source] zone::AdmError),

    #[error("Failed to delete zone")]
    DeleteZone(#[source] zone::AdmError),

    #[error("Failed to delete omicron VNICs")]
    DeleteOmicronVnics(#[source] anyhow::Error),

    #[error("Failed to delete all XDE devices")]
    DeleteXdeDevices(#[source] illumos_utils::opte::Error),

    #[error("Failed to enable ipv6-forwarding")]
    EnableIpv6Forwarding(#[from] illumos_utils::ExecutionError),

    #[error("Incorrect binary packaging: {0}")]
    IncorrectBuildPackaging(&'static str),

    #[error("Failed to start HardwareManager: {0}")]
    StartHardwareManager(String),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),

    #[error("Failed to start sled-agent server: {0}")]
    FailedStartingServer(String),

    #[error("Failed to commit sled agent request to ledger")]
    CommitToLedger(#[from] ledger::Error),

    #[error("Failed to initialize bootstrap dropshot server: {0}")]
    InitBootstrapDropshotServer(String),

    #[error("Failed to bind sprocket server")]
    BindSprocketsServer(#[source] io::Error),
}

/// Wraps a [Agent] object, and provides helper methods for exposing it
/// via an HTTP interface and a tcp server used for rack initialization.
pub struct Server {
    bootstrap_agent: Arc<Agent>,
    sprockets_server_handle: JoinHandle<()>,
    _http_server: dropshot::HttpServer<Arc<Agent>>,
}

impl Server {
    pub async fn start(config: SledConfig) -> Result<Self, StartError> {
        // Do all initial setup that we can do even before we start listening
        // for and handling hardware updates (e.g., discovery if we're a
        // scrimlet or discovery of hard drives). If any step of this fails, we
        // fail to start.
        let BootstrapAgentStartup {
            config,
            global_zone_bootstrap_ip,
            ddm_admin_localhost_client,
            base_log,
            startup_log,
            managers,
            key_manager_handle,
        } = BootstrapAgentStartup::run(config).await?;

        // From this point on we will listen for hardware notifications and
        // potentially start the switch zone and be notified of new disks; we
        // are responsible for responding to updates from this point on.
        let mut hardware_monitor = managers.hardware.monitor();
        let storage_resources = managers.storage.resources();

        // Wait for our boot M.2 to show up.
        wait_while_handling_hardware_updates(
            wait_for_boot_m2(storage_resources, &startup_log),
            &mut hardware_monitor,
            &managers,
            None, // No underlay network yet
            &startup_log,
            "waiting for boot M.2",
        )
        .await;

        // Wait for the bootstore to start.
        let bootstore_handles = wait_while_handling_hardware_updates(
            BootstoreHandles::spawn(
                storage_resources,
                ddm_admin_localhost_client,
                managers.hardware.baseboard(),
                global_zone_bootstrap_ip,
                &base_log,
            ),
            &mut hardware_monitor,
            &managers,
            None, // No underlay network yet
            &startup_log,
            "waiting for boot M.2",
        )
        .await?;

        // Do we have a StartSledAgentRequest stored in the ledger?
        let maybe_ledger = wait_while_handling_hardware_updates(
            async {
                let paths = sled_config_paths(storage_resources).await?;
                let maybe_ledger =
                    Ledger::<PersistentSledAgentRequest<'static>>::new(
                        &startup_log,
                        paths,
                    )
                    .await;
                Ok::<_, StartError>(maybe_ledger)
            },
            &mut hardware_monitor,
            &managers,
            None, // No underlay network yet
            &startup_log,
            "waiting for boot M.2",
        )
        .await?;

        // We don't yet _act_ on the `StartSledAgentRequest` if we have one, but
        // if we have one we init our `RssAccess` noting that we're already
        // initialized. We'll start the sled-agent described by `maybe_ledger`
        // below.
        let rss_access = RssAccess::new(maybe_ledger.is_some());

        // Create a channel for requesting sled reset. We use a channel depth
        // of 1: if there's a pending sled reset request, there's no need to
        // enqueue another, and we can send back an HTTP busy.
        let (sled_reset_tx, sled_reset_rx) = mpsc::channel(1);

        // Start the bootstrap dropshot server.
        let bootstrap_context = BootstrapServerContext {
            base_log: base_log.clone(),
            global_zone_bootstrap_ip,
            storage_resources: storage_resources.clone(),
            bootstore_node_handle: bootstore_handles.node_handle.clone(),
            baseboard: managers.hardware.baseboard(),
            rss_access,
            updates: config.updates,
            sled_reset_tx,
        };
        let bootstrap_http_server = start_dropshot_server(bootstrap_context)?;

        todo!()
        /*
        let (drain, registration) = slog_dtrace::with_drain(
            config.log.to_logger("SledAgent").map_err(|message| {
                format!("initializing logger: {}", message)
            })?,
        );
        let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("Failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(msg);
        } else {
            debug!(log, "registered DTrace probes");
        }

        // Find address objects to pass to maghemite.
        let mg_addr_objs = underlay::find_nics(&sled_config.data_links)
            .map_err(|err| {
                format!("Failed to find address objects for maghemite: {err}")
            })?;
        if mg_addr_objs.is_empty() {
            return Err(
                "underlay::find_nics() returned 0 address objects".to_string()
            );
        }

        info!(log, "Starting mg-ddm service");
        maghemite::enable_mg_ddm_service(log.clone(), mg_addr_objs.clone())
            .await
            .map_err(|err| format!("Failed to start mg-ddm: {err}"))?;

        info!(log, "setting up bootstrap agent server");
        let bootstrap_agent =
            Agent::new(log.clone(), config.clone(), sled_config)
                .await
                .map_err(|e| e.to_string())?;
        info!(log, "bootstrap agent finished initialization successfully");
        let bootstrap_agent = Arc::new(bootstrap_agent);

        let mut dropshot_config = dropshot::ConfigDropshot::default();
        dropshot_config.request_body_max_bytes = 1024 * 1024;
        dropshot_config.bind_address =
            SocketAddr::V6(bootstrap_agent.http_address());
        let dropshot_log =
            log.new(o!("component" => "dropshot (BootstrapAgent)"));
        let http_server = dropshot::HttpServerStarter::new(
            &dropshot_config,
            http_api(),
            bootstrap_agent.clone(),
            &dropshot_log,
        )
        .map_err(|error| format!("initializing server: {}", error))?
        .start();

        // Start the currently-misnamed sprockets server, which listens for raw
        // TCP connections (which should ultimately be secured via sprockets).
        let sprockets_server =
            SprocketsServer::bind(Arc::clone(&bootstrap_agent), &log)
                .await
                .map_err(|err| {
                    format!("Failed to bind sprockets server: {err}")
                })?;
        let sprockets_server_handle = tokio::spawn(sprockets_server.run());

        let server = Server {
            bootstrap_agent,
            sprockets_server_handle,
            _http_server: http_server,
        };
        Ok(server)
        */
    }

    pub fn agent(&self) -> &Arc<Agent> {
        &self.bootstrap_agent
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        match self.sprockets_server_handle.await {
            Ok(()) => Ok(()),
            Err(err) => {
                if err.is_cancelled() {
                    // We control cancellation of `sprockets_server_handle`,
                    // which only happens if we intentionally abort it in
                    // `close()`; that should not result in an error here.
                    Ok(())
                } else {
                    Err(format!("Join on server tokio task failed: {err}"))
                }
            }
        }
    }

    pub async fn close(self) -> Result<(), String> {
        self.sprockets_server_handle.abort();
        self.wait_for_finish().await
    }
}

fn start_dropshot_server(
    context: BootstrapServerContext,
) -> Result<HttpServer<BootstrapServerContext>, StartError> {
    let mut dropshot_config = dropshot::ConfigDropshot::default();
    dropshot_config.request_body_max_bytes = 1024 * 1024;
    dropshot_config.bind_address = SocketAddr::V6(SocketAddrV6::new(
        context.global_zone_bootstrap_ip,
        BOOTSTRAP_AGENT_HTTP_PORT,
        0,
        0,
    ));
    let dropshot_log =
        context.base_log.new(o!("component" => "dropshot (BootstrapAgent)"));
    let http_server = dropshot::HttpServerStarter::new(
        &dropshot_config,
        http_entrypoints::api(),
        context,
        &dropshot_log,
    )
    .map_err(|error| {
        StartError::InitBootstrapDropshotServer(error.to_string())
    })?
    .start();

    Ok(http_server)
}

/// Wait for at least the M.2 we booted from to show up.
///
/// TODO-correctness Subsequent steps may assume all M.2s that will ever be
/// present are present once we return from this function; see
/// https://github.com/oxidecomputer/omicron/issues/3815.
async fn wait_for_boot_m2(storage_resources: &StorageResources, log: &Logger) {
    // Wait for at least the M.2 we booted from to show up.
    loop {
        match storage_resources.boot_disk().await {
            Some(disk) => {
                info!(log, "Found boot disk M.2: {disk:?}");
                break;
            }
            None => {
                info!(log, "Waiting for boot disk M.2...");
                tokio::time::sleep(core::time::Duration::from_millis(250))
                    .await;
            }
        }
    }
}

struct MissingM2Paths(&'static str);

impl From<MissingM2Paths> for StartError {
    fn from(value: MissingM2Paths) -> Self {
        StartError::MissingM2Paths(value.0)
    }
}

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

// Helper function to wait for `fut` while handling any updates about hardware.
async fn wait_while_handling_hardware_updates<F: Future<Output = T>, T>(
    fut: F,
    hardware_monitor: &mut broadcast::Receiver<HardwareUpdate>,
    managers: &BootstrapManagers,
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

                managers.handle_hardware_update(
                    hardware_update,
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

#[derive(Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
struct PersistentSledAgentRequest<'a> {
    request: Cow<'a, StartSledAgentRequest>,
}

impl<'a> Ledgerable for PersistentSledAgentRequest<'a> {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}

/// Runs the OpenAPI generator, emitting the spec to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_api()
        .openapi("Oxide Bootstrap Agent API", "0.0.1")
        .description("API for interacting with individual sleds")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}
