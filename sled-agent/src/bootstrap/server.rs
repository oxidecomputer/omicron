// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::config::BOOTSTRAP_AGENT_HTTP_PORT;
use super::http_entrypoints;
use super::params::RackInitializeRequest;
use super::params::StartSledAgentRequest;
use super::pre_server::BootstrapManagers;
use super::rack_ops::RackInitId;
use super::views::SledAgentResponse;
use super::BootstrapError;
use super::RssAccessError;
use crate::bootstrap::bootstore::BootstoreHandles;
use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use crate::bootstrap::http_entrypoints::api as http_api;
use crate::bootstrap::http_entrypoints::BootstrapServerContext;
use crate::bootstrap::maghemite;
use crate::bootstrap::pre_server::BootstrapAgentStartup;
use crate::bootstrap::rack_ops::RssAccess;
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::bootstrap::sprockets_server::SprocketsServer;
use crate::config::Config as SledConfig;
use crate::config::ConfigError;
use crate::server::Server as SledAgentServer;
use crate::sled_agent::SledAgent;
use crate::storage_manager::StorageResources;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use ddm_admin_client::Client as DdmAdminClient;
use ddm_admin_client::DdmError;
use dropshot::HttpServer;
use futures::Future;
use futures::StreamExt;
use illumos_utils::dladm;
use illumos_utils::zfs;
use illumos_utils::zone;
use illumos_utils::zone::Zones;
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
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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

/// Server for the bootstrap agent.
///
/// Wraps an inner tokio task that handles low-level operations like
/// initializating and resetting this sled (and starting / stopping the
/// sled-agent server).
pub struct Server {
    inner_task: JoinHandle<()>,
    bootstrap_http_server: HttpServer<BootstrapServerContext>,
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

        // Check the latest hardware snapshot; we could have missed events
        // between the creation of the hardware manager and our subscription of
        // its monitor.
        managers.check_latest_hardware_snapshot(None, &startup_log).await;

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
                ddm_admin_localhost_client.clone(),
                managers.hardware.baseboard(),
                global_zone_bootstrap_ip,
                &base_log,
            ),
            &mut hardware_monitor,
            &managers,
            None, // No underlay network yet
            &startup_log,
            "initializing bootstore",
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
            "loading sled-agent request from ledger",
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
            updates: config.updates.clone(),
            sled_reset_tx,
        };
        let bootstrap_http_server = start_dropshot_server(bootstrap_context)?;

        // Start the currently-misnamed sprockets server, which listens for raw
        // TCP connections (which should ultimately be secured via sprockets).
        let (sled_init_tx, sled_init_rx) = mpsc::channel(1);

        // We don't bother to wrap this bind in a
        // `wait_while_handling_hardware_updates()` because (a) binding should
        // be fast and (b) can succeed regardless of any pending hardware
        // updates; we'll resume monitoring and handling them shortly.
        let sprockets_server = SprocketsServer::bind(
            SocketAddrV6::new(
                global_zone_bootstrap_ip,
                BOOTSTRAP_AGENT_RACK_INIT_PORT,
                0,
                0,
            ),
            sled_init_tx,
            &base_log,
        )
        .await
        .map_err(StartError::BindSprocketsServer)?;
        let sprockets_server_handle = tokio::spawn(sprockets_server.run());

        // Do we have a persistent sled-agent request that we need to restore?
        let state = if let Some(ledger) = maybe_ledger {
            let sled_request = ledger.data();
            let sled_agent_server = wait_while_handling_hardware_updates(
                start_sled_agent(
                    &config,
                    &sled_request.request,
                    &bootstore_handles.node_handle,
                    &managers,
                    &ddm_admin_localhost_client,
                    &base_log,
                    &startup_log,
                ),
                &mut hardware_monitor,
                &managers,
                None, // No underlay network yet
                &startup_log,
                "restoring sled-agent (cold boot)",
            )
            .await?;

            let sled_agent = sled_agent_server.sled_agent();

            // We've created sled-agent; we need to (possibly) reconfigure the
            // switch zone, if we're a scrimlet, to give it our underlay network
            // information.
            let underlay_network_info = sled_agent.switch_zone_underlay_info();
            info!(
                startup_log, "Sled Agent started; rescanning hardware";
                "underlay_network_info" => ?underlay_network_info,
            );
            managers
                .check_latest_hardware_snapshot(Some(&sled_agent), &startup_log)
                .await;

            // For cold boot specifically, we now need to load the services
            // we're responsible for, while continuing to handle hardware
            // notifications. This cannot fail: we retry indefinitely until
            // we're done loading services.
            wait_while_handling_hardware_updates(
                sled_agent.load_services(),
                &mut hardware_monitor,
                &managers,
                Some(&sled_agent),
                &startup_log,
                "restoring sled-agent services (cold boot)",
            )
            .await;

            SledAgentState::ServerStarted(sled_agent_server)
        } else {
            SledAgentState::Bootstrapping
        };

        // Spawn our inner task that handles any future hardware updates and any
        // requests from our dropshot or sprockets server that affect the sled
        // agent state.
        let inner = Inner {
            config,
            hardware_monitor,
            state,
            sled_init_rx,
            sled_reset_rx,
            managers,
            ddm_admin_localhost_client,
            bootstore_handles,
            _sprockets_server_handle: sprockets_server_handle,
            _key_manager_handle: key_manager_handle,
            base_log,
        };
        let inner_task = tokio::spawn(inner.run());

        Ok(Self { inner_task, bootstrap_http_server })
    }

    pub fn start_rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, RssAccessError> {
        self.bootstrap_http_server.app_private().start_rack_initialize(request)
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        match self.inner_task.await {
            Ok(()) => Ok(()),
            Err(err) => {
                Err(format!("bootstrap agent inner task panicked: {err}"))
            }
        }
    }
}

// Describes the states the sled-agent server can be in; controlled by us (the
// bootstrap server).
enum SledAgentState {
    // We're still in the bootstrapping phase, waiting for a sled-agent request.
    Bootstrapping,
    // ... or the sled agent server is running.
    ServerStarted(SledAgentServer),
}

impl SledAgentState {
    fn sled_agent(&self) -> Option<&SledAgent> {
        match self {
            SledAgentState::Bootstrapping => None,
            SledAgentState::ServerStarted(server) => Some(server.sled_agent()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SledAgentServerStartError {
    #[error("Failed to start sled-agent server: {0}")]
    FailedStartingServer(String),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),

    #[error("Failed to commit sled agent request to ledger")]
    CommitToLedger(#[from] ledger::Error),
}

impl From<SledAgentServerStartError> for StartError {
    fn from(value: SledAgentServerStartError) -> Self {
        match value {
            SledAgentServerStartError::FailedStartingServer(s) => {
                Self::FailedStartingServer(s)
            }
            SledAgentServerStartError::MissingM2Paths(dataset) => {
                Self::MissingM2Paths(dataset)
            }
            SledAgentServerStartError::CommitToLedger(err) => {
                Self::CommitToLedger(err)
            }
        }
    }
}

async fn start_sled_agent(
    config: &SledConfig,
    request: &StartSledAgentRequest,
    bootstore: &bootstore::NodeHandle,
    managers: &BootstrapManagers,
    ddmd_client: &DdmAdminClient,
    base_log: &Logger,
    log: &Logger,
) -> Result<SledAgentServer, SledAgentServerStartError> {
    info!(log, "Loading Sled Agent: {:?}", request);

    // TODO-correctness: If we fail partway through, we do not cleanly roll back
    // all the changes we've made (e.g., initializing LRTQ, informing the
    // storage manager about keys, advertising prefixes, ...).

    // Initialize the secret retriever used by the `KeyManager`
    if request.use_trust_quorum {
        info!(log, "KeyManager: using lrtq secret retriever");
        let salt = request.hash_rack_id();
        LrtqOrHardcodedSecretRetriever::init_lrtq(salt, bootstore.clone())
    } else {
        info!(log, "KeyManager: using hardcoded secret retriever");
        LrtqOrHardcodedSecretRetriever::init_hardcoded();
    }

    // Inform the storage service that the key manager is available
    managers.storage.key_manager_ready().await;

    // Start trying to notify ddmd of our sled prefix so it can
    // advertise it to other sleds.
    //
    // TODO-security This ddmd_client is used to advertise both this
    // (underlay) address and our bootstrap address. Bootstrap addresses are
    // unauthenticated (connections made on them are auth'd via sprockets),
    // but underlay addresses should be exchanged via authenticated channels
    // between ddmd instances. It's TBD how that will work, but presumably
    // we'll need to do something different here for underlay vs bootstrap
    // addrs (either talk to a differently-configured ddmd, or include info
    // indicating which kind of address we're advertising).
    ddmd_client.advertise_prefix(request.subnet);

    // Server does not exist, initialize it.
    let server = SledAgentServer::start(
        config,
        base_log.clone(),
        request.clone(),
        managers.service.clone(),
        managers.storage.clone(),
        bootstore.clone(),
    )
    .await
    .map_err(SledAgentServerStartError::FailedStartingServer)?;

    info!(log, "Sled Agent loaded; recording configuration");

    // Record this request so the sled agent can be automatically
    // initialized on the next boot.
    let paths = sled_config_paths(managers.storage.resources()).await?;

    let mut ledger = Ledger::new_with(
        &log,
        paths,
        PersistentSledAgentRequest { request: Cow::Borrowed(request) },
    );
    ledger.commit().await?;

    Ok(server)
}

// Clippy doesn't like `StartError` due to
// https://github.com/oxidecomputer/usdt/issues/133; remove this once that issue
// is addressed.
#[allow(clippy::result_large_err)]
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
/// <https://github.com/oxidecomputer/omicron/issues/3815>.
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
        Self::MissingM2Paths(value.0)
    }
}

impl From<MissingM2Paths> for SledAgentServerStartError {
    fn from(value: MissingM2Paths) -> Self {
        Self::MissingM2Paths(value.0)
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
    sled_agent: Option<&SledAgent>,
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
                    sled_agent,
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

struct Inner {
    config: SledConfig,
    hardware_monitor: broadcast::Receiver<HardwareUpdate>,
    state: SledAgentState,
    sled_init_rx: mpsc::Receiver<(
        StartSledAgentRequest,
        oneshot::Sender<Result<SledAgentResponse, String>>,
    )>,
    sled_reset_rx: mpsc::Receiver<oneshot::Sender<Result<(), BootstrapError>>>,
    managers: BootstrapManagers,
    ddm_admin_localhost_client: DdmAdminClient,
    bootstore_handles: BootstoreHandles,
    _sprockets_server_handle: JoinHandle<()>,
    _key_manager_handle: JoinHandle<()>,
    base_log: Logger,
}

impl Inner {
    async fn run(mut self) {
        let log = self.base_log.new(o!("component" => "SledAgentMain"));
        loop {
            // TODO-correctness We pause handling hardware update messages while
            // we handle sled init/reset requests - is that okay?
            tokio::select! {
                // Cancel-safe per the docs on `broadcast::Receiver::recv()`.
                hardware_update = self.hardware_monitor.recv() => {
                    self.handle_hardware_update(hardware_update, &log).await;
                }

                // Cancel-safe per the docs on `mpsc::Receiver::recv()`.
                Some((request, response_tx)) = self.sled_init_rx.recv() => {
                    self.handle_start_sled_agent_request(
                        request,
                        response_tx,
                        &log,
                    ).await;
                }

                // Cancel-safe per the docs on `mpsc::Receiver::recv()`.
                Some(response_tx) = self.sled_reset_rx.recv() => {
                    // Try to reset the sled, but do not exit early on error.
                    let result = async {
                        self.uninstall_zones().await?;
                        self.uninstall_sled_local_config().await?;
                        self.uninstall_networking(&log).await?;
                        self.uninstall_storage(&log).await?;
                        Ok::<(), BootstrapError>(())
                    }
                    .await;

                    _ = response_tx.send(result);
                }
            }
        }
    }

    async fn handle_hardware_update(
        &self,
        hardware_update: Result<HardwareUpdate, broadcast::error::RecvError>,
        log: &Logger,
    ) {
        info!(
            log,
            "Handling hardware update message";
            "phase" => "bootstore-steady-state",
            "update" => ?hardware_update,
        );

        self.managers
            .handle_hardware_update(
                hardware_update,
                self.state.sled_agent(),
                &log,
            )
            .await;
    }

    async fn handle_start_sled_agent_request(
        &mut self,
        request: StartSledAgentRequest,
        response_tx: oneshot::Sender<Result<SledAgentResponse, String>>,
        log: &Logger,
    ) {
        match &self.state {
            SledAgentState::Bootstrapping => {
                let response = match start_sled_agent(
                    &self.config,
                    &request,
                    &self.bootstore_handles.node_handle,
                    &self.managers,
                    &self.ddm_admin_localhost_client,
                    &self.base_log,
                    &log,
                )
                .await
                {
                    Ok(server) => {
                        // We've created sled-agent; we need to (possibly)
                        // reconfigure the switch zone, if we're a scrimlet, to
                        // give it our underlay network information.
                        self.managers
                            .check_latest_hardware_snapshot(
                                Some(server.sled_agent()),
                                log,
                            )
                            .await;

                        self.state = SledAgentState::ServerStarted(server);
                        Ok(SledAgentResponse { id: request.id })
                    }
                    Err(err) => Err(format!("{err:#}")),
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

    // Uninstall all oxide zones (except the switch zone)
    async fn uninstall_zones(&self) -> Result<(), BootstrapError> {
        const CONCURRENCY_CAP: usize = 32;
        futures::stream::iter(Zones::get().await?)
            .map(Ok::<_, anyhow::Error>)
            // Use for_each_concurrent_then_try to delete as much as possible.
            // We only return one error though -- hopefully that's enough to
            // signal to the caller that this failed.
            .for_each_concurrent_then_try(CONCURRENCY_CAP, |zone| async move {
                if zone.name() != "oxz_switch" {
                    Zones::halt_and_remove(zone.name()).await?;
                }
                Ok(())
            })
            .await
            .map_err(BootstrapError::Cleanup)?;
        Ok(())
    }

    async fn uninstall_sled_local_config(&self) -> Result<(), BootstrapError> {
        let config_dirs = self
            .managers
            .storage
            .resources()
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter();

        for dir in config_dirs {
            for entry in dir.read_dir_utf8().map_err(|err| {
                BootstrapError::Io { message: format!("Deleting {dir}"), err }
            })? {
                let entry = entry.map_err(|err| BootstrapError::Io {
                    message: format!("Deleting {dir}"),
                    err,
                })?;

                let path = entry.path();
                let file_type =
                    entry.file_type().map_err(|err| BootstrapError::Io {
                        message: format!("Deleting {path}"),
                        err,
                    })?;

                if file_type.is_dir() {
                    tokio::fs::remove_dir_all(path).await
                } else {
                    tokio::fs::remove_file(path).await
                }
                .map_err(|err| BootstrapError::Io {
                    message: format!("Deleting {path}"),
                    err,
                })?;
            }
        }
        Ok(())
    }

    async fn uninstall_networking(
        &self,
        log: &Logger,
    ) -> Result<(), BootstrapError> {
        // NOTE: This is very similar to the invocations
        // in "sled_hardware::cleanup::cleanup_networking_resources",
        // with a few notable differences:
        //
        // - We can't remove bootstrap-related networking -- this operation
        // is performed via a request on the bootstrap network.
        // - We avoid deleting addresses using the chelsio link. Removing
        // these addresses would delete "cxgbe0/ll", and could render
        // the sled inaccessible via a local interface.

        sled_hardware::cleanup::delete_underlay_addresses(&log)
            .map_err(BootstrapError::Cleanup)?;
        sled_hardware::cleanup::delete_omicron_vnics(&log)
            .await
            .map_err(BootstrapError::Cleanup)?;
        illumos_utils::opte::delete_all_xde_devices(&log)?;
        Ok(())
    }

    async fn uninstall_storage(
        &self,
        log: &Logger,
    ) -> Result<(), BootstrapError> {
        let datasets = zfs::get_all_omicron_datasets_for_delete()
            .map_err(BootstrapError::ZfsDatasetsList)?;
        for dataset in &datasets {
            info!(log, "Removing dataset: {dataset}");
            zfs::Zfs::destroy_dataset(dataset)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::address::Ipv6Subnet;
    use omicron_test_utils::dev::test_setup_log;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

    #[tokio::test]
    async fn persistent_sled_agent_request_serialization() {
        let logctx =
            test_setup_log("persistent_sled_agent_request_serialization");
        let log = &logctx.log;

        let request = PersistentSledAgentRequest {
            request: Cow::Owned(StartSledAgentRequest {
                id: Uuid::new_v4(),
                rack_id: Uuid::new_v4(),
                ntp_servers: vec![String::from("test.pool.example.com")],
                dns_servers: vec!["1.1.1.1".parse().unwrap()],
                use_trust_quorum: false,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            }),
        };

        let tempdir = camino_tempfile::Utf8TempDir::new().unwrap();
        let paths = vec![tempdir.path().join("test-file")];

        let mut ledger = Ledger::new_with(log, paths.clone(), request.clone());
        ledger.commit().await.expect("Failed to write to ledger");

        let ledger = Ledger::<PersistentSledAgentRequest>::new(log, paths)
            .await
            .expect("Failed to read request");

        assert!(&request == ledger.data(), "serialization round trip failed");
        logctx.cleanup_successful();
    }

    #[test]
    fn test_persistent_sled_agent_request_schema() {
        let schema = schemars::schema_for!(PersistentSledAgentRequest<'_>);
        expectorate::assert_contents(
            "../schema/persistent-sled-agent-request.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
