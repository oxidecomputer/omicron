// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::BootstrapError;
use super::RssAccessError;
use super::config::BOOTSTRAP_AGENT_HTTP_PORT;
use super::http_entrypoints;
use super::views::SledAgentResponse;
use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use crate::bootstrap::http_entrypoints::BootstrapServerContext;
use crate::bootstrap::maghemite;
use crate::bootstrap::pre_server::BootstrapAgentStartup;
use crate::bootstrap::pumpkind;
use crate::bootstrap::rack_ops::RssAccess;
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::bootstrap::sprockets_server::SprocketsServer;
use crate::config::Config as SledConfig;
use crate::config::ConfigError;
use crate::long_running_tasks::LongRunningTaskHandles;
use crate::server::Server as SledAgentServer;
use crate::services::ServiceManager;
use crate::sled_agent::SledAgent;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use dropshot::HttpServer;
use futures::StreamExt;
use illumos_utils::dladm;
use illumos_utils::zfs;
use illumos_utils::zone;
use illumos_utils::zone::Api;
use illumos_utils::zone::Zones;
use omicron_common::ledger;
use omicron_common::ledger::Ledger;
use omicron_ddm_admin_client::DdmError;
use omicron_ddm_admin_client::types::EnableStatsRequest;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackInitUuid;
use sled_agent_types::rack_init::RackInitializeRequest;
use sled_agent_types::sled::StartSledAgentRequest;
use sled_hardware::underlay;
use sled_storage::dataset::CONFIG_DATASET;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::io;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
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

    #[error("Failed to register DTrace probes: {0}")]
    RegisterDTraceProbes(String),

    #[error("Failed to find address objects for maghemite")]
    FindMaghemiteAddrObjs(#[source] underlay::Error),

    #[error("underlay::find_nics() returned 0 address objects")]
    NoUnderlayAddrObjs,

    #[error("Failed to enable mg-ddm")]
    EnableMgDdm(#[from] maghemite::Error),

    #[error("Failed to enable pumpkind")]
    EnablePumpkind(#[from] pumpkind::Error),

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
    EnsureZfsRamdiskDataset(#[source] zfs::EnsureDatasetError),

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

    #[error("Failed to initialize lrtq node as learner: {0}")]
    FailedLearnerInit(bootstore::NodeRequestError),
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
            base_log,
            startup_log,
            service_manager,
            long_running_task_handles,
            sled_agent_started_tx,
        } = BootstrapAgentStartup::run(config).await?;

        // Do we have a StartSledAgentRequest stored in the ledger?
        let paths =
            sled_config_paths(&long_running_task_handles.storage_manager)
                .await?;
        let maybe_ledger =
            Ledger::<StartSledAgentRequest>::new(&startup_log, paths).await;

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
            storage_manager: long_running_task_handles.storage_manager.clone(),
            bootstore_node_handle: long_running_task_handles.bootstore.clone(),
            baseboard: long_running_task_handles.hardware_manager.baseboard(),
            rss_access,
            updates: config.updates.clone(),
            sled_reset_tx,
            sprockets: config.sprockets.clone(),
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
            config.sprockets.clone(),
            &base_log,
        )
        .await
        .map_err(StartError::BindSprocketsServer)?;
        let sprockets_server_handle = tokio::spawn(sprockets_server.run());

        // Do we have a persistent sled-agent request that we need to restore?
        let state = if let Some(ledger) = maybe_ledger {
            let start_sled_agent_request = ledger.into_inner();
            let sled_agent_server = start_sled_agent(
                &config,
                start_sled_agent_request,
                long_running_task_handles.clone(),
                service_manager.clone(),
                &base_log,
                &startup_log,
            )
            .await?;

            // Give the HardwareMonitory access to the `SledAgent`
            let sled_agent = sled_agent_server.sled_agent();
            sled_agent_started_tx
                .send(sled_agent.clone())
                .map_err(|_| ())
                .expect("Failed to send to StorageMonitor");

            // For cold boot specifically, we now need to load the services
            // we're responsible for, while continuing to handle hardware
            // notifications. This cannot fail: we retry indefinitely until
            // we're done loading services.
            sled_agent.load_services().await;
            SledAgentState::ServerStarted(sled_agent_server)
        } else {
            SledAgentState::Bootstrapping(Some(sled_agent_started_tx))
        };

        // Spawn our inner task that handles any future hardware updates and any
        // requests from our dropshot or sprockets server that affect the sled
        // agent state.
        let inner = Inner {
            config,
            state,
            sled_init_rx,
            sled_reset_rx,
            long_running_task_handles,
            service_manager,
            _sprockets_server_handle: sprockets_server_handle,
            base_log,
        };
        let inner_task = tokio::spawn(inner.run());

        Ok(Self { inner_task, bootstrap_http_server })
    }

    pub fn start_rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<RackInitUuid, RssAccessError> {
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
    Bootstrapping(Option<oneshot::Sender<SledAgent>>),
    // ... or the sled agent server is running.
    ServerStarted(SledAgentServer),
}

#[derive(thiserror::Error, Debug)]
pub enum SledAgentServerStartError {
    #[error("Failed to start sled-agent server: {0}")]
    FailedStartingServer(String),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),

    #[error("Failed to commit sled agent request to ledger")]
    CommitToLedger(#[from] ledger::Error),

    #[error("Failed to initialize this lrtq node as a learner: {0}")]
    FailedLearnerInit(#[from] bootstore::NodeRequestError),
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
            SledAgentServerStartError::FailedLearnerInit(err) => {
                Self::FailedLearnerInit(err)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_sled_agent(
    config: &SledConfig,
    request: StartSledAgentRequest,
    long_running_task_handles: LongRunningTaskHandles,
    service_manager: ServiceManager,
    base_log: &Logger,
    log: &Logger,
) -> Result<SledAgentServer, SledAgentServerStartError> {
    info!(log, "Loading Sled Agent: {:?}", request);

    // TODO-correctness: If we fail partway through, we do not cleanly roll back
    // all the changes we've made (e.g., initializing LRTQ, informing the
    // storage manager about keys, advertising prefixes, ...).

    // Initialize the secret retriever used by the `KeyManager`
    if request.body.use_trust_quorum {
        info!(log, "KeyManager: using lrtq secret retriever");
        let salt = request.hash_rack_id();
        LrtqOrHardcodedSecretRetriever::init_lrtq(
            salt,
            long_running_task_handles.bootstore.clone(),
        )
    } else {
        info!(log, "KeyManager: using hardcoded secret retriever");
        LrtqOrHardcodedSecretRetriever::init_hardcoded();
    }

    if request.body.use_trust_quorum && request.body.is_lrtq_learner {
        info!(log, "Initializing sled as learner");
        match long_running_task_handles.bootstore.init_learner().await {
            Err(bootstore::NodeRequestError::Fsm(
                bootstore::ApiError::AlreadyInitialized,
            )) => {
                // This is a cold boot. Let's ignore this error and continue.
            }
            Err(e) => return Err(e.into()),
            Ok(()) => (),
        }
    }

    // Inform the storage service that the key manager is available
    long_running_task_handles.storage_manager.key_manager_ready().await;

    // Inform our DDM reconciler of our underlay subnet and the information it
    // needs for maghemite to enable Oximeter stats.
    let ddm_reconciler = service_manager.ddm_reconciler();
    ddm_reconciler.set_underlay_subnet(request.body.subnet);
    ddm_reconciler.enable_stats(EnableStatsRequest {
        rack_id: request.body.rack_id,
        sled_id: request.body.id.into_untyped_uuid(),
    });

    // Server does not exist, initialize it.
    let server = SledAgentServer::start(
        config,
        base_log.clone(),
        request.clone(),
        long_running_task_handles.clone(),
        service_manager,
    )
    .await
    .map_err(SledAgentServerStartError::FailedStartingServer)?;

    info!(log, "Sled Agent loaded; recording configuration");

    // Record this request so the sled agent can be automatically
    // initialized on the next boot.
    let paths =
        sled_config_paths(&long_running_task_handles.storage_manager).await?;

    let mut ledger = Ledger::new_with(&log, paths, request);
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
    dropshot_config.default_request_body_max_bytes = 1024 * 1024;
    dropshot_config.bind_address = SocketAddr::V6(SocketAddrV6::new(
        context.global_zone_bootstrap_ip,
        BOOTSTRAP_AGENT_HTTP_PORT,
        0,
        0,
    ));
    let dropshot_log =
        context.base_log.new(o!("component" => "dropshot (BootstrapAgent)"));
    let http_server = dropshot::ServerBuilder::new(
        http_entrypoints::api(),
        context,
        dropshot_log,
    )
    .config(dropshot_config)
    .start()
    .map_err(|error| {
        StartError::InitBootstrapDropshotServer(error.to_string())
    })?;

    Ok(http_server)
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
    storage: &StorageHandle,
) -> Result<Vec<Utf8PathBuf>, MissingM2Paths> {
    let resources = storage.get_latest_disks().await;
    let paths: Vec<_> = resources
        .all_m2_mountpoints(CONFIG_DATASET)
        .into_iter()
        .map(|p| p.join(SLED_AGENT_REQUEST_FILE))
        .collect();

    if paths.is_empty() {
        return Err(MissingM2Paths(CONFIG_DATASET));
    }
    Ok(paths)
}

struct Inner {
    config: SledConfig,
    state: SledAgentState,
    sled_init_rx: mpsc::Receiver<(
        StartSledAgentRequest,
        oneshot::Sender<Result<SledAgentResponse, String>>,
    )>,
    sled_reset_rx: mpsc::Receiver<oneshot::Sender<Result<(), BootstrapError>>>,
    long_running_task_handles: LongRunningTaskHandles,
    service_manager: ServiceManager,
    _sprockets_server_handle: JoinHandle<()>,
    base_log: Logger,
}

impl Inner {
    async fn run(mut self) {
        let log = self.base_log.new(o!("component" => "SledAgentMain"));
        loop {
            tokio::select! {
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

    async fn handle_start_sled_agent_request(
        &mut self,
        request: StartSledAgentRequest,
        response_tx: oneshot::Sender<Result<SledAgentResponse, String>>,
        log: &Logger,
    ) {
        match &mut self.state {
            SledAgentState::Bootstrapping(sled_agent_started_tx) => {
                let request_id = request.body.id.into_untyped_uuid();

                // Extract from options to satisfy the borrow checker.
                // It is not possible for `start_sled_agent` to be cancelled
                // or fail in a safe, restartable manner. Therefore, for now,
                // we explicitly unwrap here, and panic on error below.
                //
                // See https://github.com/oxidecomputer/omicron/issues/4494
                let sled_agent_started_tx =
                    sled_agent_started_tx.take().unwrap();

                let response = match start_sled_agent(
                    &self.config,
                    request,
                    self.long_running_task_handles.clone(),
                    self.service_manager.clone(),
                    &self.base_log,
                    &log,
                )
                .await
                {
                    Ok(server) => {
                        // We've created sled-agent; we need to (possibly)
                        // reconfigure the switch zone, if we're a scrimlet, to
                        // give it our underlay network information.
                        sled_agent_started_tx
                            .send(server.sled_agent().clone())
                            .map_err(|_| ())
                            .expect("Failed to send to StorageMonitor");
                        self.state = SledAgentState::ServerStarted(server);
                        Ok(SledAgentResponse { id: request_id })
                    }
                    Err(err) => {
                        // This error is unrecoverable, and if returned we'd
                        // end up in maintenance mode anyway.
                        error!(log, "Failed to start sled agent: {err:#}");
                        panic!("Failed to start sled agent: {err:#}");
                    }
                };
                _ = response_tx.send(response);
            }
            SledAgentState::ServerStarted(server) => {
                info!(log, "Sled Agent already loaded");
                let initial = server.sled_agent().start_request();
                let response = if initial != &request {
                    Err(format!(
                        "Sled Agent already running: 
                        initital request = {:?}, 
                        current request: {:?}",
                        initial, request
                    ))
                } else {
                    Ok(SledAgentResponse {
                        // TODO-cleanup use typed UUIDs everywhere
                        id: server.id().into_untyped_uuid(),
                    })
                };

                _ = response_tx.send(response);
            }
        }
    }

    // Uninstall all oxide zones (except the switch zone)
    async fn uninstall_zones(&self) -> Result<(), BootstrapError> {
        const CONCURRENCY_CAP: usize = 32;
        futures::stream::iter(Zones::real_api().get().await?)
            .map(Ok::<_, anyhow::Error>)
            // Use for_each_concurrent_then_try to delete as much as possible.
            // We only return one error though -- hopefully that's enough to
            // signal to the caller that this failed.
            .for_each_concurrent_then_try(CONCURRENCY_CAP, |zone| async move {
                if zone.name() != "oxz_switch" {
                    Zones::real_api().halt_and_remove(zone.name()).await?;
                }
                Ok(())
            })
            .await
            .map_err(BootstrapError::Cleanup)?;
        Ok(())
    }

    async fn uninstall_sled_local_config(&self) -> Result<(), BootstrapError> {
        let config_dirs = self
            .long_running_task_handles
            .storage_manager
            .get_latest_disks()
            .await
            .all_m2_mountpoints(CONFIG_DATASET)
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
            .await
            .map_err(BootstrapError::ZfsDatasetsList)?;
        for dataset in &datasets {
            info!(log, "Removing dataset: {dataset}");
            zfs::Zfs::destroy_dataset(dataset).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::address::Ipv6Subnet;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::SledUuid;
    use sled_agent_types::sled::StartSledAgentRequestBody;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

    #[tokio::test]
    async fn start_sled_agent_request_serialization() {
        let logctx =
            test_setup_log("persistent_sled_agent_request_serialization");
        let log = &logctx.log;

        let request = StartSledAgentRequest {
            generation: 0,
            schema_version: 1,
            body: StartSledAgentRequestBody {
                id: SledUuid::new_v4(),
                rack_id: Uuid::new_v4(),
                use_trust_quorum: false,
                is_lrtq_learner: false,
                subnet: Ipv6Subnet::new(Ipv6Addr::LOCALHOST),
            },
        };

        let tempdir = camino_tempfile::Utf8TempDir::new().unwrap();
        let paths = vec![tempdir.path().join("test-file")];

        let mut ledger = Ledger::new_with(log, paths.clone(), request.clone());
        ledger.commit().await.expect("Failed to write to ledger");

        let ledger = Ledger::<StartSledAgentRequest>::new(log, paths)
            .await
            .expect("Failed to read request");

        assert!(&request == ledger.data(), "serialization round trip failed");
        logctx.cleanup_successful();
    }

    #[test]
    fn test_persistent_sled_agent_request_schema() {
        let schema = schemars::schema_for!(StartSledAgentRequest);
        expectorate::assert_contents(
            "../schema/start-sled-agent-request.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}
