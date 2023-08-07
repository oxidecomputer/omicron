// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Server API for bootstrap-related functionality.

use super::agent::Agent;
use super::config::Config;
use crate::bootstrap::http_entrypoints::api as http_api;
use crate::bootstrap::maghemite;
use crate::bootstrap::sprockets_server::SprocketsServer;
use crate::config::Config as SledConfig;
use crate::config::ConfigError;
use ddm_admin_client::DdmError;
use illumos_utils::dladm;
use illumos_utils::zfs;
use illumos_utils::zone;
use omicron_common::ledger;
use omicron_common::FileKv;
use sled_hardware::underlay;
use slog::Drain;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;

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
    pub async fn start(
        config: Config,
        sled_config: SledConfig,
    ) -> Result<Self, String> {
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
