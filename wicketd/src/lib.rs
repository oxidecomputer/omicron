// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod artifacts;
mod bootstrap_addrs;
mod config;
mod context;
mod helpers;
mod http_entrypoints;
mod installinator_progress;
pub mod mgs;
mod nexus_proxy;
mod preflight_check;
mod rss_config;
mod transceivers;
mod update_tracker;

use anyhow::{Context, Result, anyhow, bail};
use artifacts::{
    WicketdArtifactStore, WicketdInstallinatorApiImpl,
    WicketdInstallinatorContext,
};
use bootstrap_addrs::BootstrapPeers;
pub use config::Config;
pub(crate) use context::ServerContext;
use display_error_chain::DisplayErrorChain;
use dropshot::{ConfigDropshot, HandlerTaskMode, HttpServer};
pub use installinator_progress::{IprUpdateTracker, RunningUpdateState};
use internal_dns_resolver::Resolver;
use mgs::make_mgs_client;
pub(crate) use mgs::{MgsHandle, MgsManager};
use nexus_proxy::NexusTcpProxy;
use omicron_common::FileKv;
use omicron_common::address::{AZ_PREFIX, Ipv6Subnet};
use preflight_check::PreflightCheckerHandler;
use sled_hardware_types::Baseboard;
use slog::{Drain, debug, error, o};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use std::{
    net::{SocketAddr, SocketAddrV6},
    sync::Arc,
};
use transceivers::Manager as TransceiverManager;
pub use update_tracker::{StartUpdateError, UpdateTracker};

/// Command line arguments for wicketd
pub struct Args {
    pub address: SocketAddrV6,
    pub artifact_address: SocketAddrV6,
    pub mgs_address: SocketAddrV6,
    pub nexus_proxy_address: SocketAddrV6,
    pub baseboard: Option<Baseboard>,
    pub rack_subnet: Option<Ipv6Subnet<AZ_PREFIX>>,
}

pub struct SmfConfigValues {
    pub address: SocketAddrV6,
    pub rack_subnet: Option<Ipv6Subnet<AZ_PREFIX>>,
}

impl SmfConfigValues {
    #[cfg(target_os = "illumos")]
    pub fn read_current() -> Result<Self> {
        use illumos_utils::scf::ScfHandle;

        const CONFIG_PG: &str = "config";
        const PROP_RACK_SUBNET: &str = "rack-subnet";
        const PROP_ADDRESS: &str = "address";

        let scf = ScfHandle::new()?;
        let instance = scf.self_instance()?;
        let snapshot = instance.running_snapshot()?;
        let config = snapshot.property_group(CONFIG_PG)?;

        let rack_subnet = config.value_as_string(PROP_RACK_SUBNET)?;

        let rack_subnet = if rack_subnet == "unknown" {
            None
        } else {
            let addr = rack_subnet.parse().with_context(|| {
                format!(
                    "failed to parse {CONFIG_PG}/{PROP_RACK_SUBNET} \
                     value {rack_subnet:?} as an IP address"
                )
            })?;
            Some(Ipv6Subnet::new(addr))
        };

        let address = {
            let address = config.value_as_string(PROP_ADDRESS)?;
            address.parse().with_context(|| {
                format!(
                    "failed to parse {CONFIG_PG}/{PROP_ADDRESS} \
                     value {address:?} as a socket address"
                )
            })?
        };

        Ok(Self { address, rack_subnet })
    }

    #[cfg(not(target_os = "illumos"))]
    pub fn read_current() -> Result<Self> {
        Err(anyhow!("reading SMF config only available on illumos"))
    }
}

pub struct Server {
    pub wicketd_server: HttpServer<ServerContext>,
    pub installinator_server: HttpServer<WicketdInstallinatorContext>,
    pub artifact_store: WicketdArtifactStore,
    pub update_tracker: Arc<UpdateTracker>,
    pub ipr_update_tracker: IprUpdateTracker,
    nexus_tcp_proxy: NexusTcpProxy,
}

impl Server {
    /// Run an instance of the wicketd server
    pub async fn start(log: slog::Logger, args: Args) -> anyhow::Result<Self> {
        let (drain, registration) = slog_dtrace::with_drain(log);

        let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            bail!(msg);
        } else {
            debug!(log, "registered DTrace probes");
        };

        let dropshot_config = ConfigDropshot {
            bind_address: SocketAddr::V6(args.address),
            default_request_body_max_bytes: 8 * 1024 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
        };

        let mgs_manager = MgsManager::new(&log, args.mgs_address);
        let mgs_handle = mgs_manager.get_handle();
        tokio::spawn(async move {
            mgs_manager.run().await;
        });

        let transceiver_manager = TransceiverManager::new(&log);
        let transceiver_handle = transceiver_manager.get_handle();
        tokio::spawn(async move {
            transceiver_manager.run().await;
        });

        let (ipr_artifact, ipr_update_tracker) =
            crate::installinator_progress::new(&log);

        let store = WicketdArtifactStore::new(&log);
        let update_tracker = Arc::new(UpdateTracker::new(
            args.mgs_address,
            &log,
            store.clone(),
            ipr_update_tracker.clone(),
        ));

        let bootstrap_peers = BootstrapPeers::new(&log);
        let internal_dns_resolver = args
            .rack_subnet
            .map(|addr| {
                Resolver::new_from_subnet(
                    log.new(o!("component" => "InternalDnsResolver")),
                    addr,
                )
                .map_err(|err| {
                    anyhow!(err)
                        .context("Could not create internal DNS resolver")
                })
            })
            .transpose()?;

        let internal_dns_resolver = Arc::new(Mutex::new(internal_dns_resolver));
        let nexus_tcp_proxy = NexusTcpProxy::start(
            args.nexus_proxy_address,
            Arc::clone(&internal_dns_resolver),
            &log,
        )
        .await
        .map_err(|err| {
            anyhow!(err).context("failed to start Nexus TCP proxy")
        })?;

        let wicketd_server = {
            let ds_log = log.new(o!("component" => "dropshot (wicketd)"));
            let mgs_client = make_mgs_client(log.clone(), args.mgs_address);
            dropshot::ServerBuilder::new(
                http_entrypoints::api(),
                ServerContext {
                    bind_address: args.address,
                    mgs_handle,
                    mgs_client,
                    transceiver_handle,
                    log: log.clone(),
                    local_switch_id: OnceLock::new(),
                    bootstrap_peers,
                    update_tracker: update_tracker.clone(),
                    baseboard: args.baseboard,
                    rss_config: Default::default(),
                    preflight_checker: PreflightCheckerHandler::new(&log),
                    internal_dns_resolver,
                },
                ds_log,
            )
            .config(dropshot_config)
            .start()
            .map_err(|err| anyhow!(err).context("initializing http server"))?
        };

        let installinator_server = {
            let installinator_config = installinator_api::default_config(
                SocketAddr::V6(args.artifact_address),
            );
            let api_description =
                installinator_api::installinator_api_mod::api_description::<
                    WicketdInstallinatorApiImpl,
                >()?;

            dropshot::ServerBuilder::new(
                api_description,
                WicketdInstallinatorContext::new(
                    &log,
                    store.clone(),
                    ipr_artifact,
                ),
                log,
            )
            .config(installinator_config)
            .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                dropshot::ClientSpecifiesVersionInHeader::new(
                    omicron_common::api::VERSION_HEADER,
                    installinator_api::latest_version(),
                ),
            )))
            .start()
            .map_err(|err| {
                anyhow!(err)
                    .context("failed to create installinator artifact server")
            })?
        };

        Ok(Self {
            wicketd_server,
            installinator_server,
            artifact_store: store,
            update_tracker,
            ipr_update_tracker,
            nexus_tcp_proxy,
        })
    }

    /// Close all running dropshot servers.
    pub async fn close(mut self) -> Result<()> {
        self.wicketd_server.close().await.map_err(|error| {
            anyhow!("error closing wicketd server: {error}")
        })?;
        self.installinator_server.close().await.map_err(|error| {
            anyhow!("error closing artifact server: {error}")
        })?;
        self.nexus_tcp_proxy.shutdown();
        Ok(())
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        // Both servers should keep running indefinitely unless close() is
        // called. Bail if either server exits.
        tokio::select! {
            res = self.wicketd_server => {
                match res {
                    Ok(()) => Err("wicketd server exited unexpectedly".to_owned()),
                    Err(err) => Err(format!("running wicketd server: {err}")),
                }
            }
            res = self.installinator_server => {
                match res {
                    Ok(()) => Err("artifact server exited unexpectedly".to_owned()),
                    // The artifact server returns an anyhow::Error, which has a
                    // `Debug` impl that prints out the chain of errors.
                    Err(err) => Err(format!("running artifact server: {err:?}")),
                }
            }
        }
    }

    /// Instruct a running server at the specified address to reload its config
    /// parameters
    pub async fn refresh_config(
        log: slog::Logger,
        address: SocketAddrV6,
    ) -> Result<()> {
        // It's possible we're being told to refresh a server's config before
        // it's ready to receive such a request, so we'll give it a healthy
        // amount of time before we give up: we'll set a client timeout and also
        // retry a few times. See
        // https://github.com/oxidecomputer/omicron/issues/4604.
        const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);
        const SLEEP_BETWEEN_RETRIES: Duration = Duration::from_secs(10);
        const NUM_RETRIES: usize = 3;

        let client = reqwest::Client::builder()
            .connect_timeout(CLIENT_TIMEOUT)
            .timeout(CLIENT_TIMEOUT)
            .build()
            .context("failed to construct reqwest Client")?;

        let client = wicketd_client::Client::new_with_client(
            &format!("http://{address}"),
            client,
            log,
        );
        let log = client.inner();

        let mut attempt = 0;
        loop {
            attempt += 1;

            // If we succeed, we're done.
            let Err(err) = client.post_reload_config().await else {
                return Ok(());
            };

            // If we failed, either warn+sleep and try again, or fail.
            if attempt < NUM_RETRIES {
                slog::warn!(
                    log,
                    "failed to refresh wicketd config \
                     (attempt {attempt} of {NUM_RETRIES}); \
                     will retry after {CLIENT_TIMEOUT:?}";
                    "err" => %DisplayErrorChain::new(&err),
                );
                tokio::time::sleep(SLEEP_BETWEEN_RETRIES).await;
            } else {
                slog::error!(
                    log,
                    "failed to refresh wicketd config \
                     (tried {NUM_RETRIES} times)";
                    "err" => %DisplayErrorChain::new(&err),
                );
                return Err(err).context("failed to contact wicketd");
            }
        }
    }
}
