// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod config;
mod context;
mod error;
mod management_switch;
pub mod metrics;
mod serial_console;

pub mod http_entrypoints; // TODO pub only for testing - is this right?

pub use config::Config;
pub use context::ServerContext;
pub use error::*;

use dropshot::ShutdownWaitFuture;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use gateway_sp_comms::InMemoryHostPhase2Provider;
pub use management_switch::LocationConfig;
pub use management_switch::LocationDescriptionConfig;
pub use management_switch::LocationDeterminationConfig;
pub use management_switch::ManagementSwitch;
pub use management_switch::RetryConfig;
pub use management_switch::SpIdentifier;
pub use management_switch::SpType;
pub use management_switch::SwitchConfig;
pub use management_switch::SwitchPortConfig;
pub use management_switch::SwitchPortDescription;
use omicron_common::FileKv;

use dropshot::ConfigDropshot;
use dropshot::HandlerTaskMode;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

pub struct MgsArguments {
    pub id: Uuid,
    pub addresses: Vec<SocketAddrV6>,
    pub rack_id: Option<Uuid>,
}

type HttpServer = dropshot::HttpServer<Arc<ServerContext>>;

pub struct Server {
    /// shared state used by API request handlers
    apictx: Arc<ServerContext>,
    /// dropshot servers for requests from nexus or wicketd, keyed by their bind
    /// address
    http_servers: HashMap<SocketAddrV6, HttpServer>,
    /// collection of `wait_for_shutdown` futures for each server inserted into
    /// `http_servers`
    all_servers_shutdown: FuturesUnordered<ShutdownWaitFuture>,
    default_request_body_max_bytes: usize,
    /// handle to the SP sensor metrics subsystem
    metrics: metrics::Metrics,
    log: Logger,
}

// Helper function to:
//
// 1. start a dropshot server on a particular address
// 2. insert it into an `http_servers` hash map
// 3. insert a shutdown handle into a an `all_servers_shutdown` collection
//
// which is used by both `start()` (on initial startup) and
// `adjust_dropshot_addresses` (which may need to start new servers if we're
// given new addresses).
fn start_dropshot_server(
    apictx: &Arc<ServerContext>,
    addr: SocketAddrV6,
    default_request_body_max_bytes: usize,
    http_servers: &mut HashMap<SocketAddrV6, HttpServer>,
    all_servers_shutdown: &FuturesUnordered<ShutdownWaitFuture>,
    log: &Logger,
) -> Result<(), String> {
    match http_servers.entry(addr) {
        Entry::Vacant(slot) => {
            let dropshot = ConfigDropshot {
                bind_address: SocketAddr::V6(addr),
                default_request_body_max_bytes,
                default_handler_task_mode: HandlerTaskMode::Detached,
                log_headers: vec![],
            };

            let http_server = dropshot::ServerBuilder::new(
                http_entrypoints::api(),
                Arc::clone(apictx),
                log.new(o!("component" => "dropshot")),
            )
            .config(dropshot)
            .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
                dropshot::ClientSpecifiesVersionInHeader::new(
                    omicron_common::api::VERSION_HEADER,
                    gateway_api::latest_version(),
                ),
            )))
            .start()
            .map_err(|error| {
                format!(
                    "initializing http server listening at {addr}: {}",
                    error
                )
            })?;

            all_servers_shutdown.push(http_server.wait_for_shutdown());
            slot.insert(http_server);
            Ok(())
        }
        Entry::Occupied(_) => {
            Err(format!("duplicate listening address: {addr}"))
        }
    }
}

impl Server {
    /// Start a gateway server.
    pub async fn start(
        config: Config,
        args: MgsArguments,
        log: Logger,
    ) -> Result<Server, String> {
        if args.addresses.is_empty() {
            return Err("Cannot start server with no addresses".to_string());
        }

        let log = log.new(o!("name" => args.id.to_string()));
        info!(log, "setting up gateway server");

        match gateway_sp_comms::register_probes() {
            Ok(_) => debug!(log, "successfully registered DTrace USDT probes"),
            Err(err) => {
                warn!(
                    log, "failed to register DTrace USDT probes";
                    InlineErrorChain::new(&err),
                );
            }
        }

        let host_phase2_provider =
            Arc::new(InMemoryHostPhase2Provider::with_capacity(
                config.host_phase2_recovery_image_cache_max_images,
            ));
        let apictx = ServerContext::new(
            args.id,
            host_phase2_provider,
            config.switch,
            args.rack_id,
            &log,
        )
        .await
        .map_err(|error| format!("initializing server context: {}", error))?;

        let mut http_servers = HashMap::with_capacity(args.addresses.len());
        let all_servers_shutdown = FuturesUnordered::new();

        let metrics =
            metrics::Metrics::new(&log, &args, config.metrics, apictx.clone());

        for addr in args.addresses {
            start_dropshot_server(
                &apictx,
                addr,
                config.dropshot.default_request_body_max_bytes,
                &mut http_servers,
                &all_servers_shutdown,
                &log,
            )?;
        }

        Ok(Server {
            apictx,
            http_servers,
            all_servers_shutdown,
            default_request_body_max_bytes: config
                .dropshot
                .default_request_body_max_bytes,
            metrics,
            log,
        })
    }

    /// Get a handle to our [`ManagementSwitch`].
    pub fn management_switch(&self) -> &ManagementSwitch {
        &self.apictx.mgmt_switch
    }

    /// Get a handle to the dropshot server listening on `address`, if one
    /// exists.
    ///
    /// This exists for integration tests; in general clients of this class
    /// should not interact with specific dropshot server instances.
    pub fn dropshot_server_for_address(
        &self,
        address: SocketAddrV6,
    ) -> Option<&HttpServer> {
        self.http_servers.get(&address)
    }

    /// Close all running dropshot servers.
    pub async fn close(self) -> Result<(), String> {
        for (_, server) in self.http_servers {
            server.close().await?;
        }
        Ok(())
    }

    /// Wait for the server to shut down
    ///
    /// Note that this doesn't initiate a graceful shutdown, so if you call this
    /// immediately after calling `start()`, the program will block indefinitely
    /// or until something else initiates a graceful shutdown.
    pub async fn wait_for_finish(&mut self) -> Result<(), String> {
        while let Some(result) = self.all_servers_shutdown.next().await {
            result?;
        }
        Ok(())
    }

    /// Adjust dropshot bind addresses on which this server is listening.
    ///
    /// Addresses fall into three categories:
    ///
    /// 1. Present in `addresses` and we're already listening on the address:
    ///    the running dropshot server is left unchanged.
    /// 2. Present in `addresses` but we're not already listening on the
    ///    address: we start a new dropshot server (sharing the same server
    ///    context) on the address.
    /// 3. We're listening on it but it's not present in `addresses`: we shut
    ///    the server down. This method blocks waiting for these shutdowns to
    ///    complete.
    ///
    /// This method fails if any operation required for 2 or 3 fails.
    pub async fn adjust_dropshot_addresses(
        &mut self,
        addresses: &[SocketAddrV6],
    ) -> Result<(), String> {
        if addresses.is_empty() {
            return Err(
                "Cannot reconfigure server with no addresses".to_string()
            );
        }

        let mut http_servers = HashMap::with_capacity(addresses.len());

        // For each address in `addresses`, either start a new server or move
        // the existing one from `self.http_servers` into `http_servers`.
        for &addr in addresses {
            if let Some(existing) = self.http_servers.remove(&addr) {
                info!(
                    self.apictx.log,
                    "adjusting dropshot addresses; keeping existing addr";
                    "addr" => %addr,
                );
                http_servers.insert(addr, existing);
            } else {
                info!(
                    self.apictx.log,
                    "adjusting dropshot addresses; starting new server";
                    "addr" => %addr,
                );
                start_dropshot_server(
                    &self.apictx,
                    addr,
                    self.default_request_body_max_bytes,
                    &mut http_servers,
                    &self.all_servers_shutdown,
                    &self.log,
                )?;
            }
        }

        // `http_servers` now contains all the servers we want for `addresses`.
        // Swap it into `self.http_servers`, and shut down any servers that
        // remain.
        mem::swap(&mut http_servers, &mut self.http_servers);

        for (addr, server) in http_servers {
            info!(
                self.apictx.log,
                "adjusting dropshot addresses; stopping old server";
                "addr" => %addr,
            );
            server.close().await?;
        }

        self.metrics.update_server_addrs(addresses).await;

        Ok(())
    }

    /// The rack_id will be set on a refresh of the SMF property when the sled
    /// agent starts.
    pub fn set_rack_id(&mut self, rack_id: Option<Uuid>) {
        if let Some(rack_id) = rack_id {
            let val = self.apictx.rack_id.get_or_init(|| rack_id);
            if *val != rack_id {
                error!(
                    self.apictx.log,
                    "Ignoring attempted change to rack ID";
                    "current_rack_id" => %val,
                    "ignored_new_rack_id" => %rack_id);
            } else {
                info!(self.apictx.log, "Set rack_id"; "rack_id" => %rack_id);
                self.metrics.set_rack_id(rack_id);
            }
        } else {
            warn!(self.apictx.log, "SMF refresh called without a rack id");
        }
    }
}

/// Start an instance of the [Server].
pub async fn start_server(
    config: Config,
    args: MgsArguments,
) -> Result<Server, String> {
    use slog::Drain;
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger("gateway")
            .map_err(|message| format!("initializing logger: {}", message))?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
    if let slog_dtrace::ProbeRegistration::Failed(err) = registration {
        error!(log, "failed to register DTrace probes"; "err" => &err);
        return Err(format!("failed to register DTrace probes: {err}"));
    } else {
        debug!(log, "registered DTrace probes");
    }
    let server = Server::start(config, args, log).await?;
    Ok(server)
}
