// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod config;
mod context;
mod error;
mod management_switch;
mod serial_console;

pub mod http_entrypoints; // TODO pub only for testing - is this right?

pub use config::Config;
pub use context::ServerContext;
use dropshot::ShutdownWaitFuture;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
pub use management_switch::LocationConfig;
pub use management_switch::LocationDeterminationConfig;
pub use management_switch::ManagementSwitch;
pub use management_switch::SpType;
pub use management_switch::SwitchPortConfig;
pub use management_switch::SwitchPortDescription;

use dropshot::ConfigDropshot;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

/// Run the OpenAPI generator for the API; which emits the OpenAPI spec
/// to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_entrypoints::api()
        .openapi("Oxide Management Gateway Service API", "0.0.1")
        .description("API for interacting with the Oxide control plane's gateway service")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

pub struct MgsArguments {
    pub id: Uuid,
    pub addresses: Vec<SocketAddrV6>,
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
    request_body_max_bytes: usize,
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
    request_body_max_bytes: usize,
    http_servers: &mut HashMap<SocketAddrV6, HttpServer>,
    all_servers_shutdown: &FuturesUnordered<ShutdownWaitFuture>,
    log: &Logger,
) -> Result<(), String> {
    let dropshot = ConfigDropshot {
        bind_address: SocketAddr::V6(addr),
        request_body_max_bytes,
        ..Default::default()
    };
    let http_server_starter = dropshot::HttpServerStarter::new(
        &dropshot,
        http_entrypoints::api(),
        Arc::clone(&apictx),
        &log.new(o!("component" => "dropshot")),
    )
    .map_err(|error| format!("initializing http server: {}", error))?;

    match http_servers.entry(addr) {
        Entry::Vacant(slot) => {
            let http_server = http_server_starter.start();
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
        _rack_id: Uuid,
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
                warn!(log, "failed to register DTrace USDT probes: {}", err);
            }
        }

        let apictx =
            ServerContext::new(config.switch, &log).await.map_err(|error| {
                format!("initializing server context: {}", error)
            })?;

        let mut http_servers = HashMap::with_capacity(args.addresses.len());
        let all_servers_shutdown = FuturesUnordered::new();

        for addr in args.addresses {
            start_dropshot_server(
                &apictx,
                addr,
                config.dropshot.request_body_max_bytes,
                &mut http_servers,
                &all_servers_shutdown,
                &log,
            )?;
        }

        Ok(Server {
            apictx,
            http_servers,
            all_servers_shutdown,
            request_body_max_bytes: config.dropshot.request_body_max_bytes,
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
                    self.request_body_max_bytes,
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

        Ok(())
    }

    // TODO does MGS register itself with oximeter?
    // Register the Nexus server as a metric producer with `oximeter.
    // pub async fn register_as_producer(&self) {
    // self.apictx
    // .nexus
    // .register_as_producer(self.http_server_internal.local_addr())
    // .await;
    // }
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
    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    let rack_id = Uuid::new_v4();
    let server = Server::start(config, args, rack_id, log).await?;
    // server.register_as_producer().await;
    Ok(server)
}
