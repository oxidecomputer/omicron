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
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
pub use management_switch::LocationConfig;
pub use management_switch::LocationDeterminationConfig;
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
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::pin::Pin;
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
type HttpServerShutdownFut =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

pub struct Server {
    /// shared state used by API request handlers
    apictx: Arc<ServerContext>,
    /// dropshot servers for requests from nexus or wicketd, keyed by their bind
    /// address
    http_servers: HashMap<SocketAddrV6, HttpServer>,
    /// collection of `wait_for_shutdown` futures for each server inserted into
    /// `http_servers`
    all_servers_shutdown: FuturesUnordered<HttpServerShutdownFut>,
}

impl Server {
    /// Start a gateway server.
    ///
    /// # Panics
    ///
    /// Panics if `args.addresses` is empty (i.e., we are not given any
    /// addresses on which to bind dropshot servers).
    pub async fn start(
        config: Config,
        args: MgsArguments,
        _rack_id: Uuid,
        log: &Logger,
    ) -> Result<Server, String> {
        assert!(
            !args.addresses.is_empty(),
            "Cannot start server with no addresses"
        );

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
            let dropshot = ConfigDropshot {
                bind_address: SocketAddr::V6(addr),
                request_body_max_bytes: config.dropshot.request_body_max_bytes,
                ..Default::default()
            };
            let http_server_starter = dropshot::HttpServerStarter::new(
                &dropshot,
                http_entrypoints::api(),
                Arc::clone(&apictx),
                &log.new(o!("component" => "dropshot")),
            )
            .map_err(|error| format!("initializing http server: {}", error))?;

            let http_server = http_server_starter.start();

            // TODO Remove boxed() via dropshot PR?
            all_servers_shutdown.push(http_server.wait_for_shutdown().boxed());

            if http_servers.insert(addr, http_server).is_some() {
                return Err(format!("duplicate listening address: {addr}"));
            }
        }

        Ok(Server { apictx, http_servers, all_servers_shutdown })
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

    // TODO does MGS register itself with oximeter?
    // Register the Nexus server as a metric producer with `oximeter.
    // pub async fn register_as_producer(&self) {
    // self.apictx
    // .nexus
    // .register_as_producer(self.http_server_internal.local_addr())
    // .await;
    // }
}

/// Run an instance of the [Server].
pub async fn run_server(
    config: Config,
    args: MgsArguments,
) -> Result<(), String> {
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
    let mut server = Server::start(config, args, rack_id, &log).await?;
    // server.register_as_producer().await;
    server.wait_for_finish().await
}
