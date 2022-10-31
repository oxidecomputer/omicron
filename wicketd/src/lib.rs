// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod config;
mod context;
mod http_entrypoints;
mod inventory;
mod mgs_manager;

pub use config::Config;
pub(crate) use context::ServerContext;
pub use inventory::RackV1Inventory;
pub(crate) use mgs_manager::{MgsHandle, MgsManager};

use dropshot::ConfigDropshot;
use slog::{debug, error, o, Drain};
use std::net::{SocketAddr, SocketAddrV6};

/// Run the OpenAPI generator for the API; which emits the OpenAPI spec
/// to stdout.
pub fn run_openapi() -> Result<(), String> {
    http_entrypoints::api()
        .openapi("Oxide Technician Port Control Service", "0.0.1")
        .description("API for use by the technician port TUI: wicket")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/// Command line arguments for wicketd
pub struct Args {
    pub address: SocketAddrV6,
}

/// Run an instance of the wicketd [Server]
pub async fn run_server(config: Config, args: Args) -> Result<(), String> {
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger("wicketd")
            .map_err(|msg| format!("initializing logger: {}", msg))?,
    );

    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        return Err(msg);
    } else {
        debug!(log, "registered DTrace probes");
    };

    let dropshot_config = ConfigDropshot {
        bind_address: SocketAddr::V6(args.address),
        request_body_max_bytes: 8 << 20, // 8 MiB
        ..Default::default()
    };

    let mgs_manager = MgsManager::new(&log, config.mgs_addr);
    let mgs_handle = mgs_manager.get_handle();
    tokio::spawn(async move {
        mgs_manager.run().await;
    });

    let server = dropshot::HttpServerStarter::new(
        &dropshot_config,
        http_entrypoints::api(),
        ServerContext { mgs_handle },
        &log.new(o!("component" => "dropshot (wicketd)")),
    )
    .map_err(|err| format!("initializing http server: {}", err))?
    .start();

    server.await
}
