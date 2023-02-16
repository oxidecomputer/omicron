// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod artifacts;
mod config;
mod context;
mod http_entrypoints;
mod inventory;
mod mgs;

use artifacts::WicketdArtifactStore;
pub use config::Config;
pub(crate) use context::ServerContext;
pub use inventory::{RackV1Inventory, SpInventory};
pub(crate) use mgs::{MgsHandle, MgsManager};

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
    pub artifact_address: SocketAddrV6,
    pub mgs_address: SocketAddrV6,
}

/// Run an instance of the wicketd server
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
        // The maximum request size is set to 4 GB -- artifacts can be large and there's currently
        // no way to set a larger request size for some endpoints.
        request_body_max_bytes: 4 << 30,
        ..Default::default()
    };

    let mgs_manager = MgsManager::new(&log, args.mgs_address);
    let mgs_handle = mgs_manager.get_handle();
    tokio::spawn(async move {
        mgs_manager.run().await;
    });

    let store = WicketdArtifactStore::new(&log);

    let wicketd_server_fut = dropshot::HttpServerStarter::new(
        &dropshot_config,
        http_entrypoints::api(),
        ServerContext { mgs_handle, artifact_store: store.clone() },
        &log.new(o!("component" => "dropshot (wicketd)")),
    )
    .map_err(|err| format!("initializing http server: {}", err))?
    .start();

    let artifact_server_fut = installinator_artifactd::ArtifactServer::new(
        store,
        args.artifact_address,
        &log,
    )
    .start();

    // Both servers should keep running indefinitely. Bail if either server exits, whether as Ok or
    // as Err.
    tokio::select! {
        res = wicketd_server_fut => {
            match res {
                Ok(()) => Err("wicketd server exited unexpectedly".to_owned()),
                Err(err) => Err(format!("running wicketd server: {err}")),
            }
        }
        res = artifact_server_fut => {
            match res {
                Ok(()) => Err("artifact server exited unexpectedly".to_owned()),
                // The artifact server returns an anyhow::Error, which has a `Debug` impl that
                // prints out the chain of errors.
                Err(err) => Err(format!("running artifact server: {err:?}")),
            }
        }
    }
}
