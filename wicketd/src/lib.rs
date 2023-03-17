// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod artifacts;
mod config;
mod context;
mod http_entrypoints;
mod installinator_progress;
mod inventory;
pub mod mgs;
mod update_events;
mod update_tracker;

use anyhow::{anyhow, Result};
use artifacts::{WicketdArtifactServer, WicketdArtifactStore};
pub use config::Config;
pub(crate) use context::ServerContext;
pub use inventory::{RackV1Inventory, SpInventory};
use mgs::make_mgs_client;
pub(crate) use mgs::{MgsHandle, MgsManager};

use dropshot::{ConfigDropshot, HttpServer};
use slog::{debug, error, o, Drain};
use std::net::{SocketAddr, SocketAddrV6};
use update_tracker::UpdateTracker;

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

pub struct Server {
    pub wicketd_server: HttpServer<ServerContext>,
    pub artifact_server: HttpServer<installinator_artifactd::ServerContext>,
}

impl Server {
    /// Run an instance of the wicketd server
    pub async fn start(log: slog::Logger, args: Args) -> Result<Self, String> {
        let (drain, registration) = slog_dtrace::with_drain(log);

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

        let (ipr_artifact, ipr_update_tracker) =
            crate::installinator_progress::new(&log);

        let store = WicketdArtifactStore::new(&log);
        let update_tracker =
            UpdateTracker::new(args.mgs_address, &log, ipr_update_tracker);

        let wicketd_server = {
            let log = log.new(o!("component" => "dropshot (wicketd)"));
            let mgs_client = make_mgs_client(log.clone(), args.mgs_address);
            dropshot::HttpServerStarter::new(
                &dropshot_config,
                http_entrypoints::api(),
                ServerContext {
                    mgs_handle,
                    mgs_client,
                    artifact_store: store.clone(),
                    update_tracker,
                },
                &log,
            )
            .map_err(|err| format!("initializing http server: {}", err))?
            .start()
        };

        let server = WicketdArtifactServer::new(&log, store, ipr_artifact);
        let artifact_server = installinator_artifactd::ArtifactServer::new(
            server,
            args.artifact_address,
            &log,
        )
        .start()
        .map_err(|error| {
            format!("failed to start artifact server: {error:?}")
        })?;

        Ok(Self { wicketd_server, artifact_server })
    }

    /// Close all running dropshot servers.
    pub async fn close(self) -> Result<()> {
        self.wicketd_server.close().await.map_err(|error| {
            anyhow!("error closing wicketd server: {error}")
        })?;
        self.artifact_server.close().await.map_err(|error| {
            anyhow!("error closing artifact server: {error}")
        })?;
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
            res = self.artifact_server => {
                match res {
                    Ok(()) => Err("artifact server exited unexpectedly".to_owned()),
                    // The artifact server returns an anyhow::Error, which has a `Debug` impl that
                    // prints out the chain of errors.
                    Err(err) => Err(format!("running artifact server: {err:?}")),
                }
            }
        }
    }
}
