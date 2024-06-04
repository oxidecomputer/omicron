// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use context::ServerContext;
use omicron_common::FileKv;
use slog::debug;
use slog::error;
use slog::Drain;
use slog_dtrace::ProbeRegistration;
use slog_error_chain::SlogInlineError;
use std::error::Error;
use std::io;
use std::sync::Arc;

mod cockroach_cli;
mod config;
mod context;
mod http_entrypoints;

pub use cockroach_cli::CockroachCli;
pub use cockroach_cli::CockroachCliError;
pub use config::Config;

/// Run the OpenAPI generator for the API; this emits the OpenAPI spec to
/// stdout.
pub fn run_openapi() -> Result<(), String> {
    http_entrypoints::api()
        .openapi("Oxide CockroachDb Cluster Admin API", "0.0.1")
        .description(
            "API for interacting with the Oxide \
             control plane's CockroachDb cluster",
        )
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum StartError {
    #[error("failed to initialize logger")]
    InitializeLogger(#[source] io::Error),
    #[error("failed to register dtrace probes: {0}")]
    RegisterDtraceProbes(String),
    #[error("failed to initialize HTTP server")]
    InitializeHttpServer(#[source] Box<dyn Error + Send + Sync>),
}

pub type Server = dropshot::HttpServer<Arc<ServerContext>>;

/// Start the dropshot server
pub async fn start_server(
    cockroach_cli: CockroachCli,
    server_config: Config,
) -> Result<Server, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        server_config
            .log
            .to_logger("cockroach-admin")
            .map_err(StartError::InitializeLogger)?,
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!(FileKv));
    match registration {
        ProbeRegistration::Success => {
            debug!(log, "registered DTrace probes");
        }
        ProbeRegistration::Failed(err) => {
            let err = StartError::RegisterDtraceProbes(err);
            error!(log, "failed to register DTrace probes"; &err);
            return Err(err);
        }
    }

    let context = ServerContext::new(
        cockroach_cli,
        log.new(slog::o!("component" => "ServerContext")),
    );
    let http_server_starter = dropshot::HttpServerStarter::new(
        &server_config.dropshot,
        http_entrypoints::api(),
        Arc::new(context),
        &log.new(slog::o!("component" => "dropshot")),
    )
    .map_err(StartError::InitializeHttpServer)?;

    Ok(http_server_starter.start())
}
