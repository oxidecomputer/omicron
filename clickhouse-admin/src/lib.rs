// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use context::{ServerContext, SingleServerContext};
use dropshot::HttpServer;
use omicron_common::FileKv;
use slog::{debug, error, Drain};
use slog_dtrace::ProbeRegistration;
use slog_error_chain::SlogInlineError;
use std::io;
use std::net::SocketAddrV6;
use std::sync::Arc;

mod clickhouse_cli;
mod clickward;
mod config;
mod context;
mod http_entrypoints;

pub use clickhouse_cli::ClickhouseCli;
pub use clickward::Clickward;
pub use config::Config;

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum StartError {
    #[error("failed to initialize logger")]
    InitializeLogger(#[source] io::Error),
    #[error("failed to register dtrace probes: {0}")]
    RegisterDtraceProbes(String),
    #[error("failed to initialize HTTP server")]
    InitializeHttpServer(#[source] dropshot::BuildError),
}

/// Start the dropshot server for `clickhouse-admin-server` which
/// manages clickhouse replica servers.
pub async fn start_server_admin_server(
    clickward: Clickward,
    binary_path: Utf8PathBuf,
    listen_address: SocketAddrV6,
    server_config: Config,
) -> Result<HttpServer<Arc<ServerContext>>, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        server_config
            .log
            .to_logger("clickhouse-admin-server")
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

    let clickhouse_cli = ClickhouseCli::new(
        binary_path,
        listen_address,
        log.new(slog::o!("component" => "ClickhouseCli")),
    );
    let context = ServerContext::new(clickward, clickhouse_cli);
    dropshot::ServerBuilder::new(
        http_entrypoints::clickhouse_admin_server_api(),
        Arc::new(context),
        log.new(slog::o!("component" => "dropshot")),
    )
    .config(server_config.dropshot)
    .start()
    .map_err(StartError::InitializeHttpServer)
}

/// Start the dropshot server for `clickhouse-admin-server` which
/// manages clickhouse replica servers.
pub async fn start_keeper_admin_server(
    clickward: Clickward,
    binary_path: Utf8PathBuf,
    listen_address: SocketAddrV6,
    server_config: Config,
) -> Result<HttpServer<Arc<ServerContext>>, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        server_config
            .log
            .to_logger("clickhouse-admin-keeper")
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

    let clickhouse_cli = ClickhouseCli::new(
        binary_path,
        listen_address,
        log.new(slog::o!("component" => "ClickhouseCli")),
    );
    let context = ServerContext::new(clickward, clickhouse_cli);
    dropshot::ServerBuilder::new(
        http_entrypoints::clickhouse_admin_keeper_api(),
        Arc::new(context),
        log.new(slog::o!("component" => "dropshot")),
    )
    .config(server_config.dropshot)
    .start()
    .map_err(StartError::InitializeHttpServer)
}

/// Start the dropshot server for `clickhouse-admin-single` which
/// manages a single-node ClickHouse database.
pub async fn start_single_admin_server(
    binary_path: Utf8PathBuf,
    listen_address: SocketAddrV6,
    server_config: Config,
) -> Result<HttpServer<Arc<SingleServerContext>>, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        server_config
            .log
            .to_logger("clickhouse-admin-single")
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

    let clickhouse_cli = ClickhouseCli::new(
        binary_path,
        listen_address,
        log.new(slog::o!("component" => "ClickhouseCli")),
    );
    let context = SingleServerContext::new(clickhouse_cli);
    dropshot::ServerBuilder::new(
        http_entrypoints::clickhouse_admin_single_api(),
        Arc::new(context),
        log.new(slog::o!("component" => "dropshot")),
    )
    .config(server_config.dropshot)
    .start()
    .map_err(StartError::InitializeHttpServer)
}
