use context::ServerContext;
use omicron_common::FileKv;
use slog::Drain;
use slog::debug;
use slog::error;
use slog_dtrace::ProbeRegistration;
use slog_error_chain::SlogInlineError;
use std::io;
use std::sync::Arc;

mod config;
mod context;
mod http_entrypoints;

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

pub type Server = dropshot::HttpServer<Arc<ServerContext>>;

/// Start the dropshot server
pub async fn start_server(server_config: Config) -> Result<Server, StartError> {
    let (drain, registration) = slog_dtrace::with_drain(
        server_config
            .log
            .to_logger("ntp-admin")
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

    let context =
        ServerContext::new(log.new(slog::o!("component" => "ServerContext")));
    dropshot::ServerBuilder::new(
        http_entrypoints::api(),
        Arc::new(context),
        log.new(slog::o!("component" => "dropshot")),
    )
    .config(server_config.dropshot)
    .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
        dropshot::ClientSpecifiesVersionInHeader::new(
            omicron_common::api::VERSION_HEADER,
            update_admin_api::VERSION_INITIAL,
        ),
    )))
    .start()
    .map_err(StartError::InitializeHttpServer)
}
