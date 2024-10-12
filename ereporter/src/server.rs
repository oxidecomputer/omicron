use crate::buffer;
use crate::EreportData;
use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    PaginationParams, Query, RequestContext, ResultsPage, WhichPage,
};
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use omicron_common::FileKv;
use slog::debug;
use slog::error;
use slog::warn;
use slog::Drain;
use slog::Logger;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// Our public interface depends directly or indirectly on these types; we
// export them so that consumers need not depend on dropshot themselves and
// to simplify how we stage incompatible upgrades.
pub use dropshot::ConfigLogging;
pub use dropshot::ConfigLoggingIfExists;
pub use dropshot::ConfigLoggingLevel;

pub struct ServerStarter {
    config: Config,
    ctx: ServerContext,
    ereports: mpsc::Receiver<EreportData>,
    requests: mpsc::Receiver<buffer::ServerReq>,
    dns_resolver: Option<Resolver>,
}

pub struct RunningServer {
    // Handle to the buffer task.
    buffer_task: tokio::task::JoinHandle<()>,
    // TODO(eliza): hang onto the running dropshot stuff.
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ReporterIdentity {
    /// UUID of the reporter.
    pub id: Uuid,
    /// The address to listen for ereport collection requests on.
    pub address: SocketAddr,
}

pub struct Config {
    pub reporter: ReporterIdentity,
    /// How to discover the Nexus API to register the reporter.
    pub registration_address: Option<SocketAddr>,
    /// The maximum size of Dropshot requests.
    pub request_body_max_bytes: usize,
    /// The maximum number of ereports to buffer before exerting backpressure on producers.
    pub buffer_capacity: usize,
    pub request_channel_capacity: usize,
    /// The logging configuration or actual logger used to emit logs.
    pub log: LogConfig,
}

/// Either configuration for building a logger, or an actual logger already
/// instantiated.
///
/// This can be used to start a [`Server`] with a new logger or a child of a
/// parent logger if desired.
#[derive(Debug, Clone)]
pub enum LogConfig {
    /// Configuration for building a new logger.
    Config(ConfigLogging),
    /// An explicit logger to use.
    Logger(Logger),
}

/// How to discover Nexus' IP for registration.
#[derive(Clone)]
enum NexusDiscovery {
    /// Use the provided socket address for the Nexus API.
    Addr(SocketAddr),
    /// Discover Nexus from internal DNS
    Dns(Resolver),
}

#[derive(Clone)]
struct ServerContext {
    tx: mpsc::Sender<buffer::ServerReq>,
}

struct EreporterApiImpl;

impl ServerStarter {
    pub fn new(config: Config) -> (crate::Reporter, Self) {
        let (ereport_tx, ereports) = mpsc::channel(config.buffer_capacity);
        let (tx, requests) = mpsc::channel(128);
        let this = Self {
            config,
            ereports,
            ctx: ServerContext { tx },
            requests,
            dns_resolver: None,
        };
        (crate::Reporter(ereport_tx), this)
    }

    /// Use the provided internal DNS resolver rather than creating a new one.
    pub fn with_resolver(self, resolver: Resolver) -> Self {
        Self { dns_resolver: Some(resolver), ..self }
    }

    pub async fn start(self) -> anyhow::Result<RunningServer> {
        let Self { config, ctx, ereports, requests, dns_resolver } = self;
        let log = {
            let base_logger = match config.log {
                LogConfig::Config(conf) => conf.to_logger("ereporter")?,
                LogConfig::Logger(log) => log.clone(),
            };
            let (drain, registration) = slog_dtrace::with_drain(base_logger);
            let log = Logger::root(drain.fuse(), slog::o!(FileKv));
            if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
                error!(log, "failed to register DTrace probes: {e}",);
            } else {
                debug!(log, "registered DTrace probes");
            }
            log
        };

        // 1. discover nexus

        // Create a resolver if needed, or use Nexus's address directly.
        let discovery = match (config.registration_address, dns_resolver) {
            (Some(addr), _) => {
                if addr.port() == 0 {
                    anyhow::bail!(
                        "Nexus registration address must have a real port"
                    );
                }
                debug!(
                    log,
                    "Nexus IP provided explicitly, registering with it";
                    "addr" => %addr,
                );
                NexusDiscovery::Addr(addr)
            }
            (None, None) => {
                // Ensure that we've been provided with an IPv6 address if we're
                // using DNS to resolve Nexus. That's required because we need
                // to use the /48 to find our DNS server itself.
                let IpAddr::V6(our_addr) = config.reporter.address.ip() else {
                    anyhow::bail!("server address must be IPv6 in order to resolve Nexus from DNS")
                };
                debug!(
                    log,
                    "Nexus IP not provided, will create an internal \
                     DNS resolver to resolve it"
                );

                let resolver = Resolver::new_from_ip(
                    log.new(slog::o!("component" => "internal-dns-resolver")),
                    our_addr,
                )?;
                NexusDiscovery::Dns(resolver)
            }
            (None, Some(resolver)) => {
                debug!(
                    log,
                    "Nexus IP not provided, will use DNS to resolve it"
                );
                NexusDiscovery::Dns(resolver)
            }
        };
        let nexus_addr = discovery.nexus_addr(&log).await;
        let nexus_client = nexus_client::Client::new(
            &format!("http://{nexus_addr}"),
            log.clone(),
        );

        // 2. register server and recover sequence number
        // TODO(eliza): perhaps registrations should be periodically refreshed?
        let nexus_client::types::EreporterRegistered { seq } =
            config.reporter.register(&log, &nexus_client).await;

        // 3. spawn buffer task
        let buffer_task = tokio::spawn(
            crate::buffer::Buffer {
                seq,
                buf: VecDeque::with_capacity(config.buffer_capacity),
                log,
                id: config.reporter.id,
                ereports,
                requests,
            }
            .run(),
        );

        // 4. spawn dropshot server
        // TODO(eliza): actually do that

        Ok(RunningServer { buffer_task })
    }
}

impl NexusDiscovery {
    async fn nexus_addr(&self, log: &Logger) -> SocketAddr {
        match self {
            Self::Addr(addr) => *addr,
            Self::Dns(resolver) => {
                let log_failure = |error, delay| {
                    warn!(
                        log,
                        "failed to lookup Nexus IP, will retry";
                        "delay" => ?delay,
                        "error" => ?error,
                    );
                };
                let do_lookup = || async {
                    resolver
                        .lookup_socket_v6(ServiceName::Nexus)
                        .await
                        .map_err(|e| BackoffError::transient(e.to_string()))
                        .map(Into::into)
                };
                backoff::retry_notify(
                    backoff::retry_policy_internal_service(),
                    do_lookup,
                    log_failure,
                )
                .await
                .expect("Expected infinite retry loop resolving Nexus address")
            }
        }
    }
}

impl ReporterIdentity {
    async fn register(
        &self,
        log: &Logger,
        client: &nexus_client::Client,
    ) -> nexus_client::types::EreporterRegistered {
        let log_failure = |error, delay| {
            warn!(
                log,
                "failed to register ereporter with Nexus, will retry";
                "delay" => ?delay,
                "error" => ?error,
            );
        };
        let info = nexus_client::types::EreporterInfo {
            address: self.address.to_string(),
            reporter_id: self.id,
        };

        let do_register = || async {
            client
                .cpapi_ereporters_post(&info)
                .await
                .map(|response| response.into_inner())
                .map_err(|e| BackoffError::transient(e))
        };
        backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            do_register,
            log_failure,
        )
        .await
        .expect("Expected infinite retry loop registering ereporter")
    }
}

impl ereporter_api::EreporterApi for EreporterApiImpl {
    type Context = ServerContext;

    async fn ereports_list(
        reqctx: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<ereporter_api::Ereport>>, HttpError>
    {
        let ctx = reqctx.context();

        let pagination = query.into_inner();
        let limit = reqctx.page_limit(&pagination)?.get() as usize;

        let start_seq = match pagination.page {
            WhichPage::First(..) => None,
            WhichPage::Next(seq) => Some(seq),
        };

        slog::debug!(
            reqctx.log,
            "received ereport list request";
            "start_seq" => ?start_seq,
            "limit" => limit,
        );
        let (tx, rx) = oneshot::channel();
        ctx.tx
            .send(buffer::ServerReq::List { start_seq, limit, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        let list = rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })?;
        let page = ResultsPage::new(
            list,
            &EmptyScanParams {},
            |ereport: &ereporter_api::Ereport, _| ereport.seq,
        )?;
        Ok(HttpResponseOk(page))
    }

    async fn ereports_truncate(
        reqctx: RequestContext<Self::Context>,
        path: dropshot::Path<ereporter_api::SeqPathParam>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let ctx = reqctx.context();
        let ereporter_api::SeqPathParam { seq } = path.into_inner();
        slog::debug!(reqctx.log, "received ereport truncate request"; "seq" => ?seq);
        let (tx, rx) = oneshot::channel();
        ctx.tx
            .send(buffer::ServerReq::TruncateTo { seq, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })??;
        Ok(HttpResponseDeleted())
    }
}
