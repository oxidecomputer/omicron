use crate::buffer;
use crate::registry::ReporterRegistry;
use dropshot::{
    ConfigDropshot, EmptyScanParams, HttpError, HttpResponseDeleted,
    HttpResponseOk, PaginationParams, Path, Query, RequestContext, ResultsPage,
    WhichPage,
};
use ereporter_api::ListPathParams;
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use slog::debug;
use slog::warn;
use slog_error_chain::SlogInlineError;
use std::net::IpAddr;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub server_address: SocketAddr,
    /// How to discover the Nexus API to register the reporter.
    pub registration_address: Option<SocketAddr>,
    // /// The maximum size of Dropshot requests.
    pub request_body_max_bytes: usize,
}

pub struct RunningServer {
    _server: dropshot::HttpServer<ReporterRegistry>,
}

#[derive(Clone, Debug)]
pub(crate) struct State {
    server_address: SocketAddr,
    nexus: NexusDiscovery,
}

impl ReporterRegistry {
    pub fn start_server(
        &self,
        config: Config,
        dns_resolver: Option<Resolver>,
    ) -> anyhow::Result<RunningServer> {
        let log = &self.0.log;

        let _server = {
            let dropshot_cfg = ConfigDropshot {
                bind_address: config.server_address,
                request_body_max_bytes: config.request_body_max_bytes,
                default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
                log_headers: vec![],
            };
            let log = log.new(slog::o!("component" => "dropshot"));
            let api = ereporter_api::ereporter_api_mod::api_description::<
                EreporterApiImpl,
            >()?;
            dropshot::HttpServerStarter::new(
                &dropshot_cfg,
                api,
                self.clone(),
                &log,
            )
            .map_err(|e| {
                anyhow::anyhow!("could not start dropshot server: {e}")
            })?
            .start()
        };

        // Create a resolver if needed, or use Nexus's address directly.
        let nexus = match (config.registration_address, dns_resolver) {
            (Some(addr), _) => {
                if addr.port() == 0 {
                    anyhow::bail!(
                        "Nexus registration address must have a real port"
                    );
                }
                debug!(
                    self.0.log,
                    "Nexus IP provided explicitly, registering with it";
                    "addr" => %addr,
                );
                NexusDiscovery::Addr(addr)
            }
            (None, None) => {
                // Ensure that we've been provided with an IPv6 address if we're
                // using DNS to resolve Nexus. That's required because we need
                // to use the /48 to find our DNS server itself.
                let IpAddr::V6(our_addr) = config.server_address.ip() else {
                    anyhow::bail!("server address must be IPv6 in order to resolve Nexus from DNS")
                };
                debug!(
                    self.0.log,
                    "Nexus IP not provided, will create an internal \
                     DNS resolver to resolve it"
                );

                let resolver = Resolver::new_from_ip(
                    self.0
                        .log
                        .new(slog::o!("component" => "internal-dns-resolver")),
                    our_addr,
                )?;
                NexusDiscovery::Dns(resolver)
            }
            (None, Some(resolver)) => {
                debug!(
                    self.0.log,
                    "Nexus IP not provided, will use DNS to resolve it"
                );
                NexusDiscovery::Dns(resolver)
            }
        };

        self.0
            .server_tx
            .send(Some(State { server_address: config.server_address, nexus }))
            .expect("receivers should never be dropped");
        Ok(RunningServer { _server })
    }
}

struct EreporterApiImpl;
impl ereporter_api::EreporterApi for EreporterApiImpl {
    type Context = ReporterRegistry;

    async fn ereports_list(
        reqctx: RequestContext<Self::Context>,
        path: Path<ListPathParams>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<ereporter_api::Entry>>, HttpError>
    {
        let registry = reqctx.context();
        let pagination = query.into_inner();
        let limit = reqctx.page_limit(&pagination)?.get() as usize;
        let ereporter_api::ListPathParams { reporter_id } = path.into_inner();

        let start_seq = match pagination.page {
            WhichPage::First(..) => None,
            WhichPage::Next(seq) => Some(seq),
        };

        slog::debug!(
            reqctx.log,
            "received ereport list request";
            "reporter_id" => %reporter_id,
            "start_seq" => ?start_seq,
            "limit" => limit,
        );
        let worker = registry.get_worker(&reporter_id).ok_or(
            HttpError::for_not_found(
                Some("NO_REPORTER".to_string()),
                format!("no reporter with ID {reporter_id}"),
            ),
        )?;
        let (tx, rx) = oneshot::channel();
        worker
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
            |entry: &ereporter_api::Entry, _| entry.seq,
        )?;
        Ok(HttpResponseOk(page))
    }

    async fn ereports_acknowledge(
        reqctx: RequestContext<Self::Context>,
        path: Path<ereporter_api::AcknowledgePathParams>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let registry = reqctx.context();
        let ereporter_api::AcknowledgePathParams { reporter_id, seq } =
            path.into_inner();
        slog::debug!(
            reqctx.log,
            "received ereport acknowledge request";
            "reporter_id" => %reporter_id,
            "seq" => ?seq,
        );

        let worker = registry.get_worker(&reporter_id).ok_or(
            HttpError::for_not_found(
                Some("NO_REPORTER".to_string()),
                format!("no reporter with ID {reporter_id}"),
            ),
        )?;
        let (tx, rx) = oneshot::channel();
        worker
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

/// How to discover Nexus' IP for registration.
#[derive(Clone)]
enum NexusDiscovery {
    Addr(SocketAddr),
    Dns(Resolver),
}

impl State {
    pub(crate) async fn register_reporter(
        &self,
        log: &slog::Logger,
        reporter_id: Uuid,
    ) -> Generation {
        #[derive(Debug, thiserror::Error, SlogInlineError)]
        enum RegistrationError {
            #[error(transparent)]
            Dns(#[from] internal_dns::resolver::ResolveError),
            #[error(transparent)]
            Client(#[from] nexus_client::Error<nexus_client::types::Error>),
        }

        let info = nexus_client::types::EreporterInfo {
            reporter_id,
            address: self.server_address.to_string(),
        };
        let do_register = || async {
            let nexus_addr = self.nexus.nexus_addr().await.map_err(|e| {
                BackoffError::transient(RegistrationError::from(e))
            })?;
            let nexus_client = nexus_client::Client::new(
                &format!("http://{nexus_addr}"),
                log.clone(),
            );
            let nexus_client::types::EreporterRegistered { seq } = nexus_client
                .cpapi_ereporters_post(&info)
                .await
                .map_err(|e| {
                    BackoffError::transient(RegistrationError::from(e))
                })?
                .into_inner();
            Ok(seq)
        };
        let log_failure = |error: RegistrationError, delay| {
            warn!(
                log,
                "failed to register ereporter with Nexus; retrying...";
                "reporter_id" => %reporter_id,
                "reporter_address" => %self.server_address,
                "error" => error,
                "delay" => ?delay,
            );
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

impl NexusDiscovery {
    async fn nexus_addr(
        &self,
    ) -> Result<SocketAddr, internal_dns::resolver::ResolveError> {
        match self {
            NexusDiscovery::Addr(addr) => Ok(*addr),
            NexusDiscovery::Dns(resolver) => resolver
                .lookup_socket_v6(ServiceName::Nexus)
                .await
                .map(Into::into),
        }
    }
}

impl std::fmt::Debug for NexusDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Addr(addr) => f.debug_tuple("Addr").field(&addr).finish(),
            Self::Dns(_) => f.debug_tuple("Dns").finish(),
        }
    }
}
