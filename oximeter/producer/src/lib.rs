// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for serving produced metric data to an Oximeter collector server.

// Copyright 2024 Oxide Computer Company

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpServer;
use dropshot::HttpServerStarter;
use dropshot::Path;
use dropshot::RequestContext;
use internal_dns_resolver::ResolveError;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_client::types::ProducerEndpoint as ApiProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::backoff;
use omicron_common::backoff::BackoffError;
use omicron_common::FileKv;
use oximeter::types::ProducerRegistry;
use oximeter::types::ProducerResults;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Drain;
use slog::Logger;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

// Our public interface depends directly or indirectly on these types; we
// export them so that consumers need not depend on dropshot themselves and
// to simplify how we stage incompatible upgrades.
pub use dropshot::ConfigLogging;
pub use dropshot::ConfigLoggingIfExists;
pub use dropshot::ConfigLoggingLevel;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Error running producer HTTP server: {0}")]
    Server(String),

    #[error("Producer registry and config UUIDs do not match")]
    UuidMismatch,

    #[error(
        "The producer must listen on an IPv6 address \
        to resolve Nexus using DNS"
    )]
    Ipv6AddressRequiredForResolution,

    #[error("Error resolving Nexus using DNS")]
    Resolution(#[source] ResolveError),

    #[error("Invalid port number provided for Nexus registration address")]
    InvalidRegistrationPort,
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

/// Information used to configure a [`Server`]
#[derive(Debug, Clone)]
pub struct Config {
    /// The information for contacting this server, and collecting its metrics.
    pub server_info: ProducerEndpoint,
    /// The address at which we attempt to register as a producer.
    ///
    /// If the address is not provided, the address of Nexus will be resolved
    /// using internal DNS, based on the local address of the server being
    /// configured.
    pub registration_address: Option<SocketAddr>,
    /// The maximum size of Dropshot requests.
    pub request_body_max_bytes: usize,
    /// The logging configuration or actual logger used to emit logs.
    pub log: LogConfig,
}

/// A Dropshot server used to expose metrics to be collected over the network.
pub struct Server {
    registry: ProducerRegistry,
    registration_task: tokio::task::JoinHandle<()>,
    server: HttpServer<ProducerRegistry>,
}

impl Server {
    /// Start a new metric server, registering it with the chosen endpoint, and listening for
    /// requests on the associated address and route.
    ///
    /// Note that the producer server is registered with Nexus in a background
    /// task. That task also periodically re-registers with Nexus to ensure that
    /// data continues to be collected.
    pub fn start(config: &Config) -> Result<Self, Error> {
        Self::with_registry(
            ProducerRegistry::with_id(config.server_info.id),
            &config,
        )
    }

    /// Create a new metric producer server, with an existing registry.
    ///
    /// Note that the producer server is registered with Nexus in a background
    /// task.
    pub fn with_registry(
        registry: ProducerRegistry,
        config: &Config,
    ) -> Result<Self, Error> {
        Self::new_impl(
            registry,
            config.server_info.clone(),
            config.registration_address.as_ref(),
            config.request_body_max_bytes,
            &config.log,
        )
    }

    /// Serve requests for metrics.
    pub async fn serve_forever(self) -> Result<(), Error> {
        let res = self.server.await.map_err(Error::Server);
        self.registration_task.abort();
        res
    }

    /// Close the server
    pub async fn close(self) -> Result<(), Error> {
        self.registration_task.abort();
        self.server.close().await.map_err(Error::Server)
    }

    /// Return the [`ProducerRegistry`] managed by this server.
    ///
    /// The registry is thread-safe and clonable, so the returned reference can be used throughout
    /// an application to register types implementing the [`Producer`](oximeter::traits::Producer)
    /// trait. The samples generated by the registered producers will be included in response to a
    /// request on the collection endpoint.
    pub fn registry(&self) -> &ProducerRegistry {
        &self.registry
    }

    /// Return the server's local listening address
    pub fn address(&self) -> std::net::SocketAddr {
        self.server.local_addr()
    }

    fn build_logger(log: &LogConfig) -> Result<Logger, Error> {
        // Build a logger, either using the configuration or actual logger
        // provided. First build the base logger from the configuration or a
        // clone of the provided logger, and then add the DTrace and Dropshot
        // loggers on top of it.
        let base_logger = match log {
            LogConfig::Config(conf) => conf
                .to_logger("metric-server")
                .map_err(|msg| Error::Server(msg.to_string()))?,
            LogConfig::Logger(log) => log.clone(),
        };
        let (drain, registration) = slog_dtrace::with_drain(base_logger);
        let log = Logger::root(drain.fuse(), slog::o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("failed to register DTrace probes: {}", e);
            error!(log, "failed to register DTrace probes: {}", e);
            return Err(Error::Server(msg));
        } else {
            debug!(log, "registered DTrace probes");
        }
        Ok(log)
    }

    fn build_dropshot_server(
        log: &Logger,
        registry: &ProducerRegistry,
        dropshot: &ConfigDropshot,
    ) -> Result<HttpServer<ProducerRegistry>, Error> {
        let dropshot_log = log.new(o!("component" => "dropshot"));
        HttpServerStarter::new(
            dropshot,
            metric_server_api(),
            registry.clone(),
            &dropshot_log,
        )
        .map_err(|e| Error::Server(e.to_string()))
        .map(HttpServerStarter::start)
    }

    // Create a new server registering with Nexus.
    fn new_impl(
        registry: ProducerRegistry,
        mut server_info: ProducerEndpoint,
        registration_address: Option<&SocketAddr>,
        request_body_max_bytes: usize,
        log: &LogConfig,
    ) -> Result<Self, Error> {
        if registry.producer_id() != server_info.id {
            return Err(Error::UuidMismatch);
        }

        // Build the logger / server.
        let log = Self::build_logger(log)?;
        let dropshot = ConfigDropshot {
            bind_address: server_info.address,
            request_body_max_bytes,
            default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
            log_headers: vec![],
        };
        let server = Self::build_dropshot_server(&log, &registry, &dropshot)?;

        // Update the producer endpoint address with the actual server's
        // address, to handle cases where client listens on any available
        // address.
        if server_info.address != server.local_addr() {
            assert_eq!(server_info.address.port(), 0);
            debug!(
                log,
                "Requested any available port, Dropshot server has been bound to {}",
                server.local_addr(),
            );
            server_info.address = server.local_addr();
        }

        // Create a resolver if needed, or use Nexus's address directly.
        let find_nexus = match registration_address {
            Some(addr) => {
                if addr.port() == 0 {
                    return Err(Error::InvalidRegistrationPort);
                }
                debug!(
                    log,
                    "Nexus IP provided explicitly, will use it";
                    "addr" => %addr,
                );
                FindNexus::ByAddr(*addr)
            }
            None => {
                // Ensure that we've been provided with an IPv6 address if we're
                // using DNS to resolve Nexus. That's required because we need
                // to use the /48 to find our DNS server itself.
                let IpAddr::V6(our_addr) = server_info.address.ip() else {
                    return Err(Error::Ipv6AddressRequiredForResolution);
                };
                debug!(
                    log,
                    "Nexus IP not provided, will use DNS to resolve it"
                );
                Resolver::new_from_ip(
                    log.new(o!("component" => "internal-dns-resolver")),
                    our_addr,
                )
                .map_err(Error::Resolution)
                .map(FindNexus::WithResolver)?
            }
        };

        // Spawn the task that will register with Nexus in the background.
        debug!(log, "starting producer registration task");
        let info = ApiProducerEndpoint::from(&server_info);
        let registration_task = tokio::task::spawn(registration_task(
            find_nexus,
            log.new(o!("component" => "producer-registration-task")),
            info,
        ));
        info!(
            log,
            "starting oximeter metric producer server";
            "producer_id" => ?registry.producer_id(),
            "address" => server.local_addr(),
            "interval" => ?server_info.interval,
        );
        Ok(Self { registry, registration_task, server })
    }
}

/// Helper passed to the renewal task, used to determine whether / how to find
/// Nexus periodically.
enum FindNexus {
    /// An explicit address was provided at creation time, just use it.
    ByAddr(SocketAddr),
    /// An address was not provided, we'll resolve it on each attempt to renew
    /// the lease.
    WithResolver(Resolver),
}

/// The rate at which we renew, as a fraction of the renewal interval.
//
// E.g., a value of 4 means wait no more than 1/4 the period before renewing the
// lease. Be aware that renewal occurs with backoff, so it may be useful to
// register quite aggressively to avoid Nexus pruning the producer too early.
const RENEWAL_RATE: u32 = 4;

/// A backround task that periodically renews this producer's lease with Nexus.
async fn registration_task(
    find_nexus: FindNexus,
    log: Logger,
    endpoint: ApiProducerEndpoint,
) {
    loop {
        debug!(
            log,
            "registering / renewing oximeter producer lease with Nexus"
        );
        let address = match &find_nexus {
            FindNexus::ByAddr(addr) => *addr,
            FindNexus::WithResolver(resolver) => {
                resolve_nexus_with_backoff(&log, resolver).await
            }
        };
        debug!(log, "using nexus address for registration"; "addr" => ?address);
        let lease_duration =
            register_with_backoff(address, &log, &endpoint).await;
        debug!(log, "registered with nexus successfully");

        // Wait for a reasonable fraction of the renewal period, and then hit
        // 'em again.
        let wait =
            lease_duration.checked_div(RENEWAL_RATE).unwrap_or(lease_duration);
        debug!(
            log,
            "pausing until time to renew lease";
            "lease_duration" => ?lease_duration,
            "wait_period" => ?wait,
        );
        tokio::time::sleep(wait).await;
    }
}

// Register API endpoints of the `Server`.
fn metric_server_api() -> ApiDescription<ProducerRegistry> {
    let mut api = ApiDescription::new();
    api.register(collect).expect("Failed to register handler for collect");
    api
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerIdPathParams {
    /// The ID of the producer to be polled.
    pub producer_id: Uuid,
}

/// Collect metric data from this producer.
#[endpoint {
    method = GET,
    path = "/{producer_id}",
}]
async fn collect(
    request_context: RequestContext<ProducerRegistry>,
    path_params: Path<ProducerIdPathParams>,
) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
    let registry = request_context.context();
    let producer_id = path_params.into_inner().producer_id;
    if producer_id == registry.producer_id() {
        Ok(HttpResponseOk(registry.collect()))
    } else {
        Err(HttpError::for_not_found(
            None,
            format!(
                "Producer ID {} is not valid, expected {}",
                producer_id,
                registry.producer_id()
            ),
        ))
    }
}

/// Resolve Nexus's address using the provided resolver.
async fn resolve_nexus_with_backoff(
    log: &Logger,
    resolver: &Resolver,
) -> SocketAddr {
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

/// Register as a metric producer with Nexus, retrying endlessly with backoff.
///
/// This returns the lease renewal period that we're required to re-register
/// within.
async fn register_with_backoff(
    addr: SocketAddr,
    log: &Logger,
    endpoint: &ApiProducerEndpoint,
) -> Duration {
    let log_failure = |error, delay| {
        warn!(
            log,
            "failed to register as a producer with Nexus, will retry";
            "delay" => ?delay,
            "error" => ?error,
        );
    };
    // For the purposes of oximeter registration, all errors are retryable. The
    // main reason for this is that there's just not much better we can do.
    // Panicking seems bad, but stopping the retry loop is also not great
    // without a way to kick it to start trying again. We may want to add
    // better reporting, such as a counter or way to fetch the last registration
    // result.
    let do_register = || async {
        let client =
            nexus_client::Client::new(&format!("http://{}", addr), log.clone());
        client
            .cpapi_producers_post(&endpoint.into())
            .await
            .map(|response| response.into_inner().lease_duration.into())
            .map_err(|e| BackoffError::transient(e.to_string()))
    };
    backoff::retry_notify(
        backoff::retry_policy_internal_service(),
        do_register,
        log_failure,
    )
    .await
    .expect("Expected infinite retry loop registering as a producer with")
}

#[cfg(test)]
mod tests {
    use super::Config;
    use super::LogConfig;
    use super::ProducerEndpoint;
    use super::Server;
    use dropshot::endpoint;
    use dropshot::ApiDescription;
    use dropshot::ConfigDropshot;
    use dropshot::HttpError;
    use dropshot::HttpResponseCreated;
    use dropshot::HttpServer;
    use dropshot::HttpServerStarter;
    use dropshot::RequestContext;
    use omicron_common::api::internal::nexus::ProducerKind;
    use omicron_common::api::internal::nexus::ProducerRegistrationResponse;
    use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
    use slog::Drain;
    use slog::Logger;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_logger() -> Logger {
        let dec =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(dec).build().fuse();
        let log =
            Logger::root(drain, slog::o!("component" => "fake-cleanup-task"));
        log
    }

    // Re-registration interval for tests.
    const INTERVAL: Duration = Duration::from_secs(1);

    type Context = Arc<AtomicU32>;

    // Mock endpoint for the test Nexus server.
    #[endpoint {
        method = POST,
        path = "/metrics/producers",
    }]
    async fn register_producer(
        rqctx: RequestContext<Context>,
    ) -> Result<HttpResponseCreated<ProducerRegistrationResponse>, HttpError>
    {
        rqctx.context().fetch_add(1, Ordering::SeqCst);
        Ok(HttpResponseCreated(ProducerRegistrationResponse {
            lease_duration: INTERVAL,
        }))
    }

    // Start a Dropshot server mocking the Nexus registration endpoint.
    fn spawn_fake_nexus_server(log: &Logger) -> HttpServer<Context> {
        let mut api = ApiDescription::new();
        api.register(register_producer).expect("Expected to register endpoint");
        HttpServerStarter::new(
            &ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                request_body_max_bytes: 2048,
                ..Default::default()
            },
            api,
            Arc::new(AtomicU32::new(0)),
            log,
        )
        .expect("Expected to start Dropshot server")
        .start()
    }

    #[tokio::test]
    async fn test_producer_registration_task() {
        let log = test_logger();
        let fake_nexus = spawn_fake_nexus_server(&log);
        slog::info!(
            log,
            "fake nexus test server listening";
            "address" => ?fake_nexus.local_addr(),
        );

        let address = "[::1]:0".parse().unwrap();
        let config = Config {
            server_info: ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: ProducerKind::Service,
                address,
                interval: Duration::from_secs(10),
            },
            registration_address: Some(fake_nexus.local_addr()),
            request_body_max_bytes: 1024,
            log: LogConfig::Logger(log),
        };

        // Ideally, we would check pretty carefully that there are exactly N
        // registrations after N renewal periods. That's brittle, especially on
        // a loaded system. Instead, we'll wait until we've received the
        // expected number of registration requests.
        let _server = Server::start(&config).unwrap();
        const N_REQUESTS: u32 = 10;
        const POLL_INTERVAL: Duration = Duration::from_millis(100);

        // The poll interval is 1s (see `INTERVAL`), and the producer attempts
        // to register every 1/4 interval, so this should be quite sufficient
        // for even heavily-loaded tests.
        const POLL_DURATION: Duration = Duration::from_secs(30);
        wait_for_condition(
            || async {
                if fake_nexus.app_private().load(Ordering::SeqCst) >= N_REQUESTS
                {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &POLL_INTERVAL,
            &POLL_DURATION,
        )
        .await
        .expect("Expected all registration requests to be made within timeout");
        fake_nexus.close().await.expect("Expected to close server");
    }
}
