/*!
 * Shared state used by API request handlers
 */
use super::authn;
use super::config;
use super::db;
use super::Nexus;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::HttpAuthnScheme;
use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use uuid::Uuid;

/**
 * Shared state available to all API request handlers
 */
pub struct ServerContext {
    /** reference to the underlying nexus */
    pub nexus: Arc<Nexus>,
    /** debug log */
    pub log: Logger,
    /** authenticator for external HTTP requests */
    pub external_authn: authn::external::Authenticator<Arc<ServerContext>>,
    /** internal API request latency tracker */
    pub internal_latencies: LatencyTracker,
    /** external API request latency tracker */
    pub external_latencies: LatencyTracker,
    /** registry of metric producers */
    pub producer_registry: ProducerRegistry,
}

impl ServerContext {
    /**
     * Create a new context with the given rack id and log.  This creates the
     * underlying nexus as well.
     */
    pub fn new(
        rack_id: &Uuid,
        log: Logger,
        pool: db::Pool,
        config: &config::Config,
    ) -> Arc<ServerContext> {
        let nexus_schemes = config
            .authn_schemes_external
            .iter()
            .map(|name| match name {
                config::SchemeName::Spoof => Box::new(HttpAuthnSpoof),
            }
                as Box<dyn HttpAuthnScheme<Arc<ServerContext>>>)
            .collect::<Vec<Box<dyn HttpAuthnScheme<Arc<ServerContext>>>>>();
        let external_authn = authn::external::Authenticator::new(nexus_schemes);
        let create_tracker = |name: &str| {
            let target = HttpService { name: name.to_string(), id: config.id };
            const START_LATENCY_DECADE: i8 = -6;
            const END_LATENCY_DECADE: i8 = 3;
            LatencyTracker::with_latency_decades(
                target,
                START_LATENCY_DECADE,
                END_LATENCY_DECADE,
            )
            .unwrap()
        };
        let internal_latencies = create_tracker("nexus-internal");
        let external_latencies = create_tracker("nexus-external");
        let producer_registry = ProducerRegistry::with_id(config.id);
        producer_registry
            .register_producer(internal_latencies.clone())
            .unwrap();
        producer_registry
            .register_producer(external_latencies.clone())
            .unwrap();

        Arc::new(ServerContext {
            nexus: Nexus::new_with_id(
                rack_id,
                log.new(o!("component" => "nexus")),
                pool,
                config,
            ),
            log,
            external_authn,
            internal_latencies,
            external_latencies,
            producer_registry,
        })
    }
}

/// Provides general facilities scoped to whatever operation Nexus is currently
/// doing
///
/// The idea is that whatever code path you're looking at in Nexus, it should
/// eventually have an OpContext that allows it to:
///
/// - log a message (with relevant operation-specific metadata)
/// - bump a counter (exported via Oximeter)
/// - emit tracing data
/// - do an authorization check
///
/// OpContexts are constructed when Nexus begins doing something.  This is often
/// when it starts handling an API request, but it could be when starting a
/// background operation or something else.
// Not all of these fields are used yet, but they may still prove useful for
// debugging.
#[allow(dead_code)]
pub struct OpContext {
    pub log: slog::Logger,
    pub authn: authn::Context,

    created_instant: Instant,
    created_walltime: SystemTime,
    metadata: BTreeMap<String, String>,
    kind: OpKind,
}

pub enum OpKind {
    /// Handling an external API request
    ExternalApiRequest,
    /// Background operations in Nexus
    Background,
}

impl OpContext {
    /// Authenticates an incoming request to the external API and produces a new
    /// operation context for it
    pub async fn for_external_api(
        rqctx: &dropshot::RequestContext<Arc<ServerContext>>,
    ) -> Result<OpContext, dropshot::HttpError> {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let apictx = rqctx.context();
        let log = apictx.log.new(o!());
        let authn = apictx.external_authn.authn_request(rqctx).await?;

        let request = rqctx.request.lock().await;
        let mut metadata = BTreeMap::new();
        metadata.insert(String::from("request_id"), rqctx.request_id.clone());
        metadata
            .insert(String::from("http_method"), request.method().to_string());
        metadata.insert(String::from("http_uri"), request.uri().to_string());

        Ok(OpContext {
            log,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind: OpKind::ExternalApiRequest,
        })
    }

    /// Returns a context suitable for use in background operations in Nexus
    pub fn for_background(log: slog::Logger) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        OpContext {
            log,
            authn: authn::Context::internal_unauthenticated(),
            created_instant,
            created_walltime,
            metadata: BTreeMap::new(),
            kind: OpKind::Background,
        }
    }
}
