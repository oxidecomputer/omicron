/*!
 * Shared state used by API request handlers
 */
use super::authn;
use super::config;
use super::db;
use super::Nexus;
use async_trait::async_trait;
use authn::external::session_cookie::HttpAuthnSessionCookie;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::HttpAuthnScheme;
use chrono::{DateTime, Utc};
use omicron_common::api::external::{LookupResult, UpdateResult};
use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::sync::Arc;
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
            .map::<Box<dyn HttpAuthnScheme<Arc<ServerContext>>>, _>(|name| {
                match name {
                    config::SchemeName::Spoof => Box::new(HttpAuthnSpoof),
                    config::SchemeName::SessionCookie => {
                        Box::new(HttpAuthnSessionCookie)
                    }
                }
            })
            .collect();
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

// TODO: make this generic in the session type so we can use tuples in the tests
pub trait Session {
    fn user_id(&self) -> Uuid;
    fn last_used(&self) -> DateTime<Utc>;
    fn time_created(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait SessionStore {
    type SessionModel;

    async fn session_fetch(
        &self,
        token: String,
    ) -> LookupResult<Self::SessionModel>;

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> UpdateResult<Self::SessionModel>;
}

#[async_trait]
impl SessionStore for Arc<ServerContext> {
    type SessionModel = db::model::ConsoleSession;

    async fn session_fetch(
        &self,
        token: String,
    ) -> LookupResult<db::model::ConsoleSession> {
        self.nexus.session_fetch(token).await
    }

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> UpdateResult<db::model::ConsoleSession> {
        self.nexus.session_update_last_used(token).await
    }
}

impl Session for db::model::ConsoleSession {
    fn user_id(&self) -> Uuid {
        self.user_id
    }
    fn last_used(&self) -> DateTime<Utc> {
        self.last_used
    }
    fn time_created(&self) -> DateTime<Utc> {
        self.time_created
    }
}
