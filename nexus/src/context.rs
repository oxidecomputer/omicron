// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared state used by API request handlers
use super::authn;
use super::authz;
use super::config;
use super::db;
use super::Nexus;
use crate::authn::external::session_cookie::SessionStore;
use crate::authn::ConsoleSessionWithSiloId;
use crate::saga_interface::SagaContext;
use async_trait::async_trait;
use authn::external::session_cookie::HttpAuthnSessionCookie;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::token::HttpAuthnToken;
use authn::external::HttpAuthnScheme;
use chrono::Duration;
use internal_dns::{ServiceName, SRV};
use nexus_db_queries::context::{OpContext, OpKind};
use omicron_common::address::{Ipv6Subnet, AZ_PREFIX, COCKROACH_PORT};
use omicron_common::nexus_config;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Shared state available to all API request handlers
pub struct ServerContext {
    /// reference to the underlying nexus
    pub nexus: Arc<Nexus>,
    /// debug log
    pub log: Logger,
    /// authenticator for external HTTP requests
    pub external_authn: authn::external::Authenticator<ServerContext>,
    /// authentication context used for internal HTTP requests
    pub internal_authn: Arc<authn::Context>,
    /// authorizer
    pub authz: Arc<authz::Authz>,
    /// internal API request latency tracker
    pub internal_latencies: LatencyTracker,
    /// external API request latency tracker
    pub external_latencies: LatencyTracker,
    /// registry of metric producers
    pub producer_registry: ProducerRegistry,
    /// tunable settings needed for the console at runtime
    pub console_config: ConsoleConfig,
}

pub struct ConsoleConfig {
    /// how long a session can be idle before expiring
    pub session_idle_timeout: Duration,
    /// how long a session can exist before expiring
    pub session_absolute_timeout: Duration,
    /// how long browsers can cache static assets
    pub cache_control_max_age: Duration,
    /// directory containing static file to serve
    pub static_dir: Option<PathBuf>,
}

impl ServerContext {
    /// Create a new context with the given rack id and log.  This creates the
    /// underlying nexus as well.
    pub async fn new(
        rack_id: Uuid,
        log: Logger,
        config: &config::Config,
    ) -> Result<Arc<ServerContext>, String> {
        let nexus_schemes = config
            .pkg
            .authn
            .schemes_external
            .iter()
            .map::<Box<dyn HttpAuthnScheme<ServerContext>>, _>(
                |name| match name {
                    config::SchemeName::Spoof => Box::new(HttpAuthnSpoof),
                    config::SchemeName::SessionCookie => {
                        Box::new(HttpAuthnSessionCookie)
                    }
                    config::SchemeName::AccessToken => Box::new(HttpAuthnToken),
                },
            )
            .collect();
        let external_authn = authn::external::Authenticator::new(nexus_schemes);
        let internal_authn = Arc::new(authn::Context::internal_api());
        let authz = Arc::new(authz::Authz::new(&log));
        let create_tracker = |name: &str| {
            let target = HttpService {
                name: name.to_string(),
                id: config.deployment.id,
            };
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
        let producer_registry = ProducerRegistry::with_id(config.deployment.id);
        producer_registry
            .register_producer(internal_latencies.clone())
            .unwrap();
        producer_registry
            .register_producer(external_latencies.clone())
            .unwrap();

        // Support both absolute and relative paths. If configured dir is
        // absolute, use it directly. If not, assume it's relative to the
        // current working directory.
        let static_dir = if config.pkg.console.static_dir.is_absolute() {
            Some(config.pkg.console.static_dir.to_owned())
        } else {
            env::current_dir()
                .map(|root| root.join(&config.pkg.console.static_dir))
                .ok()
        };

        // We don't want to fail outright yet, but we do want to try to make
        // problems slightly easier to debug. The only way it's None is if
        // current_dir() fails.
        if static_dir.is_none() {
            error!(log, "No assets directory configured. All console page and asset requests will 404.");
        }

        // TODO: check that asset directory exists, check for particular assets
        // like console index.html. leaving that out for now so we don't break
        // nexus in dev for everyone

        // Set up DNS Client
        let az_subnet =
            Ipv6Subnet::<AZ_PREFIX>::new(config.deployment.subnet.net().ip());
        info!(log, "Setting up resolver on subnet: {:?}", az_subnet);
        let resolver =
            internal_dns::resolver::Resolver::new_from_subnet(az_subnet)
                .map_err(|e| format!("Failed to create DNS resolver: {}", e))?;

        // Set up DB pool
        let url = match &config.deployment.database {
            nexus_config::Database::FromUrl { url } => url.clone(),
            nexus_config::Database::FromDns => {
                info!(log, "Accessing DB url from DNS");
                let address = resolver
                    .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
                    .await
                    .map_err(|e| format!("Failed to lookup IP: {}", e))?;
                info!(log, "DB address: {}", address);
                PostgresConfigWithUrl::from_str(&format!(
                    "postgresql://root@[{}]:{}/omicron?sslmode=disable",
                    address, COCKROACH_PORT
                ))
                .map_err(|e| format!("Cannot parse Postgres URL: {}", e))?
            }
        };
        let pool = db::Pool::new(&db::Config { url });
        let nexus = Nexus::new_with_id(
            rack_id,
            log.new(o!("component" => "nexus")),
            Arc::new(tokio::sync::Mutex::new(resolver)),
            pool,
            &producer_registry,
            config,
            Arc::clone(&authz),
        )
        .await;

        Ok(Arc::new(ServerContext {
            nexus,
            log,
            external_authn,
            internal_authn,
            authz,
            internal_latencies,
            external_latencies,
            producer_registry,
            console_config: ConsoleConfig {
                session_idle_timeout: Duration::minutes(
                    config.pkg.console.session_idle_timeout_minutes.into(),
                ),
                session_absolute_timeout: Duration::minutes(
                    config.pkg.console.session_absolute_timeout_minutes.into(),
                ),
                static_dir,
                cache_control_max_age: Duration::minutes(
                    config.pkg.console.cache_control_max_age_minutes.into(),
                ),
            },
        }))
    }
}

/// Authenticates an incoming request to the external API and produces a new
/// operation context for it
pub async fn op_context_for_external_api(
    rqctx: &dropshot::RequestContext<Arc<ServerContext>>,
) -> Result<OpContext, dropshot::HttpError> {
    let apictx = rqctx.context();
    OpContext::new_async(
        &rqctx.log,
        async {
            let authn =
                Arc::new(apictx.external_authn.authn_request(rqctx).await?);
            let datastore = Arc::clone(apictx.nexus.datastore());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&apictx.authz),
                datastore,
            );
            Ok((authn, authz))
        },
        |metadata| OpContext::load_request_metadata(rqctx, metadata),
        OpKind::ExternalApiRequest,
    )
    .await
}

pub async fn op_context_for_internal_api(
    rqctx: &dropshot::RequestContext<Arc<ServerContext>>,
) -> OpContext {
    let apictx = rqctx.context();
    OpContext::new_async(
        &rqctx.log,
        async {
            let authn = Arc::clone(&apictx.internal_authn);
            let datastore = Arc::clone(apictx.nexus.datastore());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&apictx.authz),
                datastore,
            );
            Ok::<_, std::convert::Infallible>((authn, authz))
        },
        |metadata| OpContext::load_request_metadata(rqctx, metadata),
        OpKind::InternalApiRequest,
    )
    .await
    .expect("infallible")
}

pub fn op_context_for_saga_action<T>(
    sagactx: &steno::ActionContext<T>,
    serialized_authn: &authn::saga::Serialized,
) -> OpContext
where
    T: steno::SagaType<ExecContextType = Arc<SagaContext>>,
{
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let datastore = Arc::clone(nexus.datastore());

    // TODO-debugging This would be a good place to put the saga name, but
    // we don't have it available here.  This log maybe should come from
    // steno, prepopulated with useful metadata similar to the way
    let log = osagactx.log().new(o!(
        "saga_node" => sagactx.node_label()
    ));

    OpContext::new(
        &log,
        || {
            let authn = Arc::new(serialized_authn.to_authn());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&osagactx.authz()),
                datastore,
            );
            Ok::<_, std::convert::Infallible>((authn, authz))
        },
        |metadata| {
            metadata.insert(String::from("saga_node"), sagactx.node_label());
        },
        OpKind::Saga,
    )
    .expect("infallible")
}

#[async_trait]
impl authn::external::SiloUserSilo for ServerContext {
    async fn silo_user_silo(
        &self,
        silo_user_id: Uuid,
    ) -> Result<Uuid, authn::Reason> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.lookup_silo_for_authn(opctx, silo_user_id).await
    }
}

#[async_trait]
impl authn::external::token::TokenContext for ServerContext {
    async fn token_actor(
        &self,
        token: String,
    ) -> Result<authn::Actor, authn::Reason> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.device_access_token_actor(opctx, token).await
    }
}

#[async_trait]
impl SessionStore for ServerContext {
    type SessionModel = ConsoleSessionWithSiloId;

    async fn session_fetch(&self, token: String) -> Option<Self::SessionModel> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.session_fetch(opctx, token).await.ok()
    }

    async fn session_update_last_used(
        &self,
        token: String,
    ) -> Option<Self::SessionModel> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.session_update_last_used(&opctx, &token).await.ok()
    }

    async fn session_expire(&self, token: String) -> Option<()> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.session_hard_delete(opctx, &token).await.ok()
    }

    fn session_idle_timeout(&self) -> Duration {
        self.console_config.session_idle_timeout
    }

    fn session_absolute_timeout(&self) -> Duration {
        self.console_config.session_absolute_timeout
    }
}
