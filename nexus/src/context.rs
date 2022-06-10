// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared state used by API request handlers
use super::authn;
use super::authz;
use super::config;
use super::db;
use super::Nexus;
use crate::authn::external::session_cookie::{Session, SessionStore};
use crate::authn::ConsoleSessionWithSiloId;
use crate::authz::AuthorizedResource;
use crate::db::DataStore;
use crate::saga_interface::SagaContext;
use async_trait::async_trait;
use authn::external::session_cookie::HttpAuthnSessionCookie;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::HttpAuthnScheme;
use chrono::{DateTime, Duration, Utc};
use internal_dns_client::names::SRV;
use omicron_common::address::{
    Ipv6Subnet, AZ_PREFIX, COCKROACH_PORT,
};
use omicron_common::api::external::Error;
use omicron_common::nexus_config;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Debug;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use uuid::Uuid;

/// Shared state available to all API request handlers
pub struct ServerContext {
    /// reference to the underlying nexus
    pub nexus: Arc<Nexus>,
    /// debug log
    pub log: Logger,
    /// authenticator for external HTTP requests
    pub external_authn: authn::external::Authenticator<Arc<ServerContext>>,
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
        let internal_authn = Arc::new(authn::Context::internal_api());
        let authz = Arc::new(authz::Authz::new(&log));
        let create_tracker = |name: &str| {
            let target =
                HttpService { name: name.to_string(), id: config.runtime.id };
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
        let producer_registry = ProducerRegistry::with_id(config.runtime.id);
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
            Ipv6Subnet::<AZ_PREFIX>::new(config.runtime.subnet.net().ip());
        info!(log, "Setting up resolver on subnet: {:?}", az_subnet);
        let resolver =
            internal_dns_client::multiclient::create_resolver(az_subnet)
                .map_err(|e| format!("Failed to create DNS resolver: {}", e))?;

        // Set up DB pool
        let url = match &config.runtime.database {
            nexus_config::Database::FromUrl { url } => url.clone(),
            nexus_config::Database::FromDns => {
                info!(log, "Accessing DB url from DNS");
                let response = resolver
                    .lookup_ip(&SRV::Service("cockroachdb".to_string()).to_string())
                    .await
                    .map_err(|e| format!("Failed to lookup IP: {}", e))?;
                let address = response.iter().next().ok_or_else(|| {
                    "no addresses returned from DNS resolver".to_string()
                })?;
                info!(log, "DB address: {}", address);
                PostgresConfigWithUrl::from_str(&format!(
                    "postgresql://root@[{}]:{}/omicron?sslmode=disable",
                    address, COCKROACH_PORT
                ))
                .map_err(|e| format!("Cannot parse Postgres URL: {}", e))?
            }
        };
        let pool = db::Pool::new(&db::Config { url });
        let nexus =  Nexus::new_with_id(
            rack_id,
            log.new(o!("component" => "nexus")),
            pool,
            config,
            Arc::clone(&authz),
        );

        // Do not return until a rack exists in the DB with the provided UUID.
        let populate_ctx = nexus.opctx_for_background();
        loop {
            let result = nexus.rack_insert(&populate_ctx, rack_id)
                .await;
            if let Err(e) = result {
                info!(log, "Failed to create initial rack: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            } else {
                info!(log, "Rack with UUID {} exists in the database", rack_id);
                nexus.rack_lookup(&populate_ctx, &rack_id).await.unwrap();
                break;
            }
        }

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
    pub authn: Arc<authn::Context>,

    authz: authz::Context,
    created_instant: Instant,
    created_walltime: SystemTime,
    metadata: BTreeMap<String, String>,
    kind: OpKind,
}

enum OpKind {
    /// Handling an external API request
    ExternalApiRequest,
    /// Handling an internal API request
    InternalApiRequest,
    /// Executing a saga activity
    Saga,
    /// Background operations in Nexus
    Background,
    /// Automated testing (unit tests and integration tests)
    Test,
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
        let authn = Arc::new(apictx.external_authn.authn_request(rqctx).await?);
        let datastore = Arc::clone(apictx.nexus.datastore());
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::clone(&apictx.authz),
            datastore,
        );

        let (log, mut metadata) =
            OpContext::log_and_metadata_for_authn(&rqctx.log, &authn);
        OpContext::load_request_metadata(rqctx, &mut metadata).await;

        Ok(OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind: OpKind::ExternalApiRequest,
        })
    }

    /// Returns a context suitable for use in handling internal API operations
    // TODO-security this should eventually do some kind of authentication
    pub async fn for_internal_api(
        rqctx: &dropshot::RequestContext<Arc<ServerContext>>,
    ) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let apictx = rqctx.context();
        let authn = Arc::clone(&apictx.internal_authn);
        let datastore = Arc::clone(apictx.nexus.datastore());
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::clone(&apictx.authz),
            datastore,
        );

        let (log, mut metadata) =
            OpContext::log_and_metadata_for_authn(&rqctx.log, &authn);
        OpContext::load_request_metadata(rqctx, &mut metadata).await;

        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind: OpKind::InternalApiRequest,
        }
    }

    fn log_and_metadata_for_authn(
        log: &slog::Logger,
        authn: &authn::Context,
    ) -> (slog::Logger, BTreeMap<String, String>) {
        let mut metadata = BTreeMap::new();

        let log = if let Some(actor) = authn.actor() {
            let actor_id = actor.actor_id();
            let actor_type = actor.actor_type();
            metadata
                .insert(String::from("authenticated"), String::from("true"));
            metadata.insert(
                String::from("actor_type"),
                format!("{:?}", actor_type),
            );
            metadata.insert(String::from("actor_id"), actor_id.to_string());
            log.new(
                o!("authenticated" => true, "actor" => actor_id.to_string()),
            )
        } else {
            metadata
                .insert(String::from("authenticated"), String::from("false"));
            log.new(o!("authenticated" => false))
        };

        (log, metadata)
    }

    async fn load_request_metadata<T: Send + Sync + 'static>(
        rqctx: &dropshot::RequestContext<T>,
        metadata: &mut BTreeMap<String, String>,
    ) {
        let request = rqctx.request.lock().await;
        metadata.insert(String::from("request_id"), rqctx.request_id.clone());
        metadata
            .insert(String::from("http_method"), request.method().to_string());
        metadata.insert(String::from("http_uri"), request.uri().to_string());
    }

    pub fn for_saga_action<T>(
        sagactx: &steno::ActionContext<T>,
        serialized_authn: &authn::saga::Serialized,
    ) -> OpContext
    where
        T: steno::SagaType<ExecContextType = Arc<SagaContext>>,
    {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let osagactx = sagactx.user_data();
        let nexus = osagactx.nexus();
        let datastore = Arc::clone(nexus.datastore());
        let authn = Arc::new(serialized_authn.to_authn());
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::clone(&osagactx.authz()),
            datastore,
        );
        let (log, mut metadata) =
            OpContext::log_and_metadata_for_authn(osagactx.log(), &authn);

        // TODO-debugging This would be a good place to put the saga template
        // name, but we don't have it available here.  This log maybe should
        // come from steno, prepopulated with useful metadata similar to the
        // way dropshot::RequestContext does.
        let log = log.new(o!(
            "saga_node" => sagactx.node_label().to_string(),
        ));
        metadata.insert(
            String::from("saga_node"),
            sagactx.node_label().to_string(),
        );

        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata,
            kind: OpKind::Saga,
        }
    }

    /// Returns a context suitable for use in background operations in Nexus
    pub fn for_background(
        log: slog::Logger,
        authz: Arc<authz::Authz>,
        authn: authn::Context,
        datastore: Arc<DataStore>,
    ) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let authn = Arc::new(authn);
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::clone(&authz),
            Arc::clone(&datastore),
        );
        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata: BTreeMap::new(),
            kind: OpKind::Background,
        }
    }

    /// Returns a context suitable for automated tests where an OpContext is
    /// needed outside of a Dropshot context
    // Ideally this would only be exposed under `#[cfg(test)]`.  However, it's
    // used by integration tests (via `app::test_interfaces::TestInterfaces`) in
    // order to construct OpContexts that let them observe and muck with state
    // outside public interfaces.
    pub fn for_tests(
        log: slog::Logger,
        datastore: Arc<DataStore>,
    ) -> OpContext {
        let created_instant = Instant::now();
        let created_walltime = SystemTime::now();
        let authn = Arc::new(authn::Context::privileged_test_user());
        let authz = authz::Context::new(
            Arc::clone(&authn),
            Arc::new(authz::Authz::new(&log)),
            Arc::clone(&datastore),
        );
        OpContext {
            log,
            authz,
            authn,
            created_instant,
            created_walltime,
            metadata: BTreeMap::new(),
            kind: OpKind::Test,
        }
    }

    /// Check whether the actor performing this request is authorized for
    /// `action` on `resource`.
    pub async fn authorize<Resource>(
        &self,
        action: authz::Action,
        resource: &Resource,
    ) -> Result<(), Error>
    where
        Resource: AuthorizedResource + Debug + Clone,
    {
        // TODO-cleanup In an ideal world, Oso would consume &Action and
        // &Resource.  Instead, it consumes owned types.  As a result, they're
        // not available to us (even for logging) after we make the authorize()
        // call.  We work around this by cloning.
        trace!(self.log, "authorize begin";
            "actor" => ?self.authn.actor(),
            "action" => ?action,
            "resource" => ?*resource
        );
        let result = self.authz.authorize(self, action, resource.clone()).await;
        debug!(self.log, "authorize result";
            "actor" => ?self.authn.actor(),
            "action" => ?action,
            "resource" => ?*resource,
            "result" => ?result,
        );
        result
    }
}

#[cfg(test)]
mod test {
    use super::OpContext;
    use crate::authn;
    use crate::authz;
    use authz::Action;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::Error;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_background_context() {
        let logctx = dev::test_setup_log("test_background_context");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let opctx = OpContext::for_background(
            logctx.log.new(o!()),
            Arc::new(authz::Authz::new(&logctx.log)),
            authn::Context::internal_unauthenticated(),
            datastore,
        );

        // This is partly a test of the authorization policy.  Today, background
        // contexts should have no privileges.  That's misleading because in
        // fact they do a bunch of privileged things, but we haven't yet added
        // privilege checks to those code paths.  Eventually we'll probably want
        // to define a particular internal user (maybe even a different one for
        // different background contexts) with specific privileges and test
        // those here.
        //
        // For now, we check what we currently expect, which is that this
        // context has no official privileges.
        let error = opctx
            .authorize(Action::Query, &authz::DATABASE)
            .await
            .expect_err("expected authorization error");
        assert!(matches!(error, Error::Unauthenticated { .. }));
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_test_context() {
        let logctx = dev::test_setup_log("test_background_context");
        let mut db = test_setup_database(&logctx.log).await;
        let (_, datastore) =
            crate::db::datastore::datastore_test(&logctx, &db).await;
        let opctx = OpContext::for_tests(logctx.log.new(o!()), datastore);

        // Like in test_background_context(), this is essentially a test of the
        // authorization policy.  The unit tests assume this user can do
        // basically everything.  We don't need to verify that -- the tests
        // themselves do that -- but it's useful to have a basic santiy test
        // that we can construct such a context it's authorized to do something.
        opctx
            .authorize(Action::Query, &authz::DATABASE)
            .await
            .expect("expected authorization to succeed");
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}

#[async_trait]
impl authn::external::spoof::SpoofContext for Arc<ServerContext> {
    async fn silo_user_silo(
        &self,
        silo_user_id: Uuid,
    ) -> Result<Uuid, authn::Reason> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.lookup_silo_for_authn(opctx, silo_user_id).await
    }
}

#[async_trait]
impl SessionStore for Arc<ServerContext> {
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

impl Session for ConsoleSessionWithSiloId {
    fn silo_user_id(&self) -> Uuid {
        self.console_session.silo_user_id
    }
    fn silo_id(&self) -> Uuid {
        self.silo_id
    }
    fn time_last_used(&self) -> DateTime<Utc> {
        self.console_session.time_last_used
    }
    fn time_created(&self) -> DateTime<Utc> {
        self.console_session.time_created
    }
}
