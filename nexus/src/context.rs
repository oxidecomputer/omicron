// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared state used by API request handlers
use super::Nexus;
use crate::saga_interface::SagaContext;
use async_trait::async_trait;
use authn::external::HttpAuthnScheme;
use authn::external::session_cookie::HttpAuthnSessionCookie;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::token::HttpAuthnToken;
use camino::Utf8PathBuf;
use chrono::Duration;
use nexus_config::NexusConfig;
use nexus_config::OmdbConfig;
use nexus_config::SchemeName;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::authn::ConsoleSessionWithSiloId;
use nexus_db_queries::authn::external::session_cookie::SessionStore;
use nexus_db_queries::context::{OpContext, OpKind};
use nexus_db_queries::{authn, authz, db};
use omicron_common::address::{AZ_PREFIX, Ipv6Subnet};
use omicron_uuid_kinds::ConsoleSessionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use oximeter::types::ProducerRegistry;
use oximeter_instruments::http::{HttpService, LatencyTracker};
use slog::Logger;
use std::env;
use std::sync::Arc;
use uuid::Uuid;

/// Indicates the kind of HTTP server.
#[derive(Clone, Copy)]
pub enum ServerKind {
    /// This serves the internal API.
    Internal,
    /// This serves the external API over the normal public network.
    External,
    /// This serves the external API proxied over the technician port.
    Techport,
}

/// The API context for each distinct Dropshot server.
///
/// This packages up the main server context, which is shared by all API servers
/// (e.g., internal, external, and techport). It also includes the
/// [`ServerKind`], which makes it possible to know which server is handling any
/// particular request.
#[derive(Clone)]
pub struct ApiContext {
    /// The kind of server.
    pub kind: ServerKind,
    /// Shared state available to all endpoint handlers.
    pub context: Arc<ServerContext>,
}

impl ApiContext {
    /// Create a new context with a rack ID and logger. This creates the
    /// underlying `Nexus` as well.
    pub async fn for_internal(
        rack_id: Uuid,
        log: Logger,
        config: &NexusConfig,
    ) -> Result<Self, String> {
        ServerContext::new(rack_id, log, config)
            .await
            .map(|context| Self { kind: ServerKind::Internal, context })
    }

    /// Clone self for use by the external Dropshot server.
    pub fn for_external(&self) -> Self {
        Self { kind: ServerKind::External, context: self.context.clone() }
    }

    /// Clone self for use by the techport Dropshot server.
    pub fn for_techport(&self) -> Self {
        Self { kind: ServerKind::Techport, context: self.context.clone() }
    }
}

impl std::borrow::Borrow<ServerContext> for ApiContext {
    fn borrow(&self) -> &ServerContext {
        &self.context
    }
}

/// Shared state available to all API request handlers
pub struct ServerContext {
    /// reference to the underlying nexus
    pub nexus: Arc<Nexus>,
    /// debug log
    #[allow(dead_code)]
    log: Logger,
    /// authenticator for external HTTP requests
    pub(crate) external_authn: authn::external::Authenticator<ServerContext>,
    /// authentication context used for internal HTTP requests
    pub(crate) internal_authn: Arc<authn::Context>,
    /// authorizer
    pub(crate) authz: Arc<authz::Authz>,
    /// internal API request latency tracker
    pub(crate) internal_latencies: LatencyTracker,
    /// external API request latency tracker
    pub(crate) external_latencies: LatencyTracker,
    /// registry of metric producers
    pub(crate) producer_registry: ProducerRegistry,
    /// TLS enabled on the external Dropshot server
    pub(crate) external_tls_enabled: bool,
    /// tunable settings needed for the console at runtime
    pub(crate) console_config: ConsoleConfig,
    /// config supporting `omdb` system introspection
    pub(crate) omdb_config: Option<OmdbConfig>,
}

pub(crate) struct ConsoleConfig {
    /// how long a session can be idle before expiring
    pub session_idle_timeout: Duration,
    /// how long a session can exist before expiring
    pub session_absolute_timeout: Duration,
    /// directory containing static file to serve
    pub static_dir: Option<Utf8PathBuf>,
}

impl ServerContext {
    /// Create a new context with the given rack id and log.  This creates the
    /// underlying nexus as well.
    pub async fn new(
        rack_id: Uuid,
        log: Logger,
        config: &NexusConfig,
    ) -> Result<Arc<ServerContext>, String> {
        let nexus_schemes = config
            .pkg
            .authn
            .schemes_external
            .iter()
            .map::<Box<dyn HttpAuthnScheme<ServerContext>>, _>(
                |name| match name {
                    SchemeName::Spoof => Box::new(HttpAuthnSpoof),
                    SchemeName::SessionCookie => {
                        Box::new(HttpAuthnSessionCookie)
                    }
                    SchemeName::AccessToken => Box::new(HttpAuthnToken),
                },
            )
            .collect();
        let external_authn = authn::external::Authenticator::new(nexus_schemes);
        let internal_authn = Arc::new(authn::Context::internal_api());
        let authz = Arc::new(authz::Authz::new(&log));
        let create_tracker = |name: &str| {
            let target = HttpService {
                name: name.to_string().into(),
                id: config.deployment.id.into_untyped_uuid(),
            };
            // Start at 1 microsecond == 1e3 nanoseconds.
            const LATENCY_START_POWER: u16 = 3;
            // End at 1000s == (1e9 * 1e3) == 1e12 nanoseconds.
            const LATENCY_END_POWER: u16 = 12;
            LatencyTracker::with_log_linear_bins(
                target,
                LATENCY_START_POWER,
                LATENCY_END_POWER,
            )
            .unwrap()
        };
        let internal_latencies = create_tracker("nexus-internal");
        let external_latencies = create_tracker("nexus-external");
        let producer_registry =
            ProducerRegistry::with_id(config.deployment.id.into_untyped_uuid());
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
            match env::current_dir() {
                Ok(root) => match Utf8PathBuf::try_from(root) {
                    Ok(root) => Some(root.join(&config.pkg.console.static_dir)),
                    Err(err) => {
                        error!(
                            log,
                            "Failed to convert current directory to UTF-8, \
                                         setting assets dir to None: {}",
                            err
                        );
                        None
                    }
                },
                Err(error) => {
                    error!(
                        log,
                        "Failed to get current directory, \
                         setting assets dir to None: {}",
                        error
                    );
                    None
                }
            }
        };

        // We don't want to fail outright yet, but we do want to try to make
        // problems slightly easier to debug.
        if static_dir.is_none() {
            error!(
                log,
                "No assets directory configured. All console page and asset requests will 404."
            );
        }

        // TODO: check that asset directory exists, check for particular assets
        // like console index.html. leaving that out for now so we don't break
        // nexus in dev for everyone

        // Set up DNS Client (both traditional and qorb-based, until we've moved
        // every consumer over to qorb)
        let (resolver, qorb_resolver) = match config.deployment.internal_dns {
            nexus_config::InternalDns::FromSubnet { subnet } => {
                let az_subnet =
                    Ipv6Subnet::<AZ_PREFIX>::new(subnet.net().addr());
                info!(
                    log,
                    "Setting up resolver using DNS servers for subnet: {:?}",
                    az_subnet
                );
                let resolver =
                    internal_dns_resolver::Resolver::new_from_subnet(
                        log.new(o!("component" => "DnsResolver")),
                        az_subnet,
                    )
                    .map_err(|e| {
                        format!("Failed to create DNS resolver: {}", e)
                    })?;
                let qorb_resolver = internal_dns_resolver::QorbResolver::new(
                    internal_dns_resolver::Resolver::servers_from_subnet(
                        az_subnet,
                    ),
                );

                (resolver, qorb_resolver)
            }
            nexus_config::InternalDns::FromAddress { address } => {
                info!(
                    log,
                    "Setting up resolver using DNS address: {:?}", address
                );

                let resolver = internal_dns_resolver::Resolver::new_from_addrs(
                    log.new(o!("component" => "DnsResolver")),
                    &[address],
                )
                .map_err(|e| format!("Failed to create DNS resolver: {}", e))?;
                let qorb_resolver =
                    internal_dns_resolver::QorbResolver::new(vec![address]);

                (resolver, qorb_resolver)
            }
        };

        // Once this database pool is created, it spawns workers which will
        // be continually attempting to access database backends.
        //
        // It must be explicitly terminated, so be cautious about returning
        // results beyond this point.
        let pool = match &config.deployment.database {
            nexus_config::Database::FromUrl { url } => {
                info!(
                    log, "Setting up qorb database pool from a single host";
                    "url" => #?url,
                );
                db::Pool::new_single_host(
                    &log,
                    &db::Config { url: url.clone() },
                )
            }
            nexus_config::Database::FromDns => {
                info!(
                    log, "Setting up qorb database pool from DNS";
                    "dns_addrs" => ?qorb_resolver.bootstrap_dns_ips(),
                );
                db::Pool::new(&log, &qorb_resolver)
            }
        };

        let pool = Arc::new(pool);
        let nexus = match Nexus::new_with_id(
            rack_id,
            log.new(o!("component" => "nexus")),
            resolver,
            qorb_resolver,
            pool.clone(),
            &producer_registry,
            config,
            Arc::clone(&authz),
        )
        .await
        {
            Ok(nexus) => nexus,
            Err(err) => {
                pool.terminate().await;
                return Err(err);
            }
        };

        Ok(Arc::new(ServerContext {
            nexus,
            log,
            external_authn,
            internal_authn,
            authz,
            internal_latencies,
            external_latencies,
            producer_registry,
            external_tls_enabled: config.deployment.dropshot_external.tls,
            console_config: ConsoleConfig {
                session_idle_timeout: Duration::minutes(
                    config.pkg.console.session_idle_timeout_minutes.into(),
                ),
                session_absolute_timeout: Duration::minutes(
                    config.pkg.console.session_absolute_timeout_minutes.into(),
                ),
                static_dir,
            },
            omdb_config: config.pkg.omdb.clone(),
        }))
    }
}

/// Authenticates an incoming request to the external API and produces a new
/// operation context for it
pub(crate) async fn op_context_for_external_api(
    rqctx: &dropshot::RequestContext<ApiContext>,
) -> Result<OpContext, dropshot::HttpError> {
    let apictx = rqctx.context();
    OpContext::new_async(
        &rqctx.log,
        async {
            let authn = Arc::new(
                apictx.context.external_authn.authn_request(rqctx).await?,
            );
            let datastore = Arc::clone(apictx.context.nexus.datastore());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&apictx.context.authz),
                datastore,
            );
            Ok((authn, authz))
        },
        |metadata| OpContext::load_request_metadata(rqctx, metadata),
        OpKind::ExternalApiRequest,
    )
    .await
}

pub(crate) async fn op_context_for_internal_api(
    rqctx: &dropshot::RequestContext<ApiContext>,
) -> OpContext {
    let apictx = &rqctx.context();
    OpContext::new_async(
        &rqctx.log,
        async {
            let authn = Arc::clone(&apictx.context.internal_authn);
            let datastore = Arc::clone(apictx.context.nexus.datastore());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&apictx.context.authz),
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

pub(crate) fn op_context_for_saga_action<T>(
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
impl authn::external::AuthenticatorContext for ServerContext {
    async fn silo_authn_policy_for(
        &self,
        actor: &authn::Actor,
    ) -> Result<
        Option<authn::SiloAuthnPolicy>,
        omicron_common::api::external::Error,
    > {
        let Some(silo_id) = actor.silo_id() else { return Ok(None) };

        // TODO-performance In general, this could almost always use a
        // nexus_db_model::Silo from the ExternalEndpoints subsystem instead of
        // doing an explicit database lookup here.  However, that's potentially
        // out of date (e.g., immediately after creating a Silo), so we'd have
        // to fallback to an explicit lookup.
        let opctx = self.nexus.opctx_external_authn();
        let datastore = self.nexus.datastore();
        let (_, db_silo) =
            LookupPath::new(opctx, datastore).silo_id(silo_id).fetch().await?;
        let silo_authn_policy = authn::SiloAuthnPolicy::try_from(&db_silo)?;
        Ok(Some(silo_authn_policy))
    }
}

#[async_trait]
impl authn::external::SiloUserSilo for ServerContext {
    async fn silo_user_silo(
        &self,
        silo_user_id: SiloUserUuid,
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
        id: ConsoleSessionUuid,
    ) -> Option<Self::SessionModel> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.session_update_last_used(&opctx, id).await.ok()
    }

    async fn session_expire(&self, token: String) -> Option<()> {
        let opctx = self.nexus.opctx_external_authn();
        self.nexus.session_hard_delete_by_token(opctx, token).await.ok()
    }

    fn session_idle_timeout(&self) -> Duration {
        self.console_config.session_idle_timeout
    }

    fn session_absolute_timeout(&self) -> Duration {
        self.console_config.session_absolute_timeout
    }
}
