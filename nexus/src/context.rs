/*!
 * Shared state used by API request handlers
 */
use super::authn;
use super::config;
use super::db;
use super::Nexus;
use authn::external::spoof::HttpAuthnSpoof;
use authn::external::HttpAuthnScheme;
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
        let external_authn =
            authn::external::Authenticator::new(nexus_schemes);
        Arc::new(ServerContext {
            nexus: Nexus::new_with_id(
                rack_id,
                log.new(o!("component" => "nexus")),
                pool,
                config,
            ),
            log,
            external_authn,
        })
    }
}
