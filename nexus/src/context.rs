/*!
 * Shared state used by API request handlers
 */
use super::authn;
use super::config;
use super::db;
use super::Nexus;

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
    pub http_authn: authn::HttpAuthn,
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
        let http_authn = authn::HttpAuthn::new(&config.authn_modes_external);
        Arc::new(ServerContext {
            nexus: Nexus::new_with_id(
                rack_id,
                log.new(o!("component" => "nexus")),
                pool,
                config,
            ),
            log,
            http_authn,
        })
    }
}
