/*!
 * Shared state used by API request handlers
 */
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
    ) -> Arc<ServerContext> {
        Arc::new(ServerContext {
            nexus: Arc::new(Nexus::new_with_id(
                rack_id,
                log.new(o!("component" => "nexus")),
                pool,
            )),
            log,
        })
    }
}
