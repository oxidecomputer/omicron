/*!
 * Shared state used by API request handlers
 */
use super::OxideController;

use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Shared state available to all API request handlers
 */
pub struct ControllerServerContext {
    /** reference to the underlying OXC */
    pub controller: Arc<OxideController>,
    /** debug log */
    pub log: Logger,
}

impl ControllerServerContext {
    /**
     * Create a new context with the given rack id and log.  This creates the
     * underlying OXC as well.
     */
    pub fn new(rack_id: &Uuid, log: Logger) -> Arc<ControllerServerContext> {
        Arc::new(ControllerServerContext {
            controller: Arc::new(OxideController::new_with_id(
                rack_id,
                log.new(o!("component" => "controller")),
            )),
            log,
        })
    }
}
