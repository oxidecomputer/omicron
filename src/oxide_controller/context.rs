/*!
 * Shared state used by API request handlers
 */
use super::OxideController;
use super::OxideControllerServer;

use dropshot::RequestContext;
use slog::Logger;
use std::any::Any;
use std::sync::Arc;
use uuid::Uuid;

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.
 */
pub struct ControllerServerContext {
    pub controller: Arc<OxideController>,
    pub log: Logger,
}

impl ControllerServerContext {
    pub fn new(rack_id: &Uuid, log: Logger) -> Arc<ControllerServerContext> {
        Arc::new(ControllerServerContext {
            controller: Arc::new(OxideController::new_with_id(
                rack_id,
                log.new(o!("component" => "controller")),
            )),
            log: log,
        })
    }

    /**
     * Retrieves our API-specific context out of the generic RequestContext
     * structure
     */
    pub fn from_request(
        rqctx: &Arc<RequestContext>,
    ) -> Arc<ControllerServerContext> {
        Self::from_private(Arc::clone(&rqctx.server.private))
    }

    /**
     * Retrieves our API-specific context out of the generic HttpServer
     * structure.
     */
    pub fn from_server(
        server: &OxideControllerServer,
    ) -> Arc<ControllerServerContext> {
        Self::from_private(server.http_server_external.app_private())
    }

    /**
     * Retrieves our API-specific context from the generic one stored in
     * Dropshot.
     */
    fn from_private(
        ctx: Arc<dyn Any + Send + Sync + 'static>,
    ) -> Arc<ControllerServerContext> {
        /*
         * It should not be possible for this downcast to fail unless the caller
         * has passed us a RequestContext from a totally different HttpServer
         * or a totally different HttpServer itself (in either case created with
         * a different type for its private data).  This seems quite unlikely in
         * practice.
         * TODO-cleanup: can we make this API statically type-safe?
         */
        ctx.downcast::<ControllerServerContext>()
            .expect("ApiContext: wrong type for private data")
    }
}
