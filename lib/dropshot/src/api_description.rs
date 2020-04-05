/*!
 * Describes the endpoints and handler functions in your API
 */

use crate::router::HttpRouter;
use crate::Endpoint;
use crate::RouteHandler;
use http::Method;

/**
 * An ApiDescription represents the endpoints and handler functions in your API.
 * Other metadata could also be provided here.  This object can be used to
 * generate an OpenAPI spec or to run an HTTP server implementing the API.
 */
pub struct ApiDescription {
    /** In practice, all the information we need is encoded in the router. */
    router: HttpRouter,
}

impl ApiDescription {
    pub fn new() -> Self {
        ApiDescription {
            router: HttpRouter::new(),
        }
    }

    pub fn register(
        &mut self,
        method: Method,
        path: &str,
        handler: Box<dyn RouteHandler>,
    ) {
        self.router.insert(method, path, handler);
    }

    pub fn register2(&mut self, endpoint: &dyn Endpoint) {
        let foo = endpoint.get();
        self.router.insert2(endpoint.get());
    }

    pub fn print_openapi(&self) {
        self.router.print_openapi();
    }

    /*
     * TODO-cleanup is there a way to make this available only within this
     * crate?  Once we do that, we don't need to consume the ApiDescription to
     * do this.
     */
    pub fn into_router(self) -> HttpRouter {
        self.router
    }
}
