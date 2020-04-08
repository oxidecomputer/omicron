/*!
 * Describes the endpoints and handler functions in your API
 */

use crate::router::HttpRouter;
use crate::RouteHandler;
use http::Method;

#[derive(Debug)]
/**
 * An Endpoint represents a single API endpoint associated with an
 * ApiDescription.
 *
 * TODO: it will need to have parameter information TBD
 */
pub struct Endpoint<'a> {
    pub handler: Box<dyn RouteHandler>,
    pub method: Method,
    pub path: &'a str,
}

impl<'a> Endpoint<'a> {
    pub fn new(
        handler: Box<dyn RouteHandler>,
        method: Method,
        path: &'a str,
    ) -> Self {
        Endpoint {
            handler,
            method,
            path,
        }
    }
}

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

    /**
     * Register a new API endpoint.
     */
    pub fn register<'a, T>(&mut self, endpoint: T)
    where
        T: Into<Endpoint<'a>>,
    {
        let e = endpoint.into();
        self.router.insert(e.method.clone(), &e.path, e.handler)
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
