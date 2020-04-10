/*!
 * Describes the endpoints and handler functions in your API
 */

use crate::handler::{HttpHandlerFunc, HttpResponseWrap};
use crate::router::HttpRouter;
use crate::{Extractor, HttpRouteHandler, RouteHandler};

use http::Method;

/**
 * An Endpoint represents a single API endpoint associated with an
 * ApiDescription.
 *
 * TODO: it will need to have parameter information TBD
 */
#[derive(Debug)]
pub struct Endpoint<'a> {
    pub handler: Box<dyn RouteHandler>,
    pub method: Method,
    pub path: &'a str,
    pub parameters: Vec<EndpointParameter>,
}

impl<'a> Endpoint<'a> {
    pub fn new<HandlerType, FuncParams, ResponseType>(
        handler: HandlerType,
        method: Method,
        path: &'a str,
    ) -> Self
    where
        HandlerType: HttpHandlerFunc<FuncParams, ResponseType>,
        FuncParams: Extractor + 'static,
        ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
    {
        Endpoint {
            handler: HttpRouteHandler::new(handler),
            method: method,
            path: path,
            parameters: FuncParams::generate(),
        }
    }
}

#[derive(Debug)]
pub struct EndpointParameter {
    pub name: String,
    pub inn: EndpointParameterLocation,
    pub description: Option<String>,
    pub required: bool,
    // TODO: schema
    pub examples: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum EndpointParameterLocation {
    Path,
    Query,
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
        self.router.insert(e.method, &e.path, e.handler, e.parameters)
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
