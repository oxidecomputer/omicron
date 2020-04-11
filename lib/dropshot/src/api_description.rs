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
 */
#[derive(Debug)]
pub struct ApiEndpoint {
    pub handler: Box<dyn RouteHandler>,
    pub method: Method,
    pub path: String,
    pub parameters: Vec<ApiEndpointParameter>,
    pub description: Option<String>,
}

impl<'a> ApiEndpoint {
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
        ApiEndpoint {
            handler: HttpRouteHandler::new(handler),
            method: method,
            path: path.to_string(),
            parameters: FuncParams::generate(),
            description: None,
        }
    }
}

#[derive(Debug)]
pub struct ApiEndpointParameter {
    pub name: String,
    pub inn: ApiEndpointParameterLocation,
    pub description: Option<String>,
    pub required: bool,
    // TODO: schema
    pub examples: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum ApiEndpointParameterLocation {
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
        T: Into<ApiEndpoint>,
    {
        let e = endpoint.into();
        self.router.insert(e);
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
