use super::handler::RouteHandler;
use http::Method;

pub trait Endpoint {
    fn get(&self) -> &EndpointInfo;
}

#[derive(Debug)]
pub struct EndpointInfo {
    /** Caller-supplied handler */
    pub handler: Box<dyn RouteHandler>,
    pub method: Method,
    pub path: String,
    pub parameters: Vec<EndpointParameter>,
}

#[derive(Debug)]
pub struct EndpointParameter {
    // everything I need for OpenAPI
    name: String,
    location: EndpointParameterLocation,
    description: Option<String>,
}

#[derive(Debug)]
pub enum EndpointParameterLocation {
    Cookie,
    Header,
    Path,
    Query,
}

impl Endpoint for EndpointInfo {
    fn get(&self) -> &EndpointInfo {
        self
    }
}
