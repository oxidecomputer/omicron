/*!
 * Describes the endpoints and handler functions in your API
 */

use crate::handler::{HttpHandlerFunc, HttpResponseWrap};
use crate::router::HttpRouter;
use crate::{Extractor, HttpRouteHandler, RouteHandler};

use http::Method;
use std::collections::HashSet;

/**
 * ApiEndpoint represents a single API endpoint associated with an
 * ApiDescription. It has a handler, HTTP method (e.g. GET, POST), and a path--
 * provided explicitly--as well as parameters and a description which can be
 * inferred from function parameter types and doc comments (respectively).
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

/**
 * ApiEndpointParameter the discrete path and query parameters for a given API
 * endpoint. These are typically derived from the members of stucts used as
 * parameters to handler functions.
 */
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
    pub fn register<'a, T>(&mut self, endpoint: T) -> Result<(), String>
    where
        T: Into<ApiEndpoint>,
    {
        let e = endpoint.into();

        // Gather up the path parameters and the path variable components, and
        // make sure they're identical.
        let path = e
            .path
            .split("/")
            .filter_map(|segment| {
                if segment.starts_with("{") && segment.ends_with("}") {
                    Some(&segment[1..segment.len() - 1])
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();
        let vars = e
            .parameters
            .iter()
            .filter_map(|p| match p.inn {
                ApiEndpointParameterLocation::Path => Some(p.name.as_str()),
                _ => None,
            })
            .collect::<HashSet<_>>();

        if path != vars {
            let mut p = path
                .difference(&vars)
                .into_iter()
                .map(|s| *s)
                .collect::<Vec<_>>();
            let mut v = vars
                .difference(&path)
                .into_iter()
                .map(|s| *s)
                .collect::<Vec<_>>();
            p.sort();
            v.sort();

            return match (p.is_empty(), v.is_empty()) {
                (false, true) => Err(format!(
                    "{} {}",
                    "path parameters are not consumed",
                    p.join(","),
                )),
                (true, false) => Err(format!(
                    "{} {}",
                    "specified parameters do not appear in the path",
                    v.join(",")
                )),
                _ => Err(format!(
                    "{} {} and {} {}",
                    "path parameters are not consumed",
                    p.join(","),
                    "specified parameters do not appear in the path",
                    v.join(",")
                )),
            };
        }

        self.router.insert(e);

        Ok(())
    }

    /**
     * Emit the OpenAPI Spec document describing this API in its JSON form.
     */
    pub fn print_openapi(&self) {
        let mut openapi = openapiv3::OpenAPI::default();

        for (path, method, endpoint) in &self.router {
            let path = openapi.paths.entry(path).or_insert(
                openapiv3::ReferenceOr::Item(openapiv3::PathItem::default()),
            );

            let pathitem = match path {
                openapiv3::ReferenceOr::Item(ref mut item) => item,
                _ => panic!("reference not expected"),
            };

            let method_ref = match &method[..] {
                "GET" => &mut pathitem.get,
                "PUT" => &mut pathitem.put,
                "POST" => &mut pathitem.post,
                "DELETE" => &mut pathitem.delete,
                "OPTIONS" => &mut pathitem.options,
                "HEAD" => &mut pathitem.head,
                "PATCH" => &mut pathitem.patch,
                "TRACE" => &mut pathitem.trace,
                other => panic!("unexpected method `{}`", other),
            };
            let mut operation = openapiv3::Operation::default();
            operation.description = endpoint.description.clone();

            operation.parameters = endpoint
                .parameters
                .iter()
                .map(|param| {
                    let parameter_data = openapiv3::ParameterData {
                        name: param.name.clone(),
                        description: param.description.clone(),
                        required: true,
                        deprecated: None,
                        format: openapiv3::ParameterSchemaOrContent::Schema(
                            openapiv3::ReferenceOr::Item(openapiv3::Schema {
                                schema_data: openapiv3::SchemaData::default(),
                                schema_kind: openapiv3::SchemaKind::Type(
                                    openapiv3::Type::String(
                                        openapiv3::StringType::default(),
                                    ),
                                ),
                            }),
                        ),
                        example: None,
                        examples: indexmap::map::IndexMap::new(),
                    };
                    match param.inn {
                        ApiEndpointParameterLocation::Query => {
                            openapiv3::ReferenceOr::Item(
                                openapiv3::Parameter::Query {
                                    parameter_data: parameter_data,
                                    allow_reserved: true,
                                    style: openapiv3::QueryStyle::Form,
                                    allow_empty_value: None,
                                },
                            )
                        }
                        ApiEndpointParameterLocation::Path => {
                            openapiv3::ReferenceOr::Item(
                                openapiv3::Parameter::Path {
                                    parameter_data: parameter_data,
                                    style: openapiv3::PathStyle::Simple,
                                },
                            )
                        }
                    }
                })
                .collect::<Vec<_>>();

            method_ref.replace(operation);
        }

        println!("{}", serde_json::to_string_pretty(&openapi).unwrap());
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

#[cfg(test)]
mod test {
    use super::super::error::HttpError;
    use super::super::handler::HttpRouteHandler;
    use super::super::handler::RequestContext;
    use super::super::handler::RouteHandler;
    use super::super::ExtractedParameter;
    use super::super::Path;
    use super::ApiDescription;
    use super::ApiEndpoint;
    use http::Method;
    use hyper::Body;
    use hyper::Response;
    use serde::Deserialize;
    use std::sync::Arc;

    async fn test_handler(
        _: Arc<RequestContext>,
    ) -> Result<Response<Body>, HttpError> {
        panic!("test handler is not supposed to run");
    }

    fn new_handler_named(name: &str) -> Box<dyn RouteHandler> {
        HttpRouteHandler::new_with_name(test_handler, name)
    }

    fn new_endpoint(
        handler: Box<dyn RouteHandler>,
        method: Method,
        path: &str,
    ) -> ApiEndpoint {
        ApiEndpoint {
            handler: handler,
            method: method,
            path: path.to_string(),
            parameters: vec![],
            description: None,
        }
    }

    #[test]
    fn test_openapi() -> Result<(), String> {
        let mut api = ApiDescription::new();
        api.register(new_endpoint(
            new_handler_named("root_get"),
            Method::GET,
            "/",
        ))?;
        api.register(new_endpoint(
            new_handler_named("root_post"),
            Method::POST,
            "/",
        ))?;

        api.print_openapi();

        Ok(())
    }

    #[derive(Deserialize, ExtractedParameter)]
    #[dropshot(crate = "super::super")]
    #[allow(dead_code)]
    struct TestPath {
        a: String,
        b: String,
    }

    async fn test_badpath_handler(
        _: Arc<RequestContext>,
        _: Path<TestPath>,
    ) -> Result<Response<Body>, HttpError> {
        panic!("test handler is not supposed to run");
    }

    #[test]
    fn test_badpath1() {
        let mut api = ApiDescription::new();
        let ret = api.register(ApiEndpoint::new(
            test_badpath_handler,
            Method::GET,
            "/",
        ));
        assert_eq!(
            ret,
            Err("specified parameters do not appear in the path a,b"
                .to_string())
        )
    }

    #[test]
    fn test_badpath2() {
        let mut api = ApiDescription::new();
        let ret = api.register(ApiEndpoint::new(
            test_badpath_handler,
            Method::GET,
            "/{a}/{aa}/{b}/{bb}",
        ));
        assert_eq!(
            ret,
            Err("path parameters are not consumed aa,bb".to_string())
        );
    }

    #[test]
    fn test_badpath3() {
        let mut api = ApiDescription::new();
        let ret = api.register(ApiEndpoint::new(
            test_badpath_handler,
            Method::GET,
            "/{c}/{d}",
        ));
        assert_eq!(
            ret,
            Err("path parameters are not consumed c,d and specified \
                 parameters do not appear in the path a,b"
                .to_string())
        );
    }
}
