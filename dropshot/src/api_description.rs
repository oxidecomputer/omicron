/*!
 * Describes the endpoints and handler functions in your API
 */

use crate::handler::HttpHandlerFunc;
use crate::handler::HttpResponseWrap;
use crate::handler::HttpRouteHandler;
use crate::handler::RouteHandler;
use crate::router::path_to_segments;
use crate::router::HttpRouter;
use crate::router::PathSegment;
use crate::Extractor;

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
 * ApiEndpointParameter represents the discrete path and query parameters for a
 * given API endpoint. These are typically derived from the members of stucts
 * used as parameters to handler functions.
 */
#[derive(Debug)]
pub struct ApiEndpointParameter {
    pub name: ApiEndpointParameterName,
    pub description: Option<String>,
    pub required: bool,
    pub schema: Option<schemars::schema::Schema>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum ApiEndpointParameterLocation {
    Path,
    Query,
}

#[derive(Debug, Clone)]
pub enum ApiEndpointParameterName {
    Path(String),
    Query(String),
    Body,
}

impl From<(ApiEndpointParameterLocation, String)> for ApiEndpointParameterName {
    fn from((location, name): (ApiEndpointParameterLocation, String)) -> Self {
        match location {
            ApiEndpointParameterLocation::Path => {
                ApiEndpointParameterName::Path(name)
            }
            ApiEndpointParameterLocation::Query => {
                ApiEndpointParameterName::Query(name)
            }
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
    pub fn register<'a, T>(&mut self, endpoint: T) -> Result<(), String>
    where
        T: Into<ApiEndpoint>,
    {
        let e = endpoint.into();

        // Gather up the path parameters and the path variable components, and
        // make sure they're identical.
        let path = path_to_segments(&e.path)
            .iter()
            .filter_map(|segment| match PathSegment::from(segment) {
                PathSegment::Varname(v) => Some(v),
                _ => None,
            })
            .collect::<HashSet<_>>();
        let vars = e
            .parameters
            .iter()
            .filter_map(|p| match &p.name {
                ApiEndpointParameterName::Path(name) => Some(name.clone()),
                _ => None,
            })
            .collect::<HashSet<_>>();

        if path != vars {
            let mut p = path.difference(&vars).into_iter().collect::<Vec<_>>();
            let mut v = vars.difference(&path).into_iter().collect::<Vec<_>>();
            p.sort();
            v.sort();

            let pp =
                p.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(",");
            let vv =
                v.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(",");

            return match (p.is_empty(), v.is_empty()) {
                (false, true) => Err(format!(
                    "{} ({})",
                    "path parameters are not consumed", pp,
                )),
                (true, false) => Err(format!(
                    "{} ({})",
                    "specified parameters do not appear in the path", vv,
                )),
                _ => Err(format!(
                    "{} ({}) and {} ({})",
                    "path parameters are not consumed",
                    pp,
                    "specified parameters do not appear in the path",
                    vv,
                )),
            };
        }

        self.router.insert(e);

        Ok(())
    }

    /**
     * Emit the OpenAPI Spec document describing this API in its JSON form.
     */
    // TODO: There's a bunch of error handling we need here such as checking
    // for duplicate parameter names.
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
                .filter_map(|param| {
                    let (name, location) = match &param.name {
                        ApiEndpointParameterName::Body => return None,
                        ApiEndpointParameterName::Path(name) => {
                            (name, ApiEndpointParameterLocation::Path)
                        }
                        ApiEndpointParameterName::Query(name) => {
                            (name, ApiEndpointParameterLocation::Query)
                        }
                    };
                    let parameter_data = openapiv3::ParameterData {
                        name: name.clone(),
                        description: param.description.clone(),
                        required: true,
                        deprecated: None,
                        format: openapiv3::ParameterSchemaOrContent::Schema(
                            openapiv3::ReferenceOr::Item(openapiv3::Schema {
                                schema_data: openapiv3::SchemaData::default(),
                                // TODO we shouldn't be hard-coding string here
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
                    match location {
                        ApiEndpointParameterLocation::Query => {
                            Some(openapiv3::ReferenceOr::Item(
                                openapiv3::Parameter::Query {
                                    parameter_data: parameter_data,
                                    allow_reserved: true,
                                    style: openapiv3::QueryStyle::Form,
                                    allow_empty_value: None,
                                },
                            ))
                        }
                        ApiEndpointParameterLocation::Path => {
                            Some(openapiv3::ReferenceOr::Item(
                                openapiv3::Parameter::Path {
                                    parameter_data: parameter_data,
                                    style: openapiv3::PathStyle::Simple,
                                },
                            ))
                        }
                    }
                })
                .collect::<Vec<_>>();

            operation.request_body = endpoint
                .parameters
                .iter()
                .filter_map(|param| {
                    match &param.name {
                        ApiEndpointParameterName::Body => (),
                        _ => return None,
                    }

                    let schema = param.schema.as_ref().map(j2oas_schema);

                    let mut content = indexmap::map::IndexMap::new();
                    content.insert(
                        "application/json".to_string(),
                        openapiv3::MediaType {
                            schema: schema,
                            example: None,
                            examples: indexmap::map::IndexMap::new(),
                            encoding: indexmap::map::IndexMap::new(),
                        },
                    );

                    Some(openapiv3::ReferenceOr::Item(openapiv3::RequestBody {
                        description: None,
                        content: content,
                        required: true,
                    }))
                })
                .nth(0);

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

fn j2oas_schema(
    schema: &schemars::schema::Schema,
) -> openapiv3::ReferenceOr<openapiv3::Schema> {
    match schema {
        schemars::schema::Schema::Bool(_) => todo!(),
        schemars::schema::Schema::Object(obj) => j2oas_schema_object(obj),
    }
}

fn j2oas_schema_object(
    obj: &schemars::schema::SchemaObject,
) -> openapiv3::ReferenceOr<openapiv3::Schema> {
    if let Some(reference) = &obj.reference {
        return openapiv3::ReferenceOr::Reference {
            reference: reference.clone(),
        };
    }

    let ty = match &obj.instance_type {
        Some(schemars::schema::SingleOrVec::Single(ty)) => Some(ty.as_ref()),
        Some(schemars::schema::SingleOrVec::Vec(_)) => {
            unimplemented!("unsupported by openapiv3")
        }
        None => None,
    };

    let kind = match (ty, &obj.subschemas) {
        (Some(schemars::schema::InstanceType::Null), None) => todo!(),
        (Some(schemars::schema::InstanceType::Boolean), None) => todo!(),
        (Some(schemars::schema::InstanceType::Object), None) => {
            j2oas_object(&obj.object)
        }
        (Some(schemars::schema::InstanceType::Array), None) => {
            j2oas_array(&obj.array)
        }
        (Some(schemars::schema::InstanceType::Number), None) => {
            j2oas_number(&obj.number)
        }
        (Some(schemars::schema::InstanceType::String), None) => {
            j2oas_string(&obj.string)
        }
        (Some(schemars::schema::InstanceType::Integer), None) => todo!(),
        (None, Some(subschema)) => j2oas_subschemas(subschema),
        (None, None) => todo!("missed a valid case {:?}", obj),
        _ => panic!("invalid"),
    };

    let mut data = openapiv3::SchemaData::default();

    if let Some(metadata) = &obj.metadata {
        data.title = metadata.title.clone();
        data.description = metadata.description.clone();
        // TODO skipping `default` since it's a little tricky to handle
        data.deprecated = metadata.deprecated;
        data.read_only = metadata.read_only;
        data.write_only = metadata.write_only;
    }

    openapiv3::ReferenceOr::Item(openapiv3::Schema {
        schema_data: data,
        schema_kind: kind,
    })
}

fn j2oas_subschemas(
    subschemas: &Box<schemars::schema::SubschemaValidation>,
) -> openapiv3::SchemaKind {
    match (&subschemas.all_of, &subschemas.any_of, &subschemas.one_of) {
        (Some(all_of), None, None) => openapiv3::SchemaKind::AllOf {
            all_of: all_of
                .iter()
                .map(|schema| j2oas_schema(schema))
                .collect::<Vec<_>>(),
        },
        (None, Some(any_of), None) => openapiv3::SchemaKind::AnyOf {
            any_of: any_of
                .iter()
                .map(|schema| j2oas_schema(schema))
                .collect::<Vec<_>>(),
        },
        (None, None, Some(one_of)) => openapiv3::SchemaKind::OneOf {
            one_of: one_of
                .iter()
                .map(|schema| j2oas_schema(schema))
                .collect::<Vec<_>>(),
        },
        (None, None, None) => todo!("missed a valid case"),
        _ => panic!("invalid"),
    }
}

fn j2oas_number(
    _number: &Option<Box<schemars::schema::NumberValidation>>,
) -> openapiv3::SchemaKind {
    todo!()
}

fn j2oas_string(
    string: &Option<Box<schemars::schema::StringValidation>>,
) -> openapiv3::SchemaKind {
    let mut string_type = openapiv3::StringType::default();
    if let Some(string) = string.as_ref() {
        string_type.max_length = string.max_length.map(|n| n as usize);
        string_type.min_length = string.min_length.map(|n| n as usize);
        string_type.pattern = string.pattern.clone();
    }
    openapiv3::SchemaKind::Type(openapiv3::Type::String(string_type))
}

fn j2oas_array(
    _array: &Option<Box<schemars::schema::ArrayValidation>>,
) -> openapiv3::SchemaKind {
    todo!()
}

fn j2oas_object(
    object: &Option<Box<schemars::schema::ObjectValidation>>,
) -> openapiv3::SchemaKind {
    let obj = object.as_ref().unwrap();
    openapiv3::SchemaKind::Type(openapiv3::Type::Object(
        openapiv3::ObjectType {
            properties: obj
                .properties
                .iter()
                .map(|(prop, schema)| {
                    (prop.clone(), match j2oas_schema(schema) {
                        openapiv3::ReferenceOr::Item(schema) => {
                            openapiv3::ReferenceOr::boxed_item(schema)
                        }
                        openapiv3::ReferenceOr::Reference {
                            reference,
                        } => openapiv3::ReferenceOr::Reference {
                            reference,
                        },
                    })
                })
                .collect::<_>(),
            required: obj.required.iter().map(|s| s.clone()).collect::<_>(),
            additional_properties: obj.additional_properties.as_ref().map(
                |schema| {
                    openapiv3::AdditionalProperties::Schema(Box::new(
                        j2oas_schema(schema),
                    ))
                },
            ),
            min_properties: obj.min_properties.map(|n| n as usize),
            max_properties: obj.max_properties.map(|n| n as usize),
        },
    ))
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
            Err("specified parameters do not appear in the path (a,b)"
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
            Err("path parameters are not consumed (aa,bb)".to_string())
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
            Err("path parameters are not consumed (c,d) and specified \
                 parameters do not appear in the path (a,b)"
                .to_string())
        );
    }
}
