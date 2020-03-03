/*!
 * Routes requests to handlers
 */

use crate::api_error::ApiHttpError;
use crate::api_handler::RouteHandler;

use http::Method;
use http::StatusCode;
use std::collections::BTreeMap;
use url::Url;

#[derive(Debug)]
pub struct HttpRouter {
    root: Box<HttpRouterNode>
}

#[derive(Debug)]
struct HttpRouterNode {
    method_handlers: BTreeMap<String, Box<dyn RouteHandler>>,
    edges_constants: BTreeMap<String, Box<HttpRouterNode>>,
    edge_varname: Option<HttpRouterEdgeVariable>
}

#[derive(Debug)]
struct HttpRouterEdgeVariable(String, Box<HttpRouterNode>);

#[derive(Debug)]
enum PathSegment {
    Literal(String),
    Varname(String)
}

#[derive(Debug)]
pub struct LookupResult<'a> {
    pub handler: &'a Box<dyn RouteHandler>,
    pub variables: BTreeMap<String, String>,
}

impl PathSegment {
    fn from(segment: &String)
        -> PathSegment
    {
        /*
         * TODO-cleanup use of percent-encoding here
         * TODO-correctness figure out if we _should_ be using percent-encoding
         * here or not -- i.e., is the matching actually correct?
         */
        if !segment.starts_with("%7B")
            || !segment.ends_with("%7D")
            || segment.chars().count() < 7 {
            PathSegment::Literal(segment.to_string())
        } else {
            let segment_chars: Vec<char> = segment.chars().collect();
            let newlast = segment_chars.len() - 3;
            let varname_chars = &segment_chars[3..newlast];
            PathSegment::Varname(varname_chars.iter().collect())
        }
    }
}

impl HttpRouter {
    pub fn new()
        -> Self
    {
        HttpRouter {
            root: Box::new(HttpRouterNode {
                method_handlers: BTreeMap::new(),
                edges_constants: BTreeMap::new(),
                edge_varname: None
            })
        }
    }

    fn path_to_segments(path: &str)
        -> Vec<String>
    {
        /* TODO-cleanup is this really the right way?  Feels like a hack. */
        let base = Url::parse("http://127.0.0.1/").unwrap();
        let url = match base.join(path) {
            Ok(parsed) => parsed,
            Err(e) => {
                panic!("attempted to create route for invalid URL: {}: \"{}\"",
                    path, e);
            }
        };

        /*
         * TODO-correctness is it possible for bad input to cause this to fail?
         * If so, we should provide a better error message.
         */
        url.path_segments().unwrap().map(String::from).collect()
    }

    pub fn insert(&mut self, method: Method, path: &str,
        handler: Box<dyn RouteHandler>)
    {
        let all_segments = HttpRouter::path_to_segments(path);

        let mut node: &mut Box<HttpRouterNode> = &mut self.root;
        for raw_segment in all_segments {
            let segment = PathSegment::from(&raw_segment);

            /*
             * XXX panic if this segment doesn't match the existing edges from
             * this node.
             */
            node = match segment {
                PathSegment::Literal(lit) => {
                    if !node.edges_constants.contains_key(&lit) {
                        let newnode = Box::new(HttpRouterNode {
                            method_handlers: BTreeMap::new(),
                            edges_constants: BTreeMap::new(),
                            edge_varname: None
                        });

                        node.edges_constants.insert(lit.clone(), newnode);
                    }

                    node.edges_constants.get_mut(&lit).unwrap()
                },

                PathSegment::Varname(new_varname) => {
                    if node.edge_varname.is_none() {
                        let newnode = Box::new(HttpRouterNode {
                            method_handlers: BTreeMap::new(),
                            edges_constants: BTreeMap::new(),
                            edge_varname: None
                        });

                        node.edge_varname = Some(HttpRouterEdgeVariable(
                            new_varname.clone(), newnode));
                    } else {
                        /* TODO-cleanup should panic with better message */
                        assert_eq!(*new_varname,
                            *node.edge_varname.as_ref().unwrap().0);
                    }

                    &mut node.edge_varname.as_mut().unwrap().1
                }
            };
        }

        let methodname = method.as_str().to_uppercase();
        if let Some(_) = node.method_handlers.get(&methodname) {
            panic!("attempted to create duplicate route for {} \"{}\"",
                method, path);
        }

        node.method_handlers.insert(methodname, handler);
    }

    /*
     * TODO-cleanup
     * consider defining a separate struct type for url-encoded vs. not?
     */
    pub fn lookup_route<'a, 'b>(&'a self, method: &'b Method, path: &'b str)
        -> Result<LookupResult<'a>, ApiHttpError>
    {
        let all_segments = HttpRouter::path_to_segments(path);
        let mut node: &Box<HttpRouterNode> = &self.root;
        let mut variables: BTreeMap<String, String> = BTreeMap::new();

        for segment in all_segments {
            let segment_string = segment.to_string();
            if let Some(n) = node.edges_constants.get(&segment_string) {
                node = n;
            } else if let Some(edge) = &node.edge_varname {
                variables.insert(edge.0.clone(), segment_string);
                node = &edge.1
            } else {
                return Err(ApiHttpError::for_status(StatusCode::NOT_FOUND))
            }
        }

        let methodname = method.as_str().to_uppercase();
        if let Some(handler) = node.method_handlers.get(&methodname) {
            Ok(LookupResult {
                handler: handler,
                variables: variables
            })
        } else {
            Err(ApiHttpError::for_status(StatusCode::METHOD_NOT_ALLOWED))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::api_error::ApiHttpError;
    use crate::api_handler::api_handler_create;
    use crate::api_handler::RouteHandler;
    use crate::api_handler::RequestContext;
    use hyper::Response;
    use hyper::Body;
    use std::sync::Arc;
    use http::Method;
    use super::HttpRouter;

    async fn test_handler(_: Arc<RequestContext>)
        -> Result<Response<Body>, ApiHttpError>
    {
        panic!("test handler is not supposed to run");
    }

    fn new_handler()
        -> Box<dyn RouteHandler>
    {
        api_handler_create(test_handler)
    }

    #[test]
    fn test_router()
    {
        let mut router = HttpRouter::new();

        eprintln!("router: {:?}", router);
        router.insert(Method::GET, "/foo/{bar}/baz", new_handler());
        router.insert(Method::GET, "/boo", new_handler());
        eprintln!("router: {:?}", router);
    }
}
