/*!
 * Routes requests to handlers
 */

use crate::api_handler::RouteHandler;

use http::Method;
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

impl PathSegment {
    fn from(segment: &str)
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
        -> Vec<PathSegment>
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
         * TODO-cleanup can we do this with iterators instead of a vector?  I
         * ran into a lot of trouble with lifetimes trying to do this.
         */
        let path_segments: Vec<PathSegment> = url
            .path_segments()
            .unwrap()
            .map(PathSegment::from)
            .collect();
        path_segments
    }

    pub fn insert(&mut self, method: Method, path: &str,
        handler: Box<dyn RouteHandler>)
    {
        let all_segments = HttpRouter::path_to_segments(path);
        let mut segments = all_segments.as_slice();

        let mut node: &mut Box<HttpRouterNode> = &mut self.root;
        while let Some(segment) = segments.first() {
            /*
             * XXX panic if this segment doesn't match the existing edges from
             * this node.
             */
            node = match segment {
                PathSegment::Literal(lit) => {
                    if !node.edges_constants.contains_key(lit) {
                        let newnode = Box::new(HttpRouterNode {
                            method_handlers: BTreeMap::new(),
                            edges_constants: BTreeMap::new(),
                            edge_varname: None
                        });

                        node.edges_constants.insert(lit.clone(), newnode);
                    }

                    node.edges_constants.get_mut(lit).unwrap()
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

            segments = &segments[1..];
        }

        let methodname = method.as_str().to_uppercase();
        if let Some(_) = node.method_handlers.get(&methodname) {
            panic!("attempted to create duplicate route for {} \"{}\"",
                method, path);
        }

        node.method_handlers.insert(methodname, handler);
    }
}

#[cfg(test)]
mod test {
    use crate::api_error::ApiHttpError;
    use crate::api_handler::api_handler_create;
    use crate::api_server::ApiServerState;
    use hyper::Request;
    use hyper::Response;
    use hyper::Body;
    use std::sync::Arc;
    use http::Method;

    async fn demo_handler(_server: Arc<ApiServerState>, _request: Request<Body>)
        -> Result<Response<Body>, ApiHttpError>
    {
        unimplemented!();
    }
    
    #[test]
    fn test_router()
    {
        let mut router = super::HttpRouter::new();
        let handler = || api_handler_create(demo_handler);
    
        eprintln!("router: {:?}", router);
        router.insert(Method::GET, "/foo/{bar}/baz", handler());
        router.insert(Method::GET, "/boo", handler());
        eprintln!("router: {:?}", router);
    }
}
