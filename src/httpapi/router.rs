/*!
 * Routes incoming HTTP requests to handler functions
 */

use super::error::HttpError;
use super::handler::RouteHandler;

use http::Method;
use http::StatusCode;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/**
 * `HttpRouter` is a simple data structure for routing incoming HTTP requests to
 * specific handler functions based on the request method and URI path.
 *
 * ## Examples
 *
 * ```
 * use http::Method;
 * use http::StatusCode;
 * use hyper::Body;
 * use hyper::Response;
 * use oxide_api_prototype::httpapi::HttpError;
 * use oxide_api_prototype::httpapi::RequestContext;
 * use oxide_api_prototype::httpapi::HttpRouteHandler;
 * use oxide_api_prototype::httpapi::RouteHandler;
 * use oxide_api_prototype::httpapi::HttpRouter;
 * use oxide_api_prototype::httpapi::RouterLookupResult;
 * use std::sync::Arc;
 *
 * /// Example HTTP request handler function
 * async fn demo_handler(_: Arc<RequestContext>)
 *     -> Result<Response<Body>, HttpError>
 * {
 *      Ok(Response::builder()
 *          .status(StatusCode::NO_CONTENT)
 *          .body(Body::empty())?)
 * }
 *
 * fn demo()
 *     -> Result<(), HttpError>
 * {
 *      // Create a router and register a few routes.
 *      let mut router = HttpRouter::new();
 *      router.insert(Method::GET, "/projects",
 *          HttpRouteHandler::new(demo_handler));
 *      router.insert(Method::GET, "/projects/{project_id}",
 *          HttpRouteHandler(demo_handler));
 *
 *      // Basic lookup for a literal path.
 *      let lookup: RouterLookupResult = router.lookup_route(
 *          &Method::GET,
 *          "/projects"
 *      )?;
 *      let handler: &Box<dyn RouteHandler> = lookup.handler;
 *      assert!(lookup.variables.is_empty());
 *      // handler.handle_request(...)
 *
 *      // Basic lookup with path variables
 *      let lookup: RouterLookupResult = router.lookup_route(
 *          &Method::GET,
 *          "/projects/proj123"
 *      )?;
 *      assert_eq!(
 *          *lookup.variables.get(&"project_id".to_string()).unwrap(),
 *          "proj123".to_string()
 *      );
 *      let handler: &Box<dyn RouteHandler> = lookup.handler;
 *      // handler.handle_request(...)
 *
 *      // If a route is not found, we get back a 404.
 *      let error = router.lookup_route(&Method::GET, "/foo").unwrap_err();
 *      assert_eq!(error.status_code, StatusCode::NOT_FOUND);
 *
 *      // If a route is found, but there's no handler for this method,
 *      // we get back a 405.
 *      let error = router.lookup_route(&Method::PUT, "/projects").unwrap_err();
 *      assert_eq!(error.status_code, StatusCode::METHOD_NOT_ALLOWED);
 *      Ok(())
 * }
 * ```
 *
 * ## Usage details
 *
 * Routes are registered and looked up according to a path, like `"/foo/bar"`.
 * Paths are split into segments separated by one or more '/' characters.  When
 * registering a route, a path segment may be either a literal string or a
 * variable.  Variables are specified by wrapping the segment in braces.
 *
 * For example, a handler registered for `"/foo/bar"` will match only
 * `"/foo/bar"` (after normalization, that is -- it will also match
 * `"/foo///bar"`).  A handler registered for `"/foo/{bar}"` uses a
 * variable for the second segment, so it will match `"/foo/123"` (with `"bar"`
 * assigned to `"123"`) as well as `"/foo/bar456"` (with `"bar"` mapped to
 * `"bar456"`).  Only one segment is matched per variable, so `"/foo/{bar}"`
 * will not match `"/foo/123/456"`.
 *
 * The implementation here is essentially a trie where edges represent segments
 * of the URI path.  ("Segments" here are chunks of the path separated by one or
 * more "/" characters.)  To register or look up the path `"/foo/bar/baz"`, we
 * would start at the root and traverse edges for the literal strings `"foo"`,
 * `"bar"`, and `"baz"`, arriving at a particular node.  Each node has a set of
 * handlers, each associated with one HTTP method.
 *
 * We make (and, in some cases, enforce) a number of simplifying assumptions.
 * These could be relaxed, but it's not clear that's useful, and enforcing them
 * makes it easier to catch some types of bugs:
 *
 * * A particular resource (node) may have child resources (edges) with either
 *   literal path segments or variable path segments, but not both.  For
 *   example, you can't register both `"/projects/{id}"` and
 *   `"/projects/default"`.
 *
 * * If a given resource has an edge with a variable name, all routes through
 *   this node must use the same name for that variable.  That is, you can't
 *   define routes for `"/projects/{id}"` and `"/projects/{project_id}/info"`.
 *
 * * A given path cannot use the same variable name twice.  For example, you
 *   can't register path `"/projects/{id}/instances/{id}"`.
 *
 * * A given resource may have at most one handler for a given HTTP method.
 *
 * * The expectation is that during server initialization,
 *   `HttpRouter::insert()` will be invoked to register a number of route
 *   handlers.  After that initialization period, the router will be
 *   read-only.  This behavior isn't enforced by `HttpRouter`.
 */
#[derive(Debug)]
pub struct HttpRouter {
    /** root of the trie */
    root: Box<HttpRouterNode>
}

/**
 * Each node in the tree represents a group of HTTP resources having the same
 * handler functions.  As described above, these may correspond to exactly one
 * canonical path (e.g., `"/foo/bar"`) or a set of paths that differ by some
 * number of variable assignments (e.g., `"/projects/123/instances"` and
 * `"/projects/456/instances"`).
 *
 * Edges of the tree come in one of type types: edges for literal strings and
 * edges for variable strings.  A given node has either literal string edges or
 * variable edges, but not both.  However, we don't necessarily know what type
 * of outgoing edges a node will have when we create it.
 */
#[derive(Debug)]
struct HttpRouterNode {
    /** Handlers for each of the HTTP methods defined for this node. */
    method_handlers: BTreeMap<String, Box<dyn RouteHandler>>,
    /** Outgoing edges for different literal paths. */
    edges_literals: BTreeMap<String, Box<HttpRouterNode>>,
    /** Outgoing edges for variable-named paths. */
    edge_varname: Option<HttpRouterEdgeVariable>
}

/**
 * Represents an outgoing edge having a variable name.  (See the `HttpRouter`
 * comments for details.)  This is just used to group the variable name and the
 * Node pointer.  There's no corresponding struct for literal-named edges
 * because they don't have any data aside from the Node pointer.
 */
#[derive(Debug)]
struct HttpRouterEdgeVariable(String, Box<HttpRouterNode>);

/**
 * `PathSegment` represents a segment in a URI path when the router is being
 * configured.  Each segment may be either a literal string or a variable (the
 * latter indicated by being wrapped in braces.
 */
#[derive(Debug)]
enum PathSegment {
    /** a path segment for a literal string */
    Literal(String),
    /** a path segment for a variable */
    Varname(String)
}

impl PathSegment {
    /**
     * Given a `&String` representing a path segment from a Uri, return a
     * PathSegment.  This is used to parse a sequence of path segments to the
     * corresponding `PathSegment`, which basically means determining whether
     * it's a variable or a literal.
     */
    fn from(segment: &String)
        -> PathSegment
    {
        if segment.starts_with("{") || segment.ends_with("}") {
            assert!(segment.starts_with("{"), "HTTP URI path segment \
                variable missing leading \"{\"");
            assert!(segment.ends_with("}"), "HTTP URI path segment \
                variable missing trailing \"}\"");
            assert!(segment.len() > 2,
                "HTTP URI path segment variable name cannot be empty");

            let segment_chars: Vec<char> = segment.chars().collect();
            let newlast = segment_chars.len() - 1;
            let varname_chars = &segment_chars[1..newlast];
            PathSegment::Varname(varname_chars.iter().collect())
        } else {
            PathSegment::Literal(segment.to_string())
        }
    }
}

/**
 * `RouterLookupResult` represents the result of invoking
 * `HttpRouter::lookup_route()`.  A successful route lookup includes both the
 * handler and a mapping of variables in the configured path to the
 * corresponding values in the actual path.
 */
#[derive(Debug)]
pub struct RouterLookupResult<'a> {
    pub handler: &'a Box<dyn RouteHandler>,
    pub variables: BTreeMap<String, String>,
}

impl HttpRouter {
    /**
     * Returns a new `HttpRouter` with no routes configured.
     */
    pub fn new()
        -> Self
    {
        HttpRouter {
            root: Box::new(HttpRouterNode {
                method_handlers: BTreeMap::new(),
                edges_literals: BTreeMap::new(),
                edge_varname: None
            })
        }
    }

    /**
     * Helper function for taking a Uri path and producing a `Vec<String>` of
     * URL-encoded strings, each representing one segment of the path.
     */
    fn path_to_segments(path: &str)
        -> Vec<String>
    {
        /*
         * We're given the "path" portion of a URI and we want to construct an
         * array of the segments of the path.   Relevant references:
         *
         *    RFC 7230 HTTP/1.1 Syntax and Routing
         *             (particularly: 2.7.3 on normalization)
         *    RFC 3986 Uniform Resource Identifier (URI): Generic Syntax
         *             (particularly: 6.2.2 on comparison)
         *
         * TODO-hardening We should revisit this.  We want to consider a few
         * things:
         * - whether our input is already (still?) percent-encoded or not
         * - whether our returned representation is percent-encoded or not
         * - what it means (and what we should do) if the path does not begin
         *   with a leading "/"
         * - whether we want to collapse consecutive "/" characters
         *   (presumably we do, both at the start of the path and later)
         * - how to handle paths that end in "/" (in some cases, ought this send
         *   a 300-level redirect?)
         * - are there other normalization considerations? e.g., ".", ".."
         *
         * It seems obvious to reach for the Rust "url" crate.  That crate
         * parses complete URLs, which include a scheme and authority section
         * that does not apply here.  We could certainly make one up (e.g.,
         * "http://127.0.0.1") and construct a URL whose path matches the path
         * we were given.  However, while it seems natural that our internal
         * representaiton would not be percent-encoded, the "url" crate
         * percent-encodes any path that it's given.  Further, we probably want
         * to treat consecutive "/" characters as equivalent to a single "/",
         * but that crate treats them separately (which is not unreasonable,
         * since it's not clear that the above RFCs say anything about whether
         * empty segments should be ignored).  The net result is that that crate
         * doesn't buy us much here, but it does create more work, so we'll just
         * split it ourselves.
         */
        path
            .split("/")
            .filter(|segment| segment.len() > 0)
            .map(String::from)
            .collect()
    }

    /**
     * Configure a route for HTTP requests based on the HTTP `method` and
     * URI `path`.  See the `HttpRouter` docs for information about how `path`
     * is processed.  Requests matching `path` will be resolved to `handler`.
     */
    pub fn insert(&mut self, method: Method, path: &str,
        handler: Box<dyn RouteHandler>)
    {
        let all_segments = HttpRouter::path_to_segments(path);
        let mut varnames: BTreeSet<String> = BTreeSet::new();

        let mut node: &mut Box<HttpRouterNode> = &mut self.root;
        for raw_segment in all_segments {
            let segment = PathSegment::from(&raw_segment);

            node = match segment {
                PathSegment::Literal(lit) => {
                    /*
                     * We do not allow both literal and variable edges from the
                     * same node.  This could be supported (with some caveats
                     * about how matching would work), but it seems more likely
                     * to be a mistake.
                     */
                    if let Some(HttpRouterEdgeVariable(varname, _)) =
                        &node.edge_varname {
                        panic!("URI path \"{}\": attempted to register route \
                            for literal path segment \"{}\" when a route \
                            exists for variable path segment (variable name: \
                            \"{}\")", path, lit, varname);
                    }

                    if !node.edges_literals.contains_key(&lit) {
                        let newnode = Box::new(HttpRouterNode {
                            method_handlers: BTreeMap::new(),
                            edges_literals: BTreeMap::new(),
                            edge_varname: None
                        });

                        node.edges_literals.insert(lit.clone(), newnode);
                    }

                    node.edges_literals.get_mut(&lit).unwrap()
                },

                PathSegment::Varname(new_varname) => {
                    /*
                     * See the analogous check above about combining literal and
                     * variable path segments from the same resource.
                     */
                    if ! node.edges_literals.is_empty() {
                        panic!("URI path \"{}\": attempted to register route \
                            for variable path segment (variable name: \"{}\") \
                            when a route already exists for a literal path \
                            segment", path, new_varname);
                    }

                    /*
                     * Do not allow the same variable name to be used more than
                     * once in the path.  Again, this could be supported (with
                     * some caveats), but it seems more likely to be a mistake.
                     */
                    if varnames.contains(&new_varname) {
                        panic!("URI path \"{}\": variable name \"{}\" is used \
                            more than once", path, new_varname);
                    }
                    varnames.insert(new_varname.clone());

                    if node.edge_varname.is_none() {
                        let newnode = Box::new(HttpRouterNode {
                            method_handlers: BTreeMap::new(),
                            edges_literals: BTreeMap::new(),
                            edge_varname: None
                        });

                        node.edge_varname = Some(HttpRouterEdgeVariable(
                            new_varname.clone(), newnode));
                    } else if *new_varname !=
                            *node.edge_varname.as_ref().unwrap().0 {
                        /*
                         * Don't allow people to use different names for the
                         * same part of the path.  Again, this could be
                         * supported, but it seems likely to be confusing and
                         * probably a mistake.
                         */
                        panic!("URI path \"{}\": attempted to use variable \
                            name \"{}\", but a different name (\"{}\") has \
                            already been used for this", path, new_varname,
                            node.edge_varname.as_ref().unwrap().0);
                    }

                    &mut node.edge_varname.as_mut().unwrap().1
                }
            };
        }

        let methodname = method.as_str().to_uppercase();
        if let Some(_) = node.method_handlers.get(&methodname) {
            panic!("URI path \"{}\": attempted to create duplicate route for \
                method \"{}\"", path, method);
        }

        node.method_handlers.insert(methodname, handler);
    }

    /**
     * Look up the route handler for an HTTP request having method `method` and
     * URI path `path`.  A successful lookup produces a `RouterLookupResult`,
     * which includes both the handler that can process this request and a map
     * of variables assigned based on the request path as part of the lookup.
     * On failure, this returns an `HttpError` appropriate for the failure
     * mode.
     *
     * TODO-cleanup
     * consider defining a separate struct type for url-encoded vs. not?
     */
    pub fn lookup_route<'a, 'b>(&'a self, method: &'b Method, path: &'b str)
        -> Result<RouterLookupResult<'a>, HttpError>
    {
        let all_segments = HttpRouter::path_to_segments(path);
        let mut node: &Box<HttpRouterNode> = &self.root;
        let mut variables: BTreeMap<String, String> = BTreeMap::new();

        for segment in all_segments {
            let segment_string = segment.to_string();
            if let Some(n) = node.edges_literals.get(&segment_string) {
                node = n;
            } else if let Some(edge) = &node.edge_varname {
                variables.insert(edge.0.clone(), segment_string);
                node = &edge.1
            } else {
                return Err(HttpError::for_status(StatusCode::NOT_FOUND))
            }
        }

        /*
         * As a somewhat special case, if one requests a node with no handlers
         * at all, report a 404.  We could probably treat this as a 405 as well.
         */
        if node.method_handlers.is_empty() {
            return Err(HttpError::for_status(StatusCode::NOT_FOUND))
        }

        let methodname = method.as_str().to_uppercase();
        if let Some(handler) = node.method_handlers.get(&methodname) {
            Ok(RouterLookupResult {
                handler: handler,
                variables: variables
            })
        } else {
            Err(HttpError::for_status(StatusCode::METHOD_NOT_ALLOWED))
        }
    }
}

#[cfg(test)]
mod test {
    use super::error::HttpError;
    use super::handler::RouteHandler;
    use super::handler::RequestContext;
    use http::StatusCode;
    use hyper::Response;
    use hyper::Body;
    use std::sync::Arc;
    use http::Method;
    use super::HttpRouter;

    async fn test_handler(_: Arc<RequestContext>)
        -> Result<Response<Body>, HttpError>
    {
        panic!("test handler is not supposed to run");
    }

    fn new_handler()
        -> Box<dyn RouteHandler>
    {
        super::handler::HttpRouteHandler::new(test_handler);
    }

    fn new_handler_named(name: &str)
        -> Box<dyn RouteHandler>
    {
        super::handler::HttpRouteHandler::new_handler_named(test_handler, name);
    }

    #[test]
    #[should_panic(expected = "HTTP URI path segment variable name \
        cannot be empty")]
    fn test_variable_name_empty()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/foo/{}", new_handler());
    }

    #[test]
    #[should_panic(expected = "HTTP URI path segment variable \
        missing trailing \"}\"")]
    fn test_variable_name_bad_end()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/foo/{asdf/foo", new_handler());
    }

    #[test]
    #[should_panic(expected = "HTTP URI path segment variable \
        missing leading \"{\"")]
    fn test_variable_name_bad_start()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/foo/asdf}/foo", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/boo\": attempted to create \
        duplicate route for method \"GET\"")]
    fn test_duplicate_route1()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/boo", new_handler());
        router.insert(Method::GET, "/boo", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/foo//bar\": attempted to create \
        duplicate route for method \"GET\"")]
    fn test_duplicate_route2()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/foo/bar", new_handler());
        router.insert(Method::GET, "/foo//bar", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"//\": attempted to create \
        duplicate route for method \"GET\"")]
    fn test_duplicate_route3()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/", new_handler());
        router.insert(Method::GET, "//", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/projects/{id}/insts/{id}\": \
        variable name \"id\" is used more than once")]
    fn test_duplicate_varname()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/projects/{id}/insts/{id}", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/projects/{id}\": attempted to use \
        variable name \"id\", but a different name (\"project_id\") has \
        already been used for this")]
    fn test_inconsistent_varname()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/projects/{project_id}", new_handler());
        router.insert(Method::GET, "/projects/{id}", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/projects/{id}\": attempted to \
        register route for variable path segment (variable name: \"id\") when \
        a route already exists for a literal path segment")]
    fn test_variable_after_literal()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/projects/default", new_handler());
        router.insert(Method::GET, "/projects/{id}", new_handler());
    }

    #[test]
    #[should_panic(expected = "URI path \"/projects/default\": attempted to \
        register route for literal path segment \"default\" when a route \
        exists for variable path segment (variable name: \"id\")")]
    fn test_literal_after_variable()
    {
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/projects/{id}", new_handler());
        router.insert(Method::GET, "/projects/default", new_handler());
    }

    #[test]
    fn test_error_cases()
    {
        let mut router = HttpRouter::new();

        /*
         * Check a few initial conditions.
         */
        let error = router.lookup_route(&Method::GET, "/").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(&Method::GET, "////").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(&Method::GET, "/foo/bar").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(
            &Method::GET, "//foo///bar").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);

        /*
         * Insert a route into the middle of the tree.  This will let us look at
         * parent nodes, sibling nodes, and child nodes.
         */
        router.insert(Method::GET, "/foo/bar", new_handler());
        assert!(router.lookup_route(&Method::GET, "/foo/bar").is_ok());
        assert!(router.lookup_route(&Method::GET, "/foo/bar/").is_ok());
        assert!(router.lookup_route(&Method::GET, "//foo/bar").is_ok());
        assert!(router.lookup_route(&Method::GET, "//foo//bar").is_ok());
        assert!(router.lookup_route(&Method::GET, "//foo//bar//").is_ok());
        assert!(router.lookup_route(&Method::GET, "///foo///bar///").is_ok());

        /*
         * TODO-cleanup: consider having a "build" step that constructs a
         * read-only router and does validation like making sure that there's a
         * GET route on all nodes?
         */
        let error = router.lookup_route(&Method::GET, "/").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(&Method::GET, "/foo").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(&Method::GET, "//foo").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);
        let error = router.lookup_route(
            &Method::GET, "/foo/bar/baz").unwrap_err();
        assert_eq!(error.status_code, StatusCode::NOT_FOUND);

        let error = router.lookup_route(&Method::PUT, "/foo/bar").unwrap_err();
        assert_eq!(error.status_code, StatusCode::METHOD_NOT_ALLOWED);
        let error = router.lookup_route(&Method::PUT, "/foo/bar/").unwrap_err();
        assert_eq!(error.status_code, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test]
    fn test_router_basic()
    {
        let mut router = HttpRouter::new();

        /*
         * Insert a handler at the root and verify that we get that handler
         * back, even if we use different names that normalize to "/".
         * Before we start, sanity-check that there's nothing at the root
         * already.  Other test cases examine the errors in more detail.
         */
        assert!(router.lookup_route(&Method::GET, "/").is_err());
        router.insert(Method::GET, "/", new_handler_named("h1"));
        let result = router.lookup_route(&Method::GET, "/").unwrap();
        assert_eq!(result.handler.label(), "h1");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "//").unwrap();
        assert_eq!(result.handler.label(), "h1");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "///").unwrap();
        assert_eq!(result.handler.label(), "h1");
        assert!(result.variables.is_empty());

        /*
         * Now insert a handler for a different method at the root.  Verify that
         * we get both this handler and the previous one if we ask for the
         * corresponding method and that we get no handler for a different,
         * third method.
         */
        assert!(router.lookup_route(&Method::PUT, "/").is_err());
        router.insert(Method::PUT, "/", new_handler_named("h2"));
        let result = router.lookup_route(&Method::PUT, "/").unwrap();
        assert_eq!(result.handler.label(), "h2");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "/").unwrap();
        assert_eq!(result.handler.label(), "h1");
        assert!(router.lookup_route(&Method::DELETE, "/").is_err());
        assert!(result.variables.is_empty());

        /*
         * Now insert a handler one level deeper.  Verify that all the previous
         * handlers behave as we expect, and that we have one handler at the new
         * path, whichever name we use for it.
         */
        assert!(router.lookup_route(&Method::GET, "/foo").is_err());
        router.insert(Method::GET, "/foo", new_handler_named("h3"));
        let result = router.lookup_route(&Method::PUT, "/").unwrap();
        assert_eq!(result.handler.label(), "h2");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "/").unwrap();
        assert_eq!(result.handler.label(), "h1");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "/foo").unwrap();
        assert_eq!(result.handler.label(), "h3");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "/foo/").unwrap();
        assert_eq!(result.handler.label(), "h3");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "//foo//").unwrap();
        assert_eq!(result.handler.label(), "h3");
        assert!(result.variables.is_empty());
        let result = router.lookup_route(&Method::GET, "/foo//").unwrap();
        assert_eq!(result.handler.label(), "h3");
        assert!(result.variables.is_empty());
        assert!(router.lookup_route(&Method::PUT, "/foo").is_err());
        assert!(router.lookup_route(&Method::PUT, "/foo/").is_err());
        assert!(router.lookup_route(&Method::PUT, "//foo//").is_err());
        assert!(router.lookup_route(&Method::PUT, "/foo//").is_err());
    }

    #[test]
    fn test_embedded_non_variable()
    {
        /*
         * This isn't an important use case today, but we'd like to know if we
         * change the behavior, intentionally or otherwise.
         */
        let mut router = HttpRouter::new();
        assert!(router.lookup_route(&Method::GET, "/not{a}variable").is_err());
        router.insert(Method::GET, "/not{a}variable", new_handler_named("h4"));
        let result = router.lookup_route(
            &Method::GET, "/not{a}variable").unwrap();
        assert_eq!(result.handler.label(), "h4");
        assert!(result.variables.is_empty());
        assert!(router.lookup_route(&Method::GET, "/not{b}variable").is_err());
        assert!(router.lookup_route(&Method::GET, "/notnotavariable").is_err());
    }

    #[test]
    fn test_variables_basic()
    {
        /*
         * Basic test using a variable.
         */
        let mut router = HttpRouter::new();
        router.insert(Method::GET, "/projects/{project_id}",
            new_handler_named("h5"));
        assert!(router.lookup_route(&Method::GET, "/projects").is_err());
        assert!(router.lookup_route(&Method::GET, "/projects/").is_err());
        let result = router.lookup_route(
            &Method::GET, "/projects/p12345").unwrap();
        assert_eq!(result.handler.label(), "h5");
        assert_eq!(
            result.variables.keys().collect::<Vec<&String>>(),
            vec!["project_id"]
        );
        assert_eq!(result.variables.get("project_id").unwrap(), "p12345");
        assert!(router.lookup_route(
            &Method::GET, "/projects/p12345/child").is_err());
        let result = router.lookup_route(
            &Method::GET, "/projects/p12345/").unwrap();
        assert_eq!(result.handler.label(), "h5");
        assert_eq!(result.variables.get("project_id").unwrap(), "p12345");
        let result = router.lookup_route(
            &Method::GET, "/projects///p12345//").unwrap();
        assert_eq!(result.handler.label(), "h5");
        assert_eq!(result.variables.get("project_id").unwrap(), "p12345");
        /* Trick question! */
        let result = router.lookup_route(
            &Method::GET, "/projects/{project_id}").unwrap();
        assert_eq!(result.handler.label(), "h5");
        assert_eq!(result.variables.get("project_id").unwrap(), "{project_id}");
    }

    #[test]
    fn test_variables_multi()
    {
        /*
         * Exercise a case with multiple variables.
         */
        let mut router = HttpRouter::new();
        router.insert(
            Method::GET,
            "/projects/{project_id}/instances/{instance_id}/fwrules\
                /{fwrule_id}/info",
            new_handler_named("h6"));
        let result = router.lookup_route(
            &Method::GET,
            "/projects/p1/instances/i2/fwrules/fw3/info"
        ).unwrap();
        assert_eq!(result.handler.label(), "h6");
        assert_eq!(
            result.variables.keys().collect::<Vec<&String>>(),
            vec!["fwrule_id", "instance_id", "project_id"]
        );
        assert_eq!(result.variables.get("project_id").unwrap(), "p1");
        assert_eq!(result.variables.get("instance_id").unwrap(), "i2");
        assert_eq!(result.variables.get("fwrule_id").unwrap(), "fw3");
    }

    #[test]
    fn test_empty_variable()
    {
        /*
         * Exercise a case where a broken implementation might erroneously
         * assign a variable to the empty string.
         */
        let mut router = HttpRouter::new();
        router.insert(
            Method::GET,
            "/projects/{project_id}/instances",
            new_handler_named("h7")
        );
        assert!(router.lookup_route(
            &Method::GET, "/projects/instances").is_err());
        assert!(router.lookup_route(
            &Method::GET, "/projects//instances").is_err());
        assert!(router.lookup_route(
            &Method::GET, "/projects///instances").is_err());
        let result = router.lookup_route(
            &Method::GET,
            "/projects/foo/instances"
        ).unwrap();
        assert_eq!(result.handler.label(), "h7");
    }
}
