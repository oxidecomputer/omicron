/*!
 * Interface for implementing API endpoint handler functions.
 *
 * ## Endpoint function signatures
 *
 * All API endpoint handler functions must be `async` (that is, must return
 * a `Future`) and must return a `Result<Response<Body>, ApiHttpError>`.
 * Ignoring the return values, handler functions must have one of the following
 * signatures:
 *
 * 1. `f(server: Arc<ApiServerState>, request: Request<Body>)`
 * 2. `f(server: Arc<ApiServerState>, request: Request<Body>, query: Query<Q>)`
 * 3. `f(server: Arc<ApiServerState>, request: Request<Body>, json: Json<J>)`
 * 4. `f(server: Arc<ApiServerState>, request: Request<Body>, query: Query<Q>,
 *     json: Json<J>)`
 *
 * See "Extractors" below for more on the types `Query` and `Json`.
 *
 * We allow for variation in these signatures not so much for programmer
 * convenience (since parsing the query string or JSON body could be implemented
 * in line or two of code each, with the right helper functions) but rather so
 * that the type signature of the handler function can be programmatically
 * analyzed to generate an OpenAPI snippet for this endpoint.  This approach of
 * treating the server implementation as the source of truth for the API
 * specification ensures that at least in many important ways, the
 * implementation cannot diverge from the spec without us knowing it.
 *
 * ## Extractors
 *
 * The types `Query` and `Json` are called _extractors_ because they cause
 * information to be pulled out of the request and made available to the handler
 * function.
 *
 * * `Query` extracts parameters from a query string, deserializing them into
 *   an instance of type `Q`.  `Q` must implement `serde::Deserialize`.
 * * `Json` extracts content from the request body by parsing the body as JSON
 *   and deserializing it into a type `J`.  `J` must implement
 *   `serde::Deserialize`.
 *
 * If the handler takes a `Query<Q>` or a `Json<J>` and the corresponding
 * extraction cannot be completed, the request fails with status code 400 and an
 * error message reflecting a validation error.
 *
 * As with any serde-deserializable type, you can make fields optional by having
 * the corresponding property of the type be an `Option`.  Here's an example of
 * an endpoint that takes two arguments via query parameters: "limit", a
 * required u32, and "marker", an optional string:
 *
 * ```
 * use oxide_api_prototype::api_error::ApiHttpError;
 * use oxide_api_prototype::api_handler::Json;
 * use oxide_api_prototype::api_handler::Query;
 * use oxide_api_prototype::api_server::ApiServerState;
 * use http::StatusCode;
 * use hyper::Body;
 * use hyper::Request;
 * use hyper::Response;
 * use std::sync::Arc;
 *
 * #[derive(serde::Deserialize)]
 * struct MyQueryArgs {
 *     limit: u32,
 *     marker: Option<String>
 * }
 *
 * async fn handle_request(
 *     _: Arc<ApiServerState>, 
 *     _: Request<Body>,
 *     query: Query<MyQueryArgs>)
 *     -> Result<Response<Body>, ApiHttpError>
 * {
 *     let query_args = query.into_inner();
 *     let limit: u32 = query_args.limit;
 *     let marker: Option<String> = query_args.marker;
 *     Ok(Response::builder()
 *         .status(StatusCode::OK)
 *         .body(format!("limit = {}, marker = {:?}\n", limit, marker).into())?)
 * }
 * ```
 *
 * ## Implementation notes
 *
 * We go through considerable effort below to make this interface possible.
 * Both the interface (primarily) and the implementation (less so) are inspired
 * by Actix-Web.  The Actix implementation is significantly more general (and
 * commensurately complex).  It would be possible to implement richer facilities
 * here, like extractors for backend server state, path components, headers, and
 * so on; allowing for extractors to appear in arbitrary order; allowing for
 * server and request parameters to be omitted; and so on; but those other
 * facilities don't seem that valuable right now since they largely don't affect
 * the OpenAPI spec.
 */

use crate::api_error::ApiHttpError;
use crate::api_server::ApiServerState;
use crate::api_http_util::http_read_body;

use async_trait::async_trait;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

/**
 * Type alias for the result returned by API handler functions.
 */
pub type ApiHandlerResult = Result<Response<Body>, ApiHttpError>;


/**
 * `Derived` defines an interface allowing a type to be constructed from a
 * `(server: Arc<ApiServerState>, request: &mut Request<Body>)` tuple.  Unlike
 * most traits, `Derived` essentially defines only a constructor function, not
 * instance functions.
 *
 * The extractors that we provide (e.g., `Query`, `Json`) implement `Derived` in
 * order to construct themselves from the request.  For example, `Derived` is
 * implemented for `Query<Q>` with a function that reads the query string from
 * the request, parses it, and constructs a `Query<Q>` with it.
 *
 * We also define implementations of `Derived` for tuples of types that
 * themselves implement `Derived`.  See the implementation of
 * `ConcreteRouteHandler` for more on why this needed.
 */
#[async_trait]
pub trait Derived: Send + Sync + Sized
{
    /**
     * Construct an instance of this type from `server` and `request`.
     */
    async fn from_request(
        server: Arc<ApiServerState>,
        request: &mut Request<Body>) -> Result<Self, ApiHttpError>;
}

/*
 * TODO-cleanup The next three implementations could benefit from a macro.
 */

#[async_trait]
impl Derived for ()
{
    async fn from_request(_: Arc<ApiServerState>, _: &mut Request<Body>)
        -> Result<(), ApiHttpError>
    {
        Ok(())
    }
}

#[async_trait]
impl<T> Derived for (T,)
where
    T: Derived + 'static, /* TODO-cleanup static should not be necessary*/
{
    async fn from_request(server: Arc<ApiServerState>,
        request: &mut Request<Body>)
        -> Result<(T,), ApiHttpError>
    {
        Ok((T::from_request(server, request).await?,))
    }
}

#[async_trait]
impl<T1, T2> Derived for (T1, T2)
where
    T1: Derived + 'static, /* TODO-cleanup static should not be necessary */
    T2: Derived + 'static, /* TODO-cleanup static should not be necessary */
{
    async fn from_request(server: Arc<ApiServerState>,
        request: &mut Request<Body>)
        -> Result<(T1,T2), ApiHttpError>
    {
        let p1 = T1::from_request(Arc::clone(&server), request).await?;
        let p2 = T2::from_request(Arc::clone(&server), request).await?;
        Ok((p1, p2))
    }
}

/**
 * `ApiHandler` is a trait providing a single function, `handle_request()`,
 * which takes an HTTP request and produces an HTTP response (or
 * `ApiHttpError`).
 *
 * As described above, handler functions can have a number of different
 * signatures.  They all consume a reference to the server state and the
 * request.  They may also consume some number of extractor arguments.  The
 * `ApiHandler` trait is parametrized by the type `FuncParams`, which is
 * expected to be a tuple describing these extractor arguments.
 *
 * Below, we define implementations of `ApiHandler` for various function
 * types.  In this way, we can treat functions with different signatures as
 * different kinds of `ApiHandler`.  However, since the signature shows up in
 * the `FuncParams` type parameter, we'll need additional abstraction to treat
 * different handlers interchangeably.  See `RouteHandler` below.
 */
#[async_trait]
pub trait ApiHandler<FuncParams: Derived>: Send + Sync + 'static
{
    async fn handle_request(
        &self,
        server: Arc<ApiServerState>,
        request: Request<Body>,
        p: FuncParams)
        -> ApiHandlerResult;
}

/**
 * Implementation of `ApiHandler` for functions that consume no extractor
 * arguments (just the server and request arguments).
 * TODO the implementations below could benefit from a macro.
 */
#[async_trait]
impl<FuncType, FutureType> ApiHandler<()> for FuncType
where
    FuncType: Fn(Arc<ApiServerState>, Request<Body>)
        -> FutureType + Send + Sync + 'static,
    FutureType: Future<Output = ApiHandlerResult> + Send + Sync + 'static,
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>,
        _p: ())
        -> ApiHandlerResult
    {
        (self)(server, request).await
    }
}

/**
 * Implementation of `ApiHandler` for functions that consume a single `Query`
 * extractor argument in addition to the regular server and request arguments.
 */
#[async_trait]
impl<FuncType, FutureType, Q> ApiHandler<(Query<Q>,)> for FuncType
where
    FuncType: Fn(Arc<ApiServerState>, Request<Body>, Query<Q>)
        -> FutureType + Send + Sync + 'static,
    FutureType: Future<Output = ApiHandlerResult> + Send + Sync + 'static,
    Q: DeserializeOwned + Send + Sync + 'static,
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>,
        (query,): (Query<Q>,))
        -> ApiHandlerResult
    {
        (self)(server, request, query).await
    }
}

/**
 * Implementation of `ApiHandler` for functions that consume a single `Json`
 * extractor argument in addition to the regular server and request arguments.
 */
#[async_trait]
impl<FuncType, FutureType, J> ApiHandler<(Json<J>,)> for FuncType
where
    FuncType: Fn(Arc<ApiServerState>, Request<Body>, Json<J>)
        -> FutureType + Send + Sync + 'static,
    FutureType: Future<Output = ApiHandlerResult> + Send + Sync + 'static,
    J: DeserializeOwned + Send + Sync + 'static,
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>,
        (json,): (Json<J>,))
        -> ApiHandlerResult
    {
        (self)(server, request, json).await
    }
}

/**
 * Implementation of `ApiHandler` for functions that consume both a `Query` and
 * a `Json` extractor argument in addition to the regular server and request
 * arguments.  Note that the order of these arguments matters.  Reversing them
 * is not supported.
 */
#[async_trait]
impl<FuncType, FutureType, Q, J> ApiHandler<(Query<Q>, Json<J>)> for FuncType
where
    FuncType: Fn(Arc<ApiServerState>, Request<Body>, Query<Q>, Json<J>)
        -> FutureType + Send + Sync + 'static,
    FutureType: Future<Output = ApiHandlerResult> + Send + Sync + 'static,
    Q: DeserializeOwned + Send + Sync + 'static,
    J: DeserializeOwned + Send + Sync + 'static,
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>,
        (query, json): (Query<Q>, Json<J>))
        -> ApiHandlerResult
    {
        (self)(server, request, query, json).await
    }
}


/**
 * `RouteHandler` abstracts an `ApiHandler<FuncParams>` in a way that allows
 * callers to invoke the handler without knowing the handler's function
 * signature.
 *
 * The "Route" in `RouteHandler` refers to the fact that this structure is used
 * to record that a specific handler has been attached to a specific HTTP route.
 */
#[async_trait]
pub trait RouteHandler: Sync + Send {
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>)
        -> ApiHandlerResult;
}

/**
 * `ConcreteRouteHandler` is the only type that implements `RouteHandler`.  The
 * reason both exist is that we need `ConcreteRouteHandler::new()` to consume an
 * arbitrary kind of `ApiHandler<FuncParams>` and return an object that's _not_
 * parametrized by `FuncParams`.  In fact, the resulting `ConcreteRouteHandler`
 * _is_ parametrized by `FuncParams`, but we returned it as a `RouteHandler`
 * that does not have those type parameters, allowing the caller to ignore the
 * differences between different handler function type signatures.
 */
struct ConcreteRouteHandler<HandlerType, FuncParams>
where
    HandlerType: ApiHandler<FuncParams>,
    FuncParams: Derived,
{
    /** the actual ApiHandler used to implement this route */
    handler: HandlerType,

    /**
     * In order to define `new()` below, we need a type parameter `HandlerType`
     * that implements `ApiHandler<FuncParams>`, which means we also need a
     * `FuncParams` type parameter.  However, this type parameter would be
     * unconstrained, which makes Rust upset.  Use of PhantomData<FuncParams>
     * here causes the compiler to behave as though this struct referred to a
     * `FuncParams`, which allows us to use the type parameter below.
     */
    phantom: PhantomData<FuncParams>
}

#[async_trait]
impl<HandlerType, FuncParams> RouteHandler for
    ConcreteRouteHandler<HandlerType, FuncParams>
where
    HandlerType: ApiHandler<FuncParams>,
    FuncParams: Derived,
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        mut request: Request<Body>)
        -> ApiHandlerResult
    {
        /*
         * This is where the magic happens: in the code below, `funcparams` has
         * type `FuncParams`, which is a tuple type describing the extractor
         * arguments to the handler function.  This could be `()`, `(Query<Q>)`,
         * `(Json<J>)`, `(Query<Q>, Json<J>)`, or any other combination of
         * extractors we decide to support in the future.  Whatever it is must
         * implement `Derived`, which means we can invoke
         * `Derived::from_request()` to construct the argument tuple, generally
         * from information available in the `request` object.  We pass this
         * down to the `ApiHandler`, for which there's a different
         * implementation for each value of `FuncParams`.  The `ApiHandler` for
         * each `FuncParams` just pulls the arguments out of the `funcparams`
         * tuple and makes them actual function arguments for the actual handler
         * function.  From this point down, all of this is resolved
         * statically.makes them actual function arguments for the actual
         * handler function.  From this point down, all of this is resolved
         * statically.
         */
        let funcparams = Derived::from_request(
            Arc::clone(&server), &mut request).await?;
        self.handler.handle_request(server, request, funcparams).await
    }
}

/*
 * Public interfaces
 */

/**
 * Given a function matching one of the supported API handler function
 * signatures, return a RouteHandler that can be used to respond to HTTP
 * requests using this function.
 */
pub fn api_handler_create<
    FuncParams, HandlerType>(handler: HandlerType)
    -> Box<dyn RouteHandler>
where
    HandlerType: ApiHandler<FuncParams>,
    FuncParams: Derived + 'static,
{
    Box::new(ConcreteRouteHandler {
        handler: handler,
        phantom: PhantomData
    })
}


/*
 * Extractors
 */

/*
 * Query: query string extractor
 */

/**
 * `Query<QueryType>` is an extractor used to deserialize an instance of
 * `QueryType` from an HTTP request's query string.  `QueryType` is any
 * structure of yours that implements `serde::Deserialize`.  See this module's
 * documentation for more information.
 */
pub struct Query<QueryType> {
    inner: QueryType
}

impl<QueryType> Query<QueryType> {
    /*
     * TODO drop this in favor of Deref?  + Display and Debug for convenience?
     */
    pub fn into_inner(self) -> QueryType {
        self.inner
    }
}

/**
 * Given an HTTP request, pull out the query string and attempt to deserialize
 * it as an instance of `QueryType`.
 */
fn http_request_load_query<QueryType>(request: &Request<Body>)
    -> Result<Query<QueryType>, ApiHttpError>
where
    QueryType: DeserializeOwned
{
    let raw_query_string = request.uri().query().unwrap_or("");
    /*
     * TODO-correctness: are query strings defined to be urlencoded in this way?
     */
    match serde_urlencoded::from_str(raw_query_string) {
        Ok(q) => Ok(Query { inner: q }),
        Err(e) => Err(ApiHttpError::for_bad_request(
            format!("unable to parse query string: {}", e)))
    }
}

/*
 * The `Derived` implementation for Query<QueryType> describes how to construct
 * an instance of `Query<QueryType>` from an HTTP request: namely, by parsing
 * the query string to an instance of `QueryType`.
 * TODO-cleanup We shouldn't have to use the "'static" bound on `QueryType`
 * here.  It seems like we ought to be able to use 'async_trait, but that
 * doesn't seem to be defined.
 */
#[async_trait]
impl<QueryType> Derived for Query<QueryType>
where
    QueryType: DeserializeOwned + Send + Sync + 'static
{
    async fn from_request(_: Arc<ApiServerState>, request: &mut Request<Body>)
        -> Result<Query<QueryType>, ApiHttpError>
    {
        http_request_load_query(&request)
    }
}

/*
 * JSON: json body extractor
 */

/**
 * `Json<JsonType>` is an extractor used to deserialize an instance of
 * `JsonType` from an HTTP request body.  `JsonType` is any structure of yours
 * that implements `serde::Deserialize`.  See this module's documentation for
 * more information.
 */
pub struct Json<JsonType> {
    inner: JsonType
}

impl<JsonType> Json<JsonType> {
    /*
     * TODO drop this in favor of Deref?  + Display and Debug for convenience?
     */
    pub fn into_inner(self) -> JsonType {
        self.inner
    }
}

/**
 * Given an HTTP request, attempt to read the body, parse it as JSON, and
 * deserialize an instance of `JsonType` from it.
 */
async fn http_request_load_json_body<JsonType>(
    server: Arc<ApiServerState>,
    request: &mut Request<Body>)
    -> Result<Json<JsonType>, ApiHttpError>
where
    JsonType: DeserializeOwned
{
    let body_bytes = http_read_body(
        request.body_mut(), server.config.request_body_max_bytes).await?;
    let value: Result<JsonType, serde_json::Error> =
        serde_json::from_slice(&body_bytes);
    match value {
        Ok(j) => Ok(Json { inner: j }),
        Err(e) => Err(ApiHttpError::for_bad_request(
            format!("unable to parse body JSON: {}", e)))
    }
}

/*
 * The `Derived` implementation for Json<JsonType> describes how to construct an
 * instance of `Json<JsonType>` from an HTTP request: namely, by reading the
 * request body and parsing it as JSON into type `JsonType`.
 * TODO-cleanup We shouldn't have to use the "'static" bound on `JsonType` here.
 * It seems like we ought to be able to use 'async_trait, but that doesn't seem
 * to be defined.
 */
#[async_trait]
impl<JsonType> Derived for Json<JsonType>
where
    JsonType: DeserializeOwned + Send + Sync + 'static,
{
    async fn from_request(server: Arc<ApiServerState>,
        mut request: &mut Request<Body>)
        -> Result<Json<JsonType>, ApiHttpError>
    {
        http_request_load_json_body(server, &mut request).await
    }
}
