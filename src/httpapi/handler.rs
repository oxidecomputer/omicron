/*!
 * Interface for implementing HTTP endpoint handler functions.
 *
 * ## Endpoint function signatures
 *
 * All endpoint handler functions must be `async` (that is, must return
 * a `Future`) and must return something that can be turned into a
 * `Result<Response<Body>, HttpError>`.  Ignoring the return values for a
 * minute, handler functions must have one of the following signatures:
 *
 * 1. `f(rqctx: Arc<RequestContext>)`
 * 2. `f(rqctx: Arc<RequestContext>, query: Query<Q>)`
 * 3. `f(rqctx: Arc<RequestContext>, json: Json<J>)`
 * 4. `f(rqctx: Arc<RequestContext>, query: Query<Q>, json: Json<J>)`
 *
 * See "Extractors" below for more on the types `Query` and `Json`.
 *
 * We allow for variation in the function arguments not so much for programmer
 * convenience (since parsing the query string or JSON body could be implemented
 * in line or two of code each, with the right helper functions) but rather so
 * that the type signature of the handler function can be programmatically
 * analyzed to generate an OpenAPI snippet for this endpoint.  This approach of
 * treating the server implementation as the source of truth for the API
 * specification ensures that at least in many important ways, the
 * implementation cannot diverge from the spec without us knowing it.
 *
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
 * use oxide_api_prototype::httpapi::HttpError;
 * use oxide_api_prototype::httpapi::Json;
 * use oxide_api_prototype::httpapi::Query;
 * use oxide_api_prototype::httpapi::RequestContext;
 * use http::StatusCode;
 * use hyper::Body;
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
 *     _: Arc<RequestContext>,
 *     query: Query<MyQueryArgs>)
 *     -> Result<Response<Body>, HttpError>
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
 *
 * ## Endpoint function return types
 *
 * Just like we want API input types to be represented in function arguments, we
 * want API response types to be represented in function return values so that
 * OpenAPI tooling can identify them at build time.  Ultimately, a handler
 * function's return value needs to become a `Result<Response<Body>,
 * HttpError>`.  However, handler functions may return `Result<T, HttpError>`
 * for any `T` that implements `Into<Result<Response<Body>, HttpError>>`.  Note
 * that there's an extra level of `Result` there to account for the possibility
 * that the conversion may fail.
 *
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

use super::error::HttpError;
use super::http_util::http_read_body;
use super::server::ServerState;

use async_trait::async_trait;
use futures::lock::Mutex;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

/**
 * Type alias for the result returned by HTTP handler functions.
 */
pub type HttpHandlerResult = Result<Response<Body>, HttpError>;

/**
 * Handle for various interfaces useful during request processing.
 * TODO-cleanup What's the right way to package up "request"?  The only time we
 * need it to be mutable is when we're reading the body (e.g., as part of the
 * JSON extractor).  In order to support that, we wrap it in something that
 * supports interior mutability.  It also needs to be thread-safe, since we're
 * using async/await.  That brings us to Arc<Mutex<...>>, but it seems like
 * overkill since it will only really be used by one thread at a time (at all,
 * let alone mutably) and there will never be contention on the Mutex.
 */
pub struct RequestContext {
    /** shared server state */
    pub server: Arc<ServerState>,
    /** HTTP request details */
    pub request: Arc<Mutex<Request<Body>>>,
    /** HTTP request routing variables */
    pub path_variables: BTreeMap<String, String>,
}

/**
 * `Derived` defines an interface allowing a type to be constructed from a
 * `RequestContext`.  Unlike most traits, `Derived` essentially defines only a
 * constructor function, not instance functions.
 *
 * The extractors that we provide (e.g., `Query`, `Json`) implement `Derived` in
 * order to construct themselves from the request.  For example, `Derived` is
 * implemented for `Query<Q>` with a function that reads the query string from
 * the request, parses it, and constructs a `Query<Q>` with it.
 *
 * We also define implementations of `Derived` for tuples of types that
 * themselves implement `Derived`.  See the implementation of
 * `HttpRouteHandler` for more on why this needed.
 */
#[async_trait]
pub trait Derived: Send + Sync + Sized {
    /**
     * Construct an instance of this type from a `RequestContext`.
     */
    async fn from_request(
        rqctx: Arc<RequestContext>,
    ) -> Result<Self, HttpError>;
}

/**
 * `impl_derived_for_tuple!` defines implementations of `Derived` for tuples
 * whose elements themselves implement `Derived`.
 */
macro_rules! impl_derived_for_tuple ({ $( $T:ident),*} => {
    #[async_trait]
    impl< $($T: Derived + 'static,)* > Derived for ($($T,)*)
    {
        async fn from_request(_rqctx: Arc<RequestContext>)
            -> Result<( $($T,)* ), HttpError>
        {
            Ok( ($($T::from_request(Arc::clone(&_rqctx)).await?,)* ) )
        }
    }
});

impl_derived_for_tuple!();
impl_derived_for_tuple!(T1);
impl_derived_for_tuple!(T1, T2);

/**
 * `HttpHandlerFunc` is a trait providing a single function, `handle_request()`,
 * which takes an HTTP request and produces an HTTP response (or
 * `HttpError`).
 *
 * As described above, handler functions can have a number of different
 * signatures.  They all consume a reference to the current request context.
 * They may also consume some number of extractor arguments.  The
 * `HttpHandlerFunc` trait is parametrized by the type `FuncParams`, which is
 * expected to be a tuple describing these extractor arguments.
 *
 * Below, we define implementations of `HttpHandlerFunc` for various function
 * types.  In this way, we can treat functions with different signatures as
 * different kinds of `HttpHandlerFunc`.  However, since the signature shows up
 * in the `FuncParams` type parameter, we'll need additional abstraction to
 * treat different handlers interchangeably.  See `RouteHandler` below.
 */
#[async_trait]
pub trait HttpHandlerFunc<FuncParams, ResponseType>:
    Send + Sync + 'static
where
    FuncParams: Derived,
    ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
{
    async fn handle_request(
        &self,
        rqctx: Arc<RequestContext>,
        p: FuncParams,
    ) -> Result<ResponseType, HttpError>;
}

/*
 * Note: the second element in the tuple below (the type parameter, `$T:tt`)
 * ought to be an "ident".  However, that causes us to run straight into
 * issue dtolnay/async-trait#46.
 */
macro_rules! impl_HttpHandlerFunc_for_func_with_params
    ( { $(($i:tt, $T:tt)),* } => {

    #[async_trait]
    impl<FuncType, FutureType, ResponseType, $($T,)*>
        HttpHandlerFunc<($($T,)*), ResponseType> for FuncType
    where
        FuncType: Fn(Arc<RequestContext>, $($T,)*)
            -> FutureType + Send + Sync + 'static,
        FutureType: Future<Output = Result<ResponseType, HttpError>>
            + Send + 'static,
        ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
        $($T: Derived + Send + Sync + 'static,)*
    {
        async fn handle_request(
            &self,
            rqctx: Arc<RequestContext>,
            _param_tuple: ($($T,)*)
        ) -> Result<ResponseType, HttpError>
        {
            (self)(rqctx, $(_param_tuple.$i,)*).await
        }
    }
});

impl_HttpHandlerFunc_for_func_with_params!();
impl_HttpHandlerFunc_for_func_with_params!((0, T0));
impl_HttpHandlerFunc_for_func_with_params!((0, T1), (1, T2));

/**
 * `RouteHandler` abstracts an `HttpHandlerFunc<FuncParams, ResponseType>` in a
 * way that allows callers to invoke the handler without knowing the handler's
 * function signature.
 *
 * The "Route" in `RouteHandler` refers to the fact that this structure is used
 * to record that a specific handler has been attached to a specific HTTP route.
 */
#[async_trait]
pub trait RouteHandler: Debug + Send + Sync {
    /**
     * Returns a description of this handler.  This might be a function name,
     * for example.  This is not guaranteed to be unique.
     */
    fn label(&self) -> &String;

    /**
     * Handle an incoming HTTP request.
     */
    async fn handle_request(&self, rqctx: RequestContext) -> HttpHandlerResult;
}

/**
 * `HttpRouteHandler` is the only type that implements `RouteHandler`.  The
 * reason both exist is that we need `HttpRouteHandler::new()` to consume an
 * arbitrary kind of `HttpHandlerFunc<FuncParams>` and return an object that's
 * _not_ parametrized by `FuncParams`.  In fact, the resulting
 * `HttpRouteHandler` _is_ parametrized by `FuncParams`, but we returned it
 * as a `RouteHandler` that does not have those type parameters, allowing the
 * caller to ignore the differences between different handler function type
 * signatures.
 */
pub struct HttpRouteHandler<HandlerType, FuncParams, ResponseType>
where
    HandlerType: HttpHandlerFunc<FuncParams, ResponseType>,
    FuncParams: Derived,
    ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
{
    /** the actual HttpHandlerFunc used to implement this route */
    handler: HandlerType,

    /** debugging label for the handler */
    label: String,

    /**
     * In order to define `new()` below, we need a type parameter `HandlerType`
     * that implements `HttpHandlerFunc<FuncParams>`, which means we also need a
     * `FuncParams` type parameter.  However, this type parameter would be
     * unconstrained, which makes Rust upset.  Use of PhantomData<FuncParams>
     * here causes the compiler to behave as though this struct referred to a
     * `FuncParams`, which allows us to use the type parameter below.
     */
    phantom: PhantomData<(FuncParams, ResponseType)>,
}

impl<HandlerType, FuncParams, ResponseType> Debug
    for HttpRouteHandler<HandlerType, FuncParams, ResponseType>
where
    HandlerType: HttpHandlerFunc<FuncParams, ResponseType>,
    FuncParams: Derived,
    ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "handler: {}", self.label)
    }
}

#[async_trait]
impl<HandlerType, FuncParams, ResponseType> RouteHandler
    for HttpRouteHandler<HandlerType, FuncParams, ResponseType>
where
    HandlerType: HttpHandlerFunc<FuncParams, ResponseType>,
    FuncParams: Derived + 'static,
    ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
{
    fn label(&self) -> &String {
        &self.label
    }

    async fn handle_request(
        &self,
        rqctx_raw: RequestContext,
    ) -> HttpHandlerResult {
        /*
         * This is where the magic happens: in the code below, `funcparams` has
         * type `FuncParams`, which is a tuple type describing the extractor
         * arguments to the handler function.  This could be `()`, `(Query<Q>)`,
         * `(Json<J>)`, `(Query<Q>, Json<J>)`, or any other combination of
         * extractors we decide to support in the future.  Whatever it is must
         * implement `Derived`, which means we can invoke
         * `Derived::from_request()` to construct the argument tuple, generally
         * from information available in the `request` object.  We pass this
         * down to the `HttpHandlerFunc`, for which there's a different
         * implementation for each value of `FuncParams`.  The `HttpHandlerFunc`
         * for each `FuncParams` just pulls the arguments out of the
         * `funcparams` tuple and makes them actual function arguments for the
         * actual handler function.  From this point down, all of this is
         * resolved statically.makes them actual function arguments for the
         * actual handler function.  From this point down, all of this is
         * resolved statically.
         */
        let rqctx = Arc::new(rqctx_raw);
        let funcparams = Derived::from_request(Arc::clone(&rqctx)).await?;
        let future = self.handler.handle_request(rqctx, funcparams);
        let response_responsetype: ResponseType = future.await?;
        let response_as_wrap: HttpResponseWrap = response_responsetype.into();
        let response_as_response: Response<Body> = response_as_wrap.wrapped?;
        Ok(response_as_response)
    }
}

/*
 * Public interfaces
 */

impl<HandlerType, FuncParams, ResponseType>
    HttpRouteHandler<HandlerType, FuncParams, ResponseType>
where
    HandlerType: HttpHandlerFunc<FuncParams, ResponseType>,
    FuncParams: Derived + 'static,
    ResponseType: Into<HttpResponseWrap> + Send + Sync + 'static,
{
    /**
     * Given a function matching one of the supported API handler function
     * signatures, return a RouteHandler that can be used to respond to HTTP
     * requests using this function.
     */
    pub fn new(handler: HandlerType) -> Box<dyn RouteHandler> {
        HttpRouteHandler::new_with_name(handler, "<unlabeled handler>")
    }

    /**
     * Given a function matching one of the supported API handler function
     * signatures, return a RouteHandler that can be used to respond to HTTP
     * requests using this function.
     */
    pub fn new_with_name(
        handler: HandlerType,
        label: &str,
    ) -> Box<dyn RouteHandler> {
        Box::new(HttpRouteHandler {
            label: label.to_string(),
            handler: handler,
            phantom: PhantomData,
        })
    }
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
pub struct Query<QueryType: Send + Sync> {
    inner: QueryType,
}

impl<QueryType: Send + Sync> Query<QueryType> {
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
fn http_request_load_query<QueryType: Send + Sync>(
    request: &Request<Body>,
) -> Result<Query<QueryType>, HttpError>
where
    QueryType: DeserializeOwned,
{
    let raw_query_string = request.uri().query().unwrap_or("");
    /*
     * TODO-correctness: are query strings defined to be urlencoded in this way?
     */
    match serde_urlencoded::from_str(raw_query_string) {
        Ok(q) => Ok(Query {
            inner: q,
        }),
        Err(e) => Err(HttpError::for_bad_request(format!(
            "unable to parse query string: {}",
            e
        ))),
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
    QueryType: DeserializeOwned + Send + Sync + 'static,
{
    async fn from_request(
        rqctx: Arc<RequestContext>,
    ) -> Result<Query<QueryType>, HttpError> {
        let request = rqctx.request.lock().await;
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
pub struct Json<JsonType: Send + Sync> {
    inner: JsonType,
}

impl<JsonType: Send + Sync> Json<JsonType> {
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
    rqctx: Arc<RequestContext>,
) -> Result<Json<JsonType>, HttpError>
where
    JsonType: DeserializeOwned + Send + Sync,
{
    let server = &rqctx.server;
    let mut request = rqctx.request.lock().await;
    let body_bytes = http_read_body(
        request.body_mut(),
        server.config.request_body_max_bytes,
    )
    .await?;
    let value: Result<JsonType, serde_json::Error> =
        serde_json::from_slice(&body_bytes);
    match value {
        Ok(j) => Ok(Json {
            inner: j,
        }),
        Err(e) => Err(HttpError::for_bad_request(format!(
            "unable to parse body JSON: {}",
            e
        ))),
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
    async fn from_request(
        rqctx: Arc<RequestContext>,
    ) -> Result<Json<JsonType>, HttpError> {
        http_request_load_json_body(rqctx).await
    }
}

/*
 * Response handler types
 * TODO document me
 */
pub struct HttpResponseWrap {
    wrapped: Result<Response<Body>, HttpError>,
}

impl HttpResponseWrap {
    pub fn new(result: Result<Response<Body>, HttpError>) -> HttpResponseWrap {
        HttpResponseWrap {
            wrapped: result,
        }
    }
}

impl From<Result<Response<Body>, HttpError>> for HttpResponseWrap {
    fn from(result: Result<Response<Body>, HttpError>) -> HttpResponseWrap {
        HttpResponseWrap::new(result)
    }
}

impl From<Response<Body>> for HttpResponseWrap {
    fn from(response: Response<Body>) -> HttpResponseWrap {
        HttpResponseWrap::new(Ok(response))
    }
}
