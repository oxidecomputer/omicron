/*!
 * server-wide state and facilities
 */

use crate::api_error::ApiHttpError;
use crate::api_model::ApiBackend;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_http_entrypoints;
use crate::sim;
use sim::SimulatorBuilder;

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BufMut;
use futures::FutureExt;
use http::StatusCode;
use hyper::Body;
use hyper::Method;
use hyper::Request;
use hyper::Response;
use hyper::body::HttpBody;
use hyper::server::conn::AddrStream;
use hyper::service::Service;
use serde::Serialize;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

/* TODO Replace this with ApiError? */
type GenericError = Box<dyn std::error::Error + Send + Sync>;

/**
 * Stores shared state used by API endpoints
 */
pub struct ApiServerState {
    /** the API backend to use for servicing requests */
    pub backend: Arc<dyn ApiBackend>,
    /** static server configuration parameters */
    pub config: ApiServerConfig,
}

/**
 * Stores static configuration associated with the server
 */
pub struct ApiServerConfig {
    /** maximum allowed size of a request body */
    pub request_body_max_bytes: usize
}

/**
 * A thin wrapper around a Hyper Server object that exposes some interfaces that
 * we find useful (e.g., close()).
 * TODO-cleanup: this mechanism should probably do better with types.  In
 * particular, once you call run(), you shouldn't be able to call it again
 * (i.e., it should consume self).  But you should be able to close() it.  Once
 * you've called close(), you shouldn't be able to call it again.
 */
pub struct ApiHttpServer {
    server_future: Option<Pin<Box<
        dyn Future<Output=Result<(), hyper::error::Error>> + Send
    >>>,
    local_addr: SocketAddr,
    close_channel: Option<tokio::sync::oneshot::Sender<()>>
} 

impl ApiHttpServer {
    pub fn local_addr(&self)
        -> SocketAddr
    {
        self.local_addr.clone()
    }

    pub fn close(mut self)
    {
        /*
         * It should be impossible to close a channel that's already been closed
         * because close() consumes self.  It should also be impossible to fail
         * to send the close signal because nothing else can cause the server to
         * exit.
         */
        let channel = self.close_channel.take().expect("already closed somehow");
        channel.send(()).expect("failed to send close signal");
    }

    pub fn run(&mut self)
        -> tokio::task::JoinHandle<Result<(), hyper::error::Error>>
    {
        let future = self.server_future.take()
            .expect("cannot run() more than once");
        tokio::spawn(async {
            future.await
        })
    }
}

/**
 * Set up an HTTP server bound on the specified address that runs the API.  You
 * must invoke `run()` on the returned instance of `ApiHttpServer` (and await
 * the result) to actually start the server.  You can call `close()` to begin a
 * graceful shutdown of the server, which will be complete when the `run()`
 * Future is resolved.
 */
pub fn setup_server(bind_address: &SocketAddr)
    -> Result<ApiHttpServer, hyper::error::Error>
{
    let app_state = setup_server_state();
    let make_service = server_handler(app_state);
    let builder = hyper::Server::try_bind(bind_address)?;
    let server = builder.serve(make_service);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let local_addr = server.local_addr();
    let graceful = server.with_graceful_shutdown(async {
        rx.await.ok();
    });

    Ok(ApiHttpServer {
        server_future: Some(graceful.boxed()),
        local_addr: local_addr,
        close_channel: Some(tx),
    })
}

/**
 * Set up initial server-wide shared state.
 * TODO-cleanup too many Arcs?
 */
pub fn setup_server_state()
    -> Arc<ApiServerState>
{
    let mut simbuilder = SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    Arc::new(ApiServerState {
        backend: Arc::new(simbuilder.build()),
        config: ApiServerConfig {
            /* We start aggressively to make sure we cover this in our tests. */
            request_body_max_bytes: 1024
        }
    })
}

/**
 * Returns a Service intended to be used with a hyper Server to accept
 * connections and handle them using our API implementation.
 * TODO-cleanup: should return a trait type (Service)?  Then
 * ApiServerConnectionHandler can be non-public.
 */
pub fn server_handler(app_state: Arc<ApiServerState>)
    -> ApiServerConnectionHandler
{
    ApiServerConnectionHandler::new(app_state)
}


/**
 * Initial entry point for handling a new connection to the HTTP server.
 * This is invoked by Hyper when a new connection is accepted.  This function
 * must return a Hyper Service object that will handle requests for this
 * connection.
 */
async fn http_connection_handle(
    server: Arc<ApiServerState>,
    remote_addr: SocketAddr)
    -> Result<ApiServerRequestHandler, GenericError>
{
    eprintln!("accepted connection from: {}", remote_addr);
    Ok(ApiServerRequestHandler::new(server))
}

/**
 * Initial entry point for handling a new request to the HTTP server.  This is
 * invoked by Hyper when a new request is received.  This function returns a
 * Result that either represents a valid HTTP response or an error (which will
 * also get turned into an HTTP response).
 */
async fn http_request_handle_wrap(
    server: Arc<ApiServerState>,
    request: Request<Body>)
    -> Result<Response<Body>, GenericError>
{
    /*
     * This extra level of indirection makes error handling much more
     * straightforward, since the request handling code can simply return early
     * with an error and we'll treat it like an error from any of the endpoints
     * themselves.
     */
    match http_request_handle(server, request).await {
        Ok(response) => Ok(response),
        Err(e) => Ok(e.into_response())
    }
}

async fn http_request_handle(
    server: Arc<ApiServerState>,
    mut request: Request<Body>)
    -> Result<Response<Body>, ApiHttpError>
{
    /*
     * For now, we essentially use statically-defined request routing -- namely,
     * the code below calls the appropriate functions.
     *
     * The first step will be to read the entire request body.  Whether it's a
     * HEAD or GET (in which case the body should be 0 bytes) or a PUT or POST
     * (in which case we're going to parse the body as JSON anyway in order to
     * process it), we want to read the whole body.
     *
     * Note that in order to bound memory usage and avoid DoS from broken or
     * malicious clients, we put aggressive caps on how many bytes of the
     * body we'll actually store.  If the body exceeds that cap, we'll end up
     * reporting a 400-level error.  However, to preserve HTTP framing, we want
     * to read the whole request before sending the response.
     * TODO-hardening: check that this is appropriate behavior
     * TODO-hardening: add a request read timeout as well so that we don't allow
     * this to take forever.
     * TODO-correctness: check that URL processing (particularly with slashes as
     * the only separator) is correct.  (Do we need to URL-escape or un-escape
     * here?  Redirect container URls that don't end it "/"?)
     * TODO-cleanup: use "url" crate for better URL parsing?
     */
    let method = request.method().clone();
    let uri = request.uri().clone();
    let uri_parts : Vec<&str> = uri
        .path()
        .split("/")
        .filter(|s| *s != "")
        .collect();
    eprintln!("handling request: method = {}, uri = {}", method.as_str(), uri);

    /* XXX Testing the ApiHandler stuff. */
    let mut generic_handler: Option<Box<dyn ApiHandler>> = None;
    if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo0" {
        generic_handler = Some(Box::new(ApiHandlerFunc0{ func: demo_handler_args_0 }));
    }
    if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo1" {
        generic_handler = Some(Box::new(ApiHandlerFunc1{ func: demo_handler_args_1 }));
    }
    if generic_handler.is_some() {
        return invoke_handler(server, request, generic_handler.unwrap()).await;
    }
    // if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo2" {
    //     return invoke_handler(server, request, demo_handler_args_2).await;
    // }
    // if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo3" {
    //     return invoke_handler(server, request,
    //         FuncQueryWrapper{
    //             func: demo_handler_args_3query,
    //             phantom: PhantomData
    //         }).await;
    // }

    /*
     * The URI must have started with a "/".  When we split by "/", we should
     * get at least two components: at least one for the characters before that
     * initial "/" character, and one for the characters after it.  However,
     * both could be empty, as in the case of the URI "/", and we filtered out
     * empty components above.
     *
     * We currently only support paths under "/projects", so if we had zero
     * non-empty components (which would indicate the resource "/") or anything
     * whose first component is not "projects", bail out now.
     */
    if uri_parts.is_empty() || uri_parts[0] != "projects" {
        // TODO do we need to do this?  Will hyper do it for us?
        http_dump_body(request.body_mut()).await?;
        return Err(ApiHttpError::for_status(StatusCode::NOT_FOUND));
    }

    /*
     * Similarly, we don't support any nested resources beneath
     * "/projects/{project_id}", so if there are more than two URI components,
     * bail out.
     */
    if uri_parts.len() > 2 {
        // TODO do we need to do this?  Will hyper do it for us?
        http_dump_body(request.body_mut()).await?;
        return Err(ApiHttpError::for_status(StatusCode::NOT_FOUND));
    }

    /*
     * Operations on specific projects.
     */
    if uri_parts.len() == 2 {
        let project_id = uri_parts[1];

        /* PUT /projects/{project_id} */
        if method == Method::PUT {
            let project_id = uri_parts[1];
            let body_bytes = http_read_body(
                request.body_mut(), server.config.request_body_max_bytes).await?;
            let update_params: ApiProjectUpdateParams =
                serde_json::from_slice(&body_bytes)?;
            let response = api_http_entrypoints::api_projects_put_project(
                &server, project_id.to_string(), &update_params).await?;
            return Ok(response);
        }

        /*
         * The remaining supported methods do not support bodies.
         */
        let nbytesread = http_dump_body(request.body_mut()).await?;
        if nbytesread != 0 {
            return Err(ApiHttpError::for_bad_request(
                "expected empty body".to_string()));
        }

        /* GET /projects/{project_id} */
        if method == Method::GET {
            let response = api_http_entrypoints::api_projects_get_project(
                &server, project_id.to_string()).await?;
            return Ok(response);
        }

        /* DELETE /projects/{project_id} */
        if method == Method::DELETE {
            let response = api_http_entrypoints::api_projects_delete_project(
                &server, project_id.to_string()).await?;
            return Ok(response);
        }

        return Err(ApiHttpError::for_status(StatusCode::METHOD_NOT_ALLOWED));
    }

    /*
     * Operations on the "/projects" collection.
     */
    assert!(uri_parts.len() == 1 && uri_parts[0] == "projects");

    /* POST /projects */
    if method == Method::POST {
        let body_bytes = http_read_body(
            request.body_mut(), server.config.request_body_max_bytes).await?;
        let create_params: ApiProjectCreateParams =
            serde_json::from_slice(&body_bytes)?;
        // TODO-understanding: "server" is an Arc.  How is it that we can pass a
        // reference to it when the callee is expecting the type itself?  Does
        // it implicitly get cloned?  Are we passing ownership?
        let response = api_http_entrypoints::api_projects_post(
            &server, &create_params).await?;
        return Ok(response);
    }

    /* GET /projects */
    if method == Method::GET {
        /* GETs do not support bodies. */
        let nbytesread = http_dump_body(request.body_mut()).await?;
        if nbytesread != 0 {
            return Err(ApiHttpError::for_bad_request(
                "expected empty body".to_string()));
        }

        // XXX support for query params
        let query_params = api_http_entrypoints::ListQueryParams {
            marker: None,
            limit: Some(10)
        };
        let response = api_http_entrypoints::api_projects_get(
            &server, &query_params).await?;
        return Ok(response);
    }

    return Err(ApiHttpError::for_status(StatusCode::METHOD_NOT_ALLOWED));
}

/**
 * Reads the rest of the body from the request up to the given number of bytes.
 * If the body fits within the specified cap, a buffer is returned with all the
 * bytes read.  If not, an error is returned.
 */
async fn http_read_body<T>(body: &mut T, cap: usize)
    -> Result<Bytes, ApiHttpError>
    where T: HttpBody<Data=Bytes, Error=hyper::error::Error> + std::marker::Unpin,
{
    /*
     * This looks a lot like the implementation of hyper::body::to_bytes(), but
     * applies the requested cap.  We've skipped the optimization for the
     * 1-buffer case for now, as it seems likely this implementation will change
     * anyway.
     * TODO should this use some Stream interface instead?
     * TODO why does this look so different in type signature (Data=Bytes,
     * std::marker::Unpin, &mut T)
     * TODO Error type shouldn't have to be hyper Error -- Into<ApiError> should
     * work too?
     */
    let mut parts = std::vec::Vec::new();
    let mut nbytesread: usize = 0;
    while let Some(maybebuf) = body.data().await {
        let buf = maybebuf?;
        let bufsize = buf.len();

        if nbytesread + bufsize > cap {
            http_dump_body(body).await?;
            // TODO-correctness check status code
            return Err(ApiHttpError::for_bad_request(
                format!("request body exceeded maximum size of {} bytes", cap)));
        }

        nbytesread += bufsize;
        parts.put(buf);
    }

    /*
     * Read the trailers as well, even though we're not going to do anything
     * with them.
     */
    body.trailers().await?;
    /*
     * TODO-correctness why does the is_end_stream() assertion fail and the next
     * one panic?
     */
    // assert!(body.is_end_stream());
    // assert!(body.data().await.is_none());
    // assert!(body.trailers().await?.is_none());
    Ok(parts.into())
}

/**
 * Reads the rest of the body from the request, dropping all the bytes.  This is
 * useful after encountering error conditions.
 */
async fn http_dump_body<T>(body: &mut T)
    -> Result<usize, T::Error>
    where T: HttpBody<Data=Bytes> + std::marker::Unpin
{
    /*
     * TODO should this use some Stream interface instead?
     * TODO-hardening: does this actually cap the amount of data that will be
     * read?  What if the underlying implementation chooses to wait for a much
     * larger number of bytes?
     * TODO better understand pin_mut!()
     */
    let mut nbytesread: usize = 0;
    while let Some(maybebuf) = body.data().await {
        let buf = maybebuf?;
        nbytesread += buf.len();
    }

    assert!(body.is_end_stream());
    Ok(nbytesread)
}

/**
 * ApiServerConnectionHandler is a Hyper Service implementation that forwards
 * incoming connections to `http_connection_handle()`, providing the server
 * state object as an additional argument.  We could use `make_service_fn` here
 * using a closure to capture the state object, but the resulting code is a bit
 * simpler without it.
 */
pub struct ApiServerConnectionHandler {
    /** backend state that will be made available to the connection handler */
    server: Arc<ApiServerState>
}

impl ApiServerConnectionHandler
{
    /**
     * Create an ApiServerConnectionHandler with the given state object that
     * will be made available to the handler.
     */
    fn new(server: Arc<ApiServerState>)
        -> Self
    {
        ApiServerConnectionHandler {
            server: Arc::clone(&server)
        }
    }
}

impl Service<&AddrStream> for ApiServerConnectionHandler
{
    /*
     * Recall that a Service in this context is just something that takes a
     * request (which could be anything) and produces a response (which could be
     * anything).  This being a connection handler, the request type is an
     * AddrStream (which wraps a TCP connection) and the response type is
     * another Service: one that accepts HTTP requests and produces HTTP
     * responses.
     */
    type Response = ApiServerRequestHandler;
    type Error = GenericError;
    type Future = Pin<Box<
        dyn Future<Output = Result<Self::Response, Self::Error>> + Send
    >>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        // TODO is this right?
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, conn: &AddrStream)
        -> Self::Future
    {
        /*
         * We're given a borrowed reference to the AddrStream, but our interface
         * is async (which is good, so that we can support time-consuming
         * operations as part of receiving requests).  To avoid having to ensure
         * that conn's lifetime exceeds that of this async operation, we simply
         * copy the only useful information out of the conn: the SocketAddr.  We
         * may want to create our own connection type to encapsulate the socket
         * address and any other per-connection state that we want to keep.
         */
        let server = Arc::clone(&self.server);
        let remote_addr = conn.remote_addr();
        Box::pin(http_connection_handle(server, remote_addr))
    }
}

/**
 * ApiServerRequestHandler is a Hyper Service implementation that forwards
 * incoming requests to `http_request_handle_wrap()`, including as an argument
 * the backend server state object.  We could use `service_fn` here using a
 * closure to capture the server state object, but the resulting code is a bit
 * simpler without all that.
 */
pub struct ApiServerRequestHandler {
    /** backend state that will be made available to the request handler */
    server: Arc<ApiServerState>
}

impl ApiServerRequestHandler
{
    /**
     * Create an ApiServerRequestHandler object with the given state object that
     * will be provided to the handler function.
     */
    fn new(server: Arc<ApiServerState>)
        -> Self
    {
        ApiServerRequestHandler {
            server: Arc::clone(&server)
        }
    }
}

impl Service<Request<Body>> for ApiServerRequestHandler
{
    type Response = Response<Body>;
    type Error = GenericError;
    type Future = Pin<Box<
        dyn Future<Output = Result<Self::Response, Self::Error> > + Send
    >>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>)
        -> Poll<Result<(), Self::Error>>
    {
        // TODO is this right?
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>)
        -> Self::Future
    {
        Box::pin(http_request_handle_wrap(Arc::clone(&self.server), req))
    }
}


/*
 * API HANDLER INTERFACE
 * TODO implement me!  This is vaporware.
 *
 * We consider it a key goal to be able to automatically generate an OpenAPI
 * specification from our API endpoint handler functions.  That way, the
 * implementation serves as the source of truth, and it cannot get out of sync
 * with the API spec that we publish.  For this to work and for the generated
 * spec to describe the schemas of both inputs (particulary: query parameters
 * and request bodies) and outputs (status codes and response bodies), we want
 * the API handler functions themselves to consume parameters that describe
 * their actual inputs (called extractors) and produce a result that describes
 * the output.  That sounds obvious, but it rules out the more obvious approach
 * of retrieving parameters from a generic request object and producing a
 * generic response object.  It also means we need to support API handler
 * functions with a variety of type signatures.
 *
 * To achieve this, we take an approach inspired by the actix-web crate (but
 * much simpler and more explicit than that crate's implementation):
 *
 * - We define an ApiHandler trait with just one method called "handle_request"
 *   that takes a single parameter and asynchronously produces a single output.
 *   ApiHandler is parametrized so that the output type may be any object
 *   implementing Serializable or a Stream of objects implementing Serializable.
 *
 * - We define implementations for ApiHandler for the following function
 *   signatures:
 *
 *       SIGNATURE                          CONSUMES
 *       f()                                nothing
 *       f(server)                          server state only
 *       f(server, request)                 server state and request
 *       f(server, request, query)          above plus query parameters
 *       f(server, request, body)           above plus a request body
 *       f(server, request, query, body)    all of the above
 *
 *   where
 *
 *       "server" is Arc<ApiServerState>
 *       "request" is an HTTP Request object for access to more info
 *       "query" is an extractor for parameters from query strings
 *       "body" is an extractor for a request body
 *
 *   The "query" and "body" extractors look similar to Query and Json in Actix:
 *   they are parametrized by a specific type and deserialize either query
 *   parameters or a JSON object into that type.  For example, Json<T> causes
 *   the body to fully read and parsed into an instance of T (which must derive
 *   serde::Deserialize).  This process is type-safe.  Failure to extract the
 *   parameters will result in a 400 response sent to the client with an error
 *   message.  (The type parameter for Query and Json can contain Option values
 *   in order to make a parameter optional.)
 *
 *   The implementations of ApiHandler for these various function types take
 *   care of handling the appropriate extraction.  This is a lot less flexible
 *   than the actix-web approach, but that flexibility does not appear useful in
 *   this context and this solution has the advantage of being much more
 *   explicit.
 *
 * - As a transition step, in the handcoded router above, instead of deciding by
 *   hand whether to parse a body, extract query parameters, etc., we will just
 *   find the appropriate API endpoint function, treat it as an ApiHandler, and
 *   invoke "handle_request".
 *
 * - The next step will be to build a proper router using a more general data
 *   structure and algorithm, rather than implementing it by hand in
 *   http_request_handle().
 *
 * - Separately, we should figure out how to allow the handler functions to
 *   return more specific types while allowing them to be treated uniformly from
 *   http_request_handle() (and in the above data structures).
 */

#[async_trait]
trait ApiHandler: Send + Sync {
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        request: Request<Body>)
        -> Result<Response<Body>, ApiHttpError>;
}

struct ApiHandlerFunc0<F, T>
    where F: Fn() -> T + Send + Sync + 'static,
          T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static
{
    func: F
}

impl<F, T> From<F> for ApiHandlerFunc0<F, T>
    where F: Fn() -> T + Send + Sync + 'static,
          T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static
{
    fn from(func: F)
        -> ApiHandlerFunc0<F, T>
    {
        ApiHandlerFunc0{ func: func }
    }
}

#[async_trait]
impl<F, T> ApiHandler for ApiHandlerFunc0<F, T>
    where F: Fn() -> T + Send + Sync + 'static,
          T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static
{
    async fn handle_request(&self,
        _server: Arc<ApiServerState>,
        _request: Request<Body>)
        -> Result<Response<Body>, ApiHttpError>
    {
        (self.func)().await
    }
}

struct ApiHandlerFunc1<F, T>
    where F: Fn(Arc<ApiServerState>) -> T + Send + Sync + 'static,
          T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static
{
    func: F
}

#[async_trait]
impl<F, T> ApiHandler for ApiHandlerFunc1<F, T>
    where F: Fn(Arc<ApiServerState>) -> T + Send + Sync + 'static,
          T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static
{
    async fn handle_request(&self,
        server: Arc<ApiServerState>,
        _request: Request<Body>)
        -> Result<Response<Body>, ApiHttpError>
    {
        (self.func)(server).await
    }
}

async fn invoke_handler(
    server: Arc<ApiServerState>,
    request: Request<Body>,
    handler: Box<dyn ApiHandler>)
    -> Result<Response<Body>, ApiHttpError>
{
    handler.handle_request(server, request).await
}

async fn demo_handler_args_0()
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("demo_handler_args_0".into())?)
}

async fn demo_handler_args_1(_server: Arc<ApiServerState>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("demo_handler_args_1".into())?)
}

async fn demo_handler_args_2(
    _server: Arc<ApiServerState>,
    _request: Request<Body>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("demo_handler_args_2".into())?)
}

// /*
//  * Query support
//  */
// struct Query<QueryType> {
//     inner: QueryType
// }
// impl<QueryType> Query<QueryType> {
//     /*
//      * TODO drop this in favor of Deref?  + Display and Debug for convenience?
//      */
//     fn into_inner(self) -> QueryType {
//         self.inner
//     }
// }
// 
// struct FuncQueryWrapper<F, T, Q>
//     where F: Fn(Arc<ApiServerState>, Request<Body>, Query<Q>) -> T + Send + Sync + 'static,
//           T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static,
//           Q: DeserializeOwned + Send + Sync
// {
//     func: F,
//     phantom: PhantomData<Q>
// }
// 
// #[async_trait]
// impl<F, T, Q> ApiHandler<ApiHandlerImplArgs3Query> for FuncQueryWrapper<F, T, Q>
//     where F: Fn(Arc<ApiServerState>, Request<Body>, Query<Q>) -> T + Send + Sync + 'static,
//           T: Future<Output=Result<Response<Body>, ApiHttpError>> + Send + Sync + 'static,
//           Q: DeserializeOwned + Send + Sync
// {
//     async fn handle_request(&self,
//         server: Arc<ApiServerState>,
//         request: Request<Body>)
//         -> Result<Response<Body>, ApiHttpError>
//     {
//         let query: Query<Q> = http_request_load_query(&request)?;
//         (self.func)(server, request, query).await
//     }
// }
// 
// fn http_request_load_query<Q>(request: &Request<Body>)
//     -> Result<Query<Q>, ApiHttpError>
//     where Q: DeserializeOwned
// {
//     let raw_query_string = request.uri().query().unwrap_or("");
//     /*
//      * TODO-correctness: are query strings defined to be urlencoded in this way?
//      */
//     match serde_urlencoded::from_str(raw_query_string) {
//         Ok(q) => Ok(Query { inner: q }),
//         Err(e) => Err(ApiHttpError::for_bad_request(
//             format!("unable to parse query string: {}", e)))
//     }
// 
// }
//  
// #[derive(Serialize, Deserialize)]
// struct DemoQueryArgs {
//     test1: String,
//     test2: Option<u32>
// }
// 
// async fn demo_handler_args_3query(
//     _server: Arc<ApiServerState>,
//     _request: Request<Body>,
//     query: Query<DemoQueryArgs>)
//     -> Result<Response<Body>, ApiHttpError>
// {
//     Ok(Response::builder()
//         .status(StatusCode::OK)
//         .body(serde_json::to_string(&query.into_inner()).unwrap().into())?)
// }
