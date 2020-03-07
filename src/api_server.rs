/*!
 * server-wide state and facilities
 */

use crate::api_handler::RequestContext;
use crate::api_error::ApiHttpError;
use crate::api_model::ApiBackend;
use crate::api_http_router::HttpRouter;
use crate::api_http_entrypoints;
use crate::sim;
use sim::SimulatorBuilder;

use futures::FutureExt;
use futures::lock::Mutex;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use hyper::server::conn::AddrStream;
use hyper::service::Service;
use std::future::Future;
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
    pub backend: Arc<dyn ApiBackend + Send + Sync>,
    /** static server configuration parameters */
    pub config: ApiServerConfig,
    /** request router */
    pub router: HttpRouter,
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
pub fn api_server_create(bind_address: &SocketAddr)
    -> Result<ApiHttpServer, hyper::error::Error>
{
    let mut simbuilder = SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    let mut router = HttpRouter::new();
    api_http_entrypoints::api_register_entrypoints(&mut router);

    /* TODO-hardening: this should not be built in except under test. */
    test_endpoints::register_test_endpoints(&mut router);

    /* TODO-cleanup too many Arcs? */
    let app_state = Arc::new(ApiServerState {
        backend: Arc::new(simbuilder.build()),
        config: ApiServerConfig {
            /* We start aggressively to make sure we cover this in our tests. */
            request_body_max_bytes: 1024
        },
        router: router
    });

    let make_service = ApiServerConnectionHandler::new(app_state);
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
    request: Request<Body>)
    -> Result<Response<Body>, ApiHttpError>
{
    /*
     * TODO-hardening: is it correct to (and do we correctly) read the entire
     * request body even if we decide it's too large and are going to send a 400
     * response?
     * TODO-hardening: add a request read timeout as well so that we don't allow
     * this to take forever.
     * TODO-correctness: check that URL processing (particularly with slashes as
     * the only separator) is correct.  (Do we need to URL-escape or un-escape
     * here?  Redirect container URls that don't end it "/"?)
     * TODO-correctness: Do we need to dump the body on errors?
     */
    let method = request.method();
    let uri = request.uri();
    eprintln!("handling request: method = {}, uri = {}", method.as_str(), uri);
    let lookup_result = server.router.lookup_route(&method, uri.path())?;
    let rqctx = RequestContext {
        server: Arc::clone(&server),
        request: Arc::new(Mutex::new(request)),
        path_variables: lookup_result.variables,
    };
    return lookup_result.handler.handle_request(rqctx).await;
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
 * Demo handler functions
 * TODO-cleanup these should not be registered except in test mode.  They kind
 * of belong under #[cfg(test)], except that we want to be able to use these
 * from integration tests.
 * Maybe instead we should have the unit / integration test instantiate its own
 * server with handlers like these.
 */
pub mod test_endpoints {
    use crate::api_error::ApiHttpError;
    use crate::api_handler::Json;
    use crate::api_handler::Query;
    use crate::api_handler::RequestContext;
    use crate::api_handler::api_handler_create;
    use crate::api_http_router::HttpRouter;
    use crate::api_http_util::CONTENT_TYPE_JSON;
    use http::StatusCode;
    use hyper::Body;
    use hyper::Method;
    use hyper::Response;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Arc;

    pub fn register_test_endpoints(router: &mut HttpRouter)
    {
        router.insert(Method::GET, "/testing/demo1",
            api_handler_create(demo_handler_args_1));
        router.insert(Method::GET, "/testing/demo2query",
            api_handler_create(demo_handler_args_2query));
        router.insert(Method::GET, "/testing/demo2json",
            api_handler_create(demo_handler_args_2json));
        router.insert(Method::GET, "/testing/demo3",
            api_handler_create(demo_handler_args_3));
    }

    async fn demo_handler_args_1(_rqctx: Arc<RequestContext>)
        -> Result<Response<Body>, ApiHttpError>
    {
        Ok(Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .status(StatusCode::OK)
            .body("demo_handler_args_1\n".into())?)
    }

    #[derive(Serialize, Deserialize)]
    pub struct DemoQueryArgs {
        pub test1: String,
        pub test2: Option<u32>
    }

    async fn demo_handler_args_2query(
        _rqctx: Arc<RequestContext>,
        query: Query<DemoQueryArgs>)
        -> Result<Response<Body>, ApiHttpError>
    {
        Ok(Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .status(StatusCode::OK)
            .body(serde_json::to_string(&query.into_inner()).unwrap().into())?)
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct DemoJsonBody {
        pub test1: String,
        pub test2: Option<u32>
    }

    async fn demo_handler_args_2json(
        _rqctx: Arc<RequestContext>,
        json: Json<DemoJsonBody>)
        -> Result<Response<Body>, ApiHttpError>
    {
        Ok(Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .status(StatusCode::OK)
            .body(serde_json::to_string(&json.into_inner()).unwrap().into())?)
    }

    #[derive(Serialize)]
    pub struct DemoJsonAndQuery {
        pub query: DemoQueryArgs,
        pub json: DemoJsonBody
    }
    async fn demo_handler_args_3(
        _rqctx: Arc<RequestContext>,
        query: Query<DemoQueryArgs>,
        json: Json<DemoJsonBody>)
        -> Result<Response<Body>, ApiHttpError>
    {
        let combined = DemoJsonAndQuery {
            query: query.into_inner(),
            json: json.into_inner(),
        };
        Ok(Response::builder()
            .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .status(StatusCode::OK)
            .body(serde_json::to_string(&combined).unwrap().into())?)
    }
}
