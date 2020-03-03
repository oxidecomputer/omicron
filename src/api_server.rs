/*!
 * server-wide state and facilities
 */

use crate::api_handler::api_handler_create;
use crate::api_handler::RequestContext;
use crate::api_handler::RouteHandler;
use crate::api_handler::Json;
use crate::api_handler::Query;
use crate::api_error::ApiHttpError;
use crate::api_model::ApiBackend;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_http_router::HttpRouter;
use crate::api_http_util::http_dump_body;
use crate::api_http_util::http_read_body;
use crate::api_http_entrypoints;
use crate::sim;
use sim::SimulatorBuilder;

use futures::FutureExt;
use futures::lock::Mutex;
use http::StatusCode;
use hyper::Body;
use hyper::Method;
use hyper::Request;
use hyper::Response;
use hyper::server::conn::AddrStream;
use hyper::service::Service;
use serde::Deserialize;
use serde::Serialize;
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
    // /** request router */
    // pub router: HttpRouter,
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

    /* TODO-cleanup too many Arcs? */
    let app_state = Arc::new(ApiServerState {
        backend: Arc::new(simbuilder.build()),
        config: ApiServerConfig {
            /* We start aggressively to make sure we cover this in our tests. */
            request_body_max_bytes: 1024
        },
   //     router: HttpRouter::new()
    });

    let bar: Box<dyn crate::api_handler::ApiHandler<(Query<_>,)>> =
        Box::new(api_http_entrypoints::api_projects_get);
    // app_state.router.insert(Method::GET, "/projects",
    //     api_handler_create(api_http_entrypoints::api_projects_get));
    // app_state.router.insert(Method::POST, "/projects",
    //     api_handler_create(api_http_entrypoints::api_projects_post));
    // app_state.router.insert(Method::GET, "/projects/{project_id}",
    //     api_handler_create(api_http_entrypoints::api_projects_get_project));
    // app_state.router.insert(Method::DELETE, "/projects/{project_id}",
    //     api_handler_create(api_http_entrypoints::api_projects_delete_project));
    // app_state.router.insert(Method::PUT, "/projects/{project_id}",
    //     api_handler_create(api_http_entrypoints::api_projects_put_project));

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
    let uri_path = uri.path();
    let uri_parts : Vec<&str> = uri_path
        .split("/")
        .filter(|s| *s != "")
        .collect();
    eprintln!("handling request: method = {}, uri = {}", method.as_str(), uri);

    /* XXX Testing the ApiHandler stuff. */
    let mut generic_handler: Option<Box<dyn RouteHandler>> = None;
    if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo0" {
        generic_handler = Some(api_handler_create(demo_handler_args_0));
    }
    if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo1" {
        generic_handler = Some(api_handler_create(demo_handler_args_1));
    }
    if method == Method::GET && uri_parts.len() == 1 && uri_parts[0] == "foo3query" {
        generic_handler = Some(api_handler_create(demo_handler_args_3query));
    }
    if method == Method::PUT && uri_parts.len() == 1 && uri_parts[0] == "foo3json" {
        generic_handler = Some(api_handler_create(demo_handler_args_3json));
    }
    if method == Method::PUT && uri_parts.len() == 1 && uri_parts[0] == "foo4" {
        generic_handler = Some(api_handler_create(demo_handler_args_4));
    }

    if generic_handler.is_some() {
        let rqctx = RequestContext {
            server: server,
            request: Arc::new(Mutex::new(request)),
            path_variables: std::collections::BTreeMap::new(),
        };
        return generic_handler.unwrap().handle_request(rqctx).await;
    }

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

//    /* GET /projects */
//    if method == Method::GET {
//        /* GETs do not support bodies. */
//        let nbytesread = http_dump_body(request.body_mut()).await?;
//        if nbytesread != 0 {
//            return Err(ApiHttpError::for_bad_request(
//                "expected empty body".to_string()));
//        }
//
//        // XXX support for query params
//        let query_params = api_http_entrypoints::ListQueryParams {
//            marker: None,
//            limit: Some(10)
//        };
//        let response = api_http_entrypoints::api_projects_get(
//            &server, &query_params).await?;
//        return Ok(response);
//    }

    return Err(ApiHttpError::for_status(StatusCode::METHOD_NOT_ALLOWED));
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
 * TODO-cleanup compile and expose these only under cfg(test).  We do want to
 * test these to guarantee that we've covered all the cases that we support but
 * we don't want them exposed in a real server.
 */

async fn demo_handler_args_0(_rqctx: Arc<RequestContext>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("demo_handler_args_0\n".into())?)
}

async fn demo_handler_args_1(_rqctx: Arc<RequestContext>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("demo_handler_args_1\n".into())?)
}

#[derive(Serialize, Deserialize)]
struct DemoQueryArgs {
    test1: String,
    test2: Option<u32>
}

async fn demo_handler_args_3query(
    _rqctx: Arc<RequestContext>,
    query: Query<DemoQueryArgs>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(serde_json::to_string(&query.into_inner()).unwrap().into())?)
}

#[derive(Serialize, Deserialize)]
struct DemoJsonBody {
    test1: String,
    test2: Option<u32>
}

async fn demo_handler_args_3json(
    _rqctx: Arc<RequestContext>,
    json: Json<DemoJsonBody>)
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(serde_json::to_string(&json.into_inner()).unwrap().into())?)
}

#[derive(Serialize)]
struct DemoJsonAndQuery {
    query: DemoQueryArgs,
    json: DemoJsonBody
}
async fn demo_handler_args_4(
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
        .status(StatusCode::OK)
        .body(serde_json::to_string(&combined).unwrap().into())?)
}
