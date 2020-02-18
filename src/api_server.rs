/*!
 * server-wide state and facilities
 */

use crate::api_model;
use crate::sim;
use sim::SimulatorBuilder;

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
    pub backend: Arc<dyn api_model::ApiBackend>
}

/**
 * Set up initial server-wide shared state.
 * TODO too many Arcs?
 */
pub fn setup_server_state()
    -> Arc<ApiServerState>
{
    let mut simbuilder = SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    Arc::new(ApiServerState {
        backend: Arc::new(simbuilder.build())
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
async fn http_request_handle(_server: Arc<ApiServerState>, _req: Request<Body>)
    -> Result<Response<Body>, GenericError>
{
    Ok(Response::new("Hello\n".into()))
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
 * incoming requests to `http_request_handle()`, including as an argument the
 * backend server state object.  We could use `service_fn` here using a closure
 * to capture the server state object, but the resulting code is a bit simpler
 * without all that.
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
        Box::pin(http_request_handle(Arc::clone(&self.server), req))
    }
}
