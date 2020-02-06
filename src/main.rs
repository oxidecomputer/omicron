/*!
 * API server process
 */

/*
 * TODO Figure out appropriate TCP and HTTP keepalive parameters
 * TODO Set hostname
 * TODO Disable signals?
 * TODO Most of this could move to a library function, with the executable
 * itself only being responsible for things like command-line arguments.
 */

/** number of worker threads to start */
const SERVER_NWORKERS: usize = 4;
/** server socket's listen backlog */
const SERVER_BACKLOG: i32 = 64;

/** See actix web server client_timeout() */
const SERVER_CLIENT_REQUEST_TIMEOUT_MS: u64 = 5000;

/** See actix web server client_shutdown() */
const SERVER_CLIENT_SHUTDOWN_TIMEOUT_MS: u64 = 5000;

/** See actix web server shutdown_timeout() */
const SERVER_WORKER_SHUTDOWN_TIMEOUT_S: u64 = 10;

/** maximum number of connections to be handled per worker */
const SERVER_WORKER_MAX_CONN: usize = 64;
/** maximum number of not-yet-established connections per worker */
const SERVER_WORKER_MAX_CONN_CONNECTING: usize = 64;

/** TCP IP address and port on which to bind */
const SERVER_BIND_ADDRESS: &str = "127.0.0.1:12220";

mod api;
mod api_http;
mod api_error;
mod api_model;
mod sim;

#[actix_rt::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let app_state = setup_server_state();

    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .configure(api::register_actix_api)
            .app_data(app_state.clone())
    })
        .workers(SERVER_NWORKERS)
        .maxconn(SERVER_WORKER_MAX_CONN)
        .maxconnrate(SERVER_WORKER_MAX_CONN_CONNECTING)
        .backlog(SERVER_BACKLOG)
        .client_timeout(SERVER_CLIENT_REQUEST_TIMEOUT_MS)
        .client_shutdown(SERVER_CLIENT_SHUTDOWN_TIMEOUT_MS)
        .shutdown_timeout(SERVER_WORKER_SHUTDOWN_TIMEOUT_S)
        .bind(SERVER_BIND_ADDRESS)?;

    for (addr, scheme) in server.addrs_with_scheme() {
        eprintln!("listening: {}://{}", scheme, addr);
    }

    server.run().await?;

    return Ok(())
}

/**
 * Set up initial server-wide shared state.
 */
fn setup_server_state()
    -> actix_web::web::Data<api::ApiServerState>
{
    let mut simulator = sim::Simulator::new();
    simulator.project_create("simproject1");
    simulator.project_create("simproject2");
    simulator.project_create("simproject3");

    /*
     * The use of Box and particularly Box::leak() here is worth explaining.
     * We'd like to use Actix's facilities for streaming HTTP responses out to
     * end users.  In order to do that, when we send a futures Stream
     * representing the response body, it must have lifetime 'static.  (See
     * actix_web::dev::HttpResponseBuilder::streaming().)  Actix needs to be
     * able to use that object well after our (async) handler function has
     * completed.
     *
     * In practice, this Stream's lifetime will be limited by the lifetime of
     * the backend that created it.  In the case of the Simulator backend: the
     * stream cannot live after the simulator (whose data is being emitted from
     * the stream) has itself been dropped.
     *
     * To resolve this, we must elevate the lifetime of the Simulator to
     * 'static.  We do this by moving it to the heap and then deliberately
     * leaking the Box.
     */
    let simbox = Box::new(simulator);
    let backend = Box::leak(simbox);

    actix_web::web::Data::new(api::ApiServerState {
        backend: backend
    })
}
