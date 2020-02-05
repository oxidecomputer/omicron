/*!
 * main.rs: API server process
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

#[actix_rt::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let server = actix_web::HttpServer::new(|| {
        actix_web::App::new()
            .configure(api::register_actix_api)
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
