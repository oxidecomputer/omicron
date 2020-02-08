/*!
 * API server process
 */

use actix_web::App;
use actix_web::HttpServer;

mod api_error;
mod api_http_entrypoints;
mod api_http_util;
mod api_model;
mod api_server;
mod sim;

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

#[actix_rt::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    /*
     * TODO it's not clear this is correct because we're not wrapping it in an
     * actix_web::Data instance.  It may be that we get a new app_state for each
     * thread.  However, wrapping it does not work at all.  It may be time to
     * move past Actix...
     */
    let app_state = api_server::setup_server_state();

    let server = HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .configure(api_http_entrypoints::register_api_entrypoints)
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
