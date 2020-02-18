/*!
 * API server process
 */

/*
 * TODO
 * - TCP and HTTP KeepAlive parameters
 * - Server hostname
 * - Disable signals?
 * - Analogs for actix client_timeout (request timeout), client_shutdown (client
 *   shutdown timeout), server backlog, number of workers, max connections per
 *   worker, max connect-in-progress sockets, shutdown_timeout (server shutdown
 *   timeout)
 * - Move even more of the server setup into api_server.rs
 */

mod api_error;
mod api_http_entrypoints;
mod api_http_util;
mod api_model;
mod api_server;
mod sim;

use std::net::SocketAddr;

/** TCP IP address and port on which to bind */
const SERVER_BIND_ADDRESS: &str = "127.0.0.1:12220";

#[tokio::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let app_state = api_server::setup_server_state();
    let make_service = api_server::server_handler(app_state);
    let bind_addr: SocketAddr = SERVER_BIND_ADDRESS.parse().unwrap();
    let builder = hyper::Server::bind(&bind_addr);
    let server = builder.serve(make_service);
    eprintln!("listening: http://{}", server.local_addr());
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
    return Ok(())
}
