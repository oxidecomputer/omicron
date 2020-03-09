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

use std::net::SocketAddr;

/** TCP IP address and port on which to bind */
const SERVER_BIND_ADDRESS: &str = "127.0.0.1:12220";

#[tokio::main]
async fn main()
{
    let bind_addr: SocketAddr = SERVER_BIND_ADDRESS.parse().unwrap();
    let mut server = 
        match oxide_api_prototype::ApiServer::new(&bind_addr) {
            Err(e) => {
                eprintln!("failed to set up server: {}", e);
                std::process::exit(1);
            },
            Ok(s) => s
        };
    eprintln!("listening: http://{}", server.local_addr());
    if let Err(error) = server.http_server.run().await {
        eprintln!("server failed: {}", error);
        std::process::exit(1);
    }
}
