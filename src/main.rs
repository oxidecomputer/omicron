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

use oxide_api_prototype::ApiServerConfig;

#[tokio::main]
async fn main() {
    let cmd_args = std::env::args().collect::<Vec<String>>();

    if cmd_args.len() != 2 {
        eprintln!("usage: {} CONFIG_FILE_PATH", cmd_args[0]);
        eprintln!("See README.adoc for more information.");
        std::process::exit(2);
    }

    let config_file_path = std::path::Path::new(&cmd_args[1]);
    let config = match ApiServerConfig::from_file(config_file_path) {
        Ok(c) => c,
        Err(error) => {
            eprintln!("{}: {}", cmd_args[0], error);
            std::process::exit(1);
        }
    };

    let mut server = match oxide_api_prototype::ApiServer::new(&config) {
        Err(e) => {
            eprintln!("{}: {}", cmd_args[0], e);
            std::process::exit(1);
        }
        Ok(s) => s,
    };

    if let Err(error) = server.http_server.run().await {
        eprintln!("{}: {}", cmd_args[0], error);
        std::process::exit(1);
    }
}
