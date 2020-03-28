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

use clap::{App, Arg};
use oxide_api_prototype::ApiServerConfig;

#[tokio::main]
async fn main() {
    let matches = App::new("oxide-api-prototype")
        .after_help("See README.adoc for more information")
        .arg(
            Arg::with_name("openapi")
                .short("O")
                .long("openapi")
                .help("Print the OpenAPI Spec document and exit"),
        )
        .arg(Arg::with_name("CONFIG_FILE_PATH").required(true).index(1))
        .get_matches();

    let config_file = matches.value_of("CONFIG_FILE_PATH").unwrap();
    let config_file_path = std::path::Path::new(config_file);
    let config = match ApiServerConfig::from_file(config_file_path) {
        Ok(c) => c,
        Err(error) => {
            eprintln!("{}: {}", std::env::args().nth(0).unwrap(), error);
            std::process::exit(1);
        }
    };

    if matches.is_present("openapi") {
        oxide_api_prototype::run_openapi();
    } else {
        if let Err(error) = oxide_api_prototype::run_server(&config).await {
            eprintln!("{}: {}", std::env::args().nth(0).unwrap(), error);
            std::process::exit(1);
        }
    }
}
