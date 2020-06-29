/*!
 * Executable program to run the Oxide Controller (OXC)
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
 */

use clap::{App, Arg};
use oxide_api_prototype::controller_run_openapi_external;
use oxide_api_prototype::controller_run_server;
use oxide_api_prototype::fatal;
use oxide_api_prototype::CmdError;
use oxide_api_prototype::ConfigController;
use std::path::Path;

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let matches = App::new("oxide-api-prototype")
        .after_help("See README.adoc for more information")
        .arg(
            Arg::with_name("openapi")
                .short("O")
                .long("openapi")
                .help("Print the OpenAPI Spec document and exit"),
        )
        .arg(Arg::with_name("CONFIG_FILE_PATH").required(true).index(1))
        .get_matches_safe()
        .map_err(|clap_error| {
            CmdError::Usage(format!(
                "parsing arguments: {}",
                clap_error.message
            ))
        })?;

    let config_file = matches.value_of("CONFIG_FILE_PATH").unwrap();
    let config_file_path = Path::new(config_file);
    let config = ConfigController::from_file(config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    if matches.is_present("openapi") {
        controller_run_openapi_external();
        Ok(())
    } else {
        controller_run_server(&config).await.map_err(CmdError::Failure)
    }
}
