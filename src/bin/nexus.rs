/*!
 * Executable program to run the Oxide Nexus.
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

use oxide_api_prototype::cmd::fatal;
use oxide_api_prototype::cmd::CmdError;
use oxide_api_prototype::nexus::run_openapi_external;
use oxide_api_prototype::nexus::run_server;
use oxide_api_prototype::nexus::Config;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "nexus",
    about = "See README.adoc for more information"
)]
struct Args {
    #[structopt(
        short = "O",
        long = "openapi",
        help = "Print the OpenAPI Spec document and exit"
    )]
    openapi: bool,

    #[structopt(name = "CONFIG_FILE_PATH", parse(from_os_str))]
    config_file_path: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::from_args_safe().map_err(|err| {
        CmdError::Usage(format!("parsing arguments: {}", err.message))
    })?;

    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    if args.openapi {
        run_openapi_external().map_err(CmdError::Failure)
    } else {
        run_server(&config).await.map_err(CmdError::Failure)
    }
}
