/*!
 * Executable program to run the bootstrap agent
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use oxide_api_prototype::bootstrap_agent::Config;
use oxide_api_prototype::bootstrap_agent::Server;
use oxide_api_prototype::fatal;
use oxide_api_prototype::CmdError;
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "boostrap_agent",
    help = "See README.adoc for more information"
)]
struct Args {
    #[structopt(parse(try_from_str))]
    uuid: Uuid,

    #[structopt(parse(try_from_str))]
    bootstrap_agent_addr: SocketAddr,
}

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::from_args_safe().map_err(|err| {
        CmdError::Usage(format!("parsing arguments: {}", err.message))
    })?;

    let config = Config {
        id: args.uuid,
        dropshot: ConfigDropshot {
            bind_address: args.bootstrap_agent_addr,
            ..Default::default()
        },
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    };

    Server::start(&config)
        .await
        .map_err(CmdError::Failure)?
        .wait_for_finish()
        .await
        .map_err(CmdError::Failure)
}
