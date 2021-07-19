/*!
 * Executable program to run the bootstrap agent
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_bootstrap_agent::config::Config;
use omicron_bootstrap_agent::server::{run_openapi, Server};
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "boostrap_agent",
    about = "See README.adoc for more information"
)]
enum Args {
    /// Generates the OpenAPI specification
    OpenApi,
    /// Runs the Bootstrap Agent server.
    Run {
        #[structopt(parse(try_from_str))]
        uuid: Uuid,

        #[structopt(parse(try_from_str))]
        bootstrap_agent_addr: SocketAddr,
    },
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

    match args {
        Args::OpenApi => run_openapi().map_err(CmdError::Failure),
        Args::Run { uuid, bootstrap_agent_addr } => {
            let config = Config {
                id: uuid,
                dropshot: ConfigDropshot {
                    bind_address: bootstrap_agent_addr,
                    ..Default::default()
                },
                log: ConfigLogging::StderrTerminal {
                    level: ConfigLoggingLevel::Info,
                },
            };
            Server::start(&config)
                .await
                .map_err(CmdError::Failure)?
                .wait_for_finish()
                .await
                .map_err(CmdError::Failure)
        }
    }
}
