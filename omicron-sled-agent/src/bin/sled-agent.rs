//! Executable program to run the sled agent

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::config::Config;
use omicron_sled_agent::server::run_server;
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled_agent",
    about = "See README.adoc for more information"
)]
struct Args {
    #[structopt(name = "SA_UUID", parse(try_from_str))]
    uuid: Uuid,

    #[structopt(name = "SA_IP:PORT", parse(try_from_str))]
    sled_agent_addr: SocketAddr,

    #[structopt(name = "NEXUS_IP:PORT", parse(try_from_str))]
    nexus_addr: SocketAddr,
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
        nexus_address: args.nexus_addr,
        dropshot: ConfigDropshot {
            bind_address: args.sled_agent_addr,
            ..Default::default()
        },
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    };

    run_server(&config).await.map_err(CmdError::Failure)
}
