/*!
 * Executable program to run a simulated sled agent
 */

/*
 * TODO see the TODO for nexus.
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron::cmd::fatal;
use omicron::cmd::CmdError;
use omicron::sled_agent::run_server;
use omicron::sled_agent::Config;
use omicron::sled_agent::SimMode;
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

fn parse_sim_mode(src: &str) -> Result<SimMode, String> {
    match src {
        "auto" => Ok(SimMode::Auto),
        "explicit" => Ok(SimMode::Explicit),
        mode => Err(format!("Invalid sim mode: {}", mode)),
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled_agent",
    about = "See README.adoc for more information"
)]
struct Args {
    #[structopt(
        long = "sim-mode",
        parse(try_from_str = parse_sim_mode),
        default_value = "auto",
        help = "Automatically simulate transitions",
    )]
    sim_mode: SimMode,

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
        sim_mode: args.sim_mode,
        nexus_address: args.nexus_addr,
        dropshot: ConfigDropshot {
            bind_address: args.sled_agent_addr,
            ..Default::default()
        },
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    };

    run_server(&config).await.map_err(CmdError::Failure)
}
