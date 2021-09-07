//! Executable program to run the sled agent

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::common::vlan::VlanID;
use omicron_sled_agent::config::Config;
use omicron_sled_agent::server::{run_openapi, run_server};
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled_agent",
    about = "See README.adoc for more information"
)]
enum Args {
    /// Generates the OpenAPI specification.
    OpenApi,
    /// Runs the Sled Agent server.
    Run {
        #[structopt(name = "SA_UUID", parse(try_from_str))]
        uuid: Uuid,

        #[structopt(name = "SA_IP:PORT", parse(try_from_str))]
        sled_agent_addr: SocketAddr,

        #[structopt(name = "NEXUS_IP:PORT", parse(try_from_str))]
        nexus_addr: SocketAddr,

        #[structopt(long = "vlan")]
        vlan: Option<VlanID>,
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
        Args::Run { uuid, sled_agent_addr, nexus_addr, vlan } => {
            let config = Config {
                id: uuid,
                nexus_address: nexus_addr,
                dropshot: ConfigDropshot {
                    bind_address: sled_agent_addr,
                    ..Default::default()
                },
                log: ConfigLogging::StderrTerminal {
                    level: ConfigLoggingLevel::Info,
                },
                vlan,
            };
            run_server(&config).await.map_err(CmdError::Failure)
        }
    }
}
