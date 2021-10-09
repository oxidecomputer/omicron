//! Executable program to run the sled agent

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::external::Error;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::bootstrap::{
    config::Config as BootstrapConfig, server as bootstrap_server,
};
use omicron_sled_agent::{
    common::vlan::VlanID, config::Config as SledConfig, server as sled_server,
};
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug)]
enum ApiRequest {
    Bootstrap,
    Sled,
}

impl std::str::FromStr for ApiRequest {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "bootstrap" => Ok(ApiRequest::Bootstrap),
            "sled" => Ok(ApiRequest::Sled),
            _ => Err(Error::InvalidValue {
                label: s.to_string(),
                message: "Invalid value: try one of {bootstrap, sled}"
                    .to_string(),
            }),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sled_agent",
    about = "See README.adoc for more information"
)]
enum Args {
    /// Generates the OpenAPI specification.
    Openapi {
        #[structopt(name = "api_type", parse(try_from_str))]
        api_requested: ApiRequest,
    },
    /// Runs the Sled Agent server.
    Run {
        #[structopt(name = "SA_UUID", parse(try_from_str))]
        uuid: Uuid,

        #[structopt(name = "BA_IP:PORT", parse(try_from_str))]
        bootstrap_agent_addr: SocketAddr,

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
        Args::Openapi { api_requested } => match api_requested {
            ApiRequest::Bootstrap => {
                bootstrap_server::run_openapi().map_err(CmdError::Failure)
            }
            ApiRequest::Sled => {
                sled_server::run_openapi().map_err(CmdError::Failure)
            }
        },
        Args::Run {
            uuid,
            bootstrap_agent_addr,
            sled_agent_addr,
            nexus_addr,
            vlan,
        } => {
            // Configure and run the Bootstrap server.
            let config = BootstrapConfig {
                id: uuid,
                dropshot: ConfigDropshot {
                    bind_address: bootstrap_agent_addr,
                    ..Default::default()
                },
                log: ConfigLogging::StderrTerminal {
                    level: ConfigLoggingLevel::Info,
                },
            };
            let boot_server = bootstrap_server::Server::start(&config)
                .await
                .map_err(CmdError::Failure)?;

            // Configure and run the Sled server.
            let config = SledConfig {
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

            let sled_server = sled_server::Server::start(&config)
                .await
                .map_err(CmdError::Failure)?;

            tokio::select! {
                _ = boot_server.wait_for_finish() => {
                    eprintln!("Boot server exited unexpectedly");
                },
                _ = sled_server.wait_for_finish() => {
                    eprintln!("Sled server exited unexpectedly");
                },
            }
            Ok(())
        }
    }
}
