/*!
 * Executable program to run a simulated sled agent
 */

/*
 * TODO see the TODO for oxide-controller.
 */

use clap::{App, Arg};
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use oxide_api_prototype::fatal;
use oxide_api_prototype::sa_run_server;
use oxide_api_prototype::CmdError;
use oxide_api_prototype::ConfigSledAgent;
use oxide_api_prototype::SimMode;
use std::net::SocketAddr;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let matches = App::new("sled_agent")
        .after_help("See README.adoc for more information")
        .arg(
            Arg::with_name("sim-mode")
                .long("sim-mode")
                .takes_value(true)
                .help("automatically simulate transitions")
                .possible_value("auto")
                .possible_value("explicit")
                .default_value("auto"),
        )
        .arg(Arg::with_name("SA_UUID").required(true).index(1))
        .arg(Arg::with_name("SA_IP:PORT").required(true).index(2))
        .arg(Arg::with_name("CONTROLLER_IP:PORT").required(true).index(3))
        .get_matches_safe()
        .map_err(|clap_error| {
            CmdError::Usage(format!(
                "parsing arguments: {}",
                clap_error.message
            ))
        })?;

    let sa_id = {
        let value_str = matches.value_of("SA_UUID").unwrap();
        Uuid::parse_str(value_str)
            .map_err(|e| CmdError::Usage(format!("parsing SA_UUID: {}", e)))?
    };

    let sa_addr = {
        let value_str = matches.value_of("SA_IP:PORT").unwrap();
        value_str.parse::<SocketAddr>().map_err(|e| {
            CmdError::Usage(format!("parsing SA_IP:PORT: {}", e))
        })?
    };

    let controller_addr = {
        let value_str = matches.value_of("CONTROLLER_IP:PORT").unwrap();
        value_str.parse::<SocketAddr>().map_err(|e| {
            CmdError::Usage(format!("parsing CONTROLLER_IP:PORT: {}", e))
        })?
    };

    let sim_mode = match matches.value_of("sim-mode").unwrap() {
        "auto" => SimMode::Auto,
        mode => {
            assert_eq!(mode, "explicit");
            SimMode::Explicit
        }
    };

    let config = ConfigSledAgent {
        id: sa_id,
        sim_mode,
        controller_address: controller_addr,
        dropshot: ConfigDropshot {
            bind_address: sa_addr,
            ..Default::default()
        },
        log: ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Info,
        },
    };

    sa_run_server(&config).await.map_err(CmdError::Failure)
}
