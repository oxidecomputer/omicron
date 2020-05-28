/*!
 * Simulated server controller
 */

/*
 * TODO see the TODO for oxide-controller.
 */

use std::process::exit;

use clap::{App, Arg};
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use oxide_api_prototype::run_server_controller_api_server;
use oxide_api_prototype::ConfigServerController;
use oxide_api_prototype::SimMode;
use std::net::SocketAddr;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), String> {
    let matches = App::new("server_controller")
        .after_help("See README.adoc for more information")
        .arg(Arg::with_name("SC_UUID").required(true).index(1))
        .arg(Arg::with_name("SC_IP:PORT").required(true).index(2))
        .arg(Arg::with_name("CONTROLLER_IP:PORT").required(true).index(3))
        .get_matches_safe()
        .map_err(|clap_error| {
            format!("parsing arguments: {}", clap_error.message)
        })?;

    let sc_id = {
        let value_str = matches.value_of("SC_UUID").unwrap();
        Uuid::parse_str(value_str)
            .map_err(|e| format!("parsing SC_UUID: {}", e))?
    };

    let sc_addr = {
        let value_str = matches.value_of("SC_IP:PORT").unwrap();
        value_str
            .parse::<SocketAddr>()
            .map_err(|e| format!("parsing SC_IP:PORT: {}", e))?
    };

    let controller_addr = {
        let value_str = matches.value_of("CONTROLLER_IP:PORT").unwrap();
        value_str
            .parse::<SocketAddr>()
            .map_err(|e| format!("parsing CONTROLLER_IP:PORT: {}", e))?
    };

    let config = ConfigServerController {
        id: sc_id,
        sim_mode: SimMode::Auto,
        controller_address: controller_addr,
        dropshot: ConfigDropshot {
            bind_address: sc_addr,
        },
        log: ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Debug,
        },
    };

    run_server_controller_api_server(config).await
}

fn fatal(message: String) {
    eprintln!("{}: {}", std::env::args().nth(0).unwrap(), message);
    exit(1);
}
