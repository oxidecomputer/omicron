//! Main entry point to run an `oximeter` server in the control plane.
// Copyright 2021 Oxide Computer Company

use std::net::SocketAddr;

use dropshot::{ConfigDropshot, ConfigLogging, ConfigLoggingLevel};
use structopt::StructOpt;

use oximeter::{oximeter_server::Config, Oximeter};

/// Run an oximeter metric collection server in the Oxide Control Plane.
#[derive(StructOpt)]
struct Args {
    /// Address on which to serve internal API request
    #[structopt(short, long, default_value = "[::1]:12223")]
    address: SocketAddr,

    /// Address at which to connect to Nexus
    #[structopt(short, long, default_value = "127.0.0.1:12221")]
    nexus: SocketAddr,

    /// Address at which to connect to ClickHouse
    #[structopt(short, long, default_value = "[::1]:9000")]
    _clickhouse: SocketAddr,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    let config = Config {
        nexus_address: args.nexus,
        dropshot: ConfigDropshot {
            bind_address: args.address,
            ..Default::default()
        },
        log: ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    };
    Oximeter::new(&config).await.unwrap().serve_forever().await.unwrap();
}
