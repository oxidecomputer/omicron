//! Main entry point to run an `oximeter` server in the control plane.
// Copyright 2021 Oxide Computer Company

use structopt::StructOpt;

use oximeter::{oximeter_server::Config, Oximeter};

/// Run an oximeter metric collection server in the Oxide Control Plane.
#[derive(StructOpt)]
struct Args {
    /// Path to TOML file with configuration for the server
    config_file: String,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    let config = Config::from_file(args.config_file).unwrap();
    Oximeter::new(&config).await.unwrap().serve_forever().await.unwrap();
}
