// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Main entry point to run an `oximeter` server in the control plane.
// Copyright 2021 Oxide Computer Company

use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use oximeter_collector::{oximeter_api, Config, Oximeter, OximeterArguments};
use std::net::SocketAddrV6;
use std::path::PathBuf;
use uuid::Uuid;

pub fn run_openapi() -> Result<(), String> {
    oximeter_api()
        .openapi("Oxide Oximeter API", "0.0.1")
        .description("API for interacting with oximeter")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())
        .map_err(|e| e.to_string())
}

/// Run an oximeter metric collection server in the Oxide Control Plane.
#[derive(Parser)]
#[clap(name = "oximeter", about = "See README.adoc for more information")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    Openapi,

    /// Start an Oximeter server
    Run {
        /// Path to TOML file with configuration for the server
        #[clap(name = "CONFIG_FILE", action)]
        config_file: PathBuf,

        #[clap(short, long, action)]
        id: Uuid,

        #[clap(short, long, action)]
        address: SocketAddrV6,
    },
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();
    match args {
        Args::Openapi => run_openapi().map_err(CmdError::Failure),
        Args::Run { config_file, id, address } => {
            let config = Config::from_file(config_file).unwrap();
            let args = OximeterArguments { id, address };
            Oximeter::new(&config, &args)
                .await
                .unwrap()
                .serve_forever()
                .await
                .map_err(|e| CmdError::Failure(e.to_string()))
        }
    }
}
