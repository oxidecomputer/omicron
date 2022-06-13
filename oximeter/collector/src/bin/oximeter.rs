// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Main entry point to run an `oximeter` server in the control plane.
// Copyright 2021 Oxide Computer Company

use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use oximeter_collector::{oximeter_api, Config, Oximeter};
use std::path::PathBuf;

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
struct Args {
    #[clap(
        short = 'O',
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit"
    )]
    openapi: bool,

    /// Path to TOML file with configuration for the server
    #[clap(name = "CONFIG_FILE", parse(from_os_str))]
    config_file: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();
    let config = Config::from_file(args.config_file).unwrap();
    if args.openapi {
        run_openapi().map_err(CmdError::Failure)
    } else {
        Oximeter::new(&config)
            .await
            .unwrap()
            .serve_forever()
            .await
            .map_err(|e| CmdError::Failure(e.to_string()))
    }
}
