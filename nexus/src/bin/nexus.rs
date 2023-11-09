// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run Nexus, the heart of the control plane

// TODO
// - Server hostname
// - Disable signals?
// - General networking and runtime tuning for availability and security: see
//   omicron#2184, omicron#2414.

use anyhow::anyhow;
use clap::Parser;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_nexus::run_openapi_external;
use omicron_nexus::run_openapi_internal;
use omicron_nexus::run_server;
use omicron_nexus::Config;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(name = "nexus", about = "See README.adoc for more information")]
struct Args {
    #[clap(
        short = 'O',
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit",
        action
    )]
    openapi: bool,

    #[clap(
        short = 'I',
        long = "openapi-internal",
        help = "Print the internal OpenAPI Spec document and exit",
        action
    )]
    openapi_internal: bool,

    #[clap(name = "CONFIG_FILE_PATH", action)]
    config_file_path: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(anyhow!(e)))?;

    if args.openapi {
        run_openapi_external().map_err(|err| CmdError::Failure(anyhow!(err)))
    } else if args.openapi_internal {
        run_openapi_internal().map_err(|err| CmdError::Failure(anyhow!(err)))
    } else {
        run_server(&config).await.map_err(|err| CmdError::Failure(anyhow!(err)))
    }
}
