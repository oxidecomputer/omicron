// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run gateway, the management gateway service

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
use omicron_gateway::{run_openapi, run_server, Config};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(name = "gateway", about = "See README.adoc for more information")]
struct Args {
    #[clap(
        short = 'O',
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit",
        action
    )]
    openapi: bool,

    #[clap(
        name = "CONFIG_FILE_PATH",
        action,
        required_unless_present = "openapi"
    )]
    config_file_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    if args.openapi {
        run_openapi().map_err(CmdError::Failure)
    } else {
        // `.unwrap()` here is fine because our clap config requires
        // `config_file_path` to be passed if `openapi` is not.
        let config = Config::from_file(args.config_file_path.unwrap())
            .map_err(|e| CmdError::Failure(e.to_string()))?;

        run_server(config).await.map_err(CmdError::Failure)
    }
}
