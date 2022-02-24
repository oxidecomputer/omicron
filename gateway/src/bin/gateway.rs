// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//!
//! Executable program to run gateway, the management gateway service
//!

use omicron_common::cmd::{fatal, CmdError};
use omicron_gateway::{run_openapi, run_server, Config};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "gateway", about = "See README.adoc for more information")]
struct Args {
    #[structopt(
        short = "O",
        long = "openapi",
        help = "Print the external OpenAPI Spec document and exit"
    )]
    openapi: bool,

    #[structopt(name = "CONFIG_FILE_PATH", parse(from_os_str))]
    config_file_path: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(cmd_error) = do_run().await {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::from_args_safe().map_err(|err| {
        CmdError::Usage(format!("parsing arguments: {}", err.message))
    })?;

    let config = Config::from_file(args.config_file_path)
        .map_err(|e| CmdError::Failure(e.to_string()))?;

    if args.openapi {
        run_openapi().map_err(CmdError::Failure)
    } else {
        run_server(&config).await.map_err(CmdError::Failure)
    }
}
