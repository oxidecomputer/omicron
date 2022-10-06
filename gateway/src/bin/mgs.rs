// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run gateway, the management gateway service

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
use omicron_gateway::{run_openapi, run_server, Config, MgsArguments};
use std::net::SocketAddrV6;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[clap(name = "gateway", about = "See README.adoc for more information")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    #[clap(help = "Print the external OpenAPI Spec document and exit", action)]
    Openapi,

    /// Start an MGS server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: PathBuf,

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
        Args::Run { config_file_path, id, address } => {
            let config = Config::from_file(&config_file_path).map_err(|e| {
                CmdError::Failure(format!(
                    "failed to parse {}: {}",
                    config_file_path.display(),
                    e
                ))
            })?;

            let args = MgsArguments { id, address };
            run_server(config, args).await.map_err(CmdError::Failure)
        }
    }
}
