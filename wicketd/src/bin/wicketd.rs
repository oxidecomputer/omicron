// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable for wicketd: technician port based management service

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
use std::net::SocketAddrV6;
use std::path::PathBuf;
use wicketd::{self, run_openapi, run_server, Config};

#[derive(Debug, Parser)]
#[clap(name = "wicketd", about = "See README.adoc for more information")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    Openapi,

    /// Start a wicketd server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: PathBuf,

        #[clap(short, long, action)]
        address: SocketAddrV6,

        #[clap(long, action)]
        artifact_address: SocketAddrV6,
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
        Args::Run { config_file_path, address, artifact_address } => {
            let config = Config::from_file(&config_file_path).map_err(|e| {
                CmdError::Failure(format!(
                    "failed to parse {}: {}",
                    config_file_path.display(),
                    e
                ))
            })?;

            let args = wicketd::Args { address, artifact_address };
            run_server(config, args).await.map_err(CmdError::Failure)
        }
    }
}
