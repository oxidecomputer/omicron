// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable for wicketd: technician port based management service

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
use sled_hardware::Baseboard;
use std::net::SocketAddrV6;
use std::path::PathBuf;
use wicketd::{self, run_openapi, Config, Server};

#[derive(Debug, Parser)]
#[clap(name = "wicketd", about = "See README.adoc for more information")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    Openapi,

    /// Start a wicketd server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: PathBuf,

        /// The address for the technician port
        #[clap(short, long, action)]
        address: SocketAddrV6,

        /// The address on the bootstrap network to serve artifacts at
        #[clap(long, action)]
        artifact_address: SocketAddrV6,

        /// The port on localhost for MGS
        #[clap(long, action)]
        mgs_address: SocketAddrV6,

        #[clap(long)]
        baseboard_file: Option<PathBuf>,
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
        Args::Run {
            config_file_path,
            address,
            artifact_address,
            mgs_address,
            baseboard_file,
        } => {
            let baseboard = if let Some(baseboard_file) = baseboard_file {
                let baseboard_file =
                    std::fs::read_to_string(&baseboard_file)
                        .map_err(|e| CmdError::Failure(e.to_string()))?;
                let baseboard: Baseboard =
                    serde_json::from_str(&baseboard_file)
                        .map_err(|e| CmdError::Failure(e.to_string()))?;

                // TODO-correctness `Baseboard::unknown()` is slated for removal
                // after some refactoring in sled-agent, at which point we'll need a
                // different way for sled-agent to tell us it doesn't know our
                // baseboard.
                if matches!(baseboard, Baseboard::Unknown) {
                    None
                } else {
                    Some(baseboard)
                }
            } else {
                None
            };

            let config = Config::from_file(&config_file_path).map_err(|e| {
                CmdError::Failure(format!(
                    "failed to parse {}: {}",
                    config_file_path.display(),
                    e
                ))
            })?;

            let args = wicketd::Args {
                address,
                artifact_address,
                mgs_address,
                baseboard,
            };
            let log = config.log.to_logger("wicketd").map_err(|msg| {
                CmdError::Failure(format!("initializing logger: {}", msg))
            })?;
            let server =
                Server::start(log, args).await.map_err(CmdError::Failure)?;
            server.wait_for_finish().await.map_err(CmdError::Failure)
        }
    }
}
