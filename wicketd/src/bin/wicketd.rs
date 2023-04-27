// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable for wicketd: technician port based management service

use clap::Parser;
use omicron_common::cmd::{fatal, CmdError};
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

        #[clap(flatten)]
        baseboard: Option<Baseboard>,
    },
}

// clap's support for `flatten` on an `Option` is a little wonky - we mark all
// of these fields as `required = false`, which has the effect of not requiring
// them _unless_ one of them is passed, in which case all of them are required.
#[derive(Debug, clap::Args)]
struct Baseboard {
    /// The identifier (serial number) of the Gimlet on which we're running
    #[clap(long, required = false)]
    baseboard_identifier: String,

    /// The model of the Gimlet on which we're running
    #[clap(long, required = false)]
    baseboard_model: String,

    /// The revision of the Gimlet on which we're running
    #[clap(long, required = false)]
    baseboard_revision: i64,
}

impl From<Baseboard> for sled_hardware::Baseboard {
    fn from(b: Baseboard) -> Self {
        Self::new(
            b.baseboard_identifier,
            b.baseboard_model,
            b.baseboard_revision,
        )
    }
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
            baseboard,
        } => {
            let baseboard = baseboard
                .map(sled_hardware::Baseboard::from)
                .unwrap_or_else(sled_hardware::Baseboard::unknown);

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
