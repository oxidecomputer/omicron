// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable for wicketd: technician port based management service

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_common::{
    address::Ipv6Subnet,
    cmd::{CmdError, fatal},
};
use sled_hardware_types::Baseboard;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::PathBuf;
use wicketd::{Config, Server, SmfConfigValues};

#[derive(Debug, Parser)]
#[clap(name = "wicketd", about = "See README.adoc for more information")]
enum Args {
    /// Start a wicketd server
    Run {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: Utf8PathBuf,

        /// The address on which the main wicketd dropshot server should listen
        #[clap(short, long, action)]
        address: SocketAddrV6,

        /// The address on the bootstrap network to serve artifacts at
        #[clap(long, action)]
        artifact_address: SocketAddrV6,

        /// The address (expected to be on localhost) for MGS
        #[clap(long, action)]
        mgs_address: SocketAddrV6,

        /// The address (expected to be on localhost) on which we'll serve a TCP
        /// proxy to Nexus's "techport external" API
        #[clap(long, action)]
        nexus_proxy_address: SocketAddrV6,

        /// Path to a file containing our baseboard information
        #[clap(long)]
        baseboard_file: Option<PathBuf>,

        /// Read dynamic properties from our SMF config instead of passing them
        /// on the command line
        #[clap(long)]
        read_smf_config: bool,

        /// The subnet for the rack; typically read directly from our SMF config
        /// via `--read-smf-config` or an SMF refresh
        #[clap(long, action, conflicts_with("read_smf_config"))]
        rack_subnet: Option<Ipv6Addr>,
    },

    /// Instruct a running wicketd server to refresh its config
    ///
    /// Mechanically, this hits a specific endpoint served by wicketd's dropshot
    /// server
    RefreshConfig {
        #[clap(name = "CONFIG_FILE_PATH", action)]
        config_file_path: Utf8PathBuf,

        /// The address of the server to refresh
        #[clap(short, long, action)]
        address: SocketAddrV6,
    },
}

fn main() {
    if let Err(cmd_error) = omicron_runtime::run(do_run()) {
        fatal(cmd_error);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Run {
            config_file_path,
            address,
            artifact_address,
            mgs_address,
            nexus_proxy_address,
            baseboard_file,
            read_smf_config,
            rack_subnet,
        } => {
            let baseboard = if let Some(baseboard_file) = baseboard_file {
                let baseboard_file = std::fs::read_to_string(baseboard_file)
                    .map_err(|e| CmdError::Failure(anyhow!(e)))?;
                let baseboard: Baseboard =
                    serde_json::from_str(&baseboard_file)
                        .map_err(|e| CmdError::Failure(anyhow!(e)))?;

                // TODO-correctness `Baseboard::unknown()` is slated for removal
                // after some refactoring in sled-agent, at which point we'll
                // need a different way for sled-agent to tell us it doesn't
                // know our baseboard.
                if matches!(baseboard, Baseboard::Unknown) {
                    None
                } else {
                    Some(baseboard)
                }
            } else {
                None
            };

            let config = Config::from_file(&config_file_path)
                .with_context(|| format!("failed to parse {config_file_path}"))
                .map_err(CmdError::Failure)?;

            let rack_subnet = match rack_subnet {
                Some(addr) => Some(Ipv6Subnet::new(addr)),
                None if read_smf_config => {
                    let smf_values = SmfConfigValues::read_current()
                        .map_err(CmdError::Failure)?;
                    smf_values.rack_subnet
                }
                None => None,
            };

            let args = wicketd::Args {
                address,
                artifact_address,
                mgs_address,
                nexus_proxy_address,
                baseboard,
                rack_subnet,
            };
            let log = config
                .log
                .to_logger("wicketd")
                .context("failed to initialize logger")
                .map_err(CmdError::Failure)?;
            let server =
                Server::start(log, args).await.map_err(CmdError::Failure)?;
            server
                .wait_for_finish()
                .await
                .map_err(|err| CmdError::Failure(anyhow!(err)))
        }
        Args::RefreshConfig { config_file_path, address } => {
            let config = Config::from_file(&config_file_path)
                .with_context(|| format!("failed to parse {config_file_path}"))
                .map_err(CmdError::Failure)?;

            let log = config
                .log
                .to_logger("wicketd")
                .context("failed to initialize logger")
                .map_err(CmdError::Failure)?;

            // When run via `svcadm refresh ...`, we need to respect the special
            // [SMF exit codes](https://illumos.org/man/7/smf_method). Returning
            // an error from main exits with code 1 (from libc::EXIT_FAILURE),
            // which does not collide with any special SMF codes.
            Server::refresh_config(log, address)
                .await
                .map_err(CmdError::Failure)
        }
    }
}
