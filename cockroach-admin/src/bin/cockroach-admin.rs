// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the Omicron CockroachDb admin interface (not to be
//! confused with CockroachDb's built-in HTTP API)

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_cockroach_admin::CockroachCli;
use omicron_cockroach_admin::Config;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use omicron_uuid_kinds::OmicronZoneUuid;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

#[derive(Debug, Parser)]
#[clap(name = "cockroach-admin", about = "Omicron CRDB cluster admin server")]
enum Args {
    /// Start the CRDB admin server
    Run {
        /// Path to the `cockroach` CLI
        #[clap(long, action)]
        path_to_cockroach_binary: Utf8PathBuf,

        /// Socket address for a running cockroach server instance
        #[clap(long, action)]
        cockroach_address: SocketAddrV6,

        /// Address on which this server should run
        #[clap(long, action)]
        http_address: SocketAddrV6,

        /// Path to the server config file
        #[clap(long, action)]
        config_file_path: Utf8PathBuf,

        /// ID of the zone within which we're running
        #[clap(long, action)]
        zone_id: OmicronZoneUuid,
    },
}

fn main() {
    if let Err(err) = oxide_tokio_rt::run(main_impl()) {
        fatal(err);
    }
}

async fn main_impl() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Run {
            path_to_cockroach_binary,
            cockroach_address,
            http_address,
            config_file_path,
            zone_id,
        } => {
            let cockroach_cli =
                CockroachCli::new(path_to_cockroach_binary, cockroach_address);
            let mut config = Config::from_file(&config_file_path)
                .map_err(|err| CmdError::Failure(anyhow!(err)))?;
            config.dropshot.bind_address = SocketAddr::V6(http_address);
            let server = omicron_cockroach_admin::start_server(
                zone_id,
                cockroach_cli,
                config,
            )
            .await
            .map_err(|err| CmdError::Failure(anyhow!(err)))?;
            server.await.map_err(|err| {
                CmdError::Failure(anyhow!(
                    "server failed after starting: {err}"
                ))
            })
        }
    }
}
