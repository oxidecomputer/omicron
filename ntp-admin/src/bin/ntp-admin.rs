// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the Omicron NTP admin interface

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use omicron_ntp_admin::Config;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

#[derive(Debug, Parser)]
#[clap(name = "ntp-admin", about = "Omicron NTP admin server")]
enum Args {
    /// Start the NTP admin server
    Run {
        /// Address on which this server should run
        #[clap(long, action)]
        address: SocketAddrV6,

        /// Path to the server config file
        #[clap(long, action)]
        config_file_path: Utf8PathBuf,
    },
}

fn main() {
    let mut builder = oxide_tokio_rt::Builder::new_multi_thread();
    builder.worker_threads(8);
    if let Err(err) = oxide_tokio_rt::run_builder(&mut builder, main_impl()) {
        fatal(err);
    }
}

async fn main_impl() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Run { address, config_file_path } => {
            let mut config = Config::from_file(&config_file_path)
                .map_err(|err| CmdError::Failure(anyhow!(err)))?;
            config.dropshot.bind_address = SocketAddr::V6(address);
            let server = omicron_ntp_admin::start_server(config)
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
