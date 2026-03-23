// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the Omicron ClickHouse admin interface for
//! clickhouse keepers.

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_clickhouse_admin::Config;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use std::net::{SocketAddr, SocketAddrV6};

#[derive(Debug, Parser)]
#[clap(
    name = "clickhouse-admin-keeper",
    about = "Omicron ClickHouse cluster admin server for keepers"
)]
enum Args {
    /// Start the ClickHouse admin server
    Run {
        /// Address on which this server should run
        #[clap(long, short = 'a', action)]
        http_address: SocketAddrV6,

        /// Path to the server configuration file
        #[clap(long, short, action)]
        config: Utf8PathBuf,

        /// Address on which the clickhouse server or keeper is listening on
        #[clap(long, short = 'l', action)]
        listen_address: SocketAddrV6,

        /// Path to the clickhouse binary
        #[clap(long, short, action)]
        binary_path: Utf8PathBuf,
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
        Args::Run { http_address, config, listen_address, binary_path } => {
            let mut config = Config::from_file(&config)
                .map_err(|err| CmdError::Failure(anyhow!(err)))?;
            config.dropshot.bind_address = SocketAddr::V6(http_address);
            let server = omicron_clickhouse_admin::start_keeper_admin_server(
                binary_path,
                listen_address,
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
