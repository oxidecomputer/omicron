// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run the Omicron ClickHouse admin interface

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use omicron_clickhouse_admin::{Clickward, Config};
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use std::net::{SocketAddr, SocketAddrV6};

#[derive(Debug, Parser)]
#[clap(
    name = "clickhouse-admin",
    about = "Omicron ClickHouse cluster admin server"
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
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = main_impl().await {
        fatal(err);
    }
}

async fn main_impl() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Run { http_address, config } => {
            let mut config = Config::from_file(&config)
                .map_err(|err| CmdError::Failure(anyhow!(err)))?;
            config.dropshot.bind_address = SocketAddr::V6(http_address);

            let clickward = Clickward::new();

            let server =
                omicron_clickhouse_admin::start_server(clickward, config)
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
