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
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

#[derive(Debug, Parser)]
#[clap(
    name = "clickhouse-admin",
    about = "Omicron ClickHouse cluster admin server"
)]
enum Args {
    /// Start the ClickHouse admin server
    Run {
        // TODO: take the clickhouse address as an argument
        /// Address on which this server should run
        #[clap(long, short = 'H', action)]
        http_address: SocketAddrV6,

        /// Path to the server configuration file
        #[clap(long, short, action)]
        config: Utf8PathBuf,
    },
}

// TODO: Remove this comment and move config file to smf/clickhouse-admin
// Test with cargo run --bin=clickhouse-admin -- run -H [::1]:8888 -c ./clickhouse-admin/dummy-config.toml

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

            // TODO: Change for the actual clickhouse address
            let dummy_address =
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0);
            let clickward = Clickward::new(dummy_address);

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
