// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// See RFD 248
// See https://github.com/oxidecomputer/omicron/issues/718
//
// Milestones:
// - Dropshot server
// - Sqlite task
// - DNS task

use anyhow::anyhow;
use anyhow::Context;
use clap::Parser;
use std::net::{SocketAddr, SocketAddrV6};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, action)]
    config_file: PathBuf,

    #[clap(long, action)]
    server_address: SocketAddrV6,

    #[clap(long, action)]
    dns_address: SocketAddrV6,

    #[clap(long, action)]
    dns_zone: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let config_file = &args.config_file;
    let config_file_contents = std::fs::read_to_string(config_file)
        .with_context(|| format!("read config file {:?}", config_file))?;
    let mut config: dns_server::Config =
        toml::from_str(&config_file_contents)
            .with_context(|| format!("parse config file {:?}", config_file))?;

    config.dropshot.bind_address = SocketAddr::V6(args.server_address);
    eprintln!("{:?}", config);

    let log = config
        .log
        .to_logger("internal-dns")
        .context("failed to create logger")?;

    let (_dns_server, dropshot_server) = dns_server::start(
        log,
        config,
        args.dns_zone,
        args.dns_address.into(),
    )
    .await?;

    dropshot_server
        .await
        .map_err(|error_message| anyhow!("server exiting: {}", error_message))
}
