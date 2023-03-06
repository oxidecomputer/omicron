// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap documentation

use anyhow::anyhow;
use anyhow::Context;
use clap::Parser;
use slog::info;
use std::net::{SocketAddr, SocketAddrV6};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, action)]
    config_file: PathBuf,

    #[clap(long, action)]
    http_address: SocketAddrV6,

    #[clap(long, action)]
    dns_address: SocketAddrV6,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let config_file = &args.config_file;
    let config_file_contents = std::fs::read_to_string(config_file)
        .with_context(|| format!("read config file {:?}", config_file))?;
    let mut config: dns_server::Config = toml::from_str(&config_file_contents)
        .with_context(|| format!("parse config file {:?}", config_file))?;

    // XXX-dap do not override dropshot bind_address
    config.dropshot.bind_address = SocketAddr::V6(args.http_address);
    eprintln!("{:?}", config);

    let log = config
        .log
        .to_logger("dns-server")
        .context("failed to create logger")?;
    info!(&log, "config"; "config" => ?config);

    // XXX-dap do not override dns_address
    let (_dns_server, dropshot_server) =
        dns_server::start(log, config, args.dns_address.into())
            .await?;

    dropshot_server
        .await
        .map_err(|error_message| anyhow!("server exiting: {}", error_message))
}
