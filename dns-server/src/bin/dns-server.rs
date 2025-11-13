// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable that starts the HTTP-configurable DNS server used for both
//! internal DNS (RFD 248) and external DNS (RFD 357) for the Oxide system

use anyhow::Context;
use anyhow::anyhow;
use clap::Parser;
use serde::Deserialize;
use slog::info;
use slog::o;
use std::net::{SocketAddr, SocketAddrV6};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, action)]
    config_file: PathBuf,

    #[clap(long, action)]
    http_address: SocketAddrV6,

    #[clap(long, action)]
    dns_address: SocketAddr,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub log: dropshot::ConfigLogging,
    pub dropshot: dropshot::ConfigDropshot,
    pub storage: dns_server::storage::Config,
}

fn main() -> Result<(), anyhow::Error> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let config_file = &args.config_file;
    let config_file_contents = std::fs::read_to_string(config_file)
        .with_context(|| format!("read config file {:?}", config_file))?;
    let mut config: Config = toml::from_str(&config_file_contents)
        .with_context(|| format!("parse config file {:?}", config_file))?;

    config.dropshot.bind_address = SocketAddr::V6(args.http_address);
    eprintln!("{:?}", config);

    let log = config
        .log
        .to_logger("dns-server")
        .context("failed to create logger")?;

    let dns_server_config = dns_server::dns::server::Config {
        bind_address: args.dns_address,
        ..Default::default()
    };

    info!(&log, "config";
        "config" => ?config,
        "dns_config" => ?dns_server_config
    );

    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "store")),
        &config.storage,
    )
    .context("initializing persistent storage")?;

    let (_dns_server, dropshot_server) = dns_server::start_servers(
        log,
        store,
        &dns_server_config,
        &config.dropshot,
    )
    .await?;

    dropshot_server
        .await
        .map_err(|error_message| anyhow!("server exiting: {}", error_message))
}
