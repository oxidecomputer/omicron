// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap documentation

use anyhow::anyhow;
use anyhow::Context;
use clap::Parser;
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
    // XXX-dap log arguments or put arguments into config
    info!(&log, "config"; "config" => ?config);

    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "store")),
        &config.storage,
    )
    .context("initializing persistent storage")?;

    let _dns_server = {
        let dns_server_config = dns_server::dns_server::Config {
            bind_address: args.dns_address.to_string(), // XXX-dap
        };
        dns_server::dns_server::Server::start(
            log.new(o!("component" => "dns")),
            store.clone(),
            dns_server_config,
        )
        .await
        .context("starting DNS server")?
    };

    let dropshot_server = {
        let http_api = dns_server::http_server::api();
        let http_api_context = dns_server::http_server::Context::new(store);

        dropshot::HttpServerStarter::new(
            &config.dropshot,
            http_api,
            http_api_context,
            &log.new(o!("component" => "http")),
        )
        .map_err(|error| anyhow!("setting up HTTP server: {:#}", error))?
        .start()
    };

    dropshot_server
        .await
        .map_err(|error_message| anyhow!("server exiting: {}", error_message))
}
