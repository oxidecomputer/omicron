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
use std::sync::Arc;

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
    let dns_address = &args.dns_address;
    let zone = &args.dns_zone;
    let config_file_contents = std::fs::read_to_string(config_file)
        .with_context(|| format!("read config file {:?}", config_file))?;
    let mut config: internal_dns::Config =
        toml::from_str(&config_file_contents)
            .with_context(|| format!("parse config file {:?}", config_file))?;

    config.dropshot.bind_address = SocketAddr::V6(args.server_address);
    eprintln!("{:?}", config);

    let log = config
        .log
        .to_logger("internal-dns")
        .context("failed to create logger")?;

    let db = Arc::new(sled::open(&config.data.storage_path)?);

    let _dns_server = {
        let db = db.clone();
        let log = log.clone();
        let dns_config = internal_dns::dns_server::Config {
            bind_address: dns_address.to_string(),
            zone: zone.to_string(),
        };
        internal_dns::dns_server::run(log, db, dns_config).await?
    };

    let dropshot_server = internal_dns::start_server(config, log, db).await?;
    dropshot_server
        .await
        .map_err(|error_message| anyhow!("server exiting: {}", error_message))
}
