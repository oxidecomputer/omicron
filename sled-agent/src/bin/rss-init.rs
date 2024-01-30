// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Program to initialize the rack directly via a bootstrap agent client
//! Mainly used for testbed runs. In production, wicket is used.

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use std::net::SocketAddrV6;

const BOOTSTRAP_AGENT_HTTP_PORT: u16 = 80;

#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, value_name = "FILE")]
    config: Utf8PathBuf,

    #[arg(short, long, value_name = "IP_ADDR")]
    bootstrap_agent_ip: std::net::Ipv6Addr,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let drain = slog::Discard;
    let log = slog::Logger::root(drain, slog::o!());

    let content = std::fs::read_to_string(&args.config)?;
    let config = toml::from_str(&content)
        .with_context(|| "parsing config-rss as TOML".to_string())?;

    let addr = SocketAddrV6::new(
        args.bootstrap_agent_ip,
        BOOTSTRAP_AGENT_HTTP_PORT,
        0,
        0,
    );
    let client =
        bootstrap_agent_client::Client::new(&format!("http://{}", addr), log);

    client.rack_initialize(&config).await?;

    Ok(())
}
