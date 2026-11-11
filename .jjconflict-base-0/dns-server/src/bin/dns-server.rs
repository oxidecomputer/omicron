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

/// Helper type to parse one or more socket address from CLI arguments.
#[derive(Clone, Debug)]
struct SocketAddrs(Vec<SocketAddr>);

impl SocketAddrs {
    /// Construct self from a comma-delimited list.
    fn from_delimited_list(input: &str) -> anyhow::Result<Self> {
        // NOTE: This is either non-empty or we fail, because split() always
        // returns _something_ and so we always have something to parse.
        let addrs = input
            .split(',')
            .map(|each| {
                each.parse::<SocketAddr>().with_context(|| {
                    format!("Parsing '{}' as socket addr", each)
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self(addrs))
    }
}

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, action)]
    config_file: PathBuf,

    /// Socket address for the Dropshot server used to program DNS records.
    #[clap(long, action)]
    http_address: SocketAddrV6,

    /// One or more socket addresses on which to serve DNS records.
    ///
    /// Multiple addresses may be separated by a comma (,). The DNS server will
    /// listen for requests on each address, using the same underlying storage.
    #[clap(
        long,
        action,
        value_parser = SocketAddrs::from_delimited_list,
        // Preserve the pre-existing `--dns-address` flag, which is how SMF
        // populates it.
        visible_alias = "dns-address",
    )]
    dns_addresses: SocketAddrs,
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

    let dns_server_config =
        dns_server::dns_server::Config::new(args.dns_addresses.0)
            .context("building DNS configuration")?;

    info!(&log, "config";
        "config" => ?config,
        "dns_config" => ?dns_server_config
    );

    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "store")),
        &config.storage,
    )
    .context("initializing persistent storage")?;

    let (dns_server, dropshot_server) = dns_server::start_servers(
        log,
        store,
        &dns_server_config,
        &config.dropshot,
    )
    .await?;

    // Wait for either the Dropshot server or any of the tasks in the DNS
    // server to exit.
    tokio::select! {
        dropshot_result = dropshot_server => {
            dropshot_result
                .map_err(|e| anyhow!("server exiting: {}", e))
        }
        dns_result = dns_server.wait_for_exit() => {
            match dns_result {
                Ok(res) => anyhow::bail!("DNS server task exited unexpectedly: {res:?}"),
                Err(je) => anyhow::bail!(
                    "Error joining DNS server task: '{je}'"
                ),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::SocketAddrs;
    use std::net::SocketAddr;

    #[test]
    fn can_parse_single_socket_addr() {
        let addr = "[fd00::1]:53".parse::<SocketAddr>().unwrap();
        assert_eq!(
            SocketAddrs::from_delimited_list(addr.to_string().as_str())
                .unwrap()
                .0,
            vec![addr],
        );
    }

    #[test]
    fn can_parse_multiple_valid_socket_addrs() {
        let addr1 = "[fd00::1]:53".parse::<SocketAddr>().unwrap();
        let addr2 = "1.2.3.4:53".parse::<SocketAddr>().unwrap();
        assert_eq!(
            SocketAddrs::from_delimited_list(
                format!("{},{}", addr1, addr2).as_str(),
            )
            .unwrap()
            .0,
            vec![addr1, addr2],
        );
    }

    #[test]
    fn fail_on_one_invalid_socket_addr() {
        let addr1 = "[fd00::1]:53".parse::<SocketAddr>().unwrap();
        let res =
            SocketAddrs::from_delimited_list(format!("{},foo", addr1).as_str());
        assert!(res.unwrap_err().to_string().contains("as socket addr"));
    }

    #[test]
    fn fail_on_empty_input() {
        assert!(SocketAddrs::from_delimited_list("").is_err());
    }
}
