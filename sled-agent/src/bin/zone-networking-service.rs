// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to set up zone networking

use anyhow::anyhow;
// use anyhow::bail;
// use anyhow::Context;
// use bytes::Buf;
// use bytes::BufMut;
// use bytes::BytesMut;
// use camino::Utf8PathBuf;
// use chrono::Local;
// use clap::Args;
// use clap::Parser;
// use clap::Subcommand;
use clap::{arg, command, Command};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::Route;
// use futures::stream::StreamExt;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
//use sled_agent_client::types::CleanupContextUpdate;
//use sled_agent_client::types::Duration;
//use sled_agent_client::types::PriorityDimension;
//use sled_agent_client::types::PriorityOrder;
//use sled_agent_client::Client;
//use slog::Drain;
use slog::Level;
// use slog::LevelFilter;
// use slog::Logger;
// use slog_term::FullFormat;
// use slog_term::TermDecorator;
// use std::collections::BTreeSet;
use std::net::Ipv6Addr;
//use std::time::SystemTime;
//use tar::Builder;
//use tar::Header;
//use tokio::io::AsyncWriteExt;
//use uuid::Uuid;

// TODO: Set logger
fn _parse_log_level(s: &str) -> anyhow::Result<Level> {
    s.parse().map_err(|_| anyhow!("Invalid log level"))
}

fn parse_ipv6(s: &str) -> anyhow::Result<Ipv6Addr> {
    s.parse().map_err(|_| anyhow!("Invalid IPv6 address"))
}

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let matches = command!()
        .subcommand(
            Command::new("ensure-if").about("TODO description").arg(
                arg!(
                    -d --datalink <STRING> "datalink"
                )
                .required(true),
            ),
        )
        .subcommand(
            Command::new("set-mtu").about("TODO description").arg(
                arg!(
                    -d --datalink <STRING> "datalink"
                )
                .required(true),
            ),
        )
        .subcommand(
            Command::new("set-addrs")
                .about("TODO description")
                .arg(
                    arg!(
                        -l --listen_addr <Ipv6Addr> "listen_addr"
                    )
                    .required(true)
                    .value_parser(parse_ipv6),
                )
                .arg(
                    arg!(
                        -d --datalink <STRING> "datalink"
                    )
                    .required(true),
                ),
        )
        .subcommand(
            Command::new("add-route").about("TODO description").arg(
                arg!(
                    -g --gateway <Ipv6Addr> "gateway"
                )
                .required(true)
                .value_parser(parse_ipv6),
            ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("ensure-if") {
        let datalink: &String = matches.get_one("datalink").unwrap();
        Ipadm::set_temp_interface_for_datalink(&datalink).unwrap();
        println!("Temporary interface '{:?}' set", datalink);
    }

    if let Some(matches) = matches.subcommand_matches("set-mtu") {
        let datalink: &String = matches.get_one("datalink").unwrap();
        Ipadm::set_interface_mtu(&datalink).unwrap();
        println!("MTU set to 9000 for IPv6 and IPv4 on: {:?}", datalink);
    }

    if let Some(matches) = matches.subcommand_matches("set-addrs") {
        let listen_addr: &Ipv6Addr = matches.get_one("listen_addr").unwrap();
        let datalink: &String = matches.get_one("datalink").unwrap();
        Ipadm::create_static_and_autoconfigured_addrs(&datalink, listen_addr)
            .unwrap();
        println!("Static and auto-configured addresses set: {:?}", datalink);
    }

    if let Some(matches) = matches.subcommand_matches("add-route") {
        let gateway: &Ipv6Addr = matches.get_one("gateway").unwrap();
        Route::ensure_default_route_with_gateway(gateway).unwrap();
        println!("Default route with gateway '{:?}' set", gateway);
    }

    Ok(())
}
