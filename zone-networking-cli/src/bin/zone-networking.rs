// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to set up zone networking

use anyhow::anyhow;
use clap::{arg, command, Command};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::Route;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use slog::Level;
use std::net::Ipv6Addr;

// TODO: Set logger?
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
            Command::new("ensure-if").about("Ensures a temporary IP interface is created with the given data link").arg(
                arg!(
                    -d --datalink <STRING> "datalink"
                )
                .required(true),
            ),
        )
        .subcommand(
            Command::new("set-mtu").about("Sets MTU to 9000 for IPv6 and IPv4 on the given data link").arg(
                arg!(
                    -d --datalink <STRING> "datalink"
                )
                .required(true),
            ),
        )
        .subcommand(
            Command::new("set-addrs")
                .about("Ensures static and auto-configured addresses are set on the given data link")
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
            Command::new("add-route").about("Ensures there is a default route with the given gateway").arg(
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
