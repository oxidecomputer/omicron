// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone networking

use anyhow::anyhow;
use clap::{arg, command};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use slog::info;
use std::fs;
use std::net::Ipv6Addr;

pub const HOSTS_FILE: &str = "/etc/inet/hosts";

fn parse_ipv6(s: &str) -> anyhow::Result<Ipv6Addr> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing input value"));
    };
    s.parse().map_err(|_| anyhow!("ERROR: Invalid IPv6 address"))
}

fn parse_datalink(s: &str) -> anyhow::Result<String> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing data link"));
    };
    s.parse().map_err(|_| anyhow!("ERROR: Invalid data link"))
}

#[tokio::main]
async fn main() {
    if let Err(message) = do_run().await {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let log = dropshot::ConfigLogging::File {
        path: "/dev/stderr".into(),
        level: dropshot::ConfigLoggingLevel::Info,
        if_exists: dropshot::ConfigLoggingIfExists::Append,
    }
    .to_logger("zone-networking")
    .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    let matches = command!()
        .arg(
            arg!(
                -d --datalink <STRING> "datalink"
            )
            .required(true)
            .value_parser(parse_datalink),
        )
        .arg(
            arg!(
                -g --gateway <Ipv6Addr> "gateway"
            )
            .required(true)
            .value_parser(parse_ipv6),
        )
        .arg(
            arg!(
                -s --static_addr <Ipv6Addr> "static_addr"
            )
            .required(true)
            .value_parser(parse_ipv6),
        )
        .get_matches();

    let zonename =
        zone::current().await.expect("Could not determine local zone name");
    let datalink: &String = matches.get_one("datalink").unwrap();
    let static_addr: &Ipv6Addr = matches.get_one("static_addr").unwrap();
    let gateway: Ipv6Addr = *matches.get_one("gateway").unwrap();

    // TODO: remove when https://github.com/oxidecomputer/stlouis/issues/435 is
    // addressed
    info!(&log, "Ensuring a temporary IP interface is created"; "data link" => ?datalink);
    Ipadm::set_temp_interface_for_datalink(&datalink)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Setting MTU to 9000 for IPv6 and IPv4"; "data link" => ?datalink);
    Ipadm::set_interface_mtu(&datalink)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Ensuring static and auto-configured addresses are set on the IP interface"; "data link" => ?datalink, "static address" => ?static_addr);
    Ipadm::create_static_and_autoconfigured_addrs(&datalink, static_addr)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Ensuring there is a default route"; "gateway" => ?gateway);
    Route::ensure_default_route_with_gateway(Gateway::Ipv6(gateway))
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Populating hosts file for zone"; "zonename" => ?zonename);
    fs::write(
        HOSTS_FILE,
        format!(
            r#"
::1 localhost loghost
127.0.0.1 localhost loghost
{static_addr} {zonename}.local {zonename}
"#
        ),
    )
    .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    Ok(())
}
