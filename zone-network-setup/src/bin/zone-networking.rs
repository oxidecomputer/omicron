// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone networking

use anyhow::anyhow;
use clap::{arg, command, ArgMatches, Command};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use slog::{info, Logger};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub const HOSTS_FILE: &str = "/etc/inet/hosts";

fn parse_ip(s: &str) -> anyhow::Result<IpAddr> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing input value"));
    };
    s.parse().map_err(|_| anyhow!("ERROR: Invalid IP address"))
}

fn parse_ipv4(s: &str) -> anyhow::Result<Ipv4Addr> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing input value"));
    };
    s.parse().map_err(|_| anyhow!("ERROR: Invalid IPv4 address"))
}

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

fn parse_opte_iface(s: &str) -> anyhow::Result<String> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing OPTE interface"));
    };
    s.parse().map_err(|_| anyhow!("ERROR: Invalid OPTE interface"))
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
        .subcommand(
            Command::new("set-up")
                .about(
                    "Sets up common networking configuration across all zones",
                )
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
                ),
        )
        .subcommand(
            Command::new("opte-interface-set-up")
                .about("Sets up OPTE interface")
                .arg(
                    arg!(
                        -i --opte_interface <STRING> "opte_interface"
                    )
                    .required(true)
                    .value_parser(parse_opte_iface),
                )
                .arg(
                    arg!(
                        -g --opte_gateway <Ipv4Addr> "opte_gateway"
                    )
                    .required(true)
                    .value_parser(parse_ipv4),
                )
                .arg(
                    arg!(
                        -p --opte_ip <IpAddr> "opte_ip"
                    )
                    .required(true)
                    .value_parser(parse_ip),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("set-up") {
        set_up(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches("opte-interface-set-up") {
        opte_interface_set_up(matches, log.clone()).await?;
    }

    Ok(())
}

async fn set_up(matches: &ArgMatches, log: Logger) -> Result<(), CmdError> {
    let datalink: &String = matches.get_one("datalink").unwrap();
    let static_addr: &Ipv6Addr = matches.get_one("static_addr").unwrap();
    let gateway: Ipv6Addr = *matches.get_one("gateway").unwrap();
    let zonename = zone::current().await.map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not determine local zone name: {}",
            err
        ))
    })?;

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

async fn opte_interface_set_up(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
    let interface: &String = matches.get_one("opte_interface").unwrap();
    let gateway: Ipv4Addr = *matches.get_one("opte_gateway").unwrap();
    let opte_ip: &IpAddr = matches.get_one("opte_ip").unwrap();

    info!(&log, "Creating gateway on the OPTE IP interface if it doesn't already exist"; "OPTE interface" => ?interface);
    Ipadm::create_opte_gateway(interface)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Ensuring there is a gateway route"; "OPTE gateway" => ?gateway, "OPTE interface" => ?interface, "OPTE IP" => ?opte_ip);
    Route::ensure_opte_route(&gateway, interface, &opte_ip)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Ensuring there is a default route"; "gateway" => ?gateway);
    Route::ensure_default_route_with_gateway(Gateway::Ipv4(gateway))
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    Ok(())
}
