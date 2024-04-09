// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::anyhow;
use clap::{arg, command, value_parser, Arg, ArgMatches, Command};
// TODO: Removeme
// use illumos_utils::chronyd::Chronyd;
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use illumos_utils::svcadm::Svcadm;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use slog::{info, Logger};
use std::fs::{metadata, read_to_string, set_permissions, write, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::chown;
use std::path::Path;

pub const HOSTS_FILE: &str = "/etc/inet/hosts";
pub const CHRONY_CONFIG_FILE: &str = "/etc/inet/chrony.conf";
pub const LOGADM_CONFIG_FILE: &str = "/etc/logadm.d/chrony.logadm.conf";
pub const INTERNAL_NTP_CONFIG_TPL: &str = "/etc/inet/chrony.conf.internal";
pub const BOUNDARY_NTP_CONFIG_TPL: &str = "/etc/inet/chrony.conf.boundary";

pub const COMMON_NW_CMD: &str = "common-networking";
pub const OPTE_INTERFACE_CMD: &str = "opte-interface";
pub const CHRONY_SETUP_CMD: &str = "chrony-setup";
// TODO: Removeme before merging
pub const NTP_CMD: &str = "ntp";
pub const NTP_START_CMD: &str = "start";

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

fn parse_allow(s: &str) -> anyhow::Result<String> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing allowed address range"));
    };

    s.parse().map_err(|_| anyhow!("ERROR: Invalid allowed address range"))
}

fn parse_chrony_conf(s: &str) -> anyhow::Result<String> {
    if s == "unknown" {
        return Err(anyhow!("ERROR: Missing chrony configuration file"));
    };

    // TODO: actually check the format of the string ends with "chrony.conf"
    s.parse().map_err(|_| anyhow!("ERROR: Invalid chrony configuration file"))
}

fn parse_boundary(s: &str) -> anyhow::Result<bool> {
    s.parse().map_err(|_| anyhow!("ERROR: Invalid boundary input"))
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
    .to_logger("zone-setup")
    .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    let matches = command!()
        .subcommand(
            Command::new(COMMON_NW_CMD)
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
            Command::new(OPTE_INTERFACE_CMD)
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
        .subcommand(
            Command::new(CHRONY_SETUP_CMD)
                .about("Sets up Chrony configuration for NTP zone") 
                .arg(
                    arg!(-f --file <String> "Chrony configuration file")
                    .default_value(CHRONY_CONFIG_FILE)
            .value_parser(parse_chrony_conf)
                )
            .arg(
                arg!(-b --boundary <bool> "Whether this is a boundary or internal NTP zone")
                .required(true)
        .value_parser(parse_boundary),
            )
            .arg(
                Arg::new("servers")
                .short('s')
                .long("servers")
                .num_args(1..)
                .value_delimiter(' ')
                .value_parser(value_parser!(String))
                .help("List of NTP servers separated by a space")
                .required(true)
                // TODO: Add some parsing to this?
            )
            .arg(
                arg!(-a --allow <String> "Allowed IPv6 range")                       
                .value_parser(parse_allow),
            ),
        )
        // TODO: Removeme before merging
        .subcommand(
            Command::new(NTP_CMD)
                .about("Start (set up), refresh and stop methods for NTP zone")
                .subcommand(
                    Command::new(NTP_START_CMD)
                        .about("NTP zone setup and start chronyd daemon") 
                        .arg(
                            arg!(-f --file <String> "Chrony configuration file")
                            .default_value(CHRONY_CONFIG_FILE)
                    .value_parser(parse_chrony_conf)
                        )
                    .arg(
                        arg!(-b --boundary <bool> "Whether this is a boundary or internal NTP zone")
                        .required(true)
                .value_parser(parse_boundary),
                    )
                    .arg(
                        Arg::new("servers")
                        .short('s')
                        .long("servers")
                        .num_args(1..)
                        .value_delimiter(' ')
                        .value_parser(value_parser!(String))
                        .help("List of NTP servers separated by a space")
                        .required(true)
                        // TODO: Add some parsing to this?
                    )
                    .arg(
                        arg!(-a --allow <String> "Allowed IPv6 range")                       
                        .value_parser(parse_allow),
                    ),
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches(COMMON_NW_CMD) {
        common_nw_set_up(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches(OPTE_INTERFACE_CMD) {
        opte_interface_set_up(matches, log.clone()).await?;
    }

    // TODO: Removeme
    if let Some(matches) = matches.subcommand_matches(NTP_CMD) {
        ntp_smf_methods(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches(CHRONY_SETUP_CMD) {
        chrony_setup(matches, log.clone()).await?;
    }

    Ok(())
}

async fn ntp_smf_methods(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
    if let Some(matches) = matches.subcommand_matches(NTP_START_CMD) {
        chrony_setup(matches, log.clone()).await?;
    }

    // TODO: Add refresh and stop
    Ok(())
}

async fn chrony_setup(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
    let servers =
        matches.get_many::<String>("servers").unwrap().collect::<Vec<_>>();
    let allow: Option<&String> = matches.get_one("allow");

    let file: &String = matches.get_one("file").unwrap();
    let is_boundary: &bool = matches.get_one("boundary").unwrap();
    println!(
        "servers: {:?}\nfile: {}\nallow: {:?}\nboundary: {:?}",
        servers, file, allow, is_boundary
    );

    // TODO: Extract gen_config_file function here?
    let template = if *is_boundary {
        BOUNDARY_NTP_CONFIG_TPL
    } else {
        INTERNAL_NTP_CONFIG_TPL
    };
    info!(&log, "Generating chrony configuration file"; "configuration file" => ?file, "configuration template" => ?template, "allowed IPs" => ?allow, "servers" => ?servers, "is boundary" => ?is_boundary);

    // Generate config file
    let mut contents = read_to_string(template).map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not read chrony configuration template {}: {}",
            template,
            err
        ))
    })?;
    let new_config = if *is_boundary {
        let mut contents = contents.replace(
            "@ALLOW@",
            &allow.expect("Chrony allowed address not supplied").to_string(),
        );
        for s in servers {
            let str_line =
                format!("pool {} iburst maxdelay 0.1 maxsources 16\n", s);
            contents.push_str(&str_line)
        }
        contents
    } else {
        for s in servers {
            let str_line = format!("server {} iburst minpoll 0 maxpoll 4\n", s);
            contents.push_str(&str_line)
        }
        contents
    };

    // We read the contents from the old configuration file if it existed
    // so that we can verify if it changed.
    let old_file = if Path::exists(Path::new(file)) {
        Some(read_to_string(file).map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not read old chrony configuration file {}: {}",
                file,
                err
            ))
        })?)
    } else {
        None
    };

    let mut config_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file)
        .map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not create chrony configuration file {}: {}",
                file,
                err
            ))
        })?;
    config_file.write(new_config.as_bytes()).map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not write to chrony configuration file {}: {}",
            file,
            err
        ))
    })?;

    if old_file.clone().is_some_and(|f| f != new_config) {
        info!(&log, "Chrony configuration file has changed"; "old configuration file" => ?old_file);
    }
    // End gen_config_file here

    // Update logadm
    //
    // The NTP zone delivers a logadm fragment into /etc/logadm.d/ that needs to
    // be added to the system's /etc/logadm.conf. Unfortunately, the service which
    // does this - system/logadm-upgrade - only processes files with mode 444 and
    // root:sys ownership so we need to adjust things here (until omicron package
    // supports including ownership and permissions in the generated tar files).
    let mut perms = metadata(LOGADM_CONFIG_FILE)
        .map_err(|err| {
            CmdError::Failure(anyhow!(
            "Could not retrieve chrony logadm configuration file {} metadata: {}",
            LOGADM_CONFIG_FILE,
            err
        ))
        })?
        .permissions();
    perms.set_readonly(true);
    set_permissions(LOGADM_CONFIG_FILE, perms).map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not set 444 permissions on chrony logadm configuration file {}: {}",
                    LOGADM_CONFIG_FILE,
                    err
                ))
            })?;

    chown(LOGADM_CONFIG_FILE, Some(0), Some(3)).map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not set ownership of logadm configuration file {}: {}",
            LOGADM_CONFIG_FILE,
            err
        ))
    })?;

    info!(&log, "Updating logadm"; "logadm config" => ?LOGADM_CONFIG_FILE);
    Svcadm::refresh_logadm_upgrade()
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;
 // TODO: Removeme
 //   info!(&log, "Starting chronyd daemon"; "chrony config" => ?file);
 //   Chronyd::start_daemon(file)
 //       .map_err(|err| CmdError::Failure(anyhow!(err)))?;
    Ok(())
}

async fn common_nw_set_up(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
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
    write(
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
