// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::anyhow;
use clap::{arg, command, value_parser, Arg, ArgMatches, Command};
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
use uzers::{get_group_by_name, get_user_by_name};

pub const HOSTS_FILE: &str = "/etc/inet/hosts";
pub const CHRONY_CONFIG_FILE: &str = "/etc/inet/chrony.conf";
pub const LOGADM_CONFIG_FILE: &str = "/etc/logadm.d/chrony.logadm.conf";
pub const SWITCH_ZONE_BASEBOARD_FILE: &str = "/opt/oxide/baseboard.json";
pub const ROOT: &str = "root";
pub const SYS: &str = "sys";

pub const COMMON_NW_CMD: &str = "common-networking";
pub const OPTE_INTERFACE_CMD: &str = "opte-interface";
pub const CHRONY_SETUP_CMD: &str = "chrony-setup";
pub const SWITCH_ZONE_SETUP_CMD: &str = "switch-zone";

// User information for the switch zone
struct SwitchZoneUser {
    user: String,
    group: String,
    gecos: String,
    nopasswd: bool,
    homedir: Option<String>,
    shell: String,
    profiles: Option<Vec<String>>,
}

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

fn parse_chrony_conf(s: &str) -> anyhow::Result<String> {
    if s == "" {
        return Err(anyhow!("ERROR: Missing chrony configuration file"));
    };

    s.parse().map_err(|_| anyhow!("ERROR: Invalid chrony configuration file"))
}

fn parse_wicket_conf(s: &str) -> anyhow::Result<String> {
    if s == "" {
        return Err(anyhow!("ERROR: Missing baseboard configuration file"));
    };

    s.parse()
        .map_err(|_| anyhow!("ERROR: Invalid baseboard configuration file"))
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
                    Arg::new("static_addrs")
                    .short('s')
                    .long("static_addrs")
                    .num_args(1..)
                    .value_delimiter(' ')
                    .value_parser(parse_ipv6)
                    .help("List of static addresses separated by a space")
                    .required(true)
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
            Command::new(SWITCH_ZONE_SETUP_CMD)
                .about("Sets up switch zone configuration")
                .arg(
                    arg!(
                        -b --baseboard_file <STRING> "baseboard_file"
                    )
                    .default_value(SWITCH_ZONE_BASEBOARD_FILE)
                    .value_parser(parse_wicket_conf),
                )
                .arg(
                    arg!(
                        -i --baseboard_info <STRING> "baseboard_info"
                    )
                    .value_parser(parse_wicket_conf),
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
                )
                .arg(
                    arg!(-a --allow <String> "Allowed IPv6 range")
                    .num_args(0..=1)
                ),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches(COMMON_NW_CMD) {
        common_nw_set_up(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches(OPTE_INTERFACE_CMD) {
        opte_interface_set_up(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches(CHRONY_SETUP_CMD) {
        chrony_setup(matches, log.clone()).await?;
    }

    if let Some(matches) = matches.subcommand_matches(SWITCH_ZONE_SETUP_CMD) {
        switch_zone_setup(matches, log.clone()).await?;
    }

    Ok(())
}

async fn switch_zone_setup(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
    let file: &String = matches.get_one("baseboard_file").unwrap();
    let info: &String = matches.get_one("baseboard_info").unwrap();

    info!(&log, "Generating baseboard.json file"; "baseboard file" => ?file, "baseboard info" => ?info);
    generate_switch_zone_baseboard_file(file, info)?;

    Ok(())
}

fn generate_switch_zone_baseboard_file(
    file: &String,
    info: &String,
) -> Result<(), CmdError> {
    let mut config_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file)
        .map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not create baseboard configuration file {}: {}",
                file,
                err
            ))
        })?;
    config_file.write(info.as_bytes()).map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not write to baseboard configuration file {}: {}",
            file,
            err
        ))
    })?;
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

    generate_chrony_config(&log, is_boundary, allow, file, servers)?;

    // The NTP zone delivers a logadm fragment into /etc/logadm.d/ that needs to
    // be added to the system's /etc/logadm.conf. Unfortunately, the service which
    // does this - system/logadm-upgrade - only processes files with mode 444 and
    // root:sys ownership so we need to adjust things here (until omicron package
    // supports including ownership and permissions in the generated tar files).
    info!(&log, "Setting mode 444 and root:sys ownership to logadm fragment file"; "logadm config" => ?LOGADM_CONFIG_FILE);
    set_permissions_for_logadm_config()?;

    info!(&log, "Updating logadm"; "logadm config" => ?LOGADM_CONFIG_FILE);
    Svcadm::refresh_logadm_upgrade()
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    Ok(())
}

fn set_permissions_for_logadm_config() -> Result<(), CmdError> {
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

    let root_uid = match get_user_by_name(ROOT) {
        Some(user) => user.uid(),
        None => {
            return Err(CmdError::Failure(anyhow!(format!(
                "Could not retrieve ID from user: {}",
                ROOT
            ))))
        }
    };

    let sys_gid = match get_group_by_name(SYS) {
        Some(group) => group.gid(),
        None => {
            return Err(CmdError::Failure(anyhow!(format!(
                "Could not retrieve ID from group: {}",
                SYS
            ))))
        }
    };

    chown(LOGADM_CONFIG_FILE, Some(root_uid), Some(sys_gid)).map_err(
        |err| {
            CmdError::Failure(anyhow!(
                "Could not set ownership of logadm configuration file {}: {}",
                LOGADM_CONFIG_FILE,
                err
            ))
        },
    )?;

    Ok(())
}

fn generate_chrony_config(
    log: &Logger,
    is_boundary: &bool,
    allow: Option<&String>,
    file: &String,
    servers: Vec<&String>,
) -> Result<(), CmdError> {
    let internal_ntp_tpl = String::from(
        "#
# Configuration file for an internal NTP server - one which communicates with
# boundary NTP servers within the rack.
#

driftfile /var/lib/chrony/drift
ntsdumpdir /var/lib/chrony
dumpdir /var/lib/chrony
pidfile /var/run/chrony/chronyd.pid
logdir /var/log/chrony

log measurements statistics tracking

# makestep <threshold> <limit>
# We allow chrony to step the system clock if we are more than a day out,
# regardless of how many clock updates have occurred since boot.
# The boundary NTP servers are configured with local reference mode, which
# means that if they start up without external connectivity, they will appear
# as authoritative servers even if they are advertising January 1987
# (which is the default system clock on a gimlet after boot).
# This configuration allows a one-off adjustment once RSS begins and the
# boundary servers are synchronised, after which the clock will advance
# monotonically forwards.
makestep 86400 -1

# When a leap second occurs we slew the clock over approximately 37 seconds.
leapsecmode slew
maxslewrate 2708.333

",
    );

    let boundary_ntp_tpl = String::from(
        "#
# Configuration file for a boundary NTP server - one which communicates with
# NTP servers outside the rack.
#

driftfile /var/lib/chrony/drift
ntsdumpdir /var/lib/chrony
dumpdir /var/lib/chrony
pidfile /var/run/chrony/chronyd.pid
logdir /var/log/chrony

log measurements statistics tracking

allow fe80::/10
allow @ALLOW@

# Enable local reference mode, which keeps us operating as an NTP server that
# appears synchronised even if there are currently no active upstreams. When
# in this mode, we report as stratum 10 to clients.
local stratum 10

# makestep <threshold> <limit>
# We allow chrony to step the system clock during the first three time updates
# if we are more than 0.1 seconds out.
makestep 0.1 3

# When a leap second occurs we slew the clock over approximately 37 seconds.
leapsecmode slew
maxslewrate 2708.333

",
    );

    let mut contents =
        if *is_boundary { boundary_ntp_tpl } else { internal_ntp_tpl };

    if let Some(allow) = allow {
        contents = contents.replace("@ALLOW@", &allow.to_string());
    }

    let new_config = if *is_boundary {
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
        info!(&log, "Chrony configuration file has changed"; 
        "old configuration file" => ?old_file, "new configuration file" => ?new_config,);
    }

    Ok(())
}

async fn common_nw_set_up(
    matches: &ArgMatches,
    log: Logger,
) -> Result<(), CmdError> {
    let datalink: &String = matches.get_one("datalink").unwrap();
    let static_addrs = matches
        .get_many::<Ipv6Addr>("static_addrs")
        .unwrap()
        .collect::<Vec<_>>();
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

    for addr in &static_addrs {
        info!(&log, "Ensuring static and auto-configured addresses are set on the IP interface"; "data link" => ?datalink, "static address" => ?addr);
        Ipadm::create_static_and_autoconfigured_addrs(&datalink, addr)
            .map_err(|err| CmdError::Failure(anyhow!(err)))?;
    }

    info!(&log, "Ensuring there is a default route"; "gateway" => ?gateway);
    Route::ensure_default_route_with_gateway(Gateway::Ipv6(gateway))
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(&log, "Populating hosts file for zone"; "zonename" => ?zonename);
    let mut hosts_contents = String::from(
        r#"
::1 localhost loghost
127.0.0.1 localhost loghost
"#,
    );

    for addr in static_addrs.clone() {
        let s = format!(
            r#"{addr} {zonename}.local {zonename}
"#
        );
        hosts_contents.push_str(s.as_str())
    }

    write(HOSTS_FILE, hosts_contents)
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
