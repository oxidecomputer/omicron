// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::anyhow;
use clap::{ArgAction, Args, Parser, Subcommand};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use illumos_utils::svcadm::Svcadm;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use slog::{info, Logger};
use std::fmt::Write as _;
use std::fs::{metadata, read_to_string, set_permissions, write, OpenOptions};
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::chown;
use std::path::PathBuf;
use uzers::{get_group_by_name, get_user_by_name};

#[derive(Debug, Parser)]
struct ZoneSetup {
    #[command(subcommand)]
    command: ZoneSetupCommand,
}

#[derive(Debug, Subcommand)]
enum ZoneSetupCommand {
    /// Sets up common networking configuration across all zones
    CommonNetworking(CommonNetworkingArgs),
    /// Sets up OPTE interface
    OpteInterface(OpteInterfaceArgs),
    /// Sets up Chrony configuration for NTP zone
    ChronySetup(ChronySetupArgs),
}

// Some of our `String` cli arguments may be set to the literal "unknown" if
// sled-agent hasn't populated our SMF config correctly. Reject this at argument
// parsing time.
fn parse_string_rejecting_unknown(s: &str) -> anyhow::Result<String> {
    if s != "unknown" {
        Ok(s.to_string())
    } else {
        Err(anyhow!("missing input value"))
    }
}

#[derive(Debug, Args)]
struct CommonNetworkingArgs {
    #[arg(short, long, value_parser = parse_string_rejecting_unknown)]
    datalink: String,
    #[arg(short, long)]
    gateway: Ipv6Addr,
    #[arg(short, long)]
    static_addr: Ipv6Addr,
}

#[derive(Debug, Args)]
struct OpteInterfaceArgs {
    #[arg(short, long, value_parser = parse_string_rejecting_unknown)]
    interface: String,
    #[arg(short, long)]
    gateway: Ipv4Addr,
    #[arg(short = 'p', long)]
    ip: IpAddr,
}

#[derive(Debug, Args)]
struct ChronySetupArgs {
    /// chrony configuration file
    #[arg(short, long, default_value = CHRONY_CONFIG_FILE)]
    file: PathBuf,
    /// whether this is a boundary or internal NTP zone
    #[arg(short, long, action = ArgAction::Set)]
    boundary: bool,
    /// list of NTP servers
    #[arg(short, long, num_args = 1..)]
    servers: Vec<String>,
    /// allowed IPv6 range
    #[arg(short, long)]
    allow: Option<String>,
}

const HOSTS_FILE: &str = "/etc/inet/hosts";
const CHRONY_CONFIG_FILE: &str = "/etc/inet/chrony.conf";
const LOGADM_CONFIG_FILE: &str = "/etc/logadm.d/chrony.logadm.conf";
const ROOT: &str = "root";
const SYS: &str = "sys";

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

    let args = ZoneSetup::try_parse()
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    match args.command {
        ZoneSetupCommand::CommonNetworking(args) => {
            common_nw_set_up(args, &log).await
        }
        ZoneSetupCommand::OpteInterface(args) => {
            opte_interface_set_up(args, &log).await
        }
        ZoneSetupCommand::ChronySetup(args) => chrony_setup(args, &log).await,
    }
}

async fn chrony_setup(
    args: ChronySetupArgs,
    log: &Logger,
) -> Result<(), CmdError> {
    println!("running chrony setup: {args:?}");

    generate_chrony_config(args, &log)?;

    // The NTP zone delivers a logadm fragment into /etc/logadm.d/ that needs to
    // be added to the system's /etc/logadm.conf. Unfortunately, the service
    // which does this - system/logadm-upgrade - only processes files with mode
    // 444 and root:sys ownership so we need to adjust things here (until
    // omicron package supports including ownership and permissions in the
    // generated tar files).
    info!(
        log, "Setting mode 444 and root:sys ownership to logadm fragment file";
        "logadm config" => ?LOGADM_CONFIG_FILE,
    );
    set_permissions_for_logadm_config()?;

    info!(
        log, "Updating logadm";
        "logadm config" => ?LOGADM_CONFIG_FILE,
    );
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
    args: ChronySetupArgs,
    log: &Logger,
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
# in this mode, we report as stratum 10 to clients. The `distance' parameter
# controls when we will decide to abandon the upstreams and switch to the local
# reference. By setting `activate`, we prevent the server from ever activating
# its local reference until it has synchronised with upstream at least once and
# the root distance has dropped below the provided threshold. This prevents
# a boundary server in a cold booted rack from authoritatively advertising a
# time from the 1980s prior to gaining external connectivity.
#
# distance: Distance from root above which we use the local reference, opting
#           to ignore the upstream.
# activate: Distance from root below which we must fall once to ever consider
#           the local reference.
#
local stratum 10 distance 0.4 activate 0.5

# makestep <threshold> <limit>
# We allow chrony to step the system clock during the first three time updates
# if we are more than 0.1 seconds out.
makestep 0.1 3

# When a leap second occurs we slew the clock over approximately 37 seconds.
leapsecmode slew
maxslewrate 2708.333

",
    );

    let mut new_config =
        if args.boundary { boundary_ntp_tpl } else { internal_ntp_tpl };

    if let Some(allow) = args.allow {
        new_config = new_config.replace("@ALLOW@", &allow);
    }

    if args.boundary {
        for s in args.servers {
            writeln!(
                &mut new_config,
                "pool {s} iburst maxdelay 0.1 maxsources 16"
            )
            .expect("write to String is infallible");
        }
    } else {
        for s in args.servers {
            writeln!(&mut new_config, "server {s} iburst minpoll 0 maxpoll 4")
                .expect("write to String is infallible");
        }
    }

    // We read the contents from the old configuration file if it existed
    // so that we can verify if it changed.
    let old_file = if args.file.exists() {
        Some(read_to_string(&args.file).map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not read old chrony configuration file {}: {}",
                args.file.display(),
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
        .open(&args.file)
        .map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not create chrony configuration file {}: {}",
                args.file.display(),
                err
            ))
        })?;
    config_file.write(new_config.as_bytes()).map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not write to chrony configuration file {}: {}",
            args.file.display(),
            err
        ))
    })?;

    if old_file.clone().is_some_and(|f| f != new_config) {
        info!(
            &log, "Chrony configuration file has changed";
            "old configuration file" => ?old_file,
            "new configuration file" => ?new_config,
        );
    }

    Ok(())
}

async fn common_nw_set_up(
    args: CommonNetworkingArgs,
    log: &Logger,
) -> Result<(), CmdError> {
    let zonename = zone::current().await.map_err(|err| {
        CmdError::Failure(anyhow!(
            "Could not determine local zone name: {}",
            err
        ))
    })?;

    // TODO: remove when https://github.com/oxidecomputer/stlouis/issues/435 is
    // addressed
    info!(
        log, "Ensuring a temporary IP interface is created";
        "data link" => ?args.datalink,
    );
    Ipadm::set_temp_interface_for_datalink(&args.datalink)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log, "Setting MTU to 9000 for IPv6 and IPv4";
        "data link" => ?args.datalink,
    );
    Ipadm::set_interface_mtu(&args.datalink)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log,
        "Ensuring static and auto-configured addresses are set on the IP \
         interface";
        "data link" => ?args.datalink,
        "static address" => ?args.static_addr);
    Ipadm::create_static_and_autoconfigured_addrs(
        &args.datalink,
        &args.static_addr,
    )
    .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?args.gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv6(args.gateway))
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log, "Populating hosts file for zone";
        "zonename" => ?zonename,
    );
    write(
        HOSTS_FILE,
        format!(
            r#"
::1 localhost loghost
127.0.0.1 localhost loghost
{} {zonename}.local {zonename}
"#,
            args.static_addr
        ),
    )
    .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    Ok(())
}

async fn opte_interface_set_up(
    args: OpteInterfaceArgs,
    log: &Logger,
) -> Result<(), CmdError> {
    info!(
        log,
        "Creating gateway on the OPTE IP interface if it doesn't already exist";
        "OPTE interface" => ?args.interface);
    Ipadm::create_opte_gateway(&args.interface)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log, "Ensuring there is a gateway route";
        "OPTE gateway" => ?args.gateway,
        "OPTE interface" => ?args.interface,
        "OPTE IP" => ?args.ip,
    );
    Route::ensure_opte_route(&args.gateway, &args.interface, &args.ip)
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?args.gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv4(args.gateway))
        .map_err(|err| CmdError::Failure(anyhow!(err)))?;

    Ok(())
}
