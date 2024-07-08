// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::{anyhow, Context};
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
    if let Err(err) = do_run().await {
        fatal(CmdError::Failure(err));
    }
}

async fn do_run() -> anyhow::Result<()> {
    let log = dropshot::ConfigLogging::File {
        path: "/dev/stderr".into(),
        level: dropshot::ConfigLoggingLevel::Info,
        if_exists: dropshot::ConfigLoggingIfExists::Append,
    }
    .to_logger("zone-setup")
    .context("failed to construct stderr logger")?;

    let args = ZoneSetup::parse();

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
) -> anyhow::Result<()> {
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
        .context("failed to refresh logadm-upgrade")?;

    Ok(())
}

fn set_permissions_for_logadm_config() -> anyhow::Result<()> {
    let mut perms = metadata(LOGADM_CONFIG_FILE)
        .with_context(|| {
            format!("failed to read metadata of {LOGADM_CONFIG_FILE}")
        })?
        .permissions();
    perms.set_readonly(true);
    set_permissions(LOGADM_CONFIG_FILE, perms).with_context(|| {
        format!("failed to set 444 permissions on {LOGADM_CONFIG_FILE}")
    })?;

    let root_uid = get_user_by_name(ROOT)
        .with_context(|| format!("failed to look up user {ROOT}"))?
        .uid();

    let sys_gid = get_group_by_name(SYS)
        .with_context(|| format!("failed to look up group {SYS}"))?
        .gid();

    chown(LOGADM_CONFIG_FILE, Some(root_uid), Some(sys_gid)).with_context(
        || {
            format!(
            "could not set ownership to {ROOT}:{SYS} on {LOGADM_CONFIG_FILE}"
            )
        },
    )?;

    Ok(())
}

fn generate_chrony_config(
    args: ChronySetupArgs,
    log: &Logger,
) -> anyhow::Result<()> {
    let internal_ntp_tpl = "#
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

";

    let boundary_ntp_tpl = "#
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

";

    let ChronySetupArgs {
        file: config_path,
        boundary: is_boundary,
        servers,
        allow,
    } = args;

    let mut new_config =
        if is_boundary { boundary_ntp_tpl } else { internal_ntp_tpl }
            .to_string();

    if let Some(allow) = allow {
        new_config = new_config.replace("@ALLOW@", &allow);
    }

    if is_boundary {
        for s in servers {
            writeln!(
                &mut new_config,
                "pool {s} iburst maxdelay 0.1 maxsources 16"
            )
            .expect("write to String is infallible");
        }
    } else {
        for s in servers {
            writeln!(&mut new_config, "server {s} iburst minpoll 0 maxpoll 4")
                .expect("write to String is infallible");
        }
    }

    // We read the contents from the old configuration file if it existed
    // so that we can verify if it changed.
    let old_file = if config_path.exists() {
        Some(read_to_string(&config_path).with_context(|| {
            format!(
                "failed reading old chrony config file {}",
                config_path.display(),
            )
        })?)
    } else {
        None
    };

    let mut config_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&config_path)
        .with_context(|| {
            format!(
                "failed to create chrony config config_path {}",
                config_path.display(),
            )
        })?;
    config_file.write(new_config.as_bytes()).with_context(|| {
        format!(
            "failed writing chrony configuration file {}",
            config_path.display(),
        )
    })?;

    if let Some(old_config) = old_file {
        if old_config != new_config {
            info!(
                log, "Chrony configuration file has changed";
                "old configuration file" => ?old_config,
                "new configuration file" => ?new_config,
            );
        }
    }

    Ok(())
}

async fn common_nw_set_up(
    args: CommonNetworkingArgs,
    log: &Logger,
) -> anyhow::Result<()> {
    let zonename =
        zone::current().await.context("could not determine local zone name")?;
    let CommonNetworkingArgs { datalink, gateway, static_addr } = args;

    // TODO: remove when https://github.com/oxidecomputer/stlouis/issues/435 is
    // addressed
    info!(
        log, "Ensuring a temporary IP interface is created";
        "data link" => ?datalink,
    );
    Ipadm::set_temp_interface_for_datalink(&datalink).with_context(|| {
        format!(
            "failed to ensure temporary IP interface on datalink {datalink}",
        )
    })?;

    info!(
        log, "Setting MTU to 9000 for IPv6 and IPv4";
        "data link" => ?datalink,
    );
    Ipadm::set_interface_mtu(&datalink)
        .with_context(|| format!("failed to set MTU on datalink {datalink}"))?;

    info!(
        log,
        "Ensuring static and auto-configured addresses are set on the IP \
         interface";
        "data link" => ?datalink,
        "static address" => ?static_addr);
    Ipadm::create_static_and_autoconfigured_addrs(&datalink, &static_addr)
        .with_context(|| {
            format!(
                "failed to ensure static address {static_addr} \
                on datalink {datalink}",
            )
        })?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv6(gateway))
        .with_context(|| {
            format!("failed to ensure default route via gateway {gateway}")
        })?;

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
{static_addr} {zonename}.local {zonename}
"#,
        ),
    )
    .with_context(|| format!("failed to write hosts file {HOSTS_FILE}"))?;

    Ok(())
}

async fn opte_interface_set_up(
    args: OpteInterfaceArgs,
    log: &Logger,
) -> anyhow::Result<()> {
    let OpteInterfaceArgs { interface, gateway, ip } = args;
    info!(
        log,
        "Creating gateway on the OPTE IP interface if it doesn't already exist";
        "OPTE interface" => ?interface
    );
    Ipadm::create_opte_gateway(&interface).with_context(|| {
        format!("failed to create OPTE gateway on interface {interface}")
    })?;

    info!(
        log, "Ensuring there is a gateway route";
        "OPTE gateway" => ?gateway,
        "OPTE interface" => ?interface,
        "OPTE IP" => ?ip,
    );
    Route::ensure_opte_route(&gateway, &interface, &ip).with_context(|| {
        format!(
            "failed to ensure OPTE gateway route on interface {interface} \
                 with gateway {gateway} and IP {ip}",
        )
    })?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv4(gateway))
        .with_context(|| {
            format!("failed to ensure default route via gateway {gateway}")
        })?;

    Ok(())
}
