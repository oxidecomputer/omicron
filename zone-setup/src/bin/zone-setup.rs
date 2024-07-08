// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::{anyhow, Context};
use clap::{ArgAction, Args, Parser, Subcommand};
use illumos_utils::addrobj::{AddrObject, IPV6_LINK_LOCAL_ADDROBJ_NAME};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use illumos_utils::svcadm::Svcadm;
use illumos_utils::zone::{AddressRequest, Zones};
use illumos_utils::ExecutionError;
use omicron_common::backoff::{retry_notify, retry_policy_local, BackoffError};
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_sled_agent::services::SWITCH_ZONE_BASEBOARD_FILE;
use sled_hardware_types::underlay::BOOTSTRAP_PREFIX;
use slog::{info, Logger};
use std::fmt::Write as _;
use std::fs::{metadata, read_to_string, set_permissions, write, OpenOptions};
use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::chown;
use std::path::{Path, PathBuf};
use uzers::{get_group_by_name, get_user_by_name};
use zone_setup::switch_zone_user::SwitchZoneUser;

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
    /// Sets up switch zone configuration
    SwitchZone(SwitchZoneArgs),
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
    // TODO-john test flag with no args, replace comment
    #[arg(short, long, num_args = 0..=1)]
    gateway: Option<Ipv6Addr>,
    /// list of static addresses
    #[arg(short, long, num_args = 1..)]
    static_addrs: Vec<Ipv6Addr>,
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

#[derive(Debug, Args)]
struct SwitchZoneArgs {
    /// path to the baseboard file
    #[arg(short, long, default_value = SWITCH_ZONE_BASEBOARD_FILE)]
    baseboard_file: PathBuf,
    /// baseboard info JSON blob
    #[arg(short = 'i', long)]
    baseboard_info: serde_json::Value,
    /// bootstrap IPv6 address
    #[arg(short = 'a', long)]
    bootstrap_addr: Ipv6Addr,
    /// bootstrap VNIC name
    // TODO-john test empty string
    #[arg(short = 'v', long)]
    bootstrap_vnic: String,
    /// global zone local link IPv6 address
    #[arg(short, long)]
    gz_local_link_addr: Ipv6Addr,
    /// list of links that require link local addresses
    // TODO-john test space separation
    #[arg(short, long, num_args = 0..)]
    link_local_links: Vec<String>,
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
        ZoneSetupCommand::SwitchZone(args) => {
            switch_zone_setup(args, &log).await
        }
    }
}

async fn switch_zone_setup(
    args: SwitchZoneArgs,
    log: &Logger,
) -> anyhow::Result<()> {
    let SwitchZoneArgs {
        baseboard_file: file,
        baseboard_info: info,
        bootstrap_addr,
        bootstrap_vnic,
        gz_local_link_addr,
        link_local_links: links,
    } = args;

    info!(
        log, "Generating baseboard.json file";
        "baseboard file" => %file.display(),
        "baseboard info" => ?info,
    );
    generate_switch_zone_baseboard_file(&file, info)?;

    info!(&log, "Setting up the users required for wicket and support");
    let wicket_user = SwitchZoneUser::new(
        "wicket".to_string(),
        "wicket".to_string(),
        "Wicket User".to_string(),
        true,
        "/bin/sh".to_string(),
    );

    let support_user = SwitchZoneUser::new(
        "support".to_string(),
        "support".to_string(),
        "Oxide Support".to_string(),
        false,
        "/bin/bash".to_string(),
    )
    .with_homedir("/home/support".to_string())
    .with_profiles(vec!["Primary Administrator".to_string()]);

    let users = [wicket_user, support_user];
    for u in users {
        u.setup_switch_zone_user(log)?;
    }

    if links.is_empty() {
        info!(log, "No link local links to be configured");
    } else {
        info!(
            log, "Ensuring link local links";
            "links" => ?links,
        );
        for link in &links {
            Zones::ensure_has_link_local_v6_address(
                None,
                // TODO-john unwrap
                &AddrObject::new(link, IPV6_LINK_LOCAL_ADDROBJ_NAME).unwrap(),
            )
            .with_context(|| {
                format!("Could not ensure link local link {link:?}")
            })?;
        }
    }

    info!(
        log, "Ensuring bootstrap address exists in zone";
        "bootstrap address" => ?bootstrap_addr,
        "bootstrap vnic" => ?bootstrap_vnic,
    );
    let addrtype =
        AddressRequest::new_static(std::net::IpAddr::V6(bootstrap_addr), None);
    let addrobj_name = "bootstrap6";
    let addrobj =
        AddrObject::new(&bootstrap_vnic, addrobj_name).with_context(|| {
            format!("Could not create new addrobj {addrobj_name:?}")
        })?;

    let _ = Zones::create_address_internal(None, &addrobj, addrtype)
        .with_context(|| {
            format!("Could not create bootstrap address {addrobj} {addrtype:?}")
        })?;

    info!(
        log,
        "Forwarding bootstrap traffic via {bootstrap_vnic} to \
         {gz_local_link_addr}",
    );
    Route::add_bootstrap_route(
        BOOTSTRAP_PREFIX,
        gz_local_link_addr,
        &bootstrap_vnic,
    )
    .with_context(|| {
        format!(
            "Could not add bootstrap route via {bootstrap_vnic} \
             to {gz_local_link_addr}"
        )
    })?;

    Ok(())
}

fn generate_switch_zone_baseboard_file(
    file: &Path,
    info: serde_json::Value,
) -> anyhow::Result<()> {
    let config_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file)
        .with_context(|| {
            format!(
                "Could not create baseboard configuration file {}",
                file.display(),
            )
        })?;
    serde_json::to_writer(config_file, &info).with_context(|| {
        format!(
            "Could not write to baseboard configuration file {}",
            file.display(),
        )
    })?;
    Ok(())
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
# We allow chrony to step the system clock up to three times on startup if we
# are more than a second out. Even though boundary NTP servers are configured
# with local reference mode, they will not appear as authoritative servers
# until they synchronize upstream. This configuration allows us to converge
# quickly once the boundary servers converge. After this the clock will advance
# monotonically forward.
makestep 1.0 3

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

    let mut new_config =
        if args.boundary { boundary_ntp_tpl } else { internal_ntp_tpl }
            .to_string();

    if let Some(allow) = args.allow {
        new_config = new_config.replace("@ALLOW@", &allow);
    }

    if args.boundary {
        for s in args.servers {
            writeln!(
                &mut new_config,
                "pool {s} iburst maxdelay 0.1 minpoll 0 maxpoll 3 maxsources 16"
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
        Some(read_to_string(&args.file).with_context(|| {
            format!(
                "failed reading old chrony config file {}",
                args.file.display(),
            )
        })?)
    } else {
        None
    };

    let mut config_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.file)
        .with_context(|| {
            format!(
                "failed to create chrony config file {}",
                args.file.display(),
            )
        })?;
    config_file.write(new_config.as_bytes()).with_context(|| {
        format!(
            "failed writing chrony configuration file {}",
            args.file.display(),
        )
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
) -> anyhow::Result<()> {
    let zonename =
        zone::current().await.context("could not determine local zone name")?;

    info!(
        log,
        "Ensuring IP interface exists on datalink";
        "datalink" => args.datalink
    );
    Ipadm::ensure_ip_interface_exists(&args.datalink).with_context(|| {
        format!(
            "failed to ensure temporary IP interface on datalink {}",
            args.datalink
        )
    })?;

    info!(
        log, "Setting MTU to 9000 for IPv6 and IPv4";
        "datalink" => args.datalink,
    );
    Ipadm::set_interface_mtu(&args.datalink).with_context(|| {
        format!("failed to set MTU on datalink {}", args.datalink)
    })?;

    if args.static_addrs.is_empty() {
        info!(
            &log,
            "No static addresses provided; will not ensure static and \
             auto-configured addresses are set on the IP interface"
        );
    } else {
        for &addr in &args.static_addrs {
            if addr != Ipv6Addr::LOCALHOST {
                info!(
                    log,
                    "Ensuring static and auto-configured addresses are set on \
                     the IP interface";
                    "data link" => ?args.datalink,
                    "static address" => %addr);
                Ipadm::create_static_and_autoconfigured_addrs(
                    &args.datalink,
                    &addr,
                )
                .with_context(|| {
                    format!(
                        "failed to ensure static address {addr} on datalink {}",
                        args.datalink
                    )
                })?;
            } else {
                info!(
                    log,
                    "Static address is localhost; will not ensure it's set on \
                     the IP interface"
                );
            }
        }
    }

    match args.gateway {
        // Only the switch zone will sometimes have an unknown underlay address
        // at zone boot.
        None => {
            info!(
                log,
                "Underlay is not available yet; will not ensure default route",
            );
        }
        Some(gw) => {
            // Ensuring default route with gateway must happen after peer agents
            // have been initialized. Omicron zones will be able ensure a
            // default route with gateway immediately, but the switch zone on
            // the secondary scrimlet might need a few tries while it waits.
            retry_notify(
                  retry_policy_local(),
                  || async {
                        info!(
                            log, "Ensuring there is a default route";
                            "gateway" => ?gw,
                        );
                        Route::ensure_default_route_with_gateway(Gateway::Ipv6(gw))
                        .map_err(|err| {
                            let err_with_context = |err: ExecutionError| {
                                anyhow!(err).context(
            format!(
                "failed to ensure default route via gateway {gw}",
            ))
                            };
                            match err {
                                ExecutionError::CommandFailure(ref e) => {
                                    if e.stdout.contains("Network is unreachable") {
                                        BackoffError::transient(
                                            err_with_context(err),
                                        )
                                    } else {
                                        BackoffError::permanent(
                                            err_with_context(err),
                                        )
                                    }
                                }
                                _ => {
                                    BackoffError::permanent(
                                        err_with_context(err),
                                    )
                                }
                            }
                        })
                },
                |err, delay| {
                    info!(
                        &log,
                        "Cannot ensure there is a default route yet (retrying in {:?})",
                        delay;
                        "error" => ?err
                    );
                },
                )
                .await?;
        }
    }

    info!(
        log, "Populating hosts file for zone";
        "zonename" => ?zonename,
    );
    let mut hosts_contents = String::from(
        r#"
::1 localhost loghost
127.0.0.1 localhost loghost
"#,
    );

    for &addr in &args.static_addrs {
        writeln!(&mut hosts_contents, "{addr} {zonename}.local {zonename}")
            .expect("write to String is infallible");
    }

    write(HOSTS_FILE, hosts_contents)
        .with_context(|| format!("failed to write hosts file {HOSTS_FILE}"))?;

    Ok(())
}

async fn opte_interface_set_up(
    args: OpteInterfaceArgs,
    log: &Logger,
) -> anyhow::Result<()> {
    info!(
        log,
        "Creating gateway on the OPTE IP interface if it doesn't already exist";
        "OPTE interface" => ?args.interface
    );
    Ipadm::create_opte_gateway(&args.interface).with_context(|| {
        format!("failed to create OPTE gateway on interface {}", args.interface)
    })?;

    info!(
        log, "Ensuring there is a gateway route";
        "OPTE gateway" => ?args.gateway,
        "OPTE interface" => ?args.interface,
        "OPTE IP" => ?args.ip,
    );
    Route::ensure_opte_route(&args.gateway, &args.interface, &args.ip)
        .with_context(|| {
            format!(
                "failed to ensure OPTE gateway route on interface {} \
                 with gateway {} and IP {}",
                args.interface, args.gateway, args.ip
            )
        })?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?args.gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv4(args.gateway))
        .with_context(|| {
            format!(
                "failed to ensure default route via gateway {}",
                args.gateway
            )
        })?;

    Ok(())
}
