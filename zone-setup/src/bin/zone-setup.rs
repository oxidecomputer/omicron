// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI to set up zone configuration

use anyhow::{Context, anyhow, bail};
use clap::builder::{
    NonEmptyStringValueParser, StringValueParser, TypedValueParser,
};
use clap::{ArgAction, Args, Parser, Subcommand};
use illumos_utils::ExecutionError;
use illumos_utils::addrobj::{AddrObject, IPV6_LINK_LOCAL_ADDROBJ_NAME};
use illumos_utils::ipadm::Ipadm;
use illumos_utils::route::{Gateway, Route};
use illumos_utils::svcadm::Svcadm;
use illumos_utils::zone::{AddressRequest, Zones};
use omicron_common::backoff::{BackoffError, retry_notify, retry_policy_local};
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
use oxnet::Ipv6Net;
use sled_agent_types::sled::SWITCH_ZONE_BASEBOARD_FILE;
use sled_hardware_types::underlay::BOOTSTRAP_PREFIX;
use slog::{Logger, info};
use std::fmt::Write as _;
use std::fs::{OpenOptions, metadata, read_to_string, set_permissions, write};
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
    if s.is_empty() || s == "unknown" {
        Err(anyhow!("missing input value"))
    } else {
        Ok(s.to_string())
    }
}

#[derive(Debug, Args)]
struct CommonNetworkingArgs {
    #[arg(short, long, value_parser = parse_string_rejecting_unknown)]
    datalink: String,
    // The `num_args = 0..=1` here allows passing the `-g` gateway flag with no
    // value; the SMF manifest for the switch zone will do this when the
    // underlay isn't up yet.
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
    /// OPTE-specific gateway for external connectivity via boundary services
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
    ///
    /// At least one must be provided if this is a boundary NTP zone. Ignored if
    /// this is an internal NTP zone: internal NTP zones find their servers, the
    /// boundary NTP zones, via DNS.
    #[arg(
        short,
        long,
        num_args = 0..,
        value_parser = NonEmptyStringValueParser::default(),
    )]
    servers: Vec<String>,
    /// allowed IPv6 range
    #[arg(short, long)]
    allow: Ipv6Net,
    /// DNS name for the boundary NTP zone pool
    #[arg(
        short = 'p',
        long,
        value_parser = NonEmptyStringValueParser::default(),
    )]
    boundary_pool: String,
}

// The default clap parser for `serde_json::Value` is to wrap the argument in a
// `Value::String`; this value parser parses the argument as JSON instead.
fn json_value_parser() -> impl TypedValueParser<Value = serde_json::Value> {
    StringValueParser::new().try_map(|s| s.parse())
}

#[derive(Debug, Args)]
struct SwitchZoneArgs {
    /// path to the baseboard file
    #[arg(short, long, default_value = SWITCH_ZONE_BASEBOARD_FILE)]
    baseboard_file: PathBuf,
    /// baseboard info JSON blob
    #[arg(short = 'i', long, value_parser = json_value_parser())]
    baseboard_info: serde_json::Value,
    /// bootstrap IPv6 address
    #[arg(short = 'a', long)]
    bootstrap_addr: Ipv6Addr,
    /// bootstrap VNIC name
    #[arg(
        short = 'v',
        long,
        value_parser = NonEmptyStringValueParser::default(),
    )]
    bootstrap_vnic: String,
    /// global zone local link IPv6 address
    #[arg(short, long)]
    gz_local_link_addr: Ipv6Addr,
    /// list of links that require link local addresses
    #[arg(
        short,
        long,
        num_args = 0..,
        value_parser = NonEmptyStringValueParser::default(),
    )]
    link_local_links: Vec<String>,
}

const HOSTS_FILE: &str = "/etc/inet/hosts";
const CHRONY_CONFIG_FILE: &str = "/etc/inet/chrony.conf";
const LOGADM_CONFIG_FILE: &str = "/etc/logadm.d/chrony.logadm.conf";
const ROOT: &str = "root";
const SYS: &str = "sys";

fn main() {
    if let Err(err) = omicron_runtime::run(do_run()) {
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

    info!(log, "Setting up the users required for wicket and support");
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
    .with_homedir("/home/support".into())
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
            let addrobj = AddrObject::new(link, IPV6_LINK_LOCAL_ADDROBJ_NAME)
                .with_context(|| {
                format!("invalid link name for addrobj: {link:?}")
            })?;
            Zones::ensure_has_link_local_v6_address(None, &addrobj)
                .await
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
        .await
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
    .await
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
    info!(log, "running chrony setup"; "args" => ?args);

    generate_chrony_config(args, log)?;

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
        .await
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
    // Rack Time Synchronisation
    // -------------------------
    //
    // Within an Oxide rack, every sled in the cluster runs an NTP server zone.
    // Two of these zones are nominated to be "Boundary NTP Servers" which
    // means that they have external connectivity via boundary networking
    // services and are configured with the NTP server address(es)/name(s)
    // provided during RSS. The other zones are "Internal NTP Servers", do not
    // have external connectivity, and synchronise with the boundary servers
    // across the rack's underlay network.
    //
    // Every sled initially starts up with the notion that it is late December
    // 1986, and there are a number of challenges in order to reach consensus
    // around time in the rack, particularly in situations where one of more
    // boundary servers lacks external connectivity, either at startup or
    // later. A number of strategies are employed in the configurations
    // below.
    //
    // - Each boundary server can authoratitively advertise time at stratum
    //   10 based on its local clock and will do this when there are no
    //   "selectable" upstream servers. However, to avoid the situation
    //   where December 1986 is advertised with authority, they will not use
    //   this local source until the clock has been successfully
    //   synchronised to an upstream source at least once. In the event that
    //   a rack starts up with no external NTP connectivity everything
    //   stops, waiting for time synchronisation to occur (that is, for the
    //   networking issue to be resolved).
    //
    // - Each boundary server has its upstream sources configured with:
    //   - maximum poll interval 2^5 (32 seconds). When a time source is
    //     considered trustworthy and relatively stable over time, the rate
    //     at which it is queried is reduced. We set a ceiling on the
    //     polling rate so that we can still react relatively quickly to
    //     events such as loss of external connectivity. Note that if
    //     an update fails, the poll interval will rapidly decrease back
    //     down towards one second.
    //   - maximum number of retained samples is 8. This sets an upper limit
    //     on the number of samples so that trust degrades more quickly in
    //     the event the source is not contactable.
    //   - The "failfast" flag causes the source to be immediately marked as
    //     "unselectable" if it has not been contactable for several
    //     consecutive attempts. Without this flag, the source would remain
    //     selected and its root dispersion (and therefore its distance)
    //     would increase fairly slowly. The source would become
    //     unselectable after around an hour given the rest of the
    //     configuration, which is far too slow.
    //
    // - The boundary servers include each other in their list of sources.
    //   While they will see themselves in their source list, they will
    //   automatically discount that to prevent a loop. Due to the "orphan"
    //   tab on the local source mentioned earlier, when both boundary
    //   servers fall back to their local clock source, the one with the
    //   lowest reference ID will be preferred, protecting against a split
    //   brain scenario when neither server has upstream connectivity and
    //   both are are advertising their local clock with authority.

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

# Refresh boundary NTP servers every two minutes instead of every two weeks
refresh 120

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
# reference, although this is largely redundant due to the upstream sources
# being flagged as 'failfast'. By setting `activate`, we prevent the server
# from ever activating its local reference until it has synchronised with
# upstream at least once and the root distance has dropped below the
# provided threshold. This prevents a boundary server in a cold booted rack
# from authoritatively advertising a time from the 1980s prior to gaining
# external connectivity.
#
# distance: Distance from root above which we use the local reference, opting
#           to ignore the upstream.
# activate: Distance from root below which we must fall once to ever consider
#           the local reference.
# orphan:   This option enables orphan mode, where sources with the same
#           stratum as our local are ignored unless no other source is
#           selectable and their reference IDs are smaller than ours. This
#           protects against a split brain situation when neither boundary
#           server has connectivity.
#
local stratum 10 orphan distance 0.4 activate 0.5

# makestep <threshold> <limit>
# We allow chrony to step the system clock during the first three time updates
# if we are more than 0.1 seconds out.
makestep 0.1 3

# When a leap second occurs we slew the clock over approximately 37 seconds.
leapsecmode slew
maxslewrate 2708.333

# Refresh boundary NTP servers every two minutes instead of every two weeks
refresh 120

# When a source is unreachable, increase its dispersion by 60 microseconds/s
# instead of the default of 1.
maxclockerror 60

";

    let ChronySetupArgs {
        file: config_path,
        boundary: is_boundary,
        servers,
        allow,
        boundary_pool,
    } = args;

    let mut new_config =
        if is_boundary { boundary_ntp_tpl } else { internal_ntp_tpl }
            .to_string();

    new_config = new_config.replace("@ALLOW@", &allow.to_string());

    if is_boundary {
        if servers.is_empty() {
            bail!(
                "at least one upstream server must be provided for \
                 boundary NTP zones"
            );
        }

        for s in servers {
            writeln!(
                &mut new_config,
                "pool {s} iburst maxdelay 0.1 maxsources 16 \
                 minpoll 0 maxpoll 5 maxsamples 8 failfast"
            )
            .expect("write to String is infallible");
        }
    }
    writeln!(
        &mut new_config,
        "pool {boundary_pool} iburst maxdelay 0.1 maxsources 16",
    )
    .expect("write to String is infallible");

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
    let CommonNetworkingArgs { datalink, gateway, static_addrs } = args;

    info!(
        log,
        "Ensuring IP interface exists on datalink";
        "datalink" => &datalink
    );
    Ipadm::ensure_ip_interface_exists(&datalink).await.with_context(|| {
        format!(
            "failed to ensure temporary IP interface on datalink {datalink}",
        )
    })?;

    info!(
        log, "Setting MTU to 9000 for IPv6 and IPv4";
        "datalink" => &datalink,
    );
    Ipadm::set_interface_mtu(&datalink)
        .await
        .with_context(|| format!("failed to set MTU on datalink {datalink}"))?;

    info!(
        log, "Setting TCP recv_buf size to 1 MB";
    );
    Ipadm::set_tcp_recv_buf().await.context("failed to set TCP recv_buf")?;

    info!(
        log, "Setting TCP congestion control algorithm to cubic";
    );
    Ipadm::set_tcp_congestion_control()
        .await
        .context("failed to set TCP congestion_control")?;

    if static_addrs.is_empty() {
        info!(
            log,
            "No static addresses provided; will not ensure static and \
             auto-configured addresses are set on the IP interface"
        );
    } else {
        for &addr in &static_addrs {
            if addr != Ipv6Addr::LOCALHOST {
                info!(
                    log,
                    "Ensuring static and auto-configured addresses are set on \
                     the IP interface";
                    "data link" => ?datalink,
                    "static address" => %addr);
                Ipadm::create_static_and_autoconfigured_addrs(&datalink, &addr)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to ensure static address {addr} on \
                             datalink {datalink}",
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

    match gateway {
        // Only the switch zone will sometimes have an unknown underlay address
        // at zone boot.
        None => {
            info!(
                log,
                "Underlay is not available yet; will not ensure default route",
            );
        }
        Some(gw) => {
            ensure_default_route_via_gateway_with_retries(gw, log).await?;
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

    for addr in static_addrs {
        writeln!(&mut hosts_contents, "{addr} {zonename}.local {zonename}")
            .expect("write to String is infallible");
    }

    write(HOSTS_FILE, hosts_contents)
        .with_context(|| format!("failed to write hosts file {HOSTS_FILE}"))?;

    Ok(())
}

async fn ensure_default_route_via_gateway_with_retries(
    gateway: Ipv6Addr,
    log: &Logger,
) -> anyhow::Result<()> {
    // Helper to attach error context in the retry loop below.
    let err_with_context = |err: ExecutionError| {
        anyhow!(err).context(format!(
            "failed to ensure default route via gateway {gateway}",
        ))
    };

    let gateway = Gateway::Ipv6(gateway);

    // Ensuring default route with gateway must happen after peer agents
    // have been initialized. Omicron zones will be able ensure a
    // default route with gateway immediately, but the switch zone on
    // the secondary scrimlet might need a few tries while it waits.
    retry_notify(
        retry_policy_local(),
        || async {
            info!(
                log, "Ensuring there is a default route";
                "gateway" => ?gateway,
            );
            Route::ensure_default_route_with_gateway(gateway).await.map_err(
                |err| match err {
                    ExecutionError::CommandFailure(ref e) => {
                        if e.stdout.contains("Network is unreachable") {
                            BackoffError::transient(err_with_context(err))
                        } else {
                            BackoffError::permanent(err_with_context(err))
                        }
                    }
                    _ => BackoffError::permanent(err_with_context(err)),
                },
            )
        },
        |err, delay| {
            info!(
                log,
                "Cannot ensure there is a default route yet (retrying in {:?})",
                delay;
                "error" => #%err
            );
        },
    )
    .await
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
    Ipadm::create_opte_gateway(&interface).await.with_context(|| {
        format!("failed to create OPTE gateway on interface {interface}")
    })?;

    info!(
        log, "Ensuring there is a gateway route";
        "OPTE gateway" => ?gateway,
        "OPTE interface" => ?interface,
        "OPTE IP" => ?ip,
    );
    Route::ensure_opte_route(&gateway, &interface, &ip).await.with_context(
        || {
            format!(
                "failed to ensure OPTE gateway route on interface {interface} \
                 with gateway {gateway} and IP {ip}",
            )
        },
    )?;

    info!(
        log, "Ensuring there is a default route";
        "gateway" => ?gateway,
    );
    Route::ensure_default_route_with_gateway(Gateway::Ipv4(gateway))
        .await
        .with_context(|| {
            format!("failed to ensure default route via gateway {gateway}")
        })?;

    Ok(())
}
