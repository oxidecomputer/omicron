// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use end_to_end_tests::helpers::icmp::{mcast_ping4_test_run, ping4_test_run};
use end_to_end_tests::helpers::{cli::oxide_cli_style, try_create_ip_range};
use oxide_client::{
    ClientExperimentalExt, ClientLoginExt, ClientProjectsExt,
    ClientSystemHardwareExt, ClientSystemIpPoolsExt, ClientSystemStatusExt,
    ClientVpcsExt,
    types::{
        IpPoolCreate, IpPoolLinkSilo, IpPoolType, IpRange, IpVersion,
        MulticastGroupIdentifier, MulticastGroupJoinSpec, Name, NameOrId,
        PingStatus, PoolSelector, ProbeCreate, ProbeInfo, ProjectCreate,
        UsernamePasswordCredentials,
    },
};
use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None, styles = oxide_cli_style())]
struct Cli {
    /// Oxide API address i.e., http://198.51.100.20
    oxide_api: String,

    /// How long to wait for the API to become available
    #[arg(long, default_value = "60m")]
    api_timeout: humantime::Duration,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Run(RunArgs),
    Cleanup,
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// Test Duration
    #[arg(long, default_value = "100s")]
    test_duration: humantime::Duration,

    /// Warmup duration run before the measured test. Traffic is sent at the
    /// same rate but its results are discarded, letting forwarding state
    /// converge before measurement begins (analogous to a benchmark warmup
    /// phase). Defaults to no warmup.
    #[arg(long, default_value = "0s")]
    warmup: humantime::Duration,

    /// Test packet rate in packets per second
    #[arg(long, default_value_t = 10)]
    packet_rate: usize,

    /// How many lost ICMP packets may be tolerated
    #[arg(long, default_value_t = 0)]
    icmp_loss_tolerance: usize,

    /// First address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_begin: IpAddr,

    /// Last address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_end: IpAddr,

    /// Multicast group address to enroll probes into. When present, the
    /// run additionally creates a multicast IP pool covering this address
    /// (see `--mcast-pool-{begin,end}`), enrolls each probe at
    /// `probe_create` time, and verifies multicast data-plane delivery by
    /// pinging the group and confirming each member replies. Omit to skip
    /// the multicast phase.
    #[arg(long, requires_all = ["mcast_pool_begin", "mcast_pool_end"])]
    mcast_group_ip: Option<IpAddr>,

    /// First address in the multicast IP pool range covering
    /// `--mcast-group-ip`.
    #[arg(long, requires_all = ["mcast_group_ip", "mcast_pool_end"])]
    mcast_pool_begin: Option<IpAddr>,

    /// Last address in the multicast IP pool range covering
    /// `--mcast-group-ip`.
    #[arg(long, requires_all = ["mcast_group_ip", "mcast_pool_begin"])]
    mcast_pool_end: Option<IpAddr>,
}

/// Multicast configuration extracted from the CLI args.
///
/// Either all three fields are present (when `--mcast-group-ip` and the
/// two pool-range args are supplied) or the whole struct is absent.
/// The all-or-nothing invariant is enforced at clap parse time via
/// `requires_all` on the arg attributes.
#[derive(Debug, Clone, Copy)]
struct McastConfig {
    group_ip: IpAddr,
    pool_begin: IpAddr,
    pool_end: IpAddr,
}

impl RunArgs {
    /// Returns the multicast configuration if `--mcast-group-ip` et al.
    /// were supplied, or `None` to skip the multicast phase.
    fn mcast(&self) -> Option<McastConfig> {
        let group_ip = self.mcast_group_ip?;
        let pool_begin = self.mcast_pool_begin?;
        let pool_end = self.mcast_pool_end?;
        Some(McastConfig { group_ip, pool_begin, pool_end })
    }
}

const API_RETRY_ATTEMPTS: usize = 15;

fn main() -> Result<()> {
    oxide_tokio_rt::run(async {
        let cli = Cli::parse();
        match cli.command {
            Commands::Run(ref args) => run(&cli, args).await,
            Commands::Cleanup => cleanup(&cli).await,
        }
    })
}

async fn run(cli: &Cli, args: &RunArgs) -> Result<()> {
    wait_until_oxide_api_is_available(cli).await?;
    let (sleds, oxide) = rack_prepare(cli, args).await?;
    let addrs = launch_probes(args, sleds, &oxide).await?;
    test_connectivity(args, &addrs)?;
    if let Some(cfg) = args.mcast() {
        test_mcast_connectivity(args, cfg, &addrs)?;
    }
    Ok(())
}

async fn cleanup(cli: &Cli) -> Result<()> {
    wait_until_oxide_api_is_available(cli).await?;
    let oxide = cleanup_probes(cli).await?;
    rack_cleanup(&oxide).await?;
    Ok(())
}

async fn wait_until_oxide_api_is_available(cli: &Cli) -> Result<()> {
    let oxide = oxide_client::Client::new(&cli.oxide_api);
    let start = Instant::now();
    loop {
        if let Ok(result) = oxide.ping().send().await.map(|x| x.into_inner()) {
            if result.status == PingStatus::Ok {
                println!("the api is up");
                break;
            }
        }
        if Instant::now().duration_since(start)
            > Into::<Duration>::into(cli.api_timeout)
        {
            return Err(anyhow!(
                "{} deadline for system startup exceeded",
                cli.api_timeout
            ));
        }
        println!("no api response yet, wating 3s ...");
        sleep(Duration::from_secs(3)).await;
    }
    Ok(())
}

macro_rules! api_retry {
    ($call:expr) => {{
        let mut limit = API_RETRY_ATTEMPTS;
        loop {
            match $call {
                res @ Ok(_) => break res,
                Err(e) => {
                    limit -= 1;
                    if limit == 0 {
                        break Err(e);
                    }
                    println!("API call error: {e}, retrying in 3 s");
                    sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }};
}

async fn cleanup_probes(cli: &Cli) -> Result<oxide_client::Client> {
    let rqb = reqwest::ClientBuilder::new()
        .cookie_store(true)
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let oxide = oxide_client::Client::new_with_client(&cli.oxide_api, rqb);

    print!("logging in ... ");
    api_retry!(
        oxide
            .login_local()
            .silo_name(Name::try_from("recovery").unwrap())
            .body(UsernamePasswordCredentials {
                password: "oxide".parse().unwrap(),
                username: "recovery".parse().unwrap(),
            })
            .send()
            .await
    )?;
    println!("done");

    let probes: Vec<ProbeInfo> = api_retry!(
        oxide
            .probe_list()
            .project(Name::try_from("classone").unwrap())
            .limit(u32::MAX)
            .send()
            .await
    )?
    .into_inner()
    .items;

    for probe in &probes {
        print!("deleting probe {} ... ", *probe.name);
        api_retry!(
            oxide
                .probe_delete()
                .project(Name::try_from("classone").unwrap())
                .probe(probe.id)
                .send()
                .await
        )?;
        println!("done");
    }

    Ok(oxide)
}

async fn rack_cleanup(oxide: &oxide_client::Client) -> Result<()> {
    if let Err(e) = oxide
        .project_view()
        .project(Name::try_from("classone").unwrap())
        .send()
        .await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("project does not exist");
        } else {
            Err(e)?;
        }
    } else {
        print!("deleting classone subnet ... ");
        api_retry!(
            oxide
                .vpc_subnet_delete()
                .project(Name::try_from("classone").unwrap())
                .vpc(Name::try_from("default").unwrap())
                .subnet(Name::try_from("default").unwrap())
                .send()
                .await
        )?;
        println!("done");

        print!("deleting classone vpc ... ");
        api_retry!(
            oxide
                .vpc_delete()
                .project(Name::try_from("classone").unwrap())
                .vpc(Name::try_from("default").unwrap())
                .send()
                .await
        )?;
        println!("done");

        print!("deleting classone project ... ");
        api_retry!(
            oxide
                .project_delete()
                .project(Name::try_from("classone").unwrap())
                .send()
                .await
        )?;
        println!("done");
    }
    Ok(())
}

async fn rack_prepare(
    cli: &Cli,
    args: &RunArgs,
) -> Result<(Vec<Uuid>, oxide_client::Client)> {
    let rqb = reqwest::ClientBuilder::new().cookie_store(true).build().unwrap();

    let oxide = oxide_client::Client::new_with_client(&cli.oxide_api, rqb);

    print!("logging in ... ");
    api_retry!(
        oxide
            .login_local()
            .silo_name(Name::try_from("recovery").unwrap())
            .body(UsernamePasswordCredentials {
                password: "oxide".parse().unwrap(),
                username: "recovery".parse().unwrap(),
            })
            .send()
            .await
    )?;
    println!("done");

    api_retry!(if let Err(e) = oxide
        .project_view()
        .project(Name::try_from("classone").unwrap())
        .send()
        .await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("project does not exist, creating ... ");
            oxide
                .project_create()
                .body(ProjectCreate {
                    description: "A project for probes".into(),
                    name: "classone".parse().unwrap(),
                })
                .send()
                .await?;
            println!("done");
            Ok(())
        } else {
            Err(e)
        }
    } else {
        println!("classone project already exists");
        Ok(())
    })?;

    let pool_name = "default";
    api_retry!(if let Err(e) =
        oxide.system_ip_pool_view().pool("default").send().await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("default ip pool does not exist, creating ...");
            let ip_version = if args.ip_pool_begin.is_ipv4() {
                IpVersion::V4
            } else {
                IpVersion::V6
            };
            oxide
                .system_ip_pool_create()
                .body(IpPoolCreate {
                    name: pool_name.parse().unwrap(),
                    description: "Default IP pool".to_string(),
                    ip_version,
                    pool_type: IpPoolType::Unicast,
                })
                .send()
                .await?;
            oxide
                .system_ip_pool_silo_link()
                .pool(pool_name)
                .body(IpPoolLinkSilo {
                    silo: NameOrId::Name("recovery".parse().unwrap()),
                    is_default: true,
                })
                .send()
                .await?;
            println!("done");
            Ok(())
        } else {
            Err(e)
        }
    } else {
        println!("default ip pool already exists");
        Ok(())
    })?;

    let pool = api_retry!(
        oxide
            .system_ip_pool_range_list()
            .limit(u32::MAX)
            .pool(Name::try_from("default").unwrap())
            .send()
            .await
    )?
    .into_inner()
    .items;

    let range = try_create_ip_range(args.ip_pool_begin, args.ip_pool_end)?;
    let range_exists =
        pool.iter().any(|pool_range| match (&range, &pool_range.range) {
            (IpRange::V4(r1), IpRange::V4(r2)) => {
                r1.first == r2.first && r1.last == r2.last
            }
            (IpRange::V6(r1), IpRange::V6(r2)) => {
                r1.first == r2.first && r1.last == r2.last
            }
            (_, _) => false,
        });

    if !range_exists {
        print!("ip range does not exist, creating ... ");
        api_retry!(
            oxide
                .system_ip_pool_range_add()
                .pool(Name::try_from("default").unwrap())
                .body(range.clone())
                .send()
                .await
        )?;
        println!("done");
    } else {
        println!("ip range already exists");
    }

    if let Some(cfg) = args.mcast() {
        ensure_mcast_pool(&oxide, cfg.group_ip, cfg.pool_begin, cfg.pool_end)
            .await?;
    }

    print!("getting sled ids ... ");
    let sleds = api_retry!(oxide.sled_list().limit(u32::MAX).send().await)?
        .into_inner()
        .items
        .iter()
        .map(|x| x.id)
        .collect();
    println!("done");

    Ok((sleds, oxide))
}

const MCAST_POOL_NAME: &str = "mcast";

async fn ensure_mcast_pool(
    oxide: &oxide_client::Client,
    group_ip: IpAddr,
    range_begin: IpAddr,
    range_end: IpAddr,
) -> Result<()> {
    let ip_version =
        if group_ip.is_ipv4() { IpVersion::V4 } else { IpVersion::V6 };
    api_retry!(if let Err(e) =
        oxide.system_ip_pool_view().pool(MCAST_POOL_NAME).send().await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("multicast ip pool does not exist, creating ... ");
            oxide
                .system_ip_pool_create()
                .body(IpPoolCreate {
                    name: MCAST_POOL_NAME.parse().unwrap(),
                    description: "Multicast IP pool".to_string(),
                    ip_version,
                    pool_type: IpPoolType::Multicast,
                })
                .send()
                .await?;
            println!("done");
            Ok(())
        } else {
            Err(e)
        }
    } else {
        println!("multicast ip pool already exists");
        Ok(())
    })?;

    // The silo link is idempotent on re-runs: a 4xx from the API on the
    // re-link is treated as already-linked.
    if let Err(e) = oxide
        .system_ip_pool_silo_link()
        .pool(MCAST_POOL_NAME)
        .body(IpPoolLinkSilo {
            silo: NameOrId::Name("recovery".parse().unwrap()),
            is_default: false,
        })
        .send()
        .await
    {
        match e.status() {
            Some(s) if s.is_client_error() => {
                println!("multicast pool already linked to silo");
            }
            _ => Err(e)?,
        }
    }

    let pool = api_retry!(
        oxide
            .system_ip_pool_range_list()
            .limit(u32::MAX)
            .pool(Name::try_from(MCAST_POOL_NAME).unwrap())
            .send()
            .await
    )?
    .into_inner()
    .items;

    let range = try_create_ip_range(range_begin, range_end)?;
    let range_exists =
        pool.iter().any(|pool_range| match (&range, &pool_range.range) {
            (IpRange::V4(r1), IpRange::V4(r2)) => {
                r1.first == r2.first && r1.last == r2.last
            }
            (IpRange::V6(r1), IpRange::V6(r2)) => {
                r1.first == r2.first && r1.last == r2.last
            }
            (_, _) => false,
        });
    if !range_exists {
        print!("multicast ip range does not exist, creating ... ");
        api_retry!(
            oxide
                .system_ip_pool_range_add()
                .pool(Name::try_from(MCAST_POOL_NAME).unwrap())
                .body(range.clone())
                .send()
                .await
        )?;
        println!("done");
    } else {
        println!("multicast ip range already exists");
    }
    Ok(())
}

async fn launch_probes(
    args: &RunArgs,
    sleds: Vec<Uuid>,
    oxide: &oxide_client::Client,
) -> Result<Vec<Ipv4Addr>> {
    // When multicast is configured, the group IP is passed as the group
    // identifier so the first probe creation auto-creates the group out
    // of the multicast pool. Subsequent probes resolve to the same
    // group and attach as new members.
    //
    // Note: This is empty when running unicast-only.
    let multicast_groups: Vec<MulticastGroupJoinSpec> = args
        .mcast()
        .map(|cfg| {
            vec![MulticastGroupJoinSpec {
                group: MulticastGroupIdentifier(cfg.group_ip.to_string()),
                ip_version: None,
                source_ips: None,
            }]
        })
        .unwrap_or_default();
    // Probes are created sequentially so the first one drives the group
    // auto-create unambiguously, while later probes find the existing group
    // and add themselves as members.
    for (i, sled) in sleds.into_iter().enumerate() {
        println!("checking if probe{i} exists");
        api_retry!(if let Err(e) = oxide
            .probe_view()
            .project(Name::try_from("classone").unwrap())
            .probe(Name::try_from(format!("probe{i}")).unwrap())
            .send()
            .await
        {
            if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
                print!("probe{i} does not exist, creating ... ");
                oxide
                    .probe_create()
                    .project(Name::try_from("classone").unwrap())
                    .body(ProbeCreate {
                        description: format!("probe {i}"),
                        pool_selector: PoolSelector::Explicit {
                            pool: "default".parse().unwrap(),
                        },
                        name: format!("probe{i}").parse().unwrap(),
                        sled,
                        multicast_groups: multicast_groups.clone(),
                    })
                    .send()
                    .await?;
                println!("done");
                Ok(())
            } else {
                Err(e)
            }
        } else {
            println!("probe{i} already exists");
            Ok(())
        })?;
    }

    Ok(api_retry!(
        oxide
            .probe_list()
            .project(Name::try_from("classone").unwrap())
            .limit(u32::MAX)
            .send()
            .await
    )?
    .into_inner()
    .items
    .iter()
    .map(|x| x.external_ips.get(0).unwrap().ip)
    .filter_map(|x| match x {
        IpAddr::V4(ip) => Some(ip),
        IpAddr::V6(_) => None,
    })
    .collect())
}

fn test_connectivity(args: &RunArgs, addrs: &[Ipv4Addr]) -> Result<()> {
    let ttl = 255;
    let warmup: Duration = args.warmup.into();
    if !warmup.is_zero() {
        println!("warming up connectivity for {}", args.warmup);
        // Discard the warmup report. Its purpose is to let forwarding state
        // converge so the measured run is not charged for startup transients.
        let _ = ping4_test_run(addrs, ttl, args.packet_rate, warmup);
    }
    println!("testing connectivity to probes");
    let report =
        ping4_test_run(addrs, ttl, args.packet_rate, args.test_duration.into());

    let out = serde_json::to_string_pretty(&report).unwrap();
    std::fs::write("connectivity-report.json", out.as_str()).unwrap();

    for state in report.v4.iter() {
        if state.lost > args.icmp_loss_tolerance {
            panic!(
                "{} has loss = {} packets which is greater than tolerance {}",
                state.dest, state.lost, args.icmp_loss_tolerance,
            );
        }
        if state.rx_count == 0 {
            panic!("received no responses from {}", state.dest);
        }
    }
    println!("all connectivity tests within loss tolerance");
    Ok(())
}

/// Verify multicast data-plane delivery to the configured group.
///
/// Mirrors [`test_connectivity`] but pings the multicast group address rather
/// than each probe directly. Every joined member is expected to reply, so the
/// probe addresses are passed as the expected member set and each must produce
/// replies within the loss tolerance.
fn test_mcast_connectivity(
    args: &RunArgs,
    cfg: McastConfig,
    members: &[Ipv4Addr],
) -> Result<()> {
    let group = match cfg.group_ip {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(_) => {
            println!("multicast group is IPv6, skipping IPv4 data-plane test");
            return Ok(());
        }
    };

    let ttl = 255;
    let warmup: Duration = args.warmup.into();
    if !warmup.is_zero() {
        println!("warming up multicast delivery for {}", args.warmup);
        let _ =
            mcast_ping4_test_run(group, members, ttl, args.packet_rate, warmup);
    }
    println!("testing multicast data-plane delivery to group {group}");
    let report = mcast_ping4_test_run(
        group,
        members,
        ttl,
        args.packet_rate,
        args.test_duration.into(),
    );

    let out = serde_json::to_string_pretty(&report).unwrap();
    std::fs::write("multicast-connectivity-report.json", out.as_str()).unwrap();

    for state in report.members.iter() {
        if state.rx_count == 0 {
            panic!(
                "multicast group {group} member {} returned no replies",
                state.dest,
            );
        }
        if state.lost > args.icmp_loss_tolerance {
            panic!(
                "multicast member {} has loss = {} packets which is greater \
                 than tolerance {}",
                state.dest, state.lost, args.icmp_loss_tolerance,
            );
        }
    }
    println!(
        "multicast data-plane delivery verified for {} member(s)",
        report.members.len(),
    );
    Ok(())
}
