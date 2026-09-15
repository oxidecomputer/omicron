// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand};
use end_to_end_tests::helpers::icmp::{
    McastReport, mcast_ping4_test_run, ping4_test_run,
};
use end_to_end_tests::helpers::{cli::oxide_cli_style, try_create_ip_range};
use omicron_common::address::{IPV4_SSM_SUBNET, is_ssm_address};
use oxide_client::{
    ClientExperimentalExt, ClientLoginExt, ClientProjectsExt,
    ClientSystemHardwareExt, ClientSystemIpPoolsExt, ClientSystemStatusExt,
    ClientVpcsExt,
    types::{
        IpPoolAssignment, IpPoolCreate, IpPoolLinkSilo, IpPoolType, IpRange,
        IpVersion, MulticastGroupIdentifier, MulticastGroupJoinSpec,
        MulticastGroupMemberParentKind, Name, NameOrId, PingStatus,
        PoolSelector, ProbeCreate, ProbeInfo, ProjectCreate,
        UsernamePasswordCredentials,
    },
};
use serde::Serialize;
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};
use tokio::time::sleep;
use uuid::Uuid;

/// TTL for outbound ICMP echo requests.
///
/// Set to the maximum so probes remain reachable regardless of hop count.
/// The multicast phase depends on this in particular, since the kernel
/// default for `IP_MULTICAST_TTL` is 1 and would die at the first hop.
const PING_TTL: u32 = 255;

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
    /// converge before measurement begins. Defaults to no warmup.
    #[arg(long, default_value = "0s")]
    warmup: humantime::Duration,

    /// Test packet rate in packets per second
    #[arg(long, default_value_t = 10)]
    packet_rate: usize,

    /// How many lost ICMP packets may be tolerated
    #[arg(long, default_value_t = 0)]
    icmp_loss_tolerance: usize,

    /// Skip the unicast phase.
    ///
    /// Probes are still created, so the multicast phase can run against them.
    /// Only the unicast reachability check is skipped.
    #[arg(long, default_value_t = false)]
    skip_unicast: bool,

    /// Skip the multicast phase.
    ///
    /// Probes are still enrolled in the configured groups, so membership is
    /// established, but commtest does not source the verification stream.
    #[arg(long, default_value_t = false)]
    skip_mcast: bool,

    /// First address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_begin: IpAddr,

    /// Last address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_end: IpAddr,

    /// Multicast group to enroll probes into (with optional source filtering).
    ///
    /// Repeatable arg: pass once per group to exercise several groups in a
    /// single run. The value is `GROUP[@SOURCE,SOURCE,...]`. A bare `GROUP`
    /// joins as an any-source `(*, G)` member, while appending `@` and a
    /// comma-separated source list joins as a source-filtered `(S, G)` member
    /// permitting only those sources. The group must be IPv4 (commtest's
    /// verification path does not cover IPv6 yet), and for an
    /// `(S, G)` group each source must include whatever sends the verification
    /// pings or the data plane will (correctly) drop them. The source-specific
    /// range (`232.0.0.0/8`) requires at least one source. ASM groups may
    /// optionally supply sources too.
    ///
    /// When any group is present the run creates a multicast IP pool per
    /// SSM/ASM bucket the groups require, enrolls every probe into every
    /// group at probe-create time, and verifies data-plane delivery by
    /// pinging each group and confirming every member replies.
    ///
    /// Omit to skip the multicast phase.
    #[arg(
        long,
        value_parser = parse_mcast_group,
        value_name = "GROUP[@SRC,...]"
    )]
    mcast_group: Vec<McastGroup>,

    /// Multicast group whose source filter must drop this sender (negative
    /// test).
    ///
    /// Same `GROUP@SRC,...` syntax as `--mcast-group`, but the supplied sources
    /// are the permitted `(S, G)` set and must exclude whatever sends the
    /// verification pings. The run enrolls every probe as a member,
    /// pings the group, and asserts that no member receives any reply, proving
    /// the data plane drops traffic from a non-permitted source. Sources are
    /// required, meaning an any-source join would deliver and there would be
    /// nothing to deny. Repeatable.
    #[arg(
        long,
        value_parser = parse_mcast_deny_group,
        value_name = "GROUP@SRC,..."
    )]
    mcast_deny_group: Vec<McastGroup>,
}

/// A multicast group and the optional set of permitted sources for it.
///
/// An empty `sources` list is any-source `(*, G)` membership. A non-empty list
/// is source-filtered `(S, G)` membership permitting only those sources.
///
/// `deny` marks a negative test where the group is still enrolled and pinged,
/// but the permitted sources exclude this sender. The run asserts that the
/// dataplane drops the packets and no member replies.
#[derive(Debug, Clone)]
struct McastGroup {
    addr: IpAddr,
    sources: Vec<IpAddr>,
    deny: bool,
}

/// Parse a `--mcast-group` value of the form `GROUP[@SRC,SRC,...]`.
fn parse_mcast_group(s: &str) -> Result<McastGroup, String> {
    parse_mcast_spec(s, false)
}

/// Parse a `--mcast-deny-group` value of the form `GROUP@SRC,SRC,...`.
fn parse_mcast_deny_group(s: &str) -> Result<McastGroup, String> {
    parse_mcast_spec(s, true)
}

/// Parse a multicast group spec `GROUP[@SRC,SRC,...]`, tagging it `deny`.
///
/// The group must be a multicast address. Sources, when present, must match
/// the group's address family and be unique. IPv4-only enforcement lives in
/// `validate_mcast`, which rejects v6 groups across both arg lists. Invoked
/// by the clap value parsers so that a returned `Err` is surfaced as a parse
/// error.
fn parse_mcast_spec(s: &str, deny: bool) -> Result<McastGroup, String> {
    let (group_str, src_str) = match s.split_once('@') {
        Some((group_str, srcs)) => (group_str, Some(srcs)),
        None => (s, None),
    };

    let addr = group_str
        .trim()
        .parse::<IpAddr>()
        .map_err(|e| format!("invalid group address {group_str:?}: {e}"))?;
    if !addr.is_multicast() {
        return Err(format!("group address {addr} is not a multicast address"));
    }

    let sources = src_str
        .map(|srcs| {
            srcs.split(',')
                .map(|t| {
                    t.trim().parse::<IpAddr>().map_err(|e| {
                        format!("invalid source address {t:?}: {e}")
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    if let Some(bad) = sources.iter().find(|s| s.is_ipv4() != addr.is_ipv4()) {
        return Err(format!(
            "source {bad} address family does not match group {addr}"
        ));
    }
    // The API rejects requests carrying duplicate source IPs, so catch them
    // here rather than mid-run.
    if let Some(dup) = sources
        .iter()
        .enumerate()
        .find_map(|(i, s)| sources[..i].contains(s).then_some(s))
    {
        return Err(format!("duplicate source address {dup} for group {addr}"));
    }
    Ok(McastGroup { addr, sources, deny })
}

/// Multicast configuration extracted from the CLI args.
///
/// Present whenever at least one `--mcast-group` was supplied and absent
/// otherwise, in which case the multicast phase is skipped. Each entry carries
/// a group and its (possibly empty) source filter.
#[derive(Debug, Clone)]
struct McastConfig {
    groups: Vec<McastGroup>,
}

/// One multicast IP pool that must exist for a run: its name, the address
/// range covering the groups it backs, and those group addresses themselves.
struct McastPool {
    name: &'static str,
    begin: IpAddr,
    end: IpAddr,
    /// Group addresses the pool must cover, used to verify coverage when the
    /// pool already has ranges that differ from the requested one.
    addrs: Vec<IpAddr>,
}

impl McastConfig {
    /// The multicast IP pools required to back the configured groups.
    ///
    /// Nexus requires each multicast pool to be single-family and entirely SSM
    /// or entirely ASM, rejecting a range that spans the SSM boundary or mixes
    /// the two within a pool. ASM v4 occupies the multicast space on both sides
    /// of the 232/8 SSM block, so a single min..max over all ASM groups
    /// would cross 232/8 whenever groups sit on either side of it. Therefore,
    /// the groups are partitioned into up to three buckets: 1) ASM below the
    /// SSM block, 2) ASM above it, and 3) just SSM. Each non-empty bucket
    /// yields one pool whose range is the min..max span over the bucket's group
    /// addresses, which guarantees the pool contains every group it must back
    /// while staying entirely ASM or entirely SSM. Classification and the
    /// block bounds both come from the canonical `IPV4_SSM_SUBNET` (via
    /// `is_ssm_address`) to stay in lockstep with Nexus.
    fn pools(&self) -> Vec<McastPool> {
        let ssm_block_start = IpAddr::V4(IPV4_SSM_SUBNET.first_addr());
        let ssm_block_end = IpAddr::V4(IPV4_SSM_SUBNET.last_addr());
        [
            self.bucket(MCAST_POOL_V4_ASM_LOW, |ip| {
                ip.is_ipv4() && !is_ssm_address(ip) && ip < ssm_block_start
            }),
            self.bucket(MCAST_POOL_V4_ASM_HIGH, |ip| {
                ip.is_ipv4() && !is_ssm_address(ip) && ip > ssm_block_end
            }),
            self.bucket(MCAST_POOL_V4_SSM, |ip| {
                ip.is_ipv4() && is_ssm_address(ip)
            }),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Return the pool given a `name` covering every group address matching
    /// `want`, or `None` when no configured group matches. The range is the
    /// tight min..max span over the matching addresses.
    fn bucket(
        &self,
        name: &'static str,
        want: impl Fn(IpAddr) -> bool,
    ) -> Option<McastPool> {
        let addrs: Vec<IpAddr> = self
            .groups
            .iter()
            .map(|group| group.addr)
            .filter(|ip| want(*ip))
            .collect();
        let begin = *addrs.iter().min()?;
        let end = *addrs.iter().max()?;
        Some(McastPool { name, begin, end, addrs })
    }
}

impl RunArgs {
    /// Returns the multicast configuration if any group was supplied (delivery
    /// or deny), or `None` to skip the multicast phase.
    fn mcast(&self) -> Option<McastConfig> {
        if self.mcast_group.is_empty() && self.mcast_deny_group.is_empty() {
            return None;
        }
        let mut groups = self.mcast_group.clone();
        groups.extend(self.mcast_deny_group.iter().cloned());
        Some(McastConfig { groups })
    }

    /// Validate multicast group args after parsing.
    ///
    /// A `McastGroup` packs its address and sources into one value, so these
    /// rules depend on fields clap parses together and cannot enforce on its
    /// own. Checking here surfaces them as errors before any work begins:
    ///
    /// - The group must be IPv4.
    /// - A delivery group in the source-specific (SSM) range (`232.0.0.0/8`)
    ///   builds no shared `(*, G)` tree, so a join with no source is
    ///   undeliverable. Classification uses the canonical `is_ssm_address` to
    ///   stay in lockstep with Nexus.
    /// - A deny group needs at least one source: an any-source join would
    ///   deliver, leaving nothing for the negative test to deny.
    fn validate_mcast(&self) -> Result<(), String> {
        // TODO: enable IPv6 multicast:
        // - add the v6 pool buckets in `pools`
        // - split probe addresses and `McastConnectivityReport` by v4/v6
        // - add a v6 arm in `test_mcast_connectivity` running
        //   `mcast_ping6_test_run`
        for group in self.mcast_group.iter().chain(&self.mcast_deny_group) {
            if group.addr.is_ipv6() {
                return Err(format!(
                    "multicast group {} is IPv6; only IPv4 multicast is \
                     currently supported",
                    group.addr
                ));
            }
        }

        for group in &self.mcast_group {
            if is_ssm_address(group.addr) && group.sources.is_empty() {
                return Err(format!(
                    "multicast group {} is in the source-specific (SSM) range \
                     (232.0.0.0/8) but no sources were supplied; SSM groups \
                     require at least one source (use GROUP@SOURCE)",
                    group.addr
                ));
            }
        }

        for group in &self.mcast_deny_group {
            if group.sources.is_empty() {
                return Err(format!(
                    "multicast deny group {} has no sources; a source-filter \
                     denial test needs a permitted source set that excludes \
                     this sender (use GROUP@SOURCE)",
                    group.addr
                ));
            }
        }
        Ok(())
    }
}

const API_RETRY_ATTEMPTS: usize = 15;

/// Project owning every probe, VPC, and subnet commtest creates. Cleanup of
/// resources left by earlier runs looks the project up by this name, so it
/// must not change.
const PROJECT_NAME: &str = "classone";

fn main() -> Result<()> {
    oxide_tokio_rt::run(async {
        let cli = Cli::parse();

        match cli.command {
            Commands::Run(ref args) => {
                if let Err(msg) = args.validate_mcast() {
                    Cli::command()
                        .error(clap::error::ErrorKind::ValueValidation, msg)
                        .exit();
                }
                run(&cli, args).await
            }
            Commands::Cleanup => cleanup(&cli).await,
        }
    })
}

async fn run(cli: &Cli, args: &RunArgs) -> Result<()> {
    wait_until_oxide_api_is_available(cli).await?;
    let (sleds, oxide) = rack_prepare(cli, args).await?;

    let addrs = launch_probes(args, sleds, &oxide).await?;
    if !args.skip_unicast {
        test_connectivity(args, &addrs)?;
    }
    if !args.skip_mcast
        && let Some(cfg) = args.mcast()
    {
        test_mcast_connectivity(args, &cfg, &addrs)?;
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
            .project(Name::try_from(PROJECT_NAME).unwrap())
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
                .project(Name::try_from(PROJECT_NAME).unwrap())
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
        .project(Name::try_from(PROJECT_NAME).unwrap())
        .send()
        .await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("project does not exist");
        } else {
            Err(e)?;
        }
    } else {
        print!("deleting {PROJECT_NAME} subnet ... ");
        api_retry!(
            oxide
                .vpc_subnet_delete()
                .project(Name::try_from(PROJECT_NAME).unwrap())
                .vpc(Name::try_from("default").unwrap())
                .subnet(Name::try_from("default").unwrap())
                .send()
                .await
        )?;
        println!("done");

        print!("deleting {PROJECT_NAME} vpc ... ");
        api_retry!(
            oxide
                .vpc_delete()
                .project(Name::try_from(PROJECT_NAME).unwrap())
                .vpc(Name::try_from("default").unwrap())
                .send()
                .await
        )?;
        println!("done");

        print!("deleting {PROJECT_NAME} project ... ");
        api_retry!(
            oxide
                .project_delete()
                .project(Name::try_from(PROJECT_NAME).unwrap())
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
        .project(Name::try_from(PROJECT_NAME).unwrap())
        .send()
        .await
    {
        if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
            print!("project does not exist, creating ... ");
            oxide
                .project_create()
                .body(ProjectCreate {
                    description: "A project for probes".into(),
                    name: PROJECT_NAME.parse().unwrap(),
                })
                .send()
                .await?;
            println!("done");
            Ok(())
        } else {
            Err(e)
        }
    } else {
        println!("{PROJECT_NAME} project already exists");
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
                    assignment: IpPoolAssignment::Silos,
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
        // A multicast pool is single-family and entirely SSM or entirely ASM,
        // so create one pool per SSM/ASM bucket the groups require.
        for pool in cfg.pools() {
            ensure_mcast_pool(&oxide, &pool).await?;
        }
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

const MCAST_POOL_V4_ASM_LOW: &str = "mcast-v4-asm-low";
const MCAST_POOL_V4_ASM_HIGH: &str = "mcast-v4-asm-high";
const MCAST_POOL_V4_SSM: &str = "mcast-v4-ssm";

/// Whether `addr` falls within `range`, inclusive of both endpoints.
fn ip_range_contains(range: &IpRange, addr: IpAddr) -> bool {
    match (range, addr) {
        (IpRange::V4(r), IpAddr::V4(a)) => r.first <= a && a <= r.last,
        (IpRange::V6(r), IpAddr::V6(a)) => r.first <= a && a <= r.last,
        _ => false,
    }
}

/// Whether any range in `existing` fully contains `range`, and whether any
/// intersects it, as `(covered, overlaps)`.
///
/// Coverage, not equality: an existing pool range that contains the
/// requested one satisfies it. A partial overlap (a re-run against a
/// wider or shifted range) cannot be added because the API rejects
/// overlapping ranges, so the caller falls back to checking that the
/// existing ranges cover every group address the run needs.
fn range_coverage<'a>(
    range: &IpRange,
    existing: impl Iterator<Item = &'a IpRange>,
) -> (bool, bool) {
    existing.fold((false, false), |(covered, overlaps), pool_range| {
        match (range, pool_range) {
            (IpRange::V4(r1), IpRange::V4(r2)) => (
                covered || (r2.first <= r1.first && r1.last <= r2.last),
                overlaps || (r1.first <= r2.last && r2.first <= r1.last),
            ),
            (IpRange::V6(r1), IpRange::V6(r2)) => (
                covered || (r2.first <= r1.first && r1.last <= r2.last),
                overlaps || (r1.first <= r2.last && r2.first <= r1.last),
            ),
            (_, _) => (covered, overlaps),
        }
    })
}

async fn ensure_mcast_pool(
    oxide: &oxide_client::Client,
    pool: &McastPool,
) -> Result<()> {
    let (pool_name, range_begin, range_end) = (pool.name, pool.begin, pool.end);
    // The pool's own range defines its address family. Every group address
    // must fall within this range, so it is the authoritative version source.
    let ip_version =
        if range_begin.is_ipv4() { IpVersion::V4 } else { IpVersion::V6 };
    api_retry!(
        match oxide.system_ip_pool_view().pool(pool_name).send().await {
            Ok(_) => {
                println!("multicast ip pool {pool_name} already exists");
                Ok(())
            }
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                print!(
                    "multicast ip pool {pool_name} does not exist, \
                     creating ... "
                );
                oxide
                    .system_ip_pool_create()
                    .body(IpPoolCreate {
                        name: pool_name.parse().unwrap(),
                        description: "Multicast IP pool".to_string(),
                        ip_version,
                        pool_type: IpPoolType::Multicast,
                        assignment: IpPoolAssignment::Silos,
                    })
                    .send()
                    .await?;
                println!("done");
                Ok(())
            }
            Err(e) => Err(e),
        }
    )?;

    // The silo link is idempotent on re-runs: a 4xx from the API on the
    // re-link is treated as already-linked.
    match oxide
        .system_ip_pool_silo_link()
        .pool(pool_name)
        .body(IpPoolLinkSilo {
            silo: NameOrId::Name("recovery".parse().unwrap()),
            is_default: false,
        })
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) if e.status().is_some_and(|s| s.is_client_error()) => {
            println!("multicast pool already linked to silo");
        }
        Err(e) => Err(e)?,
    }

    let existing = api_retry!(
        oxide
            .system_ip_pool_range_list()
            .limit(u32::MAX)
            .pool(Name::try_from(pool_name).unwrap())
            .send()
            .await
    )?
    .into_inner()
    .items;

    let range = try_create_ip_range(range_begin, range_end)?;
    let (range_covered, range_overlaps) =
        range_coverage(&range, existing.iter().map(|pr| &pr.range));

    match (range_covered, range_overlaps) {
        (true, _) => println!("multicast ip range already exists"),
        (false, true) => {
            // What matters here is that every group address is allocatable
            // from the existing ranges, not the requested range's exact
            // bounds. For any address that is not, add a single-address
            // range. An uncovered address lies outside every existing
            // range by construction, so the add cannot collide with the
            // API's overlap rejection.
            let uncovered: Vec<IpAddr> = pool
                .addrs
                .iter()
                .copied()
                .filter(|addr| {
                    !existing
                        .iter()
                        .any(|pr| ip_range_contains(&pr.range, *addr))
                })
                .collect();
            if uncovered.is_empty() {
                println!(
                    "multicast ip range overlaps an existing pool range \
                     that covers every group address, using the existing \
                     ranges"
                );
            } else {
                print!(
                    "multicast ip ranges partially cover the requested \
                     groups, adding ranges for {uncovered:?} ... "
                );
                for addr in uncovered {
                    let single = try_create_ip_range(addr, addr)?;
                    api_retry!(
                        oxide
                            .system_ip_pool_range_add()
                            .pool(Name::try_from(pool_name).unwrap())
                            .body(single.clone())
                            .send()
                            .await
                    )?;
                }
                println!("done");
            }
        }
        (false, false) => {
            print!("multicast ip range does not exist, creating ... ");
            api_retry!(
                oxide
                    .system_ip_pool_range_add()
                    .pool(Name::try_from(pool_name).unwrap())
                    .body(range.clone())
                    .send()
                    .await
            )?;
            println!("done");
        }
    }
    Ok(())
}

async fn launch_probes(
    args: &RunArgs,
    sleds: Vec<Uuid>,
    oxide: &oxide_client::Client,
) -> Result<Vec<Ipv4Addr>> {
    // When multicast is configured, each group is passed as a group identifier
    // so the first probe creation auto-creates the groups out of the matching
    // multicast pool. Subsequent probes resolve to the same groups and attach
    // as new members.
    //
    // Every probe joins every configured group. The list is empty on a
    // unicast-only run, leaving probe creation unchanged.
    let multicast_groups: Vec<MulticastGroupJoinSpec> = args
        .mcast()
        .map(|cfg| {
            cfg.groups
                .iter()
                .map(|group| MulticastGroupJoinSpec {
                    group: MulticastGroupIdentifier(group.addr.to_string()),
                    ip_version: None,
                    // Empty sources is any-source `(*, G)`. A non-empty list is
                    // a source-filtered `(S, G)` join permitting only those
                    // sources.
                    source_ips: (!group.sources.is_empty())
                        .then(|| group.sources.clone()),
                })
                .collect()
        })
        .unwrap_or_default();

    // Probes are created sequentially so the first one drives the group
    // auto-create unambiguously, while later probes find the existing group
    // and add themselves as members.
    let managed: Vec<String> =
        (0..sleds.len()).map(|i| format!("probe{i}")).collect();
    for (i, sled) in sleds.into_iter().enumerate() {
        ensure_probe(oxide, i, sled, &multicast_groups).await?;
    }

    let probes = api_retry!(
        oxide
            .probe_list()
            .project(Name::try_from(PROJECT_NAME).unwrap())
            .limit(u32::MAX)
            .send()
            .await
    )?
    .into_inner()
    .items;

    // Collect the IPv4 external address of each probe this run ensured. A
    // probe replies to a multicast echo from its external address, and these
    // also serve as the unicast test target set. The listing covers the whole
    // project, so a probe someone else created there (manual testing shares
    // the project) is skipped rather than counted as an expected member that
    // never joined the groups and so never replies.
    //
    // TODO: the v4 filter is deliberate while the verification paths are
    // v4-only. Partition v4/v6 here when the v6 commtest bindings exist.
    let mut addrs = Vec::new();
    for probe in &probes {
        if !managed.contains(&probe.name) {
            println!(
                "ignoring foreign probe {} in project {PROJECT_NAME}",
                probe.name.as_str()
            );
            continue;
        }
        for ext in &probe.external_ips {
            if let IpAddr::V4(ip) = ext.ip {
                addrs.push(ip);
            }
        }
    }
    Ok(addrs)
}

/// Create `probe{i}` on `sled` enrolled in `groups` if it does not already
/// exist, retrying transient bring-up errors idempotently.
///
/// An existing probe is reused only when it already carries every desired
/// multicast membership. Memberships are settable only at probe creation, so
/// a probe left over from an earlier run without them (for example a
/// unicast-only run followed by a multicast one) cannot be joined in place
/// and is deleted and recreated instead. Reusing it as-is would leave every
/// group without members and fail the multicast phase with zero replies.
async fn ensure_probe(
    oxide: &oxide_client::Client,
    i: usize,
    sled: Uuid,
    groups: &[MulticastGroupJoinSpec],
) -> Result<()> {
    println!("checking if probe{i} exists");
    // Errors return into api_retry! rather than escaping it, keeping the
    // whole pass idempotent. Probe creation can transiently throw 500s during
    // bring-up, and a retry re-checks `probe_view` first, so a
    // committed-but-lost create is seen as already existing (and membership
    // complete) on the next attempt.
    api_retry!(ensure_probe_once(oxide, i, sled, groups).await)?;
    Ok(())
}

/// A single attempt of `ensure_probe`: look up the probe, verify its group
/// memberships, and delete and recreate it if they are incomplete.
async fn ensure_probe_once(
    oxide: &oxide_client::Client,
    i: usize,
    sled: Uuid,
    groups: &[MulticastGroupJoinSpec],
) -> Result<()> {
    match oxide
        .probe_view()
        .project(Name::try_from(PROJECT_NAME).unwrap())
        .probe(Name::try_from(format!("probe{i}")).unwrap())
        .send()
        .await
    {
        Ok(existing) => {
            if groups.is_empty()
                || probe_has_memberships(oxide, existing.id, groups).await?
            {
                println!("probe{i} already exists");
                return Ok(());
            }
            print!(
                "probe{i} exists without the desired multicast memberships, \
                 recreating ... "
            );
            oxide
                .probe_delete()
                .project(Name::try_from(PROJECT_NAME).unwrap())
                .probe(existing.id)
                .send()
                .await?;
            create_probe(oxide, i, sled, groups).await
        }
        Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
            print!("probe{i} does not exist, creating ... ");
            create_probe(oxide, i, sled, groups).await
        }
        Err(e) => Err(e.into()),
    }
}

/// Report whether `probe_id` is a member of every group in `groups`, with
/// each membership carrying exactly the desired source IP set.
///
/// A group that does not exist counts as a missing membership. Memberships
/// created with a probe implicitly create their groups, so a desired group
/// that is absent means the probe was created without it. Source IPs are
/// compared order-insensitively, and a diverging set (e.g. a config change
/// between runs) counts as missing so the caller recreates the probe.
async fn probe_has_memberships(
    oxide: &oxide_client::Client,
    probe_id: Uuid,
    groups: &[MulticastGroupJoinSpec],
) -> Result<bool> {
    for spec in groups {
        let members = match oxide
            .multicast_group_member_list()
            .multicast_group(spec.group.clone())
            .limit(u32::MAX)
            .send()
            .await
        {
            Ok(page) => page.into_inner().items,
            Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        };

        let Some(member) = members.iter().find(|m| {
            m.kind == MulticastGroupMemberParentKind::Probe
                && m.parent_id == probe_id
        }) else {
            return Ok(false);
        };

        let desired: BTreeSet<IpAddr> =
            spec.source_ips.iter().flatten().copied().collect();
        let actual: BTreeSet<IpAddr> =
            member.source_ips.iter().copied().collect();
        if desired != actual {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn create_probe(
    oxide: &oxide_client::Client,
    i: usize,
    sled: Uuid,
    groups: &[MulticastGroupJoinSpec],
) -> Result<()> {
    oxide
        .probe_create()
        .project(Name::try_from(PROJECT_NAME).unwrap())
        .body(ProbeCreate {
            description: format!("probe {i}"),
            pool_selector: PoolSelector::Explicit {
                pool: "default".parse().unwrap(),
            },
            name: format!("probe{i}").parse().unwrap(),
            sled,
            multicast_groups: groups.to_vec(),
        })
        .send()
        .await?;
    println!("done");
    Ok(())
}

fn test_connectivity(args: &RunArgs, addrs: &[Ipv4Addr]) -> Result<()> {
    let warmup: Duration = args.warmup.into();
    if !warmup.is_zero() {
        println!("warming up connectivity for {}", args.warmup);
        // Discard the warmup report. Its purpose is to let forwarding state
        // converge so the measured run is not charged for startup transients.
        let _ = ping4_test_run(addrs, PING_TTL, args.packet_rate, warmup);
    }
    println!("testing connectivity to probes");
    let report = ping4_test_run(
        addrs,
        PING_TTL,
        args.packet_rate,
        args.test_duration.into(),
    );

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

/// Expectation for what a group's dataplane result should be.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Expect {
    /// Every member must receive delivery (positive path).
    Deliver,
    /// No member may receive delivery, e.g. the group is source-filtered and
    /// the permitted sources exclude this sender, so the dataplane must drop
    /// the stream (negative testing path).
    Deny,
}

/// A multicast group's dataplane report tagged with its expectation, so
/// evaluation knows whether to require delivery or denial.
#[derive(Serialize)]
struct McastGroupReport<T> {
    expect: Expect,
    #[serde(flatten)]
    report: McastReport<T>,
}

/// The per-run report document holding every group's dataplane accounting,
/// serialized to `multicast-connectivity-report.json`.
#[derive(Serialize)]
struct McastConnectivityReport {
    v4: Vec<McastGroupReport<Ipv4Addr>>,
}

/// Verify every configured multicast group against its expectation: delivery
/// groups must reach every joined member, deny groups must reach none.
///
/// This mirrors unicast's `test_connectivity` but pings each multicast group
/// address rather than each probe directly. For a delivery group the probe
/// addresses are the expected member set, and each member must reply within
/// the loss tolerance. For a deny group the same stream runs and any reply is
/// a failure. Reports are written to the connectivity report before
/// evaluation, so a failing group's accounting is still persisted for
/// inspection.
fn test_mcast_connectivity(
    args: &RunArgs,
    cfg: &McastConfig,
    addrs: &[Ipv4Addr],
) -> Result<()> {
    let warmup: Duration = args.warmup.into();
    let mut reports = McastConnectivityReport { v4: Vec::new() };

    for group in &cfg.groups {
        let expect = if group.deny { Expect::Deny } else { Expect::Deliver };
        let IpAddr::V4(addr) = group.addr else {
            unreachable!("v6 multicast groups are rejected during validation");
        };

        if addrs.is_empty() {
            panic!(
                "multicast group {addr} has no IPv4 members to verify against"
            );
        }

        if !warmup.is_zero() {
            println!("warming up multicast for {addr} for {}", args.warmup);
            let _ = mcast_ping4_test_run(
                addr,
                addrs,
                PING_TTL,
                args.packet_rate,
                warmup,
            );
        }

        println!("{}", mcast_test_banner(expect, addr));
        let report = mcast_ping4_test_run(
            addr,
            addrs,
            PING_TTL,
            args.packet_rate,
            args.test_duration.into(),
        );
        reports.v4.push(McastGroupReport { expect, report });
    }

    let out = serde_json::to_string_pretty(&reports).unwrap();
    std::fs::write("multicast-connectivity-report.json", out.as_str()).unwrap();

    assert_eq!(
        reports.v4.len(),
        cfg.groups.len(),
        "expected one report per configured multicast group",
    );

    // The multicast reply travels back as unicast, so return-path transient
    // loss shares the unicast reliability and reuses the same tolerance.
    // Single-copy delivery (tx == rx) is enforced separately by the
    // over-delivery check.
    for group_report in &reports.v4 {
        verify_mcast(group_report, addrs, args.icmp_loss_tolerance);
    }
    Ok(())
}

/// The pre-test status line, phrased for the group's expectation.
fn mcast_test_banner<T: std::fmt::Display>(expect: Expect, group: T) -> String {
    match expect {
        Expect::Deliver => {
            format!("testing multicast data-plane delivery to group {group}")
        }
        Expect::Deny => format!(
            "testing multicast source-filter denial for group {group} \
             (expect no delivery)"
        ),
    }
}

/// Verify one group's report against its expectation.
fn verify_mcast<T: std::fmt::Display + PartialEq>(
    group_report: &McastGroupReport<T>,
    expected_members: &[T],
    loss_tolerance: usize,
) {
    match group_report.expect {
        Expect::Deliver => verify_mcast_report(
            &group_report.report,
            expected_members,
            loss_tolerance,
        ),
        Expect::Deny => verify_mcast_report_deny(&group_report.report),
    }
}

/// Panic if a multicast group's delivery does not match the expected member
/// set. Otherwise, this reports the group as verified.
///
/// 4 faults are checked:
/// 1) a member that never replied or lost more than the tolerance
///    (under-delivery),
/// 2) a reply from a responder outside the expected member set (a port that
///    never joined),
/// 3) a member receiving more replies than requests were sent to it
///    (over-delivery),
/// 4) the sender receiving a copy of its own request stream back from the
///    wire (sender echo).
///
/// The over-delivery check relies on the echo invariant: one request, at
/// most one reply. With single-copy delivery, `rx_count` stays at or below
/// `tx_count`. Every extra delivery path is another copy per member and
/// another echo. Both uplinks equates to double.
///
/// Path count is the upstream network's call. Ingress is active-active
/// (every switch carries every group's external entry) and delivering
/// toward both uplinks is a valid setup that could yield two copies.
///
/// The check here assumes single-copy delivery. The caller arranges that
/// upstream, however. Under-delivery gets the unicast loss window (replies come
/// back unicast). Over-delivery gets none. With one path, excess replies are a
/// replication fault.
///
/// TODO: let the caller pass the expected delivery count for multi-path
/// runs.
fn verify_mcast_report<T: std::fmt::Display + PartialEq>(
    report: &McastReport<T>,
    expected_members: &[T],
    loss_tolerance: usize,
) {
    let group = &report.group;

    for expected in expected_members {
        if !report.members.iter().any(|state| &state.member == expected) {
            panic!(
                "multicast group {group} is missing expected member {expected}",
            );
        }
    }

    for state in &report.members {
        if !expected_members.iter().any(|member| member == &state.member) {
            panic!(
                "multicast group {group} received {} reply(ies) from unexpected \
                 responder {}; only joined members should reply",
                state.rx_count, state.member,
            );
        }
        if state.rx_count == 0 {
            panic!(
                "multicast group {group} member {} returned no replies",
                state.member,
            );
        }
        if state.rx_count > state.tx_count {
            panic!(
                "multicast group {group} member {} received {} replies for {} \
                 requests; more replies than requests sent means the member got \
                 duplicate copies, so single-copy delivery failed",
                state.member, state.rx_count, state.tx_count,
            );
        }
        if state.lost > loss_tolerance {
            panic!(
                "multicast group {group} member {} has loss = {} packets which \
                 is greater than tolerance {loss_tolerance}",
                state.member, state.lost,
            );
        }
    }
    // Replication egress port lists are programmed by the control plane, and
    // the switch will replicate out whatever ports appear there, including
    // the sender's own ingress port. The sender never joins via the rack, so
    // receiving any copy of its own stream back means the fan-out included a
    // port it must exclude.
    if report.sender_self_rx != 0 {
        panic!(
            "multicast group {group}: sender received {} copy(ies) of its \
             own request stream back from the network; the dataplane must \
             not replicate a sender's group traffic toward its ingress port",
            report.sender_self_rx,
        );
    }
    println!(
        "multicast data-plane delivery verified for group {group}: {} member(s)",
        report.members.len(),
    );
}

/// We panic if any member of a source-filtered group received delivery.
///
/// Members join `(S, G)` with a permitted source set that excludes this sender,
/// so a correctly filtering dataplane drops every packet and no member replies.
/// A non-zero receive count means a packet from an unpermitted source leaked
/// through, which is the failure this negative test guards against.
///
/// Zero delivery only proves denial if traffic was actually offered, so this
/// first requires that some member was sent at least one packet.
///
/// Denial is inferred from a missing unicast reply, which travels the return
/// path rather than the replication path. A leak that coincides with a broken
/// return path therefore reads as zero delivery and passes this check.
fn verify_mcast_report_deny<T: std::fmt::Display>(report: &McastReport<T>) {
    let group = &report.group;
    let sent: u32 = report.members.iter().map(|state| state.tx_count).sum();

    if sent == 0 {
        panic!(
            "multicast group {group} had no packets sent to any member; cannot \
             conclude source-filter denial from zero delivery"
        );
    }

    for state in &report.members {
        if state.rx_count != 0 {
            panic!(
                "multicast group {group} member {} received {} packet(s) from an \
                 unpermitted source; source filtering failed to deny delivery",
                state.member, state.rx_count,
            );
        }
    }

    if report.sender_self_rx != 0 {
        panic!(
            "multicast group {group}: sender received {} copy(ies) of its \
             own request stream back from the network; a source-filtered \
             group must not replicate toward the sender's ingress port",
            report.sender_self_rx,
        );
    }
    println!(
        "multicast source-filter denial verified for group {group}: {} member(s) \
         received no delivery",
        report.members.len(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn parse_plain_group_is_any_source() {
        let group = parse_mcast_spec("224.1.2.3", false).unwrap();
        assert_eq!(group.addr, v4("224.1.2.3"));
        assert!(group.sources.is_empty());
        assert!(!group.deny);
    }

    #[test]
    fn parse_group_with_sources_and_whitespace() {
        let group = parse_mcast_spec(" 232.1.2.3 @ 10.0.0.1 , 10.0.0.2 ", true)
            .unwrap();
        assert_eq!(group.addr, v4("232.1.2.3"));
        assert_eq!(group.sources, vec![v4("10.0.0.1"), v4("10.0.0.2")]);
        assert!(group.deny);
    }

    #[test]
    fn parse_rejects_bad_group_bad_source_and_empty_source() {
        assert!(
            parse_mcast_spec("not-an-ip", false)
                .unwrap_err()
                .contains("invalid group address")
        );
        assert!(
            parse_mcast_spec("224.1.2.3@not-an-ip", false)
                .unwrap_err()
                .contains("invalid source address")
        );
        // A trailing `@` yields one empty source token, not an ASM group.
        assert!(
            parse_mcast_spec("224.1.2.3@", false)
                .unwrap_err()
                .contains("invalid source address")
        );
    }

    #[test]
    fn parse_rejects_family_mismatch() {
        let err = parse_mcast_spec("224.1.2.3@fd00::1", false).unwrap_err();
        assert!(err.contains("address family does not match"));
    }

    #[test]
    fn parse_rejects_non_multicast_group() {
        let err = parse_mcast_spec("10.0.0.1", false).unwrap_err();
        assert!(err.contains("not a multicast address"));
    }

    #[test]
    fn parse_rejects_duplicate_sources() {
        let err = parse_mcast_spec("224.1.2.3@10.0.0.1, 10.0.0.1", false)
            .unwrap_err();
        assert!(err.contains("duplicate source address"));
    }

    #[test]
    fn pools_partition_asm_around_ssm_block() {
        let config = McastConfig {
            groups: ["224.0.1.10", "231.9.9.9", "232.1.2.3", "239.0.0.5"]
                .iter()
                .map(|s| McastGroup {
                    addr: v4(s),
                    sources: vec![],
                    deny: false,
                })
                .collect(),
        };
        let pools = config.pools();
        let by_name: Vec<(&str, IpAddr, IpAddr)> =
            pools.iter().map(|p| (p.name, p.begin, p.end)).collect();
        assert_eq!(
            by_name,
            vec![
                (MCAST_POOL_V4_ASM_LOW, v4("224.0.1.10"), v4("231.9.9.9")),
                (MCAST_POOL_V4_ASM_HIGH, v4("239.0.0.5"), v4("239.0.0.5")),
                (MCAST_POOL_V4_SSM, v4("232.1.2.3"), v4("232.1.2.3")),
            ]
        );
    }

    #[test]
    fn pools_omit_empty_buckets() {
        let config = McastConfig {
            groups: vec![McastGroup {
                addr: v4("232.5.5.5"),
                sources: vec![v4("10.0.0.1")],
                deny: false,
            }],
        };
        let pools = config.pools();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].name, MCAST_POOL_V4_SSM);
        assert_eq!(pools[0].begin, v4("232.5.5.5"));
        assert_eq!(pools[0].end, v4("232.5.5.5"));
    }

    fn range(begin: &str, end: &str) -> IpRange {
        try_create_ip_range(begin.parse().unwrap(), end.parse().unwrap())
            .unwrap()
    }

    #[test]
    fn ip_range_contains_is_endpoint_inclusive() {
        let r = range("224.1.0.10", "224.1.0.20");
        assert!(ip_range_contains(&r, v4("224.1.0.10")));
        assert!(ip_range_contains(&r, v4("224.1.0.20")));
        assert!(ip_range_contains(&r, v4("224.1.0.15")));
        assert!(!ip_range_contains(&r, v4("224.1.0.9")));
        assert!(!ip_range_contains(&r, v4("224.1.0.21")));
        // A family mismatch never contains.
        assert!(!ip_range_contains(&r, "ff0e::1".parse().unwrap()));
    }

    #[test]
    fn range_coverage_partitions_cover_overlap_and_disjoint() {
        let requested = range("224.1.0.10", "224.1.0.20");
        let wider = range("224.1.0.0", "224.1.0.30");
        let shifted = range("224.1.0.15", "224.1.0.30");
        let disjoint = range("224.1.0.30", "224.1.0.40");
        let v6 = range("ff0e::1", "ff0e::10");

        assert_eq!(
            range_coverage(&requested, [&wider].into_iter()),
            (true, true)
        );
        // A shifted range intersects but does not contain, which is the
        // partial-coverage case ensure_mcast_pool falls back on.
        assert_eq!(
            range_coverage(&requested, [&shifted].into_iter()),
            (false, true)
        );
        assert_eq!(
            range_coverage(&requested, [&disjoint].into_iter()),
            (false, false)
        );
        // A v6 range never covers or overlaps a v4 request.
        assert_eq!(
            range_coverage(&requested, [&v6].into_iter()),
            (false, false)
        );
        // Flags aggregate across ranges rather than describing any single one.
        assert_eq!(
            range_coverage(
                &requested,
                [&disjoint, &shifted, &wider].into_iter()
            ),
            (true, true)
        );
        assert_eq!(range_coverage(&requested, [].into_iter()), (false, false));
    }
}
