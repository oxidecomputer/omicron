// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The rack connectivity test behind end-to-end-tests' `commtest` binary.
//!
//! [`run_cli`] is what the binary runs. [`run_cli_with`] lets a caller supply
//! the [`reqwest::ClientBuilder`] behind every HTTP client commtest makes, to
//! control how the API's name resolves ([`reqwest::ClientBuilder::resolve`],
//! [`reqwest::ClientBuilder::dns_resolver`]) or which certificates it trusts,
//! without relying on the host's resolver configuration.

mod icmp;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use icmp::ping4_test_run;
use oxide_client::{
    ClientExperimentalExt, ClientLoginExt, ClientProjectsExt,
    ClientSystemHardwareExt, ClientSystemIpPoolsExt, ClientSystemStatusExt,
    ClientVpcsExt,
    types::{
        IpPoolAssignment, IpPoolCreate, IpPoolLinkSilo, IpPoolType, IpRange,
        IpVersion, Ipv4Range, Ipv6Range, Name, NameOrId, PingStatus,
        PoolSelector, ProbeCreate, ProbeInfo, ProjectCreate,
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
#[clap(version, about, long_about = None)]
pub struct Cli {
    /// Oxide API address i.e., http://198.51.100.20
    pub oxide_api: String,

    /// How long to wait for the API to become available
    #[arg(long, default_value = "60m")]
    pub api_timeout: humantime::Duration,

    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Run(RunArgs),
    Cleanup,
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Test Duration
    #[arg(long, default_value = "100s")]
    pub test_duration: humantime::Duration,

    /// Test packet rate in packets per second
    #[arg(long, default_value_t = 10)]
    pub packet_rate: usize,

    /// How many lost ICMP packets may be tolerated
    #[arg(long, default_value_t = 0)]
    pub icmp_loss_tolerance: usize,

    /// First address in the IP pool to use for testing
    #[arg(long)]
    pub ip_pool_begin: IpAddr,

    /// Last address in the IP pool to use for testing
    #[arg(long)]
    pub ip_pool_end: IpAddr,
}

/// Produces the base [`reqwest::ClientBuilder`] for each HTTP client commtest
/// makes. Commtest layers its own settings (cookies, timeouts) on top.
pub type ClientBuilderFn = dyn Fn() -> reqwest::ClientBuilder + Send + Sync;

const API_RETRY_ATTEMPTS: usize = 15;

/// Try to construct an IP range from a pair of addresses.
///
/// An error is returned if the addresses aren't the same version, or first >
/// last.
// A copy of end-to-end-tests' helper, which this crate cannot depend on.
fn try_create_ip_range(first: IpAddr, last: IpAddr) -> Result<IpRange> {
    match (first, last) {
        (IpAddr::V4(first), IpAddr::V4(last)) => {
            anyhow::ensure!(
                first <= last,
                "IP range first address must be <= last"
            );
            Ok(IpRange::V4(Ipv4Range { first, last }))
        }
        (IpAddr::V6(first), IpAddr::V6(last)) => {
            anyhow::ensure!(
                first <= last,
                "IP range first address must be <= last"
            );
            Ok(IpRange::V6(Ipv6Range { first, last }))
        }
        (_, _) => anyhow::bail!(
            "Invalid IP addresses for IP range: {first} and {last}"
        ),
    }
}

/// Run `cli` with default HTTP clients, as the `commtest` binary does.
pub async fn run_cli(cli: &Cli) -> Result<()> {
    run_cli_with(cli, &reqwest::ClientBuilder::new).await
}

/// Run `cli`, building every HTTP client from `client_builder`.
pub async fn run_cli_with(
    cli: &Cli,
    client_builder: &ClientBuilderFn,
) -> Result<()> {
    match cli.command {
        Commands::Run(ref args) => run(cli, args, client_builder).await,
        Commands::Cleanup => cleanup(cli, client_builder).await,
    }
}

async fn run(
    cli: &Cli,
    args: &RunArgs,
    client_builder: &ClientBuilderFn,
) -> Result<()> {
    wait_until_oxide_api_is_available(cli, client_builder).await?;
    let (sleds, oxide) = rack_prepare(cli, args, client_builder).await?;
    let addrs = launch_probes(sleds, &oxide).await?;
    test_connectivity(args, addrs)?;
    Ok(())
}

async fn cleanup(cli: &Cli, client_builder: &ClientBuilderFn) -> Result<()> {
    wait_until_oxide_api_is_available(cli, client_builder).await?;
    let oxide = cleanup_probes(cli, client_builder).await?;
    rack_cleanup(&oxide).await?;
    Ok(())
}

async fn wait_until_oxide_api_is_available(
    cli: &Cli,
    client_builder: &ClientBuilderFn,
) -> Result<()> {
    // The timeouts `oxide_client::Client::new` applies.
    let oxide = oxide_client::Client::new_with_client(
        &cli.oxide_api,
        client_builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(15))
            .build()?,
    );
    let start = Instant::now();
    // Print why the API is not answering, but only when the reason changes,
    // so a slow start does not drown the output.
    let mut last_reason = String::new();
    loop {
        let reason = match oxide.ping().send().await {
            Ok(result) if result.status == PingStatus::Ok => {
                println!("the api is up");
                break;
            }
            Ok(result) => format!("ping status {:?}", result.status),
            Err(e) => {
                // A 4xx from the unauthenticated, argument-free ping cannot
                // change by waiting, so retrying until the deadline is futile.
                if let Some(status) = e.status()
                    && status.is_client_error()
                {
                    return Err(ping_rejected(&cli.oxide_api, status, &e));
                }
                describe(&e)
            }
        };
        if reason != last_reason {
            println!("api not ready: {reason}");
            last_reason = reason;
        }
        if Instant::now().duration_since(start)
            > Into::<Duration>::into(cli.api_timeout)
        {
            return Err(anyhow!(
                "{} deadline for system startup exceeded (last error: {})",
                cli.api_timeout,
                last_reason,
            ));
        }
        println!("no api response yet, wating 3s ...");
        sleep(Duration::from_secs(3)).await;
    }
    Ok(())
}

/// The error for a ping the API rejected outright. A request for an API
/// version newer than the server's is called out, since it means this
/// commtest was built from a newer omicron than the rack it is testing.
fn ping_rejected(
    api: &str,
    status: reqwest::StatusCode,
    e: &oxide_client::Error<oxide_client::types::Error>,
) -> anyhow::Error {
    let message = match e {
        oxide_client::Error::ErrorResponse(rv) => rv.message.clone(),
        _ => describe(e),
    };
    // Dropshot's versioning sends this as a bare 400 with no error code.
    if message.starts_with("server does not support this API version") {
        use oxide_client::ClientInfo;
        let version = oxide_client::Client::api_version();
        return anyhow!(
            "{api} rejected commtest's API version {version}: {message}. This \
             commtest's oxide-client is from a newer omicron than the rack's \
             Nexus; use a commtest built from the rack's omicron version (or \
             an older one)"
        );
    }
    anyhow!("{api} rejected the ping ({status}): {message}")
}

/// A short description of a failed API call: the status and the server's
/// message for an error response (whose `Display` also dumps headers, which
/// differ per response), the whole cause chain otherwise.
fn describe(e: &oxide_client::Error<oxide_client::types::Error>) -> String {
    match e {
        oxide_client::Error::ErrorResponse(rv) => {
            format!("{}: {}", rv.status(), rv.message)
        }
        _ => error_chain(e),
    }
}

/// An error and all its sources on one line. reqwest's own `Display` stops at
/// "error sending request", hiding the connect or TLS cause.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        // Some errors already include their source's text; skip repeats.
        let text = s.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = s.source();
    }
    out
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

async fn cleanup_probes(
    cli: &Cli,
    client_builder: &ClientBuilderFn,
) -> Result<oxide_client::Client> {
    let rqb = client_builder()
        .cookie_store(true)
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(15))
        .build()?;
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
    client_builder: &ClientBuilderFn,
) -> Result<(Vec<Uuid>, oxide_client::Client)> {
    let rqb = client_builder().cookie_store(true).build()?;

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
                    defaults: None,
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

async fn launch_probes(
    sleds: Vec<Uuid>,
    oxide: &oxide_client::Client,
) -> Result<Vec<Ipv4Addr>> {
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

fn test_connectivity(args: &RunArgs, addrs: Vec<Ipv4Addr>) -> Result<()> {
    let ttl = 255;
    println!("testing connectivity to probes");
    let report = ping4_test_run(
        &addrs,
        ttl,
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
