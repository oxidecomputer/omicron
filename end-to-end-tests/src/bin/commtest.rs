use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use end_to_end_tests::helpers::cli::oxide_cli_style;
use end_to_end_tests::helpers::icmp::ping4_test_run;
use oxide_client::{
    ClientExperimentalExt, ClientLoginExt, ClientProjectsExt,
    ClientSystemHardwareExt, ClientSystemIpPoolsExt, ClientSystemStatusExt,
    ClientVpcsExt,
    types::{
        IpPoolCreate, IpPoolLinkSilo, IpRange, Ipv4Range, Name, NameOrId,
        PingStatus, ProbeCreate, ProbeInfo, ProjectCreate,
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

    /// Test packet rate in packets per second
    #[arg(long, default_value_t = 10)]
    packet_rate: usize,

    /// How many lost ICMP packets may be tolerated
    #[arg(long, default_value_t = 0)]
    icmp_loss_tolerance: usize,

    /// First address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_begin: Ipv4Addr,

    /// Last address in the IP pool to use for testing
    #[arg(long)]
    ip_pool_end: Ipv4Addr,
}

const API_RETRY_ATTEMPTS: usize = 15;

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(ref args) => run(&cli, args).await,
        Commands::Cleanup => cleanup(&cli).await,
    }
}

async fn run(cli: &Cli, args: &RunArgs) -> Result<()> {
    wait_until_oxide_api_is_available(cli).await?;
    let (sleds, oxide) = rack_prepare(cli, args).await?;
    let addrs = launch_probes(sleds, &oxide).await?;
    test_connectivity(args, addrs)?;
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
    api_retry!(
        if let Err(e) = oxide.ip_pool_view().pool("default").send().await {
            if let Some(reqwest::StatusCode::NOT_FOUND) = e.status() {
                print!("default ip pool does not exist, creating ...");
                oxide
                    .ip_pool_create()
                    .body(IpPoolCreate {
                        name: pool_name.parse().unwrap(),
                        description: "Default IP pool".to_string(),
                    })
                    .send()
                    .await?;
                oxide
                    .ip_pool_silo_link()
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
        }
    )?;

    let pool = api_retry!(
        oxide
            .ip_pool_range_list()
            .limit(u32::MAX)
            .pool(Name::try_from("default").unwrap())
            .send()
            .await
    )?
    .into_inner()
    .items;

    let range = Ipv4Range { first: args.ip_pool_begin, last: args.ip_pool_end };

    let range_exists = pool
        .iter()
        .filter_map(|x| match &x.range {
            IpRange::V4(r) => Some(r),
            IpRange::V6(_) => None,
        })
        .any(|x| x.first == range.first && x.last == range.last);

    if !range_exists {
        print!("ip range does not exist, creating ... ");
        api_retry!(
            oxide
                .ip_pool_range_add()
                .pool(Name::try_from("default").unwrap())
                .body(IpRange::V4(range.clone()))
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
                        ip_pool: Some("default".parse().unwrap()),
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
