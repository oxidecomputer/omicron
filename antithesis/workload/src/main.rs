// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: Antithesis workload for the simulated Omicron control plane; see antithesis/README.adoc.

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
// TODO-RAINCLAUDE: linking the instrumentation crate is what lets an Antithesis build load libvoidstar for coverage.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;
use clap::Parser;
use clap::Subcommand;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use futures::TryStreamExt;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use nexus_lockstep_client::types::SagaState;
use oxide_client::ClientProjectsExt;
use oxide_client::ClientSystemHardwareExt;
use oxide_client::ClientSystemIpPoolsExt;
use oxide_client::ClientSystemStatusExt;
use serde_json::json;
use slog::Logger;
use slog::info;
use slog::warn;
use std::net::Ipv4Addr;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_NEXUS_EXTERNAL_URL: &str = "http://[fd00:1122:3344:101::5]:12220";
const DEFAULT_NEXUS_LOCKSTEP_URL: &str = "http://[fd00:1122:3344:101::5]:12232";
const DEFAULT_SILO: &str = "demo-silo";
const DEFAULT_USERNAME: &str = "demo-privileged";
const DEFAULT_PASSWORD: &str = "oxide";
const SEED_PROJECT_NAME: &str = "antithesis";
const SEED_IP_POOL_NAME: &str = "antithesis";
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Parser)]
#[clap(name = "omicron-antithesis-workload")]
struct Args {
    #[clap(long, default_value = DEFAULT_NEXUS_EXTERNAL_URL)]
    nexus_external_url: String,

    #[clap(long, default_value = DEFAULT_NEXUS_LOCKSTEP_URL)]
    nexus_lockstep_url: String,

    #[clap(long, default_value = DEFAULT_SILO)]
    silo: String,

    #[clap(long, default_value = DEFAULT_USERNAME)]
    username: String,

    #[clap(long, default_value = DEFAULT_PASSWORD)]
    password: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // TODO-RAINCLAUDE: waits for Nexus's external API, a working login, and a registered sled, then emits the Antithesis setup_complete signal.
    WaitReady {
        #[clap(long, default_value = "600")]
        timeout_secs: u64,
    },
    // TODO-RAINCLAUDE: creates the project and IP pool the drivers use; safe to run repeatedly.
    Seed,
    // TODO-RAINCLAUDE: waits until no saga is running and asserts that it happened within the timeout.
    SagasSettle {
        #[clap(long, default_value = "300")]
        timeout_secs: u64,
    },
}

fn main() {
    antithesis_sdk::antithesis_init();
    if let Err(error) = oxide_tokio_rt::run(do_run()) {
        eprintln!("omicron-antithesis-workload: {error:#}");
        std::process::exit(1);
    }
}

async fn do_run() -> anyhow::Result<()> {
    let args = Args::parse();
    let log = ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info }
        .to_logger("omicron-antithesis-workload")
        .context("initializing logger")?;

    match args.command {
        Command::WaitReady { timeout_secs } => {
            wait_ready(&log, &args, Duration::from_secs(timeout_secs)).await
        }
        Command::Seed => seed(&log, &args).await,
        Command::SagasSettle { timeout_secs } => {
            sagas_settle(&log, &args, Duration::from_secs(timeout_secs)).await
        }
    }
}

fn reqwest_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(60))
}

async fn login(
    log: &Logger,
    args: &Args,
) -> anyhow::Result<oxide_client::Client> {
    let login_url =
        format!("{}/v1/login/{}/local", args.nexus_external_url, args.silo);
    let username: oxide_client::types::UserId =
        args.username.parse().map_err(|error| {
            anyhow!(
                "username {:?} is not a valid user id: {error}",
                args.username
            )
        })?;
    let password: oxide_client::types::Password =
        args.password.parse().map_err(|error| {
            anyhow!("password is not a valid password: {error}")
        })?;

    let session_token = oxide_client::login(
        reqwest_builder(),
        &login_url,
        username,
        password,
    )
    .await
    .with_context(|| {
        format!(
            "logging into silo {:?} at {login_url} as {:?}; check that the \
             recovery silo and user passed to sled-agent-sim match",
            args.silo, args.username
        )
    })?;
    info!(log, "logged in"; "silo" => &args.silo, "user" => &args.username);

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::COOKIE,
        HeaderValue::from_str(&format!("session={session_token}"))
            .context("session token is not a valid header value")?,
    );
    let client = reqwest_builder()
        .default_headers(headers)
        .build()
        .context("building HTTP client")?;
    Ok(oxide_client::Client::new_with_client(&args.nexus_external_url, client))
}

async fn wait_ready(
    log: &Logger,
    args: &Args,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let unauthenticated = oxide_client::Client::new(&args.nexus_external_url);

    loop {
        match unauthenticated.ping().send().await {
            Ok(_) => break,
            Err(error) => {
                warn!(log, "Nexus external API not ready"; "error" => %error);
            }
        }
        wait_or_timeout(deadline, "Nexus external API to answer /v1/ping")
            .await?;
    }
    info!(log, "Nexus external API is up");

    let client = loop {
        match login(log, args).await {
            Ok(client) => break client,
            Err(error) => {
                warn!(log, "login not ready"; "error" => format!("{error:#}"));
            }
        }
        wait_or_timeout(deadline, "login to succeed").await?;
    };

    let sleds = loop {
        match client.sled_list().limit(1).send().await {
            Ok(page) if !page.items.is_empty() => break page.items.len(),
            Ok(_) => {
                warn!(log, "no sleds registered yet");
            }
            Err(error) => {
                warn!(log, "listing sleds failed"; "error" => %error);
            }
        }
        wait_or_timeout(deadline, "a sled to be registered").await?;
    };
    info!(log, "sled registered; system is ready");

    let details = json!({
        "nexus_external_url": args.nexus_external_url,
        "silo": args.silo,
        "sleds": sleds,
    });
    antithesis_sdk::lifecycle::setup_complete(&details);
    info!(log, "sent setup_complete");
    Ok(())
}

async fn wait_or_timeout(deadline: Instant, what: &str) -> anyhow::Result<()> {
    if Instant::now() >= deadline {
        bail!(
            "timed out waiting for {what}; Nexus or sled-agent-sim did not \
             come up (check their logs and the addresses in \
             antithesis/config/docker-compose.yaml)"
        );
    }
    tokio::time::sleep(POLL_INTERVAL).await;
    Ok(())
}

fn is_conflict<E: std::fmt::Debug>(error: &oxide_client::Error<E>) -> bool {
    match error {
        oxide_client::Error::ErrorResponse(response) => {
            response.status() == StatusCode::CONFLICT
        }
        _ => false,
    }
}

async fn seed(log: &Logger, args: &Args) -> anyhow::Result<()> {
    let client = login(log, args).await?;

    match client
        .project_create()
        .body(oxide_client::types::ProjectCreate {
            name: SEED_PROJECT_NAME.parse().unwrap(),
            description: String::from("Antithesis workload project"),
        })
        .send()
        .await
    {
        Ok(_) => info!(log, "created project"; "name" => SEED_PROJECT_NAME),
        Err(error) if is_conflict(&error) => {
            info!(log, "project already exists"; "name" => SEED_PROJECT_NAME)
        }
        Err(error) => {
            return Err(anyhow!(error)).context("creating project");
        }
    }

    match client
        .system_ip_pool_create()
        .body(oxide_client::types::IpPoolCreate {
            name: SEED_IP_POOL_NAME.parse().unwrap(),
            description: String::from("Antithesis workload IP pool"),
            ip_version: oxide_client::types::IpVersion::V4,
            pool_type: oxide_client::types::IpPoolType::Unicast,
            assignment: oxide_client::types::IpPoolAssignment::Silos,
        })
        .send()
        .await
    {
        Ok(_) => info!(log, "created IP pool"; "name" => SEED_IP_POOL_NAME),
        Err(error) if is_conflict(&error) => {
            info!(log, "IP pool already exists"; "name" => SEED_IP_POOL_NAME)
        }
        Err(error) => {
            return Err(anyhow!(error)).context("creating IP pool");
        }
    }

    // TODO-RAINCLAUDE: TEST-NET-2 (RFC 5737) so the range can never collide with anything real.
    let range =
        oxide_client::types::IpRange::V4(oxide_client::types::Ipv4Range {
            first: Ipv4Addr::new(198, 51, 100, 1),
            last: Ipv4Addr::new(198, 51, 100, 254),
        });
    match client
        .system_ip_pool_range_add()
        .pool(SEED_IP_POOL_NAME)
        .body(range)
        .send()
        .await
    {
        Ok(_) => info!(log, "added IP pool range"),
        Err(error) if is_conflict(&error) => {
            info!(log, "IP pool range already present")
        }
        Err(error) => {
            return Err(anyhow!(error)).context("adding IP pool range");
        }
    }

    match client
        .system_ip_pool_silo_link()
        .pool(SEED_IP_POOL_NAME)
        .body(oxide_client::types::IpPoolLinkSilo {
            silo: oxide_client::types::NameOrId::Name(
                args.silo.parse().map_err(|error| {
                    anyhow!("silo {:?} is not a valid name: {error}", args.silo)
                })?,
            ),
            is_default: true,
        })
        .send()
        .await
    {
        Ok(_) => info!(log, "linked IP pool to silo"; "silo" => &args.silo),
        Err(error) if is_conflict(&error) => {
            info!(log, "IP pool already linked to silo"; "silo" => &args.silo)
        }
        Err(error) => {
            return Err(anyhow!(error)).context("linking IP pool to silo");
        }
    }

    Ok(())
}

async fn sagas_settle(
    log: &Logger,
    args: &Args,
    timeout: Duration,
) -> anyhow::Result<()> {
    let client = nexus_lockstep_client::Client::new(
        &args.nexus_lockstep_url,
        log.new(slog::o!("component" => "NexusLockstepClient")),
    );
    let deadline = Instant::now() + timeout;

    let running = loop {
        let sagas: Vec<_> = client
            .saga_list_stream(None, None)
            .try_collect()
            .await
            .with_context(|| {
                format!("listing sagas at {}", args.nexus_lockstep_url)
            })?;
        let running: Vec<_> = sagas
            .iter()
            .filter(|saga| match saga.state {
                SagaState::Running => true,
                SagaState::Succeeded
                | SagaState::Failed { .. }
                | SagaState::Stuck { .. } => false,
            })
            .map(|saga| saga.id)
            .collect();
        if running.is_empty() {
            info!(log, "all sagas settled"; "total" => sagas.len());
            break running;
        }
        if Instant::now() >= deadline {
            warn!(
                log,
                "sagas still running after timeout";
                "running" => ?running,
            );
            break running;
        }
        info!(log, "waiting for sagas to settle"; "running" => running.len());
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let details =
        json!({ "running": running, "timeout_secs": timeout.as_secs() });
    antithesis_sdk::assert_always!(
        running.is_empty(),
        "workload: all sagas settle after faults stop",
        &details
    );
    if running.is_empty() {
        Ok(())
    } else {
        bail!(
            "{} saga(s) still running after {}s: {running:?}",
            running.len(),
            timeout.as_secs()
        )
    }
}

#[cfg(test)]
mod test {
    // TODO-RAINCLAUDE: guards the config shipped in the Antithesis image the same way nexus-config guards nexus/examples/config.toml.
    #[test]
    fn antithesis_nexus_config_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../config/nexus.toml");
        nexus_config::NexusConfig::from_file(path)
            .expect("antithesis/config/nexus.toml parsed");
    }
}
