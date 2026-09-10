// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod reconfigurator;

use anyhow::{Context, anyhow, ensure};
use dropshot::test_util::LogContext;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_config::PostgresConfigWithUrl;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use omicron_common::address::Ipv6Subnet;
use slog::info;
use slog::o;
use std::ffi::OsStr;
use std::net::SocketAddrV6;
use std::path::Component;
use std::sync::Arc;

/// Contains data and interfaces useful for running tests against an existing
/// deployed control plane
pub struct LiveTestContext {
    logctx: LogContext,
    opctx: OpContext,
    resolver: Resolver,
    datastore: Arc<DataStore>,
}

impl LiveTestContext {
    /// Make a new `LiveTestContext` for a test called `test_name`.
    pub async fn new(
        test_name: &'static str,
    ) -> Result<LiveTestContext, anyhow::Error> {
        let logctx = omicron_test_utils::dev::test_setup_log(test_name);
        let log = &logctx.log;
        check_execution_environment()?;
        let resolver = create_resolver(log).await?;
        let datastore = create_datastore(&log, &resolver).await?;
        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        check_hardware_environment(&opctx, &datastore).await?;
        check_configuration(&opctx, &datastore).await?;
        Ok(LiveTestContext { logctx, opctx, resolver, datastore })
    }

    /// Clean up this `LiveTestContext`
    ///
    /// This removes log files and cleans up the [`DataStore`], which
    /// but be terminated asynchronously.
    pub async fn cleanup_successful(self) {
        self.datastore.terminate().await;
        self.logctx.cleanup_successful();
    }

    /// Returns a logger suitable for use in the test
    pub fn log(&self) -> &slog::Logger {
        &self.logctx.log
    }

    /// Returns an `OpContext` suitable for use in tests
    pub fn opctx(&self) -> &OpContext {
        &self.opctx
    }

    /// Returns a `DataStore` pointing at this deployed system's database
    pub fn datastore(&self) -> &DataStore {
        &self.datastore
    }

    /// Returns a client for a Nexus internal API at the given socket address
    pub fn specific_internal_nexus_client(
        &self,
        sockaddr: SocketAddrV6,
    ) -> nexus_lockstep_client::Client {
        let url = format!("http://{}", sockaddr);
        let log = self.logctx.log.new(o!("nexus_internal_url" => url.clone()));
        nexus_lockstep_client::Client::new(&url, log)
    }

    /// Returns a list of clients for the internal APIs for all Nexus instances
    /// found in DNS
    pub async fn all_internal_nexus_clients(
        &self,
    ) -> Result<Vec<nexus_lockstep_client::Client>, anyhow::Error> {
        Ok(self
            .resolver
            .lookup_all_socket_v6(ServiceName::NexusLockstep)
            .await
            .context("looking up Nexus in internal DNS")?
            .into_iter()
            .map(|s| self.specific_internal_nexus_client(s))
            .collect())
    }
}

/// AZ subnets (/48) of the environments where live tests may be run
///
/// The base subnet is configurable in config-rss.toml, and each racklette's ULA
/// prefix was randomized at RSS time.  These are the test rigs allowed by
/// `ALLOWED_GIMLET_SERIALS`; read a rack's prefix from `/etc/resolv.conf` on
/// any of its sleds.
const CANDIDATE_AZ_SUBNETS: &[(&str, &str)] = &[
    ("default", "fd00:1122:3344::"),
    ("berlin", "fd85:375e:4c4d::"),
    ("dublin", "fd1d:b310:936f::"),
    ("london", "fd8b:57a9:e7cb::"),
    ("madrid", "fd16:1925:797b::"),
];

/// Creates a resolver for the internal DNS servers of the rack we're on
///
/// We would rather read /etc/resolv.conf, but it usually isn't populated today.
/// See oxidecomputer/omicron#2122.  Instead, probe every candidate subnet
/// concurrently and take the first that answers; at most one is reachable.
async fn create_resolver(
    log: &slog::Logger,
) -> Result<Resolver, anyhow::Error> {
    let probes = CANDIDATE_AZ_SUBNETS
        .iter()
        .map(|(rig_name, prefix)| {
            let addr = prefix
                .parse()
                .expect("CANDIDATE_AZ_SUBNETS entries are IPv6 addresses");
            let subnet = Ipv6Subnet::new(addr);
            let log = log.clone();
            // Boxed: `select_ok()` requires a uniform future type.
            Box::pin(async move {
                let resolver =
                    internal_dns_resolver::Resolver::new_from_subnet(
                        log, subnet,
                    )
                    .with_context(|| {
                        format!(
                            "creating DNS resolver for subnet {}",
                            subnet.net()
                        )
                    })?;
                resolver
                    .lookup_srv(ServiceName::InternalDns)
                    .await
                    .with_context(|| {
                        format!(
                            "looking up internal DNS in subnet {}",
                            subnet.net()
                        )
                    })?;
                Ok::<_, anyhow::Error>((*rig_name, subnet, resolver))
            })
        })
        .collect::<Vec<_>>();

    match futures::future::select_ok(probes).await {
        Ok(((rig_name, subnet, resolver), _)) => {
            eprintln!(
                "note: using internal DNS servers for subnet {} (test rig {:?})",
                subnet.net(),
                rig_name
            );
            Ok(resolver)
        }
        Err(error) => {
            let candidates = CANDIDATE_AZ_SUBNETS
                .iter()
                .map(|(rig_name, prefix)| format!("{} ({})", prefix, rig_name))
                .collect::<Vec<_>>()
                .join(", ");
            let text = format!(
                "create_resolver(): none of the known internal DNS servers \
                 responded.\n\n \
                 Are you trying to run this in a development environment?  \
                 This test can only be run on deployed systems and only from a \
                 context with connectivity to the underlay network.\n\n \
                 If you are on a rack that isn't listed here, add its AZ \
                 subnet to CANDIDATE_AZ_SUBNETS.  Subnets tried: {}\n\n \
                 last raw error: {}",
                candidates,
                slog_error_chain::InlineErrorChain::new(&*error),
            );
            Err(anyhow!("{}", textwrap::wrap(&text, 80).join("\n")))
        }
    }
}

/// Creates a DataStore pointing at the CockroachDB cluster that's in DNS
async fn create_datastore(
    log: &slog::Logger,
    resolver: &Resolver,
) -> Result<Arc<DataStore>, anyhow::Error> {
    let sockaddrs = resolver
        .lookup_all_socket_v6(ServiceName::Cockroach)
        .await
        .context("resolving CockroachDB")?;

    let url = format!(
        "postgresql://root@{}/omicron?sslmode=disable",
        sockaddrs
            .into_iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
    .parse::<PostgresConfigWithUrl>()
    .context("failed to parse constructed postgres URL")?;

    let db_config = nexus_db_queries::db::Config { url };
    let pool =
        Arc::new(nexus_db_queries::db::Pool::new_single_host(log, &db_config));
    DataStore::new_failfast(log, pool)
        .await
        .context("creating DataStore")
        .map(Arc::new)
}

/// Performs quick checks to determine if the user is running these tests in the
/// wrong place and bails out if so
///
/// This isn't perfect but seeks to fail fast in obviously bogus environments
/// that someone might accidentally try to run this in.
fn check_execution_environment() -> Result<(), anyhow::Error> {
    ensure!(
        cfg!(target_os = "illumos"),
        "live tests can only be run on deployed systems, which run illumos"
    );

    // The other requirement -- connectivity to a deployed control plane's
    // underlay -- is checked by create_resolver().

    // Warn the user if the temporary directory is /tmp.  This check is
    // heuristic.  There are other ways they may have specified a tmpfs
    // temporary directory and we don't claim to catch all of them.
    //
    // We could also just go ahead and use /var/tmp, but it's not clear we can
    // reliably do that at this point (if Rust or other components have cached
    // TMPDIR) and it would be hard to override.
    let tmpdir = std::env::temp_dir();
    let mut tmpdir_components = tmpdir.components().take(2);
    if let Some(first) = tmpdir_components.next() {
        if let Some(next) = tmpdir_components.next() {
            if first == Component::RootDir
                && next == Component::Normal(OsStr::new("tmp"))
            {
                eprintln!(
                    "WARNING: temporary directory appears to be under /tmp, \
                     which is generally tmpfs.  Consider setting \
                     TMPDIR=/var/tmp to avoid runaway tests using too much\
                     memory and swap."
                );
            }
        }
    }

    Ok(())
}

/// Performs additional checks to determine if we're running in an environment
/// that we believe is safe to run tests
///
/// These tests may make arbitrary modifications to the system.  We don't want
/// to run this in dogfood or other pre-production or production environments.
/// This function uses an allowlist of Oxide serials corresponding to test
/// environments so that it never accidentally runs on a production system.
///
/// Non-Oxide hardware (e.g., PCs, a4x2, etc.) are always allowed.
async fn check_hardware_environment(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    const ALLOWED_GIMLET_SERIALS: &[&str] = &[
        // Serial number lists can be generated with:
        // inventron env system list -Hpo serial -F type=gimlet <ENVIRONMENT>

        // test rig: "madrid"
        "BRM42220081",
        "BRM42220046",
        "BRM42220007",
        "BRM42220004",
        // test rig: "london"
        "BRM42220036",
        "2CN2M459",
        "BRM42220030",
        "2RGCFG10",
        // test rig: "dublin"
        "2F8JEXDK",
        "BRM27230037",
        "BRM23230018",
        "BRM23230010",
        // test rig: "berlin"
        "BRM42220011",
        "BRM44220007",
        "BRM42220082",
        "271FVPY0",
    ];

    // Refuse to operate in an environment that might contain real Oxide
    // hardware that's not known to be part of a test rig.  This is deliberately
    // conservative.
    let scary_sleds = datastore
        .sled_list_all_batched(opctx, SledFilter::Commissioned)
        .await
        .context("check_environment: listing commissioned sleds")?
        .into_iter()
        .filter_map(|s| {
            (s.part_number() != "i86pc"
                && !ALLOWED_GIMLET_SERIALS.contains(&s.serial_number()))
            .then(|| s.serial_number().to_owned())
        })
        .collect::<Vec<_>>();
    if scary_sleds.is_empty() {
        info!(&opctx.log, "environment verified");
        Ok(())
    } else {
        Err(anyhow!(
            "refusing to operate in an environment with an unknown system: {}",
            scary_sleds.join(", ")
        ))
    }
}

/// Performs checks on the system configuration to determine if it's appropriate
/// for live tests
///
/// Currently, this just verifies that the planner is off.
async fn check_configuration(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let reconfigurator_config = datastore
        .reconfigurator_config_get_latest(opctx)
        .await
        .expect("obtained latest reconfigurator config")
        .unwrap_or_default();
    if reconfigurator_config.config.planner_enabled {
        Err(anyhow!(
            "refusing to operate on a system with blueprint planning enabled"
        ))
    } else {
        Ok(())
    }
}
