// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{anyhow, bail, ensure, Context};
use dropshot::test_util::LogContext;
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use nexus_config::PostgresConfigWithUrl;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use omicron_common::address::Ipv6Subnet;
use slog::info;
use slog::o;
use std::net::SocketAddrV6;
use std::sync::Arc;

// XXX-dap point tmp at some place other than /tmp

pub struct LiveTestContext {
    logctx: LogContext,
    opctx: OpContext,
    resolver: Resolver,
    datastore: Arc<DataStore>,
}

impl LiveTestContext {
    pub async fn new(
        test_name: &'static str,
    ) -> Result<LiveTestContext, anyhow::Error> {
        // XXX-dap check that we're actually running inside an environment and
        // not in some workspace
        let logctx = omicron_test_utils::dev::test_setup_log(test_name);
        let log = &logctx.log;
        let resolver = create_resolver(log)?;
        check_execution_environment(&resolver).await?;
        let datastore = create_datastore(&log, &resolver).await?;
        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        check_hardware_environment(&opctx, &datastore).await?;
        Ok(LiveTestContext { logctx, opctx, resolver, datastore })
    }

    pub fn cleanup_successful(self) {
        self.logctx.cleanup_successful();
    }

    pub fn log(&self) -> &slog::Logger {
        &self.logctx.log
    }

    pub async fn any_internal_nexus_client(
        &self,
    ) -> Result<nexus_client::Client, anyhow::Error> {
        let sockaddr = self
            .resolver
            .lookup_socket_v6(ServiceName::Nexus)
            .await
            .context("looking up Nexus in internal DNS")?;
        Ok(self.specific_internal_nexus_client(sockaddr))
    }

    pub fn specific_internal_nexus_client(
        &self,
        sockaddr: SocketAddrV6,
    ) -> nexus_client::Client {
        let url = format!("http://{}", sockaddr);
        let log = self.logctx.log.new(o!("nexus_internal_url" => url.clone()));
        nexus_client::Client::new(&url, log)
    }

    pub async fn all_internal_nexus_clients(
        &self,
    ) -> Result<Vec<nexus_client::Client>, anyhow::Error> {
        Ok(self
            .resolver
            .lookup_all_socket_v6(ServiceName::Nexus)
            .await
            .context("looking up Nexus in internal DNS")?
            .into_iter()
            .map(|s| self.specific_internal_nexus_client(s))
            .collect())
    }

    pub fn opctx(&self) -> &OpContext {
        &self.opctx
    }

    pub fn datastore(&self) -> &DataStore {
        &self.datastore
    }
}

fn create_resolver(log: &slog::Logger) -> Result<Resolver, anyhow::Error> {
    // In principle, we should look at /etc/resolv.conf to find the
    // DNS servers.  In practice, this usually isn't populated
    // today.  See oxidecomputer/omicron#2122.
    //
    // However, the address selected below should work for most
    // existing Omicron deployments today.  That's because while the
    // base subnet is in principle configurable in config-rss.toml,
    // it's very uncommon to change it from the default value used
    // here.
    let subnet = Ipv6Subnet::new("fd00:1122:3344:0100::".parse().unwrap());
    eprintln!("note: using DNS server for subnet {}", subnet.net());
    internal_dns::resolver::Resolver::new_from_subnet(log.clone(), subnet)
        .with_context(|| {
            format!("creating DNS resolver for subnet {}", subnet.net())
        })
}

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
    let pool = Arc::new(nexus_db_queries::db::Pool::new(log, &db_config));
    let datastore = DataStore::new_unchecked(log.clone(), pool)
        .map_err(|s| anyhow!("creating DataStore: {s}"))?;

    // XXX-dap TODO-cleanup put all this into a Datastore::new_nowait() or
    // something
    let expected_version = nexus_db_model::SCHEMA_VERSION;
    let (found_version, found_target) = datastore
        .database_schema_version()
        .await
        .context("loading database schema version")?;
    eprintln!(
        "create_datastore(): found_version {found_version}, \
        found_target {found_target:?}"
    );

    if let Some(found_target) = found_target {
        bail!(
            "database schema check failed: apparently mid-upgrade \
            (found_target = {found_target})"
        );
    }

    if found_version != expected_version {
        bail!(
            "database schema check failed: \
            expected {expected_version}, found {found_version}",
        );
    }

    Ok(Arc::new(datastore))
}

async fn check_execution_environment(
    resolver: &Resolver,
) -> Result<(), anyhow::Error> {
    ensure!(
        cfg!(target_os = "illumos"),
        "live tests can only be run on deployed systems, which run illumos"
    );

    resolver.lookup_ip(ServiceName::InternalDns).await.map(|_| ()).map_err(
        |e| {
            let text = format!(
                "check_execution_environment(): failed to look up internal DNS \
                 in the internal DNS servers.\n\n \
                 Are you trying to run this in a development environment?  \
                 This test can only be run on deployed systems and only from a \
                 context with connectivity to the underlay network.\n\n \
                 raw error: {}",
                 slog_error_chain::InlineErrorChain::new(&e)
                );
            anyhow!("{}", textwrap::wrap(&text, 80).join("\n"))
        },
    )
}

async fn check_hardware_environment(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    const ALLOWED_GIMLET_SERIALS: &[&str] = &[
        // test rig: "madrid"
        "BRM42220004",
        "BRM42220081",
        "BRM42220007",
        "BRM42220046",
        // test rig: "london"
        "BRM42220036",
        "BRM42220062",
        "BRM42220030",
        "BRM44220007",
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
