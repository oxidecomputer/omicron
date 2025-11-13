// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot-configurable DNS server
//!
//! This crate provides a standalone program that runs a UDP-based DNS server
//! along with a Dropshot server for configuring the records served over DNS.
//! The following RFDs describe the overall design of this server and how it's
//! used:
//!
//!   RFD 248 Omicron service discovery: server side
//!   RFD 357 External DNS in the MVP
//!   RFD 367 DNS propagation in Omicron
//!
//! Here are the highlights:
//!
//! (1) This component is used for both internal and external DNS in Omicron
//!     (the control plane for the Oxide system).  These are deployed
//!     separately.  A given instance is either an internal DNS server or an
//!     external one, not both.
//!
//! (2) For the internal DNS use case, it's absolutely essential that the DNS
//!     servers have no external dependencies.  That's why we persistently store
//!     a copy of the DNS data.  After a cold start of the whole Oxide system,
//!     the DNS servers must be able to come up and serve DNS so that the rest of
//!     the control plane components and locate each other.  The internal DNS use
//!     case is expected to be a fairly small amount of data, updated fairly
//!     infrequently, and not particularly latency-sensitive.
//!
//! (3) External DNS is required for availability of user-facing services like
//!     the web console and API for the Oxide system.   Eventually, these will
//!     also provide names for user resources like Instances.  As a result,
//!     there could be a fair bit of data and it may be updated fairly
//!     frequently.
//!
//! (4) DNS data is managed by Nexus, persisted in Cockroach, and propagated out
//!     to these severs.  Critically, these servers may serve records derived
//!     from but not explicitly defined as a
//!     [`internal_dns_types::config::DnsRecord`].  SOA records are an example
//!     here.  On the HTTP interface, there is one consistent "upstream" view of
//!     the DNS server: the configuration that has been given to this DNS
//!     server.  This avoids the burden of having to rectify any intermediary
//!     state (in a database or Nexus) with synthesized "downstream" records.
//!
//! This crate provides three main pieces for running the DNS server program:
//!
//! 1. Persistent [`storage::Store`] of DNS data
//! 2. A [`dns_server::Server`], that serves data from a `storage::Store` out
//!    over the DNS protocol
//! 3. A Dropshot server that serves HTTP endpoints for reading and modifying
//!    the persistent DNS data

pub mod authority;
pub mod dns_server;
pub mod http_server;
pub mod storage;

use anyhow::{Context, anyhow};
use hickory_resolver::TokioResolver;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::name_server::TokioConnectionProvider;
use internal_dns_types::config::DnsConfigParams;
use slog::o;
use std::net::SocketAddr;

/// Starts both the HTTP and DNS servers over a given store.
pub async fn start_servers(
    log: slog::Logger,
    store: storage::Store,
    dns_server_config: &dns_server::Config,
    dropshot_config: &dropshot::ConfigDropshot,
) -> Result<
    (dns_server::ServerHandle, dropshot::HttpServer<http_server::Context>),
    anyhow::Error,
> {
    let dns_server = {
        dns_server::Server::start(
            log.new(o!("component" => "dns")),
            store.clone(),
            dns_server_config,
        )
        .await
        .context("starting DNS server")?
    };

    let dropshot_server = {
        let http_api = http_server::api();
        let http_api_context = http_server::Context::new(store);

        dropshot::ServerBuilder::new(
            http_api,
            http_api_context,
            log.new(o!("component" => "http")),
        )
        .config(dropshot_config.clone())
        .version_policy(dropshot::VersionPolicy::Dynamic(Box::new(
            dropshot::ClientSpecifiesVersionInHeader::new(
                omicron_common::api::VERSION_HEADER,
                dns_server_api::VERSION_SOA_AND_NS,
            ),
        )))
        .start()
        .map_err(|error| anyhow!("setting up HTTP server: {:#}", error))?
    };

    Ok((dns_server, dropshot_server))
}

/// An DNS server running on localhost, using a temporary directory for storage.
///
/// Intended to be used for testing only.
pub struct TransientServer {
    /// Server storage dir
    pub storage_dir: tempfile::TempDir,
    /// DNS server
    pub dns_server: dns_server::ServerHandle,
    /// Dropshot server
    pub dropshot_server: dropshot::HttpServer<http_server::Context>,
}

impl TransientServer {
    pub async fn new(log: &slog::Logger) -> Result<Self, anyhow::Error> {
        Self::new_with_address(log, "[::1]:0".parse().unwrap()).await
    }

    pub async fn new_with_address(
        log: &slog::Logger,
        dns_bind_address: SocketAddr,
    ) -> Result<Self, anyhow::Error> {
        let storage_dir = tempfile::tempdir()?;

        let dns_log = log.new(o!("kind" => "dns"));

        let store = storage::Store::new(
            log.new(o!("component" => "store")),
            &storage::Config {
                keep_old_generations: 3,
                storage_path: storage_dir
                    .path()
                    .to_string_lossy()
                    .to_string()
                    .into(),
            },
        )
        .context("initializing DNS storage")?;

        let (dns_server, dropshot_server) = start_servers(
            dns_log,
            store,
            &dns_server::Config {
                bind_address: dns_bind_address,
                ..Default::default()
            },
            &dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                default_request_body_max_bytes: 4 * 1024 * 1024,
                default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
                log_headers: vec![],
            },
        )
        .await?;
        Ok(Self { storage_dir, dns_server, dropshot_server })
    }

    pub async fn initialize_with_config(
        &self,
        log: &slog::Logger,
        dns_config: &DnsConfigParams,
    ) -> Result<(), anyhow::Error> {
        let dns_config_client = dns_service_client::Client::new(
            &format!("http://{}", self.dropshot_server.local_addr()),
            log.clone(),
        );
        dns_config_client
            .dns_config_put(&dns_config)
            .await
            .context("initializing DNS")?;
        Ok(())
    }

    pub async fn resolver(&self) -> Result<TokioResolver, anyhow::Error> {
        let mut resolver_config = ResolverConfig::new();
        resolver_config.add_name_server(NameServerConfig::new(
            self.dns_server.local_address(),
            hickory_proto::xfer::Protocol::Udp,
        ));
        let mut resolver_opts = ResolverOpts::default();
        // Enable edns for potentially larger records
        resolver_opts.edns0 = true;

        let resolver = TokioResolver::builder_with_config(
            resolver_config,
            TokioConnectionProvider::default(),
        )
        .with_options(resolver_opts)
        .build();

        Ok(resolver)
    }
}
