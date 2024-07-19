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
//! This crate provides three main pieces for running the DNS server program:
//!
//! 1. Persistent [`storage::Store`] of DNS data
//! 2. A [`dns_server::Server`], that serves data from a `storage::Store` out
//!    over the DNS protocol
//! 3. A Dropshot server that serves HTTP endpoints for reading and modifying
//!    the persistent DNS data

pub mod dns_server;
pub mod http_server;
pub mod storage;

use anyhow::{anyhow, Context};
use slog::o;
use std::net::SocketAddr;
use trust_dns_resolver::config::NameServerConfig;
use trust_dns_resolver::config::Protocol;
use trust_dns_resolver::config::ResolverConfig;
use trust_dns_resolver::config::ResolverOpts;
use trust_dns_resolver::TokioAsyncResolver;

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

        dropshot::HttpServerStarter::new(
            dropshot_config,
            http_api,
            http_api_context,
            &log.new(o!("component" => "http")),
        )
        .map_err(|error| anyhow!("setting up HTTP server: {:#}", error))?
        .start()
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
            &dns_server::Config { bind_address: dns_bind_address },
            &dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                request_body_max_bytes: 4 * 1024 * 1024,
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
        dns_config: &dns_service_client::types::DnsConfigParams,
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

    pub async fn resolver(&self) -> Result<TokioAsyncResolver, anyhow::Error> {
        let mut resolver_config = ResolverConfig::new();
        resolver_config.add_name_server(NameServerConfig {
            socket_addr: self.dns_server.local_address(),
            protocol: Protocol::Udp,
            tls_dns_name: None,
            trust_nx_responses: false,
            bind_addr: None,
        });
        let resolver =
            TokioAsyncResolver::tokio(resolver_config, ResolverOpts::default())
                .context("creating DNS resolver")?;
        Ok(resolver)
    }
}
