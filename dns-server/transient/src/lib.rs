// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A DNS server running on localhost, using a temporary directory for storage.
//!
//! Intended to be used for testing only.

use anyhow::Context;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::name_server::TokioConnectionProvider;
use internal_dns_types::config::DnsConfigParams;
use slog::o;
use std::net::SocketAddr;

// Re-export field types so callers don't need a direct dns-server dependency.
pub use dns_server::dns_server::ServerHandle;
pub use dns_server::http_server::Context as HttpServerContext;

pub struct TransientDnsServer {
    /// Server storage dir.
    pub storage_dir: tempfile::TempDir,
    /// DNS server.
    pub dns_server: ServerHandle,
    /// Dropshot server.
    pub dropshot_server: dropshot::HttpServer<HttpServerContext>,
}

impl TransientDnsServer {
    pub async fn new(log: &slog::Logger) -> Result<Self, anyhow::Error> {
        Self::new_with_address(log, "[::1]:0".parse().unwrap()).await
    }

    pub async fn new_with_address(
        log: &slog::Logger,
        dns_bind_address: SocketAddr,
    ) -> Result<Self, anyhow::Error> {
        let storage_dir = tempfile::tempdir()?;

        let dns_log = log.new(o!("kind" => "dns"));

        let store = dns_server::storage::Store::new(
            log.new(o!("component" => "store")),
            &dns_server::storage::Config {
                keep_old_generations: 3,
                storage_path: storage_dir
                    .path()
                    .to_string_lossy()
                    .to_string()
                    .into(),
            },
        )
        .context("initializing DNS storage")?;

        let (dns_server, dropshot_server) = dns_server::start_servers(
            dns_log,
            store,
            &dns_server::dns_server::Config { bind_address: dns_bind_address },
            &dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                default_request_body_max_bytes: 4 * 1024 * 1024,
                default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
                log_headers: vec![],
                compression: dropshot::CompressionConfig::None,
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
        // Enable edns for potentially larger records.
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
