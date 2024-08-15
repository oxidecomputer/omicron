// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the `oximeter` metric collection server.

// Copyright 2023 Oxide Computer Company

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::HttpError;
use dropshot::HttpServer;
use dropshot::HttpServerStarter;
use internal_dns::resolver::ResolveError;
use internal_dns::resolver::Resolver;
use internal_dns::ServiceName;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::backoff;
use omicron_common::FileKv;
use serde::Deserialize;
use serde::Serialize;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use slog::warn;
use slog::Drain;
use slog::Logger;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

mod agent;
mod http_entrypoints;
mod self_stats;
mod standalone;

pub use agent::OximeterAgent;
pub use http_entrypoints::oximeter_api;
pub use standalone::standalone_nexus_api;
pub use standalone::Server as StandaloneNexus;

/// Errors collecting metric data
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error running Oximeter collector server: {0}")]
    Server(String),

    #[error("Error collecting metric data from collector id={0}: {1}")]
    CollectionError(Uuid, String),

    #[error(transparent)]
    Database(#[from] oximeter_db::Error),

    #[error(transparent)]
    ResolveError(#[from] ResolveError),

    #[error("Error running standalone")]
    Standalone(#[from] anyhow::Error),
}

impl From<Error> for HttpError {
    fn from(e: Error) -> Self {
        HttpError::for_internal_error(e.to_string())
    }
}

/// Configuration for interacting with the metric database.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct DbConfig {
    /// Optional address of the ClickHouse server.
    ///
    /// If "None", will be inferred from DNS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<SocketAddr>,

    /// Batch size of samples at which to insert
    pub batch_size: usize,

    /// Interval on which to insert data into the database, regardless of the number of collected
    /// samples. Value is in seconds.
    pub batch_interval: u64,
}

impl DbConfig {
    /// Default number of samples to wait for before inserting a batch into
    /// ClickHouse.
    pub const DEFAULT_BATCH_SIZE: usize = 1000;

    /// Default number of seconds to wait before inserting a batch into
    /// ClickHouse.
    pub const DEFAULT_BATCH_INTERVAL: u64 = 5;

    // Construct config with an address, using the defaults for other fields
    fn with_address(address: SocketAddr) -> Self {
        Self {
            address: Some(address),
            batch_size: Self::DEFAULT_BATCH_SIZE,
            batch_interval: Self::DEFAULT_BATCH_INTERVAL,
        }
    }
}

/// Default interval on which we refresh our list of producers from Nexus.
pub const fn default_refresh_interval() -> Duration {
    Duration::from_secs(60 * 10)
}

/// Configuration used to initialize an oximeter server
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The address used to connect to Nexus.
    ///
    /// If "None", will be inferred from DNS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nexus_address: Option<SocketAddr>,

    /// The interval on which we periodically refresh our list of producers from
    /// Nexus.
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: Duration,

    /// Configuration for working with ClickHouse
    pub db: DbConfig,

    /// Logging configuration
    pub log: ConfigLogging,
}

impl Config {
    /// Load configuration for an Oximeter server from a file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, Error> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .map_err(|e| Error::Server(e.to_string()))?;
        toml::from_str(&contents).map_err(|e| Error::Server(e.to_string()))
    }
}

/// Arguments for running the `oximeter` collector.
pub struct OximeterArguments {
    pub id: Uuid,
    pub address: SocketAddrV6,
    // TODO: Remove once single node ClickHouse functionality is removed
    pub replicated: bool,
}

/// A server used to collect metrics from components in the control plane.
pub struct Oximeter {
    agent: Arc<OximeterAgent>,
    server: HttpServer<Arc<OximeterAgent>>,
}

impl Oximeter {
    /// Create a new `Oximeter` with the given configuration.
    ///
    /// This starts an HTTP server used to communicate with other agents in Omicron, especially
    /// Nexus. It also registers itself as a new `oximeter` instance with Nexus.
    pub async fn new(
        config: &Config,
        args: &OximeterArguments,
    ) -> Result<Self, Error> {
        let log = config
            .log
            .to_logger("oximeter")
            .map_err(|msg| Error::Server(msg.to_string()))?;
        Self::with_logger(config, args, log).await
    }

    /// Create a new `Oximeter`, specifying an alternative logger to use.
    ///
    /// This can be used to override / ignore the logging configuration in
    /// `config`, using `log` instead.
    ///
    /// Note that this blocks until the ClickHouse database is available **and
    /// at the expected version**.
    pub async fn with_logger(
        config: &Config,
        args: &OximeterArguments,
        log: Logger,
    ) -> Result<Self, Error> {
        let (drain, registration) = slog_dtrace::with_drain(log);
        let log = slog::Logger::root(drain.fuse(), o!(FileKv));
        if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
            let msg = format!("failed to register DTrace probes: {}", e);
            error!(log, "{}", msg);
            return Err(Error::Server(msg));
        } else {
            debug!(log, "registered DTrace probes");
        }
        info!(log, "starting oximeter server");

        let resolver = Resolver::new_from_ip(
            log.new(o!("component" => "DnsResolver")),
            *args.address.ip(),
        )?;

        let make_agent = || async {
            debug!(log, "creating ClickHouse client");
            Ok(Arc::new(
                OximeterAgent::with_id(
                    args.id,
                    args.address,
                    config.refresh_interval,
                    config.db,
                    &resolver,
                    &log,
                    args.replicated,
                )
                .await?,
            ))
        };
        let log_client_failure = |error, delay| {
            warn!(
                log,
                "failed to create ClickHouse client";
                "retry_after" => ?delay,
                "error" => ?error,
            );
        };
        let agent = backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            make_agent,
            log_client_failure,
        )
        .await
        .expect("Expected an infinite retry loop initializing the timeseries database");

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let server = HttpServerStarter::new(
            &ConfigDropshot {
                bind_address: SocketAddr::V6(args.address),
                ..Default::default()
            },
            oximeter_api(),
            Arc::clone(&agent),
            &dropshot_log,
        )
        .map_err(|e| Error::Server(e.to_string()))?
        .start();

        // Notify Nexus that this oximeter instance is available.
        let our_info = nexus_client::types::OximeterInfo {
            address: server.local_addr().to_string(),
            collector_id: agent.id,
        };
        let notify_nexus = || async {
            debug!(log, "contacting nexus");
            let nexus_address = if let Some(address) = config.nexus_address {
                address
            } else {
                SocketAddr::V6(SocketAddrV6::new(
                    resolver.lookup_ipv6(ServiceName::Nexus).await.map_err(
                        |e| backoff::BackoffError::transient(e.to_string()),
                    )?,
                    NEXUS_INTERNAL_PORT,
                    0,
                    0,
                ))
            };
            let client = nexus_client::Client::new(
                &format!("http://{nexus_address}"),
                log.clone(),
            );
            client.cpapi_collectors_post(&our_info).await.map_err(|e| {
                match &e {
                    // Failures to reach nexus, or server errors on its side
                    // are retryable. Everything else is permanent.
                    nexus_client::Error::CommunicationError(_) => {
                        backoff::BackoffError::transient(e.to_string())
                    }
                    nexus_client::Error::ErrorResponse(inner)
                        if inner.status().is_server_error() =>
                    {
                        backoff::BackoffError::transient(e.to_string())
                    }
                    _ => backoff::BackoffError::permanent(e.to_string()),
                }
            })
        };
        let log_notification_failure = |error, delay| {
            warn!(
                log,
                "failed to contact nexus, will retry in {:?}", delay;
                "error" => ?error
            );
        };
        backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            notify_nexus,
            log_notification_failure,
        )
        .await
        .expect("Expected an infinite retry loop contacting Nexus");

        // Now that we've successfully registered, we'll start periodically
        // polling for our list of producers from Nexus.
        agent.ensure_producer_refresh_task(resolver);

        info!(log, "oximeter registered with nexus"; "id" => ?agent.id);
        Ok(Self { agent, server })
    }

    /// Create a new `oximeter` collector running in standalone mode.
    pub async fn new_standalone(
        log: &Logger,
        args: &OximeterArguments,
        nexus: SocketAddr,
        clickhouse: Option<SocketAddr>,
    ) -> Result<Self, Error> {
        let db_config = clickhouse.map(DbConfig::with_address);
        let agent = Arc::new(
            OximeterAgent::new_standalone(
                args.id,
                args.address,
                crate::default_refresh_interval(),
                db_config,
                &log,
            )
            .await?,
        );

        let dropshot_log = log.new(o!("component" => "dropshot"));
        let server = HttpServerStarter::new(
            &ConfigDropshot {
                bind_address: SocketAddr::V6(args.address),
                ..Default::default()
            },
            oximeter_api(),
            Arc::clone(&agent),
            &dropshot_log,
        )
        .map_err(|e| Error::Server(e.to_string()))?
        .start();
        info!(log, "started oximeter standalone server");

        // Notify the standalone nexus.
        let client = reqwest::Client::new();
        let notify_nexus = || async {
            debug!(log, "contacting nexus");
            client
                .post(format!("http://{}/metrics/collectors", nexus))
                .json(&nexus_client::types::OximeterInfo {
                    address: server.local_addr().to_string(),
                    collector_id: agent.id,
                })
                .send()
                .await
                .map_err(|e| backoff::BackoffError::transient(e.to_string()))?
                .error_for_status()
                .map_err(|e| backoff::BackoffError::transient(e.to_string()))
        };
        let log_notification_failure = |error, delay| {
            warn!(
                log,
                "failed to contact nexus, will retry in {:?}", delay;
                "error" => ?error
            );
        };
        backoff::retry_notify(
            backoff::retry_policy_internal_service(),
            notify_nexus,
            log_notification_failure,
        )
        .await
        .expect("Expected an infinite retry loop contacting Nexus");

        Ok(Self { agent, server })
    }

    /// Serve requests forever, consuming the server.
    pub async fn serve_forever(self) -> Result<(), Error> {
        self.server.await.map_err(Error::Server)
    }

    /// Shutdown the Oximeter server
    pub async fn close(self) -> Result<(), Error> {
        self.server.close().await.map_err(Error::Server)
    }

    /// Forces Oximeter to perform a collection immediately.
    ///
    /// This is particularly useful during tests, which would prefer to
    /// avoid waiting until a collection interval completes.
    pub async fn force_collect(&self) {
        self.server.app_private().force_collection().await
    }

    /// List producers.
    ///
    /// This returns up to `limit` producers, whose ID is _strictly greater_
    /// than `start`, or all producers if `start` is `None`.
    pub async fn list_producers(
        &self,
        start: Option<Uuid>,
        limit: usize,
    ) -> Vec<ProducerEndpoint> {
        self.agent.list_producers(start, limit).await
    }

    /// Delete a producer by ID, stopping its collection task.
    pub async fn delete_producer(&self, id: Uuid) -> Result<(), Error> {
        self.agent.delete_producer(id).await
    }

    /// Return the ID of this collector.
    pub fn collector_id(&self) -> &Uuid {
        &self.agent.id
    }

    /// Return the address of the server.
    pub fn server_address(&self) -> SocketAddr {
        self.server.local_addr()
    }
}
