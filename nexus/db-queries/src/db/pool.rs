// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database connection pooling
// TODO-design Need TLS support (the types below hardcode NoTls).

use super::Config as DbConfig;
use crate::db::pool_connection::{DieselPgConnector, DieselPgConnectorArgs};

use qorb::backend;
use qorb::policy::Policy;
use qorb::resolver::{AllBackends, Resolver};
use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
use qorb::service;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;

pub use super::pool_connection::DbConnection;

type QorbConnection = async_bb8_diesel::Connection<DbConnection>;
type QorbPool = qorb::pool::Pool<QorbConnection>;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    inner: QorbPool,
}

// Provides an alternative to the DNS resolver for cases where we want to
// contact the database without performing resolution.
struct SingleHostResolver {
    tx: watch::Sender<AllBackends>,
}

impl SingleHostResolver {
    fn new(config: &DbConfig) -> Self {
        let backends = Arc::new(BTreeMap::from([(
            backend::Name::new("singleton"),
            backend::Backend { address: config.url.address() },
        )]));
        let (tx, _rx) = watch::channel(backends.clone());
        Self { tx }
    }
}

impl Resolver for SingleHostResolver {
    fn monitor(&mut self) -> watch::Receiver<AllBackends> {
        self.tx.subscribe()
    }
}

fn make_dns_resolver(
    bootstrap_dns: Vec<SocketAddr>,
) -> qorb::resolver::BoxedResolver {
    Box::new(DnsResolver::new(
        service::Name(internal_dns::ServiceName::Cockroach.srv_name()),
        bootstrap_dns,
        DnsResolverConfig {
            query_interval: tokio::time::Duration::from_secs(10),
            hardcoded_ttl: Some(tokio::time::Duration::MAX),
            ..Default::default()
        },
    ))
}

fn make_single_host_resolver(
    config: &DbConfig,
) -> qorb::resolver::BoxedResolver {
    Box::new(SingleHostResolver::new(config))
}

fn make_postgres_connector(
    log: &Logger,
) -> qorb::backend::SharedConnector<QorbConnection> {
    // Create postgres connections.
    //
    // We're currently relying on the DieselPgConnector doing the following:
    // - Disallowing full table scans in its implementation of "on_acquire"
    // - Creating async_bb8_diesel connections that also wrap DTraceConnections.
    let user = "root";
    let db = "omicron";
    let args = Some("sslmode=disable");
    Arc::new(DieselPgConnector::new(
        log,
        DieselPgConnectorArgs { user, db, args },
    ))
}

impl Pool {
    /// Creates a new qorb-backed connection pool to the database.
    ///
    /// Creating this pool does not necessarily wait for connections to become
    /// available, as backends may shift over time.
    pub fn new(log: &Logger, bootstrap_dns: Vec<SocketAddr>) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");

        let resolver = make_dns_resolver(bootstrap_dns);
        let connector = make_postgres_connector(log);

        let policy = Policy::default();
        Pool { inner: qorb::pool::Pool::new(resolver, connector, policy) }
    }

    /// Creates a new qorb-backed connection pool to a single instance of the
    /// database.
    ///
    /// This is intended for tests that want to skip DNS resolution, relying
    /// on a single instance of the database.
    ///
    /// In production, [Self::new] should be preferred.
    pub fn new_single_host(log: &Logger, db_config: &DbConfig) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");

        let resolver = make_single_host_resolver(db_config);
        let connector = make_postgres_connector(log);

        let policy = Policy::default();
        Pool { inner: qorb::pool::Pool::new(resolver, connector, policy) }
    }

    /// Creates a new qorb-backed connection pool which returns an error
    /// if claims are not quickly available.
    ///
    /// This is intended for test-only usage.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_single_host_failfast(
        log: &Logger,
        db_config: &DbConfig,
    ) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");

        let resolver = make_single_host_resolver(db_config);
        let connector = make_postgres_connector(log);

        let policy = Policy {
            claim_timeout: tokio::time::Duration::from_millis(1),
            ..Default::default()
        };
        Pool { inner: qorb::pool::Pool::new(resolver, connector, policy) }
    }

    /// Returns a connection from the pool
    pub async fn claim(
        &self,
    ) -> anyhow::Result<qorb::claim::Handle<QorbConnection>> {
        Ok(self.inner.claim().await?)
    }
}
