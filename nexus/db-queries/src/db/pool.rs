// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database connection pooling
// TODO-design Need TLS support (the types below hardcode NoTls).

use super::Config as DbConfig;
use qorb::backend;
use qorb::connectors::diesel_pg::DieselPgConnector;
use qorb::policy::Policy;
use qorb::resolver::{AllBackends, Resolver};
use qorb::resolvers::dns::{DnsResolver, DnsResolverConfig};
use qorb::service;
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
// contact the pool directly.
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
        DnsResolverConfig::default(),
    ))
}

fn make_single_host_resolver(
    config: &DbConfig,
) -> qorb::resolver::BoxedResolver {
    Box::new(SingleHostResolver::new(config))
}

fn make_postgres_connector() -> qorb::backend::SharedConnector<QorbConnection> {
    // Create postgres connections.
    //
    // TODO: "on acquire"?
    // TODO: Connection timeout for failfast?
    //
    // We're currently relying on somewhat intrusive modifications to qorb to
    // make these things possible. Might be worth a refactor?
    let user = "root";
    let db = "omicron";
    let args = Some("sslmode=disable");
    Arc::new(DieselPgConnector::new(user, db, args))
}

impl Pool {
    /// Creates a new qorb-backed connection pool to the database.
    ///
    /// Creating this pool does not necessarily wait for connections to become
    /// available, as backends may shift over time.
    pub fn new_qorb(bootstrap_dns: Vec<SocketAddr>) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");

        let resolver = make_dns_resolver(bootstrap_dns);
        let connector = make_postgres_connector();

        let policy = Policy::default();
        Pool { inner: qorb::pool::Pool::new(resolver, connector, policy) }
    }

    /// Creates a new qorb-backed connection pool to a single instance of the
    /// database.
    ///
    /// This is intended for tests that want to skip DNS resolution, relying
    /// on a single instance of the database.
    ///
    /// In production, [Self::new_qorb] should be preferred.
    // TODO: Does not need to be async
    pub async fn new_qorb_single_host(db_config: &DbConfig) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");

        let resolver = make_single_host_resolver(db_config);
        let connector = make_postgres_connector();

        let policy = Policy::default();
        let pool =
            Pool { inner: qorb::pool::Pool::new(resolver, connector, policy) };

        let stats = pool.inner.stats().clone();

        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                let backends = stats.rx.borrow();
                for (name, stats) in backends.iter() {
                    let stats = stats.get();
                    println!("Backend: {name:?}");
                    println!("  Stats: {stats:?}");
                }
                println!(
                    "Total claims: {}",
                    stats.claims.load(std::sync::atomic::Ordering::SeqCst)
                );
            }
        });

        pool
    }

    /// Returns a connection from the pool
    pub async fn claim(
        &self,
    ) -> anyhow::Result<qorb::claim::Handle<QorbConnection>> {
        Ok(self.inner.claim().await?)
    }
}
