// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database connection pooling
// TODO-design Need TLS support (the types below hardcode NoTls).

use super::Config as DbConfig;
use crate::db::pool_connection::{DieselPgConnector, DieselPgConnectorArgs};

use chrono::Utc;
use iddqd::IdOrdMap;
use internal_dns_resolver::QorbResolver;
use internal_dns_types::names::ServiceName;
use nexus_db_lookup::{AsyncConnection, DataStoreConnection};
use nexus_types::internal_api::views::HeldDbClaimInfo;
use omicron_common::api::external::Error;
use qorb::backend;
use qorb::policy::Policy;
use qorb::resolver::{AllBackends, Resolver};
use slog::Logger;
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    next_id: AtomicU64,
    inner: qorb::pool::Pool<AsyncConnection>,
    log: Logger,
    terminated: std::sync::atomic::AtomicBool,
    quiesce: watch::Sender<Quiesce>,
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

fn make_single_host_resolver(
    config: &DbConfig,
) -> qorb::resolver::BoxedResolver {
    Box::new(SingleHostResolver::new(config))
}

fn make_postgres_connector(
    log: &Logger,
) -> qorb::backend::SharedConnector<AsyncConnection> {
    // Create postgres connections.
    //
    // We're currently relying on the DieselPgConnector doing the following:
    // - Disallowing full table scans in its implementation of "on_acquire"
    // - Creating async_bb8_diesel connections that also wrap DTraceConnections.
    let user = "root";
    let db = "omicron";
    let args = vec![("sslmode", "disable")];
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
    pub fn new(log: &Logger, resolver: &QorbResolver) -> Self {
        let resolver = resolver.for_service(ServiceName::Cockroach);
        let connector = make_postgres_connector(log);
        let policy = Policy::default();
        let inner = match qorb::pool::Pool::new(
            "crdb".to_string(),
            resolver,
            connector,
            policy,
        ) {
            Ok(pool) => {
                debug!(log, "registered USDT probes");
                pool
            }
            Err(err) => {
                error!(log, "failed to register USDT probes");
                err.into_inner()
            }
        };
        Self::new_common(inner, log.clone())
    }

    /// Creates a new qorb-backed connection pool to a single instance of the
    /// database.
    ///
    /// This is intended for tests that want to skip DNS resolution, relying
    /// on a single instance of the database.
    ///
    /// In production, [Self::new] should be preferred.
    pub fn new_single_host(log: &Logger, db_config: &DbConfig) -> Self {
        let resolver = make_single_host_resolver(db_config);
        let connector = make_postgres_connector(log);
        let policy = Policy::default();
        let inner = match qorb::pool::Pool::new(
            "crdb-single-host".to_string(),
            resolver,
            connector,
            policy,
        ) {
            Ok(pool) => {
                debug!(log, "registered USDT probes");
                pool
            }
            Err(err) => {
                error!(log, "failed to register USDT probes");
                err.into_inner()
            }
        };
        Self::new_common(inner, log.clone())
    }

    /// Creates a new qorb-backed connection pool which returns an error
    /// if claims are not available within one millisecond.
    ///
    /// This is intended for test-only usage, in particular for tests where
    /// claim requests should rapidly return errors when a backend has been
    /// intentionally disabled.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_single_host_failfast(
        log: &Logger,
        db_config: &DbConfig,
    ) -> Self {
        let resolver = make_single_host_resolver(db_config);
        let connector = make_postgres_connector(log);
        let policy = Policy {
            claim_timeout: tokio::time::Duration::from_millis(1),
            ..Default::default()
        };
        let inner = match qorb::pool::Pool::new(
            "crdb-single-host-failfast".to_string(),
            resolver,
            connector,
            policy,
        ) {
            Ok(pool) => {
                debug!(log, "registered USDT probes");
                pool
            }
            Err(err) => {
                error!(log, "failed to register USDT probes");
                err.into_inner()
            }
        };
        Self::new_common(inner, log.clone())
    }

    fn new_common(
        inner: qorb::pool::Pool<AsyncConnection>,
        log: Logger,
    ) -> Self {
        let (quiesce, _) = watch::channel(Quiesce {
            new_claims_allowed: ClaimsAllowed::Allowed,
            claims_held: IdOrdMap::new(),
        });
        Pool {
            next_id: AtomicU64::new(0),
            inner,
            log,
            terminated: std::sync::atomic::AtomicBool::new(false),
            quiesce,
        }
    }

    /// Returns a connection from the pool
    pub async fn claim(&self) -> Result<DataStoreConnection, Error> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let held_since = Utc::now();
        let debug = Backtrace::force_capture().to_string(); // XXX-dap
        let allowed = self.quiesce.send_if_modified(|q| {
            if let ClaimsAllowed::Disallowed = q.new_claims_allowed {
                false
            } else {
                q.claims_held
                    .insert_unique(HeldDbClaimInfo { id, held_since, debug })
                    .expect("claim should not be present yet");
                true
            }
        });
        if !allowed {
            return Err(Error::unavail(
                "no new database claims allowed (quiescing/quiesced)",
            ));
        }

        let claim_releaser = ClaimReleaser::new(id, self.quiesce.clone());
        self.inner
            .claim()
            .await
            .map(|qorb_claim| {
                DataStoreConnection::new(qorb_claim, Box::new(claim_releaser))
            })
            .map_err(|err| {
                Error::unavail(&format!(
                    "Failed to access DB connection: {err}"
                ))
            })
    }

    /// Disables creation of all new database claims
    ///
    /// This is currently a one-way trip.  The pool cannot be un-quiesced.
    pub fn quiesce(&self) {
        // Log this before changing the config to make sure this message
        // appears before messages from code paths that saw this change.
        info!(&self.log, "starting db pool quiesce");
        self.quiesce.send_modify(|q| {
            q.new_claims_allowed = ClaimsAllowed::Disallowed;
        });
    }

    /// Wait for all outstanding claims to be released
    pub async fn wait_for_quiesced(&self) {
        let mut rx = self.quiesce.subscribe();
        // unwrap(): this can only fail if the tx side is dropped, but that
        // can't happen because we have a reference to it via `self`.
        rx.wait_for(|q| {
            q.new_claims_allowed == ClaimsAllowed::Disallowed
                && q.claims_held.is_empty()
        })
        .await
        .unwrap();
    }

    /// Returns information about held db claims
    pub fn claims_held(&self) -> IdOrdMap<HeldDbClaimInfo> {
        self.quiesce.borrow().claims_held.clone()
    }

    /// Stops the qorb background tasks, and causes all future claims to fail
    pub async fn terminate(&self) {
        let _termination_result = self.inner.terminate().await;
        self.terminated.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Dropping the pool means that qorb may have background tasks, which
        // may send requests even after this "drop" point.
        //
        // When we drop the qorb pool, we'll attempt to cancel those tasks, but
        // it's possible for these tasks to keep nudging slightly forward if
        // we're using a multi-threaded async executor.
        //
        // With this check, we'll warn if the pool is dropped without
        // terminating these worker tasks.
        if !self.terminated.load(std::sync::atomic::Ordering::SeqCst) {
            error!(
                self.log,
                "Pool dropped without invoking `terminate`. qorb background tasks
                 should be cancelled, but they may briefly still be initializing connections"
            );
        }
    }
}

/// DataStore quiesce configuration and state
#[derive(Debug, Clone)]
pub(crate) struct Quiesce {
    new_claims_allowed: ClaimsAllowed,
    claims_held: IdOrdMap<HeldDbClaimInfo>,
}

/// Policy determining whether new database claims are allowed
///
/// This is used by Nexus quiesce to disallow creating new database connections
/// when we're trying to quiesce Nexus.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ClaimsAllowed {
    /// New claims may be made (normal condition)
    Allowed,
    /// New claims may not be made (happens during quiesce)
    Disallowed,
}

struct ClaimReleaser {
    id: u64,
    tracker: watch::Sender<Quiesce>,
}

impl ClaimReleaser {
    fn new(id: u64, tracker: watch::Sender<Quiesce>) -> ClaimReleaser {
        ClaimReleaser { id, tracker }
    }
}

impl Drop for ClaimReleaser {
    fn drop(&mut self) {
        self.tracker.send_modify(|q| {
            q.claims_held
                .remove(&self.id)
                .expect("claim should still be present");
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::pub_test_utils::crdb;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_pool_can_be_terminated() {
        let logctx = dev::test_setup_log("test_pool_can_be_terminated");
        let log = &logctx.log;
        let mut db = crdb::test_setup_database(log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        {
            let pool = Pool::new_single_host(&log, &cfg);
            pool.terminate().await;
        }
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Regression test against https://github.com/oxidecomputer/omicron/issues/7821
    //
    // Dropping the pool without termination should not cause a panic anymore.
    #[tokio::test]
    async fn test_pool_drop_does_not_panic() {
        let logctx = dev::test_setup_log("test_pool_drop_does_not_panic");
        let log = &logctx.log;
        let mut db = crdb::test_setup_database(log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        {
            let pool = Pool::new_single_host(&log, &cfg);
            drop(pool);
        }
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
