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
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;

// TCP KeepAlive parameters used for connections to CockroachDB.
//
// These values result in terminating connections after 2 minutes of being
// unable to reach the other side.
//
// Recall that TCP KeepAlive is processed by the networking stack without the
// help of the application.  No amount of database slowness should trigger these
// timeouts.
const CFG_TCP_KEEPIDLE: Duration = Duration::from_secs(30);
const CFG_TCP_KEEPINTVL: Duration = Duration::from_secs(10);
const CFG_TCP_KEEPCNT: u32 = 12;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    // IDs are assigned to each connection, acting as keys within the Quiesce
    // state.  These are used to track the set of in-use connections and
    // associated metadata.
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
    //
    // We also enable TCP keepalive as configured above.
    let user = "root";
    let db = "omicron";
    let tcp_keepidle_str = CFG_TCP_KEEPIDLE.as_secs().to_string();
    let tcp_keepintvl_str = CFG_TCP_KEEPINTVL.as_secs().to_string();
    let tcp_keepcnt_str = CFG_TCP_KEEPCNT.to_string();
    let args = vec![
        ("sslmode", "disable"),
        ("keepalives", "1"),
        ("keepalives_idle", &tcp_keepidle_str),
        ("keepalives_interval", &tcp_keepintvl_str),
        ("keepalives_count", &tcp_keepcnt_str),
    ];

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

    /// Creates a new qorb-backed connection pool to a fixed set of database
    /// addresses.
    ///
    /// Unlike [`Self::new_single_host`], this constructor accepts multiple
    /// addresses and will connect to whichever backend is healthy. This is
    /// intended for tools like `omdb` that may resolve multiple CockroachDB
    /// addresses via DNS, and which don't necessarily want to re-resolve.
    pub fn new_fixed_hosts(log: &Logger, addresses: Vec<SocketAddr>) -> Self {
        let resolver: qorb::resolver::BoxedResolver =
            Box::new(qorb::resolvers::fixed::FixedResolver::new(addresses));
        let connector = make_postgres_connector(log);
        let policy = Policy::default();
        let inner = match qorb::pool::Pool::new(
            "crdb-fixed-hosts".to_string(),
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
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let held_since = Utc::now();
        let debug = Backtrace::force_capture().to_string();
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

        // `ClaimReleaser` cleans up the entry that we just added to
        // `claims_held`.  It's important not to add any early return paths
        // before creating this object because that would leak the reference and
        // prevent Nexus from quiescing.
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

    /// Returns a connection from the pool, bypassing the quiesce check
    ///
    /// This is only intended for use *during* quiesce to update our final
    /// database record.
    pub async fn claim_quiesced(
        &self,
    ) -> Result<qorb::claim::Handle<AsyncConnection>, Error> {
        self.inner.claim().await.map_err(|err| {
            Error::unavail(&format!("Failed to access DB connection: {err}"))
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
        rx.wait_for(|q| q.is_fully_quiesced()).await.unwrap();
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

/// Database quiesce configuration and state
#[derive(Debug, Clone)]
pub(crate) struct Quiesce {
    /// whether new claims are allowed right now
    new_claims_allowed: ClaimsAllowed,

    /// set of claims currently held
    claims_held: IdOrdMap<HeldDbClaimInfo>,
}

impl Quiesce {
    /// Returns whether database access is fully (and permanently) quiesced
    fn is_fully_quiesced(&self) -> bool {
        matches!(self.new_claims_allowed, ClaimsAllowed::Disallowed)
            && self.claims_held.is_empty()
    }
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

/// Object used to clean up tracking of a held claim, potentially unblocking
/// the quiesce process
///
/// This gets attached to a `DataStoreConnection`.  When it gets dropped, this
/// gets dropped.  That's when we clean up the entry in the quiesce state.
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
    use camino::Utf8Path;
    use omicron_test_utils::dev;
    use socket2::Socket;
    use std::os::fd::FromRawFd;
    use std::os::fd::IntoRawFd;

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

    /// Verifies that connections created by the pool do in practice have the
    /// right TCP keep-alive options set.
    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn test_keep_alive_set() {
        // Set up the test database.
        let logctx = dev::test_setup_log("test_pool_drop_does_not_panic");
        let log = &logctx.log;
        let mut db = crdb::test_setup_database(log).await;

        // Create a pool.  Make sure there's a connection established to the
        // database.
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = Pool::new_single_host(&log, &cfg);
        let _conn = pool.claim().await.expect("established db connection");
        let peer_addr = db.pg_config().address();

        // We want to know that the underlying socket for this connection really
        // has TCP keep-alive configured correctly.  (One might also like to
        // verify that it *works* correctly, but that's beyond the scope of this
        // test suite.)  To do that, we want to look at the configuration on the
        // fd itself.  Unfortunately, Diesel doesn't make that available to us.
        //
        // Here's where things get gross.  We're going to look at all open fds
        // in this process to see which one is our connection.  Beware that
        // other components in this process could be opening and closing fds as
        // we iterate them.  And the ones we find could be any kind of fd.  So
        // we'll ignore all errors here.  Absurdity aside, we can say a few
        // things:
        //
        // 1. The fd from our connection must be in this list.  We should be
        //    able to call `getpeername()` and `getsockopt()` on it without
        //    errors.
        //
        // 2. For any fd that's not from this pool, either it's not open by the
        //    time we get to it, or `getpeername()` should fail on it (because
        //    it's not a socket), or the peer must not be our CockroachDB
        //    instance.  That's because nothing else should spontaneously
        //    connect to the test database that we just set up.
        //
        // Thus, as long as we ignore most errors, we should find at least one
        // fd that's correctly set up.
        let mut found = false;
        let fd_dirents = Utf8Path::new("/proc/self/fd")
            .read_dir_utf8()
            .expect("read /proc/self/fd");
        for maybe_dirent in fd_dirents {
            // Ignore failures traversing the directory.
            let Ok(dirent) = maybe_dirent else {
                continue;
            };

            // Ignore anything that doesn't parse as a number.  It's not an fd.
            let maybe_fd_str = dirent.file_name();
            let Ok(fd_num) = i32::from_str_radix(maybe_fd_str, 10) else {
                continue;
            };

            // See if it's a networking socket connected to the right address.
            // unsafe(): we're not doing any mutating operations and we
            // correctly handle read operations on a closed fd or one that's
            // different than the fd we're looking for.
            let socket = unsafe { Socket::from_raw_fd(fd_num) };
            // Ignore anything that's not a socket with a valid peer address.
            let Ok(socket_peer) = socket.peer_addr() else {
                // `FromRawFd` is ambiguous about whether `self` (`Socket` in
                // this case) becomes responsible for closing the fd.  To be
                // safe, use `IntoRawFd` to get it back out.
                let _ = socket.into_raw_fd();
                continue;
            };

            // Ignore anything whose peer is not our CockroachDB server.
            if socket_peer != peer_addr.into() {
                // See above.
                let _ = socket.into_raw_fd();
                continue;
            };

            // We've finally found an fd corresponding to a socket connected to
            // our CockroachDB server.  We assume this is from our pool.
            // (Otherwise, how did somebody discover our address to make a
            // connection to it?)  It must therefore have TCP keepalive set.
            println!("found client fd: {fd_num}");

            let so_keepalive =
                socket.keepalive().expect("failed to read SO_KEEPALIVE");
            assert!(
                so_keepalive,
                "socket does not have SO_KEEPALIVE set to true"
            );

            let tcp_keepidle =
                socket.keepalive_time().expect("failed to read TCP_KEEPIDLE");
            assert_eq!(tcp_keepidle, CFG_TCP_KEEPIDLE);

            let tcp_keepintvl = socket
                .keepalive_interval()
                .expect("failed to read TCP_KEEPINTVL");
            assert_eq!(tcp_keepintvl, CFG_TCP_KEEPINTVL);

            let tcp_keepcnt =
                socket.keepalive_retries().expect("failed to read TCP_KEEPCNT");
            assert_eq!(tcp_keepcnt, CFG_TCP_KEEPCNT);

            // See above.
            let _ = socket.into_raw_fd();
            found = true;
            break;
        }

        if !found {
            panic!("found no fd matching our database connection");
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
