// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Database connection pooling
 */
/*
 * This whole thing is a placeholder for prototyping.
 *
 * TODO-robustness TODO-resilience We will want to carefully think about the
 * connection pool that we use and its parameters.  It's not clear from the
 * survey so far whether an existing module is suitable for our purposes.  See
 * the Cueball Internals document for details on the sorts of behaviors we'd
 * like here.  Even if by luck we stick with bb8, we definitely want to think
 * through the various parameters.
 *
 * Notes about bb8's behavior:
 * * When the database is completely offline, and somebody wants a connection,
 *   it still waits for the connection timeout before giving up.  That seems
 *   like not what we want.  (To be clear, this is a failure mode where we know
 *   the database is offline, not one where it's partitioned and we can't tell.)
 * * Although the `build_unchecked()` builder allows the pool to start up with
 *   no connections established (good), it also _seems_ to not establish any
 *   connections even when it could, resulting in a latency bubble for the first
 *   operation after startup.  That's not what we're looking for.
 *
 * TODO-design Need TLS support (the types below hardcode NoTls).
 */

use super::Config as DbConfig;
use anyhow::anyhow;
use async_bb8_diesel::AsyncSimpleConnection;
use async_bb8_diesel::ConnectionManager;
use diesel::PgConnection;
use diesel_dtrace::DTraceConnection;
use omicron_common::backoff;
use slog::Logger;

pub type DbConnection = DTraceConnection<PgConnection>;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    pool: bb8::Pool<ConnectionManager<DbConnection>>,
}

impl Pool {
    pub fn new(db_config: &DbConfig) -> Self {
        let manager =
            ConnectionManager::<DbConnection>::new(&db_config.url.url());
        let pool = bb8::Builder::new().build_unchecked(manager);
        Pool { pool }
    }

    /// Returns a reference to the underlying pool.
    pub fn pool(&self) -> &bb8::Pool<ConnectionManager<DbConnection>> {
        &self.pool
    }

    /// Contact CockroachDB using exponential backoff until it appears alive.
    ///
    /// Although liveness of CRDB is not guaranteed generally (such is the
    /// nature of distributed systems), this check lets a caller avoid a
    /// race condition during initialization, where Nexus may boot before
    /// the database.
    pub async fn wait_for_cockroachdb(&self, log: &Logger) {
        let check_health = || async {
            let conn =
                self.pool.get().await.map_err(|e| {
                    backoff::BackoffError::Transient(anyhow!(e))
                })?;

            // TODO: Is this command sufficient? Don't we want to show that CRDB
            // is up *and* that the tables have been populated?
            conn.batch_execute_async("SHOW DATABASES;")
                .await
                .map_err(|e| backoff::BackoffError::Transient(anyhow!(e)))
        };
        let log_failure = |_, _| {
            warn!(log, "cockroachdb not yet alive");
        };
        backoff::retry_notify(
            backoff::internal_service_policy(),
            check_health,
            log_failure,
        )
        .await
        .expect("expected an infinite retry loop waiting for crdb");

        info!(log, "CockroachDB appears online");
    }
}
