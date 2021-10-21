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
use super::TracedPgConnection;
use async_bb8_diesel::ConnectionManager;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    pool: bb8::Pool<ConnectionManager<TracedPgConnection>>,
}

impl Pool {
    pub fn new(db_config: &DbConfig) -> Self {
        let manager =
            ConnectionManager::<TracedPgConnection>::new(&db_config.url.url());
        let pool = bb8::Builder::new().build_unchecked(manager);
        Pool { pool }
    }

    /// Returns a reference to the underlying pool.
    pub(crate) fn pool(
        &self,
    ) -> &bb8::Pool<ConnectionManager<TracedPgConnection>> {
        &self.pool
    }
}
