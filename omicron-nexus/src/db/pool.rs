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
use async_bb8_diesel::DieselConnectionManager;
use diesel::PgConnection;
use omicron_common::api::external::Error;

// NOTE: This structure abstracts a connection pool.
// It *currently* wraps both a BB8 (async) and R2D2 (sync)
// connection to Postgres.
//
// Long-term, it would be ideal to migrate to a single
// (probably async) connection.
pub struct Pool {
    pool_diesel: bb8::Pool<DieselConnectionManager<diesel::PgConnection>>,
}

impl Pool {
    pub fn new(db_config: &DbConfig) -> Self {
        let manager =
            DieselConnectionManager::<PgConnection>::new(&db_config.url.url());
        let pool_diesel = bb8::Builder::new().build_unchecked(manager);
        Pool { pool_diesel }
    }

    pub fn pool(&self) -> &bb8::Pool<DieselConnectionManager<diesel::PgConnection>> {
        &self.pool_diesel
    }

    pub async fn acquire_diesel(
        &self,
    ) -> Result<
        bb8::PooledConnection<'_, DieselConnectionManager<PgConnection>>,
        Error,
    > {
        self.pool_diesel.get().await.map_err(|e| Error::ServiceUnavailable {
            message: format!(
                "failed to acquire database connection: {}",
                e.to_string()
            ),
        })
    }
}
