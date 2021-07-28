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
use bb8_postgres::PostgresConnectionManager;
use omicron_common::api::external::Error;
use std::ops::Deref;

#[derive(Debug)]
pub struct Pool {
    pool: bb8::Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
}

pub struct Conn<'a> {
    conn: bb8::PooledConnection<
        'a,
        PostgresConnectionManager<tokio_postgres::NoTls>,
    >,
}

impl<'a> Deref for Conn<'a> {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        self.conn.deref()
    }
}

impl Pool {
    pub fn new(db_config: &DbConfig) -> Self {
        let mgr = bb8_postgres::PostgresConnectionManager::new(
            (*db_config.url).clone(),
            tokio_postgres::NoTls,
        );
        let pool = bb8::Builder::new().build_unchecked(mgr);
        Pool { pool }
    }

    pub async fn acquire(&self) -> Result<Conn<'_>, Error> {
        /*
         * TODO-design It would be better to provide more detailed error
         * information here so that we could monitor various kinds of failures.
         * It would also be nice if callers could provide parameters for, e.g.,
         * how long to wait for a connection here.  Really, it would be nice if
         * they could create their own handles to the connection pool with
         * parameters like this.  It could also have its own logger.
         */
        self.pool.get().await.map(|conn| Conn { conn }).map_err(|e| {
            Error::ServiceUnavailable {
                message: format!(
                    "failed to acquire database connection: {}",
                    e.to_string()
                ),
            }
        })
    }
}
