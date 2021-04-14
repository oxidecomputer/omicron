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
 * TODO-design Need TLS support (the types below hardcode NoTls).
 */

use super::Config as DbConfig;
use bb8_postgres::PostgresConnectionManager;
use omicron_common::error::ApiError;
use std::ops::Deref;

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

    pub async fn acquire(&self) -> Result<Conn<'_>, ApiError> {
        /*
         * TODO-design It would be better to provide more detailed error
         * information here so that we could monitor various kinds of failures.
         * It would also be nice if callers could provide parameters for, e.g.,
         * how long to wait for a connection here.  Really, it would be nice if
         * they could create their own handles to the connection pool with
         * parameters like this.  It could also have its own logger.
         */
        self.pool.get().await.map(|conn| Conn { conn }).map_err(|e| {
            ApiError::ServiceUnavailable {
                message: format!(
                    "failed to acquire database connection: {}",
                    e.to_string()
                ),
            }
        })
    }
}
