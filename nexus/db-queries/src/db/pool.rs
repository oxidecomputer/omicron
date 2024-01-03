// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database connection pooling
// This whole thing is a placeholder for prototyping.
//
// TODO-robustness TODO-resilience We will want to carefully think about the
// connection pool that we use and its parameters.  It's not clear from the
// survey so far whether an existing module is suitable for our purposes.  See
// the Cueball Internals document for details on the sorts of behaviors we'd
// like here.  Even if by luck we stick with bb8, we definitely want to think
// through the various parameters.
//
// Notes about bb8's behavior:
// * When the database is completely offline, and somebody wants a connection,
//   it still waits for the connection timeout before giving up.  That seems
//   like not what we want.  (To be clear, this is a failure mode where we know
//   the database is offline, not one where it's partitioned and we can't tell.)
// * Although the `build_unchecked()` builder allows the pool to start up with
//   no connections established (good), it also _seems_ to not establish any
//   connections even when it could, resulting in a latency bubble for the first
//   operation after startup.  That's not what we're looking for.
//
// TODO-design Need TLS support (the types below hardcode NoTls).

use super::Config as DbConfig;
use async_bb8_diesel::ConnectionError;
use async_bb8_diesel::ConnectionManager;

pub use super::pool_connection::DbConnection;

/// Wrapper around a database connection pool.
///
/// Expected to be used as the primary interface to the database.
pub struct Pool {
    pool: bb8::Pool<ConnectionManager<DbConnection>>,
}

impl Pool {
    pub fn new(log: &slog::Logger, db_config: &DbConfig) -> Self {
        // Make sure diesel-dtrace's USDT probes are enabled.
        usdt::register_probes().expect("Failed to register USDT DTrace probes");
        Self::new_builder(log, db_config, bb8::Builder::new())
    }

    pub fn new_failfast_for_tests(
        log: &slog::Logger,
        db_config: &DbConfig,
    ) -> Self {
        Self::new_builder(
            log,
            db_config,
            bb8::Builder::new()
                .connection_timeout(std::time::Duration::from_millis(1)),
        )
    }

    fn new_builder(
        log: &slog::Logger,
        db_config: &DbConfig,
        builder: bb8::Builder<ConnectionManager<DbConnection>>,
    ) -> Self {
        let url = db_config.url.url();
        let log = log.new(o!(
            "database_url" => url.clone(),
            "component" => "db::Pool"
        ));
        info!(&log, "database connection pool");
        let error_sink = LoggingErrorSink::new(log);
        let manager = ConnectionManager::<DbConnection>::new(url);
        let pool = builder
            .connection_customizer(Box::new(
                super::pool_connection::ConnectionCustomizer::new(),
            ))
            .error_sink(Box::new(error_sink))
            .build_unchecked(manager);
        Pool { pool }
    }

    /// Returns a reference to the underlying pool.
    pub fn pool(&self) -> &bb8::Pool<ConnectionManager<DbConnection>> {
        &self.pool
    }
}

#[derive(Clone, Debug)]
struct LoggingErrorSink {
    log: slog::Logger,
}

impl LoggingErrorSink {
    fn new(log: slog::Logger) -> LoggingErrorSink {
        LoggingErrorSink { log }
    }
}

impl bb8::ErrorSink<ConnectionError> for LoggingErrorSink {
    fn sink(&self, error: ConnectionError) {
        error!(
            &self.log,
            "database connection error";
            "error_message" => #%error
        );
    }

    fn boxed_clone(&self) -> Box<dyn bb8::ErrorSink<ConnectionError>> {
        Box::new(self.clone())
    }
}
