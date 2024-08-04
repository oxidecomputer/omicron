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
        timeout: std::time::Duration,
    ) -> Self {
        Self::new_builder(
            log,
            db_config,
            bb8::Builder::new().connection_timeout(timeout),
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

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Context;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::IntoSql;
    use omicron_test_utils::dev;
    use slog::Logger;

    async fn create_multinode_cluster(
        log: &Logger,
        ports: &Vec<u16>,
    ) -> Vec<dev::db::CockroachInstance> {
        // Spin up all the nodes, with "start"
        let mut crdb_nodes = vec![];
        for port in ports {
            let mut builder = dev::db::CockroachStarterBuilder::new();

            // Start in "multi-node" cluster mode, joining with the other nodes,
            // possibly before they're even up yet.
            builder
                .single_node(false)
                .listen_port(*port)
                .join_ports(ports.clone())
                .redirect_stdio_to_files();

            let starter = builder
                .build()
                .expect("Creating CockroachStarter should have worked");

            info!(
                log,
                "Preparing Cockroach node";
                "cmdline" => starter.cmdline().to_string(),
                "temp_dir" => starter.temp_dir().to_string_lossy().into_owned(),
            );
            let stderr_path = starter.temp_dir().join("cockroachdb_stderr");

            let node = starter
                .start()
                .await
                .unwrap_or_else(|err| {
                    let stderr = std::fs::read_to_string(stderr_path).unwrap_or(String::new());
                    panic!("Starting Cockroach node should have worked, Error:\n{err}\nstderr:\n{stderr}");
                });
            crdb_nodes.push(node);
        }

        // Initialize the cluster. Note that this doesn't populate the cluster
        // with any data, it's just a necessary one-time setup for Cockroach
        // itself.
        crdb_nodes[0].initialize_multinode(ports[0]).await.unwrap();

        crdb_nodes
    }

    // Issues a simple operation to verify that the pool is alive
    async fn simple_pool_query(pool: &Pool) -> anyhow::Result<()> {
        let conn =
            pool.pool().get().await.context("Failed to get connection")?;
        diesel::select(1_i32.into_sql::<diesel::sql_types::Integer>())
            .get_result_async::<i32>(&*conn)
            .await
            .context("Failed to execute query")?;
        Ok(())
    }

    // This test aims to verify that with a multi-node CRDB setup,
    // we can gracefully migrate between nodes that are remaining.
    #[tokio::test]
    async fn dead_cockroach_nodes_do_not_kill_connection() {
        let logctx =
            dev::test_setup_log("dead_cockroach_nodes_do_not_kill_connection");
        let log = &logctx.log;

        // These ports are chosen somewhat arbitrarily, though they are outside
        // the typical "ephemeral ranges" where port zero can be chosen.
        //
        // I wish it was easier to start a cluster of CockroachDB nodes
        // using ephemeral ports, but it seems like the "cockroach start"
        // command wants to have both the "--listen-addr" and "--join" addresses
        // known precisely ahead-of-time.
        let all_crdb_ports = vec![7709_u16, 7710_u16, 7711_u16];

        let mut crdb_nodes =
            create_multinode_cluster(log, &all_crdb_ports).await;

        // Spin up a pool to access each node
        let mut pools = vec![];
        for node in &crdb_nodes {
            let url = node.pg_config().clone();
            info!(log, "Connecting to {url}");

            let pool = Pool::new_failfast_for_tests(
                &log,
                &crate::db::Config { url },
                // Be cautious if you update this timeout duration. Even in
                // cases with a cluster of entirely healthy nodes, accessing
                // connections from the pool can spuriously fail with timeouts
                // if the connection timeout is too short.
                std::time::Duration::from_secs(2),
            );
            pools.push(pool);
        }

        for pool in &pools {
            simple_pool_query(pool).await.unwrap();
        }

        // Kill one of the Cockroach nodes, observe that the pool fails to
        // access the connection.
        crdb_nodes[0]
            .cleanup_forceful()
            .await
            .expect("Should have been able to clear node");
        info!(log, "Shut down first node");

        // Pool on dead node: Should not be able to access DB
        let err = simple_pool_query(&pools[0]).await.unwrap_err();
        assert!(
            err.to_string().contains("Failed to get connection"),
            "Observed Error: {err:?}"
        );

        // Pools on live nodes: Should be able to access DB
        info!(log, "Querying remaining live nodes");
        simple_pool_query(&pools[1]).await.unwrap();
        simple_pool_query(&pools[2]).await.unwrap();

        // Finish cleanup before completing the test
        for mut node in crdb_nodes.into_iter() {
            info!(log, "Test complete, shutting down node"; "url" => node.pg_config().to_string());
            node.cleanup_forceful().await.unwrap();
        }
        logctx.cleanup_successful();
    }
}
