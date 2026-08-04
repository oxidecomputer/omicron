// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Customization that happens on each connection as they're acquired.

use anyhow::anyhow;
use async_bb8_diesel::AsyncR2D2Connection;
use async_bb8_diesel::AsyncSimpleConnection;
use async_trait::async_trait;
use diesel::Connection;
use nexus_db_lookup::DbConnection;
use qorb::backend::{self, Backend, Error};
use slog::Logger;
use url::Url;

pub const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

/// A [backend::Connector] which provides access to
/// [`PgConnection`](diesel::pg::PgConnection).
pub(crate) struct DieselPgConnector {
    log: Logger,
    user: String,
    db: String,
    args: Vec<(String, String)>,
}

pub(crate) struct DieselPgConnectorArgs<'a> {
    pub(crate) user: &'a str,
    pub(crate) db: &'a str,
    pub(crate) args: Vec<(&'a str, &'a str)>,
}

impl DieselPgConnector {
    /// Creates a new "connector" to a database, which
    /// swaps out the IP address at runtime depending on the selected backend.
    ///
    /// Format of the url is:
    ///
    /// - postgresql://{user}@{address}/{db}
    ///
    /// Or, if arguments are supplied:
    ///
    /// - postgresql://{user}@{address}/{db}?{args}
    pub(crate) fn new(log: &Logger, args: DieselPgConnectorArgs<'_>) -> Self {
        let DieselPgConnectorArgs { user, db, args } = args;
        Self {
            log: log.clone(),
            user: user.to_string(),
            db: db.to_string(),
            args: args
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn to_url(
        &self,
        address: std::net::SocketAddr,
    ) -> Result<String, anyhow::Error> {
        let user = &self.user;
        let db = &self.db;
        let mut url =
            Url::parse(&format!("postgresql://{user}@{address}/{db}"))?;

        for (k, v) in &self.args {
            url.query_pairs_mut().append_pair(k, v);
        }

        Ok(url.as_str().to_string())
    }
}

#[async_trait]
impl backend::Connector for DieselPgConnector {
    type Connection = async_bb8_diesel::Connection<DbConnection>;

    async fn connect(
        &self,
        backend: &Backend,
    ) -> Result<Self::Connection, Error> {
        let url = self.to_url(backend.address).map_err(Error::Other)?;

        let conn = tokio::task::spawn_blocking(move || {
            let pg_conn = DbConnection::establish(&url)
                .map_err(|e| Error::Other(anyhow!(e)))?;
            Ok::<_, Error>(async_bb8_diesel::Connection::new(pg_conn))
        })
        .await
        .expect("Task panicked establishing connection")
        .inspect_err(|e| {
            warn!(
                self.log,
                "Failed to make connection";
                "error" => e.to_string(),
                "backend" => backend.address,
            );
        })?;
        Ok(conn)
    }

    async fn on_acquire(
        &self,
        conn: &mut Self::Connection,
    ) -> Result<(), Error> {
        conn.batch_execute_async(DISALLOW_FULL_TABLE_SCAN_SQL).await.map_err(
            |e| {
                warn!(
                    self.log,
                    "Failed on_acquire execution";
                    "error" => e.to_string()
                );
                Error::Other(anyhow!(e))
            },
        )?;
        Ok(())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Error> {
        let is_broken = conn.is_broken_async().await;
        if is_broken {
            warn!(
                self.log,
                "Failed is_valid check; connection known to be broken"
            );
            return Err(Error::Other(anyhow!("Connection broken")));
        }
        conn.ping_async().await.map_err(|e| {
            warn!(
                self.log,
                "Failed is_valid check; connection failed ping";
                "error" => e.to_string()
            );
            Error::Other(anyhow!(e))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_test_utils::dev;
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::time::Duration;

    const KEEPALIVE_ARGS: &[(&str, &str)] = &[
        ("sslmode", "disable"),
        ("keepalives", "1"),
        ("keepalives_idle", "10"),
        ("keepalives_interval", "10"),
        ("keepalives_count", "12"),
    ];

    // Regression test for oxidecomputer/omicron#10668. Nexus once held a
    // dead CockroachDB connection for 45+ minutes because libpq's
    // keepalive settings were left at their slow defaults. This checks
    // that our connection URL carries the new keepalive settings. It only
    // checks the string, not whether the OS applies them; see
    // `connection_actually_gets_tcp_keepalive_settings` below for that.
    #[test]
    fn to_url_includes_keepalive_args() {
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let connector = DieselPgConnector::new(
            &log,
            DieselPgConnectorArgs {
                user: "root",
                db: "omicron",
                args: KEEPALIVE_ARGS.to_vec(),
            },
        );
        let addr: std::net::SocketAddr = "127.0.0.1:5432".parse().unwrap();
        let url = connector.to_url(addr).expect("failed to build URL");

        assert!(url.contains("sslmode=disable"));
        assert!(url.contains("keepalives=1"));
        assert!(url.contains("keepalives_idle=10"));
        assert!(url.contains("keepalives_interval=10"));
        assert!(url.contains("keepalives_count=12"));
    }

    // Opens a real connection to a real (test) CockroachDB with our exact
    // connection args, then reads back the TCP socket's real kernel
    // keepalive settings.
    //
    // Diesel's `PgConnection` hides the underlying socket, so we can't
    // inspect our own connector's connection directly. Instead we open a
    // second, plain libpq connection with the same connection string and
    // inspect that one. libpq is what actually sets these options via
    // `setsockopt`, so this checks real behavior either way.
    //
    // This works on Linux, macOS, and illumos (production) with no
    // platform-specific code. `socket2`'s `keepalive_time` /
    // `keepalive_interval` / `keepalive_retries` already handle each OS's
    // different option names. For example, macOS reports `TCP_KEEPIDLE`
    // as `TCP_KEEPALIVE`. illumos has its own older `TCP_KEEPALIVE_THRESHOLD`,
    // but also supports the same `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`
    // names Linux uses. That's what libpq sets, and what this test reads
    // back.
    #[tokio::test]
    async fn connection_actually_gets_tcp_keepalive_settings() {
        let logctx = dev::test_setup_log(
            "connection_actually_gets_tcp_keepalive_settings",
        );
        let mut db =
            crate::db::pub_test_utils::crdb::test_setup_database(&logctx.log)
                .await;

        let connector = DieselPgConnector::new(
            &logctx.log,
            DieselPgConnectorArgs {
                user: "root",
                db: "omicron",
                args: KEEPALIVE_ARGS.to_vec(),
            },
        );
        let url = connector
            .to_url(db.pg_config().address())
            .expect("failed to build URL");

        // `PQconnectdb` and friends are blocking calls; libpq has no
        // async story of its own; this is exactly what `spawn_blocking`
        // is for.
        let (raw_fd, connected) = tokio::task::spawn_blocking(move || {
            let c_url = CString::new(url).expect("URL had an embedded NUL");
            // Safety: `PQconnectdb` is libpq's ordinary synchronous
            // connect call. We own the returned `PGconn` and are
            // responsible for passing it to `PQfinish`, which we do
            // below on every path.
            let conn = unsafe { pq_sys::PQconnectdb(c_url.as_ptr()) };
            let connected =
                unsafe { pq_sys::PQstatus(conn) } == pq_sys::CONNECTION_OK;
            let dup_fd = connected.then(|| {
                // Safety: `PQsocket` only reads a field off the
                // connection; it doesn't transfer ownership of the fd.
                // We `dup` it so that `socket2::Socket`'s `Drop` (which
                // closes what it owns) can't close libpq's copy out from
                // under it. libpq closes its own fd normally when we call
                // `PQfinish` below.
                let raw = unsafe { pq_sys::PQsocket(conn) };
                unsafe { libc::dup(raw) }
            });
            // Safety: `conn` is a valid, owned `PGconn` (or null, which
            // `PQfinish` accepts and ignores).
            unsafe { pq_sys::PQfinish(conn) };
            (dup_fd, connected)
        })
        .await
        .expect("blocking task panicked");

        assert!(connected, "failed to connect to test CockroachDB");
        let fd = raw_fd.expect("connected but didn't capture a socket fd");

        // Safety: `fd` came from `libc::dup` just above; we exclusively
        // own this copy and nothing else will read, write, or close it.
        let socket = unsafe { socket2::Socket::from_raw_fd(fd) };

        assert!(
            socket.keepalive().expect("failed to read SO_KEEPALIVE"),
            "expected SO_KEEPALIVE to be enabled"
        );
        assert_eq!(
            socket.keepalive_time().expect("failed to read TCP_KEEPIDLE"),
            Duration::from_secs(10),
            "keepalives_idle=10 should set a 10 second idle time",
        );
        assert_eq!(
            socket.keepalive_interval().expect("failed to read TCP_KEEPINTVL"),
            Duration::from_secs(10),
            "keepalives_interval=10 should set a 10 second probe interval",
        );
        assert_eq!(
            socket.keepalive_retries().expect("failed to read TCP_KEEPCNT"),
            12,
            "keepalives_count=12 should set 12 allowed probe failures",
        );

        db.cleanup().await.expect("failed to clean up test database");
        logctx.cleanup_successful();
    }
}
