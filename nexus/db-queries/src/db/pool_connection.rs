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
    use qorb::backend::Connector;
    use socket2::Socket;
    use std::fs;
    use std::net::{IpAddr, SocketAddr};
    use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
    use std::time::Duration;

    // Regression test for https://github.com/oxidecomputer/omicron/issues/10668.
    // Finds the database's connection file descriptor (fd) by matching its
    // peer address and reads keepalive settings from the socket.
    #[tokio::test]
    async fn connection_has_real_tcp_keepalive_set() {
        let logctx =
            dev::test_setup_log("connection_has_real_tcp_keepalive_set");
        let mut db = dev::test_setup_database(
            &logctx.log,
            dev::StorageSource::DoNotPopulate,
        )
        .await;

        let db_url_str = db.pg_config().to_string();
        let parsed = url::Url::parse(&db_url_str)
            .expect("pg_config() did not produce a parseable URL");
        let ip = match parsed.host() {
            Some(url::Host::Ipv4(v4)) => IpAddr::V4(v4),
            Some(url::Host::Ipv6(v6)) => IpAddr::V6(v6),
            other => panic!(
                "expected pg_config() host to be a literal IP, got {other:?}"
            ),
        };
        let port = parsed.port().expect("pg_config() URL has no port");
        let addr = SocketAddr::new(ip, port);
        let backend = backend::Backend { address: addr };

        let connector = DieselPgConnector::new(
            &logctx.log,
            DieselPgConnectorArgs {
                user: "root",
                db: "omicron",
                args: vec![
                    ("sslmode", "disable"),
                    ("keepalives", "1"),
                    ("keepalives_idle", "10"),
                    ("keepalives_interval", "10"),
                    ("keepalives_count", "12"),
                ],
            },
        );

        let _conn = connector
            .connect(&backend)
            .await
            .expect("failed to establish connection");

        // Fail if multiple connections are open in the process.
        // Prevents silently using the wrong socket.
        let fd = match find_fds_with_peer_addr(addr).as_slice() {
            [fd] => *fd,
            [] => panic!("no open fd found with peer address {addr}"),
            multiple => panic!(
                "found {} fds with peer address {addr}, expected 1",
                multiple.len()
            ),
        };

        let (idle, interval, retries) = read_keepalive(fd)
            .expect("getsockopt failed on matched connection fd");

        assert!(
            idle <= Duration::from_secs(30),
            "keepalive idle too high: {idle:?}"
        );
        assert!(
            interval <= Duration::from_secs(10),
            "keepalive interval too high: {interval:?}"
        );
        assert!(retries >= 3, "keepalive retry count too low: {retries}");

        db.cleanup().await.expect("failed to clean up test database");
        logctx.cleanup_successful();
    }

    /// Returns every open fd in this process whose peer address is `addr`.
    fn find_fds_with_peer_addr(addr: SocketAddr) -> Vec<RawFd> {
        let Ok(entries) = fs::read_dir("/proc/self/fd") else {
            return Vec::new();
        };
        let mut matches = Vec::new();
        for entry in entries.flatten() {
            let Ok(fd) = entry.file_name().to_string_lossy().parse::<RawFd>()
            else {
                continue;
            };

            // Read the opened fd in this process. Don't close socket we don't
            // own. Return ownership with into_raw_fd.
            let socket = unsafe { Socket::from_raw_fd(fd) };
            let matched = socket
                .peer_addr()
                .ok()
                .and_then(|peer| peer.as_socket())
                .is_some_and(|sock_addr| sock_addr == addr);
            let _ = socket.into_raw_fd();

            if matched {
                matches.push(fd);
            }
        }
        matches
    }

    /// Read TCP keepalive counts for a raw fd.
    fn read_keepalive(fd: RawFd) -> std::io::Result<(Duration, Duration, u32)> {
        // Read the opened fd in this process. Don't close socket we don't
        // own. Return ownership with into_raw_fd.
        let socket = unsafe { Socket::from_raw_fd(fd) };
        let result = (|| -> std::io::Result<(Duration, Duration, u32)> {
            Ok((
                socket.keepalive_time()?,
                socket.keepalive_interval()?,
                socket.keepalive_retries()?,
            ))
        })();
        let _ = socket.into_raw_fd();
        result
    }
}
