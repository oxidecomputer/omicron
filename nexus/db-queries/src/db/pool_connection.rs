// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Customization that happens on each connection as they're acquired.

use anyhow::anyhow;
use async_bb8_diesel::AsyncR2D2Connection;
use async_bb8_diesel::AsyncSimpleConnection;
use async_trait::async_trait;
use diesel::Connection;
use diesel::PgConnection;
use diesel_dtrace::DTraceConnection;
use qorb::backend::{self, Backend, Error};
use slog::Logger;
use url::Url;

pub type DbConnection = DTraceConnection<PgConnection>;

pub const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

/// A [backend::Connector] which provides access to [PgConnection].
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
        .map_err(|e| {
            warn!(
                self.log,
                "Failed to make connection";
                "error" => e.to_string(),
                "backend" => backend.address,
            );
            e
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
