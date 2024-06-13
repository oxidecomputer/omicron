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

pub type DbConnection = DTraceConnection<PgConnection>;

pub const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

/// A [backend::Connector] which provides access to [PgConnection].
pub struct DieselPgConnector {
    prefix: String,
    suffix: String,
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
    pub fn new(user: &str, db: &str, args: Option<&str>) -> Self {
        Self {
            prefix: format!("postgresql://{user}@"),
            suffix: format!(
                "/{db}{}",
                args.map(|args| format!("?{args}")).unwrap_or("".to_string())
            ),
        }
    }

    fn to_url(&self, address: std::net::SocketAddr) -> String {
        format!(
            "{prefix}{address}{suffix}",
            prefix = self.prefix,
            suffix = self.suffix,
        )
    }
}

#[async_trait]
impl backend::Connector for DieselPgConnector {
    type Connection = async_bb8_diesel::Connection<DbConnection>;

    async fn connect(
        &self,
        backend: &Backend,
    ) -> Result<Self::Connection, Error> {
        let url = self.to_url(backend.address);

        let conn = tokio::task::spawn_blocking(move || {
            let pg_conn = DbConnection::establish(&url)
                .map_err(|e| Error::Other(anyhow!(e)))?;
            Ok::<_, Error>(async_bb8_diesel::Connection::new(pg_conn))
        })
        .await
        .expect("Task panicked establishing connection")?;
        Ok(conn)
    }

    async fn on_acquire(
        &self,
        conn: &mut Self::Connection,
    ) -> Result<(), Error> {
        conn.batch_execute_async(DISALLOW_FULL_TABLE_SCAN_SQL)
            .await
            .map_err(|e| Error::Other(anyhow!(e)))?;
        Ok(())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Error> {
        let is_broken = conn.is_broken_async().await;
        if is_broken {
            return Err(Error::Other(anyhow!("Connection broken")));
        }
        conn.ping_async().await.map_err(|e| Error::Other(anyhow!(e)))
    }
}
