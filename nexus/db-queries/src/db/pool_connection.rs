// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Customization that happens on each connection as they're acquired.

use async_bb8_diesel::AsyncSimpleConnection;
use async_bb8_diesel::Connection;
use async_bb8_diesel::ConnectionError;
use async_trait::async_trait;
use bb8::CustomizeConnection;
use diesel::PgConnection;
use diesel_dtrace::DTraceConnection;

pub type DbConnection = DTraceConnection<PgConnection>;

pub const DISALLOW_FULL_TABLE_SCAN_SQL: &str =
    "set disallow_full_table_scans = on; set large_full_scan_rows = 0;";

/// A customizer for all new connections made to CockroachDB, from Diesel.
#[derive(Debug)]
pub(crate) struct ConnectionCustomizer {}

impl ConnectionCustomizer {
    pub(crate) fn new() -> Self {
        Self {}
    }

    async fn disallow_full_table_scans(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        conn.batch_execute_async(DISALLOW_FULL_TABLE_SCAN_SQL).await?;
        Ok(())
    }
}

#[async_trait]
impl CustomizeConnection<Connection<DbConnection>, ConnectionError>
    for ConnectionCustomizer
{
    async fn on_acquire(
        &self,
        conn: &mut Connection<DbConnection>,
    ) -> Result<(), ConnectionError> {
        self.disallow_full_table_scans(conn).await?;
        Ok(())
    }
}
