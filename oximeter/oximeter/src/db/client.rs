//! Rust client to ClickHouse database
// Copyright 2021 Oxide Computer Company

use std::collections::BTreeMap;
use std::net::SocketAddr;

use crate::db::model;
use crate::{types::Sample, Error};

#[derive(Debug, Clone)]
pub struct Client {
    address: SocketAddr,
    url: String,
    client: reqwest::Client,
}

impl Client {
    /// Construct a new ClickHouse client of the database at `address`.
    pub async fn new(address: SocketAddr) -> Result<Self, Error> {
        let client = reqwest::Client::new();
        let url = format!("http://{}", address);
        let out = Self { address, url, client };
        out.ping().await?;
        Ok(out)
    }

    /// Ping the ClickHouse server to verify connectivitiy.
    pub async fn ping(&self) -> Result<(), Error> {
        self.client
            .get(format!("{}/ping", self.url))
            .send()
            .await
            .map_err(|e| Error::Database(e.to_string()))?
            .error_for_status()
            .map_err(|e| Error::Database(e.to_string()))?;
        Ok(())
    }

    /// Insert the given samples into the database.
    pub async fn insert_samples(
        &self,
        samples: &[Sample],
    ) -> Result<(), Error> {
        let mut rows = BTreeMap::new();
        for sample in samples.iter() {
            for (table_name, table_rows) in model::unroll_field_rows(sample) {
                rows.entry(table_name)
                    .or_insert_with(Vec::new)
                    .extend(table_rows);
            }
            let (table_name, measurement_row) =
                model::unroll_measurement_row(sample);
            rows.entry(table_name)
                .or_insert_with(Vec::new)
                .push(measurement_row);
        }
        for (table_name, rows) in rows {
            let body = format!(
                "INSERT INTO {table_name} FORMAT JSONEachRow\n{row_data}\n",
                table_name = table_name,
                row_data = rows.join("\n")
            );
            // TODO-robustness If this fails, it's likely that we'll have partially inserted
            // data. ClickHouse doesn't really support transactions or deleting data, so we
            // will probably have to validate this data against the schema prior to inserting
            // it. We should be doing that anyway, but there may be other considerations here
            // in the case that there's another kind of error, like a network outage, database
            // failure, ZooKeeper error, etc.
            self.execute(body).await?;
        }
        Ok(())
    }

    // Initialize ClickHouse with the database and metric table schema.
    #[allow(dead_code)]
    pub(crate) async fn init_db(&self) -> Result<(), Error> {
        // The HTTP client doesn't support multiple statements per query, so we break them out here
        // manually.
        let sql = include_str!("./db-init.sql");
        for query in sql.split("\n--\n") {
            self.execute(query.to_string()).await?;
        }
        Ok(())
    }

    // Wipe the ClickHouse database entirely.
    #[allow(dead_code)]
    pub(crate) async fn wipe_db(&self) -> Result<(), Error> {
        let sql = include_str!("./db-wipe.sql").to_string();
        self.execute(sql).await
    }

    // Execute a generic SQL statement.
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute(&self, sql: String) -> Result<(), Error> {
        let response = self
            .client
            .post(&self.url)
            .body(sql)
            .send()
            .await
            .map_err(|e| Error::Database(e.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            let text = response.text().await.unwrap_or_else(|e| e.to_string());
            Err(Error::Database(text))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_util;

    // This test is intentionally skipped for now, as we don't have ClickHouse set up in our CI
    // infrastructure yet. Uncomment the attribute line to enable it.
    //
    // #[tokio::test]
    async fn test_build_client() {
        Client::new("[::1]:8123".parse().unwrap()).await.unwrap();
    }

    // This test is intentionally skipped for now, as we don't have ClickHouse set up in our CI
    // infrastructure yet. Uncomment the attribute line to enable it.
    //
    // #[tokio::test]
    async fn test_client_insert() {
        let client = Client::new("[::1]:8123".parse().unwrap()).await.unwrap();
        client.wipe_db().await.unwrap();
        client.init_db().await.unwrap();
        let samples = {
            let mut s = Vec::with_capacity(8);
            for _ in 0..s.capacity() {
                s.push(test_util::make_hist_sample())
            }
            s
        };
        client.insert_samples(&samples).await.unwrap();
        client.wipe_db().await.unwrap();
    }
}
