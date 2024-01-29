// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client methods for running SQL queries againts timeseries themselves.
//!
//! This implements a prototype system for creating "virtual tables" from each
//! timeseries, letting us run SQL queries directly against them. These tables
//! are constructed via huge joins, which effectively reconstruct the entire
//! history of samples as received from the producers. Each row is the original
//! sample. This denormalization comes at a big cost, both in cycles and memory
//! usage, since we need to build the entire join in ClickHouse and send it all
//! to the client for deserialization.
//!
//! Thus this prototype is very useful for development, running analyses on
//! small datasets. It's less helpful on real deployments, where the size of
//! data makes this approach prohibitive.

// Copyright 2024 Oxide Computer Company

use super::query_summary::QuerySummary;
use crate::client::Client;
pub use crate::sql::RestrictedQuery;
use crate::Error;
pub use indexmap::IndexMap;
use slog::debug;
pub use std::time::Instant;

/// A tabular result from a SQL query against a timeseries.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Table {
    /// The name of each column in the result set.
    pub column_names: Vec<String>,
    /// The rows of the result set, one per column.
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// The full result of running a SQL query against a timeseries.
#[derive(Clone, Debug)]
pub struct QueryResult {
    /// The query as written by the client.
    pub original_query: String,
    /// The rewritten query, run against the JOINed representation of the
    /// timeseries.
    ///
    /// This is the query that is actually run in the database itself.
    pub rewritten_query: String,
    /// Summary of the resource usage of the query.
    pub summary: QuerySummary,
    /// The result of the query, with column names and rows.
    pub table: Table,
}

impl Client {
    /// Transform a SQL query against a timeseries, but do not execute it.
    pub async fn transform_query(
        &self,
        query: impl AsRef<str>,
    ) -> Result<String, Error> {
        let restricted = RestrictedQuery::new(query.as_ref())?;
        restricted.to_oximeter_sql(&*self.schema.lock().await)
    }

    /// Run a SQL query against a timeseries.
    pub async fn query(
        &self,
        query: impl AsRef<str>,
    ) -> Result<QueryResult, Error> {
        use crate::client::handle_db_response;

        let original_query = query.as_ref().trim_end_matches(';');
        let ox_sql = self.transform_query(original_query).await?;
        let rewritten = format!("{ox_sql} FORMAT JSONEachRow");
        debug!(
            self.log,
            "rewrote restricted query";
            "original_sql" => &original_query,
            "rewritten_sql" => &rewritten,
        );
        let request = self
            .client
            .post(&self.url)
            .query(&[
                ("output_format_json_quote_64bit_integers", "0"),
                ("database", crate::DATABASE_NAME),
            ])
            .body(rewritten.clone());
        let query_start = Instant::now();
        let response = handle_db_response(
            request
                .send()
                .await
                .map_err(|err| Error::DatabaseUnavailable(err.to_string()))?,
        )
        .await?;
        let summary = QuerySummary::from_headers(
            query_start.elapsed(),
            response.headers(),
        )?;
        let text = response.text().await.unwrap();
        let mut table = Table::default();
        for line in text.lines() {
            let row =
                serde_json::from_str::<IndexMap<String, serde_json::Value>>(
                    line.trim(),
                )
                .unwrap();
            if table.column_names.is_empty() {
                table.column_names.extend(row.keys().cloned())
            } else {
                assert!(table
                    .column_names
                    .iter()
                    .zip(row.keys())
                    .all(|(k1, k2)| k1 == k2));
            }
            table.rows.push(row.into_values().collect());
        }
        Ok(QueryResult {
            original_query: original_query.to_string(),
            rewritten_query: rewritten,
            summary,
            table,
        })
    }
}
