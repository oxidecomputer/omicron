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

pub use crate::sql::RestrictedQuery;
use crate::Error;
use crate::{
    client::Client,
    sql::{QueryResult, Table},
};
use anyhow::Context as _;
use slog::debug;

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
        let original_query = query.as_ref().trim_end_matches(';');
        let rewritten = self.transform_query(original_query).await?;
        debug!(
            self.log,
            "rewrote restricted query";
            "original_sql" => &original_query,
            "rewritten_sql" => &rewritten,
        );
        let result = self.execute_with_block(&rewritten).await?;
        let summary = result.query_summary();
        let block = result.data.as_ref().context("expected a data block")?;
        let mut table = Table {
            column_names: block.columns.keys().cloned().collect(),
            rows: vec![],
        };
        for row in block.json_rows().into_iter() {
            table.rows.push(row.into_iter().map(|(_k, v)| v).collect());
        }
        Ok(QueryResult {
            original_query: original_query.to_string(),
            rewritten_query: rewritten,
            summary,
            table,
        })
    }
}
