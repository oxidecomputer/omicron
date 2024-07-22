// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of client methods that write to the ClickHouse database.

// Copyright 2024 Oxide Computer Company

use crate::client::Client;
use crate::model;
use crate::Error;
use oximeter::Sample;
use oximeter::TimeseriesName;
use slog::debug;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug)]
pub(super) struct UnrolledSampleRows {
    /// The timeseries schema rows, keyed by timeseries name.
    pub new_schema: BTreeMap<TimeseriesName, String>,
    /// The rows to insert in all the other tables, keyed by the table name.
    pub rows: BTreeMap<String, Vec<String>>,
}

/// A trait allowing a [`Client`] to write data into the timeseries database.
///
/// The vanilla [`Client`] object allows users to query the timeseries database, returning
/// timeseries samples corresponding to various filtering criteria. This trait segregates the
/// methods required for _writing_ new data into the database, and is intended only for use by the
/// `oximeter-collector` crate.
#[async_trait::async_trait]
pub trait DbWrite {
    /// Insert the given samples into the database.
    async fn insert_samples(&self, samples: &[Sample]) -> Result<(), Error>;

    /// Initialize the replicated telemetry database, creating tables as needed.
    async fn init_replicated_db(&self) -> Result<(), Error>;

    /// Initialize a single node telemetry database, creating tables as needed.
    async fn init_single_node_db(&self) -> Result<(), Error>;

    /// Wipe the ClickHouse database entirely from a single node set up.
    async fn wipe_single_node_db(&self) -> Result<(), Error>;

    /// Wipe the ClickHouse database entirely from a replicated set up.
    async fn wipe_replicated_db(&self) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl DbWrite for Client {
    /// Insert the given samples into the database.
    async fn insert_samples(&self, samples: &[Sample]) -> Result<(), Error> {
        debug!(self.log, "unrolling {} total samples", samples.len());
        let UnrolledSampleRows { new_schema, rows } =
            self.unroll_samples(samples).await;
        self.save_new_schema_or_remove(new_schema).await?;
        self.insert_unrolled_samples(rows).await
    }

    /// Initialize the replicated telemetry database, creating tables as needed.
    async fn init_replicated_db(&self) -> Result<(), Error> {
        debug!(self.log, "initializing ClickHouse database");
        self.run_many_sql_statements(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schema/replicated/db-init-test.sql"
        )))
        .await
    }

    /// Wipe the ClickHouse database entirely from a replicated set up.
    async fn wipe_replicated_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        self.run_many_sql_statements(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schema/replicated/db-wipe.sql"
        )))
        .await
    }

    /// Initialize a single node telemetry database, creating tables as needed.
    async fn init_single_node_db(&self) -> Result<(), Error> {
        debug!(self.log, "initializing ClickHouse database");
        self.run_many_sql_statements(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schema/single-node/db-init.sql"
        )))
        .await
    }

    /// Wipe the ClickHouse database entirely from a single node set up.
    async fn wipe_single_node_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        self.run_many_sql_statements(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schema/single-node/db-wipe.sql"
        )))
        .await
    }
}

impl Client {
    // Unroll each sample into its consituent rows, after verifying the schema.
    //
    // Note that this also inserts the schema into the internal cache, if it
    // does not already exist there.
    pub(super) async fn unroll_samples(
        &self,
        samples: &[Sample],
    ) -> UnrolledSampleRows {
        let mut seen_timeseries = BTreeSet::new();
        let mut rows = BTreeMap::new();
        let mut new_schema = BTreeMap::new();

        for sample in samples.iter() {
            match self.verify_or_cache_sample_schema(sample).await {
                Err(_) => {
                    // Skip the sample, but otherwise do nothing. The error is logged in the above
                    // call.
                    continue;
                }
                Ok(None) => {}
                Ok(Some((name, schema))) => {
                    debug!(
                        self.log,
                        "new timeseries schema";
                        "timeseries_name" => %name,
                        "schema" => %schema
                    );
                    new_schema.insert(name, schema);
                }
            }

            // Key on both the timeseries name and key, as timeseries may actually share keys.
            let key = (
                sample.timeseries_name.as_str(),
                crate::timeseries_key(sample),
            );
            if !seen_timeseries.contains(&key) {
                for (table_name, table_rows) in model::unroll_field_rows(sample)
                {
                    rows.entry(table_name)
                        .or_insert_with(Vec::new)
                        .extend(table_rows);
                }
            }

            let (table_name, measurement_row) =
                model::unroll_measurement_row(sample);

            rows.entry(table_name)
                .or_insert_with(Vec::new)
                .push(measurement_row);

            seen_timeseries.insert(key);
        }

        UnrolledSampleRows { new_schema, rows }
    }

    // Insert unrolled sample rows into the corresponding tables.
    async fn insert_unrolled_samples(
        &self,
        rows: BTreeMap<String, Vec<String>>,
    ) -> Result<(), Error> {
        for (table_name, rows) in rows {
            let body = format!(
                "INSERT INTO {table_name} FORMAT JSONEachRow\n{row_data}\n",
                table_name = table_name,
                row_data = rows.join("\n")
            );
            // TODO-robustness We've verified the schema, so this is likely a transient failure.
            // But we may want to check the actual error condition, and, if possible, continue
            // inserting any remaining data.
            self.execute(body).await?;
            debug!(
                self.log,
                "inserted rows into table";
                "n_rows" => rows.len(),
                "table_name" => table_name,
            );
        }

        // TODO-correctness We'd like to return all errors to clients here, and there may be as
        // many as one per sample. It's not clear how to structure this in a way that's useful.
        Ok(())
    }

    // Save new schema to the database, or remove them from the cache on
    // failure.
    //
    // This attempts to insert the provided schema into the timeseries schema
    // table. If that fails, those schema are _also_ removed from the internal
    // cache.
    //
    // TODO-robustness There's still a race possible here. If two distinct clients receive new
    // but conflicting schema, they will both try to insert those at some point into the schema
    // tables. It's not clear how to handle this, since ClickHouse provides no transactions.
    // This is unlikely to happen at this point, because the design is such that there will be
    // a single `oximeter` instance, which has one client object, connected to a single
    // ClickHouse server. But once we start replicating data, the window within which the race
    // can occur is much larger, since it includes the time it takes ClickHouse to replicate
    // data between nodes.
    //
    // NOTE: This is an issue even in the case where the schema don't conflict. Two clients may
    // receive a sample with a new schema, and both would then try to insert that schema.
    pub(super) async fn save_new_schema_or_remove(
        &self,
        new_schema: BTreeMap<TimeseriesName, String>,
    ) -> Result<(), Error> {
        if !new_schema.is_empty() {
            debug!(
                self.log,
                "inserting {} new timeseries schema",
                new_schema.len()
            );
            const APPROX_ROW_SIZE: usize = 64;
            let mut body = String::with_capacity(
                APPROX_ROW_SIZE + APPROX_ROW_SIZE * new_schema.len(),
            );
            body.push_str("INSERT INTO ");
            body.push_str(crate::DATABASE_NAME);
            body.push_str(".timeseries_schema FORMAT JSONEachRow\n");
            for row_data in new_schema.values() {
                body.push_str(row_data);
                body.push('\n');
            }

            // Try to insert the schema.
            //
            // If this fails, be sure to remove the schema we've added from the
            // internal cache. Since we check the internal cache first for
            // schema, if we fail here but _don't_ remove the schema, we'll
            // never end up inserting the schema, but we will insert samples.
            if let Err(e) = self.execute(body).await {
                debug!(
                    self.log,
                    "failed to insert new schema, removing from cache";
                    "error" => ?e,
                );
                let mut schema = self.schema.lock().await;
                for name in new_schema.keys() {
                    schema
                        .remove(name)
                        .expect("New schema should have been cached");
                }
                return Err(e);
            }
        }
        Ok(())
    }

    // Run one or more SQL statements.
    //
    // This is intended to be used for the methods which run SQL from one of the
    // SQL files in the crate, e.g., the DB initialization or update files.
    async fn run_many_sql_statements(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<(), Error> {
        for stmt in sql.as_ref().split(';').filter(|s| !s.trim().is_empty()) {
            self.execute(stmt).await?;
        }
        Ok(())
    }
}
