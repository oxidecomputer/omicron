// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of client methods that write to the ClickHouse database.

// Copyright 2026 Oxide Computer Company

use crate::Error;
use crate::client::Client;
use crate::model;
use crate::model::to_block::ToBlock as _;
use crate::native::block::Block;
use camino::Utf8PathBuf;
use oximeter::Sample;
use oximeter::TimeseriesName;
use oximeter::TimeseriesSchema;
use slog::debug;
use slog::error;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::btree_map::Entry;

use super::Handle;

#[derive(Debug)]
pub(super) struct UnrolledSampleRows {
    /// The timeseries schema rows.
    pub new_schema: Vec<TimeseriesSchema>,
    /// The blocks to insert in all the other tables, keyed by the table name.
    pub blocks: BTreeMap<String, Block>,
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
        let mut handle = self.claim_connection().await?;
        debug!(self.log, "unrolling {} total samples", samples.len());
        let UnrolledSampleRows { new_schema, blocks } =
            self.unroll_samples(&mut handle, samples).await?;
        // TODO-correctness: Inserting schema here only works with exactly one
        // oximeter client. If there are conflicting schema on different
        // clients, we'll insert both. This should probably be resolved by
        // fixing https://github.com/oxidecomputer/omicron/issues/1294 and
        // related issues, storing the schema in CRDB and doing checked,
        // validated schema updates.
        self.insert_schema(&mut handle, new_schema).await?;
        self.insert_unrolled_samples(&mut handle, blocks).await
    }

    /// Initialize the replicated telemetry database, creating tables as needed.
    ///
    /// We run both db-init files since we want all tables in production.
    /// These files are intentionally disjoint so that we don't have to
    /// duplicate any setup.
    async fn init_replicated_db(&self) -> Result<(), Error> {
        let mut handle = self.claim_connection().await?;
        debug!(self.log, "initializing ClickHouse database");
        self.run_many_sql_statements(
            &mut handle,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/replicated/db-init-1.sql"
            )),
        )
        .await?;
        self.run_many_sql_statements(
            &mut handle,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/replicated/db-init-2.sql"
            )),
        )
        .await
    }

    /// Wipe the ClickHouse database entirely from a replicated set up.
    async fn wipe_replicated_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        self.run_many_sql_statements(
            &mut self.claim_connection().await?,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/replicated/db-wipe.sql"
            )),
        )
        .await
    }

    /// Initialize a single node telemetry database, creating tables as needed.
    async fn init_single_node_db(&self) -> Result<(), Error> {
        debug!(self.log, "initializing ClickHouse database");
        self.run_many_sql_statements(
            &mut self.claim_connection().await?,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/single-node/db-init.sql"
            )),
        )
        .await
    }

    /// Wipe the ClickHouse database entirely from a single node set up.
    async fn wipe_single_node_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        self.run_many_sql_statements(
            &mut self.claim_connection().await?,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/single-node/db-wipe.sql"
            )),
        )
        .await
    }
}

/// Allow initializing a minimal subset of db tables for replicated cluster
/// testing
#[async_trait::async_trait]
pub trait TestDbWrite {
    /// Initialize the replicated telemetry database, creating a subset of tables as described
    /// in `db-init-1.sql`.
    async fn init_test_minimal_replicated_db(&self) -> Result<(), Error>;

    /// Initialize the replicated telemetry database with the given file id.
    async fn init_replicated_db_from_file(
        &self,
        id: usize,
    ) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl TestDbWrite for Client {
    /// Initialize the replicated telemetry database, creating tables as needed.
    /// We run only the first db-init file, since it contains a minimum number
    /// of tables required for replication/cluster tests.
    async fn init_test_minimal_replicated_db(&self) -> Result<(), Error> {
        debug!(self.log, "initializing ClickHouse database");
        self.run_many_sql_statements(
            &mut self.claim_connection().await?,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/schema/replicated/db-init-1.sql"
            )),
        )
        .await
    }

    async fn init_replicated_db_from_file(
        &self,
        id: usize,
    ) -> Result<(), Error> {
        let file = format!("db-init-{id}.sql");
        debug!(self.log, "initializing ClickHouse database via {file}");
        let path: Utf8PathBuf =
            [env!("CARGO_MANIFEST_DIR"), "schema/replicated/", &file]
                .into_iter()
                .collect();
        let sql = tokio::fs::read_to_string(&path).await.map_err(|err| {
            Error::ReadSqlFile {
                context: format!(
                    "Reading SQL file '{}' for test initialization",
                    path,
                ),
                err,
            }
        })?;
        self.run_many_sql_statements(&mut self.claim_connection().await?, sql)
            .await
    }
}

impl Client {
    // Unroll each sample into its constituent rows, after verifying the schema
    // against the database.
    //
    // For each unique timeseries name in the batch, the database is queried to
    // fetch the existing schema. Samples whose derived schema conflicts with
    // the database are skipped with a logged error. Schemas not yet present in
    // the database are returned in `new_schema` for the caller to insert.
    pub(super) async fn unroll_samples(
        &self,
        handle: &mut Handle,
        samples: &[Sample],
    ) -> Result<UnrolledSampleRows, Error> {
        // First pass: collect one derived schema per unique timeseries name,
        // constructing the schema only on the first occurrence of each name.
        let mut derived: BTreeMap<TimeseriesName, TimeseriesSchema> =
            BTreeMap::new();
        for sample in samples.iter() {
            let name = sample
                .timeseries_name
                .parse::<TimeseriesName>()
                .expect("sample timeseries name must be valid");
            derived
                .entry(name)
                .or_insert_with(|| TimeseriesSchema::from(sample));
        }

        // Fetch whatever the database currently has for those names.
        let db_schemas =
            self.fetch_schema_from_db(handle, derived.keys()).await?;

        // Classify each derived schema by consuming the map. Schemas already
        // present in the database that match are dropped silently. Mismatched
        // schemas are logged and their names added to `skip`. New schemas are
        // moved into `new_schema` for insertion.
        let mut skip = HashSet::new();
        let mut new_schema = Vec::new();
        for (name, derived_schema) in derived {
            match db_schemas.get(&name) {
                Some(existing) if existing == &derived_schema => {}
                Some(existing) => {
                    error!(
                        self.log,
                        "timeseries schema mismatch, samples will be skipped";
                        "timeseries_name" => %name,
                        "expected" => ?existing,
                        "actual" => ?derived_schema,
                    );
                    skip.insert(String::from(name));
                }
                None => {
                    debug!(
                        self.log,
                        "new timeseries schema";
                        "timeseries_name" => %name,
                        "schema" => ?derived_schema,
                    );
                    new_schema.push(derived_schema);
                }
            }
        }

        // Second pass: build data blocks for all non-skipped samples.
        let mut seen_timeseries = HashSet::new();
        let mut table_blocks = BTreeMap::new();
        for sample in samples.iter() {
            if skip.contains(sample.timeseries_name.as_str()) {
                continue;
            }

            // Key on both the timeseries name and key, as timeseries may
            // actually share keys.
            let key = (
                sample.timeseries_name.as_str(),
                crate::timeseries_key(sample),
            );
            if !seen_timeseries.contains(&key) {
                for (table_name, block) in
                    model::fields::extract_fields_as_block(sample)
                {
                    match table_blocks.entry(table_name) {
                        Entry::Vacant(entry) => {
                            entry.insert(block);
                        }
                        Entry::Occupied(mut entry) => entry
                            .get_mut()
                            .concat(block)
                            .expect("All blocks for a table must match"),
                    }
                }
            }

            let (table_name, measurement_block) =
                model::measurements::extract_measurement_as_block(sample);
            match table_blocks.entry(table_name) {
                Entry::Vacant(entry) => {
                    entry.insert(measurement_block);
                }
                Entry::Occupied(mut entry) => entry
                    .get_mut()
                    .concat(measurement_block)
                    .expect("All blocks for a table must match"),
            }

            seen_timeseries.insert(key);
        }

        Ok(UnrolledSampleRows { new_schema, blocks: table_blocks })
    }

    // Insert unrolled sample rows into the corresponding tables.
    async fn insert_unrolled_samples(
        &self,
        handle: &mut Handle,
        blocks: BTreeMap<String, Block>,
    ) -> Result<(), Error> {
        for (table_name, block) in blocks {
            let n_rows = block.n_rows();
            let query = format!(
                "INSERT INTO {db_name}.{table_name} FORMAT Native",
                db_name = crate::DATABASE_NAME,
                table_name = table_name,
            );
            // TODO-robustness We've verified the schema, so this is likely a transient failure.
            // But we may want to check the actual error condition, and, if possible, continue
            // inserting any remaining data.
            self.insert_native(handle, &query, block).await?;
            debug!(
                self.log,
                "inserted rows into table";
                "n_rows" => n_rows,
                "table_name" => &table_name,
            );
        }

        // TODO-correctness We'd like to return all errors to clients here, and there may be as
        // many as one per sample. It's not clear how to structure this in a way that's useful.
        Ok(())
    }

    // Insert new timeseries schema into the database.
    pub(super) async fn insert_schema(
        &self,
        handle: &mut Handle,
        new_schema: Vec<TimeseriesSchema>,
    ) -> Result<(), Error> {
        if new_schema.is_empty() {
            return Ok(());
        }
        debug!(
            self.log,
            "inserting {} new timeseries schema",
            new_schema.len()
        );
        let body = const_format::formatcp!(
            "INSERT INTO {}.timeseries_schema FORMAT Native",
            crate::DATABASE_NAME
        );
        let block = TimeseriesSchema::to_block(&new_schema)?;
        self.insert_native(handle, &body, block).await.map(|_| ())
    }

    // Run one or more SQL statements.
    //
    // This is intended to be used for the methods which run SQL from one of the
    // SQL files in the crate, e.g., the DB initialization or update files.
    //
    // NOTE: This uses the native TCP connection interface to run its
    // statements.
    async fn run_many_sql_statements(
        &self,
        handle: &mut Handle,
        sql: impl AsRef<str>,
    ) -> Result<(), Error> {
        for stmt in sql.as_ref().split(';').filter(|s| !s.trim().is_empty()) {
            self.execute_native(handle, stmt).await?;
        }
        Ok(())
    }
}
