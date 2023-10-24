// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rust client to ClickHouse database

// Copyright 2023 Oxide Computer Company

use crate::model;
use crate::query;
use crate::Error;
use crate::Metric;
use crate::Target;
use crate::Timeseries;
use crate::TimeseriesKey;
use crate::TimeseriesName;
use crate::TimeseriesPageSelector;
use crate::TimeseriesScanParams;
use crate::TimeseriesSchema;
use async_trait::async_trait;
use dropshot::EmptyScanParams;
use dropshot::PaginationOrder;
use dropshot::ResultsPage;
use dropshot::WhichPage;
use oximeter::types::Sample;
use slog::debug;
use slog::error;
use slog::info;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use tokio::sync::Mutex;
use uuid::Uuid;

#[usdt::provider(provider = "clickhouse__client")]
mod probes {
    fn query__start(_: &usdt::UniqueId, sql: &str) {}
    fn query__done(_: &usdt::UniqueId) {}
}

/// A `Client` to the ClickHouse metrics database.
#[derive(Debug)]
pub struct Client {
    _id: Uuid,
    log: Logger,
    url: String,
    client: reqwest::Client,
    schema: Mutex<BTreeMap<TimeseriesName, TimeseriesSchema>>,
}

impl Client {
    /// Construct a new ClickHouse client of the database at `address`.
    pub fn new(address: SocketAddr, log: &Logger) -> Self {
        let id = Uuid::new_v4();
        let log = log.new(slog::o!(
            "component" => "clickhouse-client",
            "id" => id.to_string(),
        ));
        let client = reqwest::Client::new();
        let url = format!("http://{}", address);
        let schema = Mutex::new(BTreeMap::new());
        Self { _id: id, log, url, client, schema }
    }

    /// Ping the ClickHouse server to verify connectivitiy.
    pub async fn ping(&self) -> Result<(), Error> {
        handle_db_response(
            self.client
                .get(format!("{}/ping", self.url))
                .send()
                .await
                .map_err(|err| Error::DatabaseUnavailable(err.to_string()))?,
        )
        .await?;
        debug!(self.log, "successful ping of ClickHouse server");
        Ok(())
    }

    /// Select timeseries from criteria on the fields and start/end timestamps.
    pub async fn select_timeseries_with(
        &self,
        timeseries_name: &str,
        criteria: &[&str],
        start_time: Option<query::Timestamp>,
        end_time: Option<query::Timestamp>,
        limit: Option<NonZeroU32>,
        order: Option<PaginationOrder>,
    ) -> Result<Vec<Timeseries>, Error> {
        // Querying uses up to three queries to the database:
        //  1. Retrieve the schema
        //  2. Retrieve the keys and field names/values for matching timeseries
        //  3. Retrieve the actual timeseries measurements.
        //
        //  This seems roundabout, but it's all around better. It normalizes the database more than
        //  previously, as we don't store the stringified field values inline with the
        //  measurements. The queries are easier to write and understand. By removing the field
        //  values from the measurement rows, we avoid transferring the data from those columns
        //  to/from the database, as well as the cost of parsing them for each measurement, only to
        //  promptly throw away almost all of them (except for the first).
        let timeseries_name = TimeseriesName::try_from(timeseries_name)?;
        let schema =
            self.schema_for_timeseries(&timeseries_name).await?.ok_or_else(
                || Error::TimeseriesNotFound(format!("{timeseries_name}")),
            )?;
        let query_builder = query::SelectQueryBuilder::new(&schema)
            .start_time(start_time)
            .end_time(end_time);

        let mut query_builder = if let Some(limit) = limit {
            query_builder.limit(limit)
        } else {
            query_builder
        };

        if let Some(order) = order {
            query_builder = query_builder.order(order);
        }

        for criterion in criteria.iter() {
            query_builder = query_builder.filter_raw(criterion)?;
        }

        let query = query_builder.build();
        let info = match query.field_query() {
            Some(field_query) => {
                self.select_matching_timeseries_info(&field_query, &schema)
                    .await?
            }
            None => BTreeMap::new(),
        };

        if info.is_empty() {
            // allow queries that resolve to zero timeseries even with limit
            // specified because the results are not misleading
            Ok(vec![])
        } else if limit.is_some() && info.len() != 1 {
            // Error if a limit is specified with a query that resolves to
            // multiple timeseries. Because query results are ordered in part by
            // timeseries key, results with a limit will select measurements in
            // a way that is arbitrary with respect to the query.
            Err(Error::InvalidLimitQuery)
        } else {
            self.select_timeseries_with_keys(&query, &info, &schema).await
        }
    }

    pub async fn list_timeseries(
        &self,
        page: &WhichPage<TimeseriesScanParams, TimeseriesPageSelector>,
        limit: NonZeroU32,
    ) -> Result<ResultsPage<Timeseries>, Error> {
        let (params, offset) = match page {
            WhichPage::First(ref params) => (params, 0),
            WhichPage::Next(ref sel) => (&sel.params, sel.offset.get()),
        };
        let schema = self
            .schema_for_timeseries(&params.timeseries_name)
            .await?
            .ok_or_else(|| {
                Error::TimeseriesNotFound(format!("{}", params.timeseries_name))
            })?;
        // TODO: Handle inclusive/exclusive timestamps in general.
        //
        // These come from a query parameter, so it's not obvious what format they should have.
        let mut query_builder = query::SelectQueryBuilder::new(&schema)
            .start_time(
                params.start_time.map(|t| query::Timestamp::Inclusive(t)),
            )
            .end_time(params.end_time.map(|t| query::Timestamp::Exclusive(t)))
            .limit(limit)
            .offset(offset);
        for criterion in params.criteria.iter() {
            query_builder = query_builder.filter_str(criterion)?;
        }

        let query = query_builder.build();
        let info = match query.field_query() {
            Some(field_query) => {
                self.select_matching_timeseries_info(&field_query, &schema)
                    .await?
            }
            None => BTreeMap::new(),
        };
        let results = if info.is_empty() {
            vec![]
        } else {
            self.select_timeseries_with_keys(&query, &info, &schema).await?
        };
        Ok(ResultsPage::new(results, &params, |_, _| {
            NonZeroU32::try_from(limit.get() + offset).unwrap()
        })
        .unwrap())
    }

    /// Return the schema for a timeseries by name.
    ///
    /// Note
    /// ----
    /// This method may translate into a call to the database, if the requested metric cannot be
    /// found in an internal cache.
    pub async fn schema_for_timeseries(
        &self,
        name: &TimeseriesName,
    ) -> Result<Option<TimeseriesSchema>, Error> {
        let mut schema = self.schema.lock().await;
        if let Some(s) = schema.get(name) {
            return Ok(Some(s.clone()));
        }
        self.get_schema_locked(&mut schema).await?;
        Ok(schema.get(name).map(Clone::clone))
    }

    /// List timeseries schema, paginated.
    pub async fn timeseries_schema_list(
        &self,
        page: &WhichPage<EmptyScanParams, TimeseriesName>,
        limit: NonZeroU32,
    ) -> Result<ResultsPage<TimeseriesSchema>, Error> {
        let sql = match page {
            WhichPage::First(_) => {
                format!(
                    concat!(
                        "SELECT * ",
                        "FROM {}.timeseries_schema ",
                        "LIMIT {} ",
                        "FORMAT JSONEachRow;",
                    ),
                    crate::DATABASE_NAME,
                    limit.get(),
                )
            }
            WhichPage::Next(ref last_timeseries) => {
                format!(
                    concat!(
                        "SELECT * FROM {}.timeseries_schema ",
                        "WHERE timeseries_name > '{}' ",
                        "LIMIT {} ",
                        "FORMAT JSONEachRow;",
                    ),
                    crate::DATABASE_NAME,
                    last_timeseries,
                    limit.get(),
                )
            }
        };
        let body = self.execute_with_body(sql).await?;
        let schema = body
            .lines()
            .map(|line| {
                TimeseriesSchema::from(
                    serde_json::from_str::<model::DbTimeseriesSchema>(line)
                        .expect(
                        "Failed to deserialize TimeseriesSchema from database",
                    ),
                )
            })
            .collect::<Vec<_>>();
        ResultsPage::new(schema, &dropshot::EmptyScanParams {}, |schema, _| {
            schema.timeseries_name.clone()
        })
        .map_err(|e| Error::Database(e.to_string()))
    }

    /// Validates that the schema used by the DB matches the version used by
    /// the executable using it.
    ///
    /// This function will wipe metrics data if the version stored within
    /// the DB is less than the schema version of Oximeter.
    /// If the version in the DB is newer than what is known to Oximeter, an
    /// error is returned.
    ///
    /// NOTE: This function is not safe for concurrent usage!
    pub async fn initialize_db_with_version(
        &self,
        replicated: bool,
        expected_version: u64,
    ) -> Result<(), Error> {
        info!(self.log, "reading db version");

        // Read the version from the DB
        let version = self.read_latest_version().await?;
        info!(self.log, "read oximeter database version"; "version" => version);

        // Decide how to conform the on-disk version with this version of
        // Oximeter.
        if version < expected_version {
            info!(self.log, "wiping and re-initializing oximeter schema");
            // If the on-storage version is less than the constant embedded into
            // this binary, the DB is out-of-date. Drop it, and re-populate it
            // later.
            if !replicated {
                self.wipe_single_node_db().await?;
                self.init_single_node_db().await?;
            } else {
                self.wipe_replicated_db().await?;
                self.init_replicated_db().await?;
            }
        } else if version > expected_version {
            // If the on-storage version is greater than the constant embedded
            // into this binary, we may have downgraded.
            return Err(Error::Database(
                format!(
                    "Expected version {expected_version}, saw {version}. Downgrading is not supported.",
                )
            ));
        } else {
            // If the version matches, we don't need to update the DB
            return Ok(());
        }

        info!(self.log, "inserting current version"; "version" => expected_version);
        self.insert_version(expected_version).await?;
        Ok(())
    }

    async fn read_latest_version(&self) -> Result<u64, Error> {
        let sql = format!(
            "SELECT MAX(value) FROM {db_name}.version;",
            db_name = crate::DATABASE_NAME,
        );

        let version = match self.execute_with_body(sql).await {
            Ok(body) if body.is_empty() => {
                warn!(
                    self.log,
                    "no version in database (treated as 'version 0')"
                );
                0
            }
            Ok(body) => body.trim().parse::<u64>().map_err(|err| {
                Error::Database(format!("Cannot read version: {err}"))
            })?,
            Err(Error::Database(err))
                // Case 1: The database has not been created.
                if err.contains("Database oximeter doesn't exist") ||
                // Case 2: The database has been created, but it's old (exists
                // prior to the version table).
                    err.contains("Table oximeter.version doesn't exist") =>
            {
                warn!(self.log, "oximeter database does not exist, or is out-of-date");
                0
            }
            Err(err) => {
                warn!(self.log, "failed to read version"; "error" => err.to_string());
                return Err(err);
            }
        };
        Ok(version)
    }

    async fn insert_version(&self, version: u64) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {db_name}.version (*) VALUES ({version}, now());",
            db_name = crate::DATABASE_NAME,
        );
        self.execute_with_body(sql).await?;
        Ok(())
    }

    /// Verifies if instance is part of oximeter_cluster
    pub async fn is_oximeter_cluster(&self) -> Result<bool, Error> {
        let sql = String::from("SHOW CLUSTERS FORMAT JSONEachRow;");
        let res = self.execute_with_body(sql).await?;
        Ok(res.contains("oximeter_cluster"))
    }

    // Verifies that the schema for a sample matches the schema in the database.
    //
    // If the schema exists in the database, and the sample matches that schema, `None` is
    // returned. If the schema does not match, an Err is returned (the caller skips the sample in
    // this case). If the schema does not _exist_ in the database, Some(schema) is returned, so
    // that the caller can insert it into the database at the appropriate time.
    async fn verify_sample_schema(
        &self,
        sample: &Sample,
    ) -> Result<Option<String>, Error> {
        let sample_schema = model::schema_for(sample);
        let name = sample_schema.timeseries_name.clone();
        let mut schema = self.schema.lock().await;

        // We need to possibly check that this schema is in the local cache, or
        // in the database, all while we hold the lock to ensure there's no
        // concurrent additions. This containment check is needed so that we
        // check both the local cache and the database, to avoid adding a schema
        // a second time.
        if !schema.contains_key(&name) {
            self.get_schema_locked(&mut schema).await?;
        }
        match schema.entry(name) {
            Entry::Occupied(entry) => {
                let existing_schema = entry.get();
                if existing_schema == &sample_schema {
                    Ok(None)
                } else {
                    error!(
                        self.log,
                        "timeseries schema mismatch, sample will be skipped";
                        "expected" => ?existing_schema,
                        "actual" => ?sample_schema,
                        "sample" => ?sample,
                    );
                    Err(Error::SchemaMismatch {
                        expected: existing_schema.clone(),
                        actual: sample_schema,
                    })
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(sample_schema.clone());
                Ok(Some(
                    serde_json::to_string(&model::DbTimeseriesSchema::from(
                        sample_schema,
                    ))
                    .expect("Failed to convert schema to DB model"),
                ))
            }
        }
    }

    // Select the timeseries, including keys and field values, that match the given field-selection
    // query.
    async fn select_matching_timeseries_info(
        &self,
        field_query: &str,
        schema: &TimeseriesSchema,
    ) -> Result<BTreeMap<TimeseriesKey, (Target, Metric)>, Error> {
        let body = self.execute_with_body(field_query).await?;
        let mut results = BTreeMap::new();
        for line in body.lines() {
            let row: model::FieldSelectRow = serde_json::from_str(line)
                .expect("Unable to deserialize an expected row");
            let (id, target, metric) =
                model::parse_field_select_row(&row, schema);
            results.insert(id, (target, metric));
        }
        Ok(results)
    }

    // Given information returned from `select_matching_timeseries_info`, select the actual
    // measurements from timeseries with those keys.
    async fn select_timeseries_with_keys(
        &self,
        query: &query::SelectQuery,
        info: &BTreeMap<TimeseriesKey, (Target, Metric)>,
        schema: &TimeseriesSchema,
    ) -> Result<Vec<Timeseries>, Error> {
        let mut timeseries_by_key = BTreeMap::new();
        let keys = info.keys().copied().collect::<Vec<_>>();
        let measurement_query = query.measurement_query(&keys);
        for line in self.execute_with_body(&measurement_query).await?.lines() {
            let (key, measurement) =
                model::parse_measurement_from_row(line, schema.datum_type);
            let timeseries = timeseries_by_key.entry(key).or_insert_with(
                || {
                    let (target, metric) = info
                        .get(&key)
                        .expect("Timeseries key in measurement query but not field query")
                        .clone();
                    Timeseries {
                        timeseries_name: schema.timeseries_name.to_string(),
                        target,
                        metric,
                        measurements: Vec::new(),
                    }
                }
            );
            timeseries.measurements.push(measurement);
        }
        Ok(timeseries_by_key.into_values().collect())
    }

    // Initialize ClickHouse with the database and metric table schema.
    // Execute a generic SQL statement.
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute<S>(&self, sql: S) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        self.execute_with_body(sql).await?;
        Ok(())
    }

    // Execute a generic SQL statement, awaiting the response as text
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute_with_body<S>(&self, sql: S) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        let sql = sql.as_ref().to_string();
        trace!(self.log, "executing SQL query: {}", sql);
        let id = usdt::UniqueId::new();
        probes::query__start!(|| (&id, &sql));
        let response = handle_db_response(
            self.client
                .post(&self.url)
                // See regression test `test_unquoted_64bit_integers` for details.
                .query(&[("output_format_json_quote_64bit_integers", "0")])
                .body(sql)
                .send()
                .await
                .map_err(|err| Error::DatabaseUnavailable(err.to_string()))?,
        )
        .await?
        .text()
        .await
        .map_err(|err| Error::Database(err.to_string()));
        probes::query__done!(|| (&id));
        response
    }

    // Get timeseries schema from the database.
    //
    // Can only be called after acquiring the lock around `self.schema`.
    async fn get_schema_locked(
        &self,
        schema: &mut BTreeMap<TimeseriesName, TimeseriesSchema>,
    ) -> Result<(), Error> {
        debug!(self.log, "retrieving timeseries schema from database");
        let sql = {
            if schema.is_empty() {
                format!(
                    "SELECT * FROM {db_name}.timeseries_schema FORMAT JSONEachRow;",
                    db_name = crate::DATABASE_NAME,
                )
            } else {
                // Only collect schema that we've not already cached.
                format!(
                    concat!(
                        "SELECT * ",
                        "FROM {db_name}.timeseries_schema ",
                        "WHERE timeseries_name NOT IN ",
                        "({current_keys}) ",
                        "FORMAT JSONEachRow;",
                    ),
                    db_name = crate::DATABASE_NAME,
                    current_keys = schema
                        .keys()
                        .map(|key| format!("'{}'", key))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        };
        let body = self.execute_with_body(sql).await?;
        if body.is_empty() {
            trace!(self.log, "no new timeseries schema in database");
        } else {
            trace!(self.log, "extracting new timeseries schema");
            let new = body.lines().map(|line| {
                let schema = TimeseriesSchema::from(
                    serde_json::from_str::<model::DbTimeseriesSchema>(line)
                        .expect(
                        "Failed to deserialize TimeseriesSchema from database",
                    ),
                );
                (schema.timeseries_name.clone(), schema)
            });
            schema.extend(new);
        }
        Ok(())
    }
}

/// A trait allowing a [`Client`] to write data into the timeseries database.
///
/// The vanilla [`Client`] object allows users to query the timeseries database, returning
/// timeseries samples corresponding to various filtering criteria. This trait segregates the
/// methods required for _writing_ new data into the database, and is intended only for use by the
/// `oximeter-collector` crate.
#[async_trait]
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

#[async_trait]
impl DbWrite for Client {
    /// Insert the given samples into the database.
    async fn insert_samples(&self, samples: &[Sample]) -> Result<(), Error> {
        debug!(self.log, "unrolling {} total samples", samples.len());
        let mut seen_timeseries = BTreeSet::new();
        let mut rows = BTreeMap::new();
        let mut new_schema = Vec::new();

        for sample in samples.iter() {
            match self.verify_sample_schema(sample).await {
                Err(_) => {
                    // Skip the sample, but otherwise do nothing. The error is logged in the above
                    // call.
                    continue;
                }
                Ok(schema) => {
                    if let Some(schema) = schema {
                        debug!(self.log, "new timeseries schema"; "schema" => ?schema);
                        new_schema.push(schema);
                    }
                }
            }

            // Key on both the timeseries name and key, as timeseries may actually share keys.
            let key = (
                sample.timeseries_name.as_str(),
                crate::timeseries_key(&sample),
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

        // Insert the new schema into the database
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
        if !new_schema.is_empty() {
            debug!(
                self.log,
                "inserting {} new timeseries schema",
                new_schema.len()
            );
            let body = format!(
                "INSERT INTO {db_name}.timeseries_schema FORMAT JSONEachRow\n{row_data}\n",
                db_name = crate::DATABASE_NAME,
                row_data = new_schema.join("\n"),
            );
            self.execute(body).await?;
        }

        // Insert the actual target/metric field rows and measurement rows.
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
                "inserted {} rows into table {}",
                rows.len(),
                table_name
            );
        }

        // TODO-correctness We'd like to return all errors to clients here, and there may be as
        // many as one per sample. It's not clear how to structure this in a way that's useful.
        Ok(())
    }

    /// Initialize the replicated telemetry database, creating tables as needed.
    async fn init_replicated_db(&self) -> Result<(), Error> {
        // The HTTP client doesn't support multiple statements per query, so we break them out here
        // manually.
        debug!(self.log, "initializing ClickHouse database");
        let sql = include_str!("./db-replicated-init.sql");
        for query in sql.split("\n--\n") {
            self.execute(query.to_string()).await?;
        }
        Ok(())
    }

    /// Initialize a single node telemetry database, creating tables as needed.
    async fn init_single_node_db(&self) -> Result<(), Error> {
        // The HTTP client doesn't support multiple statements per query, so we break them out here
        // manually.
        debug!(self.log, "initializing ClickHouse database");
        let sql = include_str!("./db-single-node-init.sql");
        for query in sql.split("\n--\n") {
            self.execute(query.to_string()).await?;
        }
        Ok(())
    }

    /// Wipe the ClickHouse database entirely from a single node set up.
    async fn wipe_single_node_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        let sql = include_str!("./db-wipe-single-node.sql").to_string();
        self.execute(sql).await
    }

    /// Wipe the ClickHouse database entirely from a replicated set up.
    async fn wipe_replicated_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        let sql = include_str!("./db-wipe-single-node.sql").to_string();
        self.execute(sql).await
    }
}

// Return Ok if the response indicates success, otherwise return either the reqwest::Error, if this
// is a client-side error, or the body of the actual error retrieved from ClickHouse if the error
// was generated there.
async fn handle_db_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, Error> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        // NOTE: ClickHouse returns 404 for all errors (so far encountered). We pull the text from
        // the body if possible, which contains the actual error from the database.
        let body = response.text().await.unwrap_or_else(|e| e.to_string());
        Err(Error::Database(body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query;
    use crate::query::field_table_name;
    use crate::query::measurement_table_name;
    use chrono::Utc;
    use omicron_test_utils::dev::clickhouse::ClickHouseInstance;
    use omicron_test_utils::dev::test_setup_log;
    use oximeter::histogram::Histogram;
    use oximeter::test_util;
    use oximeter::Datum;
    use oximeter::FieldValue;
    use oximeter::Metric;
    use oximeter::Target;
    use slog::o;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    // NOTE: It's important that each test run the ClickHouse server with different ports.
    // The tests each require a clean slate. Previously, we ran the tests in a different thread,
    // but we now use a different instance of the server to avoid conflicts.
    //
    // This is at least partially because ClickHouse by default provides pretty weak consistency
    // guarantees. There are options that allow controlling consistency behavior, but we've not yet
    // explored or decided on these.
    //
    // TODO-robustness TODO-correctness: Figure out the ClickHouse options we need.

    #[tokio::test]
    async fn test_build_client() {
        let logctx = test_setup_log("test_build_client");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        assert!(!client.is_oximeter_cluster().await.unwrap());

        client.wipe_single_node_db().await.unwrap();
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    // TODO(https://github.com/oxidecomputer/omicron/issues/4001): This job fails intermittently
    // on the ubuntu CI job with "Failed to detect ClickHouse subprocess within timeout"
    #[ignore]
    async fn test_build_replicated() {
        let logctx = test_setup_log("test_build_replicated");
        let log = &logctx.log;

        // Start all Keeper coordinator nodes
        let cur_dir = std::env::current_dir().unwrap();
        let keeper_config =
            cur_dir.as_path().join("src/configs/keeper_config.xml");

        // Start Keeper 1
        let k1_port = 9181;
        let k1_id = 1;

        let mut k1 = ClickHouseInstance::new_keeper(
            k1_port,
            k1_id,
            keeper_config.clone(),
        )
        .await
        .expect("Failed to start ClickHouse keeper 1");

        // Start Keeper 2
        let k2_port = 9182;
        let k2_id = 2;

        let mut k2 = ClickHouseInstance::new_keeper(
            k2_port,
            k2_id,
            keeper_config.clone(),
        )
        .await
        .expect("Failed to start ClickHouse keeper 2");

        // Start Keeper 3
        let k3_port = 9183;
        let k3_id = 3;

        let mut k3 =
            ClickHouseInstance::new_keeper(k3_port, k3_id, keeper_config)
                .await
                .expect("Failed to start ClickHouse keeper 3");

        // Start all replica nodes
        let cur_dir = std::env::current_dir().unwrap();
        let replica_config =
            cur_dir.as_path().join("src/configs/replica_config.xml");

        // Start Replica 1
        let r1_port = 8123;
        let r1_tcp_port = 9000;
        let r1_interserver_port = 9009;
        let r1_name = String::from("oximeter_cluster node 1");
        let r1_number = String::from("01");
        let mut db_1 = ClickHouseInstance::new_replicated(
            r1_port,
            r1_tcp_port,
            r1_interserver_port,
            r1_name,
            r1_number,
            replica_config.clone(),
        )
        .await
        .expect("Failed to start ClickHouse node 1");
        let r1_address =
            SocketAddr::new("127.0.0.1".parse().unwrap(), db_1.port());

        // Start Replica 2
        let r2_port = 8124;
        let r2_tcp_port = 9001;
        let r2_interserver_port = 9010;
        let r2_name = String::from("oximeter_cluster node 2");
        let r2_number = String::from("02");
        let mut db_2 = ClickHouseInstance::new_replicated(
            r2_port,
            r2_tcp_port,
            r2_interserver_port,
            r2_name,
            r2_number,
            replica_config,
        )
        .await
        .expect("Failed to start ClickHouse node 2");
        let r2_address =
            SocketAddr::new("127.0.0.1".parse().unwrap(), db_2.port());

        // Create database in node 1
        let client_1 = Client::new(r1_address, &log);
        assert!(client_1.is_oximeter_cluster().await.unwrap());
        client_1
            .init_replicated_db()
            .await
            .expect("Failed to initialize timeseries database");

        // Wait to make sure data has been synchronised.
        // TODO(https://github.com/oxidecomputer/omicron/issues/4001): Waiting for 5 secs is a bit sloppy,
        // come up with a better way to do this.
        sleep(Duration::from_secs(5)).await;

        // Verify database exists in node 2
        let client_2 = Client::new(r2_address, &log);
        assert!(client_2.is_oximeter_cluster().await.unwrap());
        let sql = String::from("SHOW DATABASES FORMAT JSONEachRow;");

        let result = client_2.execute_with_body(sql).await.unwrap();
        assert!(result.contains("oximeter"));

        k1.cleanup().await.expect("Failed to cleanup ClickHouse keeper 1");
        k2.cleanup().await.expect("Failed to cleanup ClickHouse keeper 2");
        k3.cleanup().await.expect("Failed to cleanup ClickHouse keeper 3");
        db_1.cleanup().await.expect("Failed to cleanup ClickHouse server 1");
        db_2.cleanup().await.expect("Failed to cleanup ClickHouse server 2");

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_client_insert() {
        let logctx = test_setup_log("test_client_insert");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        let samples = {
            let mut s = Vec::with_capacity(8);
            for _ in 0..s.capacity() {
                s.push(test_util::make_hist_sample())
            }
            s
        };
        client.insert_samples(&samples).await.unwrap();
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    // This is a target with the same name as that in `lib.rs` used for other tests, but with a
    // different set of fields. This is intentionally used to test schema mismatches.
    mod name_mismatch {
        #[derive(oximeter::Target)]
        pub struct TestTarget {
            pub name: String,
            pub name2: String,
            pub num: i64,
        }
    }

    mod type_mismatch {
        #[derive(oximeter::Target)]
        pub struct TestTarget {
            pub name1: uuid::Uuid,
            pub name2: String,
            pub num: i64,
        }
    }

    #[tokio::test]
    async fn test_recall_field_value_bool() {
        let field = FieldValue::Bool(true);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_u8() {
        let field = FieldValue::U8(1);
        let as_json = serde_json::Value::from(1_u8);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_i8() {
        let field = FieldValue::I8(1);
        let as_json = serde_json::Value::from(1_i8);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_u16() {
        let field = FieldValue::U16(1);
        let as_json = serde_json::Value::from(1_u16);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_i16() {
        let field = FieldValue::I16(1);
        let as_json = serde_json::Value::from(1_i16);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_u32() {
        let field = FieldValue::U32(1);
        let as_json = serde_json::Value::from(1_u32);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_i32() {
        let field = FieldValue::I32(1);
        let as_json = serde_json::Value::from(1_i32);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_u64() {
        let field = FieldValue::U64(1);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_i64() {
        let field = FieldValue::I64(1);
        let as_json = serde_json::Value::from(1_i64);
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_string() {
        let field = FieldValue::String("foo".into());
        let as_json = serde_json::Value::from("foo");
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_ipv4addr() {
        let field = FieldValue::from(Ipv4Addr::LOCALHOST);
        let as_json = serde_json::Value::from(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped().to_string(),
        );
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_ipv6addr() {
        let field = FieldValue::from(Ipv6Addr::LOCALHOST);
        let as_json = serde_json::Value::from(Ipv6Addr::LOCALHOST.to_string());
        test_recall_field_value_impl(field, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_field_value_uuid() {
        let id = Uuid::new_v4();
        let field = FieldValue::from(id);
        let as_json = serde_json::Value::from(id.to_string());
        test_recall_field_value_impl(field, as_json).await;
    }

    async fn test_recall_field_value_impl(
        field_value: FieldValue,
        as_json: serde_json::Value,
    ) {
        let logctx = test_setup_log(
            format!("test_recall_field_value_{}", field_value.field_type())
                .as_str(),
        );
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");

        // Insert a record from this field.
        const TIMESERIES_NAME: &str = "foo:bar";
        const TIMESERIES_KEY: u64 = 101;
        const FIELD_NAME: &str = "baz";

        let mut inserted_row = serde_json::Map::new();
        inserted_row
            .insert("timeseries_name".to_string(), TIMESERIES_NAME.into());
        inserted_row
            .insert("timeseries_key".to_string(), TIMESERIES_KEY.into());
        inserted_row.insert("field_name".to_string(), FIELD_NAME.into());
        inserted_row.insert("field_value".to_string(), as_json);
        let inserted_row = serde_json::Value::from(inserted_row);

        let row = serde_json::to_string(&inserted_row).unwrap();
        let field_table = field_table_name(field_value.field_type());
        let insert_sql = format!(
            "INSERT INTO oximeter.{field_table} FORMAT JSONEachRow {row}"
        );
        client.execute(insert_sql).await.expect("Failed to insert field row");

        // Select it exactly back out.
        let select_sql = format!(
            "SELECT * FROM oximeter.{} LIMIT 1 FORMAT {};",
            field_table_name(field_value.field_type()),
            crate::DATABASE_SELECT_FORMAT,
        );
        let body = client
            .execute_with_body(select_sql)
            .await
            .expect("Failed to select field row");
        let actual_row: serde_json::Value = serde_json::from_str(&body)
            .expect("Failed to parse field row JSON");
        println!("{actual_row:?}");
        println!("{inserted_row:?}");
        assert_eq!(
            actual_row, inserted_row,
            "Actual and expected field rows do not match"
        );
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_recall_measurement_bool() {
        let datum = Datum::Bool(true);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_i8() {
        let datum = Datum::I8(1);
        let as_json = serde_json::Value::from(1_i8);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_u8() {
        let datum = Datum::U8(1);
        let as_json = serde_json::Value::from(1_u8);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_i16() {
        let datum = Datum::I16(1);
        let as_json = serde_json::Value::from(1_i16);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_u16() {
        let datum = Datum::U16(1);
        let as_json = serde_json::Value::from(1_u16);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_i32() {
        let datum = Datum::I32(1);
        let as_json = serde_json::Value::from(1_i32);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_u32() {
        let datum = Datum::U32(1);
        let as_json = serde_json::Value::from(1_u32);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_i64() {
        let datum = Datum::I64(1);
        let as_json = serde_json::Value::from(1_i64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_u64() {
        let datum = Datum::U64(1);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_f32() {
        const VALUE: f32 = 1.1;
        let datum = Datum::F32(VALUE);
        // NOTE: This is intentionally an f64.
        let as_json = serde_json::Value::from(1.1_f64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_f64() {
        const VALUE: f64 = 1.1;
        let datum = Datum::F64(VALUE);
        let as_json = serde_json::Value::from(VALUE);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_cumulative_i64() {
        let datum = Datum::CumulativeI64(1.into());
        let as_json = serde_json::Value::from(1_i64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_cumulative_u64() {
        let datum = Datum::CumulativeU64(1.into());
        let as_json = serde_json::Value::from(1_u64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_cumulative_f64() {
        let datum = Datum::CumulativeF64(1.1.into());
        let as_json = serde_json::Value::from(1.1_f64);
        test_recall_measurement_impl::<u8>(datum, None, as_json).await;
    }

    async fn histogram_test_impl<T>(hist: Histogram<T>)
    where
        T: oximeter::histogram::HistogramSupport,
        Datum: From<oximeter::histogram::Histogram<T>>,
        serde_json::Value: From<T>,
    {
        let (bins, counts) = hist.to_arrays();
        let datum = Datum::from(hist);
        let as_json = serde_json::Value::Array(
            counts.into_iter().map(Into::into).collect(),
        );
        test_recall_measurement_impl(datum, Some(bins), as_json).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_i8() {
        let hist = Histogram::new(&[0i8, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_u8() {
        let hist = Histogram::new(&[0u8, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_i16() {
        let hist = Histogram::new(&[0i16, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_u16() {
        let hist = Histogram::new(&[0u16, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_i32() {
        let hist = Histogram::new(&[0i32, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_u32() {
        let hist = Histogram::new(&[0u32, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_i64() {
        let hist = Histogram::new(&[0i64, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_u64() {
        let hist = Histogram::new(&[0u64, 1, 2]).unwrap();
        histogram_test_impl(hist).await;
    }

    // NOTE: This test is ignored intentionally.
    //
    // We're using the JSONEachRow format to return data, which loses precision
    // for floating point values. This means we return the _double_ 0.1 from
    // the database as a `Value::Number`, which fails to compare equal to the
    // `Value::Number(0.1f32 as f64)` we sent in. That's because 0.1 is not
    // exactly representable in an `f32`, but it's close enough that ClickHouse
    // prints `0.1` in the result, which converts to a slightly different `f64`
    // than `0.1_f32 as f64` does.
    //
    // See https://github.com/oxidecomputer/omicron/issues/4059 for related
    // discussion.
    #[tokio::test]
    #[ignore]
    async fn test_recall_measurement_histogram_f32() {
        let hist = Histogram::new(&[0.1f32, 0.2, 0.3]).unwrap();
        histogram_test_impl(hist).await;
    }

    #[tokio::test]
    async fn test_recall_measurement_histogram_f64() {
        let hist = Histogram::new(&[0.1f64, 0.2, 0.3]).unwrap();
        histogram_test_impl(hist).await;
    }

    async fn test_recall_measurement_impl<T: Into<serde_json::Value> + Copy>(
        datum: Datum,
        maybe_bins: Option<Vec<T>>,
        json_datum: serde_json::Value,
    ) {
        let logctx = test_setup_log(
            format!("test_recall_measurement_{}", datum.datum_type()).as_str(),
        );
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");

        // Insert a record from this datum.
        const TIMESERIES_NAME: &str = "foo:bar";
        const TIMESERIES_KEY: u64 = 101;
        let mut inserted_row = serde_json::Map::new();
        inserted_row
            .insert("timeseries_name".to_string(), TIMESERIES_NAME.into());
        inserted_row
            .insert("timeseries_key".to_string(), TIMESERIES_KEY.into());
        inserted_row.insert(
            "timestamp".to_string(),
            Utc::now()
                .format(crate::DATABASE_TIMESTAMP_FORMAT)
                .to_string()
                .into(),
        );

        // Insert the start time and possibly bins.
        if let Some(start_time) = datum.start_time() {
            inserted_row.insert(
                "start_time".to_string(),
                start_time
                    .format(crate::DATABASE_TIMESTAMP_FORMAT)
                    .to_string()
                    .into(),
            );
        }
        if let Some(bins) = &maybe_bins {
            let bins = serde_json::Value::Array(
                bins.iter().copied().map(Into::into).collect(),
            );
            inserted_row.insert("bins".to_string(), bins);
            inserted_row.insert("counts".to_string(), json_datum);
        } else {
            inserted_row.insert("datum".to_string(), json_datum);
        }
        let inserted_row = serde_json::Value::from(inserted_row);

        let measurement_table = measurement_table_name(datum.datum_type());
        let row = serde_json::to_string(&inserted_row).unwrap();
        let insert_sql = format!(
            "INSERT INTO oximeter.{measurement_table} FORMAT JSONEachRow {row}",
        );
        client
            .execute(insert_sql)
            .await
            .expect("Failed to insert measurement row");

        // Select it exactly back out.
        let select_sql = format!(
            "SELECT * FROM oximeter.{} LIMIT 1 FORMAT {};",
            measurement_table,
            crate::DATABASE_SELECT_FORMAT,
        );
        let body = client
            .execute_with_body(select_sql)
            .await
            .expect("Failed to select measurement row");
        let actual_row: serde_json::Value = serde_json::from_str(&body)
            .expect("Failed to parse measurement row JSON");
        println!("{actual_row:?}");
        println!("{inserted_row:?}");
        assert_eq!(
            actual_row, inserted_row,
            "Actual and expected measurement rows do not match"
        );
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_schema_mismatch() {
        let logctx = test_setup_log("test_schema_mismatch");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        let sample = test_util::make_sample();
        client.insert_samples(&[sample]).await.unwrap();

        let bad_name = name_mismatch::TestTarget {
            name: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let metric = test_util::TestMetric {
            id: uuid::Uuid::new_v4(),
            good: true,
            datum: 1,
        };
        let sample = Sample::new(&bad_name, &metric).unwrap();
        let result = client.verify_sample_schema(&sample).await;
        assert!(matches!(result, Err(Error::SchemaMismatch { .. })));
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    // Returns the number of timeseries schemas being used.
    async fn get_schema_count(client: &Client) -> usize {
        client
            .execute_with_body(
                "SELECT * FROM oximeter.timeseries_schema FORMAT JSONEachRow;",
            )
            .await
            .expect("Failed to SELECT from database")
            .lines()
            .count()
    }

    #[tokio::test]
    async fn test_database_version_update_idempotent() {
        let logctx = test_setup_log("test_database_version_update_idempotent");
        let log = &logctx.log;

        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let replicated = false;

        // Initialize the database...
        let client = Client::new(address, &log);
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Insert data here so we can verify it still exists later.
        //
        // The values here don't matter much, we just want to check that
        // the database data hasn't been dropped.
        assert_eq!(0, get_schema_count(&client).await);
        let sample = test_util::make_sample();
        client.insert_samples(&[sample.clone()]).await.unwrap();
        assert_eq!(1, get_schema_count(&client).await);

        // Re-initialize the database, see that our data still exists
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        assert_eq!(1, get_schema_count(&client).await);

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_version_will_not_downgrade() {
        let logctx = test_setup_log("test_database_version_will_not_downgrade");
        let log = &logctx.log;

        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let replicated = false;

        // Initialize the database
        let client = Client::new(address, &log);
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Bump the version of the database to a "too new" version
        client
            .insert_version(model::OXIMETER_VERSION + 1)
            .await
            .expect("Failed to insert very new DB version");

        // Expect a failure re-initializing the client.
        //
        // This will attempt to initialize the client with "version =
        // model::OXIMETER_VERSION", which is "too old".
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect_err("Should have failed, downgrades are not supported");

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_database_version_wipes_old_version() {
        let logctx = test_setup_log("test_database_version_wipes_old_version");
        let log = &logctx.log;
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let replicated = false;

        // Initialize the Client
        let client = Client::new(address, &log);
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Insert data here so we can remove it later.
        //
        // The values here don't matter much, we just want to check that
        // the database data gets dropped later.
        assert_eq!(0, get_schema_count(&client).await);
        let sample = test_util::make_sample();
        client.insert_samples(&[sample.clone()]).await.unwrap();
        assert_eq!(1, get_schema_count(&client).await);

        // If we try to upgrade to a newer version, we'll drop old data.
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION + 1)
            .await
            .expect("Should have initialized database successfully");
        assert_eq!(0, get_schema_count(&client).await);

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_schema_update() {
        let logctx = test_setup_log("test_schema_update");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        let sample = test_util::make_sample();

        // Verify that this sample is considered new, i.e., we return rows to update the timeseries
        // schema table.
        let result = client.verify_sample_schema(&sample).await.unwrap();
        assert!(
            matches!(result, Some(_)),
            "When verifying a new sample, the rows to be inserted should be returned"
        );

        // Clear the internal caches of seen schema
        client.schema.lock().await.clear();

        // Insert the new sample
        client.insert_samples(&[sample.clone()]).await.unwrap();

        // The internal map should now contain both the new timeseries schema
        let actual_schema = model::schema_for(&sample);
        let timeseries_name =
            TimeseriesName::try_from(sample.timeseries_name.as_str()).unwrap();
        let expected_schema = client
            .schema
            .lock()
            .await
            .get(&timeseries_name)
            .expect(
                "After inserting a new sample, its schema should be included",
            )
            .clone();
        assert_eq!(
            actual_schema,
            expected_schema,
            "The timeseries schema for a new sample was not correctly inserted into internal cache",
        );

        // This should no longer return a new row to be inserted for the schema of this sample, as
        // any schema have been included above.
        let result = client.verify_sample_schema(&sample).await.unwrap();
        assert!(
            matches!(result, None),
            "After inserting new schema, it should no longer be considered new"
        );

        // Verify that it's actually in the database!
        let sql = String::from(
            "SELECT * FROM oximeter.timeseries_schema FORMAT JSONEachRow;",
        );
        let result = client.execute_with_body(sql).await.unwrap();
        let schema = result
            .lines()
            .map(|line| {
                TimeseriesSchema::from(
                    serde_json::from_str::<model::DbTimeseriesSchema>(&line)
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(schema.len(), 1);
        assert_eq!(expected_schema, schema[0]);

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    async fn setup_filter_testcase() -> (ClickHouseInstance, Client, Vec<Sample>)
    {
        let log = slog::Logger::root(slog_dtrace::Dtrace::new().0, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");

        // Create sample data
        let (n_projects, n_instances, n_cpus, n_samples) = (2, 2, 2, 2);
        let samples = test_util::generate_test_samples(
            n_projects,
            n_instances,
            n_cpus,
            n_samples,
        );
        assert_eq!(
            samples.len(),
            n_projects * n_instances * n_cpus * n_samples
        );

        client.insert_samples(&samples).await.unwrap();
        (db, client, samples)
    }

    #[tokio::test]
    async fn test_client_select_timeseries_one() {
        let (mut db, client, samples) = setup_filter_testcase().await;
        let sample = samples.first().unwrap();
        let target_fields = sample.target_fields().collect::<Vec<_>>();
        let metric_fields = sample.metric_fields().collect::<Vec<_>>();
        let criteria = &[
            format!(
                "project_id=={}",
                target_fields
                    .iter()
                    .find(|f| f.name == "project_id")
                    .unwrap()
                    .value
            ),
            format!(
                "instance_id=={}",
                target_fields
                    .iter()
                    .find(|f| f.name == "instance_id")
                    .unwrap()
                    .value
            ),
            format!("cpu_id=={}", metric_fields[0].value),
        ];
        let results = client
            .select_timeseries_with(
                &sample.timeseries_name,
                &criteria.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "Expected to find a single timeseries");
        let timeseries = &results[0];
        assert_eq!(
            timeseries.measurements.len(),
            2,
            "Expected 2 samples per timeseries"
        );

        // Compare measurements themselves
        let expected_measurements =
            samples.iter().map(|sample| &sample.measurement);
        let actual_measurements = timeseries.measurements.iter();
        assert!(actual_measurements
            .zip(expected_measurements)
            .all(|(first, second)| first == second));
        assert_eq!(timeseries.target.name, "virtual_machine");
        // Compare fields, but order might be different.
        fn field_cmp<'a>(
            needle: &'a crate::Field,
            mut haystack: impl Iterator<Item = &'a crate::Field>,
        ) -> bool {
            needle == haystack.find(|f| f.name == needle.name).unwrap()
        }
        timeseries
            .target
            .fields
            .iter()
            .all(|field| field_cmp(field, sample.target_fields()));
        assert_eq!(timeseries.metric.name, "cpu_busy");
        timeseries
            .metric
            .fields
            .iter()
            .all(|field| field_cmp(field, sample.metric_fields()));
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    #[tokio::test]
    async fn test_field_record_count() {
        // This test verifies that the number of records in the field tables is as expected.
        //
        // Because of the schema change, inserting field records per field per unique timeseries,
        // we'd like to exercise the logic of ClickHouse's replacing merge tree engine.
        let (mut db, client, samples) = setup_filter_testcase().await;

        async fn assert_table_count(
            client: &Client,
            table: &str,
            expected_count: usize,
        ) {
            let body = client
                .execute_with_body(format!(
                    "SELECT COUNT() FROM oximeter.{};",
                    table
                ))
                .await
                .unwrap();
            let actual_count =
                body.lines().next().unwrap().trim().parse::<usize>().expect(
                    "Expected a count of the number of rows from ClickHouse",
                );
            assert_eq!(actual_count, expected_count);
        }

        // There should be (2 projects * 2 instances * 2 cpus) == 8 timeseries. For each of these
        // timeseries, there are 2 UUID fields, `project_id` and `instance_id`. So 16 UUID records.
        assert_table_count(&client, "fields_uuid", 16).await;

        // However, there's only 1 i64 field, `cpu_id`.
        assert_table_count(&client, "fields_i64", 8).await;

        assert_table_count(
            &client,
            "measurements_cumulativef64",
            samples.len(),
        )
        .await;
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    // Regression test verifying that integers are returned in the expected format from the
    // database.
    //
    // By default, ClickHouse _quotes_ 64-bit integers, which is apparently to support JavaScript
    // implementations of JSON. See https://github.com/ClickHouse/ClickHouse/issues/2375 for
    // details. This test verifies that we get back _unquoted_ integers from the database.
    #[tokio::test]
    async fn test_unquoted_64bit_integers() {
        use serde_json::Value;
        let (mut db, client, _) = setup_filter_testcase().await;
        let output = client
            .execute_with_body(
                "SELECT toUInt64(1) AS foo FORMAT JSONEachRow;".to_string(),
            )
            .await
            .unwrap();
        let json: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(json["foo"], Value::Number(1u64.into()));
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    #[tokio::test]
    async fn test_bad_database_connection() {
        let logctx = test_setup_log("test_bad_database_connection");
        let log = &logctx.log;
        let client = Client::new("127.0.0.1:443".parse().unwrap(), &log);
        assert!(matches!(
            client.ping().await,
            Err(Error::DatabaseUnavailable(_))
        ));
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_differentiate_by_timeseries_name() {
        #[derive(Debug, Default, PartialEq, oximeter::Target)]
        struct MyTarget {
            id: i64,
        }

        // These two metrics share a target and have no fields. Thus they have the same timeseries
        // keys. This test is to verify we can distinguish between them, which relies on their
        // names.
        #[derive(Debug, Default, PartialEq, oximeter::Metric)]
        struct FirstMetric {
            datum: i64,
        }

        #[derive(Debug, Default, PartialEq, oximeter::Metric)]
        struct SecondMetric {
            datum: i64,
        }

        let logctx = test_setup_log("test_differentiate_by_timeseries_name");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");

        let target = MyTarget::default();
        let first_metric = FirstMetric::default();
        let second_metric = SecondMetric::default();

        let samples = &[
            Sample::new(&target, &first_metric).unwrap(),
            Sample::new(&target, &second_metric).unwrap(),
        ];
        client
            .insert_samples(samples)
            .await
            .expect("Failed to insert test samples");

        let results = client
            .select_timeseries_with(
                "my_target:second_metric",
                &["id==0"],
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to select test samples");
        println!("{:#?}", results);
        assert_eq!(results.len(), 1, "Expected only one timeseries");
        let timeseries = &results[0];
        assert_eq!(
            timeseries.measurements.len(),
            1,
            "Expected only one sample"
        );
        assert_eq!(timeseries.target.name, "my_target");
        assert_eq!(timeseries.metric.name, "second_metric");
        logctx.cleanup_successful();
    }

    #[derive(Debug, Clone, oximeter::Target)]
    struct Service {
        name: String,
        id: uuid::Uuid,
    }
    #[derive(Debug, Clone, oximeter::Metric)]
    struct RequestLatency {
        route: String,
        method: String,
        status_code: i64,
        #[datum]
        latency: f64,
    }

    const SELECT_TEST_ID: &str = "4fa827ea-38bb-c37e-ac2d-f8432ca9c76e";
    fn setup_select_test() -> (Service, Vec<RequestLatency>, Vec<Sample>) {
        // One target
        let id = SELECT_TEST_ID.parse().unwrap();
        let target = Service { name: "oximeter".to_string(), id };

        // Many metrics
        let routes = &["/a", "/b"];
        let methods = &["GET", "POST"];
        let status_codes = &[200, 204, 500];

        // Two samples each
        let n_timeseries = routes.len() * methods.len() * status_codes.len();
        let mut metrics = Vec::with_capacity(n_timeseries);
        let mut samples = Vec::with_capacity(n_timeseries * 2);
        for (route, method, status_code) in
            itertools::iproduct!(routes, methods, status_codes)
        {
            let metric = RequestLatency {
                route: route.to_string(),
                method: method.to_string(),
                status_code: *status_code,
                latency: 0.0,
            };
            samples.push(Sample::new(&target, &metric).unwrap());
            samples.push(Sample::new(&target, &metric).unwrap());
            metrics.push(metric);
        }
        (target, metrics, samples)
    }

    async fn test_select_timeseries_with_impl(
        criteria: &[&str],
        start_time: Option<query::Timestamp>,
        end_time: Option<query::Timestamp>,
        test_fn: impl Fn(&Service, &[RequestLatency], &[Sample], &[Timeseries]),
    ) {
        let (target, metrics, samples) = setup_select_test();
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let log = Logger::root(slog::Discard, o!());
        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");
        let timeseries_name = "service:request_latency";
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                start_time,
                end_time,
                None,
                None,
            )
            .await
            .expect("Failed to select timeseries");

        // NOTE: Timeseries as returned from the database are sorted by (name, key, timestamp).
        // However, that key is a hash of the field values, which effectively randomizes each
        // timeseries with the same name relative to one another. Resort them here, so that the
        // timeseries are in ascending order of first timestamp, so that we can reliably test them.
        timeseries.sort_by(|first, second| {
            first.measurements[0]
                .timestamp()
                .cmp(&second.measurements[0].timestamp())
        });

        test_fn(&target, &metrics, &samples, &timeseries);

        db.cleanup().await.expect("Failed to cleanup database");
    }

    // Small helper to go from a mulidimensional index to a flattened array index.
    fn unravel_index(idx: &[usize; 4]) -> usize {
        let strides = [12, 6, 2, 1];
        let mut index = 0;
        for (i, stride) in idx.iter().rev().zip(strides.iter().rev()) {
            index += i * stride;
        }
        index
    }

    #[test]
    fn test_unravel_index() {
        assert_eq!(unravel_index(&[0, 0, 0, 0]), 0);
        assert_eq!(unravel_index(&[0, 0, 1, 0]), 2);
        assert_eq!(unravel_index(&[1, 0, 0, 0]), 12);
        assert_eq!(unravel_index(&[1, 0, 0, 1]), 13);
        assert_eq!(unravel_index(&[1, 0, 1, 0]), 14);
        assert_eq!(unravel_index(&[1, 1, 2, 1]), 23);
    }

    fn verify_measurements(
        measurements: &[oximeter::Measurement],
        samples: &[Sample],
    ) {
        for (measurement, sample) in measurements.iter().zip(samples.iter()) {
            assert_eq!(
                measurement, &sample.measurement,
                "Mismatch between retrieved and expected measurement",
            );
        }
    }

    fn verify_target(actual: &crate::Target, expected: &Service) {
        assert_eq!(actual.name, expected.name());
        for (field_name, field_value) in expected
            .field_names()
            .into_iter()
            .zip(expected.field_values().into_iter())
        {
            let actual_field = actual
                .fields
                .iter()
                .find(|f| f.name == *field_name)
                .expect("Missing field in recovered timeseries target");
            assert_eq!(
                actual_field.value, field_value,
                "Incorrect field value in timeseries target"
            );
        }
    }

    fn verify_metric(actual: &crate::Metric, expected: &RequestLatency) {
        assert_eq!(actual.name, expected.name());
        for (field_name, field_value) in expected
            .field_names()
            .into_iter()
            .zip(expected.field_values().into_iter())
        {
            let actual_field = actual
                .fields
                .iter()
                .find(|f| f.name == *field_name)
                .expect("Missing field in recovered timeseries metric");
            assert_eq!(
                actual_field.value, field_value,
                "Incorrect field value in timeseries metric"
            );
        }
    }

    #[tokio::test]
    async fn test_select_timeseries_with_select_one() {
        // This set of criteria should select exactly one timeseries, with two measurements.
        // The target is the same in all cases, but we're looking for the first of the metrics, and
        // the first two samples/measurements.
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code==200"];
        fn test_fn(
            target: &Service,
            metrics: &[RequestLatency],
            samples: &[Sample],
            timeseries: &[Timeseries],
        ) {
            assert_eq!(timeseries.len(), 1, "Expected one timeseries");
            let timeseries = timeseries.get(0).unwrap();
            assert_eq!(
                timeseries.measurements.len(),
                2,
                "Expected exactly two measurements"
            );
            verify_measurements(&timeseries.measurements, &samples[..2]);
            verify_target(&timeseries.target, target);
            verify_metric(&timeseries.metric, metrics.get(0).unwrap());
        }
        test_select_timeseries_with_impl(criteria, None, None, test_fn).await;
    }

    #[tokio::test]
    async fn test_select_timeseries_with_select_one_field_with_multiple_values()
    {
        // This set of criteria should select the last two metrics, and so the last two
        // timeseries. The target is the same in all cases.
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code>200"];
        fn test_fn(
            target: &Service,
            metrics: &[RequestLatency],
            samples: &[Sample],
            timeseries: &[Timeseries],
        ) {
            assert_eq!(timeseries.len(), 2, "Expected two timeseries");
            for (i, ts) in timeseries.iter().enumerate() {
                assert_eq!(
                    ts.measurements.len(),
                    2,
                    "Expected exactly two measurements"
                );

                // Metrics 1..3 in the third axis, status code.
                let sample_start = unravel_index(&[0, 0, i + 1, 0]);
                let sample_end = sample_start + 2;
                verify_measurements(
                    &ts.measurements,
                    &samples[sample_start..sample_end],
                );
                verify_target(&ts.target, target);
            }

            for (ts, metric) in timeseries.iter().zip(metrics[1..3].iter()) {
                verify_metric(&ts.metric, metric);
            }
        }
        test_select_timeseries_with_impl(criteria, None, None, test_fn).await;
    }

    #[tokio::test]
    async fn test_select_timeseries_with_select_multiple_fields_with_multiple_values(
    ) {
        // This is non-selective for the route, which is the "second axis", and has two values for
        // the third axis, status code. There should be a total of 4 timeseries, since there are
        // two methods and two possible status codes.
        let criteria = &["name==oximeter", "route==/a", "status_code>200"];
        fn test_fn(
            target: &Service,
            metrics: &[RequestLatency],
            samples: &[Sample],
            timeseries: &[Timeseries],
        ) {
            assert_eq!(timeseries.len(), 4, "Expected four timeseries");
            let indices = &[(0, 1), (0, 2), (1, 1), (1, 2)];
            for (i, ts) in timeseries.iter().enumerate() {
                assert_eq!(
                    ts.measurements.len(),
                    2,
                    "Expected exactly two measurements"
                );

                // Metrics 0..2 in the second axis, method
                // Metrics 1..3 in the third axis, status code.
                let (i0, i1) = indices[i];
                let sample_start = unravel_index(&[0, i0, i1, 0]);
                let sample_end = sample_start + 2;
                verify_measurements(
                    &ts.measurements,
                    &samples[sample_start..sample_end],
                );
                verify_target(&ts.target, target);
            }

            let mut ts_iter = timeseries.iter();
            for i in 0..2 {
                for j in 1..3 {
                    let ts = ts_iter.next().unwrap();
                    let metric = metrics.get(i * 3 + j).unwrap();
                    verify_metric(&ts.metric, metric);
                }
            }
        }
        test_select_timeseries_with_impl(criteria, None, None, test_fn).await;
    }

    #[tokio::test]
    async fn test_select_timeseries_with_all() {
        // We're selecting all timeseries/samples here.
        let criteria = &[];
        fn test_fn(
            target: &Service,
            metrics: &[RequestLatency],
            samples: &[Sample],
            timeseries: &[Timeseries],
        ) {
            assert_eq!(timeseries.len(), 12, "Expected 12 timeseries");
            for (i, ts) in timeseries.iter().enumerate() {
                assert_eq!(
                    ts.measurements.len(),
                    2,
                    "Expected exactly two measurements"
                );

                let sample_start = i * 2;
                let sample_end = sample_start + 2;
                verify_measurements(
                    &ts.measurements,
                    &samples[sample_start..sample_end],
                );
                verify_target(&ts.target, target);
                verify_metric(&ts.metric, metrics.get(i).unwrap());
            }
        }
        test_select_timeseries_with_impl(criteria, None, None, test_fn).await;
    }

    #[tokio::test]
    async fn test_select_timeseries_with_start_time() {
        let (_, metrics, samples) = setup_select_test();
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let logctx = test_setup_log("test_select_timeseries_with_start_time");
        let log = &logctx.log;
        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");
        let timeseries_name = "service:request_latency";
        let start_time = samples[samples.len() / 2].measurement.timestamp();
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                &[],
                Some(query::Timestamp::Exclusive(start_time)),
                None,
                None,
                None,
            )
            .await
            .expect("Failed to select timeseries");
        timeseries.sort_by(|first, second| {
            first.measurements[0]
                .timestamp()
                .cmp(&second.measurements[0].timestamp())
        });
        assert_eq!(timeseries.len(), metrics.len() / 2);
        for ts in timeseries.iter() {
            for meas in ts.measurements.iter() {
                assert!(meas.timestamp() > start_time);
            }
        }
        db.cleanup().await.expect("Failed to cleanup database");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_select_timeseries_with_limit() {
        let (_, _, samples) = setup_select_test();
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let logctx = test_setup_log("test_select_timeseries_with_limit");
        let log = &logctx.log;
        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");
        let timeseries_name = "service:request_latency";

        // So we have to define criteria that resolve to a single timeseries
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code==200"];

        // First, query without a limit. We should see all the results.
        let all_measurements = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to select timeseries")[0]
            .measurements;

        // Check some constraints on the number of measurements - we
        // can change these, but these assumptions make the test simpler.
        //
        // For now, assume we can cleanly cut the number of measurements in
        // half.
        assert!(all_measurements.len() >= 2);
        assert!(all_measurements.len() % 2 == 0);

        // Next, let's set a limit to half the results and query again.
        let limit =
            NonZeroU32::new(u32::try_from(all_measurements.len() / 2).unwrap())
                .unwrap();

        // We expect queries with a limit to fail when they fail to resolve to a
        // single timeseries, so run a query without any criteria to test that
        client
            .select_timeseries_with(
                timeseries_name,
                &[],
                None,
                None,
                Some(limit),
                None,
            )
            .await
            .expect_err("Should fail to select timeseries");

        // queries with limit do not fail when they resolve to zero timeseries
        let empty_result = &client
            .select_timeseries_with(
                timeseries_name,
                &["name==not_a_real_name"],
                None,
                None,
                Some(limit),
                None,
            )
            .await
            .expect("Failed to select timeseries");
        assert_eq!(empty_result.len(), 0);

        let timeseries = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                Some(limit),
                None,
            )
            .await
            .expect("Failed to select timeseries")[0];
        assert_eq!(timeseries.measurements.len() as u32, limit.get());
        assert_eq!(
            all_measurements[..all_measurements.len() / 2],
            timeseries.measurements
        );

        // Get the other half of the results.
        let timeseries = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                Some(query::Timestamp::Exclusive(
                    timeseries.measurements.last().unwrap().timestamp(),
                )),
                None,
                Some(limit),
                None,
            )
            .await
            .expect("Failed to select timeseries")[0];
        assert_eq!(timeseries.measurements.len() as u32, limit.get());
        assert_eq!(
            all_measurements[all_measurements.len() / 2..],
            timeseries.measurements
        );

        db.cleanup().await.expect("Failed to cleanup database");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_select_timeseries_with_order() {
        let (_, _, samples) = setup_select_test();
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let logctx = test_setup_log("test_select_timeseries_with_order");
        let log = &logctx.log;
        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");
        let timeseries_name = "service:request_latency";

        // Limits only work with a single timeseries, so we have to use criteria
        // that resolve to a single timeseries
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code==200"];

        // First, query without an order. We should see all the results in ascending order.
        let all_measurements = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to select timeseries")[0]
            .measurements;

        assert!(
            all_measurements.len() > 1,
            "need more than one measurement to test ordering"
        );

        // Explicitly specifying asc should give the same results
        let timeseries_asc = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                None,
                Some(PaginationOrder::Ascending),
            )
            .await
            .expect("Failed to select timeseries")[0]
            .measurements;
        assert_eq!(all_measurements, timeseries_asc);

        // Now get the results in reverse order
        let timeseries_desc = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                None,
                Some(PaginationOrder::Descending),
            )
            .await
            .expect("Failed to select timeseries")[0]
            .measurements;

        let mut timeseries_asc_rev = timeseries_asc.clone();
        timeseries_asc_rev.reverse();

        assert_ne!(timeseries_desc, timeseries_asc);
        assert_eq!(timeseries_desc, &timeseries_asc_rev);

        // can use limit 1 to get single most recent measurement
        let desc_limit_1 = &client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
                Some(NonZeroU32::new(1).unwrap()),
                Some(PaginationOrder::Descending),
            )
            .await
            .expect("Failed to select timeseries")[0]
            .measurements;

        assert_eq!(desc_limit_1.len(), 1);
        assert_eq!(
            desc_limit_1.first().unwrap(),
            timeseries_asc.last().unwrap()
        );

        db.cleanup().await.expect("Failed to cleanup database");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_get_schema_no_new_values() {
        let (mut db, client, _) = setup_filter_testcase().await;
        let original_schema = client.schema.lock().await.clone();
        let mut schema = client.schema.lock().await;
        client
            .get_schema_locked(&mut schema)
            .await
            .expect("Failed to get timeseries schema");
        assert_eq!(&original_schema, &*schema, "Schema shouldn't change");
        db.cleanup().await.expect("Failed to cleanup database");
    }

    #[tokio::test]
    async fn test_timeseries_schema_list() {
        use std::convert::TryInto;

        let (mut db, client, _) = setup_filter_testcase().await;
        let limit = 100u32.try_into().unwrap();
        let page = dropshot::WhichPage::First(dropshot::EmptyScanParams {});
        let result = client.timeseries_schema_list(&page, limit).await.unwrap();
        assert!(
            result.items.len() == 1,
            "Expected exactly 1 timeseries schema"
        );
        let last_seen = result.items.last().unwrap().timeseries_name.clone();
        let page = dropshot::WhichPage::Next(last_seen);
        let result = client.timeseries_schema_list(&page, limit).await.unwrap();
        assert!(
            result.items.is_empty(),
            "Expected the next page of schema to be empty"
        );
        assert!(
            result.next_page.is_none(),
            "Expected the next page token to be None"
        );
        db.cleanup().await.expect("Failed to cleanup database");
    }

    #[tokio::test]
    async fn test_list_timeseries() {
        use std::convert::TryInto;

        let (mut db, client, _) = setup_filter_testcase().await;
        let limit = 7u32.try_into().unwrap();
        let params = crate::TimeseriesScanParams {
            timeseries_name: TimeseriesName::try_from(
                "virtual_machine:cpu_busy",
            )
            .unwrap(),
            criteria: vec!["cpu_id==0".parse().unwrap()],
            start_time: None,
            end_time: None,
        };
        let page = dropshot::WhichPage::First(params.clone());
        let result = client.list_timeseries(&page, limit).await.unwrap();

        // We should have 4 timeseries, with 2 samples from each of the first 3 and 1 from the last
        // timeseries.
        assert_eq!(result.items.len(), 4, "Expected 4 timeseries");
        assert_eq!(
            result.items.last().unwrap().measurements.len(),
            1,
            "Expected 1 sample from the last timeseries"
        );
        assert!(
            result.items.iter().take(3).all(|ts| ts.measurements.len() == 2),
            "Expected 2 samples from the first 3 timeseries"
        );
        let last_timeseries = result.items.last().unwrap();

        // Get the next page.
        //
        // We have to recreate this as dropshot would, since we cannot build the pagination params
        // ourselves.
        let next_page = crate::TimeseriesPageSelector { params, offset: limit };
        let page = dropshot::WhichPage::Next(next_page);
        let result = client.list_timeseries(&page, limit).await.unwrap();

        // We should now have the one remaining sample
        assert_eq!(
            result.items.len(),
            1,
            "Expected only 1 timeseries after paginating"
        );
        assert_eq!(
            result.items[0].measurements.len(),
            1,
            "Expected only the last sample after paginating"
        );
        assert_eq!(
            result.items[0].timeseries_name, last_timeseries.timeseries_name,
            "Paginating should pick up where it left off"
        );
        assert_eq!(
            result.items[0].target, last_timeseries.target,
            "Paginating should pick up where it left off"
        );
        assert_eq!(
            result.items[0].metric, last_timeseries.metric,
            "Paginating should pick up where it left off"
        );
        db.cleanup().await.expect("Failed to cleanup database");
    }

    #[tokio::test]
    async fn test_update_schema_cache_on_new_sample() {
        usdt::register_probes().unwrap();
        let logctx = test_setup_log("test_update_schema_cache_on_new_sample");
        let log = &logctx.log;

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new_single_node(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");
        let samples = [test_util::make_sample()];
        client.insert_samples(&samples).await.unwrap();

        // Get the count of schema directly from the DB, which should have just
        // one.
        let response = client.execute_with_body(
            "SELECT COUNT() FROM oximeter.timeseries_schema FORMAT JSONEachRow;
        ").await.unwrap();
        assert_eq!(response.lines().count(), 1, "Expected exactly 1 schema");
        assert_eq!(client.schema.lock().await.len(), 1);

        // Clear the internal cache, and insert the sample again.
        //
        // This should cause us to look up the schema in the DB again, but _not_
        // insert a new one.
        client.schema.lock().await.clear();
        assert!(client.schema.lock().await.is_empty());

        client.insert_samples(&samples).await.unwrap();

        // Get the count of schema directly from the DB, which should still have
        // only the one schema.
        let response = client.execute_with_body(
            "SELECT COUNT() FROM oximeter.timeseries_schema FORMAT JSONEachRow;
        ").await.unwrap();
        assert_eq!(
            response.lines().count(),
            1,
            "Expected exactly 1 schema again"
        );
        assert_eq!(client.schema.lock().await.len(), 1);
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }
}
