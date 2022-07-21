// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rust client to ClickHouse database
// Copyright 2021 Oxide Computer Company

use crate::{
    model, query, Error, Metric, Target, Timeseries, TimeseriesPageSelector,
    TimeseriesScanParams, TimeseriesSchema,
};
use crate::{TimeseriesKey, TimeseriesName};
use async_trait::async_trait;
use dropshot::{EmptyScanParams, ResultsPage, WhichPage};
use oximeter::types::Sample;
use slog::{debug, error, trace, Logger};
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Mutex;
use uuid::Uuid;

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
            Ok(vec![])
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
        {
            let map = self.schema.lock().unwrap();
            if let Some(s) = map.get(name) {
                return Ok(Some(s.clone()));
            }
        }
        // `get_schema` acquires the lock internally, so the above scope is required to avoid
        // deadlock.
        self.get_schema().await?;
        Ok(self.schema.lock().unwrap().get(name).map(Clone::clone))
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
        let schema = model::schema_for(sample);
        let name = schema.timeseries_name.clone();
        let maybe_new_schema = match self.schema.lock().unwrap().entry(name) {
            Entry::Vacant(entry) => Ok(Some(entry.insert(schema).clone())),
            Entry::Occupied(entry) => {
                let existing_schema = entry.get();
                if existing_schema == &schema {
                    Ok(None)
                } else {
                    let err =
                        error_for_schema_mismatch(&schema, &existing_schema);
                    error!(
                        self.log,
                        "timeseries schema mismatch, sample will be skipped: {}",
                        err
                    );
                    Err(err)
                }
            }
        }?;
        Ok(maybe_new_schema.map(|schema| {
            serde_json::to_string(&model::DbTimeseriesSchema::from(schema))
                .expect("Failed to convert schema to DB model")
        }))
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
        Ok(timeseries_by_key.into_iter().map(|(_, item)| item).collect())
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
        handle_db_response(
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
        .map_err(|err| Error::Database(err.to_string()))
    }

    async fn get_schema(&self) -> Result<(), Error> {
        debug!(self.log, "retrieving timeseries schema from database");
        let sql = {
            let schema = self.schema.lock().unwrap();
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
            self.schema.lock().unwrap().extend(new);
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

    /// Initialize the telemetry database, creating tables as needed.
    async fn init_db(&self) -> Result<(), Error>;

    /// Wipe the ClickHouse database entirely.
    async fn wipe_db(&self) -> Result<(), Error>;
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
                        debug!(self.log, "new timeseries schema: {:?}", schema);
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

    /// Initialize the telemetry database, creating tables as needed.
    async fn init_db(&self) -> Result<(), Error> {
        // The HTTP client doesn't support multiple statements per query, so we break them out here
        // manually.
        debug!(self.log, "initializing ClickHouse database");
        let sql = include_str!("./db-init.sql");
        for query in sql.split("\n--\n") {
            self.execute(query.to_string()).await?;
        }
        Ok(())
    }

    /// Wipe the ClickHouse database entirely.
    async fn wipe_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        let sql = include_str!("./db-wipe.sql").to_string();
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

// Generate an error describing a schema mismatch
fn error_for_schema_mismatch(
    schema: &TimeseriesSchema,
    existing_schema: &TimeseriesSchema,
) -> Error {
    let expected = existing_schema
        .field_schema
        .iter()
        .map(|field| (field.name.clone(), field.ty))
        .collect();
    let actual = schema
        .field_schema
        .iter()
        .map(|field| (field.name.clone(), field.ty))
        .collect();
    Error::SchemaMismatch {
        name: schema.timeseries_name.to_string(),
        expected,
        actual,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query;
    use omicron_test_utils::dev::clickhouse::ClickHouseInstance;
    use oximeter::test_util;
    use oximeter::{Metric, Target};
    use slog::o;

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
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        Client::new(address, &log).wipe_db().await.unwrap();
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    #[tokio::test]
    async fn test_client_insert() {
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_db()
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
    async fn test_schema_mismatch() {
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_db()
            .await
            .expect("Failed to initialize timeseries database");
        let sample = test_util::make_sample();
        client.insert_samples(&vec![sample]).await.unwrap();

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
        let sample = Sample::new(&bad_name, &metric);
        let result = client.verify_sample_schema(&sample).await;
        assert!(matches!(result, Err(Error::SchemaMismatch { .. })));
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    #[tokio::test]
    async fn test_schema_update() {
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_db()
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
        client.schema.lock().unwrap().clear();

        // Insert the new sample
        client.insert_samples(&[sample.clone()]).await.unwrap();

        // The internal map should now contain both the new timeseries schema
        let actual_schema = model::schema_for(&sample);
        let timeseries_name =
            TimeseriesName::try_from(sample.timeseries_name.as_str()).unwrap();
        let expected_schema = client
            .schema
            .lock()
            .unwrap()
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
    }

    async fn setup_filter_testcase() -> (ClickHouseInstance, Client, Vec<Sample>)
    {
        let log = slog::Logger::root(slog_dtrace::Dtrace::new().0, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_db()
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
        let target_fields = sample.target_fields();
        let metric_fields = sample.metric_fields();
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
        let field_cmp = |needle: &crate::Field, haystack: &[crate::Field]| {
            needle == haystack.iter().find(|f| f.name == needle.name).unwrap()
        };
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
        let log = slog::Logger::root(slog::Discard, o!());
        let client = Client::new("127.0.0.1:443".parse().unwrap(), &log);
        assert!(matches!(
            client.ping().await,
            Err(Error::DatabaseUnavailable(_))
        ));
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

        let log = Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, &log);
        client
            .init_db()
            .await
            .expect("Failed to initialize timeseries database");

        let target = MyTarget::default();
        let first_metric = FirstMetric::default();
        let second_metric = SecondMetric::default();

        let samples = &[
            Sample::new(&target, &first_metric),
            Sample::new(&target, &second_metric),
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
            samples.push(Sample::new(&target, &metric));
            samples.push(Sample::new(&target, &metric));
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
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let log = Logger::root(slog::Discard, o!());
        let client = Client::new(address, &log);
        client
            .init_db()
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
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let log = Logger::root(slog::Discard, o!());
        let client = Client::new(address, &log);
        client
            .init_db()
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
    }

    #[tokio::test]
    async fn test_select_timeseries_with_limit() {
        let (_, _, samples) = setup_select_test();
        let mut db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());
        let log = Logger::root(slog::Discard, o!());
        let client = Client::new(address, &log);
        client
            .init_db()
            .await
            .expect("Failed to initialize timeseries database");
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");
        let timeseries_name = "service:request_latency";

        // First, query without a limit. We should see all the results.
        let all_measurements = &client
            .select_timeseries_with(timeseries_name, &[], None, None, None)
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
        let timeseries = &client
            .select_timeseries_with(
                timeseries_name,
                &[],
                None,
                None,
                Some(limit),
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
                &[],
                Some(query::Timestamp::Exclusive(
                    timeseries.measurements.last().unwrap().timestamp(),
                )),
                None,
                Some(limit),
            )
            .await
            .expect("Failed to select timeseries")[0];
        assert_eq!(timeseries.measurements.len() as u32, limit.get());
        assert_eq!(
            all_measurements[all_measurements.len() / 2..],
            timeseries.measurements
        );

        db.cleanup().await.expect("Failed to cleanup database");
    }

    #[tokio::test]
    async fn test_get_schema_no_new_values() {
        let (mut db, client, _) = setup_filter_testcase().await;
        let schema = &client.schema.lock().unwrap().clone();
        client.get_schema().await.expect("Failed to get timeseries schema");
        assert_eq!(
            schema,
            &*client.schema.lock().unwrap(),
            "Schema shouldn't change"
        );
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
}
