//! Rust client to ClickHouse database
// Copyright 2021 Oxide Computer Company

use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use slog::{debug, error, trace, Logger};

use crate::db::{model, query};
use crate::{types::Sample, Error, Field, FieldValue};

/// A `Client` to the ClickHouse metrics database.
#[derive(Debug)]
pub struct Client {
    address: SocketAddr,
    log: Logger,
    url: String,
    client: reqwest::Client,
    schema: Mutex<BTreeMap<String, model::TimeseriesSchema>>,
}

impl Client {
    /// Construct a new ClickHouse client of the database at `address`.
    pub async fn new(address: SocketAddr, log: Logger) -> Result<Self, Error> {
        let client = reqwest::Client::new();
        let url = format!("http://{}", address);
        let out = Self {
            address,
            log,
            url,
            client,
            schema: Mutex::new(BTreeMap::new()),
        };
        // TODO-robustness: We may want to remove this init_db call.
        //
        // The call will always succeed (assuming the DB can be reached), since the statements for
        // creating the database and tables have `IF NOT EXISTS` everywhere. It may be preferable
        // to remove this call and change the statements to _fail_ if the DB is already
        // initialized. This removes some of the "magic", and allows clients to know if the DB is
        // already populated or not. It also means we can connect and do stuff (such as wipe)
        // without first creating a bunch of data.
        //
        // For example, we really want to know if the DB is populated when we cold-start the rack,
        // as that would indicate a serious problem. This should probably trigger an obvious error,
        // rather than silently succeeding.
        out.init_db().await?;
        out.get_schema().await?;
        Ok(out)
    }

    /// Ping the ClickHouse server to verify connectivitiy.
    pub async fn ping(&self) -> Result<(), Error> {
        handle_db_response(
            self.client
                .get(format!("{}/ping", self.url))
                .send()
                .await
                .map_err(|err| Error::Database(err.to_string()))?,
        )
        .await?;
        debug!(self.log, "successful ping of ClickHouse server");
        Ok(())
    }

    /// Insert the given samples into the database.
    pub async fn insert_samples(
        &self,
        samples: &[Sample],
    ) -> Result<(), Error> {
        debug!(self.log, "unrolling {} total samples", samples.len());
        let mut seen_timeseries = BTreeSet::new();
        let mut rows = BTreeMap::new();
        let mut new_schema = Vec::new();

        for sample in samples.iter() {
            match self.verify_sample_schema(sample) {
                Err(_) => {
                    // Skip the sample, but otherwise do nothing. The error is logged in the above
                    // call.
                    continue;
                }
                Ok(schema) => {
                    if let Some(schema) = schema {
                        debug!(self.log, "new timeseries schema: {}", schema);
                        new_schema.push(schema);
                    }
                }
            }

            if !seen_timeseries.contains(sample.timeseries_key.as_str()) {
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

            seen_timeseries.insert(sample.timeseries_key.as_str());
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
                db_name = model::DATABASE_NAME,
                row_data = new_schema.join("\n")
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

    /// Search for timeseries matching the given criteria, building filters from the input.
    ///
    /// This method builds up the objects used to query the database from loosly-typed input,
    /// especially string field=value pairs for the timeseries fields.
    pub async fn filter_timeseries_with(
        &self,
        timeseries_name: &str,
        filters: &[query::Filter],
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Vec<model::Timeseries>, Error> {
        let schema = self
            .schema_for_timeseries(&timeseries_name)
            .await?
            .ok_or_else(|| {
                Error::QueryError(format!(
                    "No such timeseries: '{}'",
                    timeseries_name
                ))
            })?;

        // Convert the filters as strings to typed `FieldValue`s for each field in the schema.
        let mut fields = BTreeMap::new();
        for filter in filters.into_iter() {
            let ty = schema
                .fields
                .iter()
                .find(|f| f.name == filter.name)
                .ok_or_else(|| {
                    Error::QueryError(format!(
                        "No field '{}' for timeseries '{}'",
                        filter.name, timeseries_name
                    ))
                })?
                .ty;
            fields
                .entry(&filter.name)
                .or_insert_with(Vec::new)
                .push(FieldValue::parse_as_type(&filter.value, ty)?);
        }

        // Aggregate all filters on all fields
        let filters = fields
            .iter()
            .map(|(field_name, field_filters)| {
                query::FieldFilter::new(&field_name, &field_filters)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let time_filter = query::TimeFilter::from_timestamps(after, before)?;
        let filter = query::TimeseriesFilter {
            timeseries_name: timeseries_name.to_string(),
            filters,
            time_filter,
        };
        self.filter_timeseries(&filter).await
    }

    /// Search for samples from timeseries matching the given criteria.
    pub async fn filter_timeseries(
        &self,
        filter: &query::TimeseriesFilter,
    ) -> Result<Vec<model::Timeseries>, Error> {
        let schema =
            self.schema_for_timeseries(&filter.timeseries_name).await?.unwrap();
        let query = filter.as_select_query(schema.datum_type);
        let body = self.execute_with_body(query).await?;
        let mut timeseries_by_key = BTreeMap::new();
        for line in body.lines() {
            let (key, measurement) =
                model::parse_measurement_from_row(line, schema.datum_type);
            timeseries_by_key
                .entry(key)
                .or_insert_with(Vec::new)
                .push(measurement);
        }
        timeseries_by_key
            .into_iter()
            .map(|(timeseries_key, measurements)| {
                reconstitute_from_schema(&timeseries_key, &schema).map(
                    |(target, metric)| model::Timeseries {
                        timeseries_name: filter.timeseries_name.clone(),
                        timeseries_key: timeseries_key.to_string(),
                        target,
                        metric,
                        measurements,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Return the schema for a timeseries by name.
    ///
    /// Note
    /// ----
    /// This method may translate into a call to the database, if the requested metric cannot be
    /// found in an internal cache.
    pub async fn schema_for_timeseries<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Option<model::TimeseriesSchema>, Error> {
        {
            let map = self.schema.lock().unwrap();
            if let Some(s) = map.get(name.as_ref()) {
                return Ok(Some(s.clone()));
            }
        }
        // `get_schema` acquires the lock internally, so the above scope is required to avoid
        // deadlock.
        self.get_schema().await?;
        Ok(self.schema.lock().unwrap().get(name.as_ref()).map(Clone::clone))
    }

    // Verifies that the schema for a sample matches the schema in the database.
    //
    // If the schema does not match, an Err is returned (the caller skips the sample in this case).
    // If the schema does not _exist_ in the cached map of schema, its value as a row of JSON is
    // returned, so that the caller may insert them into the database at an appropriate time.
    fn verify_sample_schema(
        &self,
        sample: &Sample,
    ) -> Result<Option<String>, Error> {
        let schema = model::schema_for(sample);
        let maybe_new_schema = match self
            .schema
            .lock()
            .unwrap()
            .entry(schema.timeseries_name.clone())
        {
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
                .unwrap()
        }))
    }

    // Initialize ClickHouse with the database and metric table schema.
    pub(crate) async fn init_db(&self) -> Result<(), Error> {
        // The HTTP client doesn't support multiple statements per query, so we break them out here
        // manually.
        debug!(self.log, "initializing ClickHouse database");
        let sql = include_str!("./db-init.sql");
        for query in sql.split("\n--\n") {
            self.execute(query.to_string()).await?;
        }
        Ok(())
    }

    // Wipe the ClickHouse database entirely.
    pub async fn wipe_db(&self) -> Result<(), Error> {
        debug!(self.log, "wiping ClickHouse database");
        let sql = include_str!("./db-wipe.sql").to_string();
        self.execute(sql).await
    }

    // Execute a generic SQL statement.
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute(&self, sql: String) -> Result<(), Error> {
        self.execute_with_body(sql).await?;
        Ok(())
    }

    // Execute a generic SQL statement, awaiting the response as text
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute_with_body(&self, sql: String) -> Result<String, Error> {
        trace!(self.log, "executing SQL query: {}", sql);
        handle_db_response(
            self.client
                .post(&self.url)
                // See regression test `test_unquoted_64bit_integers` for details.
                .query(&[("output_format_json_quote_64bit_integers", "0")])
                .body(sql)
                .send()
                .await
                .map_err(|err| Error::Database(err.to_string()))?,
        )
        .await?
        .text()
        .await
        .map_err(|err| Error::Database(err.to_string()))
    }

    pub(crate) async fn get_schema(&self) -> Result<(), Error> {
        debug!(self.log, "retrieving timeseries schema from database");
        let sql = format!(
            "SELECT * FROM {}.timeseries_schema FORMAT JSONEachRow;",
            model::DATABASE_NAME,
        );
        let body = self.execute_with_body(sql).await?;
        if body.is_empty() {
            trace!(self.log, "no timeseries schema in database");
        } else {
            trace!(self.log, "extracting new timeseries schema");
            let new = body.lines().map(|line| {
                let schema = model::TimeseriesSchema::from(
                    serde_json::from_str::<model::DbTimeseriesSchema>(line)
                        .unwrap(),
                );
                (schema.timeseries_name.clone(), schema)
            });
            self.schema.lock().unwrap().extend(new);
        }
        Ok(())
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
    schema: &model::TimeseriesSchema,
    existing_schema: &model::TimeseriesSchema,
) -> Error {
    let expected = existing_schema
        .fields
        .iter()
        .map(|field| (field.name.clone(), field.ty))
        .collect();
    let actual = schema
        .fields
        .iter()
        .map(|field| (field.name.clone(), field.ty))
        .collect();
    Error::SchemaMismatch {
        name: schema.timeseries_name.clone(),
        expected,
        actual,
    }
}

// Reconstitute a target and metric struct from a timeseries key and its schema, if possible.
fn reconstitute_from_schema(
    timeseries_key: &str,
    schema: &model::TimeseriesSchema,
) -> Result<(model::Target, model::Metric), Error> {
    let (target_name, metric_name) =
        schema.timeseries_name.split_once(':').unwrap();
    let (target_fields, metric_fields): (Vec<_>, Vec<_>) = schema
        .fields
        .iter()
        .zip(timeseries_key.split(':'))
        .map(|(field, value_str)| {
            FieldValue::parse_as_type(value_str, field.ty)
                .map(|value| {
                    (field.source, Field { name: field.name.clone(), value })
                })
                .expect("Failed to parse field value from timeseries key part")
        })
        .partition(|(source, _)| source == &model::FieldSource::Target);
    let target = model::Target {
        name: target_name.to_string(),
        fields: target_fields.into_iter().map(|(_, field)| field).collect(),
    };
    let metric = model::Metric {
        name: metric_name.to_string(),
        fields: metric_fields.into_iter().map(|(_, field)| field).collect(),
        datum_type: schema.datum_type,
    };
    assert_eq!(
        target.fields.len() + metric.fields.len(),
        schema.fields.len(),
        "Missing a target or metric field in the timeseries key",
    );
    Ok((target, metric))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query;
    use crate::test_util;
    use crate::types::{DatumType, FieldType};
    use chrono::Utc;
    use omicron_test_utils::dev::clickhouse::ClickHouseInstance;
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

        Client::new(address, log).await.unwrap().wipe_db().await.unwrap();
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

        let client = Client::new(address, log).await.unwrap();
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
        #[derive(crate::Target)]
        pub struct TestTarget {
            pub name: String,
            pub name2: String,
            pub num: i64,
        }
    }

    mod type_mismatch {
        #[derive(crate::Target)]
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

        let client = Client::new(address, log).await.unwrap();
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
        let result = client.verify_sample_schema(&sample);
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

        let client = Client::new(address, log).await.unwrap();
        let sample = test_util::make_sample();

        // Verify that this sample is considered new, i.e., we return rows to update the timeseries
        // schema table.
        let result = client.verify_sample_schema(&sample).unwrap();
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
        let expected_schema = client
            .schema
            .lock()
            .unwrap()
            .get(&sample.timeseries_name)
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
        let result = client.verify_sample_schema(&sample).unwrap();
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
                model::TimeseriesSchema::from(
                    serde_json::from_str::<model::DbTimeseriesSchema>(&line)
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(schema.len(), 1);
        assert_eq!(expected_schema, schema[0]);

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    fn make_schema() -> (model::TimeseriesSchema, String) {
        let schema = model::TimeseriesSchema {
            timeseries_name: "some_target:some_metric".to_string(),
            fields: vec![
                model::Field {
                    name: String::from("target_field2"),
                    ty: FieldType::I64,
                    source: model::FieldSource::Target,
                },
                model::Field {
                    name: String::from("target_field1"),
                    ty: FieldType::I64,
                    source: model::FieldSource::Target,
                },
                model::Field {
                    name: String::from("metric_field1"),
                    ty: FieldType::I64,
                    source: model::FieldSource::Metric,
                },
                model::Field {
                    name: String::from("metric_field2"),
                    ty: FieldType::I64,
                    source: model::FieldSource::Metric,
                },
            ],
            datum_type: DatumType::F64,
            created: Utc::now(),
        };
        (schema, "0:1:2:3".to_string())
    }

    #[test]
    fn test_reconstitute_from_schema() {
        let (schema, timeseries_key) = make_schema();
        let (target, metric) =
            reconstitute_from_schema(&timeseries_key, &schema).unwrap();
        assert_eq!(target.name, "some_target");
        assert_eq!(metric.name, "some_metric");
        assert_eq!(
            target.fields.iter().collect::<Vec<_>>(),
            vec![
                &Field {
                    name: "target_field2".to_string(),
                    value: FieldValue::I64(0)
                },
                &Field {
                    name: "target_field1".to_string(),
                    value: FieldValue::I64(1)
                },
            ],
        );
        assert_eq!(
            metric.fields.iter().collect::<Vec<_>>(),
            vec![
                &Field {
                    name: "metric_field1".to_string(),
                    value: FieldValue::I64(2)
                },
                &Field {
                    name: "metric_field2".to_string(),
                    value: FieldValue::I64(3)
                },
            ],
        );
    }

    #[test]
    #[should_panic]
    fn test_reconstitute_from_schema_empty_key() {
        let (schema, _) = make_schema();
        let _ = reconstitute_from_schema("", &schema);
    }

    #[test]
    #[should_panic]
    fn test_reconstitute_from_schema_missing_field() {
        let (schema, _) = make_schema();
        let missing_field = "1:2:3";
        let _ = reconstitute_from_schema(missing_field, &schema);
    }

    async fn setup_filter_testcase() -> (ClickHouseInstance, Client, Vec<Sample>)
    {
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let db = ClickHouseInstance::new(0)
            .await
            .expect("Failed to start ClickHouse");
        let address = SocketAddr::new("::1".parse().unwrap(), db.port());

        let client = Client::new(address, log).await.unwrap();

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
    async fn test_client_filter_timeseries_one() {
        let (mut db, client, samples) = setup_filter_testcase().await;
        let sample = samples.first().unwrap();
        let filter = query::TimeseriesFilter {
            timeseries_name: String::from("virtual_machine:cpu_busy"),
            filters: vec![
                query::FieldFilter::new(
                    "project_id",
                    &[sample.target_fields()[0].value.clone()],
                )
                .unwrap(),
                query::FieldFilter::new(
                    "instance_id",
                    &[sample.target_fields()[1].value.clone()],
                )
                .unwrap(),
                query::FieldFilter::new(
                    "cpu_id",
                    &[sample.metric_fields()[0].value.clone()],
                )
                .unwrap(),
            ],
            time_filter: None,
        };
        let results = client.filter_timeseries(&filter).await.unwrap();
        assert_eq!(results.len(), 1, "Expected to find a single timeseries");
        let timeseries = &results[0];
        assert_eq!(
            timeseries.measurements.len(),
            2,
            "Expected 2 samples per timeseries"
        );
        assert_eq!(timeseries.timeseries_key, sample.timeseries_key);

        // Compare measurements themselves
        let expected_measurements =
            samples.iter().map(|sample| &sample.measurement);
        let actual_measurements = timeseries.measurements.iter();
        assert!(actual_measurements
            .zip(expected_measurements)
            .all(|(first, second)| first == second));
        assert_eq!(timeseries.target.name, "virtual_machine");
        assert_eq!(&timeseries.target.fields, sample.target_fields());
        assert_eq!(timeseries.metric.name, "cpu_busy");
        assert_eq!(&timeseries.metric.fields, sample.metric_fields());
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
}
