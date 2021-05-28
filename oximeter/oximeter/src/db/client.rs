//! Rust client to ClickHouse database
// Copyright 2021 Oxide Computer Company

use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use slog::{debug, error, trace, Logger};

use crate::db::model;
use crate::{types::Sample, Error, FieldType, FieldValue};

/// A `Client` to the ClickHouse metrics database.
#[derive(Debug, Clone)]
pub struct Client {
    address: SocketAddr,
    log: Logger,
    url: String,
    client: reqwest::Client,
    target_schema: Arc<Mutex<BTreeMap<String, model::TargetSchema>>>,
    metric_schema: Arc<Mutex<BTreeMap<String, model::MetricSchema>>>,
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
            target_schema: Arc::new(Mutex::new(BTreeMap::new())),
            metric_schema: Arc::new(Mutex::new(BTreeMap::new())),
        };
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
        trace!(self.log, "unrolling {} total samples", samples.len());
        let mut rows = BTreeMap::new();
        let mut new_target_schema = Vec::new();
        let mut new_metric_schema = Vec::new();
        for sample in samples.iter() {
            match self.verify_sample_schema(sample) {
                Err(_) => {
                    // Skip the sample, but otherwise do nothing. The error is logged in the above
                    // call.
                    continue;
                }
                Ok((new_target, new_metric)) => {
                    if let Some(target) = new_target {
                        trace!(self.log, "new target schema: {}", target);
                        new_target_schema.push(target);
                    }
                    if let Some(metric) = new_metric {
                        trace!(self.log, "new metric schema: {}", metric);
                        new_metric_schema.push(metric);
                    }
                }
            }
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
        if !new_target_schema.is_empty() {
            debug!(
                self.log,
                "inserting {} new target schema",
                new_target_schema.len()
            );
            let body = format!(
                "INSERT INTO {db_name}.target_schema FORMAT JSONEachRow\n{row_data}\n",
                db_name = model::DATABASE_NAME,
                row_data = new_target_schema.join("\n")
            );
            self.execute(body).await?;
        }
        if !new_metric_schema.is_empty() {
            debug!(
                self.log,
                "inserting {} new metric schema",
                new_metric_schema.len()
            );
            let body = format!(
                "INSERT INTO {db_name}.metric_schema FORMAT JSONEachRow\n{row_data}\n",
                db_name = model::DATABASE_NAME,
                row_data = new_metric_schema.join("\n")
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
            trace!(
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

    // Verifies that the schema for the target and metric in the sample match the database.
    //
    // If either schema does not match, an Err is returned (the caller skips the sample in this
    // case). If either schema does not _exist_ in the cached map of schema, its value as a row of
    // JSON is returned, so that the caller may insert them into the database at an appropriate
    // time.
    fn verify_sample_schema(
        &self,
        sample: &Sample,
    ) -> Result<(Option<String>, Option<String>), Error> {
        let (target_schema, metric_schema) = model::schema_for(sample);

        // Verify the target schema
        let target_schema = match self
            .target_schema
            .lock()
            .unwrap()
            .entry(target_schema.target_name.clone())
        {
            Entry::Vacant(entry) => Some(
                serde_json::to_string(&*entry.insert(target_schema)).unwrap(),
            ),
            Entry::Occupied(entry) => {
                let existing_schema = entry.get();
                if existing_schema != &target_schema {
                    let err = error_for_schema_mismatch(
                        &target_schema.target_name,
                        &existing_schema.field_names,
                        &existing_schema.field_types,
                        &sample.target.fields,
                    );
                    error!(
                        self.log,
                        "target schema mismatch, sample will be skipped: {}",
                        err
                    );
                    return Err(err);
                }
                None
            }
        };

        // Verify the metric schema
        let metric_schema = match self
            .metric_schema
            .lock()
            .unwrap()
            .entry(metric_schema.metric_name.clone())
        {
            Entry::Vacant(entry) => Some(
                serde_json::to_string(&*entry.insert(metric_schema)).unwrap(),
            ),
            Entry::Occupied(entry) => {
                let existing_schema = entry.get();
                if existing_schema != &metric_schema {
                    let err = error_for_schema_mismatch(
                        &metric_schema.metric_name,
                        &existing_schema.field_names,
                        &existing_schema.field_types,
                        &sample.metric.fields,
                    );
                    error!(
                        self.log,
                        "metric schema mismatch, sample will be skipped: {}",
                        err
                    );
                    return Err(err);
                }
                None
            }
        };

        Ok((target_schema, metric_schema))
    }

    // Initialize ClickHouse with the database and metric table schema.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub(crate) async fn wipe_db(&self) -> Result<(), Error> {
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
        debug!(self.log, "retrieving database schema");

        // Target schema
        let sql = format!(
            "SELECT * FROM {}.target_schema FORMAT JSONEachRow;",
            model::DATABASE_NAME,
        );
        let body = self.execute_with_body(sql).await?;
        if body.is_empty() {
            trace!(self.log, "no target schema in database");
        } else {
            trace!(self.log, "extracting new target schema");
            let new = body.lines().map(|line| {
                let schema =
                    serde_json::from_str::<model::TargetSchema>(line).unwrap();
                (schema.target_name.clone(), schema)
            });
            self.target_schema.lock().unwrap().extend(new);
        }

        // Metric schema
        let sql = format!(
            "SELECT * FROM {}.metric_schema FORMAT JSONEachRow;",
            model::DATABASE_NAME,
        );
        let body = self.execute_with_body(sql).await?;
        if body.is_empty() {
            trace!(self.log, "no metric schema in database");
        } else {
            trace!(self.log, "extracting new metric schema");
            let new = body.lines().map(|line| {
                let schema =
                    serde_json::from_str::<model::MetricSchema>(line).unwrap();
                (schema.metric_name.clone(), schema)
            });
            self.metric_schema.lock().unwrap().extend(new);
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
    name: &str,
    existing_names: &[String],
    existing_types: &[FieldType],
    new_fields: &BTreeMap<String, FieldValue>,
) -> Error {
    let expected = existing_names
        .iter()
        .cloned()
        .zip(existing_types.iter().cloned())
        .collect();
    let actual = new_fields
        .iter()
        .map(|(name, value)| (name.clone(), value.field_type()))
        .collect();
    Error::SchemaMismatch { name: name.to_string(), expected, actual }
}

// This set of tests is behind a configuration flag that must be explicitly enabled. These test
// interaction with the database, and each test requires a clean slate. This requires at least
// running everything with a single test thread. In addition, ClickHouse provides pretty weak
// consistency, which means tests run quickly one after the other may still show some hysteresis.
// This is possibly a problem for us more generally, but at least it requires these tests to be run
// in a single thread.
//
// TODO-completeness We would like to run these tests on any platform currently tested in CI, which
// includes macOS. ClickHouse provides pre-built macOS images, but it looks like that always pulls
// from the `master` branch, with no obvious way to specify a commit. For now, just build on Linux,
// with ClickHouse installed via the package manager. See `tools/ci_download_clickhouse` for
// details on the installation procedure.
#[cfg(all(test, target_os = "linux", feature = "clickhouse-tests"))]
mod tests {
    use super::*;
    use crate::db::test_util;
    use slog::o;

    #[tokio::test]
    async fn test_build_client() {
        let log = slog::Logger::root(slog::Discard, o!());
        Client::new("[::1]:8123".parse().unwrap(), log)
            .await
            .unwrap()
            .wipe_db()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_client_insert() {
        let log = slog::Logger::root(slog::Discard, o!());
        let client =
            Client::new("[::1]:8123".parse().unwrap(), log).await.unwrap();
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

    // This is a target with the same name as that in `db/mod.rs` used for other tests, but with a
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
        let client =
            Client::new("[::1]:8123".parse().unwrap(), log).await.unwrap();
        client.wipe_db().await.unwrap();
        client.init_db().await.unwrap();
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
            value: 1,
        };
        let sample = Sample::new(&bad_name, &metric, None);
        let result = client.verify_sample_schema(&sample);
        assert!(matches!(result, Err(Error::SchemaMismatch { .. })));
        client.wipe_db().await.unwrap();
    }

    #[tokio::test]
    async fn test_schema_update() {
        let log = slog::Logger::root(slog::Discard, o!());
        let client =
            Client::new("[::1]:8123".parse().unwrap(), log).await.unwrap();
        client.wipe_db().await.unwrap();
        client.init_db().await.unwrap();
        let sample = test_util::make_sample();

        // Verify that this sample is considered new, i.e., we return rows to update the target and
        // metric schema tables.
        let result = client.verify_sample_schema(&sample).unwrap();
        assert!(
            matches!(result, (Some(_), Some(_))),
            "When verifying a new sample, the rows to be inserted should be returned"
        );

        // Clear the internal maps, so that the below call updates them and inserts into the DB.
        // This is the peril of testing a non-public API.
        client.target_schema.lock().unwrap().clear();
        client.metric_schema.lock().unwrap().clear();

        // Insert the new sample
        client.insert_samples(&[sample.clone()]).await.unwrap();

        // The internal maps should now contain both the target and metric schema
        let expected_target_schema = client
            .target_schema
            .lock()
            .unwrap()
            .get(&sample.target.name)
            .expect(
                "After inserting a new sample, its schema should be included",
            )
            .clone();
        assert_eq!(
            expected_target_schema,
            serde_json::from_str(&result.0.unwrap()).unwrap(),
            "The target schema for a new sample was not correctly inserted into internal cache",
        );
        let expected_metric_schema = client
            .metric_schema
            .lock()
            .unwrap()
            .get(&sample.metric.name)
            .expect(
                "After inserting a new sample, its schema should be included",
            )
            .clone();
        assert_eq!(
            expected_metric_schema,
            serde_json::from_str(&result.1.unwrap()).unwrap(),
            "The metric schema for a new sample was not correctly inserted into internal cache",
        );

        // This should no longer return a new row to be inserted for the schema, as they've been
        // included above
        let result = client.verify_sample_schema(&sample).unwrap();
        assert!(
            matches!(result, (None, None)),
            "After inserting new schema, they should no longer be considered new"
        );

        // Verify that they're actually in the database!
        let sql = String::from(
            "SELECT * FROM oximeter.target_schema FORMAT JSONEachRow;",
        );
        let result = client.execute_with_body(sql).await.unwrap();
        let schema = result
            .lines()
            .map(|line| serde_json::from_str(&line).unwrap())
            .collect::<Vec<model::TargetSchema>>();
        println!("{:#?}", schema);
        assert_eq!(schema.len(), 1);
        assert_eq!(expected_target_schema, schema[0]);

        let sql = String::from(
            "SELECT * FROM oximeter.metric_schema FORMAT JSONEachRow;",
        );
        let result = client.execute_with_body(sql).await.unwrap();
        let schema = result
            .lines()
            .map(|line| serde_json::from_str(&line).unwrap())
            .collect::<Vec<model::MetricSchema>>();
        assert_eq!(schema.len(), 1);
        assert_eq!(expected_metric_schema, schema[0]);

        client.wipe_db().await.unwrap();
    }
}
