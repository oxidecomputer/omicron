//! Rust client to ClickHouse database
// Copyright 2021 Oxide Computer Company

use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::sync::Mutex;

use slog::{debug, error, trace, Logger};

use crate::db::model;
use crate::{types::Sample, Error};

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
                        trace!(self.log, "new timeseries schema: {}", schema);
                        new_schema.push(schema);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_util;
    use omicron_common::dev;
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
        let mut db = dev::clickhouse::ClickHouseInstance::new(0)
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
        let mut db = dev::clickhouse::ClickHouseInstance::new(0)
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

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = dev::clickhouse::ClickHouseInstance::new(0)
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
            value: 1,
        };
        let sample = Sample::new(&bad_name, &metric, None);
        let result = client.verify_sample_schema(&sample);
        assert!(matches!(result, Err(Error::SchemaMismatch { .. })));
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
    }

    #[tokio::test]
    async fn test_schema_update() {
        let log = slog::Logger::root(slog::Discard, o!());

        // Let the OS assign a port and discover it after ClickHouse starts
        let mut db = dev::clickhouse::ClickHouseInstance::new(0)
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
            .get(&sample.timeseries_name())
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
}
