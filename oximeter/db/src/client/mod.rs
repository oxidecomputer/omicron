// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rust client to ClickHouse database

// Copyright 2024 Oxide Computer Company

pub(crate) mod dbwrite;
#[cfg(any(feature = "oxql", test))]
pub(crate) mod oxql;
pub(crate) mod query_summary;
#[cfg(any(feature = "sql", test))]
mod sql;

pub use self::dbwrite::DbWrite;
pub use self::dbwrite::TestDbWrite;
use crate::client::query_summary::QuerySummary;
use crate::model;
use crate::query;
use crate::Error;
use crate::Metric;
use crate::Target;
use crate::Timeseries;
use crate::TimeseriesPageSelector;
use crate::TimeseriesScanParams;
use crate::TimeseriesSchema;
use dropshot::EmptyScanParams;
use dropshot::PaginationOrder;
use dropshot::ResultsPage;
use dropshot::WhichPage;
use omicron_common::backoff;
use oximeter::schema::TimeseriesKey;
use oximeter::types::Sample;
use oximeter::TimeseriesName;
use regex::Regex;
use regex::RegexBuilder;
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
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use tokio::fs;
use tokio::sync::Mutex;
use uuid::Uuid;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CLICKHOUSE_DB_MISSING: &'static str = "Database oximeter does not exist";
const CLICKHOUSE_DB_VERSION_MISSING: &'static str =
    "Table oximeter.version does not exist";

#[usdt::provider(provider = "clickhouse_client")]
mod probes {
    /// Fires when a SQL query begins, with the query string.
    fn sql__query__start(_: &usdt::UniqueId, sql: &str) {}

    /// Fires when a SQL query ends, either in success or failure.
    fn sql__query__done(_: &usdt::UniqueId) {}
}

/// A `Client` to the ClickHouse metrics database.
#[derive(Debug)]
pub struct Client {
    _id: Uuid,
    log: Logger,
    url: String,
    client: reqwest::Client,
    schema: Mutex<BTreeMap<TimeseriesName, TimeseriesSchema>>,
    request_timeout: Duration,
}

impl Client {
    /// Construct a new ClickHouse client of the database at `address`.
    pub fn new(address: SocketAddr, log: &Logger) -> Self {
        Self::new_with_request_timeout(address, log, DEFAULT_REQUEST_TIMEOUT)
    }

    /// Construct a new ClickHouse client of the database at `address`, and a
    /// custom request timeout.
    pub fn new_with_request_timeout(
        address: SocketAddr,
        log: &Logger,
        request_timeout: Duration,
    ) -> Self {
        let id = Uuid::new_v4();
        let log = log.new(slog::o!(
            "component" => "clickhouse-client",
            "id" => id.to_string(),
        ));
        let client = reqwest::Client::new();
        let url = format!("http://{}", address);
        let schema = Mutex::new(BTreeMap::new());
        Self { _id: id, log, url, client, schema, request_timeout }
    }

    /// Return the url the client is trying to connect to
    pub fn url(&self) -> &str {
        &self.url
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
                    .1
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

    /// Return a page of timeseries schema from the database.
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
                    .1
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
                        "ORDER BY timeseries_name ",
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
                        "ORDER BY timeseries_name ",
                        "LIMIT {} ",
                        "FORMAT JSONEachRow;",
                    ),
                    crate::DATABASE_NAME,
                    last_timeseries,
                    limit.get(),
                )
            }
        };
        let body = self.execute_with_body(sql).await?.1;
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
        .map_err(|err| Error::Database(err.to_string()))
    }

    /// Read the available schema versions in the provided directory.
    pub async fn read_available_schema_versions(
        log: &Logger,
        is_replicated: bool,
        schema_dir: impl AsRef<Path>,
    ) -> Result<BTreeSet<u64>, Error> {
        let dir = schema_dir.as_ref().join(if is_replicated {
            "replicated"
        } else {
            "single-node"
        });
        let mut rd =
            fs::read_dir(&dir).await.map_err(|err| Error::ReadSchemaDir {
                context: format!(
                    "Failed to read schema directory '{}'",
                    dir.display()
                ),
                err,
            })?;
        let mut versions = BTreeSet::new();
        debug!(log, "reading entries from schema dir"; "dir" => dir.display());
        while let Some(entry) =
            rd.next_entry().await.map_err(|err| Error::ReadSchemaDir {
                context: String::from("Failed to read directory entry"),
                err,
            })?
        {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|bad| Error::NonUtf8SchemaDirEntry(bad.to_owned()))?;
            let md =
                entry.metadata().await.map_err(|err| Error::ReadSchemaDir {
                    context: String::from("Failed to fetch entry metatdata"),
                    err,
                })?;
            if !md.is_dir() {
                debug!(log, "skipping non-directory"; "name" => &name);
                continue;
            }
            match name.parse() {
                Ok(ver) => {
                    debug!(log, "valid version dir"; "ver" => ver);
                    assert!(versions.insert(ver), "Versions should be unique");
                }
                Err(e) => warn!(
                    log,
                    "found directory with non-u64 name, skipping";
                    "name" => name,
                    "error" => ?e,
                ),
            }
        }
        Ok(versions)
    }

    /// Ensure that the database is upgraded to the desired version of the
    /// schema.
    ///
    /// NOTE: This function is not safe for concurrent usage!
    pub async fn ensure_schema(
        &self,
        replicated: bool,
        desired_version: u64,
        schema_dir: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let schema_dir = schema_dir.as_ref();
        let latest = self.read_latest_version().await?;
        if latest == desired_version {
            debug!(
                self.log,
                "database already at desired version";
                "version" => latest,
            );
            return Ok(());
        }
        debug!(
            self.log,
            "starting upgrade to desired version {}", desired_version
        );
        let available = Self::read_available_schema_versions(
            &self.log, replicated, schema_dir,
        )
        .await?;
        // We explicitly ignore version 0, which implies the database doesn't
        // exist at all.
        if latest > 0 && !available.contains(&latest) {
            return Err(Error::MissingSchemaVersion(latest));
        }
        if !available.contains(&desired_version) {
            return Err(Error::MissingSchemaVersion(desired_version));
        }

        // Check we have no gaps in version numbers, starting with the latest
        // version and walking through all available ones strictly greater. This
        // is to check that the _next_ version is also 1 greater than the
        // latest.
        let range = (Bound::Excluded(latest), Bound::Included(desired_version));
        if available
            .range(latest..)
            .zip(available.range(range))
            .any(|(current, next)| next - current != 1)
        {
            return Err(Error::NonSequentialSchemaVersions);
        }

        // Walk through all changes between current version (exclusive) and
        // the desired version (inclusive).
        let versions_to_apply = available.range(range);
        let mut current = latest;
        for version in versions_to_apply {
            if let Err(e) = self
                .apply_one_schema_upgrade(replicated, *version, schema_dir)
                .await
            {
                error!(
                    self.log,
                    "failed to apply schema upgrade";
                    "current_version" => current,
                    "next_version" => *version,
                    "replicated" => replicated,
                    "schema_dir" => schema_dir.display(),
                    "error" => ?e,
                );
                return Err(e);
            }
            current = *version;
            self.insert_version(current).await?;
        }
        Ok(())
    }

    fn verify_schema_upgrades(
        files: &BTreeMap<String, (PathBuf, String)>,
    ) -> Result<(), Error> {
        let re = schema_validation_regex();
        for (path, sql) in files.values() {
            if re.is_match(&sql) {
                return Err(Error::SchemaUpdateModifiesData {
                    path: path.clone(),
                    statement: sql.clone(),
                });
            }
            if sql.matches(';').count() > 1 {
                return Err(Error::MultipleSqlStatementsInSchemaUpdate {
                    path: path.clone(),
                });
            }
        }
        Ok(())
    }

    async fn apply_one_schema_upgrade(
        &self,
        replicated: bool,
        next_version: u64,
        schema_dir: impl AsRef<Path>,
    ) -> Result<(), Error> {
        let schema_dir = schema_dir.as_ref();
        let upgrade_file_contents = Self::read_schema_upgrade_sql_files(
            &self.log,
            replicated,
            next_version,
            schema_dir,
        )
        .await?;

        // We need to be pretty careful at this point with any data-modifying
        // statements. There should be no INSERT queries, for example, which we
        // check here. ClickHouse doesn't support much in the way of data
        // modification, which makes this pretty easy.
        Self::verify_schema_upgrades(&upgrade_file_contents)?;

        // Apply each file in sequence in the upgrade directory.
        for (name, (path, sql)) in upgrade_file_contents.into_iter() {
            debug!(
                self.log,
                "apply schema upgrade file";
                "version" => next_version,
                "path" => path.display(),
                "filename" => &name,
            );
            match self.execute(sql).await {
                Ok(_) => debug!(
                    self.log,
                    "successfully applied schema upgrade file";
                    "version" => next_version,
                    "path" => path.display(),
                    "name" => name,
                ),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        // Check if we have a list of timeseries that should be deleted, and
        // remove them from the history books.
        let to_delete = Self::read_timeseries_to_delete(
            replicated,
            next_version,
            schema_dir,
        )
        .await?;
        if to_delete.is_empty() {
            debug!(
                self.log,
                "schema upgrade contained timeseries list file, \
                but it did not contain any timeseries names",
            );
        } else {
            debug!(
                self.log,
                "schema upgrade includes list of timeseries to be deleted";
                "n_timeseries" => to_delete.len(),
            );
            self.expunge_timeseries_by_name(replicated, &to_delete).await?;
        }
        Ok(())
    }

    fn full_upgrade_path(
        replicated: bool,
        version: u64,
        schema_dir: impl AsRef<Path>,
    ) -> PathBuf {
        schema_dir
            .as_ref()
            .join(if replicated { "replicated" } else { "single-node" })
            .join(version.to_string())
    }

    // Read all SQL files, in order, in the schema directory for the provided
    // version.
    async fn read_schema_upgrade_sql_files(
        log: &Logger,
        replicated: bool,
        version: u64,
        schema_dir: impl AsRef<Path>,
    ) -> Result<BTreeMap<String, (PathBuf, String)>, Error> {
        let version_schema_dir =
            Self::full_upgrade_path(replicated, version, schema_dir.as_ref());
        let mut rd =
            fs::read_dir(&version_schema_dir).await.map_err(|err| {
                Error::ReadSchemaDir {
                    context: format!(
                        "Failed to read schema directory '{}'",
                        version_schema_dir.display()
                    ),
                    err,
                }
            })?;

        let mut upgrade_files = BTreeMap::new();
        debug!(log, "reading SQL files from schema dir"; "dir" => version_schema_dir.display());
        while let Some(entry) =
            rd.next_entry().await.map_err(|err| Error::ReadSchemaDir {
                context: String::from("Failed to read directory entry"),
                err,
            })?
        {
            let path = entry.path();
            let Some(ext) = path.extension() else {
                warn!(
                    log,
                    "skipping schema dir entry without an extension";
                    "dir" => version_schema_dir.display(),
                    "path" => path.display(),
                );
                continue;
            };
            let Some(ext) = ext.to_str() else {
                warn!(
                    log,
                    "skipping schema dir entry with non-UTF8 extension";
                    "dir" => version_schema_dir.display(),
                    "path" => path.display(),
                );
                continue;
            };
            if ext.eq_ignore_ascii_case("sql") {
                let Some(stem) = path.file_stem() else {
                    warn!(
                        log,
                        "skipping schema SQL file with no name";
                        "dir" => version_schema_dir.display(),
                        "path" => path.display(),
                    );
                    continue;
                };
                let Some(name) = stem.to_str() else {
                    warn!(
                        log,
                        "skipping schema SQL file with non-UTF8 name";
                        "dir" => version_schema_dir.display(),
                        "path" => path.display(),
                    );
                    continue;
                };
                let contents =
                    fs::read_to_string(&path).await.map_err(|err| {
                        Error::ReadSqlFile {
                            context: format!(
                                "Reading SQL file '{}' for upgrade",
                                path.display(),
                            ),
                            err,
                        }
                    })?;
                upgrade_files
                    .insert(name.to_string(), (path.to_owned(), contents));
            } else {
                // Warn on unexpected files, _except_ the
                // timeseries-to-delete.txt files we use to expunge timeseries.
                if path
                    .file_name()
                    .map(|name| name == crate::TIMESERIES_TO_DELETE_FILE)
                    .unwrap_or(true)
                {
                    warn!(
                        log,
                        "skipping non-SQL schema dir entry";
                        "dir" => version_schema_dir.display(),
                        "path" => path.display(),
                    );
                }
                continue;
            }
        }
        Ok(upgrade_files)
    }

    /// Validates that the schema used by the DB matches the version used by
    /// the executable using it.
    ///
    /// This function will **wipe** metrics data if the version stored within
    /// the DB is less than the schema version of Oximeter.
    /// If the version in the DB is newer than what is known to Oximeter, an
    /// error is returned.
    ///
    /// If you would like to non-destructively upgrade the database, then either
    /// the included binary `clickhouse-schema-updater` or the method
    /// [`Client::ensure_schema()`] should be used instead.
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
            return Err(Error::DatabaseVersionMismatch {
                expected: crate::model::OXIMETER_VERSION,
                found: version,
            });
        } else {
            // If the version matches, we don't need to update the DB
            return Ok(());
        }

        info!(self.log, "inserting current version"; "version" => expected_version);
        self.insert_version(expected_version).await?;
        Ok(())
    }

    /// Read the latest version applied in the database.
    pub async fn read_latest_version(&self) -> Result<u64, Error> {
        let sql = format!(
            "SELECT MAX(value) FROM {db_name}.version;",
            db_name = crate::DATABASE_NAME,
        );

        let version = match self.execute_with_body(sql).await {
            Ok((_, body)) if body.is_empty() => {
                warn!(
                    self.log,
                    "no version in database (treated as 'version 0')"
                );
                0
            }
            Ok((_, body)) => body.trim().parse::<u64>().map_err(|err| {
                Error::Database(format!("Cannot read version: {err}"))
            })?,
            Err(Error::Database(err))
                // Case 1: The database has not been created.
                if err.contains(CLICKHOUSE_DB_MISSING) ||
                // Case 2: The database has been created, but it's old (exists
                // prior to the version table).
                    err.contains(CLICKHOUSE_DB_VERSION_MISSING) =>
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

    /// Return Ok if the DB is at exactly the version compatible with this
    /// client.
    pub async fn check_db_is_at_expected_version(&self) -> Result<(), Error> {
        let ver = self.read_latest_version().await?;
        if ver == crate::model::OXIMETER_VERSION {
            Ok(())
        } else {
            Err(Error::DatabaseVersionMismatch {
                expected: crate::model::OXIMETER_VERSION,
                found: ver,
            })
        }
    }

    async fn insert_version(&self, version: u64) -> Result<(), Error> {
        let sql = format!(
            "INSERT INTO {db_name}.version (*) VALUES ({version}, now());",
            db_name = crate::DATABASE_NAME,
        );
        self.execute(sql).await
    }

    /// Verifies if instance is part of oximeter_cluster
    pub async fn is_oximeter_cluster(&self) -> Result<bool, Error> {
        let sql = "SHOW CLUSTERS FORMAT JSONEachRow;";
        let res = self.execute_with_body(sql).await?.1;
        Ok(res.contains("oximeter_cluster"))
    }

    // Verifies that the schema for a sample matches the schema in the database,
    // or cache a new one internally.
    //
    // If the schema exists in the database, and the sample matches that schema, `None` is
    // returned. If the schema does not match, an Err is returned (the caller skips the sample in
    // this case). If the schema does not _exist_ in the database,
    // Some((timeseries_name, schema)) is returned, so that the caller can
    // insert it into the database at the appropriate time. Note that the schema
    // is added to the internal cache, but not inserted into the DB at this
    // time.
    async fn verify_or_cache_sample_schema(
        &self,
        sample: &Sample,
    ) -> Result<Option<(TimeseriesName, String)>, Error> {
        let sample_schema = TimeseriesSchema::from(sample);
        let name = sample_schema.timeseries_name.clone();
        let mut schema = self.schema.lock().await;

        // We've taken the lock before we do any checks for schema. First, we
        // check if we've already got one in the cache. If not, we update all
        // the schema from the database, and then check the map again. If we
        // find a schema (which now either came from the cache or the latest
        // read of the DB), then we check that the derived schema matches. If
        // not, we can insert it in the cache and the DB.
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
                let name = entry.key().clone();
                entry.insert(sample_schema.clone());
                Ok(Some((
                    name,
                    serde_json::to_string(&model::DbTimeseriesSchema::from(
                        sample_schema,
                    ))
                    .expect("Failed to convert schema to DB model"),
                )))
            }
        }
    }

    // Select the timeseries, including keys and field values, that match the given field-selection
    // query.
    async fn select_matching_timeseries_info(
        &self,
        field_query: &str,
        schema: &TimeseriesSchema,
    ) -> Result<(QuerySummary, BTreeMap<TimeseriesKey, (Target, Metric)>), Error>
    {
        let (summary, body) = self.execute_with_body(field_query).await?;
        let mut results = BTreeMap::new();
        for line in body.lines() {
            let row: model::FieldSelectRow = serde_json::from_str(line)
                .expect("Unable to deserialize an expected row");
            let (id, target, metric) =
                model::parse_field_select_row(&row, schema);
            results.insert(id, (target, metric));
        }
        Ok((summary, results))
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
        for line in self.execute_with_body(&measurement_query).await?.1.lines()
        {
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

    // Execute a generic SQL statement.
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute<S>(&self, sql: S) -> Result<(), Error>
    where
        S: Into<String>,
    {
        self.execute_with_body(sql).await?;
        Ok(())
    }

    // Execute a generic SQL statement, awaiting the response as text
    //
    // TODO-robustness This currently does no validation of the statement.
    async fn execute_with_body<S>(
        &self,
        sql: S,
    ) -> Result<(QuerySummary, String), Error>
    where
        S: Into<String>,
    {
        let sql = sql.into();
        trace!(
            self.log,
            "executing SQL query";
            "sql" => &sql,
        );

        // Run the SQL query itself.
        //
        // This code gets a bit convoluted, so that we can fire the USDT probe
        // in all situations, even when the various fallible operations
        // complete.
        let id = usdt::UniqueId::new();
        probes::sql__query__start!(|| (&id, &sql));
        let start = Instant::now();

        // Submit the SQL request itself.
        let response = self
            .client
            .post(&self.url)
            .timeout(self.request_timeout)
            .query(&[
                ("output_format_json_quote_64bit_integers", "0"),
                // TODO-performance: This is needed to get the correct counts of
                // rows/bytes accessed during a query, but implies larger memory
                // consumption on the server and higher latency for the request.
                // We may want to sacrifice accuracy of those counts.
                ("wait_end_of_query", "1"),
            ])
            .body(sql)
            .send()
            .await
            .map_err(|err| {
                probes::sql__query__done!(|| (&id));
                Error::DatabaseUnavailable(err.to_string())
            })?;

        // Convert the HTTP response into a database response.
        let response = handle_db_response(response).await.map_err(|err| {
            probes::sql__query__done!(|| (&id));
            err
        })?;

        // Extract the query summary, measuring resource usage and duration.
        let summary =
            QuerySummary::from_headers(start.elapsed(), response.headers())
                .map_err(|err| {
                    probes::sql__query__done!(|| (&id));
                    err
                })?;

        // Extract the actual text of the response.
        let text = response.text().await.map_err(|err| {
            probes::sql__query__done!(|| (&id));
            Error::Database(err.to_string())
        })?;
        probes::sql__query__done!(|| (&id));
        Ok((summary, text))
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
        let body = self.execute_with_body(sql).await?.1;
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

    /// Given a list of timeseries by name, delete their schema and any
    /// associated data records from all tables.
    ///
    /// If the database isn't available or the request times out, this method
    /// will continue to retry the operation until it succeeds.
    async fn expunge_timeseries_by_name(
        &self,
        replicated: bool,
        to_delete: &[TimeseriesName],
    ) -> Result<(), Error> {
        let op = || async {
            self.expunge_timeseries_by_name_once(replicated, to_delete)
                .await
                .map_err(|err| match err {
                    Error::DatabaseUnavailable(_) => {
                        backoff::BackoffError::transient(err)
                    }
                    _ => backoff::BackoffError::permanent(err),
                })
        };
        let notify = |error, count, delay| {
            warn!(
                self.log,
                "failed to delete some timeseries";
                "error" => ?error,
                "call_count" => count,
                "retry_after" => ?delay,
            );
        };
        backoff::retry_notify_ext(
            backoff::retry_policy_internal_service(),
            op,
            notify,
        )
        .await
    }

    /// Attempt to delete the named timeseries once.
    async fn expunge_timeseries_by_name_once(
        &self,
        replicated: bool,
        to_delete: &[TimeseriesName],
    ) -> Result<(), Error> {
        // The version table should not have any matching data, but let's avoid
        // it entirely anyway.
        let tables = self
            .list_oximeter_database_tables(ListDetails {
                include_version: false,
                replicated,
            })
            .await?;

        // This size is arbitrary, and just something to avoid enormous requests
        // to ClickHouse. It's unlikely that we'll hit this in practice anyway,
        // given that we have far fewer than 1000 timeseries today.
        const DELETE_BATCH_SIZE: usize = 1000;
        let maybe_on_cluster = if replicated {
            format!("ON CLUSTER {}", crate::CLUSTER_NAME)
        } else {
            String::new()
        };
        for chunk in to_delete.chunks(DELETE_BATCH_SIZE) {
            let names = chunk
                .iter()
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(",");
            debug!(
                self.log,
                "deleting chunk of timeseries";
                "timeseries_names" => &names,
                "n_timeseries" => chunk.len(),
            );
            for table in tables.iter() {
                let sql = format!(
                    "ALTER TABLE {}.{} \
                    {} \
                    DELETE WHERE timeseries_name in ({})",
                    crate::DATABASE_NAME,
                    table,
                    maybe_on_cluster,
                    names,
                );
                debug!(
                    self.log,
                    "deleting timeseries from next table";
                    "table_name" => table,
                    "n_timeseries" => chunk.len(),
                );
                self.execute(sql).await?;
            }
        }
        Ok(())
    }

    async fn read_timeseries_to_delete(
        replicated: bool,
        next_version: u64,
        schema_dir: &Path,
    ) -> Result<Vec<TimeseriesName>, Error> {
        let version_schema_dir =
            Self::full_upgrade_path(replicated, next_version, schema_dir);
        let filename =
            version_schema_dir.join(crate::TIMESERIES_TO_DELETE_FILE);
        match fs::read_to_string(&filename).await {
            Ok(contents) => contents
                .lines()
                .map(|line| line.trim().parse().map_err(Error::from))
                .collect(),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(vec![]),
            Err(err) => Err(Error::ReadTimeseriesToDeleteFile { err }),
        }
    }

    /// Useful for testing and introspection
    pub async fn list_replicated_tables(&self) -> Result<Vec<String>, Error> {
        self.list_oximeter_database_tables(ListDetails {
            include_version: true,
            replicated: true,
        })
        .await
    }

    /// List tables in the oximeter database.
    async fn list_oximeter_database_tables(
        &self,
        ListDetails { include_version, replicated }: ListDetails,
    ) -> Result<Vec<String>, Error> {
        let mut sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}'",
            crate::DATABASE_NAME,
        );
        if !include_version {
            sql.push_str(" AND name != '");
            sql.push_str(crate::VERSION_TABLE_NAME);
            sql.push('\'');
        }
        // On a cluster, we need to operate on the "local" replicated tables.
        if replicated {
            sql.push_str(" AND engine = 'ReplicatedMergeTree'");
        }
        self.execute_with_body(sql).await.map(|(_summary, body)| {
            body.lines().map(ToString::to_string).collect()
        })
    }
}

/// Helper argument to `Client::list_oximeter_database_tables`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct ListDetails {
    /// If true, include the version table in the output.
    include_version: bool,
    /// If true, list tables to operate on in a replicated cluster configuration.
    ///
    /// NOTE: We would like to always operate on the "top-level table", e.g.
    /// `oximeter.measurements_u64`, regardless of whether we're working on the
    /// cluster or a single-node setup. Otherwise, we need to know which cluster
    /// we're working with, and then query either `measurements_u64` or
    /// `measurements_u64_local` based on that.
    ///
    /// However, while that works for the local tables (even replicated ones),
    /// it does _not_ work for the `Distributed` tables that we use as those
    /// "top-level tables" in a cluster setup. That table engine does not
    /// support mutations. Instead, we need to run those operations on the
    /// `*_local` tables.
    replicated: bool,
}

// A regex used to validate supported schema updates.
static SCHEMA_VALIDATION_REGEX: OnceLock<Regex> = OnceLock::new();
fn schema_validation_regex() -> &'static Regex {
    SCHEMA_VALIDATION_REGEX.get_or_init(|| {
        RegexBuilder::new(concat!(
            // Cannot insert rows
            r#"(INSERT INTO)|"#,
            // Cannot delete rows in a table
            r#"(ALTER TABLE .* DELETE)|"#,
            // Cannot update values in a table
            r#"(ALTER TABLE .* UPDATE)|"#,
            // Cannot drop column values
            r#"(ALTER TABLE .* CLEAR COLUMN)|"#,
            // Or issue lightweight deletes
            r#"(DELETE FROM)"#,
        ))
        .case_insensitive(true)
        .build()
        .expect("Invalid regex")
    })
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
        Err(Error::Database(format!("Query failed: {body}")))
    }
}

#[cfg(test)]
mod tests {
    use super::dbwrite::UnrolledSampleRows;
    use super::*;
    use crate::model::OXIMETER_VERSION;
    use crate::query;
    use crate::query::field_table_name;
    use bytes::Bytes;
    use chrono::Utc;
    use dropshot::test_util::LogContext;
    use futures::Future;
    use omicron_test_utils::dev::clickhouse::ClickHouseDeployment;
    use omicron_test_utils::dev::test_setup_log;
    use oximeter::histogram::Histogram;
    use oximeter::types::MissingDatum;
    use oximeter::Datum;
    use oximeter::FieldValue;
    use oximeter::Measurement;
    use oximeter::Metric;
    use oximeter::Target;
    use std::net::Ipv6Addr;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::time::Duration;
    use tempfile::TempDir;
    use uuid::Uuid;

    /// Use client to initialize the database.
    async fn init_db(db: &ClickHouseDeployment, client: &Client) {
        if db.is_cluster() {
            client
                .init_replicated_db()
                .await
                .expect("Failed to initialize replicated timeseries database");
        } else {
            client
                .init_single_node_db()
                .await
                .expect("Failed to initialize single-node timeseries database");
        }
    }

    /// Use client to wipe the database.
    async fn wipe_db(db: &ClickHouseDeployment, client: &Client) {
        if db.is_cluster() {
            client
                .wipe_replicated_db()
                .await
                .expect("Failed to wipe replicated timeseries database");
        } else {
            client
                .wipe_single_node_db()
                .await
                .expect("Failed to wipe single-node timeseries database");
        }
    }

    // NOTE: Please read this when adding new tests here.
    //
    // The tests here are written in a sort of opaque way that deserves some
    // explanation. The main reason for this is that we want to run some of the
    // tests both against a single-node and replicated ClickHouse cluster.
    //
    // The set up for those two is very different, and importantly, we don't
    // want to pay the cost of completely restarting all the ClickHouse
    // processes for a replicated test. Instead, we'd like to just wipe the
    // database in between them.
    //
    // So we try to share the bulk of the test itself in an implementation
    // function. That should take a reference to the database and a client, and
    // run whatever it needs to do. It can assume that the database is
    // _initialized_ for the deployment type, but that it contains no data. It
    // can do whatever it wants to with the database in the meantime -- no other
    // other tests will see those effects.
    //
    // For example, suppose we wanted to run a test called `test_do_the_thing`.
    // We would write a single-node version like this:
    //
    // ```rust
    // #[tokio::test]
    // async fn test_to_the_thing() {
    //    let logctx = test_setup_log("test_do_the_thing");
    //    let mut db =
    //        ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
    //    let client = Client::new(db.http_address().into(), &logctx.log);
    //    init_db(&db, &client).await;
    //    test_do_the_thing_impl(&db, client).await;
    //    db.cleanup().await.unwrap();
    //    logctx.cleanup_successful();
    // }
    // ```
    //
    // That calls `test_do_the_thing_impl()` to actually run the guts of the
    // test. That function can then be passed to the replicated tests, albeit
    // with some ceremony. In `test_replicated()`, which runs all the tests
    // against a replicated cluster, we put `test_do_the_thing_impl` into a list
    // of boxed closures, called `futures`, like this:
    //
    // ```
    //    // ... other tests above,
    //    (
    //        "test_do_the_thing",
    //        Box::new(move |db, client| Box::pin(test_do_the_thing_impl(db, client))),
    //    ),
    // ```
    //
    // That specifies the test name, for naming log files, and then packages up
    // the closure so all of them can be stored and executed.
    //
    // See `test_replicated()` for details and examples.

    async fn create_cluster(logctx: &LogContext) -> ClickHouseDeployment {
        let cur_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let replica_config =
            cur_dir.as_path().join("src/configs/replica_config.xml");
        let keeper_config =
            cur_dir.as_path().join("src/configs/keeper_config.xml");
        ClickHouseDeployment::new_cluster(logctx, replica_config, keeper_config)
            .await
            .expect("Failed to initialise ClickHouse Cluster")
    }

    // A horrendous type signature used to store all replicated tests in an
    // array.
    //
    // This says:
    //
    // - A boxed function trait object...
    // - that accepts a deployment and client ...
    // - and returns a boxed future ...
    // - that resolves to ().
    type AsyncTest<'a> = Box<
        dyn FnMut(
                &'a ClickHouseDeployment,
                Client,
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + Sync
            + 'a,
    >;

    #[tokio::test]
    async fn test_replicated() {
        let logctx = test_setup_log("test_replicated");
        let mut cluster = create_cluster(&logctx).await;
        let address = cluster.http_address().into();
        let client = Client::new(address, &logctx.log);
        let futures: Vec<(&'static str, AsyncTest)> = vec![
            (
                "test_is_oximeter_cluster_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_is_oximeter_cluster_impl(db, client))
                }),
            ),
            (
                "test_insert_samples_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_insert_samples_impl(db, client))
                }),
            ),
            (
                "test_schema_mismatch_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_schema_mismatch_impl(db, client))
                }),
            ),
            (
                "test_schema_updated_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_schema_updated_impl(db, client))
                }),
            ),
            (
                "test_client_select_timeseries_one_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_client_select_timeseries_one_impl(db, client))
                }),
            ),
            (
                "test_field_record_count_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_field_record_count_impl(db, client))
                }),
            ),
            (
                "test_differentiate_by_timeseries_name_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_differentiate_by_timeseries_name_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_select_one_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_select_timeseries_with_select_one_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_select_one_field_with_multiple_values_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_select_timeseries_with_select_one_field_with_multiple_values_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_select_multiple_fields_with_multiple_values_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_select_timeseries_with_select_multiple_fields_with_multiple_values_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_all_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_select_timeseries_with_all_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_start_time_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_select_timeseries_with_start_time_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_limit_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_select_timeseries_with_limit_impl(db, client))
                }),
            ),
            (
                "test_select_timeseries_with_order_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_select_timeseries_with_order_impl(db, client))
                }),
            ),
            (
                "test_get_schema_no_new_values_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_get_schema_no_new_values_impl(db, client))
                }),
            ),
            (
                "test_timeseries_schema_list_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_timeseries_schema_list_impl(db, client))
                }),
            ),
            (
                "test_list_timeseries_replicated",
                Box::new(move |db, client|{
                    Box::pin(test_list_timeseries_impl(db, client))
                }),
            ),
            (
                "test_list_timeseries_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_list_timeseries_impl(db, client))
                }),
            ),
            (
                "test_database_version_update_idempotent_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_database_version_update_is_idempotent_impl(db, client))
                }),
            ),
            (
                "test_database_version_will_not_downgrade_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_database_version_will_not_downgrade_impl(db, client))
                }),
            ),
            (
                "test_database_version_wipes_old_version_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_database_version_wipes_old_version_impl(db, client))
                }),
            ),
            (
                "test_update_schema_cache_on_new_sample_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_update_schema_cache_on_new_sample_impl(db, client))
                }),
            ),
            (
                "test_select_all_datum_types_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_select_all_datum_types_impl(db, client))
                }),
            ),
            (
                "test_new_schema_removed_when_not_inserted_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_new_schema_removed_when_not_inserted_impl(db, client))
                }),
            ),
            (
                "test_recall_of_all_fields_replicated",
                Box::new(move |db, client| {
                    Box::pin(test_recall_of_all_fields_impl(db, client))
                }),
            ),
        ];
        for (test_name, mut test) in futures {
            let testctx = test_setup_log(test_name);
            init_db(&cluster, &client).await;
            test(&cluster, Client::new(address, &logctx.log)).await;
            wipe_db(&cluster, &client).await;
            testctx.cleanup_successful();
        }
        cluster.cleanup().await.expect("Failed to cleanup ClickHouse cluster");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn bad_db_connection_test() {
        let logctx = test_setup_log("test_bad_db_connection");
        let log = &logctx.log;
        let client = Client::new("127.0.0.1:443".parse().unwrap(), &log);
        assert!(matches!(
            client.ping().await,
            Err(Error::DatabaseUnavailable(_))
        ));
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_is_oximeter_cluster() {
        let logctx = test_setup_log("test_is_oximeter_cluster");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_is_oximeter_cluster_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_is_oximeter_cluster_impl(
        db: &ClickHouseDeployment,
        client: Client,
    ) {
        if db.is_cluster() {
            assert!(client.is_oximeter_cluster().await.unwrap());
        } else {
            assert!(!client.is_oximeter_cluster().await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_insert_samples() {
        let logctx = test_setup_log("test_insert_samples");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_insert_samples_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_insert_samples_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let samples = {
            let mut s = Vec::with_capacity(8);
            for _ in 0..s.capacity() {
                s.push(oximeter_test_utils::make_hist_sample())
            }
            s
        };
        client
            .insert_samples(&samples)
            .await
            .expect("Should be able to insert samples");
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
        let logctx = test_setup_log("test_schema_mismatch");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_schema_mismatch_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_schema_mismatch_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let sample = oximeter_test_utils::make_sample();
        client.insert_samples(&[sample]).await.unwrap();
        let bad_name = name_mismatch::TestTarget {
            name: "first_name".into(),
            name2: "second_name".into(),
            num: 2,
        };
        let metric = oximeter_test_utils::TestMetric {
            id: uuid::Uuid::new_v4(),
            good: true,
            datum: 1,
        };
        let sample = Sample::new(&bad_name, &metric).unwrap();
        let result = client.verify_or_cache_sample_schema(&sample).await;
        assert!(matches!(result, Err(Error::SchemaMismatch { .. })));
    }

    #[tokio::test]
    async fn test_schema_update() {
        let logctx = test_setup_log("test_schema_update");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_schema_updated_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_schema_updated_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let sample = oximeter_test_utils::make_sample();

        // Verify that this sample is considered new, i.e., we return rows to update the timeseries
        // schema table.
        let result =
            client.verify_or_cache_sample_schema(&sample).await.unwrap();
        assert!(
                matches!(result, Some(_)),
                "When verifying a new sample, the rows to be inserted should be returned"
            );

        // Clear the internal caches of seen schema
        client.schema.lock().await.clear();

        // Insert the new sample
        client.insert_samples(&[sample.clone()]).await.unwrap();

        // The internal map should now contain both the new timeseries schema
        let actual_schema = TimeseriesSchema::from(&sample);
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
        let result =
            client.verify_or_cache_sample_schema(&sample).await.unwrap();
        assert!(
            matches!(result, None),
            "After inserting new schema, it should no longer be considered new"
        );

        // Verify that it's actually in the database!
        let sql = String::from(
            "SELECT * FROM oximeter.timeseries_schema FORMAT JSONEachRow;",
        );
        let result = client.execute_with_body(sql).await.unwrap().1;
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
    }

    #[tokio::test]
    async fn test_client_select_timeseries_one() {
        let logctx = test_setup_log("test_client_select_timeseries_one");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_client_select_timeseries_one_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_client_select_timeseries_one_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let samples = oximeter_test_utils::generate_test_samples(2, 2, 2, 2);
        client.insert_samples(&samples).await.unwrap();

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
    }

    #[tokio::test]
    async fn test_field_record_count() {
        let logctx = test_setup_log("test_field_record_cont");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_field_record_count_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_field_record_count_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        // This test verifies that the number of records in the field tables is as expected.
        //
        // Because of the schema change, inserting field records per field per unique timeseries,
        // we'd like to exercise the logic of ClickHouse's replacing merge tree engine.
        let samples = oximeter_test_utils::generate_test_samples(2, 2, 2, 2);
        client.insert_samples(&samples).await.unwrap();

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
                .unwrap()
                .1;
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
    }

    #[tokio::test]
    async fn test_unquoted_64bit_integers() {
        let logctx = test_setup_log("test_unquoted_64bit_integers");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_unquoted_64bit_integers_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Regression test verifying that integers are returned in the expected format from the
    // database.
    //
    // By default, ClickHouse _quotes_ 64-bit integers, which is apparently to support JavaScript
    // implementations of JSON. See https://github.com/ClickHouse/ClickHouse/issues/2375 for
    // details. This test verifies that we get back _unquoted_ integers from the database.
    async fn test_unquoted_64bit_integers_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        use serde_json::Value;
        let output = client
            .execute_with_body(
                "SELECT toUInt64(1) AS foo FORMAT JSONEachRow;".to_string(),
            )
            .await
            .unwrap()
            .1;
        let json: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(json["foo"], Value::Number(1u64.into()));
    }

    #[tokio::test]
    async fn test_differentiate_by_timeseries_name() {
        let logctx = test_setup_log("test_differentiate_by_timeseries_name");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_differentiate_by_timeseries_name_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_differentiate_by_timeseries_name_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
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
    }

    #[tokio::test]
    async fn select_timeseries_with_select_one() {
        let logctx = test_setup_log("test_select_timeseries_with_select_one");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_select_one_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_select_one_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (target, metrics, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        // This set of criteria should select exactly one timeseries, with two measurements.
        // The target is the same in all cases, but we're looking for the first of the metrics, and
        // the first two samples/measurements.
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code==200"];
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
                None,
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

        assert_eq!(timeseries.len(), 1, "Expected one timeseries");
        let timeseries = timeseries.get(0).unwrap();
        assert_eq!(
            timeseries.measurements.len(),
            2,
            "Expected exactly two measurements"
        );
        verify_measurements(&timeseries.measurements, &samples[..2]);
        verify_target(&timeseries.target, &target);
        verify_metric(&timeseries.metric, metrics.get(0).unwrap());
    }

    #[tokio::test]
    async fn test_select_timeseries_with_select_one_field_with_multiple_values()
    {
        let logctx = test_setup_log(
            "test_select_timeseries_with_select_one_field_with_multiple_values",
        );
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_select_one_field_with_multiple_values_impl(
            &db, client,
        )
        .await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_select_one_field_with_multiple_values_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (target, metrics, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        // This set of criteria should select the last two metrics, and so the last two
        // timeseries. The target is the same in all cases.
        let criteria =
            &["name==oximeter", "route==/a", "method==GET", "status_code>200"];
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
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
            verify_target(&ts.target, &target);
        }

        for (ts, metric) in timeseries.iter().zip(metrics[1..3].iter()) {
            verify_metric(&ts.metric, metric);
        }
    }

    #[tokio::test]
    async fn test_select_timeseries_with_select_multiple_fields_with_multiple_values(
    ) {
        let logctx =
            test_setup_log("test_select_timeseries_with_select_multiple_fields_with_multiple_values");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_select_multiple_fields_with_multiple_values_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_select_multiple_fields_with_multiple_values_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (target, metrics, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        // This is non-selective for the route, which is the "second axis", and has two values for
        // the third axis, status code. There should be a total of 4 timeseries, since there are
        // two methods and two possible status codes.
        let criteria = &["name==oximeter", "route==/a", "status_code>200"];
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                criteria,
                None,
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
            verify_target(&ts.target, &target);
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

    #[tokio::test]
    async fn test_select_timeseries_with_all() {
        let logctx = test_setup_log("test_select_timeseries_with_all");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_all_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_all_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (target, metrics, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                // We're selecting all timeseries/samples here.
                &[],
                None,
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
            verify_target(&ts.target, &target);
            verify_metric(&ts.metric, metrics.get(i).unwrap());
        }
    }

    #[tokio::test]
    async fn test_select_timeseries_with_start_time() {
        let logctx = test_setup_log("test_select_timeseries_with_start_time");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_start_time_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_start_time_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (_, metrics, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        let start_time = samples[samples.len() / 2].measurement.timestamp();
        let mut timeseries = client
            .select_timeseries_with(
                timeseries_name,
                // We're selecting all timeseries/samples here.
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
    }

    #[tokio::test]
    async fn test_select_timeseries_with_limit() {
        let logctx = test_setup_log("test_select_timeseries_with_limit");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_limit_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_limit_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (_, _, samples) = setup_select_test();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert samples");

        let timeseries_name = "service:request_latency";
        // We have to define criteria that resolve to a single timeseries
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
    }

    #[tokio::test]
    async fn test_select_timeseries_with_order() {
        let logctx = test_setup_log("test_select_timeseries_with_order");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_timeseries_with_order_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_select_timeseries_with_order_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let (_, _, samples) = setup_select_test();
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
    }

    #[tokio::test]
    async fn test_get_schema_no_new_values() {
        let logctx = test_setup_log("test_get_schema_no_new_values");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_get_schema_no_new_values_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_get_schema_no_new_values_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let samples = oximeter_test_utils::generate_test_samples(2, 2, 2, 2);
        client.insert_samples(&samples).await.unwrap();

        let original_schema = client.schema.lock().await.clone();
        let mut schema = client.schema.lock().await;
        client
            .get_schema_locked(&mut schema)
            .await
            .expect("Failed to get timeseries schema");
        assert_eq!(&original_schema, &*schema, "Schema shouldn't change");
    }

    #[tokio::test]
    async fn test_timeseries_schema_list() {
        let logctx = test_setup_log("test_timeseries_schema_list");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_timeseries_schema_list_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_timeseries_schema_list_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let samples = oximeter_test_utils::generate_test_samples(2, 2, 2, 2);
        client.insert_samples(&samples).await.unwrap();

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
    }

    #[tokio::test]
    async fn test_list_timeseries() {
        let logctx = test_setup_log("test_list_timeseries");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_list_timeseries_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_list_timeseries_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        let samples = oximeter_test_utils::generate_test_samples(2, 2, 2, 2);
        client.insert_samples(&samples).await.unwrap();

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
    }

    async fn recall_field_value_bool_test(
        client: &Client,
    ) -> Result<(), Error> {
        let field = FieldValue::Bool(true);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_u8_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::U8(1);
        let as_json = serde_json::Value::from(1_u8);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_i8_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::I8(1);
        let as_json = serde_json::Value::from(1_i8);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_u16_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::U16(1);
        let as_json = serde_json::Value::from(1_u16);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_i16_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::I16(1);
        let as_json = serde_json::Value::from(1_i16);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_u32_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::U32(1);
        let as_json = serde_json::Value::from(1_u32);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_i32_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::I32(1);
        let as_json = serde_json::Value::from(1_i32);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_u64_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::U64(1);
        let as_json = serde_json::Value::from(1_u64);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_i64_test(client: &Client) -> Result<(), Error> {
        let field = FieldValue::I64(1);
        let as_json = serde_json::Value::from(1_i64);
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_string_test(
        client: &Client,
    ) -> Result<(), Error> {
        let field = FieldValue::String("foo".into());
        let as_json = serde_json::Value::from("foo");
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_ipv6addr_test(
        client: &Client,
    ) -> Result<(), Error> {
        let field = FieldValue::from(Ipv6Addr::LOCALHOST);
        let as_json = serde_json::Value::from(Ipv6Addr::LOCALHOST.to_string());
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn recall_field_value_uuid_test(
        client: &Client,
    ) -> Result<(), Error> {
        let id = Uuid::new_v4();
        let field = FieldValue::from(id);
        let as_json = serde_json::Value::from(id.to_string());
        test_recall_field_value_impl(field, as_json, client).await?;
        Ok(())
    }

    async fn test_recall_field_value_impl(
        field_value: FieldValue,
        as_json: serde_json::Value,
        client: &Client,
    ) -> Result<(), Error> {
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
            .expect("Failed to select field row")
            .1;
        let actual_row: serde_json::Value = serde_json::from_str(&body)
            .expect("Failed to parse field row JSON");
        println!("{actual_row:?}");
        println!("{inserted_row:?}");
        assert_eq!(
            actual_row, inserted_row,
            "Actual and expected field rows do not match"
        );
        Ok(())
    }

    async fn test_recall_missing_scalar_measurement_impl(
        measurement: Measurement,
        client: &Client,
    ) -> Result<(), Error> {
        let start_time = if measurement.datum().is_cumulative() {
            Some(Utc::now())
        } else {
            None
        };
        let missing_datum = Datum::from(
            MissingDatum::new(measurement.datum_type(), start_time).unwrap(),
        );
        let missing_measurement = Measurement::new(Utc::now(), missing_datum);
        test_recall_measurement_impl(missing_measurement, client).await?;
        Ok(())
    }

    async fn recall_measurement_bool_test(
        client: &Client,
    ) -> Result<(), Error> {
        let datum = Datum::Bool(true);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_i8_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::I8(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_u8_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::U8(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_i16_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::I16(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_u16_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::U16(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_i32_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::I32(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_u32_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::U32(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_i64_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::I64(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_u64_test(client: &Client) -> Result<(), Error> {
        let datum = Datum::U64(1);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_f32_test(client: &Client) -> Result<(), Error> {
        const VALUE: f32 = 1.1;
        let datum = Datum::F32(VALUE);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_f64_test(client: &Client) -> Result<(), Error> {
        const VALUE: f64 = 1.1;
        let datum = Datum::F64(VALUE);
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_string_test(
        client: &Client,
    ) -> Result<(), Error> {
        let value = String::from("foo");
        let datum = Datum::String(value.clone());
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_bytes_test(
        client: &Client,
    ) -> Result<(), Error> {
        let value = Bytes::from(vec![0, 1, 2]);
        let datum = Datum::Bytes(value.clone());
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        // NOTE: We don't currently support missing byte array samples.
        Ok(())
    }

    async fn recall_measurement_cumulative_i64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let datum = Datum::CumulativeI64(1.into());
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_cumulative_u64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let datum = Datum::CumulativeU64(1.into());
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn recall_measurement_cumulative_f64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let datum = Datum::CumulativeF64(1.1.into());
        let measurement = Measurement::new(Utc::now(), datum);
        test_recall_measurement_impl(measurement.clone(), client).await?;
        test_recall_missing_scalar_measurement_impl(measurement, client)
            .await?;
        Ok(())
    }

    async fn histogram_test_impl<T>(
        client: &Client,
        hist: Histogram<T>,
    ) -> Result<(), Error>
    where
        T: oximeter::histogram::HistogramSupport,
        Datum: From<oximeter::histogram::Histogram<T>>,
        serde_json::Value: From<T>,
    {
        let datum = Datum::from(hist);

        // We artificially give different timestamps to avoid a test flake in
        // CI (reproducible reliably on macOS) where the two Utc::now() are the
        // same, which means we get two results on retrieval when we expect one
        let t1 = Utc::now();
        let t2 = t1 + Duration::from_nanos(1);

        let measurement = Measurement::new(t1, datum);
        let missing_datum = Datum::Missing(
            MissingDatum::new(measurement.datum_type(), Some(t2)).unwrap(),
        );
        let missing_measurement = Measurement::new(t2, missing_datum);
        test_recall_measurement_impl(measurement, client).await?;
        test_recall_measurement_impl(missing_measurement, client).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_i8_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0i8, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_u8_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0u8, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_i16_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0i16, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_u16_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0u16, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_i32_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0i32, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_u32_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0u32, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_i64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0i64, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_u64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0u64, 1, 2]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
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
    #[allow(dead_code)]
    async fn recall_measurement_histogram_f32_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0.1f32, 0.2, 0.3]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn recall_measurement_histogram_f64_test(
        client: &Client,
    ) -> Result<(), Error> {
        let hist = Histogram::new(&[0.1f64, 0.2, 0.3]).unwrap();
        histogram_test_impl(client, hist).await?;
        Ok(())
    }

    async fn test_recall_measurement_impl(
        measurement: Measurement,
        client: &Client,
    ) -> Result<(), Error> {
        // Insert a record from this datum.
        const TIMESERIES_NAME: &str = "foo:bar";
        const TIMESERIES_KEY: u64 = 101;
        let (measurement_table, inserted_row) =
            crate::model::unroll_measurement_row_impl(
                TIMESERIES_NAME.to_string(),
                TIMESERIES_KEY,
                &measurement,
            );
        let insert_sql = format!(
            "INSERT INTO {measurement_table} FORMAT JSONEachRow {inserted_row}",
        );
        println!("Inserted row: {}", inserted_row);
        client
            .execute(insert_sql)
            .await
            .expect("Failed to insert measurement row");

        // Select it exactly back out.
        let select_sql = format!(
            "SELECT * FROM {} WHERE timestamp = '{}' FORMAT {};",
            measurement_table,
            measurement.timestamp().format(crate::DATABASE_TIMESTAMP_FORMAT),
            crate::DATABASE_SELECT_FORMAT,
        );
        let body = client
            .execute_with_body(select_sql)
            .await
            .expect("Failed to select measurement row")
            .1;
        let (_, actual_row) = crate::model::parse_measurement_from_row(
            &body,
            measurement.datum_type(),
        );
        println!("Actual row: {actual_row:?}");
        assert_eq!(
            actual_row, measurement,
            "Actual and expected measurement rows do not match"
        );
        Ok(())
    }

    // Returns the number of timeseries schemas being used.
    async fn get_schema_count(client: &Client) -> usize {
        client
            .execute_with_body(
                "SELECT * FROM oximeter.timeseries_schema FORMAT JSONEachRow;",
            )
            .await
            .expect("Failed to SELECT from database")
            .1
            .lines()
            .count()
    }

    #[tokio::test]
    async fn test_recall_of_all_fields() {
        let logctx = test_setup_log("test_recall_of_all_fields");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_recall_of_all_fields_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_recall_of_all_fields_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        recall_measurement_bool_test(&client).await.unwrap();
        recall_measurement_i8_test(&client).await.unwrap();
        recall_measurement_u8_test(&client).await.unwrap();
        recall_measurement_i16_test(&client).await.unwrap();
        recall_measurement_u16_test(&client).await.unwrap();
        recall_measurement_i32_test(&client).await.unwrap();
        recall_measurement_u32_test(&client).await.unwrap();
        recall_measurement_i64_test(&client).await.unwrap();
        recall_measurement_u64_test(&client).await.unwrap();
        recall_measurement_f32_test(&client).await.unwrap();
        recall_measurement_f64_test(&client).await.unwrap();
        recall_measurement_string_test(&client).await.unwrap();
        recall_measurement_bytes_test(&client).await.unwrap();
        recall_measurement_cumulative_i64_test(&client).await.unwrap();
        recall_measurement_cumulative_u64_test(&client).await.unwrap();
        recall_measurement_cumulative_f64_test(&client).await.unwrap();
        recall_measurement_histogram_i8_test(&client).await.unwrap();
        recall_measurement_histogram_u8_test(&client).await.unwrap();
        recall_measurement_histogram_i16_test(&client).await.unwrap();
        recall_measurement_histogram_u16_test(&client).await.unwrap();
        recall_measurement_histogram_i32_test(&client).await.unwrap();
        recall_measurement_histogram_u32_test(&client).await.unwrap();
        recall_measurement_histogram_i64_test(&client).await.unwrap();
        recall_measurement_histogram_u64_test(&client).await.unwrap();
        recall_measurement_histogram_f64_test(&client).await.unwrap();
        recall_field_value_bool_test(&client).await.unwrap();
        recall_field_value_u8_test(&client).await.unwrap();
        recall_field_value_i8_test(&client).await.unwrap();
        recall_field_value_u16_test(&client).await.unwrap();
        recall_field_value_i16_test(&client).await.unwrap();
        recall_field_value_u32_test(&client).await.unwrap();
        recall_field_value_i32_test(&client).await.unwrap();
        recall_field_value_u64_test(&client).await.unwrap();
        recall_field_value_i64_test(&client).await.unwrap();
        recall_field_value_string_test(&client).await.unwrap();
        recall_field_value_ipv6addr_test(&client).await.unwrap();
        recall_field_value_uuid_test(&client).await.unwrap();
    }

    #[tokio::test]
    async fn test_database_version_update_is_idempotent() {
        let logctx =
            test_setup_log("test_database_version_update_is_idempotent");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        // NOTE: We don't init the DB, because the test explicitly tests that.
        test_database_version_update_is_idempotent_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_database_version_update_is_idempotent_impl(
        db: &ClickHouseDeployment,
        client: Client,
    ) {
        // Initialize the database...
        let replicated = db.is_cluster();
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Insert data here so we can verify it still exists later.
        //
        // The values here don't matter much, we just want to check that
        // the database data hasn't been dropped.
        assert_eq!(0, get_schema_count(&client).await);
        let sample = oximeter_test_utils::make_sample();
        client.insert_samples(&[sample.clone()]).await.unwrap();
        assert_eq!(1, get_schema_count(&client).await);

        // Re-initialize the database, see that our data still exists
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        assert_eq!(1, get_schema_count(&client).await);
    }

    #[tokio::test]
    async fn test_database_version_will_not_downgrade() {
        let logctx = test_setup_log("test_database_version_will_not_downgrade");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        // NOTE: We don't init the DB, because the test explicitly tests that.
        test_database_version_will_not_downgrade_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_database_version_will_not_downgrade_impl(
        db: &ClickHouseDeployment,
        client: Client,
    ) {
        // Initialize the database
        let replicated = db.is_cluster();
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
    }

    #[tokio::test]
    async fn test_database_version_wipes_old_version() {
        let logctx = test_setup_log("test_database_version_wipes_old_version");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        // NOTE: We don't init the DB, because the test explicitly tests that.
        test_database_version_wipes_old_version_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_database_version_wipes_old_version_impl(
        db: &ClickHouseDeployment,
        client: Client,
    ) {
        // Initialize the Client
        let replicated = db.is_cluster();
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Insert data here so we can remove it later.
        //
        // The values here don't matter much, we just want to check that
        // the database data gets dropped later.
        assert_eq!(0, get_schema_count(&client).await);
        let sample = oximeter_test_utils::make_sample();
        client.insert_samples(&[sample.clone()]).await.unwrap();
        assert_eq!(1, get_schema_count(&client).await);

        // If we try to upgrade to a newer version, we'll drop old data.
        client
            .initialize_db_with_version(replicated, model::OXIMETER_VERSION + 1)
            .await
            .expect("Should have initialized database successfully");
        assert_eq!(0, get_schema_count(&client).await);
    }

    #[tokio::test]
    async fn test_update_schema_cache_on_new_sample() {
        let logctx = test_setup_log("test_update_schema_cache_on_new_sample");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_update_schema_cache_on_new_sample_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_update_schema_cache_on_new_sample_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        usdt::register_probes().unwrap();
        let samples = [oximeter_test_utils::make_sample()];
        client.insert_samples(&samples).await.unwrap();

        // Get the count of schema directly from the DB, which should have just
        // one.
        let response = client.execute_with_body(
            "SELECT COUNT() FROM oximeter.timeseries_schema FORMAT JSONEachRow;
        ").await.unwrap().1;
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
        ").await.unwrap().1;
        assert_eq!(
            response.lines().count(),
            1,
            "Expected exactly 1 schema again"
        );
        assert_eq!(client.schema.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn test_select_all_datum_types() {
        let logctx = test_setup_log("test_select_all_datum_types");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_select_all_datum_types_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Regression test for https://github.com/oxidecomputer/omicron/issues/4336.
    //
    // This tests that we can successfully query all extant datum types from the
    // schema table. There may be no such values, but the query itself should
    // succeed.
    async fn test_select_all_datum_types_impl(
        _: &ClickHouseDeployment,
        client: Client,
    ) {
        use strum::IntoEnumIterator;
        usdt::register_probes().unwrap();
        // Attempt to select all schema with each datum type.
        for ty in oximeter::DatumType::iter() {
            let sql = format!(
                "SELECT COUNT() \
                FROM {}.timeseries_schema WHERE \
                datum_type = '{:?}'",
                crate::DATABASE_NAME,
                crate::model::DbDatumType::from(ty),
            );
            let res = client.execute_with_body(sql).await.unwrap().1;
            let count = res.trim().parse::<usize>().unwrap();
            assert_eq!(count, 0);
        }
    }

    #[tokio::test]
    async fn test_new_schema_removed_when_not_inserted() {
        let logctx =
            test_setup_log("test_new_schema_removed_when_not_inserted");
        let mut db =
            ClickHouseDeployment::new_single_node(&logctx).await.unwrap();
        let client = Client::new(db.http_address().into(), &logctx.log);
        init_db(&db, &client).await;
        test_new_schema_removed_when_not_inserted_impl(&db, client).await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Regression test for https://github.com/oxidecomputer/omicron/issues/4335.
    //
    // This tests that, when cache new schema but _fail_ to insert them, we also
    // remove them from the internal cache.
    async fn test_new_schema_removed_when_not_inserted_impl(
        db: &ClickHouseDeployment,
        client: Client,
    ) {
        usdt::register_probes().unwrap();
        let samples = [oximeter_test_utils::make_sample()];

        // We're using the components of the `insert_samples()` method here,
        // which has been refactored explicitly for this test. We need to insert
        // the schema for this sample into the internal cache, which relies on
        // access to the database (since they don't exist).
        //
        // First, insert the sample into the local cache. This method also
        // checks the DB, since this schema doesn't exist in the cache.
        let UnrolledSampleRows { new_schema, .. } =
            client.unroll_samples(&samples).await;
        assert_eq!(client.schema.lock().await.len(), 1);

        // Next, we'll kill the database, and then try to insert the schema.
        // That will fail, since the DB is now inaccessible.
        wipe_db(&db, &client).await;
        let res = client.save_new_schema_or_remove(new_schema).await;
        assert!(res.is_err(), "Should have failed since the DB is gone");
        assert!(
            client.schema.lock().await.is_empty(),
            "Failed to remove new schema from the cache when \
            they could not be inserted into the DB"
        );
    }

    // Testing helper functions

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

    // Small helper to go from a multidimensional index to a flattened array index.
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

    async fn create_test_upgrade_schema_directory(
        replicated: bool,
        versions: &[u64],
    ) -> (TempDir, Vec<PathBuf>) {
        assert!(!versions.is_empty());
        let schema_dir = TempDir::new().expect("failed to create tempdir");
        let mut paths = Vec::with_capacity(versions.len());
        for version in versions.iter() {
            let version_dir = Client::full_upgrade_path(
                replicated,
                *version,
                schema_dir.as_ref(),
            );
            fs::create_dir_all(&version_dir)
                .await
                .expect("failed to make version directory");
            paths.push(version_dir);
        }
        (schema_dir, paths)
    }

    #[tokio::test]
    async fn test_read_schema_upgrade_sql_files() {
        let logctx = test_setup_log("test_read_schema_upgrade_sql_files");
        let log = &logctx.log;
        const REPLICATED: bool = false;
        const VERSION: u64 = 1;
        let (schema_dir, version_dirs) =
            create_test_upgrade_schema_directory(REPLICATED, &[VERSION]).await;
        let version_dir = &version_dirs[0];

        // Create a few SQL files in there.
        const SQL: &str = "SELECT NOW();";
        let filenames: Vec<_> = (0..3).map(|i| format!("up-{i}.sql")).collect();
        for name in filenames.iter() {
            let full_path = version_dir.join(name);
            fs::write(full_path, SQL).await.expect("Failed to write dummy SQL");
        }

        let upgrade_files = Client::read_schema_upgrade_sql_files(
            log,
            REPLICATED,
            VERSION,
            schema_dir.path(),
        )
        .await
        .expect("Failed to read schema upgrade files");
        for filename in filenames.iter() {
            let stem = filename.split_once('.').unwrap().0;
            assert_eq!(
                upgrade_files.get(stem).unwrap().1,
                SQL,
                "upgrade SQL file contents are not correct"
            );
        }
        logctx.cleanup_successful();
    }

    async fn test_apply_one_schema_upgrade_impl(
        log: &Logger,
        address: SocketAddr,
        replicated: bool,
    ) {
        let test_name = format!(
            "test_apply_one_schema_upgrade_{}",
            if replicated { "replicated" } else { "single_node" }
        );
        let client = Client::new(address, &log);

        // We'll test moving from version 1, which just creates a database and
        // table, to version 2, which adds two columns to that table in
        // different SQL files.
        client.execute(format!("CREATE DATABASE {test_name};")).await.unwrap();
        client
            .execute(format!(
                "\
            CREATE TABLE {test_name}.tbl (\
                `col0` UInt8 \
            )\
            ENGINE = MergeTree()
            ORDER BY `col0`;\
        "
            ))
            .await
            .unwrap();

        // Write out the upgrading SQL files.
        //
        // Note that all of these statements are going in the version 2 schema
        // directory.
        let (schema_dir, version_dirs) =
            create_test_upgrade_schema_directory(replicated, &[NEXT_VERSION])
                .await;
        const NEXT_VERSION: u64 = 2;
        let first_sql =
            format!("ALTER TABLE {test_name}.tbl ADD COLUMN `col1` UInt16;");
        let second_sql =
            format!("ALTER TABLE {test_name}.tbl ADD COLUMN `col2` String;");
        let all_sql = [first_sql, second_sql];
        let version_dir = &version_dirs[0];
        for (i, sql) in all_sql.iter().enumerate() {
            let path = version_dir.join(format!("up-{i}.sql"));
            fs::write(path, sql)
                .await
                .expect("failed to write out upgrade SQL file");
        }

        // Apply the upgrade itself.
        client
            .apply_one_schema_upgrade(
                replicated,
                NEXT_VERSION,
                schema_dir.path(),
            )
            .await
            .expect("Failed to apply one schema upgrade");

        // Check that it actually worked!
        let body = client
            .execute_with_body(format!(
                "\
            SELECT name, type FROM system.columns \
            WHERE database = '{test_name}' AND table = 'tbl' \
            ORDER BY name \
            FORMAT CSV;\
        "
            ))
            .await
            .unwrap()
            .1;
        let mut lines = body.lines();
        assert_eq!(lines.next().unwrap(), "\"col0\",\"UInt8\"");
        assert_eq!(lines.next().unwrap(), "\"col1\",\"UInt16\"");
        assert_eq!(lines.next().unwrap(), "\"col2\",\"String\"");
        assert!(lines.next().is_none());
    }

    #[tokio::test]
    async fn test_apply_one_schema_upgrade_replicated() {
        const TEST_NAME: &str = "test_apply_one_schema_upgrade_replicated";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let mut cluster = create_cluster(&logctx).await;
        let address = cluster.http_address().into();
        test_apply_one_schema_upgrade_impl(log, address, true).await;
        cluster.cleanup().await.expect("Failed to cleanup ClickHouse cluster");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_apply_one_schema_upgrade_single_node() {
        const TEST_NAME: &str = "test_apply_one_schema_upgrade_single_node";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        test_apply_one_schema_upgrade_impl(log, address, false).await;
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_schema_with_version_gaps_fails() {
        let logctx =
            test_setup_log("test_ensure_schema_with_version_gaps_fails");
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        let client = Client::new(address, &log);
        const REPLICATED: bool = false;
        client
            .initialize_db_with_version(
                REPLICATED,
                crate::model::OXIMETER_VERSION,
            )
            .await
            .expect("failed to initialize DB");

        const BOGUS_VERSION: u64 = u64::MAX;
        let (schema_dir, _) = create_test_upgrade_schema_directory(
            REPLICATED,
            &[crate::model::OXIMETER_VERSION, BOGUS_VERSION],
        )
        .await;

        let err = client
            .ensure_schema(REPLICATED, BOGUS_VERSION, schema_dir.path())
            .await
            .expect_err(
                "Should have received an error when ensuring \
                non-sequential version numbers",
            );
        let Error::NonSequentialSchemaVersions = err else {
            panic!(
                "Expected an Error::NonSequentialSchemaVersions, found {err:?}"
            );
        };
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_schema_with_missing_desired_schema_version_fails() {
        let logctx = test_setup_log(
            "test_ensure_schema_with_missing_desired_schema_version_fails",
        );
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        let client = Client::new(address, &log);
        const REPLICATED: bool = false;
        client
            .initialize_db_with_version(
                REPLICATED,
                crate::model::OXIMETER_VERSION,
            )
            .await
            .expect("failed to initialize DB");

        let (schema_dir, _) = create_test_upgrade_schema_directory(
            REPLICATED,
            &[crate::model::OXIMETER_VERSION],
        )
        .await;

        const BOGUS_VERSION: u64 = u64::MAX;
        let err = client.ensure_schema(
            REPLICATED,
            BOGUS_VERSION,
            schema_dir.path(),
        ).await
            .expect_err("Should have received an error when ensuring a non-existing version");
        let Error::MissingSchemaVersion(missing) = err else {
            panic!("Expected an Error::MissingSchemaVersion, found {err:?}");
        };
        assert_eq!(missing, BOGUS_VERSION);

        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    async fn test_ensure_schema_walks_through_multiple_steps_impl(
        log: &Logger,
        address: SocketAddr,
        replicated: bool,
    ) {
        let test_name = format!(
            "test_ensure_schema_walks_through_multiple_steps_{}",
            if replicated { "replicated" } else { "single_node" }
        );
        let client = Client::new(address, &log);

        // We need to actually have the oximeter DB here, and the version table,
        // since `ensure_schema()` writes out versions to the DB as they're
        // applied.
        client.initialize_db_with_version(replicated, 1).await.unwrap();

        // We'll test moving from version 1, which just creates a database and
        // table, to version 3, stopping off at version 2. This is similar to
        // the `test_apply_one_schema_upgrade` test, but we split the two
        // modifications over two versions, rather than as multiple schema
        // upgrades in one version bump.
        client.execute(format!("CREATE DATABASE {test_name};")).await.unwrap();
        client
            .execute(format!(
                "\
            CREATE TABLE {test_name}.tbl (\
                `col0` UInt8 \
            )\
            ENGINE = MergeTree()
            ORDER BY `col0`;\
        "
            ))
            .await
            .unwrap();

        // Write out the upgrading SQL files.
        //
        // Note that each statement goes into a different version.
        const VERSIONS: [u64; 3] = [1, 2, 3];
        let (schema_dir, version_dirs) =
            create_test_upgrade_schema_directory(replicated, &VERSIONS).await;
        let first_sql = String::new();
        let second_sql =
            format!("ALTER TABLE {test_name}.tbl ADD COLUMN `col1` UInt16;");
        let third_sql =
            format!("ALTER TABLE {test_name}.tbl ADD COLUMN `col2` String;");
        let all_sql = [first_sql, second_sql, third_sql];
        for (version_dir, sql) in version_dirs.iter().zip(all_sql) {
            let path = version_dir.join("up.sql");
            fs::write(path, sql)
                .await
                .expect("failed to write out upgrade SQL file");
        }

        // Apply the sequence of upgrades.
        client
            .ensure_schema(
                replicated,
                *VERSIONS.last().unwrap(),
                schema_dir.path(),
            )
            .await
            .expect("Failed to apply one schema upgrade");

        // Check that it actually worked!
        let body = client
            .execute_with_body(format!(
                "\
            SELECT name, type FROM system.columns \
            WHERE database = '{test_name}' AND table = 'tbl' \
            ORDER BY name \
            FORMAT CSV;\
        "
            ))
            .await
            .unwrap()
            .1;
        let mut lines = body.lines();
        assert_eq!(lines.next().unwrap(), "\"col0\",\"UInt8\"");
        assert_eq!(lines.next().unwrap(), "\"col1\",\"UInt16\"");
        assert_eq!(lines.next().unwrap(), "\"col2\",\"String\"");
        assert!(lines.next().is_none());

        let latest_version = client.read_latest_version().await.unwrap();
        assert_eq!(
            latest_version,
            *VERSIONS.last().unwrap(),
            "Updated version not written to the database"
        );
    }

    #[tokio::test]
    async fn test_ensure_schema_walks_through_multiple_steps_single_node() {
        const TEST_NAME: &str =
            "test_ensure_schema_walks_through_multiple_steps_single_node";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        test_ensure_schema_walks_through_multiple_steps_impl(
            log, address, false,
        )
        .await;
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_schema_walks_through_multiple_steps_replicated() {
        const TEST_NAME: &str =
            "test_ensure_schema_walks_through_multiple_steps_replicated";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let mut cluster = create_cluster(&logctx).await;
        let address = cluster.http_address().into();
        test_ensure_schema_walks_through_multiple_steps_impl(
            log, address, true,
        )
        .await;
        cluster.cleanup().await.expect("Failed to clean up ClickHouse cluster");
        logctx.cleanup_successful();
    }

    #[test]
    fn test_verify_schema_upgrades() {
        let mut map = BTreeMap::new();

        // Check that we fail if the upgrade tries to insert data.
        map.insert(
            "up".into(),
            (
                PathBuf::from("/foo/bar/up.sql"),
                String::from(
                    "INSERT INTO oximeter.version (*) VALUES (100, now());",
                ),
            ),
        );
        assert!(Client::verify_schema_upgrades(&map).is_err());

        // Sanity check for the normal case.
        map.clear();
        map.insert(
            "up".into(),
            (
                PathBuf::from("/foo/bar/up.sql"),
                String::from("ALTER TABLE oximeter.measurements_bool ADD COLUMN foo UInt64;")
            ),
        );
        assert!(Client::verify_schema_upgrades(&map).is_ok());

        // Check that we fail if the upgrade ties to delete any data.
        map.clear();
        map.insert(
            "up".into(),
            (
                PathBuf::from("/foo/bar/up.sql"),
                String::from("ALTER TABLE oximeter.measurements_bool DELETE WHERE timestamp < NOW();")
            ),
        );
        assert!(Client::verify_schema_upgrades(&map).is_err());

        // Check that we fail if the upgrade contains multiple SQL statements.
        map.clear();
        map.insert(
            "up".into(),
            (
                PathBuf::from("/foo/bar/up.sql"),
                String::from(
                    "\
                    ALTER TABLE oximeter.measurements_bool \
                        ADD COLUMN foo UInt8; \
                    ALTER TABLE oximeter.measurements_bool \
                        ADD COLUMN bar UInt8; \
                    ",
                ),
            ),
        );
        assert!(Client::verify_schema_upgrades(&map).is_err());
    }

    // Regression test for https://github.com/oxidecomputer/omicron/issues/4369.
    //
    // This tests that we can successfully query all extant field types from the
    // schema table. There may be no such values, but the query itself should
    // succeed.
    #[tokio::test]
    async fn test_select_all_field_types() {
        use strum::IntoEnumIterator;
        usdt::register_probes().unwrap();
        let logctx = test_setup_log("test_select_all_field_types");
        let log = &logctx.log;

        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        let client = Client::new(address, &log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to initialize timeseries database");

        // Attempt to select all schema with each field type.
        for ty in oximeter::FieldType::iter() {
            let sql = format!(
                "SELECT COUNT() \
                FROM {}.timeseries_schema \
                WHERE arrayFirstIndex(x -> x = '{:?}', fields.type) > 0;",
                crate::DATABASE_NAME,
                crate::model::DbFieldType::from(ty),
            );
            let res = client.execute_with_body(sql).await.unwrap().1;
            let count = res.trim().parse::<usize>().unwrap();
            assert_eq!(count, 0);
        }
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[cfg(any(feature = "sql", test))]
    #[tokio::test]
    async fn test_sql_query_output() {
        let logctx = test_setup_log("test_sql_query_output");
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let address = db.http_address().into();
        let client = Client::new(address, &log);
        client
            .initialize_db_with_version(false, OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");
        let (_target, metrics, samples) = setup_select_test();
        client.insert_samples(&samples).await.unwrap();

        // Sanity check that we get exactly the number of samples we expected.
        let res = client
            .query("SELECT count() AS total FROM service:request_latency")
            .await
            .unwrap();
        assert_eq!(res.table.rows.len(), 1);
        let serde_json::Value::Number(n) = &res.table.rows[0][0] else {
            panic!("Expected exactly 1 row with 1 item");
        };
        assert_eq!(n.as_u64().unwrap(), samples.len() as u64);

        // Assert grouping by the keys results in exactly the number of samples
        // expected for each timeseries.
        let res = client
            .query(
                "SELECT count() AS total \
                FROM service:request_latency \
                GROUP BY timeseries_key; \
            ",
            )
            .await
            .unwrap();
        assert_eq!(res.table.rows.len(), metrics.len());
        for row in res.table.rows.iter() {
            assert_eq!(row.len(), 1);
            let serde_json::Value::Number(n) = &row[0] else {
                panic!("Expected a number in each row");
            };
            assert_eq!(
                n.as_u64().unwrap(),
                (samples.len() / metrics.len()) as u64
            );
        }

        // Read test SQL and make sure we're getting expected results.
        let sql_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test-output")
            .join("sql");
        let mut rd = tokio::fs::read_dir(&sql_dir)
            .await
            .expect("failed to read SQL test directory");
        while let Some(next_entry) =
            rd.next_entry().await.expect("failed to read directory entry")
        {
            let sql_file = next_entry.path().join("query.sql");
            let result_file = next_entry.path().join("result.txt");
            let query = tokio::fs::read_to_string(&sql_file)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "failed to read test SQL query in '{}",
                        sql_file.display()
                    )
                });
            let res = client
                .query(&query)
                .await
                .expect("failed to execute test query");
            expectorate::assert_contents(
                result_file,
                &serde_json::to_string_pretty(&res.table).unwrap(),
            );
        }
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // The schema directory, used in tests. The actual updater uses the
    // zone-image files copied in during construction.
    const SCHEMA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/schema");

    #[tokio::test]
    async fn check_actual_schema_upgrades_are_valid_single_node() {
        check_actual_schema_upgrades_are_valid_impl(false).await;
    }

    #[tokio::test]
    async fn check_actual_schema_upgrades_are_valid_replicated() {
        check_actual_schema_upgrades_are_valid_impl(true).await;
    }

    // NOTE: This does not actually run the upgrades, only checks them for
    // validity.
    async fn check_actual_schema_upgrades_are_valid_impl(replicated: bool) {
        let name = format!(
            "check_actual_schema_upgrades_are_valid_{}",
            if replicated { "replicated" } else { "single_node" }
        );
        let logctx = test_setup_log(&name);
        let log = &logctx.log;

        // We really started tracking the database version in 2. However, that
        // set of files is not valid by construction, since we were just
        // creating the full database from scratch as an "upgrade". So we'll
        // start by applying version 3, and then do all later ones.
        const FIRST_VERSION: u64 = 3;
        for version in FIRST_VERSION..=OXIMETER_VERSION {
            let upgrade_file_contents = Client::read_schema_upgrade_sql_files(
                log, replicated, version, SCHEMA_DIR,
            )
            .await
            .expect("failed to read schema upgrade files");

            if let Err(e) =
                Client::verify_schema_upgrades(&upgrade_file_contents)
            {
                panic!(
                    "Schema update files for version {version} \
                    are not valid: {e:?}"
                );
            }
        }
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn check_db_init_is_sum_of_all_up_single_node() {
        check_db_init_is_sum_of_all_up_impl(false).await;
    }

    #[tokio::test]
    async fn check_db_init_is_sum_of_all_up_replicated() {
        check_db_init_is_sum_of_all_up_impl(true).await;
    }

    // Check that the set of tables we arrive at through upgrades equals those
    // we get by creating the latest version directly.
    async fn check_db_init_is_sum_of_all_up_impl(replicated: bool) {
        let name = format!(
            "check_db_init_is_sum_of_all_up_{}",
            if replicated { "replicated" } else { "single_node" }
        );
        let logctx = test_setup_log(&name);
        let log = &logctx.log;
        let mut db = if replicated {
            create_cluster(&logctx).await
        } else {
            ClickHouseDeployment::new_single_node(&logctx)
                .await
                .expect("Failed to start ClickHouse")
        };
        let client = Client::new(db.http_address().into(), &log);

        // Let's start with version 2, which is the first tracked and contains
        // the full SQL files we need to populate the DB.
        client
            .initialize_db_with_version(replicated, 2)
            .await
            .expect("Failed to initialize timeseries database");

        // Now let's apply all the SQL updates from here to the latest.
        for version in 3..=OXIMETER_VERSION {
            client
                .ensure_schema(replicated, version, SCHEMA_DIR)
                .await
                .expect("Failed to ensure schema");
        }

        // Fetch all the tables as a JSON blob.
        let tables_through_upgrades =
            fetch_oximeter_table_details(&client).await;

        // We'll completely re-init the DB with the real version now.
        if replicated {
            client.wipe_replicated_db().await.unwrap()
        } else {
            client.wipe_single_node_db().await.unwrap()
        }
        client
            .initialize_db_with_version(replicated, OXIMETER_VERSION)
            .await
            .expect("Failed to initialize timeseries database");

        // Fetch the tables again and compare.
        let tables = fetch_oximeter_table_details(&client).await;

        // This is an annoying comparison. Since the tables are quite
        // complicated, we want to really be careful about what errors we show.
        // Iterate through all the expected tables (from the direct creation),
        // and check each expected field matches. Then we also check that the
        // tables from the upgrade path don't have anything else in them.
        for (name, json) in tables.iter() {
            let upgrade_table =
                tables_through_upgrades.get(name).unwrap_or_else(|| {
                    panic!("The tables via upgrade are missing table '{name}'")
                });
            for (key, value) in json.iter() {
                let other_value = upgrade_table.get(key).unwrap_or_else(|| {
                    panic!("Upgrade table is missing key '{key}'")
                });
                assert_eq!(
                    value,
                    other_value,
                    "{} database table {name} disagree on the value \
                    of the column {key} between the direct table creation \
                    and the upgrade path.\nDirect:\n\n{value} \
                    \n\nUpgrade:\n\n{other_value}",
                    if replicated { "Replicated" } else { "Single-node" },
                );
            }
        }

        // Check there are zero keys in the upgrade path that don't appear in
        // the direct path.
        let extra_keys: Vec<_> = tables_through_upgrades
            .keys()
            .filter(|k| !tables.contains_key(k.as_str()))
            .cloned()
            .collect();
        assert!(
            extra_keys.is_empty(),
            "The oximeter database contains tables in the upgrade path \
            that are not in the direct path: {extra_keys:?}"
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Read the relevant table details from the `oximeter` database, and return
    // it keyed on the table name.
    async fn fetch_oximeter_table_details(
        client: &Client,
    ) -> BTreeMap<String, serde_json::Map<String, serde_json::Value>> {
        let out = client
            .execute_with_body(
                "SELECT \
                name,
                engine_full,
                create_table_query,
                sorting_key,
                primary_key
            FROM system.tables \
            WHERE database = 'oximeter'\
            FORMAT JSONEachRow;",
            )
            .await
            .unwrap()
            .1;
        out.lines()
            .map(|line| {
                let json: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(&line).unwrap();
                let name = json.get("name").unwrap().to_string();
                (name, json)
            })
            .collect()
    }

    // Helper to write a test file containing timeseries to delete.
    async fn write_timeseries_to_delete_file(
        schema_dir: &Path,
        replicated: bool,
        version: u64,
        names: &[TimeseriesName],
    ) {
        let subdir = schema_dir
            .join(if replicated { "replicated" } else { "single-node" })
            .join(version.to_string());
        tokio::fs::create_dir_all(&subdir)
            .await
            .expect("failed to make subdirectories");
        let filename = subdir.join(crate::TIMESERIES_TO_DELETE_FILE);
        let contents = names
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        tokio::fs::write(&filename, contents)
            .await
            .expect("failed to write test timeseries to delete file");
    }

    #[tokio::test]
    async fn test_read_timeseries_to_delete() {
        let names: Vec<TimeseriesName> =
            vec!["a:b".parse().unwrap(), "c:d".parse().unwrap()];
        let schema_dir =
            tempfile::TempDir::new().expect("failed to make temp dir");
        const VERSION: u64 = 7;
        write_timeseries_to_delete_file(
            schema_dir.path(),
            false,
            VERSION,
            &names,
        )
        .await;
        let read = Client::read_timeseries_to_delete(
            false,
            VERSION,
            schema_dir.path(),
        )
        .await
        .expect("Failed to read timeseries to delete");
        assert_eq!(names, read, "Read incorrect list of timeseries to delete",);
    }

    #[tokio::test]
    async fn test_read_timeseries_to_delete_empty_file_is_ok() {
        let schema_dir =
            tempfile::TempDir::new().expect("failed to make temp dir");
        const VERSION: u64 = 7;
        write_timeseries_to_delete_file(schema_dir.path(), false, VERSION, &[])
            .await;
        let read = Client::read_timeseries_to_delete(
            false,
            VERSION,
            schema_dir.path(),
        )
        .await
        .expect("Failed to read timeseries to delete");
        assert!(read.is_empty(), "Read incorrect list of timeseries to delete",);
    }

    #[tokio::test]
    async fn test_read_timeseries_to_delete_nonexistent_file_is_ok() {
        let path = PathBuf::from("/this/file/better/not/exist");
        let read = Client::read_timeseries_to_delete(false, 1000000, &path)
            .await
            .expect("Failed to read timeseries to delete");
        assert!(read.is_empty(), "Read incorrect list of timeseries to delete",);
    }

    #[tokio::test]
    async fn test_expunge_timeseries_by_name_single_node() {
        const TEST_NAME: &str = "test_expunge_timeseries_by_name_single_node";
        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        test_expunge_timeseries_by_name_impl(
            log,
            db.http_address().into(),
            false,
        )
        .await;
        db.cleanup().await.expect("Failed to cleanup ClickHouse server");
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_expunge_timeseries_by_name_replicated() {
        const TEST_NAME: &str = "test_expunge_timeseries_by_name_replicated";
        let logctx = test_setup_log(TEST_NAME);
        let mut cluster = create_cluster(&logctx).await;
        let address = cluster.http_address().into();
        test_expunge_timeseries_by_name_impl(&logctx.log, address, true).await;
        cluster.cleanup().await.expect("Failed to cleanup ClickHouse cluster");
        logctx.cleanup_successful();
    }

    // Implementation of the test for expunging timeseries by name during an
    // upgrade.
    async fn test_expunge_timeseries_by_name_impl(
        log: &Logger,
        address: SocketAddr,
        replicated: bool,
    ) {
        usdt::register_probes().unwrap();
        let client = Client::new(address, &log);

        const STARTING_VERSION: u64 = 1;
        const NEXT_VERSION: u64 = 2;
        const VERSIONS: [u64; 2] = [STARTING_VERSION, NEXT_VERSION];

        // We need to actually have the oximeter DB here, and the version table,
        // since `ensure_schema()` writes out versions to the DB as they're
        // applied.
        client
            .initialize_db_with_version(replicated, STARTING_VERSION)
            .await
            .expect("failed to initialize test DB");

        // Let's insert a few samples from two different timeseries. The
        // timeseries share some field types and have others that are distinct
        // between them, so that we can test that we don't touch tables we
        // shouldn't, and only delete the parts we should.
        let samples = generate_expunge_timeseries_samples(4);
        client
            .insert_samples(&samples)
            .await
            .expect("failed to insert test samples");
        let all_timeseries: BTreeSet<TimeseriesName> = samples
            .iter()
            .map(|s| s.timeseries_name.parse().unwrap())
            .collect();
        assert_eq!(all_timeseries.len(), 2);

        // Count the number of records in all tables, by timeseries.
        let mut records_by_timeseries: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let all_tables = client
            .list_oximeter_database_tables(ListDetails {
                include_version: false,
                replicated,
            })
            .await
            .unwrap();
        for table in all_tables.iter() {
            let sql = format!(
                "SELECT * FROM {}.{} FORMAT JSONEachRow",
                crate::DATABASE_NAME,
                table,
            );
            let body = client.execute_with_body(sql).await.unwrap().1;
            for line in body.lines() {
                let json: serde_json::Value =
                    serde_json::from_str(line.trim()).unwrap();
                let name = json["timeseries_name"].to_string();
                records_by_timeseries.entry(name).or_default().push(json);
            }
        }

        // Even though we don't need SQL, we need the directory for the first
        // version too.
        let (schema_dir, _version_dirs) =
            create_test_upgrade_schema_directory(replicated, &VERSIONS).await;

        // We don't actually need any SQL files in the version we're upgrading
        // to. The function `ensure_schema` will apply any SQL and any
        // timeseries to be deleted independently. We're just testing the
        // latter.
        let to_delete = vec![all_timeseries.first().unwrap().clone()];
        write_timeseries_to_delete_file(
            schema_dir.path(),
            replicated,
            NEXT_VERSION,
            &to_delete,
        )
        .await;

        // Let's run the "schema upgrade", which should only delete these
        // particular timeseries.
        client
            .ensure_schema(replicated, NEXT_VERSION, schema_dir.path())
            .await
            .unwrap();

        // Look over all tables.
        //
        // First, we should have zero mentions of the timeseries we've deleted.
        for table in all_tables.iter() {
            let sql = format!(
                "SELECT COUNT() \
                FROM {}.{} \
                WHERE timeseries_name = '{}'
                FORMAT CSV",
                crate::DATABASE_NAME,
                table,
                &to_delete[0].to_string(),
            );
            let count: u64 = client
                .execute_with_body(sql)
                .await
                .expect("failed to get count of timeseries")
                .1
                .trim()
                .parse()
                .expect("invalid record count from query");
            assert_eq!(
                count, 0,
                "Should not have any rows associated with the deleted \
                but found {count} records in table {table}",
            );
        }

        // We should also still have all the records from the timeseries that we
        // did _not_ expunge.
        let mut found: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for table in all_tables.iter() {
            let sql = format!(
                "SELECT * FROM {}.{} FORMAT JSONEachRow",
                crate::DATABASE_NAME,
                table,
            );
            let body = client.execute_with_body(sql).await.unwrap().1;
            for line in body.lines() {
                let json: serde_json::Value =
                    serde_json::from_str(line.trim()).unwrap();
                let name = json["timeseries_name"].to_string();
                found.entry(name).or_default().push(json);
            }
        }

        // Check that all records we found exist in the previous set of found
        // records, and that they are identical.
        for (name, records) in found.iter() {
            let existing_records = records_by_timeseries
                .get(name)
                .expect("expected to find previous records for timeseries");
            assert_eq!(
                records, existing_records,
                "Some records from timeseries {name} were removed, \
                but should not have been"
            );
        }
    }

    fn generate_expunge_timeseries_samples(
        n_samples_per_timeseries: u64,
    ) -> Vec<Sample> {
        #[derive(oximeter::Target)]
        struct FirstTarget {
            first_field: String,
            second_field: Uuid,
        }

        #[derive(oximeter::Target)]
        struct SecondTarget {
            first_field: String,
            second_field: bool,
        }

        #[derive(oximeter::Metric)]
        struct SharedMetric {
            datum: u64,
        }

        let ft = FirstTarget {
            first_field: String::from("foo"),
            second_field: Uuid::new_v4(),
        };
        let st = SecondTarget {
            first_field: String::from("foo"),
            second_field: false,
        };
        let mut m = SharedMetric { datum: 0 };

        let mut out = Vec::with_capacity(2 * n_samples_per_timeseries as usize);
        for i in 0..n_samples_per_timeseries {
            m.datum = i;
            out.push(Sample::new(&ft, &m).unwrap());
        }
        for i in n_samples_per_timeseries..(2 * n_samples_per_timeseries) {
            m.datum = i;
            out.push(Sample::new(&st, &m).unwrap());
        }
        out
    }
}
