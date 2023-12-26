// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use dropshot::test_util::LogContext;
use nexus_db_model::schema::SCHEMA_VERSION as LATEST_SCHEMA_VERSION;
use nexus_db_queries::db::datastore::{
    all_sql_for_version_migration, EARLIEST_SUPPORTED_VERSION,
};
use nexus_test_utils::{db, load_test_config, ControlPlaneTestContextBuilder};
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::nexus_config::Config;
use omicron_common::nexus_config::SchemaConfig;
use omicron_test_utils::dev::db::CockroachInstance;
use pretty_assertions::{assert_eq, assert_ne};
use similar_asserts;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use tokio::time::timeout;
use tokio::time::Duration;
use uuid::Uuid;

const SCHEMA_DIR: &'static str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../schema/crdb");

async fn test_setup_just_crdb<'a>(
    log: &Logger,
    populate: bool,
) -> CockroachInstance {
    // Start up CockroachDB.
    let database = if populate {
        db::test_setup_database(log).await
    } else {
        db::test_setup_database_empty(log).await
    };
    database
}

// Helper to ensure we perform the same setup for the positive and negative test
// cases.
async fn test_setup<'a>(
    config: &'a mut Config,
    name: &'static str,
) -> ControlPlaneTestContextBuilder<'a, omicron_nexus::Server> {
    let mut builder =
        ControlPlaneTestContextBuilder::<omicron_nexus::Server>::new(
            name, config,
        );
    let populate = false;
    builder.start_crdb(populate).await;
    let schema_dir = PathBuf::from(SCHEMA_DIR);
    builder.config.pkg.schema = Some(SchemaConfig { schema_dir });
    builder.start_internal_dns().await;
    builder.start_external_dns().await;
    builder.start_dendrite(SwitchLocation::Switch0).await;
    builder.start_dendrite(SwitchLocation::Switch1).await;
    builder.start_mgd(SwitchLocation::Switch0).await;
    builder.start_mgd(SwitchLocation::Switch1).await;
    builder.populate_internal_dns().await;
    builder
}

// Attempts to apply an update as a transaction.
//
// Only returns an error if the transaction failed to commit.
async fn apply_update_as_transaction_inner(
    client: &omicron_test_utils::dev::db::Client,
    sql: &str,
) -> Result<(), tokio_postgres::Error> {
    client.batch_execute("BEGIN;").await.expect("Failed to BEGIN transaction");
    client.batch_execute(&sql).await.expect("Failed to execute update");
    client.batch_execute("COMMIT;").await?;
    Ok(())
}

// Applies an update as a transaction.
//
// Automatically retries transactions that can be retried client-side.
async fn apply_update_as_transaction(
    log: &Logger,
    client: &omicron_test_utils::dev::db::Client,
    sql: &str,
) {
    loop {
        match apply_update_as_transaction_inner(client, sql).await {
            Ok(()) => break,
            Err(err) => {
                warn!(log, "Failed to apply update as transaction"; "err" => err.to_string());
                client
                    .batch_execute("ROLLBACK;")
                    .await
                    .expect("Failed to ROLLBACK failed transaction");
                if let Some(code) = err.code() {
                    if code == &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE {
                        warn!(log, "Transaction retrying");
                        continue;
                    }
                }
                panic!("Failed to apply update: {err}");
            }
        }
    }
}

async fn apply_update(
    log: &Logger,
    crdb: &CockroachInstance,
    version: &str,
    times_to_apply: usize,
) {
    let log = log.new(o!("target version" => version.to_string()));
    info!(log, "Performing upgrade");

    let client = crdb.connect().await.expect("failed to connect");

    // We skip this for the earliest supported version because these tables
    // might not exist yet.
    if version != EARLIEST_SUPPORTED_VERSION {
        info!(log, "Updating schema version in db_metadata (setting target)");
        let sql = format!("UPDATE omicron.public.db_metadata SET target_version = '{}' WHERE singleton = true;", version);
        client
            .batch_execute(&sql)
            .await
            .expect("Failed to bump version number");
    }

    let target_dir = Utf8PathBuf::from(SCHEMA_DIR).join(version);
    let schema_change =
        all_sql_for_version_migration(&target_dir).await.unwrap();

    for _ in 0..times_to_apply {
        for nexus_db_queries::db::datastore::SchemaUpgradeStep { path, sql } in
            &schema_change.steps
        {
            info!(log, "Applying sql schema upgrade step"; "path" => path.to_string());
            apply_update_as_transaction(&log, &client, sql).await;
        }
    }

    // Normally, Nexus actually bumps the version number.
    //
    // We do so explicitly here.
    info!(log, "Updating schema version in db_metadata (removing target)");
    let sql = format!("UPDATE omicron.public.db_metadata SET version = '{}', target_version = NULL WHERE singleton = true;", version);
    client.batch_execute(&sql).await.expect("Failed to bump version number");

    client.cleanup().await.expect("cleaning up after wipe");
    info!(log, "Update to {version} applied successfully");
}

async fn query_crdb_schema_version(crdb: &CockroachInstance) -> String {
    let client = crdb.connect().await.expect("failed to connect");
    let sql =
        "SELECT version FROM omicron.public.db_metadata WHERE singleton = true";

    let row = client.query_one(sql, &[]).await.expect("failed to query schema");
    let version = row.get(0);
    client.cleanup().await.expect("cleaning up after wipe");
    version
}

// A newtype wrapper around a string, which allows us to more liberally
// interpret SQL types.
//
// Note that for the purposes of schema comparisons, we don't care about parsing
// the contents of the database, merely the schema and equality of contained data.
#[derive(PartialEq, Clone, Debug)]
enum AnySqlType {
    DateTime,
    String(String),
    Bool(bool),
    Uuid(Uuid),
    Int8(i64),
    Float4(f32),
    TextArray(Vec<String>),
    // TODO: This isn't exhaustive, feel free to add more.
    //
    // These should only be necessary for rows where the database schema changes also choose to
    // populate data.
}

impl AnySqlType {
    fn as_str(&self) -> &str {
        match self {
            AnySqlType::String(s) => s,
            _ => panic!("Not a string type"),
        }
    }
}

impl<'a> tokio_postgres::types::FromSql<'a> for AnySqlType {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        if String::accepts(ty) {
            return Ok(AnySqlType::String(String::from_sql(ty, raw)?));
        }
        if DateTime::<Utc>::accepts(ty) {
            // We intentionally drop the time here -- we only care that there
            // is some value present.
            let _ = DateTime::<Utc>::from_sql(ty, raw)?;
            return Ok(AnySqlType::DateTime);
        }
        if bool::accepts(ty) {
            return Ok(AnySqlType::Bool(bool::from_sql(ty, raw)?));
        }
        if Uuid::accepts(ty) {
            return Ok(AnySqlType::Uuid(Uuid::from_sql(ty, raw)?));
        }
        if i64::accepts(ty) {
            return Ok(AnySqlType::Int8(i64::from_sql(ty, raw)?));
        }
        if f32::accepts(ty) {
            return Ok(AnySqlType::Float4(f32::from_sql(ty, raw)?));
        }
        if Vec::<String>::accepts(ty) {
            return Ok(AnySqlType::TextArray(Vec::<String>::from_sql(
                ty, raw,
            )?));
        }
        Err(anyhow::anyhow!(
            "Cannot parse type {ty}. If you're trying to use this type in a table which is populated \
during a schema migration, consider adding it to `AnySqlType`."
            ).into())
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}

#[derive(PartialEq, Debug)]
struct NamedSqlValue {
    // It's a little redunant to include the column name alongside each value,
    // but it results in a prettier diff.
    column: String,
    value: Option<AnySqlType>,
}

impl NamedSqlValue {
    fn expect(&self, column: &str) -> Option<&AnySqlType> {
        assert_eq!(self.column, column);
        self.value.as_ref()
    }
}

// A generic representation of a row of SQL data
#[derive(PartialEq, Debug)]
struct Row {
    values: Vec<NamedSqlValue>,
}

impl Row {
    fn new() -> Self {
        Self { values: vec![] }
    }
}

enum ColumnSelector<'a> {
    ByName(&'a [&'static str]),
    Star,
}

impl<'a> From<&'a [&'static str]> for ColumnSelector<'a> {
    fn from(columns: &'a [&'static str]) -> Self {
        Self::ByName(columns)
    }
}

fn process_rows(rows: &Vec<tokio_postgres::Row>) -> Vec<Row> {
    let mut result = vec![];
    for row in rows {
        let mut row_result = Row::new();
        for i in 0..row.len() {
            let column_name = row.columns()[i].name();
            row_result.values.push(NamedSqlValue {
                column: column_name.to_string(),
                value: row.get(i),
            });
        }
        result.push(row_result);
    }
    result
}

async fn crdb_show_constraints(
    crdb: &CockroachInstance,
    table: &str,
) -> Vec<Row> {
    let client = crdb.connect().await.expect("failed to connect");

    let sql = format!("SHOW CONSTRAINTS FROM {table}");
    let rows = client
        .query(&sql, &[])
        .await
        .unwrap_or_else(|_| panic!("failed to query {table}"));
    client.cleanup().await.expect("cleaning up after wipe");

    process_rows(&rows)
}

async fn crdb_select(
    crdb: &CockroachInstance,
    columns: ColumnSelector<'_>,
    table: &str,
    constraints: Option<&str>,
) -> Vec<Row> {
    let client = crdb.connect().await.expect("failed to connect");
    let constraints = if let Some(constraints) = constraints {
        format!("WHERE {constraints}")
    } else {
        "".to_string()
    };

    let cols = match &columns {
        ColumnSelector::ByName(columns) => columns.join(","),
        ColumnSelector::Star => "*".to_string(),
    };

    // We insert the ORDER BY as a simple mechanism to ensure that we're
    // comparing equivalent data. We care about the contents of the retreived
    // rows, so normalize the order in which they are returned.
    let order = match &columns {
        ColumnSelector::ByName(_) => cols.clone(),
        ColumnSelector::Star => format!("PRIMARY KEY {table}"),
    };

    let sql =
        format!("SELECT {cols} FROM {table} {constraints} ORDER BY {order}");
    let rows = client
        .query(&sql, &[])
        .await
        .unwrap_or_else(|_| panic!("failed to query {table}"));
    client.cleanup().await.expect("cleaning up after wipe");

    process_rows(&rows)
}

async fn crdb_list_enums(crdb: &CockroachInstance) -> Vec<Row> {
    let client = crdb.connect().await.expect("failed to connect");

    // https://www.cockroachlabs.com/docs/stable/show-enums
    let rows = client
        .query("show enums;", &[])
        .await
        .unwrap_or_else(|_| panic!("failed to list enums"));
    client.cleanup().await.expect("cleaning up after wipe");

    process_rows(&rows)
}

async fn read_all_schema_versions() -> BTreeSet<SemverVersion> {
    let mut all_versions = BTreeSet::new();

    let mut dir =
        tokio::fs::read_dir(SCHEMA_DIR).await.expect("Access schema dir");
    while let Some(entry) = dir.next_entry().await.expect("Read dirent") {
        if entry.file_type().await.unwrap().is_dir() {
            let name = entry.file_name().into_string().unwrap();
            if let Ok(observed_version) = name.parse::<SemverVersion>() {
                all_versions.insert(observed_version);
            }
        }
    }
    all_versions
}

// This test confirms the following behavior:
//
// - Nexus can boot using a "1.0.0" revision of the schema
// - Nexus can automatically apply all subsequent updates automatically
// - This should eventually drive Nexus to the latest revision of the schema,
// such that it can boot.
#[tokio::test]
async fn nexus_applies_update_on_boot() {
    let mut config = load_test_config();
    let mut builder =
        test_setup(&mut config, "nexus_applies_update_on_boot").await;
    let log = &builder.logctx.log;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    // We started with an empty database -- apply an update here to bring
    // us forward to our oldest supported schema version before trying to boot nexus.
    apply_update(log, &crdb, EARLIEST_SUPPORTED_VERSION, 1).await;
    assert_eq!(
        EARLIEST_SUPPORTED_VERSION,
        query_crdb_schema_version(&crdb).await
    );

    // Start Nexus. It should auto-format itself to the latest version,
    // upgrading through each intermediate update.
    //
    // NOTE: If this grows excessively, we could break it into several smaller
    // tests.
    assert!(
        timeout(Duration::from_secs(60), builder.start_nexus_internal())
            .await
            .is_ok(),
        "Nexus should have started"
    );

    // After Nexus boots, it should have upgraded to the latest schema.
    let crdb = builder.database.as_ref().expect("Should have started CRDB");
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    builder.teardown().await;
}

// Confirm that "dbinit.sql" results in the same semver version in the DB as the one embedded into
// the Nexus binary.
#[tokio::test]
async fn dbinit_version_matches_version_known_to_nexus() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "dbinit_version_matches_version_known_to_nexus",
        &config.pkg.log,
    );
    let log = &logctx.log;
    let populate = true;
    let mut crdb = test_setup_just_crdb(&log, populate).await;

    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

// This test verifies that booting with an unknown version prevents us from
// applying any updates.
#[tokio::test]
async fn nexus_cannot_apply_update_from_unknown_version() {
    let mut config = load_test_config();
    let mut builder = test_setup(
        &mut config,
        "nexus_cannot_apply_update_from_unknown_version",
    )
    .await;
    let log = &builder.logctx.log;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    apply_update(log, &crdb, EARLIEST_SUPPORTED_VERSION, 1).await;
    assert_eq!(
        EARLIEST_SUPPORTED_VERSION,
        query_crdb_schema_version(&crdb).await
    );

    // This version is not valid; it does not exist.
    let version = "0.0.0";
    crdb.connect().await.expect("Failed to connect")
        .batch_execute(&format!("UPDATE omicron.public.db_metadata SET version = '{version}' WHERE singleton = true"))
        .await
        .expect("Failed to update schema");

    assert!(
        timeout(Duration::from_secs(15), builder.start_nexus_internal())
            .await
            .is_err(),
        "Nexus should not have started"
    );

    // The version remains invalid.
    let crdb = builder.database.as_ref().expect("Should have started CRDB");
    assert_eq!("0.0.0", query_crdb_schema_version(&crdb).await);

    builder.teardown().await;
}

// This test verifies that executing all schemas, in order, twice, still
// correctly performs an upgrade.
//
// This attempts to be a rough approximation for multiple Nexuses each
// simultaneously executing these operations.
#[tokio::test]
async fn versions_have_idempotent_up() {
    let config = load_test_config();
    let logctx =
        LogContext::new("versions_have_idempotent_up", &config.pkg.log);
    let log = &logctx.log;
    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;

    let all_versions = read_all_schema_versions().await;

    for version in &all_versions {
        apply_update(log, &crdb, &version.to_string(), 2).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

const COLUMNS: [&'static str; 7] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
];

const CONSTRAINT_COLUMN_USAGE: [&'static str; 7] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "constraint_catalog",
    "constraint_schema",
    "constraint_name",
];

const KEY_COLUMN_USAGE: [&'static str; 7] = [
    "constraint_catalog",
    "constraint_schema",
    "constraint_name",
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
];

const REFERENTIAL_CONSTRAINTS: [&'static str; 8] = [
    "constraint_catalog",
    "constraint_schema",
    "constraint_name",
    "unique_constraint_schema",
    "unique_constraint_name",
    "match_option",
    "table_name",
    "referenced_table_name",
];

const VIEWS: [&'static str; 4] =
    ["table_catalog", "table_schema", "table_name", "view_definition"];

const STATISTICS: [&'static str; 11] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "non_unique",
    "index_schema",
    "index_name",
    "seq_in_index",
    "column_name",
    "direction",
    "storing",
    "implicit",
];

const SEQUENCES: [&'static str; 12] = [
    "sequence_catalog",
    "sequence_schema",
    "sequence_name",
    "data_type",
    "numeric_precision",
    "numeric_precision_radix",
    "numeric_scale",
    "start_value",
    "minimum_value",
    "maximum_value",
    "increment",
    "cycle_option",
];

const PG_INDEXES: [&'static str; 5] =
    ["schemaname", "tablename", "indexname", "tablespace", "indexdef"];

const TABLES: [&'static str; 4] =
    ["table_catalog", "table_schema", "table_name", "table_type"];

#[derive(PartialEq, Debug)]
struct InformationSchema {
    columns: Vec<Row>,
    constraint_column_usage: Vec<Row>,
    enums: Vec<Row>,
    key_column_usage: Vec<Row>,
    referential_constraints: Vec<Row>,
    views: Vec<Row>,
    statistics: Vec<Row>,
    sequences: Vec<Row>,
    pg_indexes: Vec<Row>,
    tables: Vec<Row>,
    table_constraints: BTreeMap<String, Vec<Row>>,
}

impl InformationSchema {
    fn pretty_assert_eq(&self, other: &Self) {
        // similar_asserts gets us nice diff that only includes the relevant context.
        // the columns diff especially needs this: it can be 20k lines otherwise
        similar_asserts::assert_eq!(self.tables, other.tables);
        similar_asserts::assert_eq!(self.columns, other.columns);
        similar_asserts::assert_eq!(
            self.enums,
            other.enums,
            "Enums did not match. Members must have the same order in dbinit.sql and \
            migrations. If a migration adds a member, it should use BEFORE or AFTER \
            to add it in the same order as dbinit.sql."
        );
        similar_asserts::assert_eq!(self.views, other.views);
        similar_asserts::assert_eq!(
            self.table_constraints,
            other.table_constraints
        );
        similar_asserts::assert_eq!(
            self.constraint_column_usage,
            other.constraint_column_usage
        );
        similar_asserts::assert_eq!(
            self.key_column_usage,
            other.key_column_usage
        );
        similar_asserts::assert_eq!(
            self.referential_constraints,
            other.referential_constraints
        );
        similar_asserts::assert_eq!(
            self.statistics,
            other.statistics,
            "Statistics did not match. This often means that in dbinit.sql, a new \
            column was added into the middle of a table rather than to the end. \
            If that is the case:\n\n \
            \
            * Change dbinit.sql to add the column to the end of the table.\n\
            * Update nexus/db-model/src/schema.rs and the corresponding \
            Queryable/Insertable struct with the new column ordering."
        );
        similar_asserts::assert_eq!(self.sequences, other.sequences);
        similar_asserts::assert_eq!(self.pg_indexes, other.pg_indexes);
    }

    async fn new(crdb: &CockroachInstance) -> Self {
        // Refer to:
        // https://www.cockroachlabs.com/docs/v23.1/information-schema
        //
        // For details on each of these tables.
        let columns = crdb_select(
            crdb,
            COLUMNS.as_slice().into(),
            "information_schema.columns",
            Some("table_schema = 'public'"),
        )
        .await;

        let enums = crdb_list_enums(crdb).await;

        let constraint_column_usage = crdb_select(
            crdb,
            CONSTRAINT_COLUMN_USAGE.as_slice().into(),
            "information_schema.constraint_column_usage",
            None,
        )
        .await;

        let key_column_usage = crdb_select(
            crdb,
            KEY_COLUMN_USAGE.as_slice().into(),
            "information_schema.key_column_usage",
            None,
        )
        .await;

        let referential_constraints = crdb_select(
            crdb,
            REFERENTIAL_CONSTRAINTS.as_slice().into(),
            "information_schema.referential_constraints",
            None,
        )
        .await;

        let views = crdb_select(
            crdb,
            VIEWS.as_slice().into(),
            "information_schema.views",
            None,
        )
        .await;

        let statistics = crdb_select(
            crdb,
            STATISTICS.as_slice().into(),
            "information_schema.statistics",
            None,
        )
        .await;

        let sequences = crdb_select(
            crdb,
            SEQUENCES.as_slice().into(),
            "information_schema.sequences",
            None,
        )
        .await;

        let pg_indexes = crdb_select(
            crdb,
            PG_INDEXES.as_slice().into(),
            "pg_indexes",
            Some("schemaname = 'public'"),
        )
        .await;

        let tables = crdb_select(
            crdb,
            TABLES.as_slice().into(),
            "information_schema.tables",
            Some("table_schema = 'public'"),
        )
        .await;

        let table_constraints =
            Self::show_constraints_all_tables(&tables, crdb).await;

        Self {
            columns,
            constraint_column_usage,
            enums,
            key_column_usage,
            referential_constraints,
            views,
            statistics,
            sequences,
            pg_indexes,
            tables,
            table_constraints,
        }
    }

    async fn show_constraints_all_tables(
        tables: &Vec<Row>,
        crdb: &CockroachInstance,
    ) -> BTreeMap<String, Vec<Row>> {
        let mut map = BTreeMap::new();

        for table in tables {
            let table = &table.values;
            let table_catalog =
                table[0].expect("table_catalog").unwrap().as_str();
            let table_schema =
                table[1].expect("table_schema").unwrap().as_str();
            let table_name = table[2].expect("table_name").unwrap().as_str();
            let table_type = table[3].expect("table_type").unwrap().as_str();

            if table_type != "BASE TABLE" {
                continue;
            }

            let table_name =
                format!("{}.{}.{}", table_catalog, table_schema, table_name);
            let rows = crdb_show_constraints(crdb, &table_name).await;
            map.insert(table_name, rows);
        }
        map
    }

    // This would normally be quite an expensive operation, but we expect it'll
    // at least be slightly cheaper for the freshly populated DB, which
    // shouldn't have that many records yet.
    async fn query_all_tables(
        &self,
        log: &Logger,
        crdb: &CockroachInstance,
    ) -> BTreeMap<String, Vec<Row>> {
        let mut map = BTreeMap::new();

        for table in &self.tables {
            let table = &table.values;
            let table_catalog =
                table[0].expect("table_catalog").unwrap().as_str();
            let table_schema =
                table[1].expect("table_schema").unwrap().as_str();
            let table_name = table[2].expect("table_name").unwrap().as_str();
            let table_type = table[3].expect("table_type").unwrap().as_str();

            if table_type != "BASE TABLE" {
                continue;
            }

            let table_name =
                format!("{}.{}.{}", table_catalog, table_schema, table_name);
            info!(log, "Querying table: {table_name}");
            let rows =
                crdb_select(crdb, ColumnSelector::Star, &table_name, None)
                    .await;
            info!(log, "Saw data: {rows:?}");
            map.insert(table_name, rows);
        }

        map
    }
}

// Confirms that the application of all "up.sql" files, in order, is equivalent
// to applying "dbinit.sql", which should represent the latest-known schema.
#[tokio::test]
async fn dbinit_equals_sum_of_all_up() {
    let config = load_test_config();
    let logctx =
        LogContext::new("dbinit_equals_sum_of_all_up", &config.pkg.log);
    let log = &logctx.log;

    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;

    let all_versions = read_all_schema_versions().await;

    // Go from the first version to the latest version.
    for version in &all_versions {
        apply_update(log, &crdb, &version.to_string(), 1).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // Query the newly constructed DB for information about its schema
    let observed_schema = InformationSchema::new(&crdb).await;
    let observed_data = observed_schema.query_all_tables(log, &crdb).await;
    crdb.cleanup().await.unwrap();

    // Create a new DB with data populated from dbinit.sql for comparison
    let populate = true;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;
    let expected_schema = InformationSchema::new(&crdb).await;
    let expected_data = expected_schema.query_all_tables(log, &crdb).await;

    // Validate that the schema is identical
    observed_schema.pretty_assert_eq(&expected_schema);

    assert_eq!(observed_data, expected_data);

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

// Returns the InformationSchema object for a database populated via `sql`.
async fn get_information_schema(log: &Logger, sql: &str) -> InformationSchema {
    let populate = false;
    let mut crdb = test_setup_just_crdb(&log, populate).await;

    let client = crdb.connect().await.expect("failed to connect");
    client.batch_execute(sql).await.expect("failed to apply SQL");

    let observed_schema = InformationSchema::new(&crdb).await;
    crdb.cleanup().await.unwrap();
    observed_schema
}

// Reproduction case for https://github.com/oxidecomputer/omicron/issues/4143
#[tokio::test]
async fn compare_index_creation_differing_where_clause() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "compare_index_creation_differing_where_clause",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let schema1 = get_information_schema(log, "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS lookup_animal_by_name ON omicron.public.animal (
            name, id
        ) WHERE name IS NOT NULL AND time_deleted IS NULL;
    ").await;

    let schema2 = get_information_schema(log, "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS lookup_animal_by_name ON omicron.public.animal (
            name, id
        ) WHERE time_deleted IS NULL;
    ").await;

    // pg_indexes includes a column "indexdef" that compares partial indexes.
    // This should catch the differing "WHERE" clause.
    assert_ne!(schema1.pg_indexes, schema2.pg_indexes);

    logctx.cleanup_successful();
}

// Reproduction case for https://github.com/oxidecomputer/omicron/issues/4143
#[tokio::test]
async fn compare_index_creation_differing_columns() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "compare_index_creation_differing_columns",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let schema1 = get_information_schema(log, "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS lookup_animal_by_name ON omicron.public.animal (
            name
        ) WHERE name IS NOT NULL AND time_deleted IS NULL;
    ").await;

    let schema2 = get_information_schema(log, "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE INDEX IF NOT EXISTS lookup_animal_by_name ON omicron.public.animal (
            name, id
        ) WHERE name IS NOT NULL AND time_deleted IS NULL;
    ").await;

    // "statistics" identifies table indices.
    // These tables should differ in the "implicit" column.
    assert_ne!(schema1.statistics, schema2.statistics);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn compare_view_differing_where_clause() {
    let config = load_test_config();
    let logctx =
        LogContext::new("compare_view_differing_where_clause", &config.pkg.log);
    let log = &logctx.log;

    let schema1 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE VIEW live_view AS
            SELECT animal.id, animal.name
            FROM omicron.public.animal
            WHERE animal.time_deleted IS NOT NULL;
    ",
    )
    .await;

    let schema2 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ
        );

        CREATE VIEW live_view AS
            SELECT animal.id, animal.name
            FROM omicron.public.animal
            WHERE animal.time_deleted IS NOT NULL AND animal.name = 'Thomas';
    ",
    )
    .await;

    assert_ne!(schema1.views, schema2.views);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn compare_sequence_differing_increment() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "compare_sequence_differing_increment",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let schema1 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE SEQUENCE omicron.public.myseq START 1 INCREMENT 1;
    ",
    )
    .await;

    let schema2 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE SEQUENCE omicron.public.myseq START 1 INCREMENT 2;
    ",
    )
    .await;

    assert_ne!(schema1.sequences, schema2.sequences);

    logctx.cleanup_successful();
}

#[tokio::test]
async fn compare_table_differing_constraint() {
    let config = load_test_config();
    let logctx =
        LogContext::new("compare_table_differing_constraint", &config.pkg.log);
    let log = &logctx.log;

    let schema1 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ,

            CONSTRAINT dead_animals_have_names CHECK (
                (time_deleted IS NULL) OR
                (name IS NOT NULL)
            )
        );
    ",
    )
    .await;

    let schema2 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.animal (
            id UUID PRIMARY KEY,
            name TEXT,
            time_deleted TIMESTAMPTZ,

            CONSTRAINT dead_animals_have_names CHECK (
                (time_deleted IS NULL) OR
                (name IS NULL)
            )
        );
    ",
    )
    .await;

    assert_ne!(schema1.table_constraints, schema2.table_constraints);
    logctx.cleanup_successful();
}

#[tokio::test]
async fn compare_table_differing_not_null_order() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "compare_table_differing_not_null_order",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let schema1 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.pet ( id UUID PRIMARY KEY );
        CREATE TABLE omicron.public.employee (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            hobbies TEXT
        );
        ",
    )
    .await;

    let schema2 = get_information_schema(
        log,
        "
        CREATE DATABASE omicron;
        CREATE TABLE omicron.public.employee (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            hobbies TEXT
        );
        CREATE TABLE omicron.public.pet ( id UUID PRIMARY KEY );
        ",
    )
    .await;

    schema1.pretty_assert_eq(&schema2);
    logctx.cleanup_successful();
}
