// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use dropshot::test_util::LogContext;
use futures::future::BoxFuture;
use nexus_config::NexusConfig;
use nexus_config::SchemaConfig;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
use nexus_db_model::SCHEMA_VERSION as LATEST_SCHEMA_VERSION;
use nexus_db_model::{AllSchemaVersions, SchemaVersion};
use nexus_db_queries::db::DISALLOW_FULL_TABLE_SCAN_SQL;
use nexus_test_utils::{db, load_test_config, ControlPlaneTestContextBuilder};
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_test_utils::dev::db::{Client, CockroachInstance};
use pretty_assertions::{assert_eq, assert_ne};
use similar_asserts;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::IpAddr;
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
    config: &'a mut NexusConfig,
    name: &'static str,
) -> ControlPlaneTestContextBuilder<'a, omicron_nexus::Server> {
    let mut builder =
        ControlPlaneTestContextBuilder::<omicron_nexus::Server>::new(
            name, config,
        );
    let populate = false;
    builder.start_crdb(populate).await;
    let schema_dir = Utf8PathBuf::from(SCHEMA_DIR);
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
    version: &SchemaVersion,
    times_to_apply: usize,
) {
    let log = log.new(o!("target version" => version.semver().to_string()));
    info!(log, "Performing upgrade");

    let client = crdb.connect().await.expect("failed to connect");

    client
        .batch_execute(DISALLOW_FULL_TABLE_SCAN_SQL)
        .await
        .expect("failed to disallow full table scans");

    // We skip this for the earliest supported version because these tables
    // might not exist yet.
    if *version.semver() != EARLIEST_SUPPORTED_VERSION {
        info!(log, "Updating schema version in db_metadata (setting target)");
        let sql = format!(
            "UPDATE omicron.public.db_metadata SET target_version = '{}' \
            WHERE singleton = true;",
            version
        );
        client
            .batch_execute(&sql)
            .await
            .expect("Failed to bump version number");
    }

    // Each step is applied `times_to_apply` times, but once we start applying
    // a step within an upgrade, we will not attempt to apply prior steps.
    for step in version.upgrade_steps() {
        info!(
            log,
            "Applying sql schema upgrade step";
            "file" => step.label()
        );

        for _ in 0..times_to_apply {
            apply_update_as_transaction(&log, &client, step.sql()).await;

            // The following is a set of "versions exempt from being
            // re-applied" multiple times. PLEASE AVOID ADDING TO THIS LIST.
            const NOT_IDEMPOTENT_VERSIONS: [semver::Version; 1] = [
                // Why: This calls "ALTER TYPE ... DROP VALUE", which does not
                // support the "IF EXISTS" syntax in CockroachDB.
                //
                // https://github.com/cockroachdb/cockroach/issues/120801
                semver::Version::new(10, 0, 0),
            ];

            if NOT_IDEMPOTENT_VERSIONS.contains(&version.semver().0) {
                break;
            }
        }
    }

    // Normally, Nexus actually bumps the version number.
    //
    // We do so explicitly here.
    info!(log, "Updating schema version in db_metadata (removing target)");
    let sql = format!(
        "UPDATE omicron.public.db_metadata SET version = '{}', \
        target_version = NULL WHERE singleton = true;",
        version
    );
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

#[derive(PartialEq, Clone, Debug)]
struct SqlEnum {
    name: String,
    variant: String,
}

impl From<(&str, &str)> for SqlEnum {
    fn from((name, variant): (&str, &str)) -> Self {
        Self { name: name.to_string(), variant: variant.to_string() }
    }
}

// A newtype wrapper around a string, which allows us to more liberally
// interpret SQL types.
//
// Note that for the purposes of schema comparisons, we don't care about parsing
// the contents of the database, merely the schema and equality of contained
// data.
#[derive(PartialEq, Clone, Debug)]
enum AnySqlType {
    Bool(bool),
    DateTime,
    Enum(SqlEnum),
    Float4(f32),
    Int8(i64),
    Json(serde_json::value::Value),
    String(String),
    TextArray(Vec<String>),
    Uuid(Uuid),
    Inet(IpAddr),
    // TODO: This isn't exhaustive, feel free to add more.
    //
    // These should only be necessary for rows where the database schema changes also choose to
    // populate data.
}

impl From<bool> for AnySqlType {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<SqlEnum> for AnySqlType {
    fn from(value: SqlEnum) -> Self {
        Self::Enum(value)
    }
}

impl From<f32> for AnySqlType {
    fn from(value: f32) -> Self {
        Self::Float4(value)
    }
}

impl From<i64> for AnySqlType {
    fn from(value: i64) -> Self {
        Self::Int8(value)
    }
}

impl From<String> for AnySqlType {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Uuid> for AnySqlType {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<IpAddr> for AnySqlType {
    fn from(value: IpAddr) -> Self {
        Self::Inet(value)
    }
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
        if serde_json::value::Value::accepts(ty) {
            return Ok(AnySqlType::Json(serde_json::value::Value::from_sql(
                ty, raw,
            )?));
        }
        if Vec::<String>::accepts(ty) {
            return Ok(AnySqlType::TextArray(Vec::<String>::from_sql(
                ty, raw,
            )?));
        }
        if IpAddr::accepts(ty) {
            return Ok(AnySqlType::Inet(IpAddr::from_sql(ty, raw)?));
        }

        use tokio_postgres::types::Kind;
        match ty.kind() {
            Kind::Enum(_) => {
                Ok(AnySqlType::Enum(SqlEnum {
                    name: ty.name().to_string(),
                    variant: std::str::from_utf8(raw)?.to_string(),
                }))
            },
            _ => {
                Err(anyhow::anyhow!(
                    "Cannot parse type {ty:?}. \
                    If you're trying to use this type in a table which is populated \
                    during a schema migration, consider adding it to `AnySqlType`."
                    ).into())
            }
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}

// It's a little redunant to include the column name alongside each value,
// but it results in a prettier diff.
#[derive(PartialEq, Debug)]
struct ColumnValue {
    column: String,
    value: Option<AnySqlType>,
}

impl ColumnValue {
    fn new<V: Into<AnySqlType>>(column: &str, value: V) -> Self {
        Self { column: String::from(column), value: Some(value.into()) }
    }

    fn null(column: &str) -> Self {
        Self { column: String::from(column), value: None }
    }

    fn expect(&self, column: &str) -> Option<&AnySqlType> {
        assert_eq!(self.column, column);
        self.value.as_ref()
    }
}

// A generic representation of a row of SQL data
#[derive(PartialEq, Debug)]
struct Row {
    values: Vec<ColumnValue>,
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
            row_result.values.push(ColumnValue {
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

fn read_all_schema_versions() -> AllSchemaVersions {
    AllSchemaVersions::load(camino::Utf8Path::new(SCHEMA_DIR)).unwrap()
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
    // us forward to our oldest supported schema version before trying to boot
    // nexus.
    let all_versions = read_all_schema_versions();
    let earliest = all_versions
        .iter_versions()
        .next()
        .expect("missing earliest schema version");
    apply_update(log, &crdb, earliest, 1).await;
    assert_eq!(
        EARLIEST_SUPPORTED_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // Start Nexus. It should auto-format itself to the latest version,
    // upgrading through each intermediate update.
    //
    // The timeout here is a bit longer than usual (120s vs 60s) because if
    // lots of tests are running at the same time, there can be contention
    // here.
    //
    // NOTE: If this grows excessively, we could break it into several smaller
    // tests.
    assert!(
        timeout(Duration::from_secs(120), builder.start_nexus_internal())
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
    config.pkg.tunables.load_timeout = Some(std::time::Duration::from_secs(15));

    let mut builder = test_setup(
        &mut config,
        "nexus_cannot_apply_update_from_unknown_version",
    )
    .await;
    let log = &builder.logctx.log;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    let all_versions = read_all_schema_versions();
    let earliest = all_versions
        .iter_versions()
        .next()
        .expect("missing earliest schema version");
    apply_update(log, &crdb, earliest, 1).await;
    assert_eq!(
        EARLIEST_SUPPORTED_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // This version is not valid; it does not exist.
    let version = "0.0.0";
    crdb.connect()
        .await
        .expect("Failed to connect")
        .batch_execute(&format!(
            "UPDATE omicron.public.db_metadata SET version = '{version}' \
            WHERE singleton = true"
        ))
        .await
        .expect("Failed to update schema");

    assert!(
        builder.start_nexus_internal().await.is_err(),
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

    let all_versions = read_all_schema_versions();
    for version in all_versions.iter_versions() {
        apply_update(log, &crdb, &version, 2).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );
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

    let all_versions = read_all_schema_versions();

    // Apply the very first schema migration. In particular, this creates the
    // `omicron` database, which allows us to construct a `db::Pool` below.
    for version in all_versions.iter_versions().take(1) {
        apply_update(log, &crdb, version, 1).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );
    }

    // Create a connection pool after we apply the first schema version but
    // before applying the rest, and grab a connection from that pool. We'll use
    // it for an extra check later.
    let pool = nexus_db_queries::db::Pool::new_single_host(
        log,
        &nexus_db_queries::db::Config { url: crdb.pg_config().clone() },
    );
    let conn_from_pool =
        pool.claim().await.expect("failed to get pooled connection");

    // Go from the second version to the latest version.
    for version in all_versions.iter_versions().skip(1) {
        apply_update(log, &crdb, version, 1).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );
    }
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // Query the newly constructed DB for information about its schema
    let observed_schema = InformationSchema::new(&crdb).await;
    let observed_data = observed_schema.query_all_tables(log, &crdb).await;

    // Using the connection we got from the connection pool prior to applying
    // the schema migrations, attempt to insert a sled resource. This involves
    // the `sled_resource_kind` enum, whose OID was changed by the schema
    // migration in version 53.0.0 (by virtue of the enum being dropped and
    // added back with a different set of variants). If the diesel OID cache was
    // populated when we acquired the connection from the pool, this will fail
    // with a `type with ID $NUM does not exist` error.
    {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use nexus_db_model::schema::sled_resource::dsl;
        use nexus_db_model::Resources;
        use nexus_db_model::SledResource;
        use nexus_db_model::SledResourceKind;

        diesel::insert_into(dsl::sled_resource)
            .values(SledResource {
                id: Uuid::new_v4(),
                sled_id: Uuid::new_v4(),
                kind: SledResourceKind::Instance,
                resources: Resources {
                    hardware_threads: 8_u32.into(),
                    rss_ram: 1024_i64.try_into().unwrap(),
                    reservoir_ram: 1024_i64.try_into().unwrap(),
                },
            })
            .execute_async(&*conn_from_pool)
            .await
            .expect("failed to insert - did we poison the OID cache?");
    }
    std::mem::drop(conn_from_pool);
    pool.terminate().await;
    std::mem::drop(pool);
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

type BeforeFn = for<'a> fn(client: &'a Client) -> BoxFuture<'a, ()>;
type AfterFn = for<'a> fn(client: &'a Client) -> BoxFuture<'a, ()>;

// Describes the operations which we might take before and after
// migrations to check that they worked.
struct DataMigrationFns {
    before: Option<BeforeFn>,
    after: AfterFn,
}

// "51F0" -> "Silo"
const SILO1: Uuid = Uuid::from_u128(0x111151F0_5c3d_4647_83b0_8f3515da7be1);
const SILO2: Uuid = Uuid::from_u128(0x222251F0_5c3d_4647_83b0_8f3515da7be1);

// "6001" -> "Pool"
const POOL0: Uuid = Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f3515da7be1);
const POOL1: Uuid = Uuid::from_u128(0x11116001_5c3d_4647_83b0_8f3515da7be1);
const POOL2: Uuid = Uuid::from_u128(0x22226001_5c3d_4647_83b0_8f3515da7be1);
const POOL3: Uuid = Uuid::from_u128(0x33336001_5c3d_4647_83b0_8f3515da7be1);
const POOL4: Uuid = Uuid::from_u128(0x44446001_5c3d_4647_83b0_8f3515da7be1);

// "513D" -> "Sled"
const SLED1: Uuid = Uuid::from_u128(0x1111513d_5c3d_4647_83b0_8f3515da7be1);
const SLED2: Uuid = Uuid::from_u128(0x2222513d_5c3d_4647_83b0_8f3515da7be1);

// "7AC4" -> "Rack"
const RACK1: Uuid = Uuid::from_u128(0x11117ac4_5c3d_4647_83b0_8f3515da7be1);

// "6701" -> "Proj"ect
const PROJECT: Uuid = Uuid::from_u128(0x11116701_5c3d_4647_83b0_8f3515da7be1);

// "1257" -> "Inst"ance
const INSTANCE1: Uuid = Uuid::from_u128(0x11111257_5c3d_4647_83b0_8f3515da7be1);
const INSTANCE2: Uuid = Uuid::from_u128(0x22221257_5c3d_4647_83b0_8f3515da7be1);
const INSTANCE3: Uuid = Uuid::from_u128(0x33331257_5c3d_4647_83b0_8f3515da7be1);
const INSTANCE4: Uuid = Uuid::from_u128(0x44441257_5c3d_4647_83b0_8f3515da7be1);

// "67060115" -> "Prop"olis
const PROPOLIS: Uuid = Uuid::from_u128(0x11116706_5c3d_4647_83b0_8f3515da7be1);

// "7154"-> "Disk"
const DISK1: Uuid = Uuid::from_u128(0x11117154_5c3d_4647_83b0_8f3515da7be1);
const DISK2: Uuid = Uuid::from_u128(0x22227154_5c3d_4647_83b0_8f3515da7be1);
const DISK3: Uuid = Uuid::from_u128(0x33337154_5c3d_4647_83b0_8f3515da7be1);
const DISK4: Uuid = Uuid::from_u128(0x44447154_5c3d_4647_83b0_8f3515da7be1);

// "566F" -> "Vo"lume. V is difficult, OK?
const VOLUME1: Uuid = Uuid::from_u128(0x1111566f_5c3d_4647_83b0_8f3515da7be1);
const VOLUME2: Uuid = Uuid::from_u128(0x2222566f_5c3d_4647_83b0_8f3515da7be1);
const VOLUME3: Uuid = Uuid::from_u128(0x3333566f_5c3d_4647_83b0_8f3515da7be1);
const VOLUME4: Uuid = Uuid::from_u128(0x4444566f_5c3d_4647_83b0_8f3515da7be1);

fn before_23_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        // Create two silos
        client.batch_execute(&format!("INSERT INTO silo
            (id, name, description, time_created, time_modified, time_deleted, discoverable, authentication_mode, user_provision_type, mapped_fleet_roles, rcgen) VALUES
          ('{SILO1}', 'silo1', '', now(), now(), NULL, false, 'local', 'jit', '{{}}', 1),
          ('{SILO2}', 'silo2', '', now(), now(), NULL, false, 'local', 'jit', '{{}}', 1);
        ")).await.expect("Failed to create silo");

        // Create an IP pool for each silo, and a third "fleet pool" which has
        // no corresponding silo.
        client.batch_execute(&format!("INSERT INTO ip_pool
            (id, name, description, time_created, time_modified, time_deleted, rcgen, silo_id, is_default) VALUES
          ('{POOL0}', 'pool2', '', now(), now(), now(), 1, '{SILO2}', true),
          ('{POOL1}', 'pool1', '', now(), now(), NULL, 1, '{SILO1}', true),
          ('{POOL2}', 'pool2', '', now(), now(), NULL, 1, '{SILO2}', false),
          ('{POOL3}', 'pool3', '', now(), now(), NULL, 1, null, true),
          ('{POOL4}', 'pool4', '', now(), now(), NULL, 1, null, false);
        ")).await.expect("Failed to create IP Pool");
    })
}

fn after_23_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        // Confirm that the ip_pool_resource objects have been created
        // by the migration.
        let rows = client
            .query("SELECT * FROM ip_pool_resource ORDER BY ip_pool_id", &[])
            .await
            .expect("Failed to query ip pool resource");
        let ip_pool_resources = process_rows(&rows);

        assert_eq!(ip_pool_resources.len(), 6);

        fn assert_row(
            row: &Vec<ColumnValue>,
            ip_pool_id: Uuid,
            silo_id: Uuid,
            is_default: bool,
        ) {
            let type_silo = SqlEnum::from(("ip_pool_resource_type", "silo"));
            assert_eq!(
                row,
                &vec![
                    ColumnValue::new("ip_pool_id", ip_pool_id),
                    ColumnValue::new("resource_type", type_silo),
                    ColumnValue::new("resource_id", silo_id),
                    ColumnValue::new("is_default", is_default),
                ],
            );
        }

        // pool1 was default on silo1, so gets an entry in the join table
        // reflecting that
        assert_row(&ip_pool_resources[0].values, POOL1, SILO1, true);

        // pool1 was default on silo1, so gets an entry in the join table
        // reflecting that
        assert_row(&ip_pool_resources[1].values, POOL2, SILO2, false);

        // fleet-scoped silos are a little more complicated

        // pool3 was a fleet-level default, so now it's associated with both
        // silos. silo1 had its own default pool as well (pool1), so pool3
        // cannot also be default for silo1. silo2 did not have its own default,
        // so pool3 is default for silo2.
        assert_row(&ip_pool_resources[2].values, POOL3, SILO1, false);
        assert_row(&ip_pool_resources[3].values, POOL3, SILO2, true);

        // fleet-level pool that was not default becomes non-default on all silos
        assert_row(&ip_pool_resources[4].values, POOL4, SILO1, false);
        assert_row(&ip_pool_resources[5].values, POOL4, SILO2, false);
    })
}

fn before_24_0_0(client: &Client) -> BoxFuture<'_, ()> {
    // IP addresses were pulled off dogfood sled 16
    Box::pin(async move {
        // Create two sleds. (SLED2 is marked non_provisionable for
        // after_37_0_1.)
        client
            .batch_execute(&format!(
                "INSERT INTO sled
            (id, time_created, time_modified, time_deleted, rcgen, rack_id,
            is_scrimlet, serial_number, part_number, revision,
            usable_hardware_threads, usable_physical_ram, reservoir_size, ip,
            port, last_used_address, provision_state) VALUES

          ('{SLED1}', now(), now(), NULL, 1, '{RACK1}', true, 'abcd', 'defg',
             '1', 64, 12345678, 77, 'fd00:1122:3344:104::1', 12345,
            'fd00:1122:3344:104::1ac', 'provisionable'),
          ('{SLED2}', now(), now(), NULL, 1, '{RACK1}', false, 'zzzz', 'xxxx',
             '2', 64, 12345678, 77,'fd00:1122:3344:107::1', 12345,
            'fd00:1122:3344:107::d4', 'non_provisionable');
        "
            ))
            .await
            .expect("Failed to create sleds");
    })
}

fn after_24_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        // Confirm that the IP Addresses have the last 2 bytes changed to `0xFFFF`
        let rows = client
            .query("SELECT last_used_address FROM sled ORDER BY id", &[])
            .await
            .expect("Failed to sled last_used_address");
        let last_used_addresses = process_rows(&rows);

        let expected_addr_1: IpAddr =
            "fd00:1122:3344:104::ffff".parse().unwrap();
        let expected_addr_2: IpAddr =
            "fd00:1122:3344:107::ffff".parse().unwrap();

        assert_eq!(
            last_used_addresses[0].values,
            vec![ColumnValue::new("last_used_address", expected_addr_1)]
        );
        assert_eq!(
            last_used_addresses[1].values,
            vec![ColumnValue::new("last_used_address", expected_addr_2)]
        );
    })
}

// This reuses the sleds created in before_24_0_0.
fn after_37_0_1(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        // Confirm that the IP Addresses have the last 2 bytes changed to `0xFFFF`
        let rows = client
            .query("SELECT sled_policy, sled_state FROM sled ORDER BY id", &[])
            .await
            .expect("Failed to select sled policy and state");
        let policy_and_state = process_rows(&rows);

        assert_eq!(
            policy_and_state[0].values,
            vec![
                ColumnValue::new(
                    "sled_policy",
                    SqlEnum::from(("sled_policy", "in_service"))
                ),
                ColumnValue::new(
                    "sled_state",
                    SqlEnum::from(("sled_state", "active"))
                ),
            ]
        );
        assert_eq!(
            policy_and_state[1].values,
            vec![
                ColumnValue::new(
                    "sled_policy",
                    SqlEnum::from(("sled_policy", "no_provision"))
                ),
                ColumnValue::new(
                    "sled_state",
                    SqlEnum::from(("sled_state", "active"))
                ),
            ]
        );
    })
}

fn before_70_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async move {
        client
            .batch_execute(&format!(
                "
        INSERT INTO instance (id, name, description, time_created,
        time_modified, time_deleted, project_id, user_data, state,
        time_state_updated, state_generation, active_propolis_id,
        target_propolis_id, migration_id, ncpus, memory, hostname,
        boot_on_fault, updater_id, updater_gen) VALUES

        ('{INSTANCE1}', 'inst1', '', now(), now(), NULL, '{PROJECT}', '',
        'stopped', now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst1', false,
        NULL, 1),
        ('{INSTANCE2}', 'inst2', '', now(), now(), NULL, '{PROJECT}', '',
        'running', now(), 1, '{PROPOLIS}', NULL, NULL, 2, 1073741824, 'inst2',
        true, NULL, 1),
        ('{INSTANCE3}', 'inst3', '', now(), now(), NULL, '{PROJECT}', '',
        'failed', now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst3', false,
        NULL, 1);
        "
            ))
            .await
            .expect("failed to create instances");

        client
            .batch_execute(&format!(
                "
        INSERT INTO vmm (id, time_created, time_deleted, instance_id, state,
        time_state_updated, state_generation, sled_id, propolis_ip,
        propolis_port) VALUES

        ('{PROPOLIS}', now(), NULL, '{INSTANCE2}', 'running', now(), 1,
        '{SLED1}', 'fd00:1122:3344:200::1', '12400');
                "
            ))
            .await
            .expect("failed to create VMMs");
    })
}

fn after_70_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        let rows = client
            .query("SELECT state FROM instance ORDER BY id", &[])
            .await
            .expect("failed to load instance states");
        let instance_states = process_rows(&rows);

        assert_eq!(
            instance_states[0].values,
            vec![ColumnValue::new(
                "state",
                SqlEnum::from(("instance_state_v2", "no_vmm"))
            )]
        );
        assert_eq!(
            instance_states[1].values,
            vec![ColumnValue::new(
                "state",
                SqlEnum::from(("instance_state_v2", "vmm"))
            )]
        );
        assert_eq!(
            instance_states[2].values,
            vec![ColumnValue::new(
                "state",
                SqlEnum::from(("instance_state_v2", "failed"))
            )]
        );

        let rows = client
            .query("SELECT state FROM vmm ORDER BY id", &[])
            .await
            .expect("failed to load VMM states");
        let vmm_states = process_rows(&rows);

        assert_eq!(
            vmm_states[0].values,
            vec![ColumnValue::new(
                "state",
                SqlEnum::from(("vmm_state", "running"))
            )]
        );
    })
}

fn before_95_0_0(client: &Client) -> BoxFuture<'_, ()> {
    // This reuses the instance records created in `before_70_0_0`
    const COLUMN: &'static str = "boot_on_fault";
    Box::pin(async {
        let rows = client
            .query("SELECT boot_on_fault FROM instance ORDER BY id", &[])
            .await
            .expect("failed to load instance boot_on_fault settings");
        let instance_boot_on_faults = process_rows(&rows);

        assert_eq!(
            instance_boot_on_faults[0].values,
            vec![ColumnValue::new(COLUMN, false,)]
        );
        assert_eq!(
            instance_boot_on_faults[1].values,
            vec![ColumnValue::new(COLUMN, true)]
        );
        assert_eq!(
            instance_boot_on_faults[2].values,
            vec![ColumnValue::new(COLUMN, false)]
        );
    })
}

const COLUMN_AUTO_RESTART: &'static str = "auto_restart_policy";
const ENUM_AUTO_RESTART: &'static str = "instance_auto_restart";

fn after_95_0_0(client: &Client) -> BoxFuture<'_, ()> {
    // This reuses the instance records created in `before_70_0_0`
    Box::pin(async {
        let rows = client
            .query(
                &format!(
                    "SELECT {COLUMN_AUTO_RESTART} FROM instance ORDER BY id"
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let policies = process_rows(&rows);

        assert_eq!(
            policies[0].values,
            vec![ColumnValue::null(COLUMN_AUTO_RESTART)]
        );
        assert_eq!(
            policies[1].values,
            vec![ColumnValue::new(
                COLUMN_AUTO_RESTART,
                SqlEnum::from((ENUM_AUTO_RESTART, "all_failures"))
            )]
        );
        assert_eq!(
            policies[2].values,
            vec![ColumnValue::null(COLUMN_AUTO_RESTART)]
        );
    })
}

fn before_101_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        // Make a new instance with an explicit 'sled_failures_only' v1 auto-restart
        // policy
        client
            .batch_execute(&format!(
                "INSERT INTO instance (
                    id, name, description, time_created, time_modified,
                    time_deleted, project_id, user_data, state,
                    time_state_updated, state_generation, active_propolis_id,
                    target_propolis_id, migration_id, ncpus, memory, hostname,
                    {COLUMN_AUTO_RESTART}, updater_id, updater_gen
                )
                VALUES (
                    '{INSTANCE4}', 'inst4', '', now(), now(), NULL,
                    '{PROJECT}', '', 'no_vmm', now(), 1, NULL, NULL, NULL,
                    2, 1073741824, 'inst1', 'sled_failures_only', NULL, 1
                );"
            ))
            .await
            .expect("failed to create instance");
        // Change one of the NULLs to an explicit 'never'.
        client
            .batch_execute(&format!(
                "UPDATE instance
                SET {COLUMN_AUTO_RESTART} = 'never'
                WHERE id = '{INSTANCE3}';"
            ))
            .await
            .expect("failed to update instance");
    })
}

fn after_101_0_0(client: &Client) -> BoxFuture<'_, ()> {
    const BEST_EFFORT: &'static str = "best_effort";
    Box::pin(async {
        let rows = client
            .query(
                &format!(
                    "SELECT {COLUMN_AUTO_RESTART} FROM instance ORDER BY id"
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let policies = process_rows(&rows);

        assert_eq!(
            policies[0].values,
            vec![ColumnValue::null(COLUMN_AUTO_RESTART)],
            "null auto-restart policies should remain null",
        );
        assert_eq!(
            policies[1].values,
            vec![ColumnValue::new(
                COLUMN_AUTO_RESTART,
                SqlEnum::from((ENUM_AUTO_RESTART, BEST_EFFORT))
            )],
            "'all_failures' auto-restart policies should be migrated to \
             '{BEST_EFFORT}'",
        );
        assert_eq!(
            policies[2].values,
            vec![ColumnValue::new(
                COLUMN_AUTO_RESTART,
                SqlEnum::from((ENUM_AUTO_RESTART, "never"))
            )],
            "explicit 'never' auto-restart policies should remain 'never'",
        );
        assert_eq!(
            policies[3].values,
            vec![ColumnValue::new(
                COLUMN_AUTO_RESTART,
                SqlEnum::from((ENUM_AUTO_RESTART, BEST_EFFORT))
            )],
            "'sled_failures_only' auto-restart policies should be migrated \
             to '{BEST_EFFORT}'",
        );
    })
}

fn before_107_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        // An instance with no attached disks (4) gets a NULL boot disk.
        // An instance with one attached disk (5) gets that disk as a boot disk.
        // An instance with two attached disks (6) gets a NULL boot disk.
        client
            .batch_execute(&format!(
                "
        INSERT INTO disk (
            id, name, description, time_created,
            time_modified, time_deleted, rcgen, project_id,
            volume_id, disk_state, attach_instance_id, state_generation,
            slot, time_state_updated, size_bytes, block_size,
            origin_snapshot, origin_image, pantry_address
        ) VALUES

        ('{DISK1}', 'disk1', '', now(),
        now(), NULL, 1, '{PROJECT}',
        '{VOLUME1}', 'attached', '{INSTANCE1}', 1,
        4, now(), 65536, '512',
        NULL, NULL, NULL),
        ('{DISK2}', 'disk2', '', now(),
        now(), NULL, 1, '{PROJECT}',
        '{VOLUME2}', 'attached', '{INSTANCE2}', 1,
        4, now(), 65536, '512',
        NULL, NULL, NULL),
        ('{DISK3}', 'disk3', '', now(),
        now(), NULL, 1,'{PROJECT}',
        '{VOLUME3}', 'attached', '{INSTANCE3}', 1,
        4, now(), 65536, '512',
        NULL, NULL, NULL),
        ('{DISK4}', 'disk4', '', now(),
        now(), NULL, 1,'{PROJECT}',
        '{VOLUME4}', 'attached', '{INSTANCE3}', 1,
        4, now(), 65536, '512',
        NULL, NULL, NULL);"
            ))
            .await
            .expect("failed to create disks");
    })
}

fn after_107_0_0(client: &Client) -> BoxFuture<'_, ()> {
    Box::pin(async {
        let rows = client
            .query("SELECT id, boot_disk_id FROM instance ORDER BY id;", &[])
            .await
            .expect("failed to load instance boot disks");
        let boot_disks = process_rows(&rows);

        assert_eq!(
            boot_disks[0].values,
            vec![
                ColumnValue::new("id", INSTANCE1),
                ColumnValue::new("boot_disk_id", DISK1),
            ],
            "instance {INSTANCE1} should have one attached disk that has been \
             made the boot disk"
        );

        assert_eq!(
            boot_disks[1].values,
            vec![
                ColumnValue::new("id", INSTANCE2),
                ColumnValue::new("boot_disk_id", DISK2),
            ],
            "instance {INSTANCE2} should have a different attached disk that \
             has been made the boot disk"
        );

        assert_eq!(
            boot_disks[2].values,
            vec![
                ColumnValue::new("id", INSTANCE3),
                ColumnValue::null("boot_disk_id"),
            ],
            "instance {INSTANCE3} should have two attached disks, neither the \
             the boot disk"
        );

        assert_eq!(
            boot_disks[3].values,
            vec![
                ColumnValue::new("id", INSTANCE4),
                ColumnValue::null("boot_disk_id"),
            ],
            "instance {INSTANCE4} should have no attached disks, so \
             no boot disk"
        );
    })
}
// Lazily initializes all migration checks. The combination of Rust function
// pointers and async makes defining a static table fairly painful, so we're
// using lazy initialization instead.
//
// Each "check" is implemented as a pair of {before, after} migration function
// pointers, called precisely around the migration under test.
fn get_migration_checks() -> BTreeMap<SemverVersion, DataMigrationFns> {
    let mut map = BTreeMap::new();

    map.insert(
        SemverVersion(semver::Version::parse("23.0.0").unwrap()),
        DataMigrationFns { before: Some(before_23_0_0), after: after_23_0_0 },
    );
    map.insert(
        SemverVersion(semver::Version::parse("24.0.0").unwrap()),
        DataMigrationFns { before: Some(before_24_0_0), after: after_24_0_0 },
    );
    map.insert(
        SemverVersion(semver::Version::parse("37.0.1").unwrap()),
        DataMigrationFns { before: None, after: after_37_0_1 },
    );
    map.insert(
        SemverVersion(semver::Version::parse("70.0.0").unwrap()),
        DataMigrationFns { before: Some(before_70_0_0), after: after_70_0_0 },
    );
    map.insert(
        SemverVersion(semver::Version::parse("95.0.0").unwrap()),
        DataMigrationFns { before: Some(before_95_0_0), after: after_95_0_0 },
    );

    map.insert(
        SemverVersion(semver::Version::parse("101.0.0").unwrap()),
        DataMigrationFns { before: Some(before_101_0_0), after: after_101_0_0 },
    );

    map.insert(
        SemverVersion(semver::Version::parse("107.0.0").unwrap()),
        DataMigrationFns { before: Some(before_107_0_0), after: after_107_0_0 },
    );

    map
}

// Performs all schema changes and runs version-specific assertions.
//
// HOW TO ADD A MIGRATION CHECK:
// - Add a new "map.insert" line to "get_migration_checks", with the semver of
// the version you'd like to inspect before / after.
// - Define your "before" (optional) and "after" (required) functions. These
// act on a connection to CockroachDB, and can observe and mutate arbitrary
// state.
//
// ADVICE FOR MIGRATION CHECKS:
// - Your migration check will run in the same test as all other migration
// checks, because performing schema migrations isn't that fast. If you
// perform an operation that could be disruptive to subsequent checks, I
// recommend cleaning up after yourself (e.g., DELETE relevant rows).
// - I recommend using schema checks that are NOT strongly-typed. When you
// add a migration check, it'll happen to match the "latest" static schemas
// defined by Nexus, but that won't always be the case. As the schema
// continues to change (maybe a table you're trying to check gets a new column
// in a later version), your code should continue operating on the OLD version,
// and as such, should avoid needing any updates.
#[tokio::test]
async fn validate_data_migration() {
    let config = load_test_config();
    let logctx = LogContext::new("validate_data_migration", &config.pkg.log);
    let log = &logctx.log;

    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;
    let client = crdb.connect().await.expect("Failed to access CRDB client");

    let all_versions = read_all_schema_versions();
    let all_checks = get_migration_checks();

    // Go from the first version to the latest version.
    for version in all_versions.iter_versions() {
        // If this check has preconditions (or setup), run them.
        let checks = all_checks.get(version.semver());
        if let Some(before) = checks.and_then(|check| check.before) {
            before(&client).await;
        }

        apply_update(log, &crdb, version, 1).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );

        // If this check has postconditions (or cleanup), run them.
        if let Some(after) = checks.map(|check| check.after) {
            after(&client).await;
        }
    }
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

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
