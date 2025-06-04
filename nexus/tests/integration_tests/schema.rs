// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{self, Context};
use camino::Utf8PathBuf;
use dropshot::test_util::LogContext;
use futures::future::BoxFuture;
use nexus_config::NexusConfig;
use nexus_config::SchemaConfig;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
use nexus_db_model::KNOWN_VERSIONS;
use nexus_db_model::SCHEMA_VERSION as LATEST_SCHEMA_VERSION;
use nexus_db_model::SchemaUpgradeStep;
use nexus_db_model::{AllSchemaVersions, SchemaVersion};
use nexus_db_queries::db::DISALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use nexus_test_utils::sql::AnySqlType;
use nexus_test_utils::sql::ColumnValue;
use nexus_test_utils::sql::Row;
use nexus_test_utils::sql::SqlEnum;
use nexus_test_utils::sql::process_rows;
use nexus_test_utils::{ControlPlaneTestContextBuilder, load_test_config};
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_test_utils::dev::db::{Client, CockroachInstance};
use pretty_assertions::{assert_eq, assert_ne};
use semver::Version;
use similar_asserts;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::IpAddr;
use tokio::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const SCHEMA_DIR: &'static str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../schema/crdb");

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
    step: &SchemaUpgradeStep,
) -> Result<(), tokio_postgres::Error> {
    client.batch_execute("BEGIN;").await.expect("Failed to BEGIN transaction");
    if let Err(err) = client.batch_execute(step.sql()).await {
        panic!(
            "Failed to execute update step {}: {}",
            step.label(),
            InlineErrorChain::new(&err)
        );
    };
    client.batch_execute("COMMIT;").await?;
    Ok(())
}

// Applies an update as a transaction.
//
// Automatically retries transactions that can be retried client-side.
async fn apply_update_as_transaction(
    log: &Logger,
    client: &omicron_test_utils::dev::db::Client,
    step: &SchemaUpgradeStep,
) {
    loop {
        match apply_update_as_transaction_inner(client, step).await {
            Ok(()) => break,
            Err(err) => {
                warn!(
                    log, "Failed to apply update as transaction";
                    InlineErrorChain::new(&err),
                );
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
                panic!("Failed to apply update {}: {err}", step.label());
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
            apply_update_as_transaction(&log, &client, step).await;

            // The following is a set of "versions exempt from being
            // re-applied" multiple times. PLEASE AVOID ADDING TO THIS LIST.
            const NOT_IDEMPOTENT_VERSIONS: [semver::Version; 1] = [
                // Why: This calls "ALTER TYPE ... DROP VALUE", which does not
                // support the "IF EXISTS" syntax in CockroachDB.
                //
                // https://github.com/cockroachdb/cockroach/issues/120801
                semver::Version::new(10, 0, 0),
            ];

            if NOT_IDEMPOTENT_VERSIONS.contains(&version.semver()) {
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

enum ColumnSelector<'a> {
    ByName(&'a [&'static str]),
    Star,
}

impl<'a> From<&'a [&'static str]> for ColumnSelector<'a> {
    fn from(columns: &'a [&'static str]) -> Self {
        Self::ByName(columns)
    }
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

async fn disable_fsync(client: &Client) {
    let setting =
        "SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = true";
    if let Err(e) = client.batch_execute(setting).await {
        panic!("Failed to apply setting '{}': {}", setting, e);
    }
}

async fn apply_database_version(crdb: &CockroachInstance, version: &Version) {
    let old_dbinit = match get_old_dbinit_for_version(version).await {
        Ok(sql) => {
            println!(
                "Retrieved dbinit.sql for version {version} ({} bytes)",
                sql.len()
            );
            sql
        }
        Err(e) => {
            panic!("Failed to get old dbinit.sql for version {version}: {e}",);
        }
    };

    let client = crdb.connect().await.expect("failed to connect");

    // Apply dbinit.sql
    disable_fsync(&client).await;
    client
        .batch_execute(&old_dbinit)
        .await
        .expect("Failed to apply old dbinit.sql");
}

async fn new_database_at_version(
    log: &Logger,
    version: &Version,
) -> TestDatabase {
    let db = TestDatabase::new_populate_nothing(log).await;
    let crdb = db.crdb();
    apply_database_version(&crdb, version).await;
    db
}

// This test confirms the following behavior:
//
// - Nexus can boot using the previous revision of the schema
// - Nexus can automatically apply the subsequent updates automatically
// - This should eventually drive Nexus to the latest revision of the schema,
// such that it can boot.
#[tokio::test]
async fn nexus_applies_update_on_boot() {
    let mut config = load_test_config();
    let mut builder =
        test_setup(&mut config, "nexus_applies_update_on_boot").await;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    // Jump forward to the previous schema version before Nexus boots.
    apply_database_version(&crdb, KNOWN_VERSIONS[1].semver()).await;

    // Start Nexus. It should auto-format itself to the latest version,
    // upgrading through the last update.
    //
    // The timeout here is a bit longer than usual (120s vs 60s) because if
    // lots of tests are running at the same time, there can be contention
    // here.
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
    let db = TestDatabase::new_populate_schema_only(&log).await;
    let crdb = db.crdb();

    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(crdb).await
    );

    db.terminate().await;
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

// This test verifies that executing the most recent schema migration, in order,
// twice, still correctly performs an upgrade.
//
// This attempts to be a rough approximation for multiple Nexuses each
// simultaneously executing these operations.
#[tokio::test]
async fn last_update_has_idempotent_up() {
    let config = load_test_config();
    let logctx =
        LogContext::new("last_update_has_idempotent_up", &config.pkg.log);
    let log = &logctx.log;
    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();

    // Jump forward to the previous schema version before Nexus boots.
    let previous_version = KNOWN_VERSIONS[1].semver();
    apply_database_version(&crdb, previous_version).await;

    let all_versions = read_all_schema_versions();
    let migrations_to_apply: Vec<_> = all_versions
        .versions_range((
            std::ops::Bound::Excluded(previous_version.clone()),
            std::ops::Bound::Unbounded,
        ))
        .collect();
    for version in migrations_to_apply {
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

    db.terminate().await;
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
            If that is the case, change dbinit.sql to add the column to the \
            end of the table.\n"
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

struct MigrationContext<'a> {
    log: &'a Logger,

    // Postgres connection to database
    client: Client,
}

type BeforeFn = for<'a> fn(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()>;
type AfterFn = for<'a> fn(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()>;

// Describes the operations which we might take before and after
// migrations to check that they worked.
#[derive(Default)]
struct DataMigrationFns {
    before: Option<BeforeFn>,
    after: Option<AfterFn>,
}

impl DataMigrationFns {
    fn new() -> Self {
        Self::default()
    }
    fn before(mut self, before: BeforeFn) -> Self {
        self.before = Some(before);
        self
    }
    fn after(mut self, after: AfterFn) -> Self {
        self.after = Some(after);
        self
    }
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
const PROPOLIS2: Uuid = Uuid::from_u128(0x22226706_5c3d_4647_83b0_8f3515da7be1);

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

fn before_23_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create two silos
        ctx.client.batch_execute(&format!("INSERT INTO silo
            (id, name, description, time_created, time_modified, time_deleted, discoverable, authentication_mode, user_provision_type, mapped_fleet_roles, rcgen) VALUES
          ('{SILO1}', 'silo1', '', now(), now(), NULL, false, 'local', 'jit', '{{}}', 1),
          ('{SILO2}', 'silo2', '', now(), now(), NULL, false, 'local', 'jit', '{{}}', 1);
        ")).await.expect("Failed to create silo");

        // Create an IP pool for each silo, and a third "fleet pool" which has
        // no corresponding silo.
        ctx.client.batch_execute(&format!("INSERT INTO ip_pool
            (id, name, description, time_created, time_modified, time_deleted, rcgen, silo_id, is_default) VALUES
          ('{POOL0}', 'pool2', '', now(), now(), now(), 1, '{SILO2}', true),
          ('{POOL1}', 'pool1', '', now(), now(), NULL, 1, '{SILO1}', true),
          ('{POOL2}', 'pool2', '', now(), now(), NULL, 1, '{SILO2}', false),
          ('{POOL3}', 'pool3', '', now(), now(), NULL, 1, null, true),
          ('{POOL4}', 'pool4', '', now(), now(), NULL, 1, null, false);
        ")).await.expect("Failed to create IP Pool");
    })
}

fn after_23_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Confirm that the ip_pool_resource objects have been created
        // by the migration.
        let rows = ctx
            .client
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

fn before_24_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    // IP addresses were pulled off dogfood sled 16
    Box::pin(async move {
        // Create two sleds. (SLED2 is marked non_provisionable for
        // after_37_0_1.)
        ctx.client
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

fn after_24_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Confirm that the IP Addresses have the last 2 bytes changed to `0xFFFF`
        let rows = ctx
            .client
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
fn after_37_0_1<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Confirm that the IP Addresses have the last 2 bytes changed to `0xFFFF`
        let rows = ctx
            .client
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

fn before_70_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
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
        ctx.client
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

fn after_70_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let rows = ctx
            .client
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

        let rows = ctx
            .client
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

fn before_95_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    // This reuses the instance records created in `before_70_0_0`
    const COLUMN: &'static str = "boot_on_fault";
    Box::pin(async {
        let rows = ctx
            .client
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

fn after_95_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    // This reuses the instance records created in `before_70_0_0`
    Box::pin(async {
        let rows = ctx
            .client
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

fn before_101_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Make a new instance with an explicit 'sled_failures_only' v1 auto-restart
        // policy
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO instance (
                    id, name, description, time_created, time_modified,
                    time_deleted, project_id, user_data, state,
                    time_state_updated, state_generation, active_propolis_id,
                    target_propolis_id, migration_id, ncpus, memory, hostname,
                    {COLUMN_AUTO_RESTART}, updater_id, updater_gen
                )
                VALUES
                (
                    '{INSTANCE1}', 'inst1', '', now(), now(), NULL, '{PROJECT}', '',
                    'no_vmm', now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst1',
                    NULL, NULL, 1
                ),
                (
                    '{INSTANCE2}', 'inst2', '', now(), now(), NULL, '{PROJECT}', '',
                    'vmm', now(), 1, '{PROPOLIS}', NULL, NULL, 2, 1073741824, 'inst2',
                    'all_failures', NULL, 1
                ),
                (
                    '{INSTANCE3}', 'inst3', '', now(), now(), NULL, '{PROJECT}', '',
                    'failed', now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst3',
                    'never', NULL, 1
                ),
                (
                    '{INSTANCE4}', 'inst4', '', now(), now(), NULL, '{PROJECT}', '',
                    'no_vmm', now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst4',
                    'sled_failures_only', NULL, 1
                );"
            ))
            .await
            .expect("failed to create instance");
    })
}

fn after_101_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    const BEST_EFFORT: &'static str = "best_effort";
    Box::pin(async {
        let rows = ctx
            .client
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

fn before_107_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // An instance with no attached disks (4) gets a NULL boot disk.
        // An instance with one attached disk (5) gets that disk as a boot disk.
        // An instance with two attached disks (6) gets a NULL boot disk.
        ctx.client
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

fn after_107_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let rows = ctx
            .client
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

fn before_124_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Insert a region snapshot replacement record
        let request_id: Uuid =
            "5f867d89-a61f-48cd-ac7d-aecbcb23c2f9".parse().unwrap();
        let dataset_id: Uuid =
            "c625d694-185b-4c64-9369-402b7ba1362e".parse().unwrap();
        let region_id: Uuid =
            "bda60191-05a0-4881-8bca-0855464ecd9f".parse().unwrap();
        let snapshot_id: Uuid =
            "0b8382de-d787-450a-8516-235f33eb0946".parse().unwrap();

        ctx.client
            .batch_execute(&format!(
                "
        INSERT INTO region_snapshot_replacement (
            id,
            request_time,
            old_dataset_id,
            old_region_id,
            old_snapshot_id,
            old_snapshot_volume_id,
            new_region_id,
            replacement_state,
            operating_saga_id,
            new_region_volume_id
        ) VALUES (
            '{request_id}',
            now(),
            '{dataset_id}',
            '{region_id}',
            '{snapshot_id}',
            NULL,
            NULL,
            'requested',
            NULL,
            NULL
        );"
            ))
            .await
            .expect("failed to insert record");
    })
}

fn after_124_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let rows = ctx
            .client
            .query(
                "SELECT replacement_type FROM region_snapshot_replacement;",
                &[],
            )
            .await
            .expect("failed to load region snapshot replacements");

        let records = process_rows(&rows);

        assert_eq!(records.len(), 1);

        assert_eq!(
            records[0].values,
            vec![ColumnValue::new(
                "replacement_type",
                SqlEnum::from((
                    "read_only_target_replacement_type",
                    "region_snapshot"
                )),
            )],
            "existing region snapshot replacement should have replacement type",
        );
    })
}

fn before_125_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Insert a few bp_omicron_zone records and their parent
        // bp_sled_omicron_zones row (from which we pull its generation)
        let bp1_id: Uuid =
            "00000000-0000-0000-0000-000000000001".parse().unwrap();
        let bp2_id: Uuid =
            "00000000-0000-0000-0000-000000000002".parse().unwrap();

        let sled_id: Uuid = Uuid::new_v4();

        let bp1_generation: i64 = 3;
        let bp2_generation: i64 = 4;

        ctx.client
            .batch_execute(&format!(
                "
                    INSERT INTO bp_sled_omicron_zones (
                        blueprint_id, sled_id, generation
                    ) VALUES (
                        '{bp1_id}', '{sled_id}', {bp1_generation}
                    );

                    INSERT INTO bp_sled_omicron_zones (
                        blueprint_id, sled_id, generation
                    ) VALUES (
                        '{bp2_id}', '{sled_id}', {bp2_generation}
                    );
                "
            ))
            .await
            .expect("inserted record");

        // Insert an in-service zone and and expunged zone for each blueprint.
        let in_service_zone_id: Uuid =
            "00000001-0000-0000-0000-000000000000".parse().unwrap();
        let expunged_zone_id: Uuid =
            "00000002-0000-0000-0000-000000000000".parse().unwrap();

        // Fill in a filesystem pool for each zone. This column was NULLable
        // prior to schema version 132.0.0, but became NOT NULL in that version,
        // so it's simplest to go ahead and populate it here (otherwise our test
        // migration to 132 will fail). Operationally, we confirmed via omdb
        // that all deployed systems had non-NULL filesystem_pool values prior
        // to upgrading to 132.
        let filesystem_pool: Uuid =
            "00000000-0000-0000-0000-0000706f6f6c".parse().unwrap();

        for bp_id in [bp1_id, bp2_id] {
            for (zone_id, disposition) in [
                (in_service_zone_id, "in_service"),
                (expunged_zone_id, "expunged"),
            ] {
                ctx.client
                    .batch_execute(&format!(
                        "
                        INSERT INTO bp_omicron_zone (
                            blueprint_id,
                            sled_id,
                            id,
                            zone_type,
                            primary_service_ip,
                            primary_service_port,
                            filesystem_pool,
                            disposition
                        ) VALUES (
                            '{bp_id}',
                            '{sled_id}',
                            '{zone_id}',
                            'oximeter',
                            '::1',
                            0,
                            '{filesystem_pool}',
                            '{disposition}'
                        );
                    "
                    ))
                    .await
                    .expect("inserted record");
            }
        }
    })
}

fn after_125_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let bp1_id: Uuid =
            "00000000-0000-0000-0000-000000000001".parse().unwrap();
        let bp2_id: Uuid =
            "00000000-0000-0000-0000-000000000002".parse().unwrap();
        let bp1_generation: i64 = 3;
        let bp2_generation: i64 = 4;
        let in_service_zone_id: Uuid =
            "00000001-0000-0000-0000-000000000000".parse().unwrap();
        let expunged_zone_id: Uuid =
            "00000002-0000-0000-0000-000000000000".parse().unwrap();

        let rows = ctx
            .client
            .query(
                r#"
                SELECT
                    blueprint_id,
                    id,
                    disposition,
                    disposition_expunged_as_of_generation,
                    disposition_expunged_ready_for_cleanup
                FROM bp_omicron_zone
                ORDER BY blueprint_id, id
                "#,
                &[],
            )
            .await
            .expect("loaded bp_omicron_zone rows");

        let records = process_rows(&rows);

        assert_eq!(records.len(), 4);

        assert_eq!(
            records[0].values,
            vec![
                ColumnValue::new("blueprint_id", bp1_id),
                ColumnValue::new("id", in_service_zone_id),
                ColumnValue::new(
                    "disposition",
                    SqlEnum::from(("bp_zone_disposition", "in_service")),
                ),
                ColumnValue::null("disposition_expunged_as_of_generation"),
                ColumnValue::new(
                    "disposition_expunged_ready_for_cleanup",
                    false
                ),
            ],
            "in_service zone left in service",
        );
        assert_eq!(
            records[1].values,
            vec![
                ColumnValue::new("blueprint_id", bp1_id),
                ColumnValue::new("id", expunged_zone_id),
                ColumnValue::new(
                    "disposition",
                    SqlEnum::from(("bp_zone_disposition", "expunged")),
                ),
                ColumnValue::new(
                    "disposition_expunged_as_of_generation",
                    bp1_generation
                ),
                ColumnValue::new(
                    "disposition_expunged_ready_for_cleanup",
                    false
                ),
            ],
            "expunged zone gets correct disposition and generation",
        );
        assert_eq!(
            records[2].values,
            vec![
                ColumnValue::new("blueprint_id", bp2_id),
                ColumnValue::new("id", in_service_zone_id),
                ColumnValue::new(
                    "disposition",
                    SqlEnum::from(("bp_zone_disposition", "in_service")),
                ),
                ColumnValue::null("disposition_expunged_as_of_generation"),
                ColumnValue::new(
                    "disposition_expunged_ready_for_cleanup",
                    false
                ),
            ],
            "in_service zone left in service",
        );
        assert_eq!(
            records[3].values,
            vec![
                ColumnValue::new("blueprint_id", bp2_id),
                ColumnValue::new("id", expunged_zone_id),
                ColumnValue::new(
                    "disposition",
                    SqlEnum::from(("bp_zone_disposition", "expunged")),
                ),
                ColumnValue::new(
                    "disposition_expunged_as_of_generation",
                    bp2_generation
                ),
                ColumnValue::new(
                    "disposition_expunged_ready_for_cleanup",
                    false
                ),
            ],
            "expunged zone gets correct disposition and generation",
        );
    })
}

fn before_133_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // This VMM will have a sled_resource_vmm record and a VMM record
        let vmm1_id: Uuid =
            "00000000-0000-0000-0000-000000000001".parse().unwrap();
        // This VMM will have a sled_resource_vmm record only
        let vmm2_id: Uuid =
            "00000000-0000-0000-0000-000000000002".parse().unwrap();

        let sled_id = Uuid::new_v4();
        let instance1_id = Uuid::new_v4();
        let instance2_id = Uuid::new_v4();

        ctx.client
            .batch_execute(&format!(
                "
                    INSERT INTO sled_resource_vmm (
                        id,
                        sled_id,
                        hardware_threads,
                        rss_ram,
                        reservoir_ram,
                        instance_id
                    ) VALUES
                    (
                        '{vmm1_id}', '{sled_id}', 1, 0, 0, '{instance1_id}'
                    ),
                    (
                        '{vmm2_id}', '{sled_id}', 1, 0, 0, '{instance2_id}'
                    );

                "
            ))
            .await
            .expect("inserted record");

        // Only insert a vmm record for one of these reservations
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO vmm (
                    id,
                    time_created,
                    time_deleted,
                    instance_id,
                    time_state_updated,
                    state_generation,
                    sled_id,
                    propolis_ip,
                    propolis_port,
                    state
                ) VALUES (
                    '{vmm1_id}',
                    now(),
                    NULL,
                    '{instance1_id}',
                    now(),
                    1,
                    '{sled_id}',
                    'fd00:1122:3344:104::1',
                    12400,
                    'running'
                );
            "
            ))
            .await
            .expect("inserted record");
    })
}

fn after_133_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // This record should still have a sled_resource_vmm, and be the only
        // one.
        let vmm1_id: Uuid =
            "00000000-0000-0000-0000-000000000001".parse().unwrap();

        let rows = ctx
            .client
            .query(
                r#"
                SELECT id FROM sled_resource_vmm
                "#,
                &[],
            )
            .await
            .expect("loaded sled_resource_vmm rows");

        let records = process_rows(&rows);

        assert_eq!(records.len(), 1, "{records:?}");

        assert_eq!(
            records[0].values,
            vec![ColumnValue::new("id", vmm1_id),],
            "Unexpected sled_resource_vmm record value",
        );
    })
}

fn before_134_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // To test the size_used upgrader, create a few crucible datasets and
        // regions

        // First, a crucible dataset with no regions
        let dataset_id: Uuid =
            "00000001-0000-0000-0000-000000000000".parse().unwrap();
        let pool_id = Uuid::new_v4();
        let size_used = 0;

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO crucible_dataset VALUES (
                    '{dataset_id}',
                    now(),
                    now(),
                    null,
                    1,
                    '{pool_id}',
                    '::1',
                    10000,
                    {size_used}
                )"
            ))
            .await
            .expect("inserted");

        // Then, a crucible dataset with 1 region
        let dataset_id: Uuid =
            "00000002-0000-0000-0000-000000000000".parse().unwrap();

        let region_id = Uuid::new_v4();
        let pool_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();
        let block_size = 512;
        let blocks_per_extent = 64;
        let extent_count = 1000;

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO region VALUES (
                    '{region_id}',
                    now(),
                    now(),
                    '{dataset_id}',
                    '{volume_id}',
                    {block_size},
                    {blocks_per_extent},
                    {extent_count},
                    5000,
                    false,
                    false
                )"
            ))
            .await
            .expect("inserted");

        let size_used = block_size * blocks_per_extent * extent_count;

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO crucible_dataset VALUES (
                    '{dataset_id}',
                    now(),
                    now(),
                    null,
                    1,
                    '{pool_id}',
                    '::1',
                    10000,
                    {size_used}
                )"
            ))
            .await
            .expect("inserted");

        // Finally, a crucible dataset with 3 regions
        let dataset_id: Uuid =
            "00000003-0000-0000-0000-000000000000".parse().unwrap();
        let pool_id = Uuid::new_v4();

        let block_size = 512;
        let blocks_per_extent = 64;
        let extent_count = 7000;

        for _ in 0..3 {
            let region_id = Uuid::new_v4();
            let volume_id = Uuid::new_v4();

            ctx.client
                .batch_execute(&format!(
                    "INSERT INTO region VALUES (
                        '{region_id}',
                        now(),
                        now(),
                        '{dataset_id}',
                        '{volume_id}',
                        {block_size},
                        {blocks_per_extent},
                        {extent_count},
                        5000,
                        false,
                        false
                    )"
                ))
                .await
                .expect("inserted");
        }

        let size_used = 3 * block_size * blocks_per_extent * extent_count;

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO crucible_dataset VALUES (
                    '{dataset_id}',
                    now(),
                    now(),
                    null,
                    1,
                    '{pool_id}',
                    '::1',
                    10000,
                    {size_used}
                )"
            ))
            .await
            .expect("inserted");
    })
}

fn after_134_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // The first crucible dataset still has size_used = 0
        let rows = ctx
            .client
            .query(
                "SELECT size_used FROM crucible_dataset WHERE
                id = '00000001-0000-0000-0000-000000000000'",
                &[],
            )
            .await
            .expect("select");

        let records = process_rows(&rows);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].values,
            vec![ColumnValue::new("size_used", 0i64)],
        );

        // Note: the default crucible reservation factor is 1.25

        // The second crucible dataset has
        //  size_used = 1.25 * (512 * 64 * 1000)
        let rows = ctx
            .client
            .query(
                "SELECT size_used FROM crucible_dataset WHERE
                id = '00000002-0000-0000-0000-000000000000'",
                &[],
            )
            .await
            .expect("select");

        let records = process_rows(&rows);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].values,
            vec![ColumnValue::new("size_used", 40960000i64)],
        );

        // The third crucible dataset has
        //  size_used = 1.25 * (3 * 512 * 64 * 7000)
        let rows = ctx
            .client
            .query(
                "SELECT size_used FROM crucible_dataset WHERE
                id = '00000003-0000-0000-0000-000000000000'",
                &[],
            )
            .await
            .expect("select");

        let records = process_rows(&rows);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].values,
            vec![ColumnValue::new("size_used", 860160000i64)],
        );
    })
}

fn after_139_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let probe_alert_id: Uuid =
            "001de000-7768-4000-8000-000000000001".parse().unwrap();
        let rows = ctx
            .client
            .query(
                // Don't select timestamps, as those are variable, and we don't
                // want to assert that they always have a particular value.
                // However, we *do* need to ensure that `time_dispatched` is
                // set, so that the event is not eligible for dispatching ---
                // include a WHERE clause ensuring it is not null.
                r#"
                SELECT
                    id,
                    event_class,
                    event,
                    num_dispatched
                FROM webhook_event
                WHERE time_dispatched IS NOT NULL
                "#,
                &[],
            )
            .await
            .expect("loaded bp_omicron_zone rows");

        let records = process_rows(&rows);

        assert_eq!(
            records.len(),
            1,
            "there should be exactly one singleton event in the webhook_event \
             table"
        );

        assert_eq!(
            records[0].values,
            vec![
                ColumnValue::new("id", probe_alert_id),
                ColumnValue::new(
                    "event_class",
                    SqlEnum::from(("webhook_event_class", "probe")),
                ),
                ColumnValue::new("event", serde_json::json!({})),
                ColumnValue::new("num_dispatched", 0i64),
            ],
            "singleton liveness probe webhook event record must have the \
             correct values",
        );
    })
}

fn before_140_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        ctx.client
            .batch_execute(&format!("
                INSERT INTO instance (id, name, description, time_created, time_modified,
                    time_deleted, project_id, user_data, time_state_updated,
                    state_generation, active_propolis_id, target_propolis_id, migration_id, ncpus,
                    memory, hostname, updater_id, updater_gen, state, time_last_auto_restarted,
                    auto_restart_policy, auto_restart_cooldown, boot_disk_id)
                VALUES
                (
                    '{INSTANCE1}', 'inst1', '', now(), now(), NULL, '{PROJECT}', '',
                    now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst1',
                    NULL, 1, 'no_vmm', NULL, 'never', NULL, NULL
                ),
                (
                    '{INSTANCE2}', 'inst2', '', now(), now(), NULL, '{PROJECT}', '',
                    now(), 1, '{PROPOLIS}', NULL, NULL, 2, 1073741824, 'inst2',
                    NULL, 1, 'vmm', NULL, 'never', NULL, NULL
                ),
                (
                    '{INSTANCE3}', 'inst3', '', now(), now(), NULL, '{PROJECT}', '',
                    now(), 1, NULL, NULL, NULL, 2, 1073741824, 'inst3',
                    NULL, 1, 'failed', NULL, 'never', NULL, NULL
                ),
                (
                    '{INSTANCE4}', 'inst4', '', now(), now(), NULL, '{PROJECT}', '',
                    now(), 1, '{PROPOLIS2}', NULL, NULL, 2, 1073741824, 'inst4',
                    NULL, 1, 'vmm', NULL, 'never', NULL, NULL
                );
                "))
            .await
            .expect("failed to create instances");

        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vmm (id, time_created, time_deleted, instance_id, time_state_updated,
                state_generation, sled_id, propolis_ip, propolis_port, state)
                VALUES (
                    '{PROPOLIS}', now(), NULL, '{INSTANCE2}', now(), 1, '{SLED1}',
                    'fd00:1122:3344:104::1', 12400, 'running'
                ),
                (
                    '{PROPOLIS2}', now(), NULL, '{INSTANCE4}', now(), 1, '{SLED2}',
                    'fd00:1122:3344:104::1', 12400, 'stopped'
                );"
            ))
            .await
            .expect("inserted VMM record");
    })
}

fn after_140_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        let rows = ctx
            .client
            .query("SELECT id, intended_state FROM instance ORDER BY id", &[])
            .await
            .expect("failed to load instance auto-restart policies");
        let records = process_rows(&rows);
        let instances = records.into_iter().map(|row| {
            slog::info!(&ctx.log, "instance record: {row:?}");
            let [id_value, state_value] = &row.values[..] else {
                panic!("row did not have two columns! {row:?}");
            };
            let Some(&AnySqlType::Uuid(id)) = id_value.expect("id") else {
                panic!("id column must be non-null UUID, but found: {id_value:?}")
            };
            let AnySqlType::Enum(intended_state) = state_value
                .expect("intended_state")
                .expect("intended state must not be null")
            else {
                panic!("intended_state column must be an enum, but found: {state_value:?}");
            };
            (id, intended_state.clone())
        }).collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            instances.get(&INSTANCE1),
            Some(&SqlEnum::from(("instance_intended_state", "stopped"))),
            "instance 1 ({INSTANCE1}): state='no_vmm'"
        );
        assert_eq!(
            instances.get(&INSTANCE2),
            Some(&SqlEnum::from(("instance_intended_state", "running"))),
            "instance 2 ({INSTANCE2}): state='vmm', active_vmm='running'"
        );
        assert_eq!(
            instances.get(&INSTANCE3),
            Some(&SqlEnum::from(("instance_intended_state", "running"))),
            "instance 3 ({INSTANCE3}): state='failed'"
        );
        assert_eq!(
            instances.get(&INSTANCE4),
            Some(&SqlEnum::from(("instance_intended_state", "stopped"))),
            "instance 4 ({INSTANCE4}): state='vmm',active_vmm='stopped'"
        );
    })
}

fn before_145_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create one console_session without id, and one device_access_token without id.
        ctx.client
            .batch_execute(
                "
        INSERT INTO omicron.public.console_session
          (token, time_created, time_last_used, silo_user_id)
        VALUES
          ('tok-console-145', now(), now(), gen_random_uuid());

        INSERT INTO omicron.public.device_access_token
          (token, client_id, device_code, silo_user_id, time_created, time_requested)
        VALUES
          ('tok-device-145', gen_random_uuid(), 'code-145', gen_random_uuid(), now(), now());
        ",
            )
            .await
            .expect("failed to insert pre-migration rows for 145");
    })
}

fn after_145_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // After the migration each row should have a non-null id,
        // keep its token, and enforce primary-key/unique index.

        // console_session: check id ≠ NULL and token unchanged
        let rows = ctx
            .client
            .query(
                "SELECT id, token FROM omicron.public.console_session WHERE token = 'tok-console-145';",
                &[],
            )
            .await
            .expect("failed to query post-migration console_session");
        assert_eq!(rows.len(), 1);

        let id: Option<Uuid> = (&rows[0]).get("id");
        assert!(id.is_some());

        let token: &str = (&rows[0]).get("token");
        assert_eq!(token, "tok-console-145");

        // device_access_token: same checks
        let rows = ctx
            .client
            .query(
                "SELECT id, token FROM omicron.public.device_access_token WHERE token = 'tok-device-145';",
                &[],
            )
            .await
            .expect("failed to query post-migration device_access_token");
        assert_eq!(rows.len(), 1);

        let id: Option<Uuid> = (&rows[0]).get("id");
        assert!(id.is_some());

        let token: &str = (&rows[0]).get("token");
        assert_eq!(token, "tok-device-145",);
    })
}

const M2_DISK: &str = "db7d37d5-b32c-42cd-b871-598cf9d46782";
const M2_ZPOOL: &str = "8117dbdb-0112-4c4e-ac41-500b8ab0aaf7";
const U2_DISK: &str = "5d21f0d6-8af3-4d33-977d-63b2a79d6a58";
const U2_ZPOOL: &str = "dc28856d-3896-4b3c-bd3d-33a770d49c92";

// Insert two disks with a zpool on each: one m.2, one u.2
fn before_148_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(
                &format!("
        INSERT INTO omicron.public.physical_disk
          (id, time_created, time_modified, time_deleted, rcgen, vendor, serial, model, variant, sled_id, disk_policy, disk_state)
        VALUES
          (
            '{M2_DISK}', now(), now(), NULL, 0, 'vend', 'serial-m2', 'model', 'm2', gen_random_uuid(), 'in_service', 'active'
          ),
          (
            '{U2_DISK}', now(), now(), NULL, 0, 'vend', 'serial-u2', 'model', 'u2', gen_random_uuid(), 'in_service', 'active'
          );

        INSERT INTO omicron.public.zpool
          (id, time_created, time_modified, time_deleted, rcgen, sled_id, physical_disk_id, control_plane_storage_buffer)
        VALUES
          ('{M2_ZPOOL}', now(), now(), NULL, 0, gen_random_uuid(), '{M2_DISK}', 0),
          ('{U2_ZPOOL}', now(), now(), NULL, 0, gen_random_uuid(), '{U2_DISK}', 0)
        "),
            )
            .await
            .expect("failed to insert pre-migration rows for 148");
    })
}

// Validate that the m.2 is gone, and the u.2 still exists
fn after_148_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query("SELECT id FROM omicron.public.physical_disk;", &[])
            .await
            .expect("failed to query post-migration disks");
        assert_eq!(rows.len(), 1);

        let id: Uuid = (&rows[0]).get::<&str, Uuid>("id");
        assert_eq!(id.to_string(), U2_DISK);

        let rows = ctx
            .client
            .query("SELECT id FROM omicron.public.zpool;", &[])
            .await
            .expect("failed to query post-migration zpools");
        assert_eq!(rows.len(), 1);

        let id: Uuid = (&rows[0]).get::<&str, Uuid>("id");
        assert_eq!(id.to_string(), U2_ZPOOL);
    })
}

// Lazily initializes all migration checks. The combination of Rust function
// pointers and async makes defining a static table fairly painful, so we're
// using lazy initialization instead.
//
// Each "check" is implemented as a pair of {before, after} migration function
// pointers, called precisely around the migration under test.
fn get_migration_checks() -> BTreeMap<Version, DataMigrationFns> {
    let mut map = BTreeMap::new();

    map.insert(
        Version::new(23, 0, 0),
        DataMigrationFns::new().before(before_23_0_0).after(after_23_0_0),
    );
    map.insert(
        Version::new(24, 0, 0),
        DataMigrationFns::new().before(before_24_0_0).after(after_24_0_0),
    );
    map.insert(
        Version::new(37, 0, 1),
        DataMigrationFns::new().after(after_37_0_1),
    );
    map.insert(
        Version::new(70, 0, 0),
        DataMigrationFns::new().before(before_70_0_0).after(after_70_0_0),
    );
    map.insert(
        Version::new(95, 0, 0),
        DataMigrationFns::new().before(before_95_0_0).after(after_95_0_0),
    );
    map.insert(
        Version::new(101, 0, 0),
        DataMigrationFns::new().before(before_101_0_0).after(after_101_0_0),
    );
    map.insert(
        Version::new(107, 0, 0),
        DataMigrationFns::new().before(before_107_0_0).after(after_107_0_0),
    );
    map.insert(
        Version::new(124, 0, 0),
        DataMigrationFns::new().before(before_124_0_0).after(after_124_0_0),
    );
    map.insert(
        Version::new(125, 0, 0),
        DataMigrationFns::new().before(before_125_0_0).after(after_125_0_0),
    );
    map.insert(
        Version::new(133, 0, 0),
        DataMigrationFns::new().before(before_133_0_0).after(after_133_0_0),
    );
    map.insert(
        Version::new(134, 0, 0),
        DataMigrationFns::new().before(before_134_0_0).after(after_134_0_0),
    );
    map.insert(
        Version::new(139, 0, 0),
        DataMigrationFns::new().after(after_139_0_0),
    );
    map.insert(
        Version::new(140, 0, 0),
        DataMigrationFns::new().before(before_140_0_0).after(after_140_0_0),
    );
    map.insert(
        Version::new(145, 0, 0),
        DataMigrationFns::new().before(before_145_0_0).after(after_145_0_0),
    );
    map.insert(
        Version::new(148, 0, 0),
        DataMigrationFns::new().before(before_148_0_0).after(after_148_0_0),
    );

    map
}

// Parse the previous-schemas.txt config file to get starting versions
fn parse_previous_schemas() -> Result<BTreeSet<semver::Version>, anyhow::Error>
{
    let config_file =
        std::path::Path::new(SCHEMA_DIR).join("previous-schemas.txt");
    let content = std::fs::read_to_string(&config_file).with_context(|| {
        format!("Failed to read config file: {:?}", config_file)
    })?;

    let mut versions = BTreeSet::new();

    for line in content.lines() {
        let line = line.trim();
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line == "PREVIOUS" {
            // Get the previous version from KNOWN_VERSIONS
            if KNOWN_VERSIONS.len() < 2 {
                anyhow::bail!(
                    "Need at least 2 known versions to find previous version. Current count: {}",
                    KNOWN_VERSIONS.len()
                );
            }
            let previous_version = KNOWN_VERSIONS[1].semver().clone();
            versions.insert(previous_version);
        } else {
            // Parse as semver
            let version = semver::Version::parse(line).with_context(|| {
                format!("Failed to parse version: {}", line)
            })?;
            versions.insert(version);
        }
    }

    Ok(versions)
}

// Load dbinit.sql from a previous schema version for migration testing
async fn load_dbinit_from_previous_schema(
    version: &semver::Version,
) -> Result<String, anyhow::Error> {
    let previous_schema_dir =
        std::path::Path::new(SCHEMA_DIR).join("previous-schema");
    let dbinit_file =
        previous_schema_dir.join(version.to_string()).join("dbinit.sql");
    if !dbinit_file.exists() {
        anyhow::bail!(
            "dbinit.sql not found for version {}. Run 'cargo xtask schema generate-previous' to generate it.",
            version
        );
    }
    tokio::fs::read_to_string(&dbinit_file).await.with_context(|| {
        format!("Failed to read dbinit.sql for version {}", version)
    })
}

// Test data migration from a starting schema version to a target version
async fn validate_data_migration_from_version_to_target(
    starting_version: semver::Version,
    target_version: semver::Version,
    test_name: &str,
) {
    let overall_start = std::time::Instant::now();

    let config = load_test_config();
    let logctx = LogContext::new(test_name, &config.pkg.log);
    let log = &logctx.log;

    // Time database creation
    let db_start = std::time::Instant::now();
    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();
    let db_creation_time = db_start.elapsed();

    // Time starting schema application
    let schema_start = std::time::Instant::now();
    let starting_sql = load_dbinit_from_previous_schema(&starting_version)
        .await
        .expect("Failed to load starting schema");

    let client = crdb.connect().await.expect("Failed to access CRDB client");
    disable_fsync(&client).await;
    client
        .batch_execute(&starting_sql)
        .await
        .expect("Failed to apply starting schema");

    // Verify we're at the expected starting version
    let actual_version = query_crdb_schema_version(&crdb).await;
    assert_eq!(starting_version.to_string(), actual_version);
    let schema_apply_time = schema_start.elapsed();

    let ctx = MigrationContext { log, client };

    let all_versions = read_all_schema_versions();
    let all_checks = get_migration_checks();

    // Find all versions from starting_version to target_version (inclusive)
    let starting_nexus_version = Version::new(
        starting_version.major,
        starting_version.minor,
        starting_version.patch,
    );
    let target_nexus_version = Version::new(
        target_version.major,
        target_version.minor,
        target_version.patch,
    );

    let versions_to_apply: Vec<_> = all_versions
        .iter_versions()
        .filter(|v| {
            v.semver() > &starting_nexus_version
                && v.semver() <= &target_nexus_version
        })
        .collect();

    // Time migration steps
    let migrations_start = std::time::Instant::now();
    let mut migration_times = Vec::new();

    // Apply each migration step
    for version in &versions_to_apply {
        let migration_start = std::time::Instant::now();

        // If this check has preconditions (or setup), run them.
        let checks = all_checks.get(version.semver());
        if let Some(before) = checks.and_then(|check| check.before) {
            before(&ctx).await;
        }

        apply_update(log, &crdb, version, 1).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );

        // If this check has postconditions (or cleanup), run them.
        if let Some(after) = checks.and_then(|check| check.after) {
            after(&ctx).await;
        }

        let migration_time = migration_start.elapsed();
        migration_times.push((version.semver().clone(), migration_time));
    }

    let total_migration_time = migrations_start.elapsed();

    // Verify we reached the target version
    assert_eq!(
        target_version.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    db.terminate().await;
    logctx.cleanup_successful();

    let total_time = overall_start.elapsed();

    // Print detailed timing information
    println!("  {} timing breakdown:", test_name);
    println!("    Database creation: {:.2}s", db_creation_time.as_secs_f64());
    println!(
        "    Starting schema ({}): {:.2}s",
        starting_version,
        schema_apply_time.as_secs_f64()
    );
    println!(
        "    Migrations ({} steps): {:.2}s",
        migration_times.len(),
        total_migration_time.as_secs_f64()
    );
    for (version, time) in &migration_times {
        println!("      → {}: {:.2}s", version, time.as_secs_f64());
    }
    println!("    Total test time: {:.2}s", total_time.as_secs_f64());
}

// Find migration test ranges based on checkpoints in previous-schemas.txt
fn get_migration_test_ranges()
-> Result<Vec<(semver::Version, semver::Version)>, anyhow::Error> {
    // Get checkpoint versions from previous-schemas.txt
    let checkpoints: Vec<_> = parse_previous_schemas()?.into_iter().collect();

    // Get all migration check versions
    let migration_checks = get_migration_checks();
    let mut all_migration_versions: Vec<_> =
        migration_checks.keys().cloned().collect();
    all_migration_versions.sort();

    let mut ranges = Vec::new();

    // For each pair of consecutive checkpoints, find the migration checks in between
    for i in 0..checkpoints.len() {
        let start_checkpoint = &checkpoints[i];
        let end_checkpoint = if i + 1 < checkpoints.len() {
            &checkpoints[i + 1]
        } else {
            // For the last checkpoint, find the highest migration check version
            all_migration_versions
                .last()
                .expect("Expected at least one migration check")
        };

        // Find migration versions in this range
        let range_migrations: Vec<_> = all_migration_versions
            .iter()
            .filter(|v| **v > *start_checkpoint && **v <= *end_checkpoint)
            .collect();

        if !range_migrations.is_empty() {
            let range_start = start_checkpoint.clone();
            let range_end = (*range_migrations.last().unwrap()).clone();
            ranges.push((range_start, range_end));
        }
    }

    Ok(ranges)
}

// Verify that all migration checks are covered by our test ranges
fn verify_migration_coverage() {
    let checkpoints: Vec<_> = parse_previous_schemas()
        .expect("Failed to parse previous-schemas.txt")
        .into_iter()
        .collect();
    let migration_checks = get_migration_checks();
    let test_ranges = get_migration_test_ranges()
        .expect("Failed to get migration test ranges");

    // Get all migration check versions
    let mut all_migration_versions: Vec<_> =
        migration_checks.keys().cloned().collect();
    all_migration_versions.sort();

    // Find which migrations are covered by our test ranges
    let mut covered_migrations = std::collections::HashSet::new();
    for (start, end) in &test_ranges {
        for version in &all_migration_versions {
            if version > start && version <= end {
                covered_migrations.insert(version);
            }
        }
    }

    // Find uncovered migrations
    let mut uncovered_migrations = Vec::new();
    for version in &all_migration_versions {
        if !covered_migrations.contains(version) {
            uncovered_migrations.push(version);
        }
    }

    if uncovered_migrations.is_empty() {
        println!("  ✅ All migration checks are covered by test ranges");
        return;
    }
    eprintln!(
        "❌ UNCOVERED migration checks found: {:?}",
        uncovered_migrations
    );
    eprintln!("These migration checks are not covered by any test range!");
    eprintln!(
        "To fix this, update schema/crdb/previous-schemas.txt to add checkpoints that cover these versions."
    );

    // Provide specific suggestions
    for uncovered in &uncovered_migrations {
        let before_checkpoint =
            checkpoints.iter().filter(|cp| **cp < **uncovered).max();
        let after_checkpoint =
            checkpoints.iter().filter(|cp| **cp > **uncovered).min();

        match (before_checkpoint, after_checkpoint) {
            (Some(before), Some(after)) => {
                eprintln!(
                    "  → {} falls between checkpoints {} and {} - this should be covered automatically",
                    uncovered, before, after
                );
            }
            (None, Some(after)) => {
                eprintln!(
                    "  → {} is before first checkpoint {} - add an earlier checkpoint like {}",
                    uncovered, after, uncovered
                );
            }
            (Some(before), None) => {
                eprintln!(
                    "  → {} is after last checkpoint {} - this should be covered automatically",
                    uncovered, before
                );
            }
            (None, None) => {
                eprintln!(
                    "  → {} has no nearby checkpoints - do any checkpoints exist?",
                    uncovered
                );
            }
        }
    }

    panic!(
        "Migration coverage verification failed! {} migration checks are not covered by test ranges.",
        uncovered_migrations.len()
    );
}

// Performs all schema changes and runs version-specific assertions.
//
// HOW TO ADD A MIGRATION CHECK:
// - Add a new "map.insert" line to "get_migration_checks", with the semver of the version you'd
// like to inspect before / after.
// - Define your "before" (optional) and "after" (required) functions. These act on a connection to
// CockroachDB, and can observe and mutate arbitrary state.
// - Migrations will start at one of the schemas defined in "schema/crdb/previous-schemas.txt".
// Migrations will be executed between these checkpoint schemas. If a particular migration is
// taking a long time - for example, suppose it's going through hundreds of expensive upgrade
// operations - consider adding a "checkpoint" in this file and re-running the command: "cargo
// xtask schema generate-previous". This will save a whole copy of "dbinit.sql" from when the old
// schema migration was added, and these checkpoints can be used as starting points for upgrade
// tests.
//
// ADVICE FOR MIGRATION CHECKS:
// - Your migration check will run in the same test as all other migration checks (between
// checkpoints), because performing schema migrations isn't that fast. If you perform an operation
// that could be disruptive to subsequent checks, I recommend cleaning up after yourself (e.g.,
// DELETE relevant rows).
// - I recommend using schema checks that are NOT strongly-typed. When you add a migration check,
// it'll happen to match the "latest" static schemas defined by Nexus, but that won't always be the
// case. As the schema continues to change (maybe a table you're trying to check gets a new column
// in a later version), your code should continue operating on the OLD version, and as such, should
// avoid needing any updates.
#[tokio::test]
async fn validate_data_migration_from_configured_versions() {
    // First, verify that all migration checks are covered by our test ranges
    verify_migration_coverage();

    let test_ranges = get_migration_test_ranges()
        .expect("Failed to get migration test ranges");

    println!(
        "Starting {} migration range tests concurrently",
        test_ranges.len()
    );

    // Run all migration range tests concurrently
    let mut handles = Vec::new();

    for (start_version, end_version) in test_ranges {
        let test_name = format!(
            "validate_data_migration_from_{}_to_{}",
            start_version.to_string().replace('.', "_"),
            end_version.to_string().replace('.', "_")
        );

        let handle = tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            println!(
                "Starting migration test: {} → {}",
                start_version, end_version
            );

            validate_data_migration_from_version_to_target(
                start_version.clone(),
                end_version.clone(),
                &test_name,
            )
            .await;

            let duration = start_time.elapsed();
            println!(
                "Completed migration test: {} → {} in {:.2}s",
                start_version,
                end_version,
                duration.as_secs_f64()
            );

            (start_version, end_version, duration)
        });

        handles.push(handle);
    }

    // Wait for all tests to complete and collect results
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.expect("Migration test task failed");
        results.push(result);
    }

    // Print summary
    let total_duration: std::time::Duration =
        results.iter().map(|(_, _, d)| *d).sum();
    println!("\nMigration test summary:");
    for (start, end, duration) in &results {
        println!("  {} → {}: {:.2}s", start, end, duration.as_secs_f64());
    }
    println!(
        "Total execution time: {:.2}s (if run serially: {:.2}s)",
        results.iter().map(|(_, _, d)| d.as_secs_f64()).fold(0.0, f64::max),
        total_duration.as_secs_f64()
    );
}

// Returns the InformationSchema object for a database populated via `sql`.
async fn get_information_schema(log: &Logger, sql: &str) -> InformationSchema {
    let db = TestDatabase::new_populate_nothing(&log).await;
    let crdb = db.crdb();

    let client = crdb.connect().await.expect("failed to connect");
    client.batch_execute(sql).await.expect("failed to apply SQL");

    let observed_schema = InformationSchema::new(&crdb).await;
    db.terminate().await;
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

// Helper function to get historical dbinit.sql from pre-generated files
async fn get_old_dbinit_for_version(
    version: &Version,
) -> Result<String, anyhow::Error> {
    // Path to the pre-generated schema file using SCHEMA_DIR
    let schema_file = Utf8PathBuf::from(SCHEMA_DIR)
        .join("previous-schema")
        .join(version.to_string())
        .join("dbinit.sql");

    if !schema_file.exists() {
        anyhow::bail!(
            "Pre-generated schema file not found: {:?}. Run 'cargo xtask schema generate-previous' to generate it.",
            schema_file
        );
    }

    let content = std::fs::read_to_string(&schema_file).with_context(|| {
        format!("Failed to read schema file: {:?}", schema_file)
    })?;

    Ok(content)
}

#[tokio::test]
async fn validate_migration_from_last_version() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "validate_migration_from_last_version",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let previous_version = KNOWN_VERSIONS[1].semver();

    // Validations: The first entry is the latest version,
    // and it is not the same as the version before.
    assert_eq!(KNOWN_VERSIONS[0].semver(), &LATEST_SCHEMA_VERSION);
    assert_ne!(KNOWN_VERSIONS[0].semver(), KNOWN_VERSIONS[1].semver());

    println!(
        "Testing migration from previous version {} to current version {}",
        previous_version, LATEST_SCHEMA_VERSION
    );

    // Verify that the pre-generated schema file exists for the previous version
    let previous_schema_dir =
        Utf8PathBuf::from(SCHEMA_DIR).join("previous-schema");
    let previous_version_dir =
        previous_schema_dir.join(previous_version.to_string());

    if !previous_version_dir.join("dbinit.sql").exists() {
        panic!(
            "Pre-generated schema file not found for version {}: {:?}. Run 'cargo xtask schema generate-previous' first.",
            previous_version,
            previous_version_dir.join("dbinit.sql")
        );
    }

    println!("Testing migration from version {}", previous_version);

    // Create a new database and apply the historical dbinit.sql
    let db = new_database_at_version(&logctx.log, previous_version).await;
    let crdb = db.crdb();

    // Verify we're at the expected schema version
    let current_version = query_crdb_schema_version(&crdb).await;
    assert_eq!(previous_version.to_string(), current_version);

    // Load all schema versions and apply migrations from previous_version to latest
    let all_versions = read_all_schema_versions();
    let migrations_to_apply: Vec<_> = all_versions
        .versions_range((
            std::ops::Bound::Excluded(previous_version.clone()),
            std::ops::Bound::Unbounded,
        ))
        .collect();

    // Apply each migration
    for version in migrations_to_apply {
        apply_update(log, &crdb, version, 1).await;
        assert_eq!(
            version.semver().to_string(),
            query_crdb_schema_version(&crdb).await
        );
    }

    // Verify we're now at the latest version
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // Query the schema from our migrated database
    let migrated_schema = InformationSchema::new(&crdb).await;
    let migrated_data = migrated_schema.query_all_tables(log, &crdb).await;

    db.terminate().await;

    // Create a fresh database with current dbinit.sql for comparison
    let fresh_db = TestDatabase::new_populate_schema_only(&logctx.log).await;
    let fresh_crdb = fresh_db.crdb();
    let expected_schema = InformationSchema::new(&fresh_crdb).await;
    let expected_data =
        expected_schema.query_all_tables(log, &fresh_crdb).await;

    // Compare the schemas and data
    migrated_schema.pretty_assert_eq(&expected_schema);
    assert_eq!(
        migrated_data, expected_data,
        "Data mismatch when migrating from version {}",
        previous_version
    );

    fresh_db.terminate().await;
    println!(
        "Successfully validated migration from version {}",
        previous_version
    );

    logctx.cleanup_successful();
}
