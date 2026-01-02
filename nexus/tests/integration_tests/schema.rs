// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use dropshot::test_util::LogContext;
use futures::future::BoxFuture;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use nexus_config::NexusConfig;
use nexus_config::SchemaConfig;
use nexus_db_lookup::DataStoreConnection;
use nexus_db_model::EARLIEST_SUPPORTED_VERSION;
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
use nexus_test_utils::{ControlPlaneStarter, load_test_config};
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_test_utils::dev::db::{Client, CockroachInstance};
use pretty_assertions::{assert_eq, assert_ne};
use semver::Version;
use similar_asserts;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Mutex;
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
) -> ControlPlaneStarter<'a, omicron_nexus::Server> {
    let mut starter =
        ControlPlaneStarter::<omicron_nexus::Server>::new(name, config);
    let populate = false;
    starter.start_crdb(populate).await;
    let schema_dir = Utf8PathBuf::from(SCHEMA_DIR);
    starter.config.pkg.schema = Some(SchemaConfig { schema_dir });
    starter.start_internal_dns().await;
    starter.start_external_dns().await;
    let sp_conf: Utf8PathBuf = DEFAULT_SP_SIM_CONFIG.into();
    starter.start_gateway(SwitchLocation::Switch0, None, sp_conf.clone()).await;
    starter.start_gateway(SwitchLocation::Switch1, None, sp_conf).await;
    starter.start_dendrite(SwitchLocation::Switch0).await;
    starter.start_dendrite(SwitchLocation::Switch1).await;
    starter.start_mgd(SwitchLocation::Switch0).await;
    starter.start_mgd(SwitchLocation::Switch1).await;
    starter.populate_internal_dns().await;
    starter
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
    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();

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

// Confirms that the application of all "up.sql" files, in order, is equivalent
// to applying "dbinit.sql", which should represent the latest-known schema.
#[tokio::test]
async fn dbinit_equals_sum_of_all_up() {
    let config = load_test_config();
    let logctx =
        LogContext::new("dbinit_equals_sum_of_all_up", &config.pkg.log);
    let log = &logctx.log;

    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();

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

    db.terminate().await;

    // Create a new DB with data populated from dbinit.sql for comparison
    let db = TestDatabase::new_populate_schema_only(&logctx.log).await;
    let crdb = db.crdb();
    let expected_schema = InformationSchema::new(&crdb).await;
    let expected_data = expected_schema.query_all_tables(log, &crdb).await;

    // Validate that the schema is identical
    observed_schema.pretty_assert_eq(&expected_schema);

    assert_eq!(observed_data, expected_data);
    db.terminate().await;
    logctx.cleanup_successful();
}

struct PoolAndConnection {
    pool: nexus_db_queries::db::Pool,
    conn: DataStoreConnection,
}

impl PoolAndConnection {
    async fn cleanup(self) {
        drop(self.conn);
        self.pool.terminate().await;
    }
}

struct MigrationContext<'a> {
    log: &'a Logger,

    // Postgres connection to database
    client: Client,

    // Reference to the database itself
    crdb: &'a CockroachInstance,

    // An optionally-populated "pool and connection" from before
    // a schema version upgrade.
    //
    // This can be used to validate properties of a database pool
    // before and after a particular schema migration.
    pool_and_conn: Mutex<BTreeMap<Version, PoolAndConnection>>,
}

impl MigrationContext<'_> {
    // Populates a pool and connection.
    //
    // Typically called as a part of a "before" function, to set up a connection
    // before a schema migration.
    async fn populate_pool_and_connection(&self, version: Version) {
        let pool = nexus_db_queries::db::Pool::new_single_host(
            self.log,
            &nexus_db_queries::db::Config {
                url: self.crdb.pg_config().clone(),
            },
        );
        let conn = pool.claim().await.expect("failed to get pooled connection");

        let mut map = self.pool_and_conn.lock().unwrap();
        map.insert(version, PoolAndConnection { pool, conn });
    }

    // Takes a pool and connection if they've been populated.
    fn take_pool_and_connection(&self, version: &Version) -> PoolAndConnection {
        let mut map = self.pool_and_conn.lock().unwrap();
        map.remove(version).unwrap()
    }
}

type BeforeFn = for<'a> fn(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()>;
type AfterFn = for<'a> fn(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()>;
type AtCurrentFn =
    for<'a> fn(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()>;

// Describes the operations which we might take before and after
// migrations to check that they worked.
#[derive(Default)]
struct DataMigrationFns {
    before: Option<BeforeFn>,
    after: Option<AfterFn>,
    at_current: Option<AtCurrentFn>,
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
    fn at_current(mut self, at_current: AtCurrentFn) -> Self {
        self.at_current = Some(at_current);
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

// "566C" -> "VPC". See above on "V".
const VPC: Uuid = Uuid::from_u128(0x1111566c_5c3d_4647_83b0_8f3515da7be1);

// "7213" -> Firewall "Rule"
const FW_RULE: Uuid = Uuid::from_u128(0x11117213_5c3d_4647_83b0_8f3515da7be1);

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
                VALUES (
                    '{INSTANCE4}', 'inst4', '', now(), now(), NULL,
                    '{PROJECT}', '', 'no_vmm', now(), 1, NULL, NULL, NULL,
                    2, 1073741824, 'inst1', 'sled_failures_only', NULL, 1
                );"
            ))
            .await
            .expect("failed to create instance");
        // Change one of the NULLs to an explicit 'never'.
        ctx.client
            .batch_execute(&format!(
                "UPDATE instance
                SET {COLUMN_AUTO_RESTART} = 'never'
                WHERE id = '{INSTANCE3}';"
            ))
            .await
            .expect("failed to update instance");

        // Used as a regression test against
        // https://github.com/oxidecomputer/omicron/issues/5561
        //
        // See 'at_current_101_0_0' - we create a connection here, because connections
        // may be populating an OID cache. We use the connection after the
        // schema migration to access the "instance_auto_restart" type.
        let semver = Version::new(101, 0, 0);
        ctx.populate_pool_and_connection(semver).await;
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

fn at_current_101_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        // Used as a regression test against
        // https://github.com/oxidecomputer/omicron/issues/5561
        //
        // See 'before_101_0_0' - we created a connection to validate that we can
        // access the "instance_auto_restart" type without encountering
        // OID cache poisoning.
        //
        // We care about the following:
        //
        // 1. This type was dropped and re-created in some schema migration,
        // 2. The type still exists in the latest schema
        //
        // This can happen on any schema migration, but it happens to exist
        // here. To find other schema migrations where this might be possible,
        // try the following search from within the "omicron/schema/crdb"
        // directory:
        //
        // ```bash
        // rg 'DROP TYPE IF EXISTS (.*);' --no-filename -o --replace '$1'
        //   | sort -u
        //   | xargs -I {} rg 'CREATE TYPE .*{}' dbinit.sql
        // ```
        //
        // This finds all user-defined types which have dropped at some point,
        // but which still appear in the latest schema.
        let semver = Version::new(101, 0, 0);
        let pool_and_conn = ctx.take_pool_and_connection(&semver);

        {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use nexus_db_model::Instance;
            use nexus_db_schema::schema::instance::dsl;
            use nexus_types::external_api::params;
            use omicron_common::api::external::IdentityMetadataCreateParams;
            use omicron_uuid_kinds::InstanceUuid;

            diesel::insert_into(dsl::instance)
                .values(Instance::new(
                    InstanceUuid::new_v4(),
                    Uuid::new_v4(),
                    &params::InstanceCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "hello".parse().unwrap(),
                            description: "hello".to_string(),
                        },
                        ncpus: 2.try_into().unwrap(),
                        memory: 1024_u64.try_into().unwrap(),
                        hostname: "inst".parse().unwrap(),
                        user_data: vec![],
                        ssh_public_keys: None,
                        network_interfaces:
                            params::InstanceNetworkInterfaceAttachment::Default,
                        external_ips: vec![],
                        boot_disk: None,
                        cpu_platform: None,
                        disks: Vec::new(),
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                        multicast_groups: Vec::new(),
                    },
                ))
                .execute_async(&*pool_and_conn.conn)
                .await
                .expect("failed to insert - did we poison the OID cache?");
        }

        pool_and_conn.cleanup().await;
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
        // We'll mostly reuse the instances from previous migration tests, but
        // give instance 4 a VMM in the `Stopped` state.
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vmm (
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
                    '{PROPOLIS2}',
                    now(),
                    NULL,
                    '{INSTANCE4}',
                    now(),
                    1,
                    '{SLED2}',
                    'fd00:1122:3344:104::1',
                    12400,
                    'stopped'
                );"
            ))
            .await
            .expect("inserted VMM record");
        ctx.client
            .batch_execute(&format!(
                "UPDATE instance
                SET
                    state = 'vmm',
                    active_propolis_id = '{PROPOLIS2}'
                WHERE id = '{INSTANCE4}';"
            ))
            .await
            .expect("updated instance4 record");
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

fn before_151_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create some fake inventory data to test the zone image resolver migration.
        // Insert a sled agent record without the new zone image resolver columns.
        let sled_id = Uuid::new_v4();
        let inv_collection_id = Uuid::new_v4();

        ctx.client
            .batch_execute(&format!(
                "
        INSERT INTO omicron.public.inv_sled_agent
          (inv_collection_id, time_collected, source, sled_id, sled_agent_ip,
           sled_agent_port, sled_role, usable_hardware_threads, usable_physical_ram,
           reservoir_size, reconciler_status_kind)
        VALUES
          ('{inv_collection_id}', now(), 'test-source', '{sled_id}', '192.168.1.1',
           8080, 'gimlet', 32, 68719476736, 1073741824, 'not-yet-run');
        ",
                inv_collection_id = inv_collection_id,
                sled_id = sled_id
            ))
            .await
            .expect("inserted pre-migration inv_sled_agent data");
    })
}

fn after_151_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify that the zone image resolver columns have been added with
        // correct defaults.
        let rows = ctx
            .client
            .query(
                "SELECT zone_manifest_boot_disk_path, zone_manifest_source,
                        zone_manifest_mupdate_id, zone_manifest_boot_disk_error,
                        mupdate_override_boot_disk_path,
                        mupdate_override_id, mupdate_override_boot_disk_error
                 FROM omicron.public.inv_sled_agent
                 ORDER BY time_collected",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_agent data");

        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        // Check that path fields have the expected default message.
        let zone_manifest_path: String =
            row.get("zone_manifest_boot_disk_path");
        let mupdate_override_path: String =
            row.get("mupdate_override_boot_disk_path");
        assert_eq!(zone_manifest_path, "old-collection-data-missing");
        assert_eq!(mupdate_override_path, "old-collection-data-missing");

        // Check that the zone manifest and mupdate override source fields are
        // NULL.
        let zone_manifest_source: Option<AnySqlType> =
            row.get("zone_manifest_source");
        assert_eq!(zone_manifest_source, None);
        let zone_manifest_id: Option<Uuid> =
            row.get("zone_manifest_mupdate_id");
        let mupdate_override_id: Option<Uuid> = row.get("mupdate_override_id");
        assert_eq!(zone_manifest_id, None);
        assert_eq!(mupdate_override_id, None);

        // Check that error fields have the expected default message.
        let zone_manifest_error: String =
            row.get("zone_manifest_boot_disk_error");
        let mupdate_override_error: String =
            row.get("mupdate_override_boot_disk_error");
        assert_eq!(zone_manifest_error, "old collection, data missing");
        assert_eq!(mupdate_override_error, "old collection, data missing");
    })
}

fn before_155_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::project::*;
        use nexus_db_queries::db::fixed_data::vpc::*;
        use omicron_common::address::SERVICE_VPC_IPV6_PREFIX;

        // First, create the oxide-services vpc. The migration will only add
        // a new rule if it detects the presence of *this* VPC entry (i.e.,
        // RSS has run before).
        let svc_vpc = *SERVICES_VPC_ID;
        let svc_proj = *SERVICES_PROJECT_ID;
        let svc_router = *SERVICES_VPC_ROUTER_ID;
        let svc_vpc_name = &SERVICES_DB_NAME;
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vpc (
                    id, name, description, time_created, time_modified,
                    project_id, system_router_id, dns_name,
                    vni, ipv6_prefix, firewall_gen, subnet_gen
                )
                VALUES (
                    '{svc_vpc}', '{svc_vpc_name}', '', now(), now(),
                    '{svc_proj}', '{svc_router}', '{svc_vpc_name}',
                    100, '{}', 0, 0
                );",
                *SERVICE_VPC_IPV6_PREFIX
            ))
            .await
            .expect("failed to create services vpc record");

        // Second, create a firewall rule in a dummied-out VPC to validate
        // that we correctly convert from ENUM[] to STRING[].
        ctx.client
            .batch_execute(&format!(
                "INSERT INTO vpc_firewall_rule (
                    id,
                    name, description,
                    time_created, time_modified, vpc_id, status, direction,
                    targets, filter_protocols,
                    action, priority
                )
                VALUES (
                    '{FW_RULE}',
                    'test-fw', '',
                    now(), now(), '{VPC}', 'enabled', 'outbound',
                    ARRAY['vpc:fiction'], ARRAY['ICMP', 'TCP', 'UDP'],
                    'allow', 1234
                );"
            ))
            .await
            .expect("failed to create firewall rule");
    })
}

fn after_155_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async {
        use nexus_db_queries::db::fixed_data::vpc::*;
        // Firstly -- has the new firewall rule been added?
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, description
                    FROM vpc_firewall_rule
                    WHERE name = 'nexus-icmp' and vpc_id = '{}'",
                    *SERVICES_VPC_ID
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let records = process_rows(&rows);

        assert_eq!(records.len(), 1);

        // Secondly, have FW_RULE's filter_protocols been correctly stringified?
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT filter_protocols
                    FROM vpc_firewall_rule
                    WHERE id = '{FW_RULE}'"
                ),
                &[],
            )
            .await
            .expect("failed to load instance auto-restart policies");
        let records = process_rows(&rows);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].values,
            vec![ColumnValue::new(
                "filter_protocols",
                AnySqlType::TextArray(vec![
                    "icmp".into(),
                    "tcp".into(),
                    "udp".into(),
                ])
            )],
        );
    })
}

mod migration_156 {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;

    const INV_COLLECTION_ID_1: &str = "5cb42909-d94a-4903-be72-330eea0325d9";
    const INV_COLLECTION_ID_2: &str = "142b62c2-9348-4530-9eed-7077351fb94b";
    const SLED_ID_1: &str = "3b5b7861-03aa-420a-a057-0a14347dc4c0";
    const SLED_ID_2: &str = "de7ab4c0-30d4-4e9e-b620-3a959a9d59dd";
    const SLED_CONFIG_ID_1: &str = "a1637607-c49e-4658-b637-96473a72bb32";
    const SLED_CONFIG_ID_2: &str = "695de2f0-9c09-42b0-a22a-1e5b783a38c0";
    const SLED_CONFIG_ID_3: &str = "50a8d074-879b-4c59-a2ef-700156e49a97";

    pub(super) fn before<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Insert these tuples:
            //
            // (collection 1, sled 1, sled config ID 1)
            // (collection 1, sled 2, NULL)
            // (collection 2, sled 1, sled config ID 2)
            // (collection 2, sled 2, sled config ID 3)
            ctx.client
                .batch_execute(&format!(
                    "
                    INSERT INTO omicron.public.inv_sled_agent (
                        inv_collection_id, time_collected, source, sled_id,
                        sled_agent_ip, sled_agent_port, sled_role,
                        usable_hardware_threads, usable_physical_ram,
                        reservoir_size, last_reconciliation_sled_config,
                        reconciler_status_kind, zone_manifest_boot_disk_path,
                        zone_manifest_source, mupdate_override_boot_disk_path
                    )
                    VALUES (
                        '{INV_COLLECTION_ID_1}', now(), 'test-source',
                        '{SLED_ID_1}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_1}',
                        'not-yet-run', '/tmp', 'sled-agent', '/tmp'
                    ), (
                        '{INV_COLLECTION_ID_1}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, NULL,
                        'not-yet-run', '/tmp', 'sled-agent', '/tmp'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_1}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_2}',
                        'not-yet-run', '/tmp', 'sled-agent', '/tmp'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_3}',
                        'not-yet-run', '/tmp', 'sled-agent', '/tmp'
                    );
                    "
                ))
                .await
                .expect("inserted pre-migration data");
        })
    }

    pub(super) fn after<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Verify that the new inv_sled_config_reconciler table has 3 rows
            // corresponding to the three inv_sled_agent rows that had a
            // non-NULL last_reconciliation_sled_config inserted in `before()`.
            let expected = {
                let mut expected = BTreeSet::<(Uuid, Uuid, Uuid)>::new();
                expected.insert((
                    INV_COLLECTION_ID_1.parse().unwrap(),
                    SLED_ID_1.parse().unwrap(),
                    SLED_CONFIG_ID_1.parse().unwrap(),
                ));
                expected.insert((
                    INV_COLLECTION_ID_2.parse().unwrap(),
                    SLED_ID_1.parse().unwrap(),
                    SLED_CONFIG_ID_2.parse().unwrap(),
                ));
                expected.insert((
                    INV_COLLECTION_ID_2.parse().unwrap(),
                    SLED_ID_2.parse().unwrap(),
                    SLED_CONFIG_ID_3.parse().unwrap(),
                ));
                expected
            };

            let rows = ctx
                .client
                .query(
                    "
                    SELECT
                    inv_collection_id, sled_id, last_reconciled_config,
                    boot_disk_slot, boot_disk_error, boot_partition_a_error,
                    boot_partition_b_error
                    FROM omicron.public.inv_sled_config_reconciler
                    ",
                    &[],
                )
                .await
                .expect("queried post-migration data");

            let mut seen = BTreeSet::new();
            for row in rows {
                let inv_collection_id: Uuid = row.get("inv_collection_id");
                let sled_id: Uuid = row.get("sled_id");
                let last_reconciled_config: Uuid =
                    row.get("last_reconciled_config");
                let boot_disk_slot: Option<i16> = row.get("boot_disk_slot");
                let boot_disk_error: Option<String> =
                    row.get("boot_disk_error");
                let boot_partition_a_error: Option<String> =
                    row.get("boot_partition_a_error");
                let boot_partition_b_error: Option<String> =
                    row.get("boot_partition_b_error");

                seen.insert((
                    inv_collection_id,
                    sled_id,
                    last_reconciled_config,
                ));

                // Migration should have populated all the error fields.
                assert_eq!(boot_disk_slot, None);
                assert_eq!(
                    boot_disk_error.as_deref(),
                    Some("old collection, data missing")
                );
                assert_eq!(
                    boot_partition_a_error.as_deref(),
                    Some("old collection, data missing")
                );
                assert_eq!(
                    boot_partition_b_error.as_deref(),
                    Some("old collection, data missing")
                );
            }

            assert_eq!(seen, expected);
        })
    }
}

const BP_OXIMETER_READ_POLICY_ID_0: &str =
    "5cb42909-d94a-4903-be72-330eea0325d9";
const BP_OXIMETER_READ_POLICY_ID_1: &str =
    "142b62c2-9348-4530-9eed-7077351fb94b";
const BP_OXIMETER_READ_POLICY_ID_2: &str =
    "3b5b7861-03aa-420a-a057-0a14347dc4c0";
const BP_OXIMETER_READ_POLICY_ID_3: &str =
    "de7ab4c0-30d4-4e9e-b620-3a959a9d59dd";

// Insert two blueprints and 4 oximeter read policies, two of which do not have
// a corresponding blueprint
fn before_164_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(
                &format!("
        INSERT INTO omicron.public.blueprint
          (id, parent_blueprint_id, time_created, creator, comment, internal_dns_version, external_dns_version, cockroachdb_fingerprint, cockroachdb_setting_preserve_downgrade, target_release_minimum_generation)
        VALUES
          (
            '{BP_OXIMETER_READ_POLICY_ID_0}', NULL, now(), 'bob', 'hi', 1, 1, 'fingerprint', NULL, 1
          ),
          (
            '{BP_OXIMETER_READ_POLICY_ID_1}', NULL, now(), 'bab', 'hi', 1, 1, 'fingerprint', NULL, 1
          );

        INSERT INTO omicron.public.bp_oximeter_read_policy
          (blueprint_id, version, oximeter_read_mode)
        VALUES
          ('{BP_OXIMETER_READ_POLICY_ID_0}', 1, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_1}', 2, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_2}', 3, 'cluster'),
          ('{BP_OXIMETER_READ_POLICY_ID_3}', 4, 'cluster')
        "),
            )
            .await
            .expect("failed to insert pre-migration rows for 163");
    })
}

// Validate that rows that do not have a corresponding blueprint are gone
fn after_164_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT blueprint_id FROM omicron.public.bp_oximeter_read_policy ORDER BY blueprint_id;",
                &[],
            )
            .await
            .expect("failed to query post-migration bp_oximeter_read_policy table");
        assert_eq!(rows.len(), 2);

        let id_1: Uuid = (&rows[0]).get::<&str, Uuid>("blueprint_id");
        assert_eq!(id_1.to_string(), BP_OXIMETER_READ_POLICY_ID_1);

        let id_2: Uuid = (&rows[1]).get::<&str, Uuid>("blueprint_id");
        assert_eq!(id_2.to_string(), BP_OXIMETER_READ_POLICY_ID_0);
    })
}

const PORT_SETTINGS_ID_165_0: &str = "1e700b64-79e0-4515-9771-bcc2391b6d4d";
const PORT_SETTINGS_ID_165_1: &str = "c6b015ff-1c98-474f-b9e9-dfc30546094f";
const PORT_SETTINGS_ID_165_2: &str = "8b777d9b-62a3-4c4d-b0b7-314315c2a7fc";
const PORT_SETTINGS_ID_165_3: &str = "7c675e89-74b1-45da-9577-cf75f028107a";
const PORT_SETTINGS_ID_165_4: &str = "e2413d63-9307-4918-b9c4-bce959c63042";
const PORT_SETTINGS_ID_165_5: &str = "05df929f-1596-42f4-b78f-aebb5d7028c4";

// Insert records using the `local_pref` column before it's renamed and its
// database type is changed from INT8 to INT2. The receiving Rust type is u8
// so 2 records are outside the u8 range, 2 records are at the edge of the u8
// range, and 1 record is within the u8 range.
fn before_165_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .batch_execute(&format!("
                INSERT INTO omicron.public.switch_port_settings_route_config
                  (port_settings_id, interface_name, dst, gw, vid, local_pref)
                VALUES
                  (
                    '{PORT_SETTINGS_ID_165_0}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, -1
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_1}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 0
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_2}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 128
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_3}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 255
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_4}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, 256
                  ),
                  (
                    '{PORT_SETTINGS_ID_165_5}', 'phy0', '0.0.0.0/0', '0.0.0.0', NULL, NULL
                  );
              "),
            )
            .await
            .expect("failed to insert pre-migration rows for 165");
    })
}

// Query the records using the new `rib_priority` column and assert that the
// values were correctly clamped within the u8 range.
fn after_165_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT * FROM omicron.public.switch_port_settings_route_config;",
                &[],
            )
            .await
            .expect("failed to query post-migration switch_port_settings_route_config table");
        assert_eq!(rows.len(), 6);

        let records: HashMap<Uuid, Option<i16>> = HashMap::from([
            (Uuid::from_str(PORT_SETTINGS_ID_165_0).unwrap(), Some(0)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_1).unwrap(), Some(0)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_2).unwrap(), Some(128)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_3).unwrap(), Some(255)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_4).unwrap(), Some(255)),
            (Uuid::from_str(PORT_SETTINGS_ID_165_5).unwrap(), None),
        ]);

        for row in rows {
            let port_settings_id = row.get::<&str, Uuid>("port_settings_id");
            let rib_priority_got = row.get::<&str, Option<i16>>("rib_priority");

            let rib_priority_want = records
                .get(&port_settings_id)
                .expect("unexpected port_settings_id value when querying switch_port_settings_route_config");
            assert_eq!(rib_priority_got, *rib_priority_want);
        }
    })
}

fn before_171_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create test data in inv_sled_config_reconciler table before the new columns are added.
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot)
                 VALUES
                 ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222',
                  '33333333-3333-3333-3333-333333333333', 0);",
                &[],
            )
            .await
            .expect("inserted pre-migration rows for 171");
    })
}

fn after_171_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // After the migration, the new columns should exist and be NULL for existing rows.
        let rows = ctx
            .client
            .query(
                "SELECT
                   clear_mupdate_override_boot_success,
                   clear_mupdate_override_boot_error,
                   clear_mupdate_override_non_boot_message
                 FROM omicron.public.inv_sled_config_reconciler
                 WHERE sled_id = '22222222-2222-2222-2222-222222222222';",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_config_reconciler");
        assert_eq!(rows.len(), 1);

        // All new columns should be NULL for existing rows.
        let boot_success: Option<AnySqlType> =
            (&rows[0]).get("clear_mupdate_override_boot_success");
        assert!(boot_success.is_none());

        let boot_error: Option<String> =
            (&rows[0]).get("clear_mupdate_override_boot_error");
        assert!(boot_error.is_none());

        let non_boot_message: Option<String> =
            (&rows[0]).get("clear_mupdate_override_non_boot_message");
        assert!(non_boot_message.is_none());

        // Test that the constraint allows valid combinations.
        // Case 1: All NULL (should work).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '33333333-3333-3333-3333-333333333333',
                  'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 1, NULL, NULL, NULL);",
                &[],
            )
            .await
            .expect("inserted row with all NULL clear_mupdate_override columns");

        // Case 2: Success case (boot_success NOT NULL, boot_error NULL, non_boot_message NOT NULL).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('cccccccc-cccc-cccc-cccc-cccccccccccc', '44444444-4444-4444-4444-444444444444',
                  'dddddddd-dddd-dddd-dddd-dddddddddddd', 0,
                  'cleared', NULL, 'Non-boot disk cleared successfully');",
                &[],
            )
            .await
            .expect("inserted row with success case clear_mupdate_override columns");

        // Case 3: Error case (boot_success NULL, boot_error NOT NULL, non_boot_message NOT NULL).
        ctx.client
            .execute(
                "INSERT INTO omicron.public.inv_sled_config_reconciler
                 (inv_collection_id, sled_id, last_reconciled_config, boot_disk_slot,
                  clear_mupdate_override_boot_success, clear_mupdate_override_boot_error,
                  clear_mupdate_override_non_boot_message)
                 VALUES
                 ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', '55555555-5555-5555-5555-555555555555',
                  'ffffffff-ffff-ffff-ffff-ffffffffffff', 1,
                  NULL, 'Failed to clear mupdate override', 'Non-boot disk operation failed');",
                &[],
            )
            .await
            .expect("inserted row with error case clear_mupdate_override columns");
    })
}

const NEXUS_ID_185_0: &str = "387433f9-1473-4ca2-b156-9670452985e0";
const EXPUNGED_NEXUS_ID_185_0: &str = "287433f9-1473-4ca2-b156-9670452985e0";
const OLD_NEXUS_ID_185_0: &str = "187433f9-1473-4ca2-b156-9670452985e0";

const BP_ID_185_0: &str = "5a5ff941-3b5a-403b-9fda-db2049f4c736";
const OLD_BP_ID_185_0: &str = "4a5ff941-3b5a-403b-9fda-db2049f4c736";

fn before_185_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Create a blueprint which contains a Nexus - we'll use this for the migration.
        //
        // It also contains an exupnged Nexus, which should be ignored.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_target
                     (version, blueprint_id, enabled, time_made_target)
                     VALUES
                     (1, '{BP_ID_185_0}', true, now());",
                ),
                &[],
            )
            .await
            .expect("inserted bp_target rows for 182");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_omicron_zone (
                         blueprint_id, sled_id, id, zone_type,
                         primary_service_ip, primary_service_port,
                         second_service_ip, second_service_port,
                         dataset_zpool_name, bp_nic_id,
                         dns_gz_address, dns_gz_address_index,
                         ntp_ntp_servers, ntp_dns_servers, ntp_domain,
                         nexus_external_tls, nexus_external_dns_servers,
                         snat_ip, snat_first_port, snat_last_port,
                         external_ip_id, filesystem_pool, disposition,
                         disposition_expunged_as_of_generation,
                         disposition_expunged_ready_for_cleanup,
                         image_source, image_artifact_sha256
                     )
                     VALUES (
                         '{BP_ID_185_0}', gen_random_uuid(), '{NEXUS_ID_185_0}',
                         'nexus', '192.168.1.10', 8080, NULL, NULL, NULL, NULL,
                         NULL, NULL, NULL, NULL, NULL, false, ARRAY[]::INET[],
                         NULL, NULL, NULL, NULL, gen_random_uuid(),
                         'in_service', NULL, false, 'install_dataset', NULL
                     ),
                     (
                         '{BP_ID_185_0}', gen_random_uuid(),
                         '{EXPUNGED_NEXUS_ID_185_0}', 'nexus', '192.168.1.11',
                         8080, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         NULL, false, ARRAY[]::INET[], NULL, NULL, NULL, NULL,
                         gen_random_uuid(), 'expunged', 1, false,
                         'install_dataset', NULL
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted bp_omicron_zone rows for 182");

        // ALSO create an old blueprint, which isn't the latest target.
        //
        // We should ignore this one! No rows should be inserted for old data.
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_target
                     (version, blueprint_id, enabled, time_made_target)
                     VALUES
                     (0, '{OLD_BP_ID_185_0}', true, now());",
                ),
                &[],
            )
            .await
            .expect("inserted bp_target rows for 182");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.bp_omicron_zone (
                         blueprint_id, sled_id, id, zone_type,
                         primary_service_ip, primary_service_port,
                         second_service_ip, second_service_port,
                         dataset_zpool_name, bp_nic_id,
                         dns_gz_address, dns_gz_address_index,
                         ntp_ntp_servers, ntp_dns_servers, ntp_domain,
                         nexus_external_tls, nexus_external_dns_servers,
                         snat_ip, snat_first_port, snat_last_port,
                         external_ip_id, filesystem_pool, disposition,
                         disposition_expunged_as_of_generation,
                         disposition_expunged_ready_for_cleanup,
                         image_source, image_artifact_sha256
                     )
                     VALUES (
                         '{OLD_BP_ID_185_0}', gen_random_uuid(),
                         '{OLD_NEXUS_ID_185_0}', 'nexus', '192.168.1.10', 8080,
                         NULL, NULL, NULL, NULL,
                         NULL, NULL, NULL, NULL, NULL,
                         false, ARRAY[]::INET[], NULL, NULL, NULL,
                         NULL, gen_random_uuid(), 'in_service',
                         NULL, false, 'install_dataset', NULL
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted bp_omicron_zone rows for 182");
    })
}

fn after_185_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // After the migration, the new row should be created - only for Nexuses
        // in the latest blueprint.
        //
        // Note that "OLD_NEXUS_ID_185_0" doesn't get a row - it's in an old
        // blueprint.
        let rows = ctx
            .client
            .query(
                "SELECT
                   nexus_id,
                   last_drained_blueprint_id,
                   state
                 FROM omicron.public.db_metadata_nexus;",
                &[],
            )
            .await
            .expect("queried post-migration inv_sled_config_reconciler");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        // Create a new row for the Nexuses in the target blueprint
        assert_eq!(
            row.values[0].expect("nexus_id").unwrap(),
            &AnySqlType::Uuid(NEXUS_ID_185_0.parse().unwrap())
        );
        assert_eq!(row.values[1].expect("last_drained_blueprint_id"), None);
        assert_eq!(
            row.values[2].expect("state").unwrap(),
            &AnySqlType::Enum(SqlEnum::from((
                "db_metadata_nexus_state",
                "active"
            )))
        );
    })
}

const NEGATIVE_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce1);
const POSITIVE_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce2);
const MIXED_QUOTA: Uuid =
    Uuid::from_u128(0x00006001_5c3d_4647_83b0_8f351dda7ce3);

fn before_188_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.silo_quotas (
                        silo_id,
                        time_created,
                        time_modified,
                        cpus,
                        memory_bytes,
                        storage_bytes
                    )
                     VALUES
                     ('{NEGATIVE_QUOTA}', now(), now(), -1, -2, -3),
                     ('{POSITIVE_QUOTA}', now(), now(), 1, 2, 3),
                     ('{MIXED_QUOTA}', now(), now(), -1, 0, 3);",
                ),
                &[],
            )
            .await
            .expect("inserted silo_quotas");
    })
}

fn after_188_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows = ctx
            .client
            .query(
                "SELECT
                   silo_id,
                   cpus,
                   memory_bytes,
                   storage_bytes
                 FROM omicron.public.silo_quotas ORDER BY silo_id ASC;",
                &[],
            )
            .await
            .expect("queried post-migration positive-quotas");

        fn check_quota(
            row: &Row,
            id: Uuid,
            cpus: i64,
            memory: i64,
            storage: i64,
        ) {
            assert_eq!(
                row.values[0].expect("silo_id").unwrap(),
                &AnySqlType::Uuid(id)
            );
            assert_eq!(
                row.values[1].expect("cpus").unwrap(),
                &AnySqlType::Int8(cpus)
            );
            assert_eq!(
                row.values[2].expect("memory_bytes").unwrap(),
                &AnySqlType::Int8(memory)
            );
            assert_eq!(
                row.values[3].expect("storage_bytes").unwrap(),
                &AnySqlType::Int8(storage)
            );
        }

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 3);

        check_quota(&rows[0], NEGATIVE_QUOTA, 0, 0, 0);
        check_quota(&rows[1], POSITIVE_QUOTA, 1, 2, 3);
        check_quota(&rows[2], MIXED_QUOTA, 0, 0, 3);
    })
}

fn before_207_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                "INSERT INTO omicron.public.disk (
                    id,
                    name,
                    description,
                    time_created,
                    time_modified,
                    time_deleted,

                    rcgen,
                    project_id,

                    volume_id,

                    disk_state,
                    attach_instance_id,
                    state_generation,
                    slot,
                    time_state_updated,

                    size_bytes,
                    block_size,
                    origin_snapshot,
                    origin_image,

                    pantry_address
                  )
                  VALUES
                  (
                    'd8b7ba02-4bd5-417d-84d3-67b4e65bec70',
                    'regular',
                    'just a disk',
                    '2025-10-21T20:26:25+0000',
                    '2025-10-21T20:26:25+0000',
                    NULL,

                    0,
                    'd904d22e-cc45-4b2e-b37f-5864d8eba323',

                    '1e17286f-107d-494c-8b2d-904b70d5b706',

                    'attached',
                    '9c1f6ee5-478f-4e6a-b481-be8b69f1daab',
                    75,
                    0,
                    '2025-10-21T20:27:11+0000',

                    1073741824,
                    '512',
                    'b017fe60-9a4a-4fe6-a7d3-4a7c3ab23a98',
                    NULL,

                    '[fd00:1122:3344:101::7]:4567'
                  ),
                  (
                    '336ed8ca-9bbf-4db9-9f2d-627f8d156f91',
                    'deleted',
                    'should migrate deleted disks too, even if they are old',
                    '2024-10-21T20:26:25+0000',
                    '2024-10-21T20:26:25+0000',
                    '2025-10-15T21:38:05+0000',

                    2,
                    'c9cfb288-a39e-4ebb-ac27-b26c9248a46a',

                    '0a1d0a38-f927-4260-ad23-7f9c04277a23',

                    'detached',
                    NULL,
                    75786,
                    1,
                    '2025-10-14T20:27:11+0000',

                    1073741824,
                    '4096',
                    NULL,
                    '971395a7-4206-4dc4-ba58-e2402724270a',

                    NULL
                  );",
                &[],
            )
            .await
            .expect("inserted disks");
    })
}

fn after_207_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // first disk

        let rows = ctx
            .client
            .query(
                "SELECT
                   id,
                   name,
                   description,
                   time_created,
                   time_modified,
                   time_deleted,

                   rcgen,
                   project_id,

                   disk_state,
                   attach_instance_id,
                   state_generation,
                   slot,
                   time_state_updated,

                   size_bytes,
                   block_size
                 FROM
                   omicron.public.disk
                 WHERE
                   id = 'd8b7ba02-4bd5-417d-84d3-67b4e65bec70'
                 ;",
                &[],
            )
            .await
            .expect("queried disk");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[5].expect("time_deleted").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("id").unwrap(),
                row.values[1].expect("name").unwrap(),
                row.values[2].expect("description").unwrap(),
                row.values[3].expect("time_created").unwrap(),
                row.values[4].expect("time_modified").unwrap(),
                row.values[6].expect("rcgen").unwrap(),
                row.values[7].expect("project_id").unwrap(),
                row.values[8].expect("disk_state").unwrap(),
                row.values[9].expect("attach_instance_id").unwrap(),
                row.values[10].expect("state_generation").unwrap(),
                row.values[11].expect("slot").unwrap(),
                row.values[12].expect("time_state_updated").unwrap(),
                row.values[13].expect("size_bytes").unwrap(),
                row.values[14].expect("block_size").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "d8b7ba02-4bd5-417d-84d3-67b4e65bec70".parse().unwrap()
                ),
                &AnySqlType::String("regular".to_string()),
                &AnySqlType::String("just a disk".to_string()),
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::Int8(0),
                &AnySqlType::Uuid(
                    "d904d22e-cc45-4b2e-b37f-5864d8eba323".parse().unwrap()
                ),
                &AnySqlType::String("attached".to_string()),
                &AnySqlType::Uuid(
                    "9c1f6ee5-478f-4e6a-b481-be8b69f1daab".parse().unwrap()
                ),
                &AnySqlType::Int8(75),
                &AnySqlType::Int2(0),
                &AnySqlType::DateTime,
                &AnySqlType::Int8(1073741824),
                &AnySqlType::Enum(SqlEnum::from(("block_size", "512"))),
            ],
        );

        let rows = ctx
            .client
            .query(
                "SELECT
                   disk_id,
                   volume_id,
                   origin_snapshot,
                   origin_image,
                   pantry_address
                 FROM
                   omicron.public.disk_type_crucible
                 WHERE
                   disk_id = 'd8b7ba02-4bd5-417d-84d3-67b4e65bec70'
                 ;",
                &[],
            )
            .await
            .expect("queried disk_type_crucible");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[3].expect("origin_image").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("disk_id").unwrap(),
                row.values[1].expect("volume_id").unwrap(),
                row.values[2].expect("origin_snapshot").unwrap(),
                row.values[4].expect("pantry_address").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "d8b7ba02-4bd5-417d-84d3-67b4e65bec70".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "1e17286f-107d-494c-8b2d-904b70d5b706".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "b017fe60-9a4a-4fe6-a7d3-4a7c3ab23a98".parse().unwrap()
                ),
                &AnySqlType::String("[fd00:1122:3344:101::7]:4567".to_string()),
            ]
        );

        // second disk

        let rows = ctx
            .client
            .query(
                "SELECT
                   id,
                   name,
                   description,
                   time_created,
                   time_modified,
                   time_deleted,

                   rcgen,
                   project_id,

                   disk_state,
                   attach_instance_id,
                   state_generation,
                   slot,
                   time_state_updated,

                   size_bytes,
                   block_size
                 FROM
                   omicron.public.disk
                 WHERE
                   id = '336ed8ca-9bbf-4db9-9f2d-627f8d156f91'
                 ;",
                &[],
            )
            .await
            .expect("queried disk");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[9].expect("attach_instance_id").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("id").unwrap(),
                row.values[1].expect("name").unwrap(),
                row.values[2].expect("description").unwrap(),
                row.values[3].expect("time_created").unwrap(),
                row.values[4].expect("time_modified").unwrap(),
                row.values[5].expect("time_deleted").unwrap(),
                row.values[6].expect("rcgen").unwrap(),
                row.values[7].expect("project_id").unwrap(),
                row.values[8].expect("disk_state").unwrap(),
                row.values[10].expect("state_generation").unwrap(),
                row.values[11].expect("slot").unwrap(),
                row.values[12].expect("time_state_updated").unwrap(),
                row.values[13].expect("size_bytes").unwrap(),
                row.values[14].expect("block_size").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "336ed8ca-9bbf-4db9-9f2d-627f8d156f91".parse().unwrap()
                ),
                &AnySqlType::String("deleted".to_string()),
                &AnySqlType::String(
                    "should migrate deleted disks too, even if they are old"
                        .to_string()
                ),
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::DateTime,
                &AnySqlType::Int8(2),
                &AnySqlType::Uuid(
                    "c9cfb288-a39e-4ebb-ac27-b26c9248a46a".parse().unwrap()
                ),
                &AnySqlType::String("detached".to_string()),
                &AnySqlType::Int8(75786),
                &AnySqlType::Int2(1),
                &AnySqlType::DateTime,
                &AnySqlType::Int8(1073741824),
                &AnySqlType::Enum(SqlEnum::from(("block_size", "4096"))),
            ],
        );

        let rows = ctx
            .client
            .query(
                "SELECT
                   disk_id,
                   volume_id,
                   origin_snapshot,
                   origin_image,
                   pantry_address
                 FROM
                   omicron.public.disk_type_crucible
                 WHERE
                   disk_id = '336ed8ca-9bbf-4db9-9f2d-627f8d156f91'
                 ;",
                &[],
            )
            .await
            .expect("queried disk_type_crucible");

        let rows = process_rows(&rows);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];

        assert!(row.values[2].expect("origin_snapshot").is_none());
        assert!(row.values[4].expect("pantry_address").is_none());

        assert_eq!(
            vec![
                row.values[0].expect("disk_id").unwrap(),
                row.values[1].expect("volume_id").unwrap(),
                row.values[3].expect("origin_image").unwrap(),
            ],
            vec![
                &AnySqlType::Uuid(
                    "336ed8ca-9bbf-4db9-9f2d-627f8d156f91".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "0a1d0a38-f927-4260-ad23-7f9c04277a23".parse().unwrap()
                ),
                &AnySqlType::Uuid(
                    "971395a7-4206-4dc4-ba58-e2402724270a".parse().unwrap()
                ),
            ]
        );
    })
}

const SP_RESTART_ID: Uuid =
    Uuid::from_u128(0x0358a488_3dd1_4da2_a783_570f3a149058);
const HOST_RESTART_ID: Uuid =
    Uuid::from_u128(0x786d5a82_da41_4d28_9901_650bf168e0f3);
const HOST_EREPORT_SLED_ID: Uuid =
    Uuid::from_u128(0xdf221406_85db_414b_bb3f_5b1323592bbe);
const EREPORT_SLED_SERIAL: &str = "BRM6900420";
const GIMLET_PART_NUMBER: &str = "913-0000019";

fn before_210_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.sp_ereport (
                        restart_id,
                        ena,
                        time_deleted,
                        time_collected,
                        collector_id,
                        sp_type,
                        sp_slot,
                        serial_number,
                        part_number,
                        class,
                        report
                    )
                     VALUES (
                         '{SP_RESTART_ID}',
                         1,
                         NULL,
                         now(),
                         gen_random_uuid(),
                         'sled',
                         19,
                         '{EREPORT_SLED_SERIAL}',
                         '{GIMLET_PART_NUMBER}',
                         'my.cool.sp.ereport',
                         '{{}}'
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted sp_ereport");
        ctx.client
            .execute(
                &format!(
                    "INSERT INTO omicron.public.host_ereport (
                        restart_id,
                        ena,
                        time_deleted,
                        time_collected,
                        collector_id,
                        sled_id,
                        sled_serial,
                        class,
                        report,
                        part_number
                    )
                     VALUES (
                         '{HOST_RESTART_ID}',
                         1,
                         NULL,
                         now(),
                         gen_random_uuid(),
                         '{HOST_EREPORT_SLED_ID}',
                         '{EREPORT_SLED_SERIAL}',
                         'my.cool.host.ereport',
                         '{{}}',
                         '{GIMLET_PART_NUMBER}'
                     );"
                ),
                &[],
            )
            .await
            .expect("inserted host_ereport");
    })
}

fn after_210_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let host_rows = ctx
            .client
            .query(
                "SELECT
                   restart_id,
                   ena,
                   reporter,
                   sled_id,
                   sp_type,
                   sp_slot,
                   serial_number,
                   part_number
                 FROM omicron.public.ereport
                 WHERE
                    reporter = 'host';",
                &[],
            )
            .await
            .expect("queried post-migration host ereports");
        let rows = process_rows(&host_rows);
        assert_eq!(rows.len(), 1);
        check_row(
            &rows[0],
            HOST_RESTART_ID,
            1,
            "host",
            Some(HOST_EREPORT_SLED_ID),
            None,
            None,
        );

        let sp_rows = ctx
            .client
            .query(
                "SELECT
                   restart_id,
                   ena,
                   reporter,
                   sled_id,
                   sp_type,
                   sp_slot,
                   serial_number,
                   part_number
                 FROM omicron.public.ereport
                 WHERE
                    reporter = 'sp';",
                &[],
            )
            .await
            .expect("queried post-migration sp ereports");
        let rows = process_rows(&sp_rows);
        assert_eq!(rows.len(), 1);
        check_row(
            &rows[0],
            SP_RESTART_ID,
            1,
            "sp",
            None,
            Some("sled"),
            Some(19),
        );

        fn check_row(
            row: &Row,
            restart_id: Uuid,
            ena: i64,
            reporter: &str,
            sled_id: Option<Uuid>,
            sp_type: Option<&str>,
            sp_slot: Option<i32>,
        ) {
            assert_eq!(
                row.values[0].expect("restart_id").unwrap(),
                &AnySqlType::Uuid(restart_id)
            );
            assert_eq!(
                row.values[1].expect("ena").unwrap(),
                &AnySqlType::Int8(ena)
            );
            assert_eq!(
                row.values[2].expect("reporter").unwrap(),
                &AnySqlType::Enum(SqlEnum::from(("ereporter_type", reporter))),
            );
            assert_eq!(
                row.values[3].expect("sled_id"),
                sled_id.map(AnySqlType::Uuid).as_ref(),
            );
            assert_eq!(
                row.values[4].expect("sp_type"),
                sp_type
                    .map(|t| AnySqlType::Enum(SqlEnum::from(("sp_type", t))))
                    .as_ref(),
            );
            assert_eq!(
                row.values[5].expect("sp_slot"),
                sp_slot.map(AnySqlType::Int4).as_ref(),
            );
            assert_eq!(
                row.values[6].expect("serial_number").unwrap(),
                &AnySqlType::String(EREPORT_SLED_SERIAL.to_string()),
            );
            assert_eq!(
                row.values[7].expect("part_number").unwrap(),
                &AnySqlType::String(GIMLET_PART_NUMBER.to_string()),
            );
        }
    })
}

mod migration_211 {
    use super::*;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::SLED_PREFIX;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeSet;

    // randomly-generated IDs
    const SLED_ID_1: &str = "48e1d10b-3ec8-4abd-95e2-5f37f79da546";
    const SLED_ID_2: &str = "2144219e-d989-4929-b0b3-db895f51b7b5";
    const SLED_ID_3: &str = "d2de378b-c788-4940-9a11-60ac4e633f0c";

    // randomly-generated IPs
    const SLED_IP_1: &str = "445c:212f:f2bc:f202:2b90:5b0e:2d6c:585e";
    const SLED_IP_2: &str = "05fb:7e45:22b1:8dfb:9a06:0dbf:22ab:3eb4";
    const SLED_IP_3: &str = "92a6:42c8:0cf2:7556:5ace:5bab:26e4:4dd1";

    async fn before_impl(ctx: &MigrationContext<'_>) {
        ctx.client
            .batch_execute(&format!(
                "
        INSERT INTO omicron.public.sled (
            id, time_created, time_modified, rcgen, rack_id, is_scrimlet,
            serial_number, part_number, revision, usable_hardware_threads,
            usable_physical_ram, reservoir_size, ip, port, last_used_address,
            sled_policy, sled_state, repo_depot_port, cpu_family
        )
        VALUES
        ('{SLED_ID_1}', now(), now(), 1, gen_random_uuid(), false,
         'migration-210-serial1', 'part1', 1, 64, 12345678, 123456,
         '{SLED_IP_1}', 12345, '{SLED_IP_1}', 'in_service', 'active',
         12346, 'unknown'),
        ('{SLED_ID_2}', now(), now(), 1, gen_random_uuid(), false,
         'migration-210-serial2', 'part1', 1, 64, 12345678, 123456,
         '{SLED_IP_2}', 12345, '{SLED_IP_2}', 'no_provision', 'active',
         12346, 'unknown'),
        ('{SLED_ID_3}', now(), now(), 1, gen_random_uuid(), false,
         'migration-210-serial3', 'part1', 1, 64, 12345678, 123456,
         '{SLED_IP_3}', 12345, '{SLED_IP_3}', 'expunged', 'decommissioned',
         12346, 'unknown');

        INSERT INTO omicron.public.bp_sled_metadata (
            blueprint_id, sled_id, sled_state, sled_agent_generation,
            remove_mupdate_override, host_phase_2_desired_slot_a,
            host_phase_2_desired_slot_b
        )
        VALUES
        (gen_random_uuid(), '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL),
        (gen_random_uuid(), '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL),
        (gen_random_uuid(), '{SLED_ID_3}', 'decommissioned', 1,
         NULL, NULL, NULL);
                "
            ))
            .await
            .expect("inserted pre-migration data");
    }

    async fn after_impl(ctx: &MigrationContext<'_>) {
        let sled_id_1: Uuid = SLED_ID_1.parse().unwrap();
        let sled_id_2: Uuid = SLED_ID_2.parse().unwrap();
        let sled_id_3: Uuid = SLED_ID_3.parse().unwrap();

        let expected = [
            (
                sled_id_1,
                Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_1.parse().unwrap())
                    .to_string(),
            ),
            (
                sled_id_2,
                Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_2.parse().unwrap())
                    .to_string(),
            ),
            (
                sled_id_3,
                Ipv6Subnet::<SLED_PREFIX>::new(SLED_IP_3.parse().unwrap())
                    .to_string(),
            ),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();

        let rows = ctx
            .client
            .query(
                "
                SELECT sled_id, subnet::text
                FROM omicron.public.bp_sled_metadata
                WHERE sled_id IN ($1, $2, $3)
                ",
                &[&sled_id_1, &sled_id_2, &sled_id_3],
            )
            .await
            .expect("queried post-migration data");
        let got = rows
            .into_iter()
            .map(|row| {
                (row.get::<_, Uuid>("sled_id"), row.get::<_, String>("subnet"))
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(expected, got);
    }

    pub(super) fn before<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(before_impl(ctx))
    }

    pub(super) fn after<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(after_impl(ctx))
    }
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
        DataMigrationFns::new()
            .before(before_101_0_0)
            .after(after_101_0_0)
            .at_current(at_current_101_0_0),
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
    map.insert(
        Version::new(151, 0, 0),
        DataMigrationFns::new().before(before_151_0_0).after(after_151_0_0),
    );
    map.insert(
        Version::new(155, 0, 0),
        DataMigrationFns::new().before(before_155_0_0).after(after_155_0_0),
    );
    map.insert(
        Version::new(156, 0, 0),
        DataMigrationFns::new()
            .before(migration_156::before)
            .after(migration_156::after),
    );
    map.insert(
        Version::new(164, 0, 0),
        DataMigrationFns::new().before(before_164_0_0).after(after_164_0_0),
    );
    map.insert(
        Version::new(165, 0, 0),
        DataMigrationFns::new().before(before_165_0_0).after(after_165_0_0),
    );
    map.insert(
        Version::new(171, 0, 0),
        DataMigrationFns::new().before(before_171_0_0).after(after_171_0_0),
    );
    map.insert(
        Version::new(185, 0, 0),
        DataMigrationFns::new().before(before_185_0_0).after(after_185_0_0),
    );
    map.insert(
        Version::new(188, 0, 0),
        DataMigrationFns::new().before(before_188_0_0).after(after_188_0_0),
    );
    map.insert(
        Version::new(207, 0, 0),
        DataMigrationFns::new().before(before_207_0_0).after(after_207_0_0),
    );
    map.insert(
        Version::new(210, 0, 0),
        DataMigrationFns::new().before(before_210_0_0).after(after_210_0_0),
    );
    map.insert(
        Version::new(211, 0, 0),
        DataMigrationFns::new()
            .before(migration_211::before)
            .after(migration_211::after),
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

    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();
    let ctx = MigrationContext {
        log,
        client: crdb.connect().await.expect("Failed to access CRDB client"),
        crdb,
        pool_and_conn: Mutex::new(BTreeMap::new()),
    };

    let all_versions = read_all_schema_versions();
    let all_checks = get_migration_checks();

    // Go from the first version to the latest version.
    for version in all_versions.iter_versions() {
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
    }
    assert_eq!(
        LATEST_SCHEMA_VERSION.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // If any version changes want to query the system post-upgrade, they can.
    for version in all_versions.iter_versions() {
        let checks = all_checks.get(version.semver());
        if let Some(at_current) = checks.and_then(|check| check.at_current) {
            at_current(&ctx).await;
        }
    }

    db.terminate().await;
    logctx.cleanup_successful();
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
