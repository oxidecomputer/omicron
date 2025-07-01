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
use nexus_test_utils::sql::Row;
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
use tokio::time::Duration;
use tokio::time::Instant;
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

async fn apply_base_database_version(crdb: &CockroachInstance) {
    let old_dbinit = match load_base_dbinit().await {
        Ok(sql) => {
            println!(
                "Retrieved dbinit.sql for base dbinit ({} bytes)",
                sql.len()
            );
            sql
        }
        Err(e) => {
            panic!("Failed to get old dbinit.sql for base dbinit: {e}",);
        }
    };

    let client = crdb.connect().await.expect("failed to connect");

    // Apply dbinit.sql
    client
        .batch_execute(&old_dbinit)
        .await
        .expect("Failed to apply old dbinit.sql");
}

async fn new_base_database(log: &Logger) -> TestDatabase {
    let db = TestDatabase::new_populate_nothing(log).await;
    let crdb = db.crdb();
    apply_base_database_version(&crdb).await;
    db
}

// This test confirms the following behavior:
//
// - Nexus can boot using the 'base' revision of the schema
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
    apply_base_database_version(&crdb).await;

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
    let crdb = builder.database.as_ref().expect("Should have started CRDB");
    apply_base_database_version(&crdb).await;

    let observed_version = query_crdb_schema_version(&crdb).await;
    assert_eq!(
        EARLIEST_SUPPORTED_VERSION.to_string(),
        observed_version,
        "The 'base' schema version ({observed_version}) is not the EARLIEST_SUPPORTED_VERSION ({EARLIEST_SUPPORTED_VERSION})"
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

// This test verifies that executing the most recent schema migrations, in order,
// twice, still correctly performs an upgrade.
//
// This attempts to be a rough approximation for multiple Nexuses each
// simultaneously executing these operations.
#[tokio::test]
async fn update_since_base_has_idempotent_up() {
    let config = load_test_config();
    let logctx =
        LogContext::new("update_since_base_has_idempotent_up", &config.pkg.log);
    let log = &logctx.log;
    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();

    // Apply the base schema version before Nexus boots.
    let base_version_semver =
        get_base_schema_version().expect("Cannot read base schema version");
    apply_base_database_version(&crdb).await;

    let all_versions = read_all_schema_versions();
    let migrations_to_apply: Vec<_> = all_versions
        .versions_range((
            std::ops::Bound::Excluded(base_version_semver.clone()),
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
    #[allow(dead_code)]
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
            .expect("inserted post-migration inv_sled_agent data");

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

// Lazily initializes all migration checks. The combination of Rust function
// pointers and async makes defining a static table fairly painful, so we're
// using lazy initialization instead.
//
// Each "check" is implemented as a pair of {before, after} migration function
// pointers, called precisely around the migration under test.
fn get_migration_checks() -> BTreeMap<Version, DataMigrationFns> {
    let mut map = BTreeMap::new();

    map.insert(
        Version::new(151, 0, 0),
        DataMigrationFns::new().before(before_151_0_0).after(after_151_0_0),
    );

    map
}

// Parse the dbinit-base.sql file to get starting version
fn get_base_schema_version() -> Result<semver::Version, anyhow::Error> {
    let base_file = std::path::Path::new(SCHEMA_DIR).join("dbinit-base.sql");
    if !base_file.exists() {
        anyhow::bail!(
            "Base schema file not found: {:?}\nRun 'cargo xtask schema generate-base <tag>' to generate it.",
            base_file
        );
    }

    let content = std::fs::read_to_string(&base_file).with_context(|| {
        format!("Failed to read base schema file: {:?}", base_file)
    })?;

    // Parse the base schema version from the header
    for line in content.lines() {
        if line.starts_with("-- Schema version:") {
            let version_str = line
                .strip_prefix("-- Schema version:")
                .ok_or_else(|| {
                    anyhow::anyhow!("Invalid schema version line format")
                })?
                .trim();
            return semver::Version::parse(version_str).with_context(|| {
                format!("Failed to parse base schema version: {}", version_str)
            });
        }
    }
    anyhow::bail!("Could not find schema version in base file header")
}

// Load the base dbinit.sql for migration testing
async fn load_base_dbinit() -> Result<String, anyhow::Error> {
    let base_file = std::path::Path::new(SCHEMA_DIR).join("dbinit-base.sql");
    if !base_file.exists() {
        anyhow::bail!(
            "Base schema file not found: {:?}\nRun 'cargo xtask schema generate-base <tag>' to generate it.",
            base_file
        );
    }

    tokio::fs::read_to_string(&base_file).await.with_context(|| {
        format!("Failed to read base dbinit.sql: {:?}", base_file)
    })
}

struct MigrationTimingInfo {
    entire_task: Duration,
    schema_setup: Duration,
    migration_times: Vec<(semver::Version, Duration)>,
}

// Test data migration from a starting schema version to a target version
async fn validate_data_migration_from_version_to_target(
    base_version: semver::Version,
    target_version: semver::Version,
) -> MigrationTimingInfo {
    let start = Instant::now();
    let config = load_test_config();
    let logctx = LogContext::new("validate_data_migrations", &config.pkg.log);
    let log = &logctx.log;

    let db = TestDatabase::new_populate_nothing(&logctx.log).await;
    let crdb = db.crdb();

    let base_sql =
        load_base_dbinit().await.expect("Failed to load base schema");

    let client = crdb.connect().await.expect("Failed to access CRDB client");
    client.batch_execute(&base_sql).await.expect("Failed to apply base schema");

    // Verify we're at the expected base version
    let actual_version = query_crdb_schema_version(&crdb).await;
    assert_eq!(base_version.to_string(), actual_version);
    let schema_setup = start.elapsed();

    let ctx = MigrationContext { log, client };

    let all_versions = read_all_schema_versions();
    let mut all_checks = get_migration_checks();

    // Find all versions from base_version to target_version (inclusive)
    let base_nexus_version = Version::new(
        base_version.major,
        base_version.minor,
        base_version.patch,
    );
    let target_nexus_version = Version::new(
        target_version.major,
        target_version.minor,
        target_version.patch,
    );

    let versions_to_apply: Vec<_> = all_versions
        .iter_versions()
        .filter(|v| {
            v.semver() > &base_nexus_version
                && v.semver() <= &target_nexus_version
        })
        .collect();

    // Time migration steps
    let mut migration_times = Vec::new();

    // Apply each migration step
    for version in &versions_to_apply {
        let migration_start = Instant::now();

        // If this check has preconditions (or setup), run them.
        let checks = all_checks.remove(version.semver());
        if let Some(before) = checks.as_ref().and_then(|check| check.before) {
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

    // Verify we reached the target version
    assert_eq!(
        target_version.to_string(),
        query_crdb_schema_version(&crdb).await
    );

    // Verify there weren't any checks we missed
    if !all_checks.is_empty() {
        for version in all_checks.keys() {
            println!("  Did not test migrations upgrading to: {version}");
        }
        panic!(
            "Unused checks: must be greater than {base_version} and less than or equal to {target_version}"
        );
    }

    db.terminate().await;
    logctx.cleanup_successful();

    MigrationTimingInfo {
        entire_task: start.elapsed(),
        schema_setup,
        migration_times,
    }
}

// Performs all schema changes and runs version-specific assertions.
//
// HOW TO ADD A MIGRATION CHECK:
// - Add a new "map.insert" line to "get_migration_checks", with the semver of the version you'd
// like to inspect before / after.
// - Define your "before" (optional) and "after" (required) functions. These act on a connection to
// CockroachDB, and can observe and mutate arbitrary state.
// - Migrations will start at the base schema from schema/crdb/dbinit-base.sql
//
// ADVICE FOR MIGRATION CHECKS:
// - Your migration check will run in the same test as all other migration checks. If you perform
// an operation that could be disruptive to subsequent checks, I recommend cleaning up after
// yourself (e.g., DELETE relevant rows).
// - I recommend using schema checks that are NOT strongly-typed. When you add a migration check,
// it'll happen to match the "latest" static schemas defined by Nexus, but that won't always be the
// case. As the schema continues to change (maybe a table you're trying to check gets a new column
// in a later version), your code should continue operating on the OLD version, and as such, should
// avoid needing any updates.
#[tokio::test]
async fn validate_data_migrations() {
    let base_version =
        get_base_schema_version().expect("Failed to get base schema version");
    let latest_version = LATEST_SCHEMA_VERSION.clone();

    println!("Starting migration test: {} → {}", base_version, latest_version);

    let timing = validate_data_migration_from_version_to_target(
        base_version.clone(),
        latest_version.clone(),
    )
    .await;

    println!("\nMigration test summary:");
    println!(
        "Range: {} -> {}: {:.2}s",
        base_version,
        latest_version,
        timing.entire_task.as_secs_f64()
    );
    println!(
        "  Apply dbinit @ {}: {:.2}s",
        base_version,
        timing.schema_setup.as_secs_f64()
    );
    for (version, duration) in &timing.migration_times {
        println!("  Upgrade → {version}: {:.2}s", duration.as_secs_f64());
    }
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

#[tokio::test]
async fn validate_migration_from_base_version() {
    let config = load_test_config();
    let logctx = LogContext::new(
        "validate_migration_from_last_version",
        &config.pkg.log,
    );
    let log = &logctx.log;

    let base_version_semver =
        get_base_schema_version().expect("Cannot read base schema version");
    let base_version = KNOWN_VERSIONS
        .iter()
        .find(|v| *v.semver() == base_version_semver)
        .expect("Base schema version not  in KNOWN_VERSIONS");

    assert_ne!(base_version_semver, LATEST_SCHEMA_VERSION);

    println!(
        "Testing migration from base version {} to current version {}",
        base_version, LATEST_SCHEMA_VERSION
    );

    // Create a new database and apply the historical dbinit.sql
    let db = new_base_database(&logctx.log).await;
    let crdb = db.crdb();

    // Verify we're at the expected schema version
    let current_version = query_crdb_schema_version(&crdb).await;
    assert_eq!(base_version.to_string(), current_version);

    // Load all schema versions and apply migrations from base_version to latest
    let all_versions = read_all_schema_versions();
    let migrations_to_apply: Vec<_> = all_versions
        .versions_range((
            std::ops::Bound::Excluded(base_version_semver.clone()),
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
        base_version
    );

    fresh_db.terminate().await;
    println!("Successfully validated migration from version {}", base_version);

    logctx.cleanup_successful();
}
