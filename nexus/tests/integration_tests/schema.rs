// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{self, Context};
use camino::Utf8PathBuf;
use dropshot::test_util::LogContext;
use futures::future::BoxFuture;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
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
use nexus_test_utils::{ControlPlaneStarter, load_test_config};
use omicron_test_utils::dev::db::{Client, CockroachInstance};
use pretty_assertions::{assert_eq, assert_ne};
use semver::Version;
use similar_asserts;
use sled_agent_types::early_networking::SwitchSlot;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::str::FromStr;
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
    starter.start_gateway(SwitchSlot::Switch0, None, sp_conf.clone()).await;
    starter.start_gateway(SwitchSlot::Switch1, None, sp_conf).await;
    starter.start_dendrite(SwitchSlot::Switch0).await;
    starter.start_dendrite(SwitchSlot::Switch1).await;
    starter.start_mgd(SwitchSlot::Switch0).await;
    starter.start_mgd(SwitchSlot::Switch1).await;
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

// Applies an update without a transaction wrapper.
//
// Used for non-transactional schema updates.
async fn apply_update_without_transaction(
    client: &omicron_test_utils::dev::db::Client,
    step: &SchemaUpgradeStep,
) {
    if let Err(err) = client.batch_execute(step.sql()).await {
        panic!(
            "Failed to execute non-transactional update step {}: {}",
            step.label(),
            InlineErrorChain::new(&err)
        );
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
            "file" => step.label(),
            "non_transactional" => step.is_non_transactional(),
        );

        for _ in 0..times_to_apply {
            if step.is_non_transactional() {
                apply_update_without_transaction(&client, step).await;
            } else {
                apply_update_as_transaction(&log, &client, step).await;
            }
        }

        // After applying the step, run its verification SQL (if any) in a
        // separate transaction — just like the real Nexus startup path does.
        // This confirms that async backfill operations (CREATE INDEX,
        // ALTER COLUMN SET NOT NULL, ADD CONSTRAINT, ADD COLUMN with
        // backfill) actually completed.
        if let Some(verify_sql) = step.verification_sql() {
            info!(
                log,
                "Verifying schema change";
                "file" => step.label()
            );
            client.batch_execute(verify_sql).await.unwrap_or_else(|e| {
                panic!(
                    "Verification failed for {} in version {}: {}",
                    step.label(),
                    version.semver(),
                    e
                )
            });
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

/// Returns a map from table name to an ordered list of column names.
///
/// The column names are ordered by their ordinal position in the table,
/// which allows us to verify that the relative order of columns is the
/// same between dbinit.sql and migrations.
async fn crdb_column_ordering(
    crdb: &CockroachInstance,
) -> BTreeMap<String, Vec<String>> {
    let client = crdb.connect().await.expect("failed to connect");

    let rows = client
        .query(
            "SELECT table_name, column_name \
             FROM information_schema.columns \
             WHERE table_schema = 'public' \
             ORDER BY table_name, ordinal_position",
            &[],
        )
        .await
        .expect("failed to query column ordering");
    client.cleanup().await.expect("cleaning up after query");

    let mut result: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        result.entry(table_name).or_default().push(column_name);
    }
    result
}

/// Query explicitly persisted system settings.
///
/// Note this is different from SHOW CLUSTER SETTINGS, which includes all
/// cluster settings, and only covers public settings. System settings only
/// consists of overridden names and values, but also includes undocumented
/// settings. (We care about one such setting,
/// `bulkio.index_backfill.batch_size`.)
///
/// `system.settings` also contains CockroachDB-internal entries (like
/// `cluster.secret` and `version`) that are set during cluster
/// initialization and vary between instances.  We exclude those so we
/// only compare settings that Omicron controls.
async fn query_cluster_settings(
    crdb: &CockroachInstance,
) -> BTreeMap<String, String> {
    let client = crdb.connect().await.expect("failed to connect");
    let rows = client
        .query(
            "SELECT name, value FROM system.settings \
             WHERE name NOT IN ('cluster.secret', 'version', \
                                'diagnostics.reporting.enabled') \
             ORDER BY name",
            &[],
        )
        .await
        .expect("failed to query cluster settings");
    client.cleanup().await.expect("cleaning up after query");

    rows.iter()
        .map(|row| {
            let name: String = row.get(0);
            let value: String = row.get(1);
            (name, value)
        })
        .collect()
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
    // The timeout here is 180s (vs the usual 60s) because the full
    // v1-to-latest migration is inherently slow.  Connection reuse in
    // update_schema eliminates repeated pool-checkout overhead, but the
    // raw SQL execution time still dominates.
    assert!(
        timeout(Duration::from_secs(180), builder.start_nexus_internal())
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
    // "Table Name" -> "Columns"
    column_ordering: BTreeMap<String, Vec<String>>,
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
            self.column_ordering,
            other.column_ordering,
            "Column ordering did not match. The relative order of columns within \
            each table must be the same in dbinit.sql and migrations. Since \
            migrations can only add columns at the end of a table, new columns \
            in dbinit.sql should also be added at the end of the table definition."
        );
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
        similar_asserts::assert_eq!(self.statistics, other.statistics,);
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

        let column_ordering = crdb_column_ordering(crdb).await;

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
            column_ordering,
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

const VPC: Uuid = Uuid::from_u128(0x1111566c_5c3d_4647_83b0_8f3515da7be1);
const FW_RULE: Uuid = Uuid::from_u128(0x11117213_5c3d_4647_83b0_8f3515da7be1);

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
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_1}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, NULL,
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_1}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_2}',
                        'not-yet-run', '/test', 'sled-agent', '/test'
                    ), (
                        '{INV_COLLECTION_ID_2}', now(), 'test-source',
                        '{SLED_ID_2}', '192.168.1.1', 0, 'gimlet',
                        32, 68719476736, 1073741824, '{SLED_CONFIG_ID_3}',
                        'not-yet-run', '/test', 'sled-agent', '/test'
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

mod migration_219 {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::BTreeMap;

    // randomly-generated IDs
    const SLED_ID_1: &str = "ee1f2a5e-6b82-4487-8e78-779cf2b0a860";
    const SLED_ID_2: &str = "a653a2d2-91f7-4604-adda-9bcc525db254";
    const SLED_ID_3: &str = "8317bea6-bebc-4e58-ae09-8f1cef72ff4d";

    const BP_ID_1: &str = "64ec2dba-922c-43b3-a576-699c3564d729";
    const BP_ID_2: &str = "dd58e3d3-2760-48e9-8669-6ff0f076ba84";
    const BP_ID_3: &str = "046b9d8f-f152-4370-86b8-e48445670aff";

    const SLED_SUBNET_1: &str = "fd00:1122:3344:0101";
    const SLED_SUBNET_2: &str = "fd00:1122:3344:0102";
    const SLED_SUBNET_3: &str = "fd00:1122:3344:0103";

    async fn before_impl(ctx: &MigrationContext<'_>) {
        // Blueprint 1: 2 sleds with 3 zones each, IPs with final hextet both
        // above and below ::20 (SLED_RESERVED_ADDRESSES).
        //
        // Blueprint 2: all 3 sleds, but the third sled has no zones
        //
        // Blueprint 3: all 3 sleds with zones, but the third has no zones with
        // IPs above ::20.
        ctx.client
            .batch_execute(&format!(
                "
        -- Remove detritus from earlier migrations.
        DELETE FROM omicron.public.bp_sled_metadata WHERE 1=1;
        DELETE FROM omicron.public.bp_omicron_zone WHERE 1=1;

        -- Blueprint 1
        INSERT INTO omicron.public.bp_sled_metadata (
            blueprint_id, sled_id, sled_state, sled_agent_generation,
            remove_mupdate_override, host_phase_2_desired_slot_a,
            host_phase_2_desired_slot_b, subnet
        )
        VALUES
        ('{BP_ID_1}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_1}::/64'),
        ('{BP_ID_1}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_2}::/64');

        -- Blueprint 1 sled 1 zones
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
         '{SLED_SUBNET_1}::10', 8080, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'oximeter',
         '{SLED_SUBNET_1}::50', 8081, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_1}', '{SLED_ID_1}', gen_random_uuid(), 'crucible',
         '{SLED_SUBNET_1}::100', 8082, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 1 sled 2 zones
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'clickhouse',
         '{SLED_SUBNET_2}::15', 8080, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
         '{SLED_SUBNET_2}::25', 8081, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'crucible',
         '{SLED_SUBNET_2}::200', 8082, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_1}', '{SLED_ID_2}', gen_random_uuid(), 'internal_dns',
         '{SLED_SUBNET_2}::ffff', 8053, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 2
        INSERT INTO omicron.public.bp_sled_metadata (
            blueprint_id, sled_id, sled_state, sled_agent_generation,
            remove_mupdate_override, host_phase_2_desired_slot_a,
            host_phase_2_desired_slot_b, subnet
        )
        VALUES
        ('{BP_ID_2}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_1}::/64'),
        ('{BP_ID_2}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_2}::/64'),
        ('{BP_ID_2}', '{SLED_ID_3}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_3}::/64');

        -- Blueprint 2 sled 1 zones
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_2}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
         '{SLED_SUBNET_1}::150', 8080, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 2 sled 2 zones
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_2}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
         '{SLED_SUBNET_2}::250', 8081, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 2 sled 3: NO zones

        -- Blueprint 3
        INSERT INTO omicron.public.bp_sled_metadata (
            blueprint_id, sled_id, sled_state, sled_agent_generation,
            remove_mupdate_override, host_phase_2_desired_slot_a,
            host_phase_2_desired_slot_b, subnet
        )
        VALUES
        ('{BP_ID_3}', '{SLED_ID_1}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_1}::/64'),
        ('{BP_ID_3}', '{SLED_ID_2}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_2}::/64'),
        ('{BP_ID_3}', '{SLED_ID_3}', 'active', 1, NULL, NULL, NULL,
         '{SLED_SUBNET_3}::/64');

        -- Blueprint 3 sled 1
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_3}', '{SLED_ID_1}', gen_random_uuid(), 'clickhouse',
         '{SLED_SUBNET_1}::300', 8080, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 3 sled 2
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_3}', '{SLED_ID_2}', gen_random_uuid(), 'oximeter',
         '{SLED_SUBNET_2}::400', 8081, gen_random_uuid(), 'in_service',
         false, 'install_dataset');

        -- Blueprint 3 sled 3
        INSERT INTO omicron.public.bp_omicron_zone (
            blueprint_id, sled_id, id, zone_type,
            primary_service_ip, primary_service_port,
            filesystem_pool, disposition,
            disposition_expunged_ready_for_cleanup, image_source
        )
        VALUES
        ('{BP_ID_3}', '{SLED_ID_3}', gen_random_uuid(), 'crucible',
         '{SLED_SUBNET_3}::5', 8082, gen_random_uuid(), 'in_service',
         false, 'install_dataset'),
        ('{BP_ID_3}', '{SLED_ID_3}', gen_random_uuid(), 'clickhouse',
         '{SLED_SUBNET_3}::1f', 8080, gen_random_uuid(), 'in_service',
         false, 'install_dataset');
                "
            ))
            .await
            .expect("inserted pre-migration data");
    }

    async fn after_impl(ctx: &MigrationContext<'_>) {
        let bp_id_1: Uuid = BP_ID_1.parse().unwrap();
        let bp_id_2: Uuid = BP_ID_2.parse().unwrap();
        let bp_id_3: Uuid = BP_ID_3.parse().unwrap();
        let sled_id_1: Uuid = SLED_ID_1.parse().unwrap();
        let sled_id_2: Uuid = SLED_ID_2.parse().unwrap();
        let sled_id_3: Uuid = SLED_ID_3.parse().unwrap();

        // BP1, Sled1: max IP is ::100
        // BP1, Sled2: max IP is ::200
        // BP2, Sled1: max IP is ::150
        // BP2, Sled2: max IP is ::250
        // BP2, Sled3: no zones, default to 32
        // BP3, Sled1: max IP is ::300
        // BP3, Sled2: max IP is ::400
        // BP3, Sled3: max IP is ::1f (31); should get bumped to 32
        let expected = [
            ((bp_id_1, sled_id_1), 0x100),
            ((bp_id_1, sled_id_2), 0x200),
            ((bp_id_2, sled_id_1), 0x150),
            ((bp_id_2, sled_id_2), 0x250),
            ((bp_id_2, sled_id_3), 32),
            ((bp_id_3, sled_id_1), 0x300),
            ((bp_id_3, sled_id_2), 0x400),
            ((bp_id_3, sled_id_3), 32),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();

        let rows = ctx
            .client
            .query(
                "
                SELECT blueprint_id, sled_id, last_allocated_ip_subnet_offset
                FROM omicron.public.bp_sled_metadata
                WHERE blueprint_id IN ($1, $2, $3)
                ORDER BY blueprint_id, sled_id
                ",
                &[&bp_id_1, &bp_id_2, &bp_id_3],
            )
            .await
            .expect("queried post-migration data");

        let got = rows
            .into_iter()
            .map(|row| {
                (
                    (
                        row.get::<_, Uuid>("blueprint_id"),
                        row.get::<_, Uuid>("sled_id"),
                    ),
                    row.get::<_, i32>("last_allocated_ip_subnet_offset"),
                )
            })
            .collect::<BTreeMap<_, _>>();

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

// Test that the audit_log credential_id constraint migration (version 222)
// handles existing rows with auth_method set but credential_id NULL.
fn before_222_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert an audit_log entry with auth_method = 'session_cookie'.
        // After version 222's up1.sql adds credential_id, this row will have
        // credential_id = NULL. The migration must handle this case.
        let id = Uuid::new_v4();
        let actor_id = Uuid::new_v4();

        ctx.client
            .execute(
                "INSERT INTO omicron.public.audit_log (
                    id,
                    time_started,
                    request_id,
                    request_uri,
                    operation_id,
                    source_ip,
                    actor_id,
                    actor_kind,
                    auth_method
                ) VALUES (
                    $1,
                    now(),
                    'test-request-id',
                    '/test/uri',
                    'test_operation',
                    '127.0.0.1',
                    $2,
                    'user_builtin',
                    'session_cookie'
                )",
                &[&id, &actor_id],
            )
            .await
            .expect("inserted audit_log row with session_cookie auth_method");
    })
}

fn after_222_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Clean up the test row
        ctx.client
            .execute(
                "DELETE FROM omicron.public.audit_log WHERE request_id = 'test-request-id'",
                &[],
            )
            .await
            .expect("cleaned up audit_log test row");
    })
}

// Migration 229 fixes column ordering in console_session and device_access_token.
// The "id" column was added via ALTER TABLE in migration 145, placing it at the
// end. This migration recreates the tables with "id" as the first column.
// It also drops console sessions created more than 24 hours ago.
const SESSION_229_ID: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000001);
const SESSION_229_TOKEN: &str = "tok-console-229-migration-test";
const SESSION_229_SILO_USER: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000002);

const SESSION_229_OLD_ID: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000003);
const SESSION_229_OLD_TOKEN: &str = "tok-console-229-old-session";
const SESSION_229_OLD_SILO_USER: Uuid =
    Uuid::from_u128(0x22900001_0000_0000_0000_000000000004);

const DEVICE_229_ID: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000001);
const DEVICE_229_TOKEN: &str = "tok-device-229-migration-test";
const DEVICE_229_CLIENT_ID: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000002);
const DEVICE_229_DEVICE_CODE: &str = "code-229-migration-test";
const DEVICE_229_SILO_USER: Uuid =
    Uuid::from_u128(0x22900002_0000_0000_0000_000000000003);

fn before_229_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert test data into console_session and device_access_token.
        // These tables currently have "id" as the last column (from migration 145).
        // We insert two console sessions: one recent (should be kept) and one
        // created over 24 hours ago (should be dropped).
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.console_session
                    (id, token, time_created, time_last_used, silo_user_id)
                VALUES
                    ('{SESSION_229_ID}', '{SESSION_229_TOKEN}', now(), now(), '{SESSION_229_SILO_USER}');

                INSERT INTO omicron.public.console_session
                    (id, token, time_created, time_last_used, silo_user_id)
                VALUES
                    ('{SESSION_229_OLD_ID}', '{SESSION_229_OLD_TOKEN}',
                     now() - INTERVAL '48 hours', now() - INTERVAL '48 hours',
                     '{SESSION_229_OLD_SILO_USER}');

                INSERT INTO omicron.public.device_access_token
                    (id, token, client_id, device_code, silo_user_id, time_requested, time_created)
                VALUES
                    ('{DEVICE_229_ID}', '{DEVICE_229_TOKEN}', '{DEVICE_229_CLIENT_ID}',
                     '{DEVICE_229_DEVICE_CODE}', '{DEVICE_229_SILO_USER}', now(), now());
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 229");
    })
}

fn after_229_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Verify data was preserved after the column reordering migration.

        // Check that the recent console_session was kept
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, token, silo_user_id FROM omicron.public.console_session
                     WHERE id = '{SESSION_229_ID}'"
                ),
                &[],
            )
            .await
            .expect("failed to query post-migration console_session");
        assert_eq!(
            rows.len(),
            1,
            "recent console_session row should still exist"
        );

        let id: Uuid = rows[0].get("id");
        assert_eq!(id, SESSION_229_ID);
        let token: &str = rows[0].get("token");
        assert_eq!(token, SESSION_229_TOKEN);
        let silo_user_id: Uuid = rows[0].get("silo_user_id");
        assert_eq!(silo_user_id, SESSION_229_SILO_USER);

        // Check that the old (>24h) console_session was dropped
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id FROM omicron.public.console_session
                     WHERE id = '{SESSION_229_OLD_ID}'"
                ),
                &[],
            )
            .await
            .expect(
                "failed to query post-migration console_session for old row",
            );
        assert_eq!(
            rows.len(),
            0,
            "console_session row older than 24h should have been dropped"
        );

        // Check device_access_token
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT id, token, client_id, device_code, silo_user_id
                     FROM omicron.public.device_access_token
                     WHERE id = '{DEVICE_229_ID}'"
                ),
                &[],
            )
            .await
            .expect("failed to query post-migration device_access_token");
        assert_eq!(rows.len(), 1, "device_access_token row should still exist");

        let id: Uuid = rows[0].get("id");
        assert_eq!(id, DEVICE_229_ID);
        let token: &str = rows[0].get("token");
        assert_eq!(token, DEVICE_229_TOKEN);
        let client_id: Uuid = rows[0].get("client_id");
        assert_eq!(client_id, DEVICE_229_CLIENT_ID);
        let device_code: &str = rows[0].get("device_code");
        assert_eq!(device_code, DEVICE_229_DEVICE_CODE);
        let silo_user_id: Uuid = rows[0].get("silo_user_id");
        assert_eq!(silo_user_id, DEVICE_229_SILO_USER);

        // Clean up test data
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.console_session WHERE id = '{SESSION_229_ID}';
                DELETE FROM omicron.public.device_access_token WHERE id = '{DEVICE_229_ID}';
                "
            ))
            .await
            .expect("failed to clean up migration 229 test data");
    })
}

// Migration 230 added bgp_config.max_paths as a nullable column.
// Migration 231 backfills NULL rows with 1 and sets NOT NULL.
// We test both: a row that existed before the column was added (pre-230),
// and rows inserted after the column exists but before the NOT NULL fix (pre-231).
const BGP_ANNOUNCE_SET_230: Uuid =
    Uuid::from_u128(0x23000001_0000_0000_0000_000000000001);
// Row inserted before migration 230 (no max_paths column yet).
const BGP_CONFIG_230_PRE: Uuid =
    Uuid::from_u128(0x23000002_0000_0000_0000_000000000001);
// Row inserted after migration 230 with an explicit max_paths value.
const BGP_CONFIG_231_EXPLICIT: Uuid =
    Uuid::from_u128(0x23100002_0000_0000_0000_000000000001);
// Row inserted after migration 230 with NULL max_paths.
const BGP_CONFIG_231_NULL: Uuid =
    Uuid::from_u128(0x23100002_0000_0000_0000_000000000002);

fn before_230_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Insert a bgp_announce_set (required FK) and a bgp_config row
        // BEFORE migration 230 adds the max_paths column.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.bgp_announce_set
                    (id, name, description, time_created, time_modified)
                VALUES
                    ('{BGP_ANNOUNCE_SET_230}', 'test-announce-set-230',
                     'test', now(), now());

                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id)
                VALUES
                    ('{BGP_CONFIG_230_PRE}', 'bgp-pre-max-paths-230',
                     'inserted before max_paths column exists', now(), now(),
                     64500, '{BGP_ANNOUNCE_SET_230}');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 230");
    })
}

fn before_231_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // Now max_paths column exists (added by migration 230) but is nullable.
        // Insert one row with an explicit value and one with NULL.
        ctx.client
            .batch_execute(&format!(
                "
                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id, max_paths)
                VALUES
                    ('{BGP_CONFIG_231_EXPLICIT}', 'bgp-explicit-max-paths-231',
                     'has explicit max_paths', now(), now(),
                     64501, '{BGP_ANNOUNCE_SET_230}', 4);

                INSERT INTO omicron.public.bgp_config
                    (id, name, description, time_created, time_modified,
                     asn, bgp_announce_set_id)
                VALUES
                    ('{BGP_CONFIG_231_NULL}', 'bgp-null-max-paths-231',
                     'has NULL max_paths', now(), now(),
                     64502, '{BGP_ANNOUNCE_SET_230}');
                "
            ))
            .await
            .expect("failed to insert pre-migration rows for 231");
    })
}

fn after_231_0_0<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        // The pre-230 row (no max_paths column when inserted) should now be 1.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_230_PRE}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for pre-230 row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(
            max_paths, 1,
            "pre-230 row should have max_paths backfilled to 1"
        );

        // The row with explicit max_paths = 4 should be preserved.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_231_EXPLICIT}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for explicit row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(max_paths, 4, "explicit max_paths should be preserved");

        // The row with NULL max_paths should now be 1.
        let rows = ctx
            .client
            .query(
                &format!(
                    "SELECT max_paths FROM omicron.public.bgp_config
                     WHERE id = '{BGP_CONFIG_231_NULL}'"
                ),
                &[],
            )
            .await
            .expect("failed to query bgp_config for NULL row");
        assert_eq!(rows.len(), 1);
        let max_paths: i16 = rows[0].get("max_paths");
        assert_eq!(max_paths, 1, "NULL max_paths should be backfilled to 1");

        // Clean up test data
        ctx.client
            .batch_execute(&format!(
                "
                DELETE FROM omicron.public.bgp_config
                    WHERE id IN ('{BGP_CONFIG_230_PRE}',
                                 '{BGP_CONFIG_231_EXPLICIT}',
                                 '{BGP_CONFIG_231_NULL}');
                DELETE FROM omicron.public.bgp_announce_set
                    WHERE id = '{BGP_ANNOUNCE_SET_230}';
                "
            ))
            .await
            .expect("failed to clean up migration 231 test data");
    })
}

// Migration 242 changes the `ereport` table to make the `slot` and `slot_type`
// columns (formerly `sp_slot` and `sp_type`) allowed to be non-NULL for host OS
// ereports in addition to SP ereports, and attempts to backfill these columns
// when possible to do soo via the inventory.
//
// Tests that the migration correctly:
//  1. Backfills `slot_type` from `sp_type` for SP reporters
//  2. Backfills `slot_type` to 'sled' for host reporters
//  3. Backfills `slot` from `sp_slot` for SP reporters
//  4. Backfills `slot` for host reporters by joining through inventory
//  5. Leaves `slot` NULL for hosts with no `hw_baseboard_id` or which don't
//     exist in any inventory collection
//  6. Picks the newest inventory collection containing a sled ID when
//    multiple exist
//  7. Drops the old `sp_type` and `sp_slot` columns
mod migration_242 {
    use super::*;
    use pretty_assertions::assert_eq;

    // Two inventory collections with different timestamps to verify the
    // migration picks the slot from the newest one.
    const INV_COLL_OLDER: Uuid =
        Uuid::from_u128(24200001_0000_0000_0000_000000000001);
    const INV_COLL_NEWER: Uuid =
        Uuid::from_u128(24200001_0000_0000_0000_000000000002);

    // Baseboard ID associated with sled 1.
    const BASEBOARD_1: Uuid =
        Uuid::from_u128(24200002_0000_0000_0000_000000000001);

    // Sled 1: present in inventory with a baseboard → SP slot determinable.
    const SLED_1: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000001);
    // Sled 2: present in inventory but hw_baseboard_id is NULL.
    const SLED_2: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000002);
    // Sled 3: not present in any inventory collection.
    const SLED_3: Uuid = Uuid::from_u128(24200003_0000_0000_0000_000000000003);

    // Ereport restart IDs — one per test case.
    const SP_SLED: Uuid = Uuid::from_u128(24200004_0000_0000_0000_000000000001);
    const SP_SWITCH: Uuid =
        Uuid::from_u128(24200004_0000_0000_0000_000000000002);
    const SP_POWER: Uuid =
        Uuid::from_u128(24200004_0000_0000_0000_000000000003);
    const HOST_WITH_INV: Uuid =
        Uuid::from_u128(24200004_0000_0000_0000_000000000004);
    const HOST_NO_BB: Uuid =
        Uuid::from_u128(24200004_0000_0000_0000_000000000005);
    const HOST_NO_INV: Uuid =
        Uuid::from_u128(24200004_0000_0000_0000_000000000006);

    const COLLECTOR: Uuid =
        Uuid::from_u128(24200005_0000_0000_0000_000000000001);

    pub(super) fn before<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            ctx.client
                .batch_execute(&format!(
                    "
                    -- Two inventory collections: older and newer.
                    INSERT INTO omicron.public.inv_collection
                        (id, time_started, time_done, collector)
                    VALUES
                        ('{INV_COLL_OLDER}',
                         '2024-01-01 00:00:00+00',
                         '2024-01-01 00:01:00+00',
                         'test'),
                        ('{INV_COLL_NEWER}',
                         '2024-06-01 00:00:00+00',
                         '2024-06-01 00:01:00+00',
                         'test');

                    -- A baseboard for sled 1.
                    INSERT INTO omicron.public.hw_baseboard_id
                        (id, part_number, serial_number)
                    VALUES
                        ('{BASEBOARD_1}', 'test-part', 'test-serial');

                    -- Sled 1 in both collections (with baseboard).
                    -- Sled 2 in only the newer collection (no baseboard).
                    -- Sled 3 is deliberately absent from inventory.
                    INSERT INTO omicron.public.inv_sled_agent (
                        inv_collection_id, time_collected, source,
                        sled_id, hw_baseboard_id,
                        sled_agent_ip, sled_agent_port, sled_role,
                        usable_hardware_threads, usable_physical_ram,
                        reservoir_size, reconciler_status_kind,
                        zone_manifest_boot_disk_path,
                        zone_manifest_source,
                        mupdate_override_boot_disk_path,
                        cpu_family,
                        measurement_manifest_boot_disk_path,
                        measurement_manifest_source
                    ) VALUES
                        -- sled 1 in OLDER collection
                        ('{INV_COLL_OLDER}', now(), 'test',
                         '{SLED_1}', '{BASEBOARD_1}',
                         '192.168.1.1', 8080, 'gimlet',
                         32, 68719476736, 1073741824, 'not-yet-run',
                         '/test', 'sled-agent', '/test', 'unknown',
                         '/test', 'sled-agent'),
                        -- sled 1 in NEWER collection
                        ('{INV_COLL_NEWER}', now(), 'test',
                         '{SLED_1}', '{BASEBOARD_1}',
                         '192.168.1.1', 8080, 'gimlet',
                         32, 68719476736, 1073741824, 'not-yet-run',
                         '/test', 'sled-agent', '/test', 'unknown',
                         '/test', 'sled-agent'),
                        -- sled 2 in NEWER collection (no `hw_baseboard_id`)
                        ('{INV_COLL_NEWER}', now(), 'test',
                         '{SLED_2}', NULL,
                         '192.168.1.1', 8080, 'gimlet',
                         32, 68719476736, 1073741824, 'not-yet-run',
                         '/test', 'sled-agent', '/test', 'unknown',
                         '/test', 'sled-agent');

                    -- SP records for sled 1 in both collections,
                    -- with DIFFERENT slot numbers (3 vs 7) so we can
                    -- verify the migration picks the newer one.
                    --
                    -- In REAL LIFE, this case doesn't actually happen.
                    -- If the sled is removed from the rack and reinserted,
                    -- it will have a different `inv_sled_agent` record with
                    -- a different sled UUID. But, since the database *can*
                    -- represent this via multiple inv collection rows, let's
                    -- make sure that the migration tries to pick the newest
                    -- just in case...
                    INSERT INTO omicron.public.inv_service_processor (
                        inv_collection_id, hw_baseboard_id,
                        time_collected, source,
                        sp_type, sp_slot,
                        baseboard_revision, hubris_archive_id,
                        power_state
                    ) VALUES
                        ('{INV_COLL_OLDER}', '{BASEBOARD_1}',
                         now(), 'test', 'sled', 3, 1, 'test', 'A0'),
                        ('{INV_COLL_NEWER}', '{BASEBOARD_1}',
                         now(), 'test', 'sled', 7, 1, 'test', 'A0');

                    -- Ereport rows for every test case.
                    -- At schema version 241 the table still has sp_type
                    -- and sp_slot.
                    INSERT INTO omicron.public.ereport (
                        restart_id, ena, time_collected, collector_id,
                        report, reporter, sp_type, sp_slot, sled_id
                    ) VALUES
                        -- Case 1: SP sled reporter
                        ('{SP_SLED}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'sp', 'sled', 0, NULL),
                        -- Case 2: SP switch reporter
                        ('{SP_SWITCH}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'sp', 'switch', 1, NULL),
                        -- Case 3: SP power reporter
                        ('{SP_POWER}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'sp', 'power', 2, NULL),
                        -- Case 4: Host, sled in inventory with SP also in inventory
                        ('{HOST_WITH_INV}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'host', NULL, NULL, '{SLED_1}'),
                        -- Case 5: Host, sled in inventory, no `hw_baseboard_id`
                        ('{HOST_NO_BB}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'host', NULL, NULL, '{SLED_2}'),
                        -- Case 6: Host, sled not in inventory at all
                        ('{HOST_NO_INV}', 1, now(), '{COLLECTOR}',
                         '{{}}', 'host', NULL, NULL, '{SLED_3}');
                    "
                ))
                .await
                .expect("failed to insert test data for migration 242");
        })
    }

    pub(super) fn after<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Query all test ereport rows after the migration.
            let rows = ctx
                .client
                .query(
                    &format!(
                        "SELECT restart_id, slot_type::text, slot
                         FROM omicron.public.ereport
                         WHERE restart_id IN (
                             '{SP_SLED}', '{SP_SWITCH}', '{SP_POWER}',
                             '{HOST_WITH_INV}', '{HOST_NO_BB}',
                             '{HOST_NO_INV}'
                         )"
                    ),
                    &[],
                )
                .await
                .expect("failed to query migrated ereport rows");

            assert_eq!(rows.len(), 6, "expected 6 test ereport rows");

            let results: HashMap<Uuid, (String, Option<i32>)> = rows
                .iter()
                .map(|row| {
                    let id: Uuid = row.get("restart_id");
                    let slot_type: String = row.get("slot_type");
                    let slot: Option<i32> = row.get("slot");
                    (id, (slot_type, slot))
                })
                .collect();

            // Case 1: SP sled → slot_type='sled', slot=0
            let (st, s) = &results[&SP_SLED];
            assert_eq!(st, "sled", "SP sled reporter slot_type");
            assert_eq!(*s, Some(0), "SP sled reporter slot");

            // Case 2: SP switch → slot_type='switch', slot=1
            let (st, s) = &results[&SP_SWITCH];
            assert_eq!(st, "switch", "SP switch reporter slot_type");
            assert_eq!(*s, Some(1), "SP switch reporter slot");

            // Case 3: SP power → slot_type='power', slot=2
            let (st, s) = &results[&SP_POWER];
            assert_eq!(st, "power", "SP power reporter slot_type");
            assert_eq!(*s, Some(2), "SP power reporter slot");

            // Case 4: Host with inventory + baseboard → slot_type='sled',
            // slot=7 (from the newer collection, NOT 3 from the older one).
            // This case really shouldn't happen, but let's test it anyway.
            let (st, s) = &results[&HOST_WITH_INV];
            assert_eq!(st, "sled", "host with inventory slot_type");
            assert_eq!(
                *s,
                Some(7),
                "should use SP slot from the newest inventory collection"
            );

            // Case 5: Host in inventory but no baseboard → slot NULL
            let (st, s) = &results[&HOST_NO_BB];
            assert_eq!(st, "sled", "host without baseboard slot_type");
            assert_eq!(
                *s, None,
                "host without `hw_baseboard_id` should have NULL slot"
            );

            // Case 6: Host not in inventory at all → slot NULL
            let (st, s) = &results[&HOST_NO_INV];
            assert_eq!(st, "sled", "host not in inventory slot_type");
            assert_eq!(*s, None, "host not in inventory should have NULL slot");

            // Verify the old columns have been dropped.
            let err = ctx
                .client
                .query(
                    "SELECT sp_type FROM omicron.public.ereport LIMIT 0",
                    &[],
                )
                .await;
            assert!(err.is_err(), "sp_type column should have been dropped");

            let err = ctx
                .client
                .query(
                    "SELECT sp_slot FROM omicron.public.ereport LIMIT 0",
                    &[],
                )
                .await;
            assert!(err.is_err(), "sp_slot column should have been dropped");

            // Clean up test data.
            ctx.client
                .batch_execute(&format!(
                    "
                    DELETE FROM omicron.public.ereport
                        WHERE restart_id IN (
                            '{SP_SLED}', '{SP_SWITCH}', '{SP_POWER}',
                            '{HOST_WITH_INV}', '{HOST_NO_BB}',
                            '{HOST_NO_INV}'
                        );
                    DELETE FROM omicron.public.inv_service_processor
                        WHERE hw_baseboard_id = '{BASEBOARD_1}';
                    DELETE FROM omicron.public.inv_sled_agent
                        WHERE sled_id IN ('{SLED_1}', '{SLED_2}');
                    DELETE FROM omicron.public.inv_collection
                        WHERE id IN (
                            '{INV_COLL_OLDER}', '{INV_COLL_NEWER}'
                        );
                    DELETE FROM omicron.public.hw_baseboard_id
                        WHERE id = '{BASEBOARD_1}';
                    "
                ))
                .await
                .expect("failed to clean up migration 242 test data");
        })
    }
}

mod migration_245 {
    use super::*;
    use pretty_assertions::assert_eq;

    // A fake IPv4 pool and IGW IP pool row named "default" backed by it.
    const POOL_V4: Uuid = Uuid::from_u128(24300001_0000_0000_0000_000000000001);
    const IGW_POOL_V4: Uuid =
        Uuid::from_u128(24300002_0000_0000_0000_000000000001);

    // A fake IPv6 pool and IGW IP pool row named "default" backed by it.
    const POOL_V6: Uuid = Uuid::from_u128(24300001_0000_0000_0000_000000000002);
    const IGW_POOL_V6: Uuid =
        Uuid::from_u128(24300002_0000_0000_0000_000000000002);

    pub(super) fn before<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            ctx.client
                .batch_execute(&format!(
                    "
                    INSERT INTO omicron.public.ip_pool
                        (id, name, description, time_created, time_modified,
                         rcgen, ip_version, reservation_type, pool_type)
                    VALUES
                        ('{POOL_V4}', 'test-v4-pool-243', 'IPv4 pool',
                         now(), now(), 0, 'v4', 'external_silos', 'unicast'),
                        ('{POOL_V6}', 'test-v6-pool-243', 'IPv6 pool',
                         now(), now(), 0, 'v6', 'external_silos', 'unicast');

                    INSERT INTO omicron.public.internet_gateway_ip_pool
                        (id, name, description, time_created, time_modified,
                         internet_gateway_id, ip_pool_id)
                    VALUES
                        ('{IGW_POOL_V4}', 'default', 'default v4 pool',
                         now(), now(), gen_random_uuid(), '{POOL_V4}'),
                        ('{IGW_POOL_V6}', 'default', 'default v6 pool',
                         now(), now(), gen_random_uuid(), '{POOL_V6}');
                    "
                ))
                .await
                .expect("failed to insert pre-migration rows for 243");
        })
    }

    pub(super) fn after<'a>(
        ctx: &'a MigrationContext<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let rows = ctx
                .client
                .query(
                    &format!(
                        "SELECT name FROM omicron.public.internet_gateway_ip_pool
                         WHERE id = '{IGW_POOL_V4}'"
                    ),
                    &[],
                )
                .await
                .expect("failed to query internet_gateway_ip_pool after 243");
            assert_eq!(rows.len(), 1);
            let name: &str = rows[0].get("name");
            assert_eq!(
                name, "default-v4",
                "IGW IP pool named 'default' backed by a v4 pool \
                 should have been renamed to 'default-v4'"
            );

            let rows = ctx
                .client
                .query(
                    &format!(
                        "SELECT name FROM omicron.public.internet_gateway_ip_pool
                         WHERE id = '{IGW_POOL_V6}'"
                    ),
                    &[],
                )
                .await
                .expect("failed to query internet_gateway_ip_pool after 243");
            assert_eq!(rows.len(), 1);
            let name: &str = rows[0].get("name");
            assert_eq!(
                name, "default-v6",
                "IGW IP pool named 'default' backed by a v6 pool \
                 should have been renamed to 'default-v6'"
            );
        })
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
    map.insert(
        Version::new(219, 0, 0),
        DataMigrationFns::new()
            .before(migration_219::before)
            .after(migration_219::after),
    );
    map.insert(
        Version::new(222, 0, 0),
        DataMigrationFns::new().before(before_222_0_0).after(after_222_0_0),
    );
    map.insert(
        Version::new(229, 0, 0),
        DataMigrationFns::new().before(before_229_0_0).after(after_229_0_0),
    );
    map.insert(
        Version::new(230, 0, 0),
        DataMigrationFns::new().before(before_230_0_0),
    );
    map.insert(
        Version::new(231, 0, 0),
        DataMigrationFns::new().before(before_231_0_0).after(after_231_0_0),
    );
    map.insert(
        Version::new(242, 0, 0),
        DataMigrationFns::new()
            .before(migration_242::before)
            .after(migration_242::after),
    );
    map.insert(
        Version::new(245, 0, 0),
        DataMigrationFns::new()
            .before(migration_245::before)
            .after(migration_245::after),
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
        .expect("Base schema version not in KNOWN_VERSIONS");

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
    let migrated_cluster_settings = query_cluster_settings(&crdb).await;

    db.terminate().await;

    // Create a fresh database with current dbinit.sql for comparison
    let fresh_db = TestDatabase::new_populate_schema_only(&logctx.log).await;
    let fresh_crdb = fresh_db.crdb();
    let expected_schema = InformationSchema::new(&fresh_crdb).await;
    let expected_data =
        expected_schema.query_all_tables(log, &fresh_crdb).await;
    let expected_cluster_settings = query_cluster_settings(&fresh_crdb).await;

    // Compare the schemas and data
    migrated_schema.pretty_assert_eq(&expected_schema);
    assert_eq!(
        migrated_data, expected_data,
        "Data mismatch when migrating from version {}",
        base_version
    );

    // Validate that cluster settings match.  This catches cases where
    // dbinit.sql sets a cluster setting but migrations don't (or vice versa).
    similar_asserts::assert_eq!(
        migrated_cluster_settings,
        expected_cluster_settings,
        "Cluster settings do not match between migrations and dbinit.sql. \
         If dbinit.sql contains a SET CLUSTER SETTING statement, there must \
         be a corresponding migration (and vice versa)."
    );

    fresh_db.terminate().await;
    println!("Successfully validated migration from version {}", base_version);

    logctx.cleanup_successful();
}
