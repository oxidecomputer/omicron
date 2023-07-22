// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::LogContext;
use nexus_db_model::schema::SCHEMA_VERSION as LATEST_SCHEMA_VERSION;
use nexus_test_utils::{db, load_test_config, ControlPlaneTestContextBuilder};
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::nexus_config::Config;
use omicron_common::nexus_config::SchemaConfig;
use omicron_test_utils::dev::db::CockroachInstance;
use pretty_assertions::assert_eq;
use slog::Logger;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use tokio::time::timeout;
use tokio::time::Duration;

const SCHEMA_DIR: &'static str = concat!(env!("CARGO_MANIFEST_DIR"), "/../schema/crdb");
const FIRST_VERSION: &'static str = "1.0.0";

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
            name,
            config,
        );
    let populate = false;
    builder.start_crdb(populate).await;
    let schema_dir = PathBuf::from(SCHEMA_DIR);
    builder.config.pkg.schema = Some(SchemaConfig {
        schema_dir,
    });
    builder.start_internal_dns().await;
    builder.start_external_dns().await;
    builder.start_dendrite(SwitchLocation::Switch0).await;
    builder.start_dendrite(SwitchLocation::Switch1).await;
    builder.populate_internal_dns().await;
    builder
}

#[derive(Debug)]
enum Update {
    Apply,
    Remove,
}

async fn apply_update(
    crdb: &CockroachInstance,
    version: &str,
    up: Update,
) {
    println!("Performing {up:?} on {version}");
    let client = crdb.connect().await.expect("failed to connect");

    let file = match up {
        Update::Apply => "up.sql",
        Update::Remove => "down.sql",
    };
    let sql = tokio::fs::read_to_string(PathBuf::from(SCHEMA_DIR).join(version).join(file)).await.unwrap();
    client.batch_execute(&sql).await.expect("failed to apply update");
    client.cleanup().await.expect("cleaning up after wipe");
}

async fn query_crdb_schema_version(
    crdb: &CockroachInstance,
) -> String {
    let client = crdb.connect().await.expect("failed to connect");
    let sql = "SELECT value FROM omicron.public.db_metadata WHERE name = 'schema_version'";

    let row = client.query_one(sql, &[]).await.expect("failed to query schema");
    let version = row.get(0);
    client.cleanup().await.expect("cleaning up after wipe");
    version
}

// An incredibly generic representation of a row of SQL data
#[derive(Eq, PartialEq, Debug)]
struct Row {
    // It's a little redunant to include the column name alongside each value,
    // but it results in a prettier diff.
    values: Vec<(String, Option<String>)>,
}

impl Row {
    fn new() -> Self {
        Self {
            values: vec![],
        }
    }
}

async fn query_crdb_for_rows_of_strings(
    crdb: &CockroachInstance,
    columns: &[&str],
    table: &str,
    constraints: Option<&str>,
) -> Vec<Row> {
    let client = crdb.connect().await.expect("failed to connect");
    let constraints = if let Some(constraints) = constraints {
        format!("WHERE {constraints}")
    } else {
        "".to_string()
    };

    let values = columns.join(",");

    // We insert the ORDER BY as a simple mechanism to ensure that we're
    // comparing equivalent data. We care about the contents of the retreived
    // rows, so normalize the order in which they are returned.
    let sql = format!("SELECT {values} FROM {table} {constraints} ORDER BY {values}");
    let rows = client.query(&sql, &[]).await.expect(&format!("failed to query {table}"));
    client.cleanup().await.expect("cleaning up after wipe");

    let mut result = vec![];
    for row in rows {
        let mut row_result = Row::new();
        for i in 0..row.len() {
            row_result.values.push((columns[i].to_string(), row.get(i)));
        }
        assert_eq!(row_result.values.len(), columns.len());
        result.push(row_result);
    }
    result
}

async fn read_all_schema_versions() -> BTreeSet<SemverVersion> {
    let mut all_versions = BTreeSet::new();

    let mut dir = tokio::fs::read_dir(SCHEMA_DIR)
        .await
        .expect("Access schema dir");
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
    let mut builder = test_setup(&mut config, "nexus_applies_update_on_boot").await;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    apply_update(&crdb, FIRST_VERSION, Update::Apply).await;
    assert_eq!(FIRST_VERSION, query_crdb_schema_version(&crdb).await);

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
    assert_eq!(LATEST_SCHEMA_VERSION.to_string(), query_crdb_schema_version(&crdb).await);

    builder.teardown().await;
}

// This test verifies that booting with an unknown version prevents us from
// applying any updates.
#[tokio::test]
async fn nexus_cannot_apply_update_from_unknown_version() {
    let mut config = load_test_config();
    let mut builder = test_setup(&mut config, "nexus_cannot_apply_update_from_unknown_version").await;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    apply_update(&crdb, FIRST_VERSION, Update::Apply).await;
    assert_eq!(FIRST_VERSION, query_crdb_schema_version(&crdb).await);

    // This version is not valid; it does not exist.
    let version = "0.0.0";
    crdb.connect().await.expect("Failed to connect")
        .batch_execute(&format!("UPDATE omicron.public.db_metadata SET value = '{version}' WHERE name = 'schema_version'"))
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
    let logctx = LogContext::new("versions_have_idempotent_up", &config.pkg.log);
    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;

    let all_versions = read_all_schema_versions().await;

    for version in &all_versions {
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(LATEST_SCHEMA_VERSION.to_string(), query_crdb_schema_version(&crdb).await);

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

// This test verifies that we can execute all upgrades, and that we can also
// undo all those upgrades.
//
// It also tests that we can idempotently perform the downgrade.
#[tokio::test]
async fn versions_have_idempotent_down() {
    let config = load_test_config();
    let logctx = LogContext::new("versions_have_idempotent_down", &config.pkg.log);
    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;

    let all_versions = read_all_schema_versions().await;

    // Go from the first version to the latest version.
    for version in &all_versions {
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(LATEST_SCHEMA_VERSION.to_string(), query_crdb_schema_version(&crdb).await);

    // Go from the latest version to the first version.
    //
    // This actually also issues "down.sql" for the first version, to it ends
    // up dropping the database too.
    for version in all_versions.iter().rev() {
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
        apply_update(&crdb, &version.to_string(), Update::Remove).await;
        apply_update(&crdb, &version.to_string(), Update::Remove).await;
    }

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

const COLUMNS: [&'static str; 6] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "data_type"
];

const CHECK_CONSTRAINTS: [&'static str; 4] = [
    "constraint_catalog",
    "constraint_schema",
    "constraint_name",
    "check_clause",
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

const VIEWS: [&'static str; 4] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "view_definition",
];

const STATISTICS: [&'static str; 8] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "non_unique",
    "index_schema",
    "index_name",
    "column_name",
    "direction",
];

const TABLES: [&'static str; 4] = [
    "table_catalog",
    "table_schema",
    "table_name",
    "table_type",
];

#[derive(Eq, PartialEq, Debug)]
struct InformationSchema {
    columns: Vec<Row>,
    check_constraints: Vec<Row>,
    key_column_usage: Vec<Row>,
    referential_constraints: Vec<Row>,
    views: Vec<Row>,
    statistics: Vec<Row>,
    tables: Vec<Row>,
}

impl InformationSchema {
    fn pretty_assert_eq(&self, other: &Self) {
        // TODO: We could manually iterate here too - the Debug outputs for
        // each of these is pretty large, and can be kinda painful to read
        // when comparing e.g. "All columns that exist in the database".
        assert_eq!(self.columns, other.columns);
        assert_eq!(self.check_constraints, other.check_constraints);
        assert_eq!(self.key_column_usage, other.key_column_usage);
        assert_eq!(self.referential_constraints, other.referential_constraints);
        assert_eq!(self.views, other.views);
        assert_eq!(self.statistics, other.statistics);
        assert_eq!(self.tables, other.tables);
    }

    async fn new(crdb: &CockroachInstance) -> Self {
        // Refer to:
        // https://www.cockroachlabs.com/docs/v23.1/information-schema
        //
        // For details on each of these tables.
        let columns = query_crdb_for_rows_of_strings(
            crdb,
            &COLUMNS,
            "information_schema.columns",
            Some("table_schema = 'public'"),
        ).await;

        let check_constraints = query_crdb_for_rows_of_strings(
            crdb,
            &CHECK_CONSTRAINTS,
            "information_schema.check_constraints",
            None,
        ).await;

        let key_column_usage = query_crdb_for_rows_of_strings(
            crdb,
            &KEY_COLUMN_USAGE,
            "information_schema.key_column_usage",
            None,
        ).await;

        let referential_constraints = query_crdb_for_rows_of_strings(
            crdb,
            &REFERENTIAL_CONSTRAINTS,
            "information_schema.referential_constraints",
            None,
        ).await;

        let views = query_crdb_for_rows_of_strings(
            crdb,
            &VIEWS,
            "information_schema.views",
            None,
        ).await;

        let statistics = query_crdb_for_rows_of_strings(
            crdb,
            &STATISTICS,
            "information_schema.statistics",
            None,
        ).await;

        let tables = query_crdb_for_rows_of_strings(
            crdb,
            &TABLES,
            "information_schema.tables",
            Some("table_schema = 'public'"),
        ).await;

        Self {
            columns,
            check_constraints,
            key_column_usage,
            referential_constraints,
            views,
            statistics,
            tables,
        }
    }

    // This would normally be quite an expensive operation, but we expect it'll
    // at least be slightly cheaper for the freshly populated DB, which
    // shouldn't have that many records yet.
    async fn query_all_tables(&self, crdb: &CockroachInstance) -> HashMap<String, Vec<Row>> {
        let map = HashMap::new();

        for table in &self.tables {
//            query_crdb_for_rows_of_strings(crdb, 

        }

        map
    }
}

// Confirms that the application of all "up.sql" files, in order, is equivalent
// to applying "dbinit.sql", which should represent the latest-known schema.
#[tokio::test]
async fn dbinit_equals_sum_of_all_up() {
    let config = load_test_config();
    let logctx = LogContext::new("dbinit_equals_sum_of_all_up", &config.pkg.log);

    let populate = false;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;

    let all_versions = read_all_schema_versions().await;

    // Go from the first version to the latest version.
    for version in &all_versions {
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(LATEST_SCHEMA_VERSION.to_string(), query_crdb_schema_version(&crdb).await);

    // Query the newly constructed DB for information about its schema
    let observed = InformationSchema::new(&crdb).await;
    crdb.cleanup().await.unwrap();

    // Create a new DB with data populated from dbinit.sql for comparison
    let populate = true;
    let mut crdb = test_setup_just_crdb(&logctx.log, populate).await;
    let expected = InformationSchema::new(&crdb).await;

    observed.pretty_assert_eq(&expected);

    // TODO: Query for built-in rows. See:
    // - omicron.public.user_builtin
    //

    crdb.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

// TODO: use "generate_series" to make a bunch of fake data, try to catch "SHOW
// JOBS" from CRDB when using that?

// TODO: (idk if this is possible but) test that multiple migrations cannot
// occur at the same time?
