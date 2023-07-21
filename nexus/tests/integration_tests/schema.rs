// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model::schema::SCHEMA_VERSION as LATEST_SCHEMA_VERSION;
use nexus_test_utils::{load_test_config, ControlPlaneTestContextBuilder};
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::nexus_config::Config;
use omicron_common::nexus_config::SchemaConfig;
use omicron_test_utils::dev::db::CockroachInstance;
use std::collections::BTreeSet;
use std::path::PathBuf;
use tokio::time::timeout;
use tokio::time::Duration;

const SCHEMA_DIR: &'static str = concat!(env!("CARGO_MANIFEST_DIR"), "/../schema/crdb");
const FIRST_VERSION: &'static str = "1.0.0";

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
    let mut config = load_test_config();
    let builder = test_setup(&mut config, "versions_have_idempotent_up").await;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

    let all_versions = read_all_schema_versions().await;

    for version in &all_versions {
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        apply_update(&crdb, &version.to_string(), Update::Apply).await;
        assert_eq!(version.to_string(), query_crdb_schema_version(&crdb).await);
    }
    assert_eq!(LATEST_SCHEMA_VERSION.to_string(), query_crdb_schema_version(&crdb).await);

    builder.teardown().await;
}

// This test verifies that we can execute all upgrades, and that we can also
// undo all those upgrades.
//
// It also tests that we can idempotently perform the downgrade.
#[tokio::test]
async fn versions_have_idempotent_down() {
    let mut config = load_test_config();
    let builder = test_setup(&mut config, "versions_have_idempotent_up").await;
    let crdb = builder.database.as_ref().expect("Should have started CRDB");

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

    builder.teardown().await;
}


// TODO: Test that migration doesn't happen if Nexus sees the wrong version to
// start?

// TODO: Test that dbinit.sql = sum(up.sql)
// TODO: Test that dbwipe.sql = sum(down.sql)

// TODO: Test that "dbinit" is equal to all the versions applied after each
// other? Same with "down".
