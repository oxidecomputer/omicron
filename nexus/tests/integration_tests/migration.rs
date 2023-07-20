// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::{load_test_config, ControlPlaneTestContextBuilder};
use omicron_common::api::internal::shared::SwitchLocation;
use tokio::time::timeout;
use tokio::time::Duration;

// Helper to ensure we perform the same setup for the positive and negative test
// cases.
async fn nexus_schema_test_setup(
    builder: &mut ControlPlaneTestContextBuilder<'_, omicron_nexus::Server>,
) {
    builder.start_crdb(false).await;
    builder.start_internal_dns().await;
    builder.start_external_dns().await;
    builder.start_dendrite(SwitchLocation::Switch0).await;
    builder.start_dendrite(SwitchLocation::Switch1).await;
    builder.populate_internal_dns().await;
}

#[tokio::test]
async fn nexus_migrate_offline_update() {
    let mut config = load_test_config();

    let mut builder =
        ControlPlaneTestContextBuilder::<omicron_nexus::Server>::new(
            "nexus_migrate_offline_update",
            &mut config,
        );

    nexus_schema_test_setup(&mut builder).await;

    // TODO: Nexus needs to find the migration to apply, somehow
    // - TODO: Maybe a config option, for "where to look for schema changes"?
    // TODO: Nexus needs to apply the migration
    // TODO: We need to confirm the schema has been migrated

    assert!(
        timeout(Duration::from_secs(60), builder.start_nexus_internal(),)
            .await
            .is_ok(),
        "Nexus should have started"
    );

    builder.teardown().await;
}

// TODO: Test that migration doesn't happen if Nexus sees the wrong version to
// start?

// TODO: Test that dbinit.sql = sum(up.sql)
// TODO: Test that dbwipe.sql = sum(down.sql)

// TODO: Test that "dbinit" is equal to all the versions applied after each
// other? Same with "donw
