// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use nexus_config::Database;
use nexus_config::InternalDns;
use nexus_test_interface::NexusServer;
use nexus_test_utils::{ControlPlaneStarter, load_test_config};
use omicron_common::address::MGS_PORT;
use omicron_common::api::internal::shared::SwitchLocation;
use std::collections::HashMap;
use tokio::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;

#[tokio::test]
async fn test_nexus_boots_before_cockroach() {
    let mut config = load_test_config();

    let mut starter = ControlPlaneStarter::<omicron_nexus::Server>::new(
        "test_nexus_boots_before_cockroach",
        &mut config,
    );

    let log = starter.logctx.log.new(o!("component" => "test"));

    starter
        .start_gateway(
            SwitchLocation::Switch0,
            Some(MGS_PORT),
            DEFAULT_SP_SIM_CONFIG.into(),
        )
        .await;
    starter
        .start_gateway(
            SwitchLocation::Switch1,
            None,
            DEFAULT_SP_SIM_CONFIG.into(),
        )
        .await;

    starter.start_dendrite(SwitchLocation::Switch0).await;
    starter.start_dendrite(SwitchLocation::Switch1).await;
    starter.start_mgd(SwitchLocation::Switch0).await;
    starter.start_mgd(SwitchLocation::Switch1).await;
    starter.start_internal_dns().await;
    starter.start_external_dns().await;

    // Start Nexus, referencing the internal DNS system.
    //
    // This call won't return successfully until we can...
    // 1. Contact the internal DNS system to find Cockroach
    // 2. Contact Cockroach to ensure the database has been populated
    starter.config.deployment.database = Database::FromDns;
    starter.config.deployment.internal_dns = InternalDns::FromAddress {
        address: starter
            .internal_dns
            .as_ref()
            .expect("Must start Internal DNS before acquiring an address")
            .dns_server
            .local_address(),
    };
    let nexus_config = starter.config.clone();
    let nexus_log = log.clone();
    let nexus_handle = tokio::task::spawn(async move {
        info!(nexus_log, "Test: Trying to start Nexus (internal)");
        omicron_nexus::Server::start_internal(&nexus_config, &nexus_log)
            .await
            .unwrap();
        info!(nexus_log, "Test: Started Nexus (internal)");
    });

    // Start Cockroach and populate the Internal DNS system with CRDB records.
    //
    // This is necessary for the prior call to "start Nexus" to succeed.
    info!(log, "Starting CRDB");
    let populate = true;
    starter.start_crdb(populate).await;
    info!(log, "Started CRDB");

    info!(log, "Populating internal DNS records");
    starter.populate_internal_dns().await;
    info!(log, "Populated internal DNS records");

    // Now that Cockroach has started, we expect the request to succeed.
    nexus_handle.await.expect("Test: Task starting Nexus has failed");

    starter.teardown().await;
}

#[tokio::test]
async fn test_nexus_boots_before_dendrite() {
    let mut config = load_test_config();

    let mut starter = ControlPlaneStarter::<omicron_nexus::Server>::new(
        "test_nexus_boots_before_dendrite",
        &mut config,
    );

    let log = starter.logctx.log.new(o!("component" => "test"));

    // Start MGS + Sim SP. This is needed for the Dendrite client initialization
    // inside of Nexus initialization.  We must use MGS_PORT here because Nexus
    // hardcodes it.
    info!(&log, "Starting MGS");
    starter
        .start_gateway(
            SwitchLocation::Switch0,
            None,
            DEFAULT_SP_SIM_CONFIG.into(),
        )
        .await;
    starter
        .start_gateway(
            SwitchLocation::Switch1,
            None,
            DEFAULT_SP_SIM_CONFIG.into(),
        )
        .await;
    info!(&log, "Started MGS");

    let populate = true;
    starter.start_crdb(populate).await;
    starter.start_internal_dns().await;
    starter.start_external_dns().await;

    // Start Nexus, referencing the internal DNS system.
    //
    // This call won't return successfully until we can...
    // 1. Contact the internal DNS system to find Dendrite
    // 2. Contact Dendrite
    starter.config.deployment.database = Database::FromUrl {
        url: starter
            .database
            .as_ref()
            .expect("Must start CRDB first")
            .pg_config()
            .clone(),
    };
    starter.config.pkg.dendrite = HashMap::new();
    starter.config.deployment.internal_dns = InternalDns::FromAddress {
        address: starter
            .internal_dns
            .as_ref()
            .expect("Must start Internal DNS before acquiring an address")
            .dns_server
            .local_address(),
    };
    let nexus_config = starter.config.clone();
    let nexus_log = log.clone();
    let nexus_handle = tokio::task::spawn(async move {
        info!(nexus_log, "Test: Trying to start Nexus (internal)");
        omicron_nexus::Server::start_internal(&nexus_config, &nexus_log)
            .await
            .unwrap();
        info!(nexus_log, "Test: Started Nexus (internal)");
    });

    // Start Dendrite and populate the Internal DNS system.
    //
    // This is necessary for the prior call to "start Nexus" to succeed.
    info!(log, "Starting Dendrite");
    starter.start_dendrite(SwitchLocation::Switch0).await;
    starter.start_dendrite(SwitchLocation::Switch1).await;
    info!(log, "Started Dendrite");

    info!(log, "Starting mgd");
    starter.start_mgd(SwitchLocation::Switch0).await;
    starter.start_mgd(SwitchLocation::Switch1).await;
    info!(log, "Started mgd");

    info!(log, "Populating internal DNS records");
    starter
        .record_switch_dns(
            nexus_test_utils::SLED_AGENT_UUID.parse().unwrap(),
            SwitchLocation::Switch0,
        )
        .await;
    starter
        .record_switch_dns(
            nexus_test_utils::SLED_AGENT2_UUID.parse().unwrap(),
            SwitchLocation::Switch1,
        )
        .await;
    starter.populate_internal_dns().await;
    info!(log, "Populated internal DNS records");

    // Now that Dendrite has started, we expect the request to succeed.
    nexus_handle.await.expect("Test: Task starting Nexus has failed");

    starter.teardown().await;
}

// Helper to ensure we perform the same setup for the positive and negative test
// cases.
async fn nexus_schema_test_setup(
    starter: &mut ControlPlaneStarter<'_, omicron_nexus::Server>,
) {
    let populate = true;
    starter.start_crdb(populate).await;
    starter.start_internal_dns().await;
    starter.start_external_dns().await;

    let sp_conf: camino::Utf8PathBuf = DEFAULT_SP_SIM_CONFIG.into();
    starter.start_gateway(SwitchLocation::Switch0, None, sp_conf.clone()).await;
    starter.start_gateway(SwitchLocation::Switch1, None, sp_conf).await;
    starter.start_dendrite(SwitchLocation::Switch0).await;
    starter.start_dendrite(SwitchLocation::Switch1).await;
    starter.start_mgd(SwitchLocation::Switch0).await;
    starter.start_mgd(SwitchLocation::Switch1).await;
    starter.populate_internal_dns().await;
}

#[tokio::test]
async fn test_nexus_boots_with_valid_schema() {
    let mut config = load_test_config();

    let mut starter = ControlPlaneStarter::<omicron_nexus::Server>::new(
        "test_nexus_boots_with_valid_schema",
        &mut config,
    );

    nexus_schema_test_setup(&mut starter).await;

    assert!(
        timeout(Duration::from_secs(60), starter.start_nexus_internal(),)
            .await
            .is_ok(),
        "Nexus should have started"
    );

    starter.teardown().await;
}

#[tokio::test]
async fn test_nexus_does_not_boot_without_valid_schema() {
    let s = nexus_db_model::SCHEMA_VERSION;

    let schemas_to_test = vec![
        semver::Version::new(s.major + 1, s.minor, s.patch),
        semver::Version::new(s.major, s.minor + 1, s.patch),
        semver::Version::new(s.major, s.minor, s.patch + 1),
    ];

    for schema in schemas_to_test {
        let mut config = load_test_config();

        config.pkg.tunables.load_timeout =
            Some(std::time::Duration::from_secs(5));

        let mut starter = ControlPlaneStarter::<omicron_nexus::Server>::new(
            "test_nexus_does_not_boot_without_valid_schema",
            &mut config,
        );

        nexus_schema_test_setup(&mut starter).await;

        starter.database
            .as_ref()
            .expect("Should have started CRDB")
            .connect()
            .await
            .expect("Failed to connect to CRDB")
            .batch_execute(
                &format!(
                    "UPDATE omicron.public.db_metadata SET version = '{schema}' WHERE singleton = true"
                )
            )
            .await
            .expect("Failed to update schema");

        let err = starter
            .start_nexus_internal()
            .await
            .expect_err("Nexus should have failed to start");

        assert!(
            err.contains("Datastore should not be used"),
            "Saw error: {err}"
        );

        starter.teardown().await;
    }
}

#[tokio::test]
async fn test_nexus_does_not_boot_until_schema_updated() {
    let good_schema = nexus_db_model::SCHEMA_VERSION;
    let bad_schema = semver::Version::new(
        good_schema.major + 1,
        good_schema.minor,
        good_schema.patch,
    );

    let mut config = load_test_config();

    let mut starter = ControlPlaneStarter::<omicron_nexus::Server>::new(
        "test_nexus_does_not_boot_until_schema_updated",
        &mut config,
    );

    nexus_schema_test_setup(&mut starter).await;

    let crdb = starter
        .database
        .as_ref()
        .expect("Should have started CRDB")
        .connect()
        .await
        .expect("Failed to connect to CRDB");

    // Inject a bad schema into the DB. This should mimic the
    // "test_nexus_does_not_boot_without_valid_schema" test.
    crdb.batch_execute(
        &format!(
            "UPDATE omicron.public.db_metadata SET version = '{bad_schema}' WHERE singleton = true"
        )
    )
    .await
    .expect("Failled to update schema");

    // Let Nexus attempt to initialize with an invalid version.
    //
    // However, after a bit, mimic "operator intervention", where the
    // DB has been upgraded manually.
    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        crdb.batch_execute(
            &format!(
                "UPDATE omicron.public.db_metadata SET version = '{good_schema}' WHERE singleton = true"
            )
        )
        .await
        .expect("Failled to update schema");
    });

    assert!(
        timeout(Duration::from_secs(60), starter.start_nexus_internal(),)
            .await
            .is_ok(),
        "Nexus should have started"
    );

    starter.teardown().await;
}
