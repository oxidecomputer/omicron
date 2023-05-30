// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

use nexus_test_interface::NexusServer;
use nexus_test_utils::{load_test_config, ControlPlaneTestContextBuilder};

#[tokio::test]
async fn test_nexus_boots_before_cockroach() {
    let mut config = load_test_config();

    let mut builder =
        ControlPlaneTestContextBuilder::<omicron_nexus::Server>::new(
            "test_nexus_boots_before_cockroach",
            &mut config,
        );

    let log = builder.logctx.log.new(o!("component" => "test"));

    builder.start_dendrite().await;
    builder.start_internal_dns().await;
    builder.start_external_dns().await;

    // Start Nexus, referencing the internal DNS system.
    //
    // This call won't return successfully until we can...
    // 1. Contact the internal DNS system to find Cockroach
    // 2. Contact Cockroach to ensure the database has been populated
    builder.config.deployment.database =
        omicron_common::nexus_config::Database::FromDns;
    builder.config.deployment.internal_dns =
        omicron_common::nexus_config::InternalDns::FromAddress {
            address: *builder
                .internal_dns
                .as_ref()
                .expect("Must start Internal DNS before acquiring an address")
                .dns_server
                .local_address(),
        };
    let nexus_config = builder.config.clone();
    let nexus_log = log.clone();
    let nexus_handle = tokio::task::spawn(async move {
        info!(nexus_log, "Test: Trying to start Nexus (internal)");
        omicron_nexus::Server::start_internal(&nexus_config, &nexus_log).await;
        info!(nexus_log, "Test: Started Nexus (internal)");
    });

    // Start Cockroach and populate the Internal DNS system with CRDB records.
    //
    // This is necessary for the prior call to "start Nexus" to succeed.
    info!(log, "Starting CRDB");
    builder.start_crdb().await;
    info!(log, "Started CRDB");

    info!(log, "Populating internal DNS records");
    builder.populate_internal_dns().await;
    info!(log, "Populated internal DNS records");

    // Now that Cockroach has started, we expect the request to succeed.
    nexus_handle.await.expect("Test: Task starting Nexus has failed");

    builder.teardown().await;
}

#[tokio::test]
async fn test_nexus_boots_before_dendrite() {
    let mut config = load_test_config();

    let mut builder =
        ControlPlaneTestContextBuilder::<omicron_nexus::Server>::new(
            "test_nexus_boots_before_dendrite",
            &mut config,
        );

    let log = builder.logctx.log.new(o!("component" => "test"));

    builder.start_crdb().await;
    builder.start_internal_dns().await;
    builder.start_external_dns().await;

    // Start Nexus, referencing the internal DNS system.
    //
    // This call won't return successfully until we can...
    // 1. Contact the internal DNS system to find Dendrite
    // 2. Contact Dendrite
    builder.config.deployment.database =
        omicron_common::nexus_config::Database::FromUrl {
            url: builder
                .database
                .as_ref()
                .expect("Must start CRDB first")
                .pg_config()
                .clone(),
        };
    builder.config.pkg.dendrite = HashMap::new();
    builder.config.deployment.internal_dns =
        omicron_common::nexus_config::InternalDns::FromAddress {
            address: *builder
                .internal_dns
                .as_ref()
                .expect("Must start Internal DNS before acquiring an address")
                .dns_server
                .local_address(),
        };
    let nexus_config = builder.config.clone();
    let nexus_log = log.clone();
    let nexus_handle = tokio::task::spawn(async move {
        info!(nexus_log, "Test: Trying to start Nexus (internal)");
        omicron_nexus::Server::start_internal(&nexus_config, &nexus_log).await;
        info!(nexus_log, "Test: Started Nexus (internal)");
    });

    // Start Dendrite and populate the Internal DNS system.
    //
    // This is necessary for the prior call to "start Nexus" to succeed.
    info!(log, "Starting Dendrite");
    builder.start_dendrite().await;
    info!(log, "Started Dendrite");

    info!(log, "Populating internal DNS records");
    builder.populate_internal_dns().await;
    info!(log, "Populated internal DNS records");

    // Now that Dendrite has started, we expect the request to succeed.
    nexus_handle.await.expect("Test: Task starting Nexus has failed");

    builder.teardown().await;
}
