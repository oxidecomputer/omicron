// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for multicast enablement functionality.
//!
//! TODO: Remove once we have full multicast support in PROD.

use std::net::IpAddr;

use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project, object_create, object_get,
};
use nexus_test_utils::{load_test_config, test_setup_with_config};
use nexus_types::external_api::params::MulticastGroupCreate;
use nexus_types::external_api::views::MulticastGroup;
use omicron_common::api::external::{
    IdentityMetadataCreateParams, Instance, InstanceState, NameOrId,
};
use omicron_sled_agent::sim;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use super::*;
use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state,
};

const PROJECT_NAME: &str = "multicast-enablement-test";
const GROUP_NAME: &str = "test-group";

/// Test that when multicast is disabled, instance lifecycle operations
/// and group attachment APIs skip multicast operations but complete successfully,
/// and no multicast members are ever created.
#[tokio::test]
async fn test_multicast_enablement() {
    // Create custom config with multicast disabled (simulating PROD, for now)
    let mut config = load_test_config();
    config.pkg.multicast.enabled = false;

    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_multicast_enablement",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
        DEFAULT_SP_SIM_CONFIG.into(),
    )
    .await;

    let client = &cptestctx.external_client;

    // Set up project and multicast infrastructure
    create_default_ip_pool(&client).await;
    create_project(client, PROJECT_NAME).await;
    let _pool = create_multicast_ip_pool(client, "test-pool").await;

    // Create a multicast group
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: GROUP_NAME.parse().unwrap(),
            description: "Test group for enablement testing".to_string(),
        },
        multicast_ip: Some("224.0.1.100".parse::<IpAddr>().unwrap()),
        source_ips: None,
        pool: Some(NameOrId::Name("test-pool".parse().unwrap())),
    };

    let group_url = "/v1/multicast-groups".to_string();
    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;

    // Create instance with multicast groups specified
    // This should succeed even with multicast disabled
    let instance = instance_for_multicast_groups(
        &cptestctx,
        PROJECT_NAME,
        "test-instance-lifecycle",
        false, // don't start initially
        &[GROUP_NAME],
    )
    .await;

    // Verify instance was created successfully
    assert_eq!(instance.identity.name, "test-instance-lifecycle");

    // Verify NO multicast members were created (since multicast is disabled)
    let members =
        list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(
        members.len(),
        0,
        "No multicast members should be created when disabled"
    );

    // Start the instance - this should also succeed
    let start_url = format!(
        "/v1/instances/{}/start?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::POST,
            &start_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Instance start should succeed even with multicast disabled");

    // Simulate the instance to complete the start transition
    let get_url_for_start_sim = format!(
        "/v1/instances/{}?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );
    let instance_for_start_sim: Instance =
        object_get(client, &get_url_for_start_sim).await;
    let instance_id_for_start_sim =
        InstanceUuid::from_untyped_uuid(instance_for_start_sim.identity.id);
    instance_simulate(
        &cptestctx.server.server_context().nexus,
        &instance_id_for_start_sim,
    )
    .await;

    // Still no multicast members should exist
    let members =
        list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(
        members.len(),
        0,
        "No multicast members should be created during start when disabled"
    );

    // Stop the instance - this should also succeed
    let stop_url = format!(
        "/v1/instances/{}/stop?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::POST,
            &stop_url,
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(http::StatusCode::ACCEPTED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Instance stop should succeed even with multicast disabled");

    let get_url_for_sim = format!(
        "/v1/instances/{}?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );

    let instance_for_sim: Instance = object_get(client, &get_url_for_sim).await;
    let instance_id_for_sim =
        InstanceUuid::from_untyped_uuid(instance_for_sim.identity.id);
    // Simulate the instance to complete the stop transition
    instance_simulate(
        &cptestctx.server.server_context().nexus,
        &instance_id_for_sim,
    )
    .await;

    // Still no multicast members should exist
    let members =
        list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(
        members.len(),
        0,
        "No multicast members should be created during stop when disabled"
    );

    // Wait for instance to be fully stopped before attempting deletion
    let get_url = format!(
        "/v1/instances/{}?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );
    let stopped_instance: Instance = object_get(client, &get_url).await;
    let instance_id =
        InstanceUuid::from_untyped_uuid(stopped_instance.identity.id);

    // Wait for the instance to be stopped
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Delete the instance - this should now succeed
    let delete_url = format!(
        "/v1/instances/{}?project={}",
        "test-instance-lifecycle", PROJECT_NAME
    );
    nexus_test_utils::resource_helpers::object_delete(client, &delete_url)
        .await;

    // Verify no multicast state was ever created
    let members =
        list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(
        members.len(),
        0,
        "No multicast members should exist after instance deletion when disabled"
    );

    // Test API-level group attachment when disabled

    // Create another instance without multicast groups initially
    instance_for_multicast_groups(
        &cptestctx,
        PROJECT_NAME,
        "test-instance-api",
        false,
        &[], // No groups initially
    )
    .await;

    // Try to attach to multicast group via API - should succeed
    let attach_url = format!(
        "/v1/instances/{}/multicast-groups/{}?project={}",
        "test-instance-api", GROUP_NAME, PROJECT_NAME
    );

    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::PUT,
            &attach_url,
        )
        .expect_status(Some(http::StatusCode::CREATED)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Multicast group attach should succeed even when disabled");

    // Verify that direct API calls DO create member records even when disabled
    // (This is correct behavior for experimental APIs - they handle config management)
    let members =
        list_multicast_group_members(client, GROUP_NAME).await;
    assert_eq!(
        members.len(),
        1,
        "Direct API calls should create member records even when disabled (experimental API behavior)"
    );

    cptestctx.teardown().await;
}
