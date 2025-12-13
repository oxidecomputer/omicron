// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for multicast enablement functionality.
//!
//! TODO: Remove once we have full multicast support in PROD.

use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project, object_get,
};
use omicron_common::api::external::{Instance, InstanceState};
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
    let cptestctx =
        nexus_test_utils::ControlPlaneBuilder::new("test_multicast_enablement")
            .customize_nexus_config(&|config| {
                // Create custom config with multicast disabled (simulating
                // PROD, for now)
                config.pkg.multicast.enabled = false;
            })
            .start::<omicron_nexus::Server>()
            .await;

    let client = &cptestctx.external_client;

    // Create project and pools in parallel
    let (_, _, _) = ops::join3(
        create_default_ip_pool(&client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "test-pool"),
    )
    .await;

    // Note: With the implicit creation model, groups are created when first member joins.
    // When multicast is disabled, instance create should not implicitly create groups/members.

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

    // Verify the group doesn't exist at all (since multicast is disabled and no members were created)
    // With implicit creation, groups only exist when they have members
    let group_url = mcast_group_url(GROUP_NAME);
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::GET,
            &group_url,
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should get 404 response");

    // Start the instance - this should also succeed
    let start_url = format!(
        "/v1/instances/test-instance-lifecycle/start?project={PROJECT_NAME}"
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
    let get_url_for_start_sim =
        format!("/v1/instances/test-instance-lifecycle?project={PROJECT_NAME}");
    let instance_for_start_sim: Instance =
        object_get(client, &get_url_for_start_sim).await;
    let instance_id_for_start_sim =
        InstanceUuid::from_untyped_uuid(instance_for_start_sim.identity.id);
    instance_simulate(
        &cptestctx.server.server_context().nexus,
        &instance_id_for_start_sim,
    )
    .await;

    // Verify the group still doesn't exist (multicast disabled, no members created)
    let group_url_after_start = mcast_group_url(GROUP_NAME);
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::GET,
            &group_url_after_start,
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should get 404 response after instance start");

    // Stop the instance - this should also succeed
    let stop_url = format!(
        "/v1/instances/test-instance-lifecycle/stop?project={PROJECT_NAME}"
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

    let get_url_for_sim =
        format!("/v1/instances/test-instance-lifecycle?project={PROJECT_NAME}");

    let instance_for_sim: Instance = object_get(client, &get_url_for_sim).await;
    let instance_id_for_sim =
        InstanceUuid::from_untyped_uuid(instance_for_sim.identity.id);
    // Simulate the instance to complete the stop transition
    instance_simulate(
        &cptestctx.server.server_context().nexus,
        &instance_id_for_sim,
    )
    .await;

    // Verify the group still doesn't exist (multicast disabled, no members created)
    let group_url_after_stop = mcast_group_url(GROUP_NAME);
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::GET,
            &group_url_after_stop,
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should get 404 response after instance stop");

    // Wait for instance to be fully stopped before attempting deletion
    let get_url =
        format!("/v1/instances/test-instance-lifecycle?project={PROJECT_NAME}");
    let stopped_instance: Instance = object_get(client, &get_url).await;
    let instance_id =
        InstanceUuid::from_untyped_uuid(stopped_instance.identity.id);

    // Wait for the instance to be stopped
    instance_wait_for_state(client, instance_id, InstanceState::Stopped).await;

    // Delete the instance - this should now succeed
    let delete_url =
        format!("/v1/instances/test-instance-lifecycle?project={PROJECT_NAME}");
    nexus_test_utils::resource_helpers::object_delete(client, &delete_url)
        .await;

    // Verify no multicast state was ever created (group still doesn't exist)
    let group_url_after_delete = mcast_group_url(GROUP_NAME);
    nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::GET,
            &group_url_after_delete,
        )
        .expect_status(Some(http::StatusCode::NOT_FOUND)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should get 404 response after instance deletion");

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

    // Try to attach to multicast group via API
    // When multicast is disabled, this should fail
    let attach_url = format!(
        "/v1/instances/test-instance-api/multicast-groups/{GROUP_NAME}?project={PROJECT_NAME}"
    );

    let attach_body = serde_json::json!({});
    let attach_response = nexus_test_utils::http_testing::NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            client,
            http::Method::PUT,
            &attach_url,
        )
        .body(Some(&attach_body))
        .expect_status(Some(http::StatusCode::BAD_REQUEST)),
    )
    .authn_as(nexus_test_utils::http_testing::AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should get response from attach attempt");

    // Verify the error message indicates multicast is disabled
    let error: dropshot::HttpErrorResponseBody =
        attach_response.parsed_body().expect("Should parse error body");
    assert!(
        error.message.contains("multicast functionality is currently disabled"),
        "Error message should indicate multicast is disabled, got: {}",
        error.message
    );

    cptestctx.teardown().await;
}
