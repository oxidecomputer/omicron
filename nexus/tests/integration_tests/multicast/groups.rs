// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Integration tests for multicast group APIs and IP pool operations.
//!
//! Core multicast functionality tests:
//!
//! - IP pool range validation and allocation
//! - Member operations: add, remove, list, lookup by IP
//! - Instance deletion cleanup (removes multicast memberships)
//! - Source IP validation for SSM groups
//! - Automatic pool selection and default pool behavior
//! - Pool exhaustion handling
//! - Pool deletion protection (cannot delete pool with active groups)
//! - DPD(-client) integration: verifies groups are programmed on switches

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use dpd_client::types as dpd_types;
use dropshot::HttpErrorResponseBody;
use dropshot::ResultsPage;
use http::{Method, StatusCode};
use nexus_test_utils::dpd_client;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_instance, create_project, object_create,
    object_create_error, object_delete, object_delete_error, object_get,
    object_get_error,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    InstanceMulticastGroupJoin, IpPoolCreate, MulticastGroupMemberAdd,
};
use nexus_types::external_api::shared::{IpRange, Ipv4Range, Ipv6Range};
use nexus_types::external_api::views::{
    IpPool, IpPoolRange, IpVersion, MulticastGroup, MulticastGroupMember,
};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, InstanceState, NameOrId,
};
use omicron_uuid_kinds::InstanceUuid;

use super::*;
use crate::integration_tests::instances::{
    instance_simulate, instance_wait_for_state,
};

/// Test that multicast IP pools reject invalid ranges at the pool level
#[nexus_test]
async fn test_multicast_ip_pool_range_validation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create IPv4 multicast pool
    let pool_params = IpPoolCreate::new_multicast(
        IdentityMetadataCreateParams {
            name: "test-v4-pool".parse().unwrap(),
            description: "IPv4 multicast pool for validation tests".to_string(),
        },
        IpVersion::V4,
    );
    object_create::<_, IpPool>(client, "/v1/system/ip-pools", &pool_params)
        .await;

    let range_url = "/v1/system/ip-pools/test-v4-pool/ranges/add";

    // IPv4 non-multicast range should be rejected
    let ipv4_unicast_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 255),
        )
        .unwrap(),
    );
    object_create_error(
        client,
        range_url,
        &ipv4_unicast_range,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // IPv4 link-local multicast range should be rejected
    let ipv4_link_local_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(224, 0, 0, 1),
            Ipv4Addr::new(224, 0, 0, 255),
        )
        .unwrap(),
    );
    object_create_error(
        client,
        range_url,
        &ipv4_link_local_range,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Valid IPv4 multicast range should be accepted (using ASM range)
    let valid_ipv4_range = IpRange::V4(
        Ipv4Range::new(
            Ipv4Addr::new(224, 1, 0, 1),
            Ipv4Addr::new(224, 1, 0, 255),
        )
        .unwrap(),
    );
    object_create::<_, IpPoolRange>(client, range_url, &valid_ipv4_range).await;

    // TODO: Remove this test once IPv6 is enabled for multicast pools.
    // IPv6 ranges should currently be rejected (not yet supported)
    let ipv6_range = IpRange::V6(
        Ipv6Range::new(
            Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 255),
        )
        .unwrap(),
    );
    let error = object_create_error(
        client,
        range_url,
        &ipv6_range,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.message, "IPv6 ranges are not allowed yet");
}

#[nexus_test]
async fn test_multicast_group_member_operations(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let instance_name = "test-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client), // For instance networking
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 4, 0, 10),
            (224, 4, 0, 255),
        ),
    )
    .await;

    let instance = create_instance(client, project_name, instance_name).await;
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    let added_member: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;

    assert_eq!(
        added_member.instance_id.to_string(),
        instance.identity.id.to_string()
    );

    // Wait for member to become joined
    // Member starts in "Joining" state and transitions to "Joined" via reconciler
    // Member only transitions to "Joined" after successful DPD update
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Test listing members (should have 1 now in Joined state)
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 1, "Expected exactly 1 member");
    assert_eq!(members[0].instance_id, added_member.instance_id);
    assert_eq!(members[0].multicast_group_id, added_member.multicast_group_id);

    // Test listing groups (should include our implicitly created group)
    let groups = list_multicast_groups(&client).await;
    assert!(
        groups.iter().any(|g| g.identity.name == group_name),
        "Expected group {group_name} to appear in group listing"
    );

    // DPD Validation: Verify groups exist in dataplane after member addition
    let dpd_client = dpd_client(cptestctx);
    // Get the multicast IP from the group (since member doesn't have the IP field)
    let group_get_url = mcast_group_url(group_name);
    let group: MulticastGroup = object_get(client, &group_get_url).await;
    let external_multicast_ip = group.multicast_ip;

    // List all groups in DPD to find both external and underlay groups
    let dpd_groups = dpd_client
        .multicast_groups_list(None, None)
        .await
        .expect("Should list DPD groups");

    // Find the external IPv4 group (should exist but may not have members)
    let expect_msg =
        format!("External group {external_multicast_ip} should exist in DPD");
    dpd_groups
        .items
        .iter()
        .find(|g| {
            let ip = match g {
                dpd_types::MulticastGroupResponse::External {
                    group_ip,
                    ..
                } => *group_ip,
                dpd_types::MulticastGroupResponse::Underlay {
                    group_ip,
                    ..
                } => IpAddr::V6(group_ip.0),
            };
            ip == external_multicast_ip
                && matches!(
                    g,
                    dpd_types::MulticastGroupResponse::External { .. }
                )
        })
        .expect(&expect_msg);

    // Directly get the underlay IPv6 group by finding the admin-scoped address
    // First find the underlay group IP from the list to get the exact IPv6 address
    let underlay_ip = dpd_groups
        .items
        .iter()
        .find_map(|g| {
            match g {
                dpd_types::MulticastGroupResponse::Underlay {
                    group_ip,
                    ..
                } => {
                    // Check if it starts with ff04 (admin-scoped multicast)
                    if group_ip.0.segments()[0] == 0xff04 {
                        Some(group_ip.clone())
                    } else {
                        None
                    }
                }
                dpd_types::MulticastGroupResponse::External { .. } => None,
            }
        })
        .expect("Should find underlay group IP in DPD response");

    // Get the underlay group directly
    let underlay_group = dpd_client
        .multicast_group_get_underlay(&underlay_ip)
        .await
        .expect("Should get underlay group from DPD");

    assert_eq!(
        underlay_group.members.len(),
        1,
        "Underlay group should have exactly 1 member after member addition"
    );

    // Assert all underlay members use rear (backplane) ports with Underlay direction
    for member in &underlay_group.members {
        assert!(
            matches!(member.port_id, dpd_client::types::PortId::Rear(_)),
            "Underlay member should use rear (backplane) port, got: {:?}",
            member.port_id
        );
        assert_eq!(
            member.direction,
            dpd_client::types::Direction::Underlay,
            "Underlay member should have Underlay direction"
        );
    }

    // Test removing instance from multicast group using path-based DELETE
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group_name)
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member from multicast group");

    // Implicit deletion model: group is implicitly deleted when last member is removed
    // Wait for both Nexus group and DPD group to be deleted
    wait_for_group_deleted(client, group_name).await;
    wait_for_group_deleted_from_dpd(cptestctx, external_multicast_ip).await;
}

#[nexus_test]
async fn test_instance_multicast_endpoints(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group1_name = "mcast-group-1";
    let group2_name = "mcast-group-2";
    let instance_name = "test-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 5, 0, 10),
            (224, 5, 0, 255),
        ),
    )
    .await;

    // Implicit deletion model: Groups will implicitly create when first instance joins

    // Create an instance (starts automatically with create_instance helper)
    let instance = create_instance(client, project_name, instance_name).await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // Simulate and wait for instance to be fully running with sled_id assigned
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;
    instance_wait_for_state(client, instance_id, InstanceState::Running).await;
    wait_for_instance_sled_assignment(cptestctx, &instance_id).await;

    // Case: List instance multicast groups (should be empty initially)
    let instance_groups_url = format!(
        "/v1/instances/{instance_name}/multicast-groups?project={project_name}"
    );
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        0,
        "Instance should have no multicast memberships initially"
    );

    // Case: Join group1 using instance-centric endpoint (implicitly creates group1)
    let instance_join_group1_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group1_name}?project={project_name}"
    );
    let join_params = InstanceMulticastGroupJoin { source_ips: None };
    // Use PUT method and expect 201 Created (implicitly creating group1)
    let member1: MulticastGroupMember = NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::PUT,
            &instance_join_group1_url,
        )
        .body(Some(&join_params))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(member1.instance_id, instance.identity.id);

    // Wait for group1 to become active after implicitly create
    wait_for_group_active(client, group1_name).await;

    // Wait for member to become joined
    wait_for_member_state(
        cptestctx,
        group1_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Case: Verify membership shows up in both endpoints
    // Check group-centric view
    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    assert_eq!(group1_members.len(), 1);
    assert_eq!(group1_members[0].instance_id, instance.identity.id);

    // Check instance-centric view (test the list endpoint thoroughly)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        1,
        "Instance should have exactly 1 membership"
    );
    assert_eq!(instance_memberships.items[0].instance_id, instance.identity.id);
    assert_eq!(
        instance_memberships.items[0].multicast_group_id,
        member1.multicast_group_id
    );
    assert_eq!(instance_memberships.items[0].state, "Joined");

    // Join group2 using group-centric endpoint (implicitly creates group2, test both directions)
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group2_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    let member2: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;
    assert_eq!(member2.instance_id, instance.identity.id);

    // Wait for group2 to become active after implicitly create
    wait_for_group_active(client, group2_name).await;

    // Wait for member to become joined
    wait_for_member_state(
        cptestctx,
        group2_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify instance now belongs to both groups (comprehensive list test)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        2,
        "Instance should belong to both groups"
    );

    // Verify the list endpoint returns the correct membership details
    let membership_group_ids: Vec<_> = instance_memberships
        .items
        .iter()
        .map(|m| m.multicast_group_id)
        .collect();
    assert!(
        membership_group_ids.contains(&member1.multicast_group_id),
        "List should include group1 membership"
    );
    assert!(
        membership_group_ids.contains(&member2.multicast_group_id),
        "List should include group2 membership"
    );

    // Verify all memberships show correct instance_id and state
    for membership in &instance_memberships.items {
        assert_eq!(membership.instance_id, instance.identity.id);
        assert_eq!(membership.state, "Joined");
    }

    // Verify each group shows the instance as a member
    let group1_members =
        list_multicast_group_members(&client, group1_name).await;
    let group2_members =
        list_multicast_group_members(&client, group2_name).await;
    assert_eq!(group1_members.len(), 1);
    assert_eq!(group2_members.len(), 1);
    assert_eq!(group1_members[0].instance_id, instance.identity.id);
    assert_eq!(group2_members[0].instance_id, instance.identity.id);

    // Leave group1 using instance-centric endpoint
    let instance_leave_group1_url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group1_name}?project={project_name}"
    );
    object_delete(client, &instance_leave_group1_url).await;

    // Implicit deletion model: group1 should be deleted after last member leaves
    wait_for_group_deleted(client, group1_name).await;

    // Verify membership removed from both views
    // Check instance-centric view - should only show active memberships (group2)
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        1,
        "Instance should only show active membership (group2)"
    );
    assert_eq!(
        instance_memberships.items[0].multicast_group_id,
        member2.multicast_group_id,
        "Remaining membership should be group2"
    );
    assert_eq!(
        instance_memberships.items[0].state, "Joined",
        "Group2 membership should be Joined"
    );

    // Check group2 still has the member (group1 is already deleted)
    let group2_members =
        list_multicast_group_members(&client, group2_name).await;
    assert_eq!(group2_members.len(), 1, "Group2 should still have 1 member");

    // Leave group2 using group-centric endpoint
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group2_name)
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member from group2");

    // Wait for reconciler to process the removal
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify all memberships are gone
    let instance_memberships: ResultsPage<MulticastGroupMember> =
        object_get(client, &instance_groups_url).await;
    assert_eq!(
        instance_memberships.items.len(),
        0,
        "Instance should have no memberships"
    );

    // Implicit deletion model: Groups should be implicitly deleted after last member removed
    ops::join2(
        wait_for_group_deleted(client, group1_name),
        wait_for_group_deleted(client, group2_name),
    )
    .await;
}

#[nexus_test]
async fn test_multicast_group_member_errors(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-group";
    let nonexistent_instance = "nonexistent-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 6, 0, 10),
            (224, 6, 0, 255),
        ),
    )
    .await;

    // Implicitly create a multicast group by adding an instance as first member
    let instance_name = "test-instance";
    create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become active before testing error cases
    wait_for_group_active(&client, group_name).await;

    // Test adding nonexistent instance to group
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(nonexistent_instance.parse().unwrap()),
        source_ips: None,
    };
    object_create_error(
        client,
        &member_add_url,
        &member_params,
        StatusCode::NOT_FOUND,
    )
    .await;

    // Test adding member to nonexistent group
    let nonexistent_group = "nonexistent-group";
    let member_add_bad_group_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(nonexistent_group)
    );
    object_create_error(
        client,
        &member_add_bad_group_url,
        &member_params,
        StatusCode::NOT_FOUND,
    )
    .await;

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, group_name).await;
}

#[nexus_test]
async fn test_lookup_multicast_group_by_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let group_name = "test-lookup-group";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 7, 0, 10),
            (224, 7, 0, 255),
        ),
    )
    .await;

    // Implicitly create multicast group by adding an instance as first member
    let instance_name = "lookup-test-instance";
    create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become active
    wait_for_group_active(&client, group_name).await;

    // Get the group to find its auto-allocated IP address
    let created_group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;
    let multicast_ip = created_group.multicast_ip;

    // Test lookup by IP (using the auto-allocated IP) via the main endpoint
    // The main multicast-groups endpoint now accepts Name, ID, or IP
    let lookup_url = format!("/v1/multicast-groups/{multicast_ip}");
    let found_group: MulticastGroup = object_get(client, &lookup_url).await;
    assert_groups_eq(&created_group, &found_group);

    // Test lookup with nonexistent IP
    let nonexistent_ip = IpAddr::V4(Ipv4Addr::new(224, 0, 1, 200));
    let lookup_bad_url = format!("/v1/multicast-groups/{nonexistent_ip}");

    object_get_error(client, &lookup_bad_url, StatusCode::NOT_FOUND).await;

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, group_name).await;
}

#[nexus_test]
async fn test_instance_deletion_removes_multicast_memberships(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "springfield-squidport"; // Use the same project name as instance helpers
    let group_name = "instance-deletion-group";
    let instance_name = "deletion-test-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "mcast-pool",
            (224, 9, 0, 10),
            (224, 9, 0, 255),
        ),
    )
    .await;

    // Implicitly create multicast group by adding instance as first member
    let instance = create_instance(client, project_name, instance_name).await;
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become active after implicitly create
    wait_for_group_active(&client, group_name).await;

    // Get the group to find its auto-allocated IP address (needed for DPD check)
    let created_group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;
    let multicast_ip = created_group.multicast_ip;

    // Wait for member to join
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify member was added
    let members = list_multicast_group_members(&client, group_name).await;
    assert_eq!(members.len(), 1, "Instance should be a member of the group");
    assert_eq!(members[0].instance_id, instance.identity.id);

    // Case: Instance deletion should clean up multicast memberships
    // Use the helper function for proper instance deletion (handles Starting state)
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;

    // Verify instance is gone
    let instance_url =
        format!("/v1/instances/{instance_name}?project={project_name}");

    object_get_error(client, &instance_url, StatusCode::NOT_FOUND).await;

    // Implicit model: group is implicitly deleted when last member (instance) is removed
    wait_for_group_deleted(client, group_name).await;

    // DPD Validation: Ensure dataplane group is also cleaned up (implicit model)
    let dpd_client = dpd_client(cptestctx);
    let dpd_result = dpd_client.multicast_group_get(&multicast_ip).await;
    assert!(
        dpd_result.is_err(),
        "Multicast group should be deleted from dataplane after last member removed (implicit model)"
    );
}

/// Test that the multicast_ip field is correctly populated in MulticastGroupMember API responses.
/// This validates the denormalized multicast_ip field added for API ergonomics.
#[nexus_test]
async fn test_member_response_includes_multicast_ip(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "multicast-ip-test";
    let group_name = "test-group";
    let instance_name = "test-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "test-pool",
            (224, 30, 0, 1),
            (224, 30, 0, 10),
        ),
    )
    .await;

    // Create instance for implicit group creation
    create_instance(client, project_name, instance_name).await;

    // Implicitly create group via member-add
    let member_add_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };

    // Add member and verify multicast_ip field is present in response
    let added_member: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;

    // Wait for group to become active
    wait_for_group_active(client, group_name).await;

    // Get the group to verify its multicast_ip
    let group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    // Verify multicast_ip field is present in member response
    assert_eq!(
        added_member.multicast_ip, group.multicast_ip,
        "MulticastGroupMember API response should include multicast_ip field that matches the group's IP"
    );

    // Verify multicast_ip is in expected range from the pool
    let member_ip_str = added_member.multicast_ip.to_string();
    assert!(
        member_ip_str.starts_with("224.30.0."),
        "Member multicast_ip should be allocated from the pool range, got: {member_ip_str}"
    );

    // Case: List members and verify multicast_ip in all responses
    let members_list_url = format!(
        "{}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    let members: ResultsPage<MulticastGroupMember> =
        object_get(client, &members_list_url).await;

    assert_eq!(members.items.len(), 1, "Should have exactly one member");
    assert_eq!(
        members.items[0].multicast_ip, group.multicast_ip,
        "Listed member should also include multicast_ip field"
    );

    // Case: Remove and re-add member (reactivation) - verify field preserved
    let member_remove_url = format!(
        "{}/{instance_name}?project={project_name}",
        mcast_group_members_url(group_name)
    );
    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member");

    wait_for_group_deleted(client, group_name).await;

    // Re-create group by adding member again
    let readded_member: MulticastGroupMember =
        object_create(client, &member_add_url, &member_params).await;

    wait_for_group_active(client, group_name).await;

    let new_group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;

    // Verify multicast_ip field is present in re-added member
    assert_eq!(
        readded_member.multicast_ip, new_group.multicast_ip,
        "Re-added member should also have multicast_ip field"
    );

    NexusRequest::new(
        RequestBuilder::new(client, http::Method::DELETE, &member_remove_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should remove member for cleanup");

    wait_for_group_deleted(client, group_name).await;
}

/// Test that we cannot delete a multicast IP pool when multicast groups are
/// linked to it (allocated IPs from it).
///
/// With implicit groups:
/// - Groups implicitly create when instances join
/// - Groups hold IPs from pools while they exist
/// - Pool should be protected while groups exist
/// - After groups are implicitly deleted (last member leaves), pool can be deleted
#[nexus_test]
async fn test_cannot_delete_multicast_pool_with_groups(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "test-project";
    let pool_name = "mcast-pool-delete-test";
    let group_name = "mcast-group-blocks-delete";
    let instance_name = "pool-test-instance";

    // Create project and IP pools in parallel
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            client,
            pool_name,
            (224, 10, 0, 1),
            (224, 10, 0, 10),
        ),
    )
    .await;

    let pool_url = format!("/v1/system/ip-pools/{pool_name}");
    let range_url = format!("/v1/system/ip-pools/{pool_name}/ranges/remove");

    let range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(224, 10, 0, 1),
            std::net::Ipv4Addr::new(224, 10, 0, 10),
        )
        .unwrap(),
    );

    // Verify we can't delete the pool while it has ranges
    let error: HttpErrorResponseBody =
        object_delete_error(client, &pool_url, StatusCode::BAD_REQUEST).await;
    assert_eq!(
        error.message,
        "IP Pool cannot be deleted while it contains IP ranges"
    );

    // Create instance and implicitly create group via member-add (implicit pattern)
    create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for group to become active
    wait_for_group_active(client, group_name).await;

    // Verify we can't delete the range while groups are allocated from it
    let error: HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &range_url)
            .body(Some(&range))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(
        error.message,
        "IP pool ranges cannot be deleted while multicast groups are allocated from them"
    );

    // Verify we still can't delete the pool (indirectly protected by ranges)
    let error: HttpErrorResponseBody =
        object_delete_error(client, &pool_url, StatusCode::BAD_REQUEST).await;
    assert_eq!(
        error.message,
        "IP Pool cannot be deleted while it contains IP ranges"
    );

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, group_name).await;

    // Now we should be able to delete the range
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &range_url)
            .body(Some(&range))
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect(
        "Should be able to delete range after groups are implicitly deleted",
    );

    // And now we should be able to delete the pool
    NexusRequest::object_delete(client, &pool_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("Should be able to delete pool after ranges are deleted");
}

/// Assert that two multicast groups are equal in all fields.
fn assert_groups_eq(left: &MulticastGroup, right: &MulticastGroup) {
    assert_eq!(left.identity.id, right.identity.id);
    assert_eq!(left.identity.name, right.identity.name);
    assert_eq!(left.identity.description, right.identity.description);
    assert_eq!(left.multicast_ip, right.multicast_ip);
    assert_eq!(left.source_ips, right.source_ips);
    assert_eq!(left.mvlan, right.mvlan);
    assert_eq!(left.ip_pool_id, right.ip_pool_id);
}

/// Test that source IPs are validated when joining a multicast group.
///
/// Source IPs enable Source-Specific Multicast (SSM) where traffic is filtered
/// by both destination (multicast IP) and source addresses.
#[nexus_test]
async fn test_source_ip_validation_on_join(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "source-ip-validation-project";
    let group_name = "source-ip-test-group";
    let instance_name = "source-ip-test-instance";

    // Create project and IP pools in parallel
    // SSM groups require an SSM pool (232.x.x.x range for IPv4)
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "source-ip-mcast-pool",
            (232, 1, 0, 1),
            (232, 1, 0, 255),
        ),
    )
    .await;

    // Create instances
    create_instance(client, project_name, instance_name).await;
    let instance2_name = "source-ip-test-instance-2";
    create_instance(client, project_name, instance2_name).await;
    let instance3_name = "source-ip-test-instance-3";
    create_instance(client, project_name, instance3_name).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
    );
    let valid_source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

    // Case: Valid unicast source IP - creates SSM group
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: Some(vec![valid_source]),
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    let group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;
    assert_eq!(group.source_ips, vec![valid_source]);

    // Case: Second instance joining with same source IPs - should succeed
    let member_params2 = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance2_name.parse().unwrap()),
        source_ips: Some(vec![valid_source]),
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params2,
    )
    .await;

    // Case: Third instance joining with different source IPs - should fail
    let different_source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 99));
    let member_params3 = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance3_name.parse().unwrap()),
        source_ips: Some(vec![different_source]),
    };
    let error = object_create_error(
        client,
        &member_add_url,
        &member_params3,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        error.message.contains("source"),
        "Error should mention source IPs mismatch: {}",
        error.message
    );

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &[instance_name, instance2_name, instance3_name],
    )
    .await;
    wait_for_group_deleted(client, group_name).await;
}

/// Test default pool behavior when no pool is specified on member join.
///
/// When a member joins a group without specifying a pool:
/// - If a default multicast pool exists, use it
/// - If no default pool, fail with appropriate error
#[nexus_test]
async fn test_default_pool_on_implicit_creation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let group_name = "default-pool-test-group";
    let group_name2 = "default-pool-test-group-2";
    let project_name = "default-pool-test-project";
    let instance_name = "default-pool-test-instance";

    // Setup: project and default IP pool in parallel (but no multicast pool yet)
    let (_, _) = ops::join2(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
    )
    .await;
    create_instance(client, project_name, instance_name).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
    );

    // Case: Joining when no multicast pool exists - should fail with 400 (no pool available)
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create_error(
        client,
        &member_add_url,
        &member_params,
        StatusCode::BAD_REQUEST,
    )
    .await;

    // Create a default multicast pool
    let mcast_pool =
        create_multicast_ip_pool(&client, "default-mcast-pool").await;

    // Case: Joining when multicast pool exists - should succeed (pool auto-discovered)
    let member_add_url2 = format!(
        "/v1/multicast-groups/{group_name2}/members?project={project_name}"
    );
    let member_params2 = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url2,
        &member_params2,
    )
    .await;

    // Verify group was allocated from default pool
    let group: MulticastGroup =
        object_get(client, &mcast_group_url(group_name2)).await;
    assert_eq!(group.ip_pool_id, mcast_pool.identity.id);

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, group_name2).await;
}

/// Test pool range allocation for multicast groups.
///
/// Verifies that multicast IPs are correctly allocated from the specified
/// pool's ranges, and that exhausted ranges are handled properly.
#[nexus_test]
async fn test_pool_range_allocation(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "pool-range-test-project";

    // Create project and IP pools in parallel
    // Multicast pool has small range (3 IPs: 224.10.0.1-224.10.0.3)
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "small-range-pool",
            (224, 10, 0, 1),
            (224, 10, 0, 3),
        ),
    )
    .await;

    // Create 4 instances for testing
    let instance_names =
        ["range-inst-1", "range-inst-2", "range-inst-3", "range-inst-4"];
    for name in &instance_names {
        create_instance(client, project_name, name).await;
    }

    // Case: Create 3 groups - should succeed (uses all IPs in range)
    for i in 0..3 {
        let group_name = format!("range-group-{i}");
        let member_add_url = format!(
            "/v1/multicast-groups/{group_name}/members?project={project_name}"
        );
        let member_params = MulticastGroupMemberAdd {
            instance: NameOrId::Name(instance_names[i].parse().unwrap()),
            source_ips: None,
        };
        object_create::<_, MulticastGroupMember>(
            client,
            &member_add_url,
            &member_params,
        )
        .await;
    }

    // Case: Try to create 4th group - should fail (range exhausted)
    let group_name4 = "range-group-3";
    let member_add_url4 = format!(
        "/v1/multicast-groups/{group_name4}/members?project={project_name}"
    );
    let member_params4 = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_names[3].parse().unwrap()),
        source_ips: None,
    };
    let error = object_create_error(
        client,
        &member_add_url4,
        &member_params4,
        StatusCode::INSUFFICIENT_STORAGE, // or appropriate error code
    )
    .await;
    assert!(
        error.message.contains("IP")
            || error.message.contains("exhausted")
            || error.message.contains("available"),
        "Error should mention IP exhaustion: {}",
        error.message
    );

    // Case: Delete one group (by removing all members)
    cleanup_instances(cptestctx, client, project_name, &[instance_names[0]])
        .await;
    wait_for_group_deleted(client, "range-group-0").await;

    // Case: Create new group - should succeed (IP reclaimed)
    let group_name_new = "range-group-new";
    let member_add_url_new = format!(
        "/v1/multicast-groups/{group_name_new}/members?project={project_name}"
    );
    let member_params_new = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_names[3].parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url_new,
        &member_params_new,
    )
    .await;

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &[instance_names[1], instance_names[2], instance_names[3]],
    )
    .await;
}

/// Test that groups are allocated from the auto-discovered pool.
///
/// Pool selection is automatic - when multiple pools exist, the first one
/// alphabetically is used (after preferring any default pool).
#[nexus_test]
async fn test_automatic_pool_selection(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "pool-selection-test-project";
    let instance_name = "pool-selection-instance";

    // Setup: project and default IP pool in parallel
    let (_, _) = ops::join2(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
    )
    .await;
    create_instance(client, project_name, instance_name).await;

    // Create a multicast pool (after instance, to test auto-discovery)
    let mcast_pool = create_multicast_ip_pool_with_range(
        &client,
        "mcast-pool",
        (224, 20, 0, 1),
        (224, 20, 0, 10),
    )
    .await;

    // Case: Join group - pool is auto-discovered
    let group_name = "auto-pool-group";
    let member_add_url = format!(
        "/v1/multicast-groups/{group_name}/members?project={project_name}"
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    let group_view: MulticastGroup =
        object_get(client, &mcast_group_url(group_name)).await;
    // Pool is auto-discovered from available multicast pools
    assert_eq!(group_view.ip_pool_id, mcast_pool.identity.id);
    // Verify IP is in pool's range (224.20.0.x)
    if let IpAddr::V4(ip) = group_view.multicast_ip {
        assert_eq!(ip.octets()[0], 224);
        assert_eq!(ip.octets()[1], 20);
    } else {
        panic!("Expected IPv4 multicast address");
    }

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(client, group_name).await;
}

/// Test validation errors for pool exhaustion.
#[nexus_test]
async fn test_pool_exhaustion(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let project_name = "pool-exhaustion-test-project";

    // Create project and IP pools in parallel (multicast pool has single IP)
    let (_, _, _) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "empty-pool",
            (224, 99, 0, 1),
            (224, 99, 0, 1), // Single IP
        ),
    )
    .await;

    // Use the single IP
    let instance_name = "pool-exhaust-instance";
    create_instance(client, project_name, instance_name).await;
    let group_exhaust = "exhaust-empty-pool";
    let member_add_exhaust = format!(
        "/v1/multicast-groups/{group_exhaust}/members?project={project_name}"
    );
    let member_params_exhaust = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
        source_ips: None,
    };
    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_exhaust,
        &member_params_exhaust,
    )
    .await;

    // Now try to create another group - should fail
    let instance2_name = "pool-exhaust-instance-2";
    create_instance(client, project_name, instance2_name).await;
    let group_fail = "fail-empty-pool";
    let member_add_fail = format!(
        "/v1/multicast-groups/{group_fail}/members?project={project_name}"
    );
    let member_params_fail = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance2_name.parse().unwrap()),
        source_ips: None,
    };
    object_create_error(
        client,
        &member_add_fail,
        &member_params_fail,
        StatusCode::INSUFFICIENT_STORAGE,
    )
    .await;

    // Cleanup
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &[instance_name, instance2_name],
    )
    .await;
}

/// Test multiple instances joining different SSM groups from the same SSM pool.
///
/// Verifies:
/// - Pool allocates unique multicast IPs to each SSM group
/// - Different SSM groups can coexist with different source IP requirements
/// - Proper isolation between SSM groups on the same pool
#[nexus_test]
async fn test_multiple_ssm_groups_same_pool(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let project_name = "multiple-ssm-test";

    // Create project and IP pools in parallel
    let (_, _, ssm_pool) = ops::join3(
        create_project(&client, project_name),
        create_default_ip_pool(&client),
        create_multicast_ip_pool_with_range(
            &client,
            "ssm-shared-pool",
            (232, 50, 0, 10),
            (232, 50, 0, 50),
        ),
    )
    .await;

    // Create 3 instances
    let instance_names = ["ssm-inst-1", "ssm-inst-2", "ssm-inst-3"];
    for name in &instance_names {
        create_instance(client, project_name, name).await;
    }

    // Each instance joins a different SSM group with different sources
    let group_configs = [
        ("ssm-group-1", "10.1.1.1"),
        ("ssm-group-2", "10.2.2.2"),
        ("ssm-group-3", "10.3.3.3"),
    ];

    // Create all 3 SSM groups from the same pool
    for (i, (group_name, source_ip)) in group_configs.iter().enumerate() {
        let member_add_url = format!(
            "/v1/multicast-groups/{}/members?project={}",
            group_name, project_name
        );
        let member_params = MulticastGroupMemberAdd {
            instance: NameOrId::Name(instance_names[i].parse().unwrap()),
            source_ips: Some(vec![source_ip.parse().unwrap()]),
        };
        object_create::<_, MulticastGroupMember>(
            client,
            &member_add_url,
            &member_params,
        )
        .await;

        // Wait for group to become active
        wait_for_group_active(client, group_name).await;
    }

    // Verify all groups exist with correct properties
    let mut allocated_ips = Vec::new();
    for (group_name, expected_source) in &group_configs {
        let group: MulticastGroup =
            object_get(client, &mcast_group_url(group_name)).await;

        // Verify pool reference
        assert_eq!(
            group.ip_pool_id, ssm_pool.identity.id,
            "Group {} should reference the shared SSM pool",
            group_name
        );

        // Verify SSM range (232.x.x.x)
        if let IpAddr::V4(ip) = group.multicast_ip {
            assert_eq!(
                ip.octets()[0],
                232,
                "Group {} should have IP in SSM range (232.x.x.x)",
                group_name
            );
            assert_eq!(
                ip.octets()[1],
                50,
                "Group {} should have IP from pool range (232.50.x.x)",
                group_name
            );
        } else {
            panic!("Expected IPv4 multicast address for group {}", group_name);
        }

        // Verify source IPs
        assert_eq!(
            group.source_ips.len(),
            1,
            "Group {} should have exactly 1 source IP",
            group_name
        );
        assert_eq!(
            group.source_ips[0].to_string(),
            *expected_source,
            "Group {} should have correct source IP",
            group_name
        );

        // Collect allocated IP for uniqueness check
        allocated_ips.push(group.multicast_ip);
    }

    // Verify all allocated IPs are unique
    let unique_ips: std::collections::HashSet<_> =
        allocated_ips.iter().collect();
    assert_eq!(
        unique_ips.len(),
        allocated_ips.len(),
        "All SSM groups should have unique multicast IPs from the pool"
    );

    // Verify we can list all members for each group
    for (group_name, _) in &group_configs {
        let members = list_multicast_group_members(&client, group_name).await;
        assert_eq!(
            members.len(),
            1,
            "Group {} should have exactly 1 member",
            group_name
        );
    }

    // Test that instances cannot join groups with different source IPs
    let instance4_name = "ssm-inst-4";
    create_instance(client, project_name, instance4_name).await;

    let member_add_url_wrong_source = format!(
        "/v1/multicast-groups/ssm-group-1/members?project={}",
        project_name
    );
    let member_params_wrong_source = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance4_name.parse().unwrap()),
        source_ips: Some(vec!["10.99.99.99".parse().unwrap()]), // Different from group's 10.1.1.1
    };
    let error = object_create_error(
        client,
        &member_add_url_wrong_source,
        &member_params_wrong_source,
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(
        error.message.contains("source"),
        "Should reject instance joining SSM group with different source IPs: {}",
        error.message
    );

    let all_instances: Vec<_> = instance_names
        .iter()
        .chain(std::iter::once(&instance4_name))
        .map(|s| *s)
        .collect();
    cleanup_instances(cptestctx, client, project_name, &all_instances).await;

    // Verify all groups are deleted
    for (group_name, _) in &group_configs {
        wait_for_group_deleted(client, group_name).await;
    }
}
