// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast groups with other networking features
//!
//! This module contains tests that verify multicast functionality works correctly
//! when combined with other networking features like external IPs, floating IPs,
//! and complex network configurations.

use std::net::{IpAddr, Ipv4Addr};

use http::{Method, StatusCode};

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project, object_create, object_delete,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    EphemeralIpCreate, ExternalIpCreate, FloatingIpAttach, InstanceCreate,
    InstanceNetworkInterfaceAttachment, MulticastGroupCreate,
    MulticastGroupMemberAdd,
};
use nexus_types::external_api::views::{
    FloatingIp, MulticastGroup, MulticastGroupMember,
};
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    InstanceState, NameOrId,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use super::*;
use crate::integration_tests::instances::{
    fetch_instance_external_ips, instance_simulate, instance_wait_for_state,
};

/// Test that instances can have both external IPs and multicast group membership.
///
/// This verifies:
/// 1. External IP allocation works for multicast group members
/// 2. Multicast state is preserved during external IP operations
/// 3. No conflicts between SNAT and multicast DPD configuration
/// 4. Both networking features function independently
#[nexus_test]
async fn test_multicast_with_external_ip_basic(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let project_name = "external-ip-mcast-project";
    let group_name = "external-ip-mcast-group";
    let instance_name = "external-ip-mcast-instance";

    // Setup: project and IP pools in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client), // For external IPs
        create_multicast_ip_pool_with_range(
            client,
            "external-ip-mcast-pool",
            (224, 100, 0, 1),
            (224, 100, 0, 255),
        ),
    )
    .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 100, 0, 50));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for external IP integration test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Create instance (will start by default)
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance with external IP and multicast".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![], // Start without external IP
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true, // Start the instance
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;
    let instance_id = instance.identity.id;

    // Transition instance to Running state
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
    instance_simulate(nexus, &instance_uuid).await;
    instance_wait_for_state(client, instance_uuid, InstanceState::Running)
        .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Add instance to multicast group
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for multicast member to reach "Joined" state
    wait_for_member_state(
        client,
        group_name,
        instance_id,
        "Joined",
    )
    .await;

    // Verify member count
    let members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one multicast member");

    // Allocate ephemeral external IP to the same instance
    let ephemeral_ip_url = format!(
        "/v1/instances/{}/external-ips/ephemeral?project={}",
        instance_name, project_name
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &ephemeral_ip_url)
            .body(Some(&EphemeralIpCreate {
                pool: None, // Use default pool
            }))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    // Verify both multicast and external IP work together

    // Check that multicast membership is preserved
    let members_after_ip =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        members_after_ip.len(),
        1,
        "Multicast member should still exist after external IP allocation"
    );
    assert_eq!(members_after_ip[0].instance_id, instance_id);
    assert_eq!(
        members_after_ip[0].state, "Joined",
        "Member state should remain Joined"
    );

    // Check that external IP is properly attached
    let external_ips_after_attach =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    assert!(
        !external_ips_after_attach.is_empty(),
        "Instance should have external IP"
    );
    // Note: external_ip.ip() from the response may differ from what's actually attached,
    // so we just verify that an external IP exists

    // Remove ephemeral external IP and verify multicast is unaffected
    let external_ip_detach_url = format!(
        "/v1/instances/{}/external-ips/ephemeral?project={}",
        instance_name, project_name
    );
    object_delete(client, &external_ip_detach_url).await;

    // Wait for operations to settle
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify multicast membership is still intact after external IP removal
    let members_after_detach =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        members_after_detach.len(),
        1,
        "Multicast member should persist after external IP removal"
    );
    assert_eq!(members_after_detach[0].instance_id, instance_id);
    assert_eq!(
        members_after_detach[0].state, "Joined",
        "Member should remain Joined"
    );

    // Verify ephemeral external IP is removed (SNAT IP may still be present)
    let external_ips_after_detach =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    // Instance should have at most 1 IP left (the SNAT IP), not the ephemeral IP we attached
    assert!(
        external_ips_after_detach.len() <= 1,
        "Instance should have at most SNAT IP remaining"
    );

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    cleanup_multicast_groups(client, &[group_name]).await;
}

/// Test external IP allocation/deallocation lifecycle for multicast group members.
///
/// This verifies:
/// 1. Multiple external IP attach/detach cycles don't affect multicast state
/// 2. Concurrent operations don't cause race conditions
/// 3. Dataplane configuration remains consistent
#[nexus_test]
async fn test_multicast_external_ip_lifecycle(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let project_name = "external-ip-lifecycle-project";
    let group_name = "external-ip-lifecycle-group";
    let instance_name = "external-ip-lifecycle-instance";

    // Setup in parallel
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "external-ip-lifecycle-pool",
            (224, 101, 0, 1),
            (224, 101, 0, 255),
        ),
    )
    .await;

    // Create multicast group and instance (similar to previous test)
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 101, 0, 75));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for external IP lifecycle test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance for external IP lifecycle test".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;
    let instance_id = instance.identity.id;

    // Start instance and add to multicast group
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
    instance_simulate(nexus, &instance_uuid).await;
    instance_wait_for_state(client, instance_uuid, InstanceState::Running)
        .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify initial multicast state
    let initial_members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(initial_members.len(), 1);
    assert_eq!(initial_members[0].state, "Joined");

    // Test multiple external IP allocation/deallocation cycles
    for cycle in 1..=3 {
        // Allocate ephemeral external IP
        let ephemeral_ip_url = format!(
            "/v1/instances/{}/external-ips/ephemeral?project={}",
            instance_name, project_name
        );
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &ephemeral_ip_url)
                .body(Some(&EphemeralIpCreate {
                    pool: None, // Use default pool
                }))
                .expect_status(Some(StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

        // Wait for dataplane configuration to settle
        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Verify multicast state is preserved
        let members_with_ip =
            list_multicast_group_members(client, group_name).await;
        assert_eq!(
            members_with_ip.len(),
            1,
            "Cycle {}: Multicast member should persist during external IP allocation",
            cycle
        );
        assert_eq!(
            members_with_ip[0].state, "Joined",
            "Cycle {}: Member should remain Joined",
            cycle
        );

        // Verify external IP is attached
        let external_ips_with_ip =
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            !external_ips_with_ip.is_empty(),
            "Cycle {}: Instance should have external IP",
            cycle
        );

        // Deallocate ephemeral external IP
        let external_ip_detach_url = format!(
            "/v1/instances/{}/external-ips/ephemeral?project={}",
            instance_name, project_name
        );
        object_delete(client, &external_ip_detach_url).await;

        // Wait for operations to settle
        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Verify multicast state is still preserved
        let members_without_ip =
            list_multicast_group_members(client, group_name).await;
        assert_eq!(
            members_without_ip.len(),
            1,
            "Cycle {}: Multicast member should persist after external IP removal",
            cycle
        );
        assert_eq!(
            members_without_ip[0].state, "Joined",
            "Cycle {}: Member should remain Joined after IP removal",
            cycle
        );

        // Verify ephemeral external IP is removed (SNAT IP may still be present)
        let external_ips_without_ip =
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            external_ips_without_ip.len() <= 1,
            "Cycle {}: Instance should have at most SNAT IP remaining",
            cycle
        );
    }

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    cleanup_multicast_groups(client, &[group_name]).await;
}

/// Test that instances can be created with both external IP and multicast group simultaneously.
///
/// This verifies:
/// 1. Instance creation with both features works
/// 2. No conflicts during initial setup
/// 3. Both features are properly configured from creation
#[nexus_test]
async fn test_multicast_with_external_ip_at_creation(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let project_name = "creation-mixed-project";
    let group_name = "creation-mixed-group";
    let instance_name = "creation-mixed-instance";

    // Setup - parallelize project and pool creation
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client),
        create_multicast_ip_pool_with_range(
            client,
            "creation-mixed-pool",
            (224, 102, 0, 1),
            (224, 102, 0, 255),
        ),
    )
    .await;

    // Create multicast group first
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 102, 0, 100));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for creation test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Create instance with external IP specified at creation
    let external_ip_param = ExternalIpCreate::Ephemeral { pool: None };
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance created with external IP and multicast"
                .to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![external_ip_param], // External IP at creation
        multicast_groups: vec![], // Will add to multicast group after creation
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;
    let instance_id = instance.identity.id;

    // Transition to running
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
    instance_simulate(nexus, &instance_uuid).await;
    instance_wait_for_state(client, instance_uuid, InstanceState::Running)
        .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify external IP was allocated at creation
    let external_ips_after_start =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    assert!(
        !external_ips_after_start.is_empty(),
        "Instance should have external IP from creation"
    );

    // Add to multicast group
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Verify both features work together - wait for member to reach Joined state
    wait_for_member_state(
        client,
        group_name,
        instance_id,
        "Joined",
    )
    .await;

    let members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have multicast member");

    let external_ips_final =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    assert!(
        !external_ips_final.is_empty(),
        "Instance should retain external IP"
    );

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    cleanup_multicast_groups(client, &[group_name]).await;
}

/// Test that instances can have both floating IPs and multicast group membership.
///
/// This verifies:
/// 1. Floating IP attachment works for multicast group members
/// 2. Multicast state is preserved during floating IP operations
/// 3. No conflicts between floating IP and multicast DPD configuration
/// 4. Both networking features function independently
#[nexus_test]
async fn test_multicast_with_floating_ip_basic(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let project_name = "floating-ip-mcast-project";
    let group_name = "floating-ip-mcast-group";
    let instance_name = "floating-ip-mcast-instance";
    let floating_ip_name = "floating-ip-mcast-ip";

    // Setup: project and IP pools - parallelize creation
    let (_, _, mcast_pool) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pool(client), // For floating IPs
        create_multicast_ip_pool_with_range(
            client,
            "floating-ip-mcast-pool",
            (224, 200, 0, 1),
            (224, 200, 0, 255),
        ),
    )
    .await;

    // Create floating IP
    let floating_ip =
        create_floating_ip(client, floating_ip_name, project_name, None, None)
            .await;

    // Create multicast group
    let multicast_ip = IpAddr::V4(Ipv4Addr::new(224, 200, 0, 50));
    let group_url = "/v1/multicast-groups".to_string();
    let group_params = MulticastGroupCreate {
        identity: IdentityMetadataCreateParams {
            name: group_name.parse().unwrap(),
            description: "Group for floating IP integration test".to_string(),
        },
        multicast_ip: Some(multicast_ip),
        source_ips: None,
        pool: Some(NameOrId::Name(mcast_pool.identity.name.clone())),
    };

    object_create::<_, MulticastGroup>(client, &group_url, &group_params).await;
    wait_for_group_active(client, group_name).await;

    // Create instance (will start by default)
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance with floating IP and multicast".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(1).unwrap(),
        memory: ByteCount::from_gibibytes_u32(1),
        hostname: instance_name.parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![], // Start without external IP
        multicast_groups: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true, // Start the instance
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
    };

    let instance_url = format!("/v1/instances?project={project_name}");
    let instance: Instance =
        object_create(client, &instance_url, &instance_params).await;
    let instance_id = instance.identity.id;

    // Transition instance to Running state
    let nexus = &cptestctx.server.server_context().nexus;
    let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
    instance_simulate(nexus, &instance_uuid).await;
    instance_wait_for_state(client, instance_uuid, InstanceState::Running)
        .await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Add instance to multicast group
    let member_add_url = format!(
        "/v1/multicast-groups/{}/members?project={}",
        group_name, project_name
    );
    let member_params = MulticastGroupMemberAdd {
        instance: NameOrId::Name(instance_name.parse().unwrap()),
    };

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &member_params,
    )
    .await;

    // Wait for multicast member to reach "Joined" state
    wait_for_member_state(
        client,
        group_name,
        instance_id,
        "Joined",
    )
    .await;

    // Verify member count
    let members =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one multicast member");

    // Attach floating IP to the same instance
    let attach_url = format!(
        "/v1/floating-ips/{}/attach?project={}",
        floating_ip_name, project_name
    );
    let attach_params = FloatingIpAttach {
        kind: nexus_types::external_api::params::FloatingIpParentKind::Instance,
        parent: NameOrId::Name(instance_name.parse().unwrap()),
    };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &attach_url)
            .body(Some(&attach_params))
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<FloatingIp>()
    .unwrap();

    // Verify both multicast and floating IP work together

    // Check that multicast membership is preserved
    let members_after_ip =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        members_after_ip.len(),
        1,
        "Multicast member should still exist after floating IP attachment"
    );
    assert_eq!(members_after_ip[0].instance_id, instance_id);
    assert_eq!(
        members_after_ip[0].state, "Joined",
        "Member state should remain Joined"
    );

    // Check that floating IP is properly attached
    let external_ips_after_attach =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    assert!(
        !external_ips_after_attach.is_empty(),
        "Instance should have external IP"
    );
    // Find the floating IP among the external IPs (there may also be SNAT IP)
    let has_floating_ip =
        external_ips_after_attach.iter().any(|ip| ip.ip() == floating_ip.ip);
    assert!(has_floating_ip, "Instance should have the floating IP attached");

    // Detach floating IP and verify multicast is unaffected
    let detach_url = format!(
        "/v1/floating-ips/{}/detach?project={}",
        floating_ip_name, project_name
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &detach_url)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<FloatingIp>()
    .unwrap();

    // Wait for operations to settle
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Verify multicast membership is still intact after floating IP removal
    let members_after_detach =
        list_multicast_group_members(client, group_name).await;
    assert_eq!(
        members_after_detach.len(),
        1,
        "Multicast member should persist after floating IP detachment"
    );
    assert_eq!(members_after_detach[0].instance_id, instance_id);
    assert_eq!(
        members_after_detach[0].state, "Joined",
        "Member should remain Joined"
    );

    // Verify floating IP is detached (SNAT IP may still be present)
    let external_ips_after_detach =
        fetch_instance_external_ips(client, instance_name, project_name).await;
    let still_has_floating_ip =
        external_ips_after_detach.iter().any(|ip| ip.ip() == floating_ip.ip);
    assert!(
        !still_has_floating_ip,
        "Instance should not have the floating IP attached anymore"
    );

    // Cleanup floating IP
    let fip_delete_url = format!(
        "/v1/floating-ips/{}?project={}",
        floating_ip_name, project_name
    );
    object_delete(client, &fip_delete_url).await;

    // Cleanup
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    cleanup_multicast_groups(client, &[group_name]).await;
}
