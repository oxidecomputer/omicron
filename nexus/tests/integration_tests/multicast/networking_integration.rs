// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast groups with other networking features.
//!
//! Tests multicast + networking feature interactions:
//!
//! - External IPs: Instances with ephemeral/floating IPs can join multicast groups
//! - Floating IP attach/detach: Multicast membership unaffected by IP changes
//! - Complex network configs: Multiple NICs, VPCs, subnets with multicast

use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_project, object_create, object_delete,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::{
    EphemeralIpCreate, ExternalIpCreate, FloatingIpAttach, InstanceCreate,
    InstanceNetworkInterfaceAttachment, PoolSelector,
};
use nexus_types::external_api::views::FloatingIp;
use omicron_common::api::external::IpVersion;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    InstanceState, NameOrId,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use super::*;
use crate::integration_tests::instances::{
    fetch_instance_external_ips, instance_simulate, instance_wait_for_state,
};

/// Consolidated test for external IP scenarios with multicast group membership.
///
/// This test covers three scenarios with shared setup:
/// - Case 1: Basic external IP attach/detach with multicast
/// - Case 2: Lifecycle with 1-2 attach/detach cycles
/// - Case 3: External IP at instance creation
#[nexus_test]
async fn test_multicast_external_ip_scenarios(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "external-ip-scenarios-project";

    // Shared setup: project and IP pools
    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client), // For external IPs
        create_multicast_ip_pool_with_range(
            client,
            "external-ip-scenarios-pool",
            (224, 100, 0, 1),
            (224, 100, 0, 255),
        ),
    )
    .await;

    // Ensure multicast test prerequisites (inventory + DPD) are ready
    ensure_multicast_test_ready(cptestctx).await;

    // -------------------------------------------------------------------------
    // Case 1: Basic external IP attach/detach with multicast
    // -------------------------------------------------------------------------
    // Verify instances can have both external IPs and multicast group membership.
    // External IP allocation works for multicast group members, multicast state
    // persists through external IP operations.
    {
        let instance_name = "basic-attach-detach-instance";
        let group_name = "basic-attach-detach-group";

        // Create instance (will start by default)
        let instance_params = InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: "Instance with external IP and multicast"
                    .to_string(),
            },
            ncpus: InstanceCpuCount::try_from(1).unwrap(),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: instance_name.parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
        instance_simulate(nexus, &instance_uuid).await;
        instance_wait_for_state(client, instance_uuid, InstanceState::Running)
            .await;

        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Add instance to multicast group via instance-centric API
        multicast_group_attach(
            cptestctx,
            project_name,
            instance_name,
            group_name,
        )
        .await;
        wait_for_group_active(client, group_name).await;

        // Wait for multicast member to reach "Joined" state
        wait_for_member_state(
            cptestctx,
            group_name,
            instance_id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;

        // Verify member count
        let members = list_multicast_group_members(client, group_name).await;
        assert_eq!(members.len(), 1, "Should have one multicast member");

        // Allocate ephemeral external IP to the same instance
        let ephemeral_ip_url = format!(
            "/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}"
        );
        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, &ephemeral_ip_url)
                .body(Some(&EphemeralIpCreate {
                    pool_selector: PoolSelector::Auto {
                        ip_version: Some(IpVersion::V4),
                    },
                }))
                .expect_status(Some(StatusCode::ACCEPTED)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap();

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
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            !external_ips_after_attach.is_empty(),
            "Instance should have external IP"
        );

        // Remove ephemeral external IP and verify multicast is unaffected
        let external_ip_detach_url = format!(
            "/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}"
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
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            external_ips_after_detach.len() <= 1,
            "Instance should have at most SNAT IP remaining"
        );

        // Cleanup Case 1
        cleanup_instances(cptestctx, client, project_name, &[instance_name])
            .await;
        wait_for_group_deleted(cptestctx, group_name).await;
    }

    // -------------------------------------------------------------------------
    // Case 2: Lifecycle with 1-2 attach/detach cycles
    // -------------------------------------------------------------------------
    // Verify external IP allocation/deallocation lifecycle for multicast group
    // members. Multiple external IP attach/detach cycles don't affect multicast
    // state and dataplane configuration remains consistent throughout.
    {
        let instance_name = "lifecycle-instance";
        let group_name = "lifecycle-group";

        // Create instance
        let instance_params = InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: "Instance for external IP lifecycle test"
                    .to_string(),
            },
            ncpus: InstanceCpuCount::try_from(1).unwrap(),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: instance_name.parse().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
        instance_simulate(nexus, &instance_uuid).await;
        instance_wait_for_state(client, instance_uuid, InstanceState::Running)
            .await;

        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Add instance to multicast group via instance-centric API
        multicast_group_attach(
            cptestctx,
            project_name,
            instance_name,
            group_name,
        )
        .await;
        wait_for_group_active(client, group_name).await;

        // Wait for member to transition from "Joining"->"Joined"
        wait_for_member_state(
            cptestctx,
            group_name,
            instance_id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;

        // Verify initial multicast state
        let initial_members =
            list_multicast_group_members(client, group_name).await;
        assert_eq!(initial_members.len(), 1);
        assert_eq!(initial_members[0].state, "Joined");

        // Test 2 external IP allocation/deallocation cycles
        for cycle in 1..=2 {
            // Allocate ephemeral external IP
            let ephemeral_ip_url = format!(
                "/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}"
            );
            NexusRequest::new(
                RequestBuilder::new(client, Method::POST, &ephemeral_ip_url)
                    .body(Some(&EphemeralIpCreate {
                        pool_selector: PoolSelector::Auto {
                            ip_version: Some(IpVersion::V4),
                        },
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
                "Cycle {cycle}: Multicast member should persist during external IP allocation"
            );
            assert_eq!(
                members_with_ip[0].state, "Joined",
                "Cycle {cycle}: Member should remain Joined"
            );

            // Verify external IP is attached
            let external_ips_with_ip = fetch_instance_external_ips(
                client,
                instance_name,
                project_name,
            )
            .await;
            assert!(
                !external_ips_with_ip.is_empty(),
                "Cycle {cycle}: Instance should have external IP"
            );

            // Deallocate ephemeral external IP
            let external_ip_detach_url = format!(
                "/v1/instances/{instance_name}/external-ips/ephemeral?project={project_name}"
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
                "Cycle {cycle}: Multicast member should persist after external IP removal"
            );
            assert_eq!(
                members_without_ip[0].state, "Joined",
                "Cycle {cycle}: Member should remain Joined after IP removal"
            );

            // Verify ephemeral external IP is removed (SNAT IP may still be present)
            let external_ips_without_ip = fetch_instance_external_ips(
                client,
                instance_name,
                project_name,
            )
            .await;
            assert!(
                external_ips_without_ip.len() <= 1,
                "Cycle {cycle}: Instance should have at most SNAT IP remaining"
            );
        }

        // Cleanup Case 2
        cleanup_instances(cptestctx, client, project_name, &[instance_name])
            .await;
        wait_for_group_deleted(cptestctx, group_name).await;
    }

    // -------------------------------------------------------------------------
    // Case 3: External IP at instance creation
    // -------------------------------------------------------------------------
    // Verify instances can be created with both external IP and multicast group
    // simultaneously. Instance creation with both features works without
    // conflicts during initial setup.
    {
        let instance_name = "creation-with-ip-instance";
        let group_name = "creation-with-ip-group";

        // Create instance with external IP specified at creation
        let external_ip_param = ExternalIpCreate::Ephemeral {
            pool_selector: PoolSelector::Auto {
                ip_version: Some(IpVersion::V4),
            },
        };
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
            network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
        instance_simulate(nexus, &instance_uuid).await;
        instance_wait_for_state(client, instance_uuid, InstanceState::Running)
            .await;

        wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

        // Verify external IP was allocated at creation
        let external_ips_after_start =
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            !external_ips_after_start.is_empty(),
            "Instance should have external IP from creation"
        );

        // Add to multicast group via instance-centric API
        multicast_group_attach(
            cptestctx,
            project_name,
            instance_name,
            group_name,
        )
        .await;
        wait_for_group_active(client, group_name).await;

        // Verify both features work together - wait for member to reach Joined state
        wait_for_member_state(
            cptestctx,
            group_name,
            instance_id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;

        let members = list_multicast_group_members(client, group_name).await;
        assert_eq!(members.len(), 1, "Should have multicast member");

        let external_ips_final =
            fetch_instance_external_ips(client, instance_name, project_name)
                .await;
        assert!(
            !external_ips_final.is_empty(),
            "Instance should retain external IP"
        );

        // Cleanup Case 3
        cleanup_instances(cptestctx, client, project_name, &[instance_name])
            .await;
        wait_for_group_deleted(cptestctx, group_name).await;
    }
}

/// Verify instances can have both floating IPs and multicast group membership.
///
/// Floating IP attachment works for multicast group members, multicast state persists
/// through floating IP operations, and no conflicts occur between floating IP and
/// multicast DPD configuration.
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
    let (_, (v4_pool, _v6_pool), _) = ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client), // For floating IPs
        create_multicast_ip_pool_with_range(
            client,
            "floating-ip-mcast-pool",
            (224, 200, 0, 1),
            (224, 200, 0, 255),
        ),
    )
    .await;

    // Create floating IP (specify pool to avoid ambiguity with dual-stack default pools)
    let floating_ip = create_floating_ip(
        client,
        floating_ip_name,
        project_name,
        None,
        Some(v4_pool.identity.name.as_str()),
    )
    .await;

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
        network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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

    // Ensure multicast test prerequisites (inventory + DPD) are ready
    ensure_multicast_test_ready(cptestctx).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    // Wait for multicast member to reach "Joined" state
    wait_for_member_state(
        cptestctx,
        group_name,
        instance_id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify member count
    let members = list_multicast_group_members(client, group_name).await;
    assert_eq!(members.len(), 1, "Should have one multicast member");

    // Verify that inventory-based mapping correctly mapped sled → switch port
    verify_inventory_based_port_mapping(cptestctx, &instance_uuid)
        .await
        .expect("Port mapping verification should succeed");

    // Attach floating IP to the same instance
    let attach_url = format!(
        "/v1/floating-ips/{floating_ip_name}/attach?project={project_name}"
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
        "/v1/floating-ips/{floating_ip_name}/detach?project={project_name}"
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
    let fip_delete_url =
        format!("/v1/floating-ips/{floating_ip_name}?project={project_name}");
    object_delete(client, &fip_delete_url).await;

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}
