// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast groups with other networking features.
//!
//! Tests multicast + networking feature interactions:
//!
//! - External IPs: Instances with ephemeral/floating IPs can join multicast groups
//! - Floating IP attach/detach: Multicast membership unaffected by IP changes
//! - Sled-agent M2P/forwarding propagation on member join and group deletion
//! - Per-VMM multicast subscriptions via sled-agent

use std::net::IpAddr;

use http::{Method, StatusCode};
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::create_floating_ip;
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_project, object_create, object_delete,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::floating_ip::{
    AddressAllocator, FloatingIp, FloatingIpAttach,
};
use nexus_types::external_api::instance::{
    EphemeralIpCreate, ExternalIpCreate, InstanceCreate,
    InstanceNetworkInterfaceAttachment,
};
use nexus_types::external_api::ip_pool::{IpVersion, PoolSelector};
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, Instance, InstanceCpuCount,
    NameOrId,
};
use omicron_nexus::TestInterfaces;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use super::*;
use crate::integration_tests::instances::fetch_instance_external_ips;

/// Consolidated test for external IP scenarios with multicast group membership.
///
/// This test covers three scenarios with shared setup:
/// - Basic external IP attach/detach with multicast
/// - Lifecycle with 1-2 attach/detach cycles
/// - External IP at instance creation
#[nexus_test]
async fn test_multicast_external_ip_scenarios(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
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

    // Case: Basic external IP attach/detach with multicast
    //
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
        instance_wait_for_running_with_simulation(cptestctx, instance_uuid)
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

        // Cleanup
        cleanup_instances(cptestctx, client, project_name, &[instance_name])
            .await;
        wait_for_group_deleted(cptestctx, group_name).await;
    }

    // Case: Lifecycle with 1-2 attach/detach cycles
    //
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
        instance_wait_for_running_with_simulation(cptestctx, instance_uuid)
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

        // Cleanup
        cleanup_instances(cptestctx, client, project_name, &[instance_name])
            .await;
        wait_for_group_deleted(cptestctx, group_name).await;
    }

    // Case: External IP at instance creation
    //
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
        instance_wait_for_running_with_simulation(cptestctx, instance_uuid)
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

        // Cleanup
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
        AddressAllocator::Auto {
            pool_selector: PoolSelector::Explicit {
                pool: v4_pool.identity.name.clone().into(),
            },
        },
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

    let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
    wait_for_instance_sled_assignment(cptestctx, &instance_uuid).await;
    instance_wait_for_running_with_simulation(cptestctx, instance_uuid).await;

    // Ensure multicast test prerequisites (inventory + DPD) are ready
    ensure_multicast_test_ready(cptestctx).await;

    // Add instance to multicast group via instance-centric API
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    // Group activation is reconciler-driven; explicitly drive it to avoid flakes.
    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let group = get_multicast_group(client, group_name).await;
            if group.state == "Active" {
                Ok(())
            } else {
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!("group {group_name} did not reach Active state in time: {e:?}")
    });

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
        kind: nexus_types::external_api::floating_ip::FloatingIpParentKind::Instance,
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
    wait_for_condition(
        || async {
            let external_ips = fetch_instance_external_ips(
                client,
                instance_name,
                project_name,
            )
            .await;
            let has_floating_ip =
                external_ips.iter().any(|ip| ip.ip() == floating_ip.ip);
            if has_floating_ip {
                Ok(())
            } else {
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &POLL_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "instance did not show floating IP {} as attached within {POLL_TIMEOUT:?}: {e:?}",
            floating_ip.ip
        )
    });

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
    wait_for_condition(
        || async {
            let external_ips = fetch_instance_external_ips(
                client,
                instance_name,
                project_name,
            )
            .await;
            let still_has_floating_ip =
                external_ips.iter().any(|ip| ip.ip() == floating_ip.ip);
            if !still_has_floating_ip {
                Ok(())
            } else {
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &POLL_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "instance still showed floating IP {} as attached after {POLL_TIMEOUT:?}: {e:?}",
            floating_ip.ip
        )
    });

    // Cleanup floating IP
    let fip_delete_url =
        format!("/v1/floating-ips/{floating_ip_name}?project={project_name}");
    object_delete(client, &fip_delete_url).await;

    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Verify that when an instance joins a multicast group, the reconciler
/// pushes M2P mappings, forwarding entries, and per-VMM subscriptions
/// to the sim sled-agent. Also verify cleanup on instance deletion.
#[nexus_test]
async fn test_multicast_sled_agent_m2p_and_subscriptions(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
) {
    let client = &cptestctx.external_client;
    let project_name = "sled-agent-mcast-project";
    let group_name = "sled-agent-mcast-group";
    let instance_name = "sled-agent-mcast-instance";

    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "sled-agent-mcast-pool",
            (224, 150, 0, 1),
            (224, 150, 0, 255),
        ),
    )
    .await;

    ensure_multicast_test_ready(cptestctx).await;

    // Create and start an instance.
    let instance_params = InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: instance_name.parse().unwrap(),
            description: "Instance for sled-agent multicast test".to_string(),
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
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Attach instance to a multicast group.
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    // Wait for the member to reach "Joined" state (reconciler processes it).
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Look up the underlay multicast IPv6 address for verification.
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let group_view = get_multicast_group(client, group_name).await;
    let multicast_ip = group_view.multicast_ip;

    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, multicast_ip)
        .await
        .expect("Should look up multicast group by IP");

    let underlay_group_id = external_group
        .underlay_group_id
        .expect("Active group should have underlay_group_id");

    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("Should fetch underlay multicast group");

    let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
        IpAddr::V6(v6) => v6,
        other => panic!("Expected IPv6 underlay address, got {other}"),
    };

    // Verify M2P mapping on the sim sled-agent.
    let sled_agent = cptestctx.first_sled_agent();
    {
        let m2p = sled_agent.m2p_mappings.lock().unwrap();
        assert!(
            m2p.contains(&(multicast_ip, underlay_ipv6)),
            "Sled-agent should have M2P mapping ({multicast_ip}, \
             {underlay_ipv6}), got: {m2p:?}"
        );
    }

    // Verify forwarding entries on the sim sled-agent.
    // With a single sled, the forwarding entry exists but has no next hops
    // (no other sleds to forward to).
    {
        let fwd = sled_agent.mcast_fwd.lock().unwrap();
        assert!(
            fwd.contains_key(&underlay_ipv6),
            "Sled-agent should have forwarding entry for {underlay_ipv6}, \
             got: {fwd:?}"
        );
        let next_hops = &fwd[&underlay_ipv6];
        assert!(
            next_hops.is_empty(),
            "Single-sled setup should have empty next_hops, got: {next_hops:?}"
        );
    }

    // Verify per-VMM multicast subscription on the sim sled-agent.
    {
        let info = nexus
            .active_instance_info(&instance_id, None)
            .await
            .unwrap()
            .expect("Running instance should have active info");

        let groups = sled_agent.multicast_groups.lock().unwrap();
        let vmm_groups = groups
            .get(&info.propolis_id)
            .expect("Sled-agent should have multicast groups for propolis");

        assert!(
            vmm_groups.iter().any(|m| m.group_ip == multicast_ip),
            "VMM should be subscribed to multicast group {multicast_ip}, \
             got: {vmm_groups:?}"
        );
    }

    // Stop the instance. The member transitions "Joined" -> "Left".
    let stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should stop instance");

    wait_for_instance_stopped(cptestctx, client, instance_id, instance_name)
        .await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    // Per-VMM subscription cleanup after stop is not asserted here.
    // In production, destroying the VMM tears down the OPTE port, which
    // implicitly removes multicast subscriptions. The reconciler's
    // unsubscribe path correctly skips when the propolis_id is gone
    // (matching production semantics where the port no longer exists).
    //
    // V2P follows the same pattern: sled-agent cleanup is keyed by
    // network identity, not VMM identity.

    // M2P and forwarding should be cleared since there are no "Joined"
    // members remaining.
    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let m2p = sled_agent.m2p_mappings.lock().unwrap();
            if !m2p.contains(&(multicast_ip, underlay_ipv6)) {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("M2P should be cleared when no Joined members remain");

    // Forwarding should also be cleared when no "Joined" members remain.
    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let fwd = sled_agent.mcast_fwd.lock().unwrap();
            if !fwd.contains_key(&underlay_ipv6) {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("Forwarding should be cleared when no Joined members remain");

    // Delete the instance, which should trigger group deletion.
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;

    // Verify M2P and forwarding are cleared.
    {
        let m2p = sled_agent.m2p_mappings.lock().unwrap();
        assert!(
            !m2p.contains(&(multicast_ip, underlay_ipv6)),
            "M2P mapping should be cleared after group deletion, got: {m2p:?}"
        );
    }
    {
        let fwd = sled_agent.mcast_fwd.lock().unwrap();
        assert!(
            !fwd.contains_key(&underlay_ipv6),
            "Forwarding entry should be cleared after group deletion, \
             got: {fwd:?}"
        );
    }
}

/// Verify M2P and forwarding entries propagate to all sleds, not just the
/// hosting sled. Analogous to `test_instance_v2p_mappings` which verifies
/// V2P mappings on all sleds.
///
/// Also verifies cleanup: after instance deletion, M2P and forwarding
/// entries are removed from every sled.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_multi_sled_m2p_propagation(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "multi-sled-mcast-project";
    let group_name = "multi-sled-mcast-group";
    let instance_name = "multi-sled-mcast-instance";

    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "multi-sled-mcast-pool",
            (224, 160, 0, 1),
            (224, 160, 0, 255),
        ),
    )
    .await;

    ensure_multicast_test_ready(cptestctx).await;

    // Collect all sled agents (2 total: 1 default + 1 extra).
    // We use extra_sled_agents = 1 (not 2) because the gateway sim only
    // provides SP data for the two well-known sled UUIDs. A 3rd sled with
    // a random UUID would have no SP entry, causing inventory readiness
    // to time out. Two sleds is sufficient to verify cross-sled propagation.
    let all_sled_agents: Vec<_> =
        cptestctx.sled_agents.iter().map(|sa| sa.sled_agent()).collect();
    assert_eq!(all_sled_agents.len(), 2, "expected 2 sled agents");

    // Create and start an instance.
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true,
        &[],
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Attach to a multicast group.
    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Look up the underlay IPv6 address for verification.
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let group_view = get_multicast_group(client, group_name).await;
    let multicast_ip = group_view.multicast_ip;

    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, multicast_ip)
        .await
        .expect("Should look up multicast group by IP");

    let underlay_group_id = external_group
        .underlay_group_id
        .expect("Active group should have underlay_group_id");

    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("Should fetch underlay multicast group");

    let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
        IpAddr::V6(v6) => v6,
        other => panic!("Expected IPv6 underlay address, got {other}"),
    };

    // Look up the hosting sled for subscription verification.
    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Running instance should have active info");

    let hosting_sled_id = info.sled_id;

    // M2P and forwarding are pushed to all sleds (like V2P). Any
    // instance on any sled may send to a multicast group; without the
    // M2P mapping OPTE's overlay layer silently drops the packet.
    // Forwarding entries let sender sleds replicate to member sleds.
    for (i, sled_agent) in cptestctx.sled_agents.iter().enumerate() {
        let agent = sled_agent.sled_agent();

        // Wait for M2P on every sled. The reconciler may need an
        // additional pass after the member reaches "Joined": during
        // reconcile_member_states, propagate_m2p_and_forwarding may
        // see member_sleds=0 (member still "Joining" in DB), so the
        // actual push happens in reconcile_active_groups or the next
        // full pass.
        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let m2p = agent.m2p_mappings.lock().unwrap();
                if m2p.contains(&(multicast_ip, underlay_ipv6)) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("Sled {i} should have M2P mapping within timeout: {e:?}")
        });

        // Verify forwarding on every sled. With a single member on
        // one sled, the hosting sled's forwarding has no next hops
        // (local delivery via subscription). Non-hosting sleds list
        // the hosting sled as a next hop so senders can reach it.
        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let fwd = agent.mcast_fwd.lock().unwrap();
                if fwd.contains_key(&underlay_ipv6) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Sled {i} should have forwarding entry within timeout: {e:?}"
            )
        });

        let fwd = agent.mcast_fwd.lock().unwrap();
        let next_hops = &fwd[&underlay_ipv6];
        if sled_agent.sled_agent_id() == hosting_sled_id {
            // Hosting sled: no next hops (only local member, OPTE
            // delivers locally via subscription).
            assert!(
                next_hops.is_empty(),
                "Hosting sled forwarding should have empty next_hops, \
                 got: {next_hops:?}"
            );
        } else {
            // Non-hosting sled: next hop is the hosting sled so
            // senders on this sled can reach the member.
            assert_eq!(
                next_hops.len(),
                1,
                "Non-hosting sled {i} should have 1 next_hop (the hosting \
                 sled), got: {next_hops:?}"
            );
        }
    }

    // Verify per-VMM subscription on the hosting sled only.
    // Subscriptions are member-sled-only (not all sleds).
    let hosting_agent = cptestctx
        .sled_agents
        .iter()
        .find(|sa| sa.sled_agent_id() == hosting_sled_id)
        .unwrap()
        .sled_agent();

    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let groups = hosting_agent.multicast_groups.lock().unwrap();
            match groups.get(&info.propolis_id) {
                Some(vmm_groups)
                    if vmm_groups
                        .iter()
                        .any(|m| m.group_ip == multicast_ip) =>
                {
                    Ok(())
                }
                _ => Err(CondCheckError::NotYet::<()>),
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "VMM should be subscribed to {multicast_ip} within timeout: {e:?}"
        )
    });

    // Delete the instance, which triggers group deletion.
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;

    // Verify cleanup on every sled: M2P and forwarding removed.
    for (i, sled_agent) in all_sled_agents.iter().enumerate() {
        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let m2p = sled_agent.m2p_mappings.lock().unwrap();
                let fwd = sled_agent.mcast_fwd.lock().unwrap();
                if !m2p.contains(&(multicast_ip, underlay_ipv6))
                    && !fwd.contains_key(&underlay_ipv6)
                {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Sled {i} M2P/forwarding not cleaned up within timeout: {e:?}"
            )
        });
    }
}

/// Verify cross-sled forwarding when members exist on both sleds.
///
/// With one member on sled A and another on sled B, each sled's forwarding
/// entry should list the other sled as its sole next hop (self-exclusion).
/// This exercises the `.filter(|(id, _)| *id != sled_id)` logic in
/// `converge_forwarding`.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_cross_sled_forwarding(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());
    let project_name = "bidir-fwd-project";
    let group_name = "bidir-fwd-group";
    let instance_a_name = "bidir-instance-a";
    let instance_b_name = "bidir-instance-b";

    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "bidir-fwd-pool",
            (224, 170, 0, 1),
            (224, 170, 0, 255),
        ),
    )
    .await;

    ensure_multicast_test_ready(cptestctx).await;

    let sled_a_id = cptestctx.sled_agents[0].sled_agent_id();
    let sled_b_id = cptestctx.sled_agents[1].sled_agent_id();

    // Pin instance A to sled A by making sled B non-provisionable.
    {
        let (authz_sled, ..) = LookupPath::new(&opctx, datastore)
            .sled_id(sled_b_id)
            .lookup_for(nexus_auth::authz::Action::Modify)
            .await
            .expect("lookup sled B");
        datastore
            .sled_set_provision_policy(
                &opctx,
                &authz_sled,
                nexus_types::external_api::sled::SledProvisionPolicy::NonProvisionable,
            )
            .await
            .expect("set sled B non-provisionable");
    }

    let instance_a = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_a_name,
        true,
        &[],
    )
    .await;
    let instance_a_id = InstanceUuid::from_untyped_uuid(instance_a.identity.id);
    instance_wait_for_running_with_simulation(cptestctx, instance_a_id).await;

    // Verify instance A landed on sled A.
    let info_a = nexus
        .active_instance_info(&instance_a_id, None)
        .await
        .unwrap()
        .expect("instance A should be running");
    assert_eq!(info_a.sled_id, sled_a_id, "instance A should be on sled A");

    // Swap provisionability: sled A non-provisionable, sled B provisionable.
    {
        let (authz_sled_a, ..) = LookupPath::new(&opctx, datastore)
            .sled_id(sled_a_id)
            .lookup_for(nexus_auth::authz::Action::Modify)
            .await
            .expect("lookup sled A");
        let (authz_sled_b, ..) = LookupPath::new(&opctx, datastore)
            .sled_id(sled_b_id)
            .lookup_for(nexus_auth::authz::Action::Modify)
            .await
            .expect("lookup sled B");
        datastore
            .sled_set_provision_policy(
                &opctx,
                &authz_sled_a,
                nexus_types::external_api::sled::SledProvisionPolicy::NonProvisionable,
            )
            .await
            .expect("set sled A non-provisionable");
        datastore
            .sled_set_provision_policy(
                &opctx,
                &authz_sled_b,
                nexus_types::external_api::sled::SledProvisionPolicy::Provisionable,
            )
            .await
            .expect("set sled B provisionable");
    }

    let instance_b = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_b_name,
        true,
        &[],
    )
    .await;

    let instance_b_id = InstanceUuid::from_untyped_uuid(instance_b.identity.id);
    instance_wait_for_running_with_simulation(cptestctx, instance_b_id).await;

    // Verify instance B landed on sled B.
    let info_b = nexus
        .active_instance_info(&instance_b_id, None)
        .await
        .unwrap()
        .expect("instance B should be running");

    assert_eq!(info_b.sled_id, sled_b_id, "instance B should be on sled B");

    // Both instances join the same multicast group.
    multicast_group_attach(
        cptestctx,
        project_name,
        instance_a_name,
        group_name,
    )
    .await;

    multicast_group_attach(
        cptestctx,
        project_name,
        instance_b_name,
        group_name,
    )
    .await;

    wait_for_group_active(client, group_name).await;

    // Wait for both members to reach "Joined".
    for instance in [&instance_a, &instance_b] {
        wait_for_member_state(
            cptestctx,
            group_name,
            instance.identity.id,
            nexus_db_model::MulticastGroupMemberState::Joined,
        )
        .await;
    }

    // Resolve underlay IPv6 for forwarding assertions.
    let group_view = get_multicast_group(client, group_name).await;
    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, group_view.multicast_ip)
        .await
        .expect("lookup group by IP");

    let underlay_group = datastore
        .underlay_multicast_group_fetch(
            &opctx,
            external_group
                .underlay_group_id
                .expect("active group should have underlay_group_id"),
        )
        .await
        .expect("fetch underlay group");

    let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
        IpAddr::V6(v6) => v6,
        other => panic!("Expected IPv6 underlay address, got {other}"),
    };

    // Wait for forwarding entries on both sleds, then verify each sled's
    // forwarding lists exactly the other sled (not itself).
    let agent_a = cptestctx.sled_agents[0].sled_agent();
    let agent_b = cptestctx.sled_agents[1].sled_agent();

    for (label, agent) in [("sled A", &agent_a), ("sled B", &agent_b)] {
        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let fwd = agent.mcast_fwd.lock().unwrap();
                match fwd.get(&underlay_ipv6) {
                    Some(hops) if hops.len() == 1 => Ok(()),
                    _ => Err(CondCheckError::NotYet::<()>),
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("{label} should have exactly 1 forwarding next_hop: {e:?}")
        });
    }

    // Cleanup.
    cleanup_instances(
        cptestctx,
        client,
        project_name,
        &[instance_a_name, instance_b_name],
    )
    .await;
    wait_for_group_deleted(cptestctx, group_name).await;
}

/// Verify multicast state is re-established after simulated cold start.
/// Analogous to `test_instance_start_creates_networking_state` which tests
/// V2P re-establishment after forcibly clearing sled-agent state.
///
/// Steps: a) create instance, b) join multicast, c) stop instance,
/// d) forcibly clear all sim sled-agent multicast state, e) restart
/// instance, f) verify M2P, forwarding, and per-VMM subscriptions are
/// re-established.
#[nexus_test(extra_sled_agents = 1)]
async fn test_multicast_cold_start_reestablishment(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let project_name = "cold-start-mcast-project";
    let group_name = "cold-start-mcast-group";
    let instance_name = "cold-start-mcast-instance";

    ops::join3(
        create_project(client, project_name),
        create_default_ip_pools(client),
        create_multicast_ip_pool_with_range(
            client,
            "cold-start-mcast-pool",
            (224, 170, 0, 1),
            (224, 170, 0, 255),
        ),
    )
    .await;

    ensure_multicast_test_ready(cptestctx).await;

    let all_sled_agents: Vec<_> =
        cptestctx.sled_agents.iter().map(|sa| sa.sled_agent()).collect();

    // Create and start an instance, join a multicast group.
    let instance = instance_for_multicast_groups(
        cptestctx,
        project_name,
        instance_name,
        true,
        &[],
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    instance_wait_for_running_with_simulation(cptestctx, instance_id).await;
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    multicast_group_attach(cptestctx, project_name, instance_name, group_name)
        .await;
    wait_for_group_active(client, group_name).await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Look up the underlay IPv6.
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let group_view = get_multicast_group(client, group_name).await;
    let multicast_ip = group_view.multicast_ip;

    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, multicast_ip)
        .await
        .expect("Should look up multicast group by IP");

    let underlay_group_id = external_group
        .underlay_group_id
        .expect("Active group should have underlay_group_id");

    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("Should fetch underlay multicast group");

    let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
        IpAddr::V6(v6) => v6,
        other => panic!("Expected IPv6 underlay address, got {other}"),
    };

    // M2P and forwarding are pushed to all sleds. Verify at least the
    // hosting sled has M2P before we clear state.
    let pre_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Running instance should have active info");

    let pre_hosting_agent = cptestctx
        .sled_agents
        .iter()
        .find(|sa| sa.sled_agent_id() == pre_info.sled_id)
        .unwrap()
        .sled_agent();

    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let m2p = pre_hosting_agent.m2p_mappings.lock().unwrap();
            if m2p.contains(&(multicast_ip, underlay_ipv6)) {
                Ok(())
            } else {
                Err(CondCheckError::NotYet::<()>)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("Hosting sled M2P should exist before cold start simulation");

    // Stop the instance.
    let stop_url =
        format!("/v1/instances/{instance_name}/stop?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &stop_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should stop instance");

    wait_for_instance_stopped(cptestctx, client, instance_id, instance_name)
        .await;

    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;

    // Forcibly clear all sim sled-agent multicast state, simulating a cold
    // start where sled-agents lose in-memory state.
    for sled_agent in &all_sled_agents {
        sled_agent.m2p_mappings.lock().unwrap().clear();
        sled_agent.mcast_fwd.lock().unwrap().clear();
        sled_agent.multicast_groups.lock().unwrap().clear();
    }

    // Restart the instance.
    let start_url =
        format!("/v1/instances/{instance_name}/start?project={project_name}");
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should start instance");

    // Use `try_instance_simulate` here instead of `instance_wait_for_running_with_simulation`
    // because the old VMM may still be draining from the sim collection after
    // the stop. `instance_simulate` would panic if it pokes a VMM that was just
    // removed; `try_instance_simulate` handles that gracefully.
    wait_for_condition(
        || async {
            let _ =
                instance_helpers::try_instance_simulate(nexus, &instance_id)
                    .await;

            let url = format!("/v1/instances/{instance_id}");
            let instance: Instance = NexusRequest::object_get(client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .map_err(|_| CondCheckError::<()>::NotYet)?
                .parsed_body()
                .map_err(|_| CondCheckError::<()>::NotYet)?;

            if instance.runtime.run_state == InstanceState::Running {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .expect("Instance should reach Running after restart");

    // Wait for the reconciler to re-establish multicast state.
    wait_for_member_state(
        cptestctx,
        group_name,
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Verify M2P and forwarding re-established on all sleds.
    for (i, sled_agent) in all_sled_agents.iter().enumerate() {
        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let m2p = sled_agent.m2p_mappings.lock().unwrap();
                if m2p.contains(&(multicast_ip, underlay_ipv6)) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("Sled {i} M2P not re-established within timeout: {e:?}")
        });

        wait_for_condition_with_reconciler(
            &cptestctx.lockstep_client,
            || async {
                let fwd = sled_agent.mcast_fwd.lock().unwrap();
                if fwd.contains_key(&underlay_ipv6) {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet::<()>)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Sled {i} forwarding not re-established within timeout: {e:?}"
            )
        });
    }

    // Verify per-VMM subscription on the hosting sled (new propolis_id
    // since restart creates a new VMM).
    let post_info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("Restarted instance should have active info");

    let post_hosting_agent = cptestctx
        .sled_agents
        .iter()
        .find(|sa| sa.sled_agent_id() == post_info.sled_id)
        .unwrap()
        .sled_agent();

    wait_for_condition_with_reconciler(
        &cptestctx.lockstep_client,
        || async {
            let groups = post_hosting_agent.multicast_groups.lock().unwrap();
            match groups.get(&post_info.propolis_id) {
                Some(vmm_groups)
                    if vmm_groups
                        .iter()
                        .any(|m| m.group_ip == multicast_ip) =>
                {
                    Ok(())
                }
                _ => Err(CondCheckError::NotYet::<()>),
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "New VMM should be subscribed to {multicast_ip} after restart: \
             {e:?}"
        )
    });

    // Cleanup.
    cleanup_instances(cptestctx, client, project_name, &[instance_name]).await;
    wait_for_group_deleted(cptestctx, group_name).await;
}
