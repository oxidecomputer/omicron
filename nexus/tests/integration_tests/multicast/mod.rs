// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast integration tests.

use std::net::IpAddr;
use std::time::Duration;

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};

use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    link_ip_pool, object_create, object_delete,
};
use nexus_types::external_api::params::{
    InstanceCreate, InstanceNetworkInterfaceAttachment, IpPoolCreate,
    MulticastGroupCreate,
};
use nexus_types::external_api::shared::{IpRange, Ipv4Range};
use nexus_types::external_api::views::{
    IpPool, IpPoolRange, IpVersion, MulticastGroup, MulticastGroupMember,
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceAutoRestartPolicy, InstanceCpuCount, InstanceState, NameOrId,
};
use omicron_test_utils::dev::poll::{self, CondCheckError, wait_for_condition};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};

use crate::integration_tests::instances as instance_helpers;

// Shared type alias for all multicast integration tests
pub(crate) type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

mod api;
mod authorization;
mod enablement;
mod failures;
mod groups;
mod instances;
mod networking_integration;

// Timeout constants for test operations
const POLL_INTERVAL: Duration = Duration::from_millis(80);
const MULTICAST_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Helpers for building multicast API URLs.
/// Multicast groups are fleet-scoped, so no project parameter is needed.
pub(crate) fn mcast_groups_url() -> String {
    "/v1/multicast-groups".to_string()
}

pub(crate) fn mcast_group_url(group_name: &str) -> String {
    format!("/v1/multicast-groups/{group_name}")
}

/// Multicast group members are identified by UUID, so no project parameter is needed for listing.
pub(crate) fn mcast_group_members_url(group_name: &str) -> String {
    format!("/v1/multicast-groups/{group_name}/members")
}

/// Build URL for adding a member to a multicast group.
///
/// The `?project=` parameter is required when using instance names (for scoping)
/// but must NOT be provided when using instance UUIDs (causes 400 Bad Request).
pub(crate) fn mcast_group_member_add_url(
    group_name: &str,
    instance: &NameOrId,
    project_name: &str,
) -> String {
    let base_url = mcast_group_members_url(group_name);
    match instance {
        NameOrId::Name(_) => format!("{base_url}?project={project_name}"),
        NameOrId::Id(_) => base_url,
    }
}

/// Utility functions for running multiple async operations in parallel.
pub(crate) mod ops {
    use std::future::Future;

    /// Execute a collection of independent async operations in parallel
    pub(crate) async fn join_all<T>(
        ops: impl IntoIterator<Item = impl Future<Output = T>>,
    ) -> Vec<T> {
        futures::future::join_all(ops).await
    }

    /// Execute 2 independent async operations in parallel
    pub(crate) async fn join2<T1, T2>(
        op1: impl Future<Output = T1>,
        op2: impl Future<Output = T2>,
    ) -> (T1, T2) {
        tokio::join!(op1, op2)
    }

    /// Execute 3 independent async operations in parallel
    pub(crate) async fn join3<T1, T2, T3>(
        op1: impl Future<Output = T1>,
        op2: impl Future<Output = T2>,
        op3: impl Future<Output = T3>,
    ) -> (T1, T2, T3) {
        tokio::join!(op1, op2, op3)
    }

    /// Execute 4 independent async operations in parallel
    pub(crate) async fn join4<T1, T2, T3, T4>(
        op1: impl Future<Output = T1>,
        op2: impl Future<Output = T2>,
        op3: impl Future<Output = T3>,
        op4: impl Future<Output = T4>,
    ) -> (T1, T2, T3, T4) {
        tokio::join!(op1, op2, op3, op4)
    }
}

/// Test helper for creating multicast groups in batch operations.
#[derive(Clone)]
pub(crate) struct MulticastGroupForTest {
    pub name: &'static str,
    pub multicast_ip: IpAddr,
    pub description: Option<String>,
}

/// Create a multicast IP pool for ASM (Any-Source Multicast) testing.
pub(crate) async fn create_multicast_ip_pool(
    client: &ClientTestContext,
    pool_name: &str,
) -> IpPool {
    create_multicast_ip_pool_with_range(
        client,
        pool_name,
        (224, 0, 1, 10),  // Default ASM range start
        (224, 0, 1, 255), // Default ASM range end
    )
    .await
}

/// Create a multicast IP pool with custom ASM range.
pub(crate) async fn create_multicast_ip_pool_with_range(
    client: &ClientTestContext,
    pool_name: &str,
    range_start: (u8, u8, u8, u8),
    range_end: (u8, u8, u8, u8),
) -> IpPool {
    let pool_params = IpPoolCreate::new_multicast(
        IdentityMetadataCreateParams {
            name: pool_name.parse().unwrap(),
            description: "Multicast IP pool for testing".to_string(),
        },
        IpVersion::V4,
    );

    let pool: IpPool =
        object_create(client, "/v1/system/ip-pools", &pool_params).await;

    // Add IPv4 ASM range
    let asm_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(
                range_start.0,
                range_start.1,
                range_start.2,
                range_start.3,
            ),
            std::net::Ipv4Addr::new(
                range_end.0,
                range_end.1,
                range_end.2,
                range_end.3,
            ),
        )
        .unwrap(),
    );
    let range_url = format!("/v1/system/ip-pools/{pool_name}/ranges/add");
    object_create::<_, IpPoolRange>(client, &range_url, &asm_range).await;

    // Link the pool to the silo so it can be found by multicast group creation
    link_ip_pool(client, pool_name, &DEFAULT_SILO.id(), false).await;

    pool
}

/// Waits for the multicast group reconciler to complete.
///
/// This wraps wait_background_task with the correct task name.
pub(crate) async fn wait_for_multicast_reconciler(
    lockstep_client: &ClientTestContext,
) -> nexus_lockstep_client::types::BackgroundTask {
    nexus_test_utils::background::wait_background_task(
        lockstep_client,
        "multicast_group_reconciler",
    )
    .await
}

/// Get a single multicast group by name.
pub(crate) async fn get_multicast_group(
    client: &ClientTestContext,
    group_name: &str,
) -> MulticastGroup {
    let url = mcast_group_url(group_name);
    NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute_and_parse_unwrap::<MulticastGroup>()
        .await
}

/// List all multicast groups.
pub(crate) async fn list_multicast_groups(
    client: &ClientTestContext,
) -> Vec<MulticastGroup> {
    let url = mcast_groups_url();
    nexus_test_utils::resource_helpers::objects_list_page_authz::<
        MulticastGroup,
    >(client, &url)
    .await
    .items
}

/// List members of a multicast group.
pub(crate) async fn list_multicast_group_members(
    client: &ClientTestContext,
    group_name: &str,
) -> Vec<MulticastGroupMember> {
    let url = mcast_group_members_url(group_name);
    nexus_test_utils::resource_helpers::objects_list_page_authz::<
        MulticastGroupMember,
    >(client, &url)
    .await
    .items
}

/// Wait for a multicast group to transition to the specified state.
pub(crate) async fn wait_for_group_state(
    client: &ClientTestContext,
    group_name: &str,
    expected_state: &str,
) -> MulticastGroup {
    match wait_for_condition(
        || async {
            let group = get_multicast_group(client, group_name).await;
            if group.state == expected_state {
                Ok(group)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(group) => group,
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "group {group_name} did not reach state '{expected_state}' within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to reach state '{expected_state}': {err:?}",
            );
        }
    }
}

/// Convenience function to wait for a group to become "Active".
pub(crate) async fn wait_for_group_active(
    client: &ClientTestContext,
    group_name: &str,
) -> MulticastGroup {
    wait_for_group_state(client, group_name, "Active").await
}

/// Wait for a specific member to reach the expected state
/// (e.g., "Joined", "Joining", "Leaving", "Left").
pub(crate) async fn wait_for_member_state(
    client: &ClientTestContext,
    group_name: &str,
    instance_id: uuid::Uuid,
    expected_state: &str,
) -> MulticastGroupMember {
    match wait_for_condition(
        || async {
            let members =
                list_multicast_group_members(client, group_name).await;

            // If we're looking for "Joined" state, we need to ensure the member exists first
            // and then wait for the reconciler to process it
            if expected_state == "Joined" {
                if let Some(member) = members.iter().find(|m| m.instance_id == instance_id) {
                    match member.state.as_str() {
                        "Joined" => Ok(member.clone()),
                        "Joining" => {
                            // Member exists and is in transition - wait a bit more
                            Err(CondCheckError::NotYet)
                        }
                        "Left" => {
                            // Member in Left state, reconciler needs to process instance start - wait more
                            Err(CondCheckError::NotYet)
                        }
                        other_state => {
                            Err(CondCheckError::Failed(format!(
                                "Member {} in group {} has unexpected state '{}', expected 'Left', 'Joining' or 'Joined'",
                                instance_id, group_name, other_state
                            )))
                        }
                    }
                } else {
                    // Member doesn't exist yet - wait for it to be created
                    Err(CondCheckError::NotYet)
                }
            } else {
                // For other states, just look for exact match
                if let Some(member) = members.iter().find(|m| m.instance_id == instance_id) {
                    if member.state == expected_state {
                        Ok(member.clone())
                    } else {
                        Err(CondCheckError::NotYet)
                    }
                } else {
                    Err(CondCheckError::NotYet)
                }
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(member) => member,
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "member {instance_id} in group {group_name} did not reach state '{expected_state}' within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for member {instance_id} in group {group_name} to reach state '{expected_state}': {err:?}",
            );
        }
    }
}

/// Wait for a multicast group to have a specific number of members.
pub(crate) async fn wait_for_member_count(
    client: &ClientTestContext,
    group_name: &str,
    expected_count: usize,
) {
    match wait_for_condition(
        || async {
            let members =
                list_multicast_group_members(client, group_name).await;
            if members.len() == expected_count {
                Ok(())
            } else {
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {}
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "group {group_name} did not reach member count {expected_count} within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to reach member count {expected_count}: {err:?}",
            );
        }
    }
}

/// Wait for a multicast group to be deleted (returns 404).
pub(crate) async fn wait_for_group_deleted(
    client: &ClientTestContext,
    group_name: &str,
) {
    match wait_for_condition(
        || async {
            let group_url = mcast_group_url(group_name);
            match NexusRequest::object_get(client, &group_url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
            {
                Ok(response) => {
                    if response.status == StatusCode::NOT_FOUND {
                        Ok(())
                    } else {
                        Err(CondCheckError::<()>::NotYet)
                    }
                }
                Err(_) => Ok(()), // Assume 404 or similar error means deleted
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {}
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!("group {group_name} was not deleted within {elapsed:?}",);
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to be deleted: {err:?}",
            );
        }
    }
}

/// Create an instance with multicast groups.
pub(crate) async fn instance_for_multicast_groups(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_name: &str,
    start: bool,
    multicast_group_names: &[&str],
) -> Instance {
    let client = &cptestctx.external_client;
    let multicast_groups: Vec<NameOrId> = multicast_group_names
        .iter()
        .map(|name| NameOrId::Name(name.parse().unwrap()))
        .collect();

    let url = format!("/v1/instances?project={project_name}");

    object_create(
        client,
        &url,
        &InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!(
                    "Instance for multicast group testing: {}",
                    instance_name
                ),
            },
            ncpus: InstanceCpuCount::try_from(1).unwrap(),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: instance_name.parse::<Hostname>().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces: InstanceNetworkInterfaceAttachment::Default,
            external_ips: vec![],
            multicast_groups,
            disks: vec![],
            boot_disk: None,
            cpu_platform: None,
            start,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
    )
    .await
}

/// Create multiple instances with multicast groups attached at creation time.
pub(crate) async fn create_instances_with_multicast_groups(
    client: &ClientTestContext,
    project_name: &str,
    instance_specs: &[(&str, &[&str])], // (instance_name, group_names)
    start: bool,
) -> Vec<Instance> {
    let create_futures =
        instance_specs.iter().map(|(instance_name, group_names)| {
            let url = format!("/v1/instances?project={project_name}");
            let multicast_groups: Vec<NameOrId> = group_names
                .iter()
                .map(|name| NameOrId::Name(name.parse().unwrap()))
                .collect();

            async move {
                object_create::<_, Instance>(
                    client,
                    &url,
                    &InstanceCreate {
                        identity: IdentityMetadataCreateParams {
                            name: instance_name.parse().unwrap(),
                            description: format!(
                                "multicast test instance {instance_name}"
                            ),
                        },
                        ncpus: InstanceCpuCount::try_from(2).unwrap(),
                        memory: ByteCount::from_gibibytes_u32(4),
                        hostname: instance_name.parse().unwrap(),
                        user_data: b"#cloud-config".to_vec(),
                        ssh_public_keys: None,
                        network_interfaces:
                            InstanceNetworkInterfaceAttachment::Default,
                        external_ips: vec![],
                        disks: vec![],
                        boot_disk: None,
                        cpu_platform: None,
                        start,
                        auto_restart_policy: Some(
                            InstanceAutoRestartPolicy::Never,
                        ),
                        anti_affinity_groups: Vec::new(),
                        multicast_groups,
                    },
                )
                .await
            }
        });

    ops::join_all(create_futures).await
}

/// Attach an instance to a multicast group.
pub(crate) async fn multicast_group_attach(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
) {
    let url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );

    // Use PUT to attach instance to multicast group
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &url)
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to attach instance to multicast group");
}

/// Create multiple multicast groups from the same pool.
pub(crate) async fn create_multicast_groups(
    client: &ClientTestContext,
    pool: &IpPool,
    group_specs: &[MulticastGroupForTest],
) -> Vec<MulticastGroup> {
    let create_futures = group_specs.iter().map(|spec| {
        let group_url = mcast_groups_url();
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: spec.name.parse().unwrap(),
                description: spec
                    .description
                    .clone()
                    .unwrap_or_else(|| format!("Test group {}", spec.name)),
            },
            multicast_ip: Some(spec.multicast_ip),
            source_ips: None,
            pool: Some(NameOrId::Name(pool.identity.name.clone())),
            mvlan: None,
        };

        async move {
            object_create::<_, MulticastGroup>(client, &group_url, &params)
                .await
        }
    });

    ops::join_all(create_futures).await
}

/// Wait for multiple groups to become "Active".
pub(crate) async fn wait_for_groups_active(
    client: &ClientTestContext,
    group_names: &[&str],
) -> Vec<MulticastGroup> {
    let wait_futures =
        group_names.iter().map(|name| wait_for_group_active(client, name));

    ops::join_all(wait_futures).await
}

/// Clean up multiple groups.
pub(crate) async fn cleanup_multicast_groups(
    client: &ClientTestContext,
    group_names: &[&str],
) {
    let delete_futures = group_names.iter().map(|name| {
        let url = mcast_group_url(name);
        async move { object_delete(client, &url).await }
    });

    ops::join_all(delete_futures).await;
}

/// Clean up multiple instances, handling various states properly.
///
/// This function handles the complete instance lifecycle for cleanup:
/// 1. Starting instances: simulate -> wait for Running -> stop -> delete
/// 2. Running instances: stop -> delete
/// 3. Stopped instances: delete
/// 4. Other states: attempt delete as-is
///
/// Required for concurrent tests where instances may be in Starting state
/// and need simulation to complete state transitions.
pub(crate) async fn cleanup_instances(
    cptestctx: &ControlPlaneTestContext,
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
) {
    let mut instances_to_stop = Vec::new();
    let mut instances_to_wait_then_stop = Vec::new();

    // Categorize instances by their current state
    for name in instance_names {
        let url = format!("/v1/instances/{name}?project={project_name}");
        let instance: Instance = NexusRequest::object_get(client, &url)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap()
            .await;

        match instance.runtime.run_state {
            InstanceState::Running => instances_to_stop.push(*name),
            InstanceState::Starting => {
                instances_to_wait_then_stop.push(*name);
                eprintln!(
                    "Instance {} in Starting state - will wait for Running then stop",
                    name
                );
            }
            InstanceState::Stopped => {
                eprintln!("Instance {} already stopped", name)
            }
            _ => eprintln!(
                "Instance {} in state {:?} - will attempt to delete as-is",
                name, instance.runtime.run_state
            ),
        }
    }

    // Handle Starting instances: simulate -> wait -> add to stop list
    if !instances_to_wait_then_stop.is_empty() {
        eprintln!(
            "Waiting for {} instances to finish starting...",
            instances_to_wait_then_stop.len()
        );

        for name in &instances_to_wait_then_stop {
            let url = format!("/v1/instances/{name}?project={project_name}");
            let instance: Instance = NexusRequest::object_get(client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute_and_parse_unwrap()
                .await;
            let instance_id =
                InstanceUuid::from_untyped_uuid(instance.identity.id);

            // Simulate and wait for Running state
            instance_helpers::instance_simulate(
                &cptestctx.server.server_context().nexus,
                &instance_id,
            )
            .await;
            instance_helpers::instance_wait_for_state_as(
                client,
                AuthnMode::PrivilegedUser,
                instance_id,
                InstanceState::Running,
            )
            .await;

            eprintln!("Instance {} reached Running state", name);
        }

        instances_to_stop.extend(&instances_to_wait_then_stop);
    }

    // Stop all running instances
    if !instances_to_stop.is_empty() {
        stop_instances(cptestctx, client, project_name, &instances_to_stop)
            .await;
    }

    // Delete all instances in parallel (now that we fixed the double-delete bug)
    let delete_futures = instance_names.iter().map(|name| {
        let url = format!("/v1/instances/{name}?project={project_name}");
        async move { object_delete(client, &url).await }
    });
    ops::join_all(delete_futures).await;
}

/// Stop multiple instances using the exact same pattern as groups.rs.
pub(crate) async fn stop_instances(
    cptestctx: &ControlPlaneTestContext,
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
) {
    let nexus = &cptestctx.server.server_context().nexus;

    // First, fetch all instances in parallel
    let fetch_futures = instance_names.iter().map(|name| {
        let url = format!("/v1/instances/{name}?project={project_name}");
        async move {
            let instance_result = NexusRequest::object_get(client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await;

            match instance_result {
                Ok(response) => match response.parsed_body::<Instance>() {
                    Ok(instance) => {
                        let id = InstanceUuid::from_untyped_uuid(
                            instance.identity.id,
                        );
                        Some((*name, instance, id))
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to parse instance {name}: {e:?}"
                        );
                        None
                    }
                },
                Err(e) => {
                    eprintln!(
                        "Warning: Instance {name} not found or error: {e:?}"
                    );
                    None
                }
            }
        }
    });

    let instances: Vec<_> =
        ops::join_all(fetch_futures).await.into_iter().flatten().collect();

    // Stop all running instances in parallel
    let stop_futures =
        instances.iter().filter_map(|(name, instance, instance_id)| {
            if instance.runtime.run_state == InstanceState::Running {
                Some(async move {
                    let stop_url = format!(
                        "/v1/instances/{name}/stop?project={project_name}"
                    );
                    let stop_result = NexusRequest::new(
                        RequestBuilder::new(client, Method::POST, &stop_url)
                            .body(None as Option<&serde_json::Value>)
                            .expect_status(Some(StatusCode::ACCEPTED)),
                    )
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute()
                    .await;

                    match stop_result {
                        Ok(_) => {
                            instance_helpers::instance_simulate(
                                nexus,
                                instance_id,
                            )
                            .await;
                            instance_helpers::instance_wait_for_state(
                                client,
                                *instance_id,
                                InstanceState::Stopped,
                            )
                            .await;
                        }
                        Err(e) => {
                            eprintln!(
                                "Warning: Failed to stop instance {name}: {e:?}"
                            );
                        }
                    }
                })
            } else {
                eprintln!(
                    "Skipping instance {name} - current state: {:?}",
                    instance.runtime.run_state
                );
                None
            }
        });

    ops::join_all(stop_futures).await;
}

/// Attach multiple instances to a multicast group in parallel.
pub(crate) async fn multicast_group_attach_bulk(
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
    group_name: &str,
) {
    let attach_futures = instance_names.iter().map(|instance_name| {
        multicast_group_attach(client, project_name, instance_name, group_name)
    });
    ops::join_all(attach_futures).await;
}

/// Detach multiple instances from a multicast group in parallel.
pub(crate) async fn multicast_group_detach_bulk(
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
    group_name: &str,
) {
    let detach_futures = instance_names.iter().map(|instance_name| {
        multicast_group_detach(client, project_name, instance_name, group_name)
    });
    ops::join_all(detach_futures).await;
}

/// Detach an instance from a multicast group.
pub(crate) async fn multicast_group_detach(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
) {
    let url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );

    // Use DELETE to detach instance from multicast group
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Failed to detach instance from multicast group");
}
