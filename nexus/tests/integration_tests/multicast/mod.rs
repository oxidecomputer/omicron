// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast integration tests and shared helper methods.
//!
//! This module provides common test infrastructure:
//!
//! - URL builders: `mcast_group_url`, `mcast_group_members_url`, etc.
//! - IP pool setup: `create_multicast_ip_pool`, `create_multicast_ip_pool_with_range`
//! - Reconciler control: `wait_for_multicast_reconciler`, `activate_multicast_reconciler`
//! - State waiters: `wait_for_group_active`, `wait_for_member_state`, etc.
//! - DPD verification: `verify_inventory_based_port_mapping`, `wait_for_group_deleted_from_dpd`
//! - Instance helpers: `instance_for_multicast_groups`, `cleanup_instances`
//! - Attach/detach: `multicast_group_attach`, `multicast_group_detach`

use std::future::Future;
use std::net::IpAddr;
use std::time::Duration;

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use slog::{debug, info, warn};
use uuid::Uuid;

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    link_ip_pool, object_create, object_delete, object_get,
};
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::instance::{
    InstanceCreate, InstanceNetworkInterfaceAttachment,
};
use nexus_types::external_api::ip_pool::{
    IpPool, IpPoolCreate, IpPoolRange, IpRange, IpVersion, Ipv4Range,
};
use nexus_types::external_api::multicast::{
    InstanceMulticastGroupJoin, MulticastGroup, MulticastGroupIdentifier,
    MulticastGroupJoinSpec, MulticastGroupMember,
};
use nexus_types::identity::{Asset, Resource};
use nexus_types::internal_api::params::InstanceMigrateRequest;
use omicron_common::api::external::{
    ByteCount, DataPageParams, Hostname, IdentityMetadataCreateParams,
    Instance, InstanceCpuCount, InstanceState,
};
use omicron_nexus::TestInterfaces;
use omicron_test_utils::dev::poll::{self, CondCheckError, wait_for_condition};
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, SledUuid,
};

use crate::integration_tests::instances as instance_helpers;
use sled_agent_client::TestInterfaces as SledAgentTestInterfaces;

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
mod pool_selection;

// Timeout constants for test operations
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const MULTICAST_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Generic helper for PUT upsert requests that return 201 Created.
///
/// Useful for idempotent create-or-update APIs like multicast group join.
pub(crate) async fn put_upsert<InputType, OutputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
) -> OutputType
where
    InputType: serde::Serialize,
    OutputType: serde::de::DeserializeOwned,
{
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, path)
            .body(Some(input))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to make PUT request to {path}: {e}"))
    .parsed_body()
    .unwrap()
}

/// Build URL for listing multicast groups.
pub(crate) fn mcast_groups_url() -> String {
    "/v1/multicast-groups".to_string()
}

/// Build URL for a specific multicast group by name.
pub(crate) fn mcast_group_url(group_name: &str) -> String {
    format!("/v1/multicast-groups/{group_name}")
}

/// Build URL for listing members of a multicast group.
pub(crate) fn mcast_group_members_url(group_name: &str) -> String {
    format!("/v1/multicast-groups/{group_name}/members")
}

/// Create a multicast IP pool for ASM (Any-Source Multicast) testing.
///
/// Uses range 224.2.0.0 - 224.2.255.255 which avoids all reserved addresses:
/// - 224.0.0.0/24 (link-local)
/// - 224.0.1.1 (NTP), 224.0.1.39/40 (Cisco Auto-RP), 224.0.1.129-132 (PTP)
pub(crate) async fn create_multicast_ip_pool(
    client: &ClientTestContext,
    pool_name: &str,
) -> IpPool {
    create_multicast_ip_pool_with_range(
        client,
        pool_name,
        (224, 2, 0, 0),     // Default ASM range start
        (224, 2, 255, 255), // Default ASM range end
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

/// Create an IPv6 multicast IP pool with a global scope range (ff0e::/16).
pub(crate) async fn create_multicast_ip_pool_v6(
    client: &ClientTestContext,
    pool_name: &str,
) -> IpPool {
    use nexus_types::external_api::ip_pool::Ipv6Range;
    use std::net::Ipv6Addr;

    let pool_params = IpPoolCreate::new_multicast(
        IdentityMetadataCreateParams {
            name: pool_name.parse().unwrap(),
            description: "IPv6 multicast IP pool for testing".to_string(),
        },
        IpVersion::V6,
    );

    let pool: IpPool =
        object_create(client, "/v1/system/ip-pools", &pool_params).await;

    // Add IPv6 global scope multicast range (ff0e::/16)
    // Small range to avoid generate_series performance issues with IPv6
    let ipv6_range = IpRange::V6(
        Ipv6Range::new(
            Ipv6Addr::new(0xff0e, 0, 0, 0, 0, 0, 0, 1),
            Ipv6Addr::new(0xff0e, 0, 0, 0, 0, 0, 0, 0xff),
        )
        .unwrap(),
    );
    let range_url = format!("/v1/system/ip-pools/{pool_name}/ranges/add");
    object_create::<_, IpPoolRange>(client, &range_url, &ipv6_range).await;

    // Link the pool to the silo so it can be found by multicast group creation
    link_ip_pool(client, pool_name, &DEFAULT_SILO.id(), false).await;

    pool
}

/// The reconciler can take longer than the default 10s timeout under
/// parallel test load, especially after the CRDB graceful-shutdown
/// change (eb8ae2f8f). 30s matches other heavy background task timeouts.
const RECONCILER_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Waits for the multicast group reconciler to complete.
///
/// This wraps wait_background_task with the correct task name.
pub(crate) async fn wait_for_multicast_reconciler(
    lockstep_client: &ClientTestContext,
) -> nexus_lockstep_client::types::BackgroundTask {
    nexus_test_utils::background::wait_background_task(
        lockstep_client,
        "multicast_reconciler",
    )
    .await
}

/// Activates the multicast reconciler and waits for it to complete.
///
/// Use this when you need to explicitly activate the reconciler (e.g., after
/// restarting DPD) rather than waiting for an already-activated run.
pub(crate) async fn activate_multicast_reconciler(
    lockstep_client: &ClientTestContext,
) -> nexus_lockstep_client::types::BackgroundTask {
    nexus_test_utils::background::activate_background_task_with_timeout(
        lockstep_client,
        "multicast_reconciler",
        RECONCILER_ACTIVATION_TIMEOUT,
    )
    .await
}

/// Activate the multicast reconciler once, then poll `condition` until it
/// holds (or `timeout` elapses).
///
/// For tests that expect convergence in a single reconciler pass. We
/// poll after the activation to absorb read-after-write visibility lag
/// (DB commits, sled-agent state propagation), not to wait for further
/// reconciler iterations. If `condition` only holds after multiple
/// passes, the test author must orchestrate explicitly: activate per
/// step and assert intermediate state between steps.
pub(crate) async fn activate_then_wait_for_condition<F, Fut, T, E>(
    lockstep_client: &ClientTestContext,
    condition: F,
    poll_interval: &Duration,
    timeout: &Duration,
) -> Result<T, poll::Error<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, CondCheckError<E>>>,
{
    activate_multicast_reconciler(lockstep_client).await;
    wait_for_condition(condition, poll_interval, timeout).await
}

/// Ensure inventory collection has completed with SP data for all sleds.
///
/// This function verifies that inventory has SP data for EVERY in-service sled,
/// not just that inventory completed.
///
/// This is required for multicast member operations which map `sled_id` to
/// `sp_slot` to switch ports via inventory.
pub(crate) async fn ensure_inventory_ready(
    cptestctx: &ControlPlaneTestContext,
) {
    let log = &cptestctx.logctx.log;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    info!(log, "waiting for inventory with SP data for all sleds");

    // Wait for inventory to have SP data for all in-service sleds
    match wait_for_condition(
        || async {
            let opctx = OpContext::for_tests(log.clone(), datastore.clone());

            // Get all in-service sleds
            let sleds = match datastore
                .sled_list_all_batched(&opctx, SledFilter::InService)
                .await
            {
                Ok(sleds) => sleds,
                Err(e) => {
                    warn!(log, "failed to list sleds: {e}");
                    return Err(CondCheckError::<String>::NotYet);
                }
            };

            if sleds.is_empty() {
                warn!(log, "no in-service sleds found yet");
                return Err(CondCheckError::<String>::NotYet);
            }

            // Get latest inventory
            let inventory =
                match datastore.inventory_get_latest_collection(&opctx).await {
                    Ok(Some(inv)) => inv,
                    Ok(None) => {
                        debug!(log, "no inventory collection yet");
                        return Err(CondCheckError::<String>::NotYet);
                    }
                    Err(e) => {
                        warn!(log, "failed to get inventory: {e}");
                        return Err(CondCheckError::<String>::NotYet);
                    }
                };

            // Verify inventory has SP data for each sled
            let mut missing_sleds = Vec::new();
            for sled in &sleds {
                let has_sp = inventory.sps.iter().any(|(bb, _)| {
                    bb.serial_number == sled.serial_number()
                        && bb.part_number == sled.part_number()
                });

                if !has_sp {
                    missing_sleds.push(sled.serial_number().to_string());
                }
            }

            if missing_sleds.is_empty() {
                info!(
                    log,
                    "inventory has SP data for all {} sleds",
                    sleds.len()
                );
                Ok(())
            } else {
                debug!(
                    log,
                    "inventory missing SP data for {} sleds: {:?}",
                    missing_sleds.len(),
                    missing_sleds
                );
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {
            info!(log, "inventory ready with SP data for all sleds");
        }
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "inventory did not get SP data for all sleds within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!("failed waiting for inventory: {err}");
        }
    }
}

/// Ensure multicast test prerequisites are ready.
///
/// This combines inventory collection (for sled → switch port mapping) and
/// DPD readiness (for switch operations) into a single call. Use this at the
/// beginning of multicast tests that will add instances to groups.
pub(crate) async fn ensure_multicast_test_ready(
    cptestctx: &ControlPlaneTestContext,
) {
    ensure_inventory_ready(cptestctx).await;
    ensure_dpd_ready(cptestctx).await;
}

/// Ensure DPD (switch infrastructure) is ready and responsive.
///
/// This ensures that switch zones are up and DPD APIs are responding before
/// running tests that depend on dataplane operations. Helps prevent flaky tests
/// where the reconciler tries to contact DPD before switch zones are up.
///
/// Uses a simple ping by listing groups - any successful response means DPD is ready.
pub(crate) async fn ensure_dpd_ready(cptestctx: &ControlPlaneTestContext) {
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    let log = &cptestctx.logctx.log;

    info!(log, "waiting for DPD/switch infrastructure to be ready");

    match wait_for_condition(
        || async {
            // Try to list multicast groups - any successful response means DPD is ready
            // limit=None, page_token=None - we don't care about the results, just that DPD responds
            match dpd_client.multicast_groups_list(None, None).await {
                Ok(_) => {
                    debug!(log, "DPD is responsive");
                    Ok(())
                }
                Err(e) => {
                    debug!(
                        log,
                        "DPD not ready yet";
                        "error" => %e
                    );
                    Err(CondCheckError::<String>::NotYet)
                }
            }
        },
        &POLL_INTERVAL,
        &POLL_TIMEOUT,
    )
    .await
    {
        Ok(_) => {
            info!(log, "DPD/switch infrastructure is ready");
        }
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "DPD/switch infrastructure did not become ready within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!("Failed waiting for DPD to be ready: {err}");
        }
    }
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
    expected_state: nexus_db_model::MulticastGroupState,
) -> MulticastGroup {
    let expected_state_as_str = expected_state.to_string();
    match wait_for_condition(
        || async {
            let group = get_multicast_group(client, group_name).await;
            if group.state == expected_state_as_str {
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
                "group {group_name} did not reach state '{expected_state_as_str}' within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to reach state '{expected_state_as_str}': {err:?}",
            );
        }
    }
}

/// Convenience function to wait for a group to become "Active".
pub(crate) async fn wait_for_group_active(
    client: &ClientTestContext,
    group_name: &str,
) -> MulticastGroup {
    wait_for_group_state(
        client,
        group_name,
        nexus_db_model::MulticastGroupState::Active,
    )
    .await
}

/// Wait for a multicast group member to reach the expected state.
///
/// Ensures inventory and DPD are ready, drives one reconciler activation,
/// then asserts the member is observable in `expected_state`. If the state
/// does not match after the pass, fails loudly rather than retrying via
/// reactivation.
///
/// We poll briefly after the pass to absorb DB read-after-write lag,
/// not to wait for further reconciler iterations.
///
/// Tests that genuinely need multi-step convergence (e.g., recovery from
/// an injected external failure) must orchestrate explicitly: drive each
/// step with `activate_multicast_reconciler` and assert the intermediate
/// state between steps.
pub(crate) async fn wait_for_member_state(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
    instance_id: Uuid,
    expected_state: nexus_db_model::MulticastGroupMemberState,
) -> MulticastGroupMember {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let expected_state_as_str = expected_state.to_string();

    // "Joined" requires the dataplane: the reconciler resolves
    // sled→port and programs DPD before that transition. Pre-populate
    // DDM peers and wait for DPD readiness before polling for it.
    //
    // "Joining" and "Left" converge from DB-only transitions, so
    // don't gate those as failure-mode tests rely on being able to wait
    // on them with working DPD stopped.
    if expected_state == nexus_db_model::MulticastGroupMemberState::Joined {
        nexus_test_utils::multicast::populate_ddm_peers(cptestctx).await;
        ensure_dpd_ready(cptestctx).await;
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
        wait_for_instance_sled_assignment(cptestctx, &instance_uuid).await;
    }

    // Drive one converging pass. This explicit activate guarantees a fresh
    // pass runs after this point regardless of whether the API call that
    // triggered the test already activated the reconciler.
    activate_multicast_reconciler(lockstep_client).await;

    // Verify the post-pass state. Treat read-after-write visibility lag as
    // `NotYet`, but treat any *other* observed state as a permanent failure.
    let check_member = || async {
        let members = list_multicast_group_members(client, group_name).await;
        match members.iter().find(|m| m.instance_id == instance_id) {
            Some(member) if member.state == expected_state_as_str => {
                Ok(member.clone())
            }
            Some(member) => Err(CondCheckError::Failed(format!(
                "member {instance_id} in group {group_name} reached state \
                 '{}' after one reconciler pass, expected '{expected_state_as_str}'",
                member.state
            ))),
            None => Err(CondCheckError::NotYet),
        }
    };

    match wait_for_condition(
        check_member,
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(member) => member,
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "member {instance_id} in group {group_name} did not appear within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "reconciler did not converge member {instance_id} in group \
                 {group_name} to '{expected_state_as_str}': {err}",
            );
        }
    }
}

/// Wait for a batch of multicast group members to reach their respective
/// expected states after a single reconciler pass.
///
/// Like [`wait_for_member_state`] but checks multiple members after
/// one shared reconciler pass. Panics if any member ends up in an
/// unexpected state.
pub(crate) async fn wait_for_members_state(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
    expected: &[(Uuid, nexus_db_model::MulticastGroupMemberState)],
) -> Vec<MulticastGroupMember> {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;

    let joined_instances: Vec<InstanceUuid> = expected
        .iter()
        .filter(|(_, state)| {
            *state == nexus_db_model::MulticastGroupMemberState::Joined
        })
        .map(|(id, _)| InstanceUuid::from_untyped_uuid(*id))
        .collect();

    if !joined_instances.is_empty() {
        nexus_test_utils::multicast::populate_ddm_peers(cptestctx).await;
        ensure_dpd_ready(cptestctx).await;
        for instance_uuid in &joined_instances {
            wait_for_instance_sled_assignment(cptestctx, instance_uuid).await;
        }
    }

    activate_multicast_reconciler(lockstep_client).await;

    let check = || async {
        let members = list_multicast_group_members(client, group_name).await;
        let mut resolved = Vec::with_capacity(expected.len());
        for (instance_id, expected_state) in expected {
            let expected_str = expected_state.to_string();
            match members.iter().find(|m| m.instance_id == *instance_id) {
                Some(member) if member.state == expected_str => {
                    resolved.push(member.clone());
                }
                Some(member) => {
                    return Err(CondCheckError::Failed(format!(
                        "member {instance_id} in group {group_name} reached \
                         state '{}' after one reconciler pass, expected \
                         '{expected_str}'",
                        member.state
                    )));
                }
                None => return Err(CondCheckError::NotYet),
            }
        }
        Ok(resolved)
    };

    match wait_for_condition(
        check,
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(members) => members,
        Err(poll::Error::TimedOut(elapsed)) => panic!(
            "members in group {group_name} did not all appear within \
             {elapsed:?} (expected {expected:?})",
        ),
        Err(poll::Error::PermanentError(err)) => panic!(
            "reconciler did not converge members in group {group_name} \
             (expected {expected:?}): {err}",
        ),
    }
}

/// Wait for an instance to have a sled_id assigned.
///
/// This is a stricter check than `instance_wait_for_vmm_registration` - it ensures
/// that not only does the VMM exist and is not in "Creating" state, but also that
/// the VMM has been assigned to a specific sled. This is critical for multicast
/// member join operations which need the sled_id to program switch ports.
pub(crate) async fn wait_for_instance_sled_assignment(
    cptestctx: &ControlPlaneTestContext,
    instance_id: &InstanceUuid,
) {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());

    info!(
        log,
        "waiting for instance to have sled_id assigned";
        "instance_id" => %instance_id,
    );

    match wait_for_condition(
        || async {
            // Use the same batch fetch method the reconciler uses
            let instance_vmm_data = datastore
                .instance_and_vmm_batch_fetch(&opctx, &[*instance_id])
                .await
                .map_err(|e| {
                    CondCheckError::Failed(format!(
                        "Failed to fetch instance data: {e}"
                    ))
                })?;

            let instance_uuid = instance_id.into_untyped_uuid();
            if let Some((instance, vmm_opt)) =
                instance_vmm_data.get(&instance_uuid)
            {
                if let Some(vmm) = vmm_opt {
                    debug!(
                        log,
                        "instance VMM found, checking sled assignment";
                        "instance_id" => %instance_id,
                        "vmm_id" => %vmm.id,
                        "vmm_state" => ?vmm.state,
                        "sled_id" => %vmm.sled_id
                    );

                    // VMM exists and has a sled_id - we're good
                    Ok(())
                } else {
                    debug!(
                        log,
                        "instance exists but has no VMM yet";
                        "instance_id" => %instance_id,
                        "instance_state" => ?instance.nexus_state.state()
                    );
                    Err(CondCheckError::<String>::NotYet)
                }
            } else {
                warn!(
                    log,
                    "instance not found in batch fetch";
                    "instance_id" => %instance_id
                );
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {
            info!(
                log,
                "instance has sled_id assigned";
                "instance_id" => %instance_id
            );
        }
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "instance {instance_id} did not get sled_id assigned within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for instance {instance_id} sled assignment: {err}"
            );
        }
    }
}

/// Wait for an instance to reach Running state, driving simulation on each poll.
///
/// More robust than passively waiting, as it actively drives instance
/// simulation while polling for the Running state.
///
/// Only use for Running transitions. For Stopped state, use
/// `wait_for_instance_stopped` which handles the VMM removal race condition.
pub(crate) async fn instance_wait_for_running_with_simulation(
    cptestctx: &ControlPlaneTestContext,
    instance_id: InstanceUuid,
) -> Instance {
    let expected_state = InstanceState::Running;
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let url = format!("/v1/instances/{instance_id}");

    match wait_for_condition(
        || async {
            instance_helpers::instance_simulate(nexus, &instance_id).await;

            let response = NexusRequest::object_get(client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await
                .map_err(|e| {
                    CondCheckError::<String>::Failed(format!(
                        "request failed: {e}"
                    ))
                })?;

            let instance: Instance = response.parsed_body().map_err(|e| {
                CondCheckError::<String>::Failed(format!("parse failed: {e}"))
            })?;

            if instance.runtime.run_state == expected_state {
                Ok(instance)
            } else {
                Err(CondCheckError::<String>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(instance) => instance,
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "instance {instance_id} did not reach {expected_state:?} within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for instance {instance_id} to reach {expected_state:?}: {err}"
            );
        }
    }
}
/// Wait for an instance to reach the Stopped state, poking the simulated
/// sled-agent on each poll iteration to advance the state machine.
///
/// This is more robust than calling `instance_simulate` once because
/// concurrent operations may require multiple pokes to complete. Uses
/// `try_vmm_finish_transition` to handle the race where the VMM can
/// disappear between checking for it and poking.
pub(crate) async fn wait_for_instance_stopped(
    cptestctx: &ControlPlaneTestContext,
    client: &ClientTestContext,
    instance_id: InstanceUuid,
    instance_name: &str,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let log = &cptestctx.logctx.log;

    info!(
        log,
        "waiting for instance to stop (with sled-agent pokes)";
        "instance_id" => %instance_id,
        "instance_name" => instance_name,
    );

    let url = format!("/v1/instances/{instance_id}");
    match wait_for_condition(
        || async {
            let instance: Instance = NexusRequest::object_get(client, &url)
                .authn_as(AuthnMode::PrivilegedUser)
                .execute()
                .await?
                .parsed_body()?;

            if instance.runtime.run_state == InstanceState::Stopped {
                Ok(())
            } else {
                debug!(
                    log,
                    "instance not yet stopped, poking sled-agent";
                    "instance_id" => %instance_id,
                    "current_state" => ?instance.runtime.run_state,
                );

                // Try to poke the sled-agent. The VMM may not exist (if no
                // active VMM) or may disappear between the check and the poke
                // (race condition). Either case is fine since we're polling.
                if let Ok(Some(sled_info)) =
                    nexus.active_instance_info(&instance_id, None).await
                {
                    let _ = sled_info
                        .sled_client
                        .try_vmm_finish_transition(sled_info.propolis_id)
                        .await;
                }

                Err(CondCheckError::<anyhow::Error>::NotYet)
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {
            info!(
                log,
                "instance stopped";
                "instance_id" => %instance_id,
            );
        }
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "instance {instance_name} ({instance_id}) did not stop \
                 within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for instance {instance_name} to stop: {err}"
            );
        }
    }
}

/// Verify that inventory-based sled-to-switch-port mapping is correct.
///
/// This validates the entire flow:
/// instance → sled → inventory → sp_slot → rear{N} → DPD underlay member
pub(crate) async fn verify_inventory_based_port_mapping(
    cptestctx: &ControlPlaneTestContext,
    instance_uuid: &InstanceUuid,
) -> Result<(), String> {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Get sled_id for the running instance
    let sled_id = nexus
        .active_instance_info(instance_uuid, None)
        .await
        .map_err(|e| format!("active_instance_info failed: {e}"))?
        .ok_or_else(|| "instance not on a sled".to_string())?
        .sled_id;

    // Get the multicast member for this instance to find its external_group_id
    let members = datastore
        .multicast_group_members_list_by_instance(
            &opctx,
            *instance_uuid,
            &DataPageParams::max_page(),
        )
        .await
        .map_err(|e| format!("list members failed: {e}"))?;

    let member = members
        .first()
        .ok_or_else(|| "no multicast membership found".to_string())?;

    let external_group_id = member.external_group_id;

    // Fetch the external multicast group to get underlay_group_id
    let external_group = datastore
        .multicast_group_fetch(
            &opctx,
            MulticastGroupUuid::from_untyped_uuid(external_group_id),
        )
        .await
        .map_err(|e| format!("fetch external group failed: {e}"))?;

    let underlay_group_id = external_group
        .underlay_group_id
        .ok_or_else(|| "external group has no underlay_group_id".to_string())?;

    // Fetch the underlay group to get its multicast IP
    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .map_err(|e| format!("fetch underlay group failed: {e}"))?;

    let underlay_multicast_ip = underlay_group.multicast_ip.ip();

    // Fetch latest inventory collection
    let inventory = datastore
        .inventory_get_latest_collection(&opctx)
        .await
        .map_err(|e| format!("fetch inventory failed: {e}"))?
        .ok_or_else(|| "no inventory collection".to_string())?;

    // Get the sled record to find its baseboard info
    let sleds = datastore
        .sled_list_all_batched(&opctx, SledFilter::InService)
        .await
        .map_err(|e| format!("list sleds failed: {e}"))?;
    let sled = sleds
        .into_iter()
        .find(|s| s.id() == sled_id)
        .ok_or_else(|| "sled not found".to_string())?;

    // Find SP for this sled using baseboard matching (serial + part number)
    let sp = inventory
        .sps
        .iter()
        .find(|(bb, _)| {
            bb.serial_number == sled.serial_number()
                && bb.part_number == sled.part_number()
        })
        .or_else(|| {
            // Fallback to serial-only match if exact match not found
            inventory
                .sps
                .iter()
                .find(|(bb, _)| bb.serial_number == sled.serial_number())
        })
        .map(|(_, sp)| sp)
        .ok_or_else(|| "SP not found for sled".to_string())?;

    let expected_rear_port = sp.sp_slot;

    // Fetch DPD underlay group configuration using the underlay multicast IP
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);
    let underlay_group_response = dpd_client
        .multicast_group_get(&underlay_multicast_ip)
        .await
        .map_err(|e| format!("DPD query failed: {e}"))?
        .into_inner();

    // Extract underlay members from the response
    let members = match underlay_group_response {
        dpd_client::types::MulticastGroupResponse::Underlay {
            members, ..
        } => members,
        dpd_client::types::MulticastGroupResponse::External { .. } => {
            return Err("Expected Underlay group, got External".to_string());
        }
    };

    // Construct the expected `PortId` for comparison
    let expected_port_id = dpd_client::types::PortId::Rear(
        dpd_client::types::Rear::try_from(format!("rear{expected_rear_port}"))
            .map_err(|e| format!("invalid rear port: {e}"))?,
    );

    // Check if DPD has an underlay member with the expected rear port
    let has_expected_member = members.iter().any(|m| {
        matches!(m.direction, dpd_client::types::Direction::Underlay)
            && m.port_id == expected_port_id
    });

    if has_expected_member {
        Ok(())
    } else {
        Err(format!("DPD does not have member on rear{expected_rear_port}"))
    }
}

/// Wait for a multicast group to have a specific number of members.
///
/// Note: For expected_count=0 (last member removed), use `wait_for_group_deleted`
/// instead since the implicit deletion deletes the group when empty.
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

/// Wait for a multicast group to be fully deleted (returns 404).
///
/// Drives one reconciler activation, which runs `process_deleting_group_inner`
/// end-to-end (M2P/forwarding clear, DPD removal, underlay delete, member
/// delete, group row delete) for groups in "Deleting". Polling around the
/// API check is only for read-after-write visibility.
pub(crate) async fn wait_for_group_deleted(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;

    activate_multicast_reconciler(lockstep_client).await;

    let check = || async {
        let group_url = mcast_group_url(group_name);
        let response = NexusRequest::new(
            RequestBuilder::new(client, Method::GET, &group_url)
                .expect_status(Some(StatusCode::NOT_FOUND)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await;
        match response {
            Ok(_) => Ok(()),
            Err(_) => Err(CondCheckError::<()>::NotYet),
        }
    };

    match wait_for_condition(
        check,
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {}
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "group {group_name} was not deleted within {elapsed:?} after \
                 one reconciler pass",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to be deleted: {err:?}",
            );
        }
    }
}

/// Wait for a multicast group to be removed from DPD (dataplane).
///
/// Drives one reconciler activation, which runs `process_deleting_group_inner`
/// (DPD `remove_groups` by tag) for groups in "Deleting". Polling around the
/// DPD GET is only for read-after-write visibility.
pub(crate) async fn wait_for_group_deleted_from_dpd(
    cptestctx: &ControlPlaneTestContext,
    multicast_ip: std::net::IpAddr,
) {
    let lockstep_client = &cptestctx.lockstep_client;
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);

    activate_multicast_reconciler(lockstep_client).await;

    let check = || async {
        match dpd_client.multicast_group_get(&multicast_ip).await {
            Ok(_) => Err(CondCheckError::<()>::NotYet),
            Err(_) => Ok(()),
        }
    };

    match wait_for_condition(
        check,
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(_) => {}
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "group with IP {multicast_ip} was not deleted from DPD within \
                 {elapsed:?} after one reconciler pass",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group with IP {multicast_ip} to be deleted from DPD: {err:?}",
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
    // Ensure inventory and DPD are ready before creating instances with multicast groups
    // Inventory is needed for sled → switch port mapping, DPD for switch operations
    if !multicast_group_names.is_empty() {
        ensure_inventory_ready(cptestctx).await;
        ensure_dpd_ready(cptestctx).await;
    }

    let client = &cptestctx.external_client;
    let multicast_groups: Vec<_> = multicast_group_names
        .iter()
        .map(|name| MulticastGroupJoinSpec {
            group: MulticastGroupIdentifier::Name(name.parse().unwrap()),
            source_ips: None,
            ip_version: None,
        })
        .collect();

    let url = format!("/v1/instances?project={project_name}");

    object_create(
        client,
        &url,
        &InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!(
                    "Instance for multicast group testing: {instance_name}"
                ),
            },
            ncpus: InstanceCpuCount::try_from(1).unwrap(),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: instance_name.parse::<Hostname>().unwrap(),
            user_data: vec![],
            ssh_public_keys: None,
            network_interfaces: InstanceNetworkInterfaceAttachment::DefaultIpv4,
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

/// Attach an instance to a multicast group.
///
/// If the group doesn't exist and is referenced by name, it will be implicitly created.
pub(crate) async fn multicast_group_attach(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
) {
    multicast_group_attach_with_sources(
        cptestctx,
        project_name,
        instance_name,
        group_name,
        None,
    )
    .await
}

/// Attach an instance to a multicast group with optional source IPs.
///
/// If the group doesn't exist and is referenced by name, it will be implicitly created.
/// For SSM groups (232.0.0.0/8), source_ips must be provided.
pub(crate) async fn multicast_group_attach_with_sources(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
    source_ips: Option<Vec<IpAddr>>,
) {
    let client = &cptestctx.external_client;
    let url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );
    let body = InstanceMulticastGroupJoin { source_ips, ip_version: None };
    put_upsert::<_, MulticastGroupMember>(client, &url, &body).await;
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
                    "Instance {name} in Starting state - will wait for Running then stop",
                );
            }
            InstanceState::Stopped => {
                eprintln!("Instance {name} already stopped")
            }
            _ => eprintln!(
                "Instance {name} in state {:?} - will attempt to delete as-is",
                instance.runtime.run_state
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

            // Use the fallible version during cleanup: if sled agent
            // communication fails (e.g., because a test intentionally failed
            // DPD or the sled), we log and continue rather than panic. Real
            // issues are caught during test execution, not cleanup.
            if let Err(e) = instance_helpers::try_instance_simulate(
                &cptestctx.server.server_context().nexus,
                &instance_id,
            )
            .await
            {
                eprintln!("Warning: Failed to simulate instance {name}: {e:?}");
                continue;
            }
            instance_helpers::instance_wait_for_state_as(
                client,
                AuthnMode::PrivilegedUser,
                instance_id,
                InstanceState::Running,
            )
            .await;

            eprintln!("Instance {name} reached Running state");
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

/// Wait until each listed member's stored `sled_id` matches the expected
/// post-migration sled.
///
/// [`wait_for_member_state`] for "Joined" is satisfied as soon as the
/// member is in "Joined", which can happen with the *pre-migration*
/// `sled_id` still recorded if the reconciler has not yet re-observed
/// the new active VMM.
/// Tests that snapshot dataplane state immediately after migration must
/// wait until the DB row reflects the new placement.
///
/// Drives one reconciler activation. The members reconciler detects the
/// `member.sled_id != live_vmm.sled_id` skew and runs `handle_sled_migration`
/// inline (`members.rs:704-713`), so the row is settled by the time this
/// returns. Polling around the read is only for read-after-write visibility.
pub(crate) async fn wait_for_member_sled_ids(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
    expected: &[(Uuid, SledUuid)],
) {
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let group_id = {
        let group =
            get_multicast_group(&cptestctx.external_client, group_name).await;
        group.identity.id
    };

    activate_multicast_reconciler(lockstep_client).await;

    let check = || async {
        let members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id),
                &DataPageParams::max_page(),
            )
            .await
            .map_err(|e| {
                CondCheckError::Failed(format!("list members failed: {e}"))
            })?;

        for (instance_id, expected_sled) in expected {
            let member = members
                .iter()
                .find(|m| m.parent_id == *instance_id)
                .ok_or(CondCheckError::NotYet)?;
            let sled_id = member.sled_id.ok_or(CondCheckError::NotYet)?;
            if sled_id.into_untyped_uuid() != expected_sled.into_untyped_uuid()
            {
                return Err(CondCheckError::Failed(format!(
                    "member for instance {instance_id} reached sled_id \
                     {sled_id:?} after one reconciler pass, expected \
                     {expected_sled:?}"
                )));
            }
        }
        Ok::<_, CondCheckError<String>>(())
    };

    wait_for_condition(check, &POLL_INTERVAL, &MULTICAST_OPERATION_TIMEOUT)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "members in group {group_name} did not reach expected sled \
                 assignments {expected:?}: {e:?}"
            )
        });
}

/// Migrate an instance to a specific target sled.
///
/// No-op if the instance is already on `target_sled`. Otherwise drives
/// the standard request-then-simulate-source-then-simulate-target sequence
/// used by other integration tests, returning when the instance has
/// reached `Running` on the target.
pub(crate) async fn migrate_instance_to(
    cptestctx: &ControlPlaneTestContext,
    instance_id: InstanceUuid,
    target_sled: SledUuid,
) {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let nexus = &cptestctx.server.server_context().nexus;

    let info = nexus
        .active_instance_info(&instance_id, None)
        .await
        .unwrap()
        .expect("instance should be on a sled");
    if info.sled_id == target_sled {
        return;
    }
    let source_sled = info.sled_id;

    let migrate_url = format!("/instances/{instance_id}/migrate");
    NexusRequest::new(
        RequestBuilder::new(lockstep_client, Method::POST, &migrate_url)
            .body(Some(&InstanceMigrateRequest { dst_sled_id: target_sled }))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("should initiate migration");

    let info =
        nexus.active_instance_info(&instance_id, None).await.unwrap().unwrap();
    let src_propolis = info.propolis_id;
    let dst_propolis = info.dst_propolis_id.unwrap();

    instance_helpers::vmm_simulate_on_sled(
        cptestctx,
        nexus,
        source_sled,
        src_propolis,
    )
    .await;
    instance_helpers::instance_wait_for_state(
        client,
        instance_id,
        InstanceState::Migrating,
    )
    .await;

    instance_helpers::vmm_simulate_on_sled(
        cptestctx,
        nexus,
        target_sled,
        dst_propolis,
    )
    .await;
    instance_helpers::instance_wait_for_state(
        client,
        instance_id,
        InstanceState::Running,
    )
    .await;
}

/// Resolve the underlay admin-local IPv6 address for a multicast group
/// given its external multicast IP.
pub(crate) async fn fetch_underlay_admin_ip(
    cptestctx: &ControlPlaneTestContext,
    external_multicast_ip: IpAddr,
) -> dpd_client::types::UnderlayMulticastIpv6 {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let external_group = datastore
        .multicast_group_lookup_by_ip(&opctx, external_multicast_ip)
        .await
        .expect("should look up external multicast group by IP");
    let underlay_group_id = external_group
        .underlay_group_id
        .expect("external group should have underlay_group_id");
    let underlay_group = datastore
        .underlay_multicast_group_fetch(&opctx, underlay_group_id)
        .await
        .expect("should fetch underlay multicast group");

    match underlay_group.multicast_ip.ip() {
        IpAddr::V6(v6) => {
            dpd_client::types::UnderlayMulticastIpv6::try_from(v6)
                .expect("underlay IP should be admin-local IPv6")
        }
        IpAddr::V4(other) => {
            panic!("expected IPv6 underlay address, got {other}")
        }
    }
}

/// Stop multiple instances, poking the simulated sled-agent while waiting.
pub(crate) async fn stop_instances(
    cptestctx: &ControlPlaneTestContext,
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
) {
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
                            wait_for_instance_stopped(
                                cptestctx,
                                client,
                                *instance_id,
                                name,
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
///
/// Ensures inventory and DPD are ready once before attaching all instances, avoiding redundant checks.
pub(crate) async fn multicast_group_attach_bulk(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_names: &[&str],
    group_name: &str,
) {
    // Check inventory and DPD readiness once for all attachments
    ensure_inventory_ready(cptestctx).await;
    ensure_dpd_ready(cptestctx).await;

    let attach_futures = instance_names.iter().map(|instance_name| {
        multicast_group_attach(
            cptestctx,
            project_name,
            instance_name,
            group_name,
        )
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
    .expect("Should detach instance from multicast group");
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

/// Assert that *every* mgd in the fixture has an MRIB route for `group_ip`.
///
/// Iterates every switch zone present in `cptestctx.mgd`, so multi-switch
/// fixtures (`extra_sled_agents > 0`) catch a route that is programmed only
/// on a subset of switches.
pub(crate) async fn assert_mrib_route_exists(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
    group_ip: IpAddr,
) {
    for_each_mgd(cptestctx, |slot, mgd_client| async move {
        wait_for_condition::<_, (), _, _>(
            || async {
                let routes = mgd_client
                    .static_list_mcast_routes()
                    .await
                    .unwrap()
                    .into_inner();
                if routes
                    .iter()
                    .any(|r| mrib_route_matches_group(&r.key, group_ip))
                {
                    Ok(())
                } else {
                    Err(CondCheckError::NotYet)
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("mgd on {slot:?} never had a route for {group_ip}: {e:?}")
        });
    })
    .await;
}

/// Assert that *no* mgd in the fixture has an MRIB route for `group_ip`.
pub(crate) async fn assert_mrib_route_absent(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
    group_ip: IpAddr,
) {
    for_each_mgd(cptestctx, |slot, mgd_client| async move {
        wait_for_condition::<_, (), _, _>(
            || async {
                let routes = mgd_client
                    .static_list_mcast_routes()
                    .await
                    .unwrap()
                    .into_inner();
                if routes
                    .iter()
                    .any(|r| mrib_route_matches_group(&r.key, group_ip))
                {
                    Err(CondCheckError::NotYet)
                } else {
                    Ok(())
                }
            },
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("mgd on {slot:?} still had a route for {group_ip}: {e:?}")
        });
    })
    .await;
}

/// Run `f` against every mgd client in the fixture, in `SwitchSlot` order.
async fn for_each_mgd<F, Fut>(
    cptestctx: &nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >,
    f: F,
) where
    F: Fn(
        sled_agent_types::early_networking::SwitchSlot,
        mg_admin_client::Client,
    ) -> Fut,
    Fut: Future<Output = ()>,
{
    assert!(
        !cptestctx.mgd.is_empty(),
        "multicast MRIB assertions require at least one mgd in the test \
         fixture",
    );
    let switches: std::collections::BTreeMap<_, _> =
        cptestctx.mgd.iter().collect();
    for (slot, mgd) in switches {
        let mgd_client = mg_admin_client::Client::new(
            &format!("http://[::1]:{}", mgd.port),
            cptestctx.logctx.log.clone(),
        );
        f(*slot, mgd_client).await;
    }
}

fn mrib_route_matches_group(
    key: &mg_admin_client::types::MulticastRouteKey,
    group_ip: IpAddr,
) -> bool {
    match (key, group_ip) {
        (mg_admin_client::types::MulticastRouteKey::V4(k), IpAddr::V4(ip)) => {
            k.group == ip
        }
        (mg_admin_client::types::MulticastRouteKey::V6(k), IpAddr::V6(ip)) => {
            k.group == ip
        }
        _ => false,
    }
}
