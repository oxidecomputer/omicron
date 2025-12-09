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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use slog::{debug, info, warn};
use uuid::Uuid;

use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    link_ip_pool, object_create, object_delete,
};
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::params::{
    InstanceCreate, InstanceMulticastGroupJoin,
    InstanceNetworkInterfaceAttachment, IpPoolCreate, MulticastGroupIdentifier,
};
use nexus_types::external_api::shared::{IpRange, Ipv4Range};
use nexus_types::external_api::views::{
    IpPool, IpPoolRange, IpVersion, MulticastGroup, MulticastGroupMember,
};
use nexus_types::identity::{Asset, Resource};
use omicron_common::api::external::{
    ByteCount, Hostname, IdentityMetadataCreateParams, Instance,
    InstanceCpuCount, InstanceState, NameOrId,
};
use omicron_nexus::TestInterfaces;
use omicron_test_utils::dev::poll::{self, CondCheckError, wait_for_condition};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

use crate::integration_tests::instances as instance_helpers;

// Shared type alias for all multicast integration tests
pub(crate) type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

mod api;
mod authorization;
mod cache_invalidation;
mod enablement;
mod failures;
mod groups;
mod instances;
mod networking_integration;
mod omdb;

// Timeout constants for test operations
const POLL_INTERVAL: Duration = Duration::from_millis(80);
const MULTICAST_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

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

/// Build URL for adding a member to a multicast group.
///
/// The `?project=` parameter is required when using instance names (for scoping)
/// but must not be provided when using instance UUIDs (causes 400 Bad Request).
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
/// Use this when you need to explicitly trigger the reconciler (e.g., after
/// restarting DPD) rather than waiting for an already-triggered run.
pub(crate) async fn activate_multicast_reconciler(
    lockstep_client: &ClientTestContext,
) -> nexus_lockstep_client::types::BackgroundTask {
    nexus_test_utils::background::activate_background_task(
        lockstep_client,
        "multicast_reconciler",
    )
    .await
}

/// Wait for a condition to be true, activating the reconciler periodically.
///
/// This is like `wait_for_condition` but activates the multicast reconciler
/// periodically (not on every poll) to drive state changes. We activate the
/// reconciler every 500ms.
///
/// Useful for tests that need to wait for reconciler-driven state changes
/// (e.g., member state transitions).
pub(crate) async fn wait_for_condition_with_reconciler<F, Fut, T, E>(
    lockstep_client: &ClientTestContext,
    condition: F,
    poll_interval: &Duration,
    timeout: &Duration,
) -> Result<T, poll::Error<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, CondCheckError<E>>>,
{
    // Activate reconciler less frequently than we check the condition
    // This reduces overhead while still driving state changes forward
    const RECONCILER_ACTIVATION_INTERVAL: Duration = Duration::from_millis(500);

    let last_reconciler_activation = Arc::new(Mutex::new(Instant::now()));

    // Activate once at the start to kick things off
    wait_for_multicast_reconciler(lockstep_client).await;

    wait_for_condition(
        || async {
            // Only activate reconciler if enough time has passed
            let now = Instant::now();
            let should_activate = {
                let last = last_reconciler_activation.lock().unwrap();
                now.duration_since(*last) >= RECONCILER_ACTIVATION_INTERVAL
            };

            if should_activate {
                wait_for_multicast_reconciler(lockstep_client).await;
                *last_reconciler_activation.lock().unwrap() = now;
            }

            condition().await
        },
        poll_interval,
        timeout,
    )
    .await
}

/// Ensure inventory collection has completed with SP data for all sleds.
///
/// This function verifies that inventory has SP data for EVERY in-service sled,
/// not just that inventory completed.
///
/// This is required for multicast member operations which map `sled_id` → `sp_slot`
/// → switch ports via inventory.
pub(crate) async fn ensure_inventory_ready(
    cptestctx: &ControlPlaneTestContext,
) {
    let log = &cptestctx.logctx.log;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    info!(log, "waiting for inventory with SP data for all sleds");

    // Wait for inventory to have SP data for ALL in-service sleds
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
                    (bb.serial_number == sled.serial_number()
                        && bb.part_number == sled.part_number())
                        || bb.serial_number == sled.serial_number()
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
        &Duration::from_millis(500), // Check every 500ms
        &Duration::from_secs(120),   // Wait up to 120s
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
        &Duration::from_millis(200), // Check every 200ms
        &Duration::from_secs(30),    // Wait up to 30 seconds for switches
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

/// Wait for a specific member to reach the expected state
/// (e.g., Joined, Joining, Left).
///
/// For "Joined" state, this function uses `wait_for_condition_with_reconciler`
/// to ensure the reconciler processes member state transitions.
pub(crate) async fn wait_for_member_state(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
    instance_id: Uuid,
    expected_state: nexus_db_model::MulticastGroupMemberState,
) -> MulticastGroupMember {
    let client = &cptestctx.external_client;
    let lockstep_client = &cptestctx.lockstep_client;
    let expected_state_as_str = expected_state.to_string();

    // For "Joined" state, ensure instance has a sled_id assigned
    // (no need to check inventory again since ensure_inventory_ready() already
    // verified all sleds have SP data at test setup)
    if expected_state == nexus_db_model::MulticastGroupMemberState::Joined {
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);
        wait_for_instance_sled_assignment(cptestctx, &instance_uuid).await;
    }

    let check_member = || async {
        let members = list_multicast_group_members(client, group_name).await;

        // If we're looking for "Joined" state, we need to ensure the member exists first
        // and then wait for the reconciler to process it
        if expected_state == nexus_db_model::MulticastGroupMemberState::Joined {
            if let Some(member) =
                members.iter().find(|m| m.instance_id == instance_id)
            {
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
                    other_state => Err(CondCheckError::Failed(format!(
                        "Member {instance_id} in group {group_name} has unexpected state '{other_state}', expected 'Left', 'Joining' or 'Joined'"
                    ))),
                }
            } else {
                // Member doesn't exist yet - wait for it to be created
                Err(CondCheckError::NotYet)
            }
        } else {
            // For other states, just look for exact match
            if let Some(member) =
                members.iter().find(|m| m.instance_id == instance_id)
            {
                if member.state == expected_state_as_str {
                    Ok(member.clone())
                } else {
                    Err(CondCheckError::NotYet)
                }
            } else {
                Err(CondCheckError::NotYet)
            }
        }
    };

    // Use reconciler-activating wait for "Joined" state
    let result = if expected_state
        == nexus_db_model::MulticastGroupMemberState::Joined
    {
        wait_for_condition_with_reconciler(
            lockstep_client,
            check_member,
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
    } else {
        wait_for_condition(
            check_member,
            &POLL_INTERVAL,
            &MULTICAST_OPERATION_TIMEOUT,
        )
        .await
    };

    match result {
        Ok(member) => member,
        Err(poll::Error::TimedOut(elapsed)) => {
            panic!(
                "member {instance_id} in group {group_name} did not reach state '{expected_state_as_str}' within {elapsed:?}",
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for member {instance_id} in group {group_name} to reach state '{expected_state_as_str}': {err:?}",
            );
        }
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
                        "vmm_state" => ?vmm.runtime.state,
                        "sled_id" => %vmm.sled_id
                    );

                    // VMM exists and has a sled_id - we're good
                    Ok(())
                } else {
                    debug!(
                        log,
                        "instance exists but has no VMM yet";
                        "instance_id" => %instance_id,
                        "instance_state" => ?instance.runtime_state.nexus_state.state()
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
        .multicast_group_members_list_by_instance(&opctx, *instance_uuid)
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

/// Verify a group is either deleted or in one of the expected states.
///
/// Useful when DPD is unavailable and groups can't complete state transitions.
/// For example, when DPD is down during deletion, groups may be stuck in
/// "Creating" or "Deleting" state rather than being fully deleted.
pub(crate) async fn verify_group_deleted_or_in_states(
    client: &ClientTestContext,
    group_name: &str,
    expected_states: &[&str],
) {
    let groups_result =
        nexus_test_utils::resource_helpers::objects_list_page_authz::<
            MulticastGroup,
        >(client, "/v1/multicast-groups")
        .await;

    let matching_groups: Vec<_> = groups_result
        .items
        .into_iter()
        .filter(|g| g.identity.name == group_name)
        .collect();

    if !matching_groups.is_empty() {
        // Group still exists - should be in one of the expected states
        let actual_state = &matching_groups[0].state;
        assert!(
            expected_states.contains(&actual_state.as_str()),
            "Group {group_name} should be in one of {expected_states:?} states, found: \"{actual_state}\""
        );
    }
    // If group is gone, that's also valid - operation completed
}

/// Wait for a multicast group to be deleted from DPD (dataplane) with reconciler activation.
///
/// This function waits for the DPD to report that the multicast group no longer exists
/// (returns 404), while periodically activating the reconciler to drive the cleanup process.
pub(crate) async fn wait_for_group_deleted_from_dpd(
    cptestctx: &ControlPlaneTestContext,
    multicast_ip: std::net::IpAddr,
) {
    let lockstep_client = &cptestctx.lockstep_client;
    let dpd_client = nexus_test_utils::dpd_client(cptestctx);

    match wait_for_condition_with_reconciler(
        lockstep_client,
        || async {
            match dpd_client.multicast_group_get(&multicast_ip).await {
                Ok(_) => {
                    // Group still exists in DPD - not yet deleted
                    Err(CondCheckError::<()>::NotYet)
                }
                Err(_) => Ok(()), // Group doesn't exist - deleted
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
                "group with IP {multicast_ip} was not deleted from DPD within {elapsed:?}",
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
        .map(|name| MulticastGroupIdentifier::Name(name.parse().unwrap()))
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

/// Attach an instance to a multicast group.
///
/// If the group doesn't exist and is referenced by name, it will be implicitly created
/// using the specified pool (required for implicit creation).
pub(crate) async fn multicast_group_attach(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
) {
    multicast_group_attach_with_pool(
        cptestctx,
        project_name,
        instance_name,
        group_name,
        None,
    )
    .await
}

/// Attach an instance to a multicast group, specifying a pool for implicit creation.
///
/// If the group doesn't exist and is referenced by name, it will be implicitly created
/// using the specified pool.
pub(crate) async fn multicast_group_attach_with_pool(
    cptestctx: &ControlPlaneTestContext,
    project_name: &str,
    instance_name: &str,
    group_name: &str,
    _pool: Option<&str>,
) {
    let client = &cptestctx.external_client;
    let url = format!(
        "/v1/instances/{instance_name}/multicast-groups/{group_name}?project={project_name}"
    );

    let body = InstanceMulticastGroupJoin { source_ips: None };

    // Use PUT to attach instance to multicast group
    let response = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &url)
            .body(Some(&body))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("Should attach instance to multicast group");

    response
        .parsed_body::<MulticastGroupMember>()
        .expect("Should parse member");
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

/// Stop multiple instances using the exact same pattern as groups.rs.
pub(crate) async fn stop_instances(
    cptestctx: &ControlPlaneTestContext,
    client: &ClientTestContext,
    project_name: &str,
    instance_names: &[&str],
) {
    let nexus = &cptestctx.server.server_context().nexus;

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
