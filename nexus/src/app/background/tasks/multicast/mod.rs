// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reconciling multicast group state with dendrite switch
//! configuration.
//!
//! # Reliable Persistent Workflow (RPW)
//!
//! This module implements the RPW pattern for multicast groups. It ensures
//! eventual consistency between database state and the physical network
//! switches (Dendrite). Sagas handle immediate transactional operations;
//! RPW handles ongoing background reconciliation.
//!
//! ## Distributed State Convergence
//!
//! Multicast converges state across several distributed components:
//! - Database state (groups, members, routing configuration)
//! - Dataplane state (match-action tables via Dendrite/DPD)
//! - Instance lifecycle (start/stop/migrate affecting group membership)
//! - Network topology (sled-to-switch mappings, port configurations)
//!
//! ## Architecture: RPW +/- Sagas
//!
//! **Sagas handle immediate operations:**
//! - User API requests (create/delete groups)
//! - Instance lifecycle events (start/stop)
//! - Database state transitions
//! - Initial validation and resource allocation
//!
//! **RPW handles background reconciliation:**
//! - Dataplane state convergence
//! - Group and Member state checks and transitions ("Joining" → "Joined" → "Left")
//! - Drift detection and correction
//! - Cleanup of orphaned resources
//!
//! ## Multicast Group Architecture
//!
//! ### External vs Underlay Groups
//!
//! The multicast implementation uses a bifurcated design with paired groups:
//!
//! **External Groups** (customer-facing):
//! - IPv4/IPv6 addresses allocated from IP pools
//! - Exposed via operator APIs and network interfaces
//! - Subject to VPC routing and firewall policies
//!
//! **Underlay Groups** (admin-scoped IPv6):
//! - IPv6 multicast scope per RFC 7346; admin-local is ff04::/16
//!   <https://www.rfc-editor.org/rfc/rfc7346>
//! - Internal rack forwarding to guest instances
//! - Mapped 1:1 with external groups via deterministic mapping
//!
//! ### Forwarding Architecture (Incoming multicast traffic to guests)
//!
//! Traffic flow for multicast into the rack and to guest instances:
//! `External Network → Switch ASIC → Underlay Group → OPTE (decap) → Instance`
//!
//! 1. **External traffic** arrives into the rack on an external multicast address
//! 2. **Switch ASIC translation** performs NAT/encapsulation from external to underlay multicast
//! 3. **Underlay forwarding** via DPD-programmed P4 tables across switch fabric
//! 4. **OPTE decapsulation** removes Geneve/IPv6/Ethernet outer headers on target sleds
//! 5. **Instance delivery** of inner (guest-facing) packet to guest
//!
//! TODO: Other traffic flows like egress from instances will be documented separately
//!
//! ## Reconciliation Components
//!
//! The reconciler handles:
//! - **Group lifecycle**: "Creating" → "Active" → "Deleting" → hard-deleted
//! - **Member lifecycle**: "Joining" → "Joined" → "Left" → soft-deleted → hard-deleted
//! - **Dataplane updates**: DPD API calls for P4 table updates
//! - **Topology mapping**: Sled-to-switch-port resolution (with caching)
//!
//! ## Deletion Semantics: Groups vs Members
//!
//! **Groups** use state machine deletion:
//! - User deletes group → state="Deleting" (no `time_deleted` set yet)
//! - RPW cleans up switch config and associated resources
//! - RPW hard-deletes the row (uses `diesel::delete`)
//! - Note: `deallocate_external_multicast_group` (IP pool deallocation) sets
//!   `time_deleted` directly, but this is separate from user-initiated deletion
//!
//! **Members** use dual-purpose "Left" state with soft-delete:
//! - Instance stopped: state="Left", time_deleted=NULL
//!   - Can rejoin when instance starts
//!   - RPW can transition back to "Joining" when instance becomes valid
//! - Instance deleted: state="Left", time_deleted=SET (permanent soft-delete)
//!   - Cannot be reactivated (new attach creates new member record)
//!   - RPW removes DPD configuration
//!   - Cleanup task eventually hard-deletes the row

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use ipnet::Ipv6Net;
use serde_json::json;
use slog::{error, info};
use tokio::sync::RwLock;

use nexus_config::DEFAULT_UNDERLAY_MULTICAST_NET;
use nexus_db_model::MulticastGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::MulticastGroupReconcilerStatus;
use omicron_uuid_kinds::SledUuid;

use crate::app::background::BackgroundTask;
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::saga::StartSaga;

pub(crate) mod groups;
pub(crate) mod members;

/// Type alias for the sled mapping cache.
type SledMappingCache =
    Arc<RwLock<(SystemTime, HashMap<SledUuid, Vec<SwitchBackplanePort>>)>>;

/// Type alias for the backplane map cache.
type BackplaneMapCache = Arc<
    RwLock<
        Option<(
            SystemTime,
            BTreeMap<
                dpd_client::types::PortId,
                dpd_client::types::BackplaneLink,
            >,
        )>,
    >,
>;

/// Result of processing a state transition for multicast entities.
#[derive(Debug)]
pub(crate) enum StateTransition {
    /// No state change needed.
    NoChange,
    /// State changed successfully.
    StateChanged,
    /// Entity needs cleanup/removal.
    NeedsCleanup,
}

/// Switch port configuration for multicast group members.
#[derive(Clone, Debug)]
pub(crate) struct SwitchBackplanePort {
    /// Switch port ID
    pub port_id: dpd_client::types::PortId,
    /// Switch link ID
    pub link_id: dpd_client::types::LinkId,
    /// Direction for multicast traffic (External or Underlay)
    pub direction: dpd_client::types::Direction,
}

/// Background task that reconciles multicast group state with dendrite
/// configuration using the Saga + RPW hybrid pattern.
pub(crate) struct MulticastGroupReconciler {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    sagas: Arc<dyn StartSaga>,
    underlay_admin_prefix: Ipv6Net,
    /// Cache for sled-to-backplane-port mappings.
    /// Maps sled_id → rear backplane ports for multicast traffic routing.
    sled_mapping_cache: SledMappingCache,
    sled_cache_ttl: Duration,
    /// Cache for backplane hardware topology from DPD.
    /// Maps PortId → BackplaneLink for platform-specific port validation.
    backplane_map_cache: BackplaneMapCache,
    backplane_cache_ttl: Duration,
    /// Maximum number of members to process concurrently per group.
    member_concurrency_limit: usize,
    /// Maximum number of groups to process concurrently.
    group_concurrency_limit: usize,
    /// Whether multicast functionality is enabled (or not).
    enabled: bool,
    /// Flag to signal cache invalidation on next activation.
    ///
    /// Set to `true` when topology changes occur (sled add/remove, inventory updates).
    /// Checked and cleared at the start of each reconciliation pass.
    invalidate_cache_on_next_run: Arc<AtomicBool>,
}

impl MulticastGroupReconciler {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        sagas: Arc<dyn StartSaga>,
        enabled: bool,
        sled_cache_ttl: Duration,
        backplane_cache_ttl: Duration,
        invalidate_cache_flag: Arc<AtomicBool>,
    ) -> Self {
        // Use the configured underlay admin-local prefix
        let underlay_admin_prefix: Ipv6Net = DEFAULT_UNDERLAY_MULTICAST_NET
            .to_string()
            .parse()
            .expect("DEFAULT_UNDERLAY_MULTICAST_NET must be valid Ipv6Net");

        Self {
            datastore,
            resolver,
            sagas,
            underlay_admin_prefix,
            sled_mapping_cache: Arc::new(RwLock::new((
                SystemTime::now(),
                HashMap::new(),
            ))),
            sled_cache_ttl,
            backplane_map_cache: Arc::new(RwLock::new(None)),
            backplane_cache_ttl,
            member_concurrency_limit: 100,
            group_concurrency_limit: 100,
            enabled,
            invalidate_cache_on_next_run: invalidate_cache_flag,
        }
    }

    /// Generate tag for multicast groups.
    ///
    /// Both external and underlay groups use the same tag (the group name).
    /// This pairs them logically for management and cleanup operations.
    pub(crate) fn generate_multicast_tag(group: &MulticastGroup) -> String {
        group.name().to_string()
    }

    /// Generate admin-scoped IPv6 multicast address from an external multicast
    /// address.
    ///
    /// Maps external addresses into the configured underlay admin-local prefix
    /// (DEFAULT_UNDERLAY_MULTICAST_NET) using bitmask mapping. Preserves the
    /// lower `128 - prefix_len` bits from the external address (the group ID)
    /// and sets the high bits from the prefix.
    ///
    /// Admin-local scope (ff04::/16) is defined in RFC 7346.
    /// See: <https://www.rfc-editor.org/rfc/rfc7346>
    pub(crate) fn map_external_to_underlay_ip(
        &self,
        external_ip: IpAddr,
    ) -> Result<IpAddr, anyhow::Error> {
        map_external_to_underlay_ip_impl(
            self.underlay_admin_prefix,
            external_ip,
        )
    }

    /// Invalidate the backplane map cache, forcing refresh on next access.
    ///
    /// Called when:
    /// - Sled validation fails (sp_slot not in cached backplane map)
    /// - Need to refresh topology data after detecting potential changes
    pub(crate) async fn invalidate_backplane_cache(&self) {
        let mut cache = self.backplane_map_cache.write().await;
        *cache = None; // Clear the cache entirely
    }

    /// Invalidate the sled mapping cache, forcing refresh on next access.
    ///
    /// Called when:
    /// - Backplane topology changes detected (different port count/layout)
    /// - Need to re-validate sled mappings against new topology
    pub(crate) async fn invalidate_sled_mapping_cache(&self) {
        let mut cache = self.sled_mapping_cache.write().await;
        // Set timestamp to epoch to force refresh
        *cache = (SystemTime::UNIX_EPOCH, cache.1.clone());
    }
}

/// Pure function implementation of external-to-underlay IP mapping.
/// This can be tested independently without requiring a full reconciler instance.
fn map_external_to_underlay_ip_impl(
    underlay_admin_prefix: Ipv6Net,
    external_ip: IpAddr,
) -> Result<IpAddr, anyhow::Error> {
    // Compute base (prefix network) and host mask
    let base = underlay_admin_prefix.network();
    let prefix_len = underlay_admin_prefix.prefix_len();
    let host_bits = 128u32.saturating_sub(u32::from(prefix_len));
    let base_u128 = u128::from_be_bytes(base.octets());
    let mask: u128 = if host_bits == 128 {
        u128::MAX
    } else if host_bits == 0 {
        0
    } else {
        (1u128 << host_bits) - 1
    };

    // Derive a value to fit in the available host bits
    let host_value: u128 = match external_ip {
        IpAddr::V4(ipv4) => {
            // IPv4 addresses need at least 32 host bits to preserve full address
            // (IPv4 multicast validation happens at IP pool allocation time)
            if host_bits < 32 {
                return Err(anyhow::Error::msg(format!(
                    "Prefix {underlay_admin_prefix} has only {host_bits} host \
                     bits, but IPv4 requires at least 32 bits"
                )));
            }
            u128::from(u32::from_be_bytes(ipv4.octets()))
        }
        IpAddr::V6(ipv6) => {
            // IPv6 multicast validation (including ff01::/ff02:: exclusions)
            // happens at IP pool allocation time
            let full_addr = u128::from_be_bytes(ipv6.octets());

            // XOR-fold the full 128-bit address into the available host bits
            // to avoid collisions. This ensures different external addresses
            // (even with identical lower bits but different scopes) map to
            // different underlay addresses.
            if host_bits < 128 {
                // Split into chunks and XOR them together
                let mut result = 0u128;
                let mut remaining = full_addr;
                while remaining != 0 {
                    result ^= remaining & mask;
                    remaining >>= host_bits;
                }
                result
            } else {
                // host_bits >= 128: use full address as-is
                full_addr
            }
        }
    };

    // Combine base network + computed host value
    let underlay_u128 = (base_u128 & !mask) | (host_value & mask);
    let underlay_ipv6 = Ipv6Addr::from(underlay_u128.to_be_bytes());

    // Validate bounds
    if !underlay_admin_prefix.contains(&underlay_ipv6) {
        return Err(anyhow::Error::msg(format!(
            "Generated underlay IP {underlay_ipv6} falls outside configured \
             prefix {underlay_admin_prefix} (external {external_ip})."
        )));
    }

    Ok(IpAddr::V6(underlay_ipv6))
}

impl BackgroundTask for MulticastGroupReconciler {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            if !self.enabled {
                info!(opctx.log, "multicast group reconciler not enabled");
                let mut status = MulticastGroupReconcilerStatus::default();
                status.disabled = true;
                return json!(status);
            }

            trace!(opctx.log, "multicast group reconciler activating");
            let status = self.run_reconciliation_pass(opctx).await;

            let did_work = status.groups_created
                + status.groups_deleted
                + status.groups_verified
                + status.members_processed
                + status.members_deleted
                > 0;

            if status.errors.is_empty() {
                if did_work {
                    info!(
                        opctx.log,
                        "multicast RPW reconciliation pass completed successfully";
                        "external_groups_created" => status.groups_created,
                        "external_groups_deleted" => status.groups_deleted,
                        "active_groups_verified" => status.groups_verified,
                        "member_state_transitions" => status.members_processed,
                        "orphaned_members_cleaned" => status.members_deleted,
                        "dataplane_operations" => status.groups_created + status.groups_deleted + status.members_processed
                    );
                } else {
                    trace!(
                        opctx.log,
                        "multicast RPW reconciliation pass completed - dataplane consistent"
                    );
                }
            } else {
                error!(
                    opctx.log,
                    "multicast RPW reconciliation pass completed with dataplane inconsistencies";
                    "external_groups_created" => status.groups_created,
                    "external_groups_deleted" => status.groups_deleted,
                    "active_groups_verified" => status.groups_verified,
                    "member_state_transitions" => status.members_processed,
                    "orphaned_members_cleaned" => status.members_deleted,
                    "dataplane_error_count" => status.errors.len()
                );
            }

            json!(status)
        }
        .boxed()
    }
}

impl MulticastGroupReconciler {
    /// Execute a full reconciliation pass.
    async fn run_reconciliation_pass(
        &mut self,
        opctx: &OpContext,
    ) -> MulticastGroupReconcilerStatus {
        let mut status = MulticastGroupReconcilerStatus::default();

        trace!(opctx.log, "starting multicast reconciliation pass");

        // Check if cache invalidation was requested
        if self
            .invalidate_cache_on_next_run
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!(
                opctx.log,
                "invalidating multicast caches due to topology change"
            );
            self.invalidate_backplane_cache().await;
            self.invalidate_sled_mapping_cache().await;
        }

        // Create dataplane client (across switches) once for the entire
        // reconciliation pass (in case anything has changed)
        let dataplane_client = match MulticastDataplaneClient::new(
            self.resolver.clone(),
            opctx.log.clone(),
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                let msg = format!(
                    "failed to create multicast dataplane client: {e:#}"
                );
                status.errors.push(msg);
                return status;
            }
        };

        // Process creating groups
        match self.reconcile_creating_groups(opctx).await {
            Ok(count) => status.groups_created += count,
            Err(e) => {
                let msg = format!("failed to reconcile creating groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Process deleting groups
        match self.reconcile_deleting_groups(opctx, &dataplane_client).await {
            Ok(count) => status.groups_deleted += count,
            Err(e) => {
                let msg = format!("failed to reconcile deleting groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Reconcile active groups (verify state, update dataplane as needed)
        match self.reconcile_active_groups(opctx, &dataplane_client).await {
            Ok(count) => status.groups_verified += count,
            Err(e) => {
                let msg = format!("failed to reconcile active groups: {e:#}");
                status.errors.push(msg);
            }
        }

        // Process member state changes
        match self.reconcile_member_states(opctx, &dataplane_client).await {
            Ok(count) => status.members_processed += count,
            Err(e) => {
                let msg = format!("failed to reconcile member states: {e:#}");
                status.errors.push(msg);
            }
        }

        // Clean up deleted members ("Left" + `time_deleted`)
        match self.cleanup_deleted_members(opctx).await {
            Ok(count) => status.members_deleted += count,
            Err(e) => {
                let msg = format!("failed to cleanup deleted members: {e:#}");
                status.errors.push(msg);
            }
        }

        trace!(
            opctx.log,
            "multicast RPW reconciliation cycle completed";
            "external_groups_created" => status.groups_created,
            "external_groups_deleted" => status.groups_deleted,
            "active_groups_verified" => status.groups_verified,
            "member_lifecycle_transitions" => status.members_processed,
            "orphaned_member_cleanup" => status.members_deleted,
            "total_dpd_operations" => status.groups_created + status.groups_deleted + status.members_processed,
            "error_count" => status.errors.len()
        );

        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::{Ipv4Addr, Ipv6Addr};

    use omicron_common::address::IPV6_ADMIN_SCOPED_MULTICAST_PREFIX;

    #[test]
    fn test_map_ipv4_to_underlay_ipv6() {
        // Test IPv4 multicast mapping to admin-scoped IPv6 using default
        // prefix (ff04::/64). IPv4 fits in lower 32 bits.
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let result = map_external_to_underlay_ip_impl(
            DEFAULT_UNDERLAY_MULTICAST_NET,
            IpAddr::V4(ipv4),
        )
        .unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // Should be ff04::e001:203
                // (224=0xe0, 1=0x01, 2=0x02, 3=0x03)
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0xe001,
                        0x0203,
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv4_edge_cases() {
        // Test minimum IPv4 multicast address using production default prefix
        let ipv4_min = Ipv4Addr::new(224, 0, 0, 1);
        let result = map_external_to_underlay_ip_impl(
            DEFAULT_UNDERLAY_MULTICAST_NET,
            IpAddr::V4(ipv4_min),
        )
        .unwrap();
        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0xe000,
                        0x0001,
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }

        // Test maximum IPv4 multicast address using production default prefix
        let ipv4_max = Ipv4Addr::new(239, 255, 255, 255);
        let result = map_external_to_underlay_ip_impl(
            DEFAULT_UNDERLAY_MULTICAST_NET,
            IpAddr::V4(ipv4_max),
        )
        .unwrap();
        match result {
            IpAddr::V6(ipv6) => {
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0x0000,
                        0xefff,
                        0xffff,
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_multicast_to_admin_scoped() {
        // Test algorithm with wider /16 prefix (not used in production).
        // Tests site-local (ff05::/16) to admin-scoped (ff04::/16) with XOR folding.
        // With /16, we XOR upper 112 bits with lower 112 bits.
        let ipv6_site_local = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678, 0x9abc,
        );
        let prefix_16: Ipv6Net = "ff04::/16".parse().unwrap();
        let result = map_external_to_underlay_ip_impl(
            prefix_16,
            IpAddr::V6(ipv6_site_local),
        )
        .unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // XOR result of 112-bit chunks
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x1234,
                        0x5678,
                        0x9abc,
                        0xdef0,
                        0x1234,
                        0x5678,
                        0x65b9, // XOR folded last segment
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_global_multicast_to_admin_scoped() {
        // Test algorithm with wider /16 prefix (not used in production).
        // Tests global (ff0e::/16) to admin-scoped (ff04::/16) with XOR folding.
        // With /16, we XOR upper 112 bits with lower 112 bits.
        let ipv6_global = Ipv6Addr::new(
            0xff0e, 0xabcd, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
        );
        let prefix_16: Ipv6Net = "ff04::/16".parse().unwrap();
        let result = map_external_to_underlay_ip_impl(
            prefix_16,
            IpAddr::V6(ipv6_global),
        )
        .unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // XOR result of 112-bit chunks
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0xabcd,
                        0x1234,
                        0x5678,
                        0x9abc,
                        0xdef0,
                        0x1234,
                        0xa976, // XOR folded last segment
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_map_ipv6_already_admin_scoped() {
        // Test algorithm with wider /16 prefix (not used in production).
        // Admin-scoped multicast (ff04::/16) gets XOR folded like any other address.
        // With /16, we XOR upper 112 bits with lower 112 bits.
        let ipv6_admin = Ipv6Addr::new(
            IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
            0x1111,
            0x2222,
            0x3333,
            0x4444,
            0x5555,
            0x6666,
            0x7777,
        );
        let prefix_16: Ipv6Net = "ff04::/16".parse().unwrap();
        let result =
            map_external_to_underlay_ip_impl(prefix_16, IpAddr::V6(ipv6_admin))
                .unwrap();

        match result {
            IpAddr::V6(ipv6) => {
                // XOR result of 112-bit chunks
                assert_eq!(
                    ipv6.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x1111,
                        0x2222,
                        0x3333,
                        0x4444,
                        0x5555,
                        0x6666,
                        0x8873, // XOR folded last segment
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_prefix_validation_ipv4_too_small() {
        // Test that a prefix that's too small for IPv4 mapping is rejected
        // ff04::/120 only allows for the last 8 bits to vary, but IPv4 needs 32 bits
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let prefix: Ipv6Net = "ff04::/120".parse().unwrap();
        let result = map_external_to_underlay_ip_impl(prefix, IpAddr::V4(ipv4));

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("has only 8 host bits")
                && err_msg.contains("IPv4 requires at least 32 bits"),
            "Expected IPv4 validation error, got: {err_msg}"
        );
    }

    #[test]
    fn test_prefix_preservation_hash_space_for_large_sets() {
        // Smoke-test: For /64 (64 host bits), generating mappings for 100k
        // unique IPv6 external addresses should produce 100k unique underlay
        // addresses. With /64, we preserve segments 4-7, so vary those.
        use std::collections::HashSet;
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let mut set = HashSet::with_capacity(100_000);
        for i in 0..100_000u32 {
            // Construct a family of multicast IPv6 addresses (global scope ff0e)
            // Vary segments 4-5 (which are preserved with /64) to ensure uniqueness
            let ipv6 = Ipv6Addr::new(
                0xff0e,
                0,
                0,
                0,
                (i >> 16) as u16,
                (i & 0xffff) as u16,
                0x3333,
                0x4444,
            );
            let underlay =
                map_external_to_underlay_ip_impl(prefix, IpAddr::V6(ipv6))
                    .unwrap();
            if let IpAddr::V6(u6) = underlay {
                assert!(prefix.contains(&u6));
                set.insert(u6);
            } else {
                panic!("expected IPv6 underlay");
            }
        }
        assert_eq!(set.len(), 100_000);
    }

    #[test]
    fn test_prefix_validation_success_larger_prefix() {
        // Test that a larger prefix (e.g., /48) works correctly
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let prefix: Ipv6Net = "ff04::/48".parse().unwrap();
        let result = map_external_to_underlay_ip_impl(prefix, IpAddr::V4(ipv4));

        assert!(result.is_ok());
    }

    #[test]
    fn test_xor_folding_with_64bit_prefix() {
        // Test XOR folding with /64 prefix: upper and lower 64-bit halves
        // are XORed together to produce unique mapping
        let ipv6 = Ipv6Addr::new(
            0xff0e, 0x1234, 0x5678, 0x9abc, 0x7ef0, 0x1122, 0x3344, 0x5566,
        );
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let result =
            map_external_to_underlay_ip_impl(prefix, IpAddr::V6(ipv6)).unwrap();

        match result {
            IpAddr::V6(underlay) => {
                // Expected: XOR of upper 64 bits (ff0e:1234:5678:9abc) and
                // lower 64 bits (7ef0:1122:3344:5566) = 81fe:0316:653c:cfda
                let segments = underlay.segments();
                assert_eq!(segments[0], IPV6_ADMIN_SCOPED_MULTICAST_PREFIX);
                assert_eq!(segments[1], 0x0000);
                assert_eq!(segments[2], 0x0000);
                assert_eq!(segments[3], 0x0000);
                assert_eq!(segments[4], 0x81fe);
                assert_eq!(segments[5], 0x0316);
                assert_eq!(segments[6], 0x653c);
                assert_eq!(segments[7], 0xcfda);
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_bounded_preservation_prefix_48() {
        // Test XOR folding with /48 prefix (not used in production):
        // XORs upper 80 bits with lower 80 bits.
        let ipv6 = Ipv6Addr::new(
            0xff0e, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1122, 0x3344, 0x5566,
        );
        let prefix: Ipv6Net = "ff04:1000::/48".parse().unwrap();
        let result =
            map_external_to_underlay_ip_impl(prefix, IpAddr::V6(ipv6)).unwrap();

        match result {
            IpAddr::V6(underlay) => {
                // XOR result of 80-bit chunks
                assert_eq!(
                    underlay.segments(),
                    [
                        IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                        0x1000,
                        0x0000,
                        0x9abc,
                        0xdef0,
                        0xee2c, // XOR folded
                        0x2170, // XOR folded
                        0x031e, // XOR folded
                    ]
                );
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    #[test]
    fn test_xor_folding_prevents_collisions() {
        // Test that different external addresses with identical lower bits
        // but different upper bits (scopes) map to DIFFERENT underlay addresses.
        // XOR folding mixes upper and lower halves to avoid collisions.
        let ipv6_site = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1122, 0x3344, 0x5566,
        );
        let ipv6_global = Ipv6Addr::new(
            0xff0e, 0xabcd, 0xef00, 0x0123, 0xdef0, 0x1122, 0x3344, 0x5566,
        );

        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let result_site =
            map_external_to_underlay_ip_impl(prefix, IpAddr::V6(ipv6_site))
                .unwrap();
        let result_global =
            map_external_to_underlay_ip_impl(prefix, IpAddr::V6(ipv6_global))
                .unwrap();

        // Should map to DIFFERENT underlay addresses because XOR folding
        // incorporates the different upper 64 bits (including scope)
        assert_ne!(result_site, result_global);
    }

    #[test]
    fn test_admin_scope_xor_folding() {
        // Test that admin-scoped external addresses (ff04::) get XOR folded
        // like any other multicast address, producing unique mappings
        let external = Ipv6Addr::new(
            IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
            0,
            0,
            0,
            0x1234,
            0x5678,
            0x9abc,
            0xdef0,
        );

        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let underlay =
            map_external_to_underlay_ip_impl(prefix, IpAddr::V6(external))
                .unwrap();

        // External and underlay will be different due to XOR folding
        // (upper 64 bits XOR'd with lower 64 bits)
        assert_ne!(IpAddr::V6(external), underlay);

        // Verify XOR result: ff04:0:0:0 XOR 1234:5678:9abc:def0 = ed30:5678:9abc:def0
        if let IpAddr::V6(u) = underlay {
            assert_eq!(
                u.segments(),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0xed30, // ff04 XOR 1234
                    0x5678, // 0000 XOR 5678
                    0x9abc, // 0000 XOR 9abc
                    0xdef0, // 0000 XOR def0
                ]
            );
        } else {
            panic!("Expected IPv6 underlay");
        }
    }
}
