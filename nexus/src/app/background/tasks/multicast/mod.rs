// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for reconciling multicast group state with Dendrite switch
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
//! - Instance lifecycle events (start/stop/delete)
//! - Implicit group creation when first member joins
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
//! **Underlay Groups** (admin-local IPv6):
//! - Uses ff04::/64 prefix via [`UNDERLAY_MULTICAST_SUBNET`] (a subset of the
//!   admin-local scope ff04::/16 per [RFC 7346])
//! - Internal rack forwarding to guest instances
//! - Mapped 1:1 with external groups via deterministic XOR-fold (see below)
//!
//! ### External → Underlay Address Mapping
//!
//! External multicast addresses are mapped to underlay addresses using
//! a stateless, deterministic XOR-fold plus a stateful salt for collision
//! resolution:
//!
//! ```text
//! IPv4: Embedded in host bits (32 bits fits in 64-bit host space)
//!       224.1.1.1 → ff04::e001:101
//!
//! IPv6: XOR upper and lower 64-bit halves to fit in host bits
//!       ff3e:1234:5678:9abc:def0:1234:5678:9abc
//!       upper_64 XOR lower_64 = 0x21ce_0000_0000_0000 → ff04::21ce:0:0:0
//! ```
//!
//! Without *XOR-fold*, addresses differing only in upper bits collide:
//! `ff05::1234` and `ff08::1234` would both map to `ff04::1234`. XOR ensures all
//! bits contribute: `ff05::1234 → ff04::ff05:0:0:1234`.
//!
//! #### Collision Resolution
//!
//! The *XOR-fold* approach can produce collisions (128→64 bit compression), but
//! they're rare in the 2^64 underlay space. When collisions occur, salt
//! perturbation resolves them:
//!
//! - **Formula**: `underlay(x, salt) := xor_fold(x) ⊕ salt` where salt ∈ [0, 255]
//! - **Bijective**: XOR with distinct salts produces distinct outputs
//!   (since `a ⊕ b = a ⊕ c` implies `b = c`)
//! - **Scattered outputs**: XOR produces non-sequential outputs based on bit patterns:
//!   `0xa ⊕ [0,1,2,3,4,5,6,7]` → `[a, b, 8, 9, e, f, c, d]`
//!   - Unlike linear probing (`h + i`), scattered outputs avoid clustering
//! - **8-bit salt**: 256 unique underlay addresses per external IP
//! - **Resolution**: Exhaustion requires 256 other groups to occupy exactly
//!   those 256 scattered addresses—effectively impossible in 2^64 space
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
//! TODO: Other traffic flows like egress from instances will be documented separately.
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
//! - Last member leaves → state="Deleting" (implicit lifecycle)
//! - RPW cleans up switch config and associated resources
//! - RPW hard-deletes the row (uses `diesel::delete`)
//! - Note: `deallocate_external_multicast_group` (IP pool deallocation) sets
//!   `time_deleted` directly for cleanup
//!
//! **Members** use dual-purpose "Left" state with soft-delete:
//! - Instance stopped: state="Left", time_deleted=NULL
//!   - Can rejoin when instance starts
//!   - RPW can transition back to "Joining" when instance becomes valid
//! - Instance deleted: state="Left", time_deleted=SET (permanent soft-delete)
//!   - Cannot be reactivated (new attach creates new member record)
//!   - RPW removes DPD configuration
//!   - Cleanup task eventually hard-deletes the row
//!
//! [RFC 7346]: https://www.rfc-editor.org/rfc/rfc7346

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use serde_json::json;
use slog::{error, info};
use tokio::sync::RwLock;

use nexus_db_model::MulticastGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::MulticastGroupReconcilerStatus;
use omicron_common::address::UNDERLAY_MULTICAST_SUBNET;
use omicron_uuid_kinds::{CollectionUuid, SledUuid};

use crate::app::background::BackgroundTask;
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::saga::StartSaga;

pub(crate) mod groups;
pub(crate) mod members;

/// Type alias for the sled mapping cache.
type SledMappingCache =
    Arc<RwLock<(Instant, HashMap<SledUuid, Vec<SwitchBackplanePort>>)>>;

/// Type alias for the backplane map cache.
type BackplaneMapCache = Arc<
    RwLock<
        Option<(
            Instant,
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
    /// Entity was deleted during processing by another operation.
    EntityGone,
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

/// Background task that reconciles multicast group state with Dendrite
/// configuration using the Saga + RPW hybrid pattern.
pub(crate) struct MulticastGroupReconciler {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    sagas: Arc<dyn StartSaga>,
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
    /// Last seen inventory collection ID for cache invalidation.
    ///
    /// When the inventory collection ID changes, it indicates topology may have
    /// changed (sled add/remove, inventory updates). The caches are invalidated
    /// when a new collection ID is detected.
    ///
    /// This approach is database-driven and works across all Nexus instances,
    /// unlike in-memory hints that only affect a single Nexus.
    last_seen_collection_id: Option<CollectionUuid>,
}

impl MulticastGroupReconciler {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        sagas: Arc<dyn StartSaga>,
        enabled: bool,
        sled_cache_ttl: Duration,
        backplane_cache_ttl: Duration,
    ) -> Self {
        Self {
            datastore,
            resolver,
            sagas,
            sled_mapping_cache: Arc::new(RwLock::new((
                Instant::now(),
                HashMap::new(),
            ))),
            sled_cache_ttl,
            backplane_map_cache: Arc::new(RwLock::new(None)),
            backplane_cache_ttl,
            member_concurrency_limit: 100,
            group_concurrency_limit: 100,
            enabled,
            last_seen_collection_id: None,
        }
    }

    /// Get tag for multicast groups.
    ///
    /// Returns the stored tag which uses the group's UUID to ensure uniqueness
    /// across the group's entire lifecycle. Format: `{uuid}:{multicast_ip}`.
    pub(crate) fn get_multicast_tag(group: &MulticastGroup) -> Option<&str> {
        group.tag.as_deref()
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
        // Set timestamp to past to force refresh on next check
        *cache = (Instant::now() - self.sled_cache_ttl, cache.1.clone());
    }

    /// Check if inventory collection changed and invalidate caches if so.
    ///
    /// This is database-driven and works across all Nexus instances.
    async fn check_inventory_for_cache_invalidation(
        &mut self,
        opctx: &OpContext,
    ) {
        let latest_id = match self
            .datastore
            .inventory_get_latest_collection_id(opctx)
            .await
        {
            Ok(Some(id)) => id,
            Ok(None) => return,
            Err(e) => {
                error!(
                    opctx.log,
                    "failed to check inventory collection: {e:#}"
                );
                return;
            }
        };

        if self.last_seen_collection_id == Some(latest_id) {
            return;
        }

        // Invalidate caches (skip on first run when just initializing)
        if self.last_seen_collection_id.is_some() {
            info!(
                opctx.log,
                "invalidating multicast caches";
                "new_collection_id" => %latest_id
            );
            self.invalidate_backplane_cache().await;
            self.invalidate_sled_mapping_cache().await;
        }

        self.last_seen_collection_id = Some(latest_id);
    }
}

/// Maps an external multicast address to an underlay address in ff04::/64.
///
/// Maps external addresses into [`UNDERLAY_MULTICAST_SUBNET`] (ff04::/64,
/// a subset of the admin-local scope ff04::/16 per RFC 7346) using XOR-fold. This prefix is static
/// for consistency across racks.
///
/// See [RFC 7346] for IPv6 multicast admin-local scope.
///
/// # Salt Parameter (Collision Avoidance)
///
/// The `salt` enables collision avoidance via XOR perturbation. XOR is bijective:
/// distinct salts produce distinct outputs (since `a ⊕ b = a ⊕ c` implies `b = c`),
/// guaranteeing 256 unique addresses per external IP.
///
/// This is mathematically equivalent to [binary probing] in hash table literature
/// (`h_i(x) := h(x) ⊕ i`), though the domain context differs in that we're mapping
/// into a sparse 2^64 IPv6 address space rather than probing array slots.
///
/// ```text
/// Salt perturbation example (h = 0xa):
/// ┌──────┬─────────┬────────┐
/// │ salt │ h ⊕ salt│ output │
/// ├──────┼─────────┼────────┤
/// │  0   │ 0xa ⊕ 0 │  0xa   │
/// │  1   │ 0xa ⊕ 1 │  0xb   │
/// │  2   │ 0xa ⊕ 2 │  0x8   │
/// │  3   │ 0xa ⊕ 3 │  0x9   │
/// │  4   │ 0xa ⊕ 4 │  0xe   │
/// │  5   │ 0xa ⊕ 5 │  0xf   │
/// │  6   │ 0xa ⊕ 6 │  0xc   │
/// │  7   │ 0xa ⊕ 7 │  0xd   │
/// └──────┴─────────┴────────┘
/// Outputs: [a, b, 8, 9, e, f, c, d] — scattered, not sequential
/// ```
///
/// On collision (i.e., underlay IP already in use), we increment salt and retry.
/// This stores the successful salt with the group for deterministic
/// reconstruction.
///
/// # Implementation
///
/// ```text
/// underlay_ip = ff04:: | ((xor_fold(external_ip) ⊕ salt) & HOST_MASK)
/// ```
///
/// - IPv4: embedded directly (32 bits fits in 64-bit host space)
/// - IPv6: XOR upper and lower 64-bit halves to fold 128→64 bits
/// - Salt ∈ [0, 255]: XORed into host bits for collision retry
///
/// The `& HOST_MASK` guarantees the result stays within ff04::/64, our static
/// underlay subnet.
///
/// [RFC 7346]: https://www.rfc-editor.org/rfc/rfc7346
/// [binary probing]: https://courses.grainger.illinois.edu/CS473/fa2025/notes/05-hashing.pdf
fn map_external_to_underlay_ip(external_ip: IpAddr, salt: u8) -> IpAddr {
    // Derive constants from the default underlay multicast subnet
    const HOST_BITS: u32 = 128 - UNDERLAY_MULTICAST_SUBNET.width() as u32;
    let prefix_base =
        u128::from_be_bytes(UNDERLAY_MULTICAST_SUBNET.addr().octets());

    map_external_to_underlay_ip_impl(prefix_base, HOST_BITS, external_ip, salt)
}

/// Core implementation: maps external multicast IP to underlay IPv6 address.
///
/// Separated for testing purposes.
///
/// Parameters:
/// - `prefix_base`: Network prefix as u128 (e.g., ff04:: → 0xff04_0000_...)
/// - `host_bits`: Number of host bits (e.g., 64 for a /64 prefix)
/// - `external_ip`: The external multicast address to map
/// - `salt`: XOR perturbation for collision avoidance (0-255)
///
/// Returns: The mapped underlay IPv6 address
fn map_external_to_underlay_ip_impl(
    prefix_base: u128,
    host_bits: u32,
    external_ip: IpAddr,
    salt: u8,
) -> IpAddr {
    let host_mask: u128 =
        if host_bits >= 128 { u128::MAX } else { (1u128 << host_bits) - 1 };

    // Derive host value from external IP
    let host_value: u128 = match external_ip {
        IpAddr::V4(ipv4) => {
            // IPv4 (32 bits) fits directly in host space
            u128::from(u32::from_be_bytes(ipv4.octets()))
        }
        IpAddr::V6(ipv6) => {
            // XOR-fold 128 bits → host_bits (upper ^ lower).
            // This ensures different external addresses (even with identical
            // lower bits but different scopes) map to different underlay IPs.
            let full = u128::from_be_bytes(ipv6.octets());
            if host_bits >= 128 {
                full
            } else {
                (full >> host_bits) ^ (full & host_mask)
            }
        }
    };

    // XOR salt for collision avoidance retry, masked to stay in host bits.
    // The salt is applied after folding, ensuring different salts produce
    // different underlay IPs while staying within the prefix.
    let salted = (host_value ^ u128::from(salt)) & host_mask;

    // Combine prefix + host (masking guarantees result stays in prefix)
    let underlay = prefix_base | salted;

    IpAddr::V6(Ipv6Addr::from(underlay.to_be_bytes()))
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

        self.check_inventory_for_cache_invalidation(opctx).await;

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

        // Process member state changes
        match self.reconcile_member_states(opctx, &dataplane_client).await {
            Ok(count) => status.members_processed += count,
            Err(e) => {
                let msg = format!("failed to reconcile member states: {e:#}");
                status.errors.push(msg);
            }
        }

        // Clean up deleted members ("Left" + `time_deleted`)
        // This must happen before `cleanup_empty_groups` so empty checks are accurate.
        match self.cleanup_deleted_members(opctx).await {
            Ok(count) => status.members_deleted += count,
            Err(e) => {
                let msg = format!("failed to cleanup deleted members: {e:#}");
                status.errors.push(msg);
            }
        }

        // Implicitly delete empty groups (groups are automatically deleted when
        // last member leaves)
        // This handles the case where instance deletion causes members to be
        // soft-deleted, and after cleanup, the group becomes empty.
        match self.cleanup_empty_groups(opctx).await {
            Ok(count) => status.empty_groups_marked += count,
            Err(e) => {
                let msg = format!("failed to cleanup empty groups: {e:#}");
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

        // Process deleting groups (DPD cleanup + hard-delete from DB)
        match self.reconcile_deleting_groups(opctx, &dataplane_client).await {
            Ok(count) => status.groups_deleted += count,
            Err(e) => {
                let msg = format!("failed to reconcile deleting groups: {e:#}");
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

    use std::collections::HashSet;
    use std::net::{Ipv4Addr, Ipv6Addr};

    use ipnet::Ipv6Net;
    use omicron_common::address::IPV6_ADMIN_SCOPED_MULTICAST_PREFIX;

    /// Test wrapper that accepts `ipnet::Ipv6Net` prefix for algorithm testing.
    /// Validates input constraints and calls the core `_impl` function.
    fn map_external_to_underlay_ip_with_prefix(
        prefix: Ipv6Net,
        external_ip: IpAddr,
        salt: u8,
    ) -> Result<IpAddr, anyhow::Error> {
        let host_bits = 128u32.saturating_sub(u32::from(prefix.prefix_len()));
        let prefix_base = u128::from_be_bytes(prefix.network().octets());

        // Validate prefix has enough bits for the input type
        if matches!(external_ip, IpAddr::V4(_)) && host_bits < 32 {
            anyhow::bail!(
                "Prefix {prefix} has only {host_bits} host bits; \
                 IPv4 requires at least 32 bits"
            );
        }

        // Call the core implementation (imported via `use super::*`)
        let result = map_external_to_underlay_ip_impl(
            prefix_base,
            host_bits,
            external_ip,
            salt,
        );

        // Validate result is within prefix
        if let IpAddr::V6(underlay_ipv6) = result {
            if !prefix.contains(&underlay_ipv6) {
                anyhow::bail!(
                    "Generated underlay IP {underlay_ipv6} outside prefix {prefix}"
                );
            }
        }

        Ok(result)
    }

    /// Test IPv4 multicast mapping to admin-local IPv6 using default
    /// prefix (ff04::/64). IPv4 fits in lower 32 bits.
    #[test]
    fn test_map_ipv4_to_underlay_ipv6() {
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let res = map_external_to_underlay_ip(IpAddr::V4(ipv4), 0);

        match res {
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

    /// Test minimum IPv4 multicast address using production prefix.
    #[test]
    fn test_map_ipv4_edge_cases() {
        let ipv4_min = Ipv4Addr::new(224, 0, 0, 1);
        let res = map_external_to_underlay_ip(IpAddr::V4(ipv4_min), 0);
        match res {
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

        // Test maximum IPv4 multicast address using production prefix
        let ipv4_max = Ipv4Addr::new(239, 255, 255, 255);
        let res = map_external_to_underlay_ip(IpAddr::V4(ipv4_max), 0);
        match res {
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

    /// Test algorithm with wider /16 prefix (not used in production).
    ///
    /// Tests site-local (ff05::/16) to admin-local (ff04::/16) with XOR folding.
    /// With /16, we XOR upper 112 bits with lower 112 bits.
    #[test]
    fn test_xor_folding_16bit_site_local_to_admin_scoped() {
        let ipv6_site_local = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678, 0x9abc,
        );
        let prefix_16: Ipv6Net = "ff04::/16".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix_16,
            IpAddr::V6(ipv6_site_local),
            0,
        )
        .unwrap();

        match res {
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

    /// Test algorithm with wider /16 prefix.
    ///
    /// Tests global (ff0e::/16) to admin-local (ff04::/16) with XOR folding.
    /// With /16, we XOR upper 112 bits with lower 112 bits.
    #[test]
    fn test_xor_folding_16bit_global_to_admin_scoped() {
        let ipv6_global = Ipv6Addr::new(
            0xff0e, 0xabcd, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
        );
        let prefix_16: Ipv6Net = "ff04::/16".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix_16,
            IpAddr::V6(ipv6_global),
            0,
        )
        .unwrap();

        match res {
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

    /// Test algorithm with wider /16 prefix (not used in production).
    ///
    /// Admin-local multicast (ff04::/16) gets XOR folded like any other address.
    /// With /16, we XOR upper 112 bits with lower 112 bits.
    #[test]
    fn test_xor_folding_16bit_already_admin_scoped() {
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
        let res = map_external_to_underlay_ip_with_prefix(
            prefix_16,
            IpAddr::V6(ipv6_admin),
            0,
        )
        .unwrap();

        match res {
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

    /// Test that a prefix that's too small for IPv4 mapping is rejected.
    ///
    /// ff04::/120 only allows for the last 8 bits to vary, but IPv4 needs 32 bits.
    #[test]
    fn test_prefix_validation_ipv4_too_small() {
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let prefix: Ipv6Net = "ff04::/120".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V4(ipv4),
            0,
        );

        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("has only 8 host bits")
                && err_msg.contains("IPv4 requires at least 32 bits"),
            "Expected IPv4 validation error, got: {err_msg}"
        );
    }

    /// Smoke-test: For /64 (64 host bits), generating mappings for 100k
    /// unique IPv6 external addresses should produce 100k unique underlay
    /// addresses. With /64, we preserve segments 4-7, so vary those.
    #[test]
    fn test_prefix_preservation_hash_space_for_large_sets() {
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
            let underlay = map_external_to_underlay_ip_with_prefix(
                prefix,
                IpAddr::V6(ipv6),
                0,
            )
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

    /// Test that a larger prefix (e.g., /48) works correctly.
    #[test]
    fn test_prefix_validation_success_larger_prefix() {
        let ipv4 = Ipv4Addr::new(224, 1, 2, 3);
        let prefix: Ipv6Net = "ff04::/48".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V4(ipv4),
            0,
        );

        assert!(res.is_ok());
    }

    /// Test XOR folding with /64 prefix: upper and lower 64-bit halves
    /// are XORed together to produce unique mapping.
    #[test]
    fn test_xor_folding_with_64bit_prefix() {
        let ipv6 = Ipv6Addr::new(
            0xff0e, 0x1234, 0x5678, 0x9abc, 0x7ef0, 0x1122, 0x3344, 0x5566,
        );
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6),
            0,
        )
        .unwrap();

        match res {
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

    /// Test site-local (ff05) to admin-local (ff04) with production /64 prefix.
    ///
    /// Compare to test_map_ipv6_multicast_to_admin_scoped which uses /16.
    #[test]
    fn test_xor_folding_64bit_site_local_to_admin_scoped() {
        let ipv6_site_local = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678, 0x9abc,
        );
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6_site_local),
            0,
        )
        .unwrap();

        match res {
            IpAddr::V6(ipv6) => {
                // /64 XOR folds 64-bit upper half (ff05:1234:5678:9abc) with
                // 64-bit lower half (def0:1234:5678:9abc) into host portion.
                // Upper XOR = ff05 ^ def0 = 21f5
                // Remaining = 1234^1234=0, 5678^5678=0, 9abc^9abc=0
                assert_eq!(
                    ipv6.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX
                );
                assert_eq!(ipv6.segments()[1], 0x0000);
                assert_eq!(ipv6.segments()[2], 0x0000);
                assert_eq!(ipv6.segments()[3], 0x0000);
                assert_eq!(ipv6.segments()[4], 0x21f5);
                assert_eq!(ipv6.segments()[5], 0x0000);
                assert_eq!(ipv6.segments()[6], 0x0000);
                assert_eq!(ipv6.segments()[7], 0x0000);
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    /// Test admin-local (ff04) input with production /64 prefix.
    #[test]
    fn test_xor_folding_64bit_already_admin_scoped() {
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
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6_admin),
            0,
        )
        .unwrap();

        match res {
            IpAddr::V6(ipv6) => {
                // /64 XOR folds upper half (ff04:1111:2222:3333) with
                // lower half (4444:5555:6666:7777) into host portion.
                // ff04 ^ 4444 = bb40
                // 1111 ^ 5555 = 4444
                // 2222 ^ 6666 = 4444
                // 3333 ^ 7777 = 4444
                assert_eq!(
                    ipv6.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX
                );
                assert_eq!(ipv6.segments()[1], 0x0000);
                assert_eq!(ipv6.segments()[2], 0x0000);
                assert_eq!(ipv6.segments()[3], 0x0000);
                assert_eq!(ipv6.segments()[4], 0xbb40);
                assert_eq!(ipv6.segments()[5], 0x4444);
                assert_eq!(ipv6.segments()[6], 0x4444);
                assert_eq!(ipv6.segments()[7], 0x4444);
            }
            _ => panic!("Expected IPv6 result"),
        }
    }

    /// Test XOR folding with /48 prefix (not used in production):
    /// XORs upper 80 bits with lower 80 bits.
    #[test]
    fn test_bounded_preservation_prefix_48() {
        let ipv6 = Ipv6Addr::new(
            0xff0e, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1122, 0x3344, 0x5566,
        );
        let prefix: Ipv6Net = "ff04:1000::/48".parse().unwrap();
        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6),
            0,
        )
        .unwrap();

        match res {
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

    /// Test that different external addresses with identical lower bits
    /// but different upper bits (scopes) map to different underlay addresses.
    /// XOR folding mixes upper and lower halves for better distribution.
    #[test]
    fn test_xor_folding_distinguishes_scopes() {
        let ipv6_site = Ipv6Addr::new(
            0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1122, 0x3344, 0x5566,
        );
        let ipv6_global = Ipv6Addr::new(
            0xff0e, 0xabcd, 0xef00, 0x0123, 0xdef0, 0x1122, 0x3344, 0x5566,
        );

        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let res_site = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6_site),
            0,
        )
        .unwrap();
        let res_global = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(ipv6_global),
            0,
        )
        .unwrap();

        assert_ne!(res_site, res_global);
    }

    /// Test that admin-local external addresses (ff04::) get XOR folded
    /// like any other multicast address, producing unique mappings.
    #[test]
    fn test_admin_scope_xor_folding() {
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
        let underlay = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            0,
        )
        .unwrap();

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

    /// Test IPv4 placement in /64: IPv4 address goes in lower 32 bits.
    ///
    /// 224.1.1.1 = 0xE0010101 → ff04::e001:101
    #[test]
    fn test_xor_fold_64bit_ipv4_placement() {
        let ipv4 = Ipv4Addr::new(224, 1, 1, 1);
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let res = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V4(ipv4),
            0,
        )
        .unwrap();

        if let IpAddr::V6(u) = res {
            assert_eq!(
                u.segments(),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0x0000,
                    0x0000,
                    0xe001, // 224.1 = 0xe001
                    0x0101, // 1.1 = 0x0101
                ]
            );
            // Verify canonical form from docs
            assert_eq!(u.to_string(), "ff04::e001:101");
        } else {
            panic!("Expected IPv6 result");
        }
    }

    /// Same external IP with different salts should produce different underlay IPs.
    #[test]
    fn test_xor_fold_salt_changes_output() {
        let external = Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 0x1234);
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let res_salt_0 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            0,
        )
        .unwrap();
        let res_salt_1 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            1,
        )
        .unwrap();
        let res_salt_255 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            255,
        )
        .unwrap();

        // All results should be different
        assert_ne!(res_salt_0, res_salt_1, "salt 0 vs 1 should differ");
        assert_ne!(res_salt_0, res_salt_255, "salt 0 vs 255 should differ");
        assert_ne!(res_salt_1, res_salt_255, "salt 1 vs 255 should differ");
    }

    /// Same inputs should always produce the same output (deterministic).
    #[test]
    fn test_xor_fold_salt_deterministic() {
        let external = Ipv6Addr::new(
            0xff3e, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678, 0x9abc,
        );
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let salt = 42u8;

        let res1 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            salt,
        )
        .unwrap();
        let res2 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            salt,
        )
        .unwrap();
        let res3 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V6(external),
            salt,
        )
        .unwrap();

        assert_eq!(res1, res2);
        assert_eq!(res2, res3);
    }

    /// Regardless of salt value, the result should stay within the admin-local
    /// multicast prefix (ff04::/16 prefix is preserved, host bits vary).
    #[test]
    fn test_xor_fold_salt_stays_within_prefix() {
        let external = Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 0x1234);
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        // Test various salt values including edge cases (salt is u8, range 0-255)
        for salt in [0u8, 1, 127, 128, 255] {
            let res = map_external_to_underlay_ip_with_prefix(
                prefix,
                IpAddr::V6(external),
                salt,
            )
            .unwrap();

            if let IpAddr::V6(u) = res {
                // First segment must be admin-local multicast prefix (ff04)
                assert_eq!(
                    u.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    "salt {salt} should preserve ff04 prefix"
                );
                // Result must be a multicast address
                assert!(
                    u.is_multicast(),
                    "salt {salt} result must be multicast",
                );
            } else {
                panic!("Expected IPv6 result for salt {salt}");
            }
        }
    }

    /// Salt should also affect IPv4 mapping results.
    #[test]
    fn test_xor_fold_salt_ipv4_changes_output() {
        let ipv4 = Ipv4Addr::new(224, 1, 1, 1);
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();

        let res_salt_0 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V4(ipv4),
            0,
        )
        .unwrap();
        let res_salt_1 = map_external_to_underlay_ip_with_prefix(
            prefix,
            IpAddr::V4(ipv4),
            1,
        )
        .unwrap();

        assert_ne!(
            res_salt_0, res_salt_1,
            "IPv4 with different salts should differ"
        );

        // Both should still be valid admin-local multicast
        if let (IpAddr::V6(u0), IpAddr::V6(u1)) = (res_salt_0, res_salt_1) {
            assert_eq!(u0.segments()[0], IPV6_ADMIN_SCOPED_MULTICAST_PREFIX);
            assert_eq!(u1.segments()[0], IPV6_ADMIN_SCOPED_MULTICAST_PREFIX);
        } else {
            panic!("Expected IPv6 results");
        }
    }

    /// Binary probing guarantee: for any external IP, 256 salts produce
    /// 256 unique underlay addresses within UNDERLAY_MULTICAST_SUBNET.
    ///
    /// XOR is bijective: (X ^ a) = (X ^ b) implies a = b. So for a fixed
    /// external IP, salts 0, 1, 2, ..., 255 each map to a distinct underlay
    /// address.
    ///
    /// Collision resolution: if salt=0 collides with another group's
    /// underlay address, we increment salt and retry. Since each salt
    /// produces a unique address, we're guaranteed to find an open slot
    /// within 256 attempts (unless 256+ other groups occupy all our probe
    /// addresses, which requires those groups to have specific XOR-fold
    /// outputs, which is not a practical concern in 2^64 address space).
    #[test]
    fn test_xor_fold_salt_full_coverage_within_8bits() {
        let external = Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 0x1234);

        let results: HashSet<IpAddr> = (0u8..=255)
            .map(|salt| map_external_to_underlay_ip(IpAddr::V6(external), salt))
            .collect();

        assert_eq!(
            results.len(),
            256,
            "256 salts must produce 256 unique addresses (XOR bijective)"
        );
    }

    /// Validates all documented examples from module and function docs.
    ///
    /// Note: Unit test rather than doc test because functions are private.
    #[test]
    fn test_documented_examples() {
        // Case: Scattered output example from function doc
        //   0xa ⊕ [0,1,2,3,4,5,6,7] → [a, b, 8, 9, e, f, c, d]
        let h: u8 = 0xa;
        let expected: Vec<u8> = vec![0xa, 0xb, 0x8, 0x9, 0xe, 0xf, 0xc, 0xd];
        let actual: Vec<u8> = (0u8..8).map(|salt| h ^ salt).collect();
        assert_eq!(
            actual, expected,
            "0xa ⊕ [0..7] should produce [a, b, 8, 9, e, f, c, d]"
        );

        // Case: Bijective property (a ⊕ b = a ⊕ c implies b = c)
        //       Distinct salts produce distinct outputs
        for a in [0u8, 0xa, 0xff] {
            let outputs: std::collections::HashSet<u8> =
                (0u8..=255).map(|salt| a ^ salt).collect();
            assert_eq!(
                outputs.len(),
                256,
                "XOR is bijective: 256 salts → 256 unique outputs"
            );
        }

        // Case: Module doc IPv4 example
        // 224.1.1.1 → ff04::e001:101
        let ipv4_result = map_external_to_underlay_ip(
            IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)),
            0,
        );
        assert_eq!(
            ipv4_result,
            IpAddr::V6("ff04::e001:101".parse().unwrap()),
            "IPv4 224.1.1.1 should map to ff04::e001:101"
        );

        // Case: Module doc IPv6 XOR-fold example
        // ff3e:1234:5678:9abc:def0:1234:5678:9abc → ff04::21ce:0:0:0
        let ipv6_result = map_external_to_underlay_ip(
            IpAddr::V6(
                "ff3e:1234:5678:9abc:def0:1234:5678:9abc".parse().unwrap(),
            ),
            0,
        );
        assert_eq!(
            ipv6_result,
            IpAddr::V6("ff04::21ce:0:0:0".parse().unwrap()),
            "IPv6 XOR-fold: upper_64 XOR lower_64 should give 0x21ce_0000_0000_0000"
        );
    }

    /// Test production function with various IPv6 multicast scopes.
    ///
    /// Verifies XOR-fold produces correct and distinct results for different
    /// scope values (ff05, ff08, ff0e) using the actual ff04::/64 prefix.
    #[test]
    fn test_prod_ipv6_scope_differentiation() {
        // Same group ID (::1234) but different scopes should produce different underlays
        let site_local = map_external_to_underlay_ip(
            IpAddr::V6("ff05::1234".parse().unwrap()),
            0,
        );
        let org_local = map_external_to_underlay_ip(
            IpAddr::V6("ff08::1234".parse().unwrap()),
            0,
        );
        let global = map_external_to_underlay_ip(
            IpAddr::V6("ff0e::1234".parse().unwrap()),
            0,
        );

        // All should be different (XOR-fold incorporates scope bits)
        assert_ne!(site_local, org_local, "ff05 vs ff08 should differ");
        assert_ne!(site_local, global, "ff05 vs ff0e should differ");
        assert_ne!(org_local, global, "ff08 vs ff0e should differ");

        // All should be in ff04::/64
        for (name, addr) in
            [("site", site_local), ("org", org_local), ("global", global)]
        {
            if let IpAddr::V6(v6) = addr {
                assert_eq!(
                    v6.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    "{name} scope should map to ff04::"
                );
            }
        }
    }

    /// Test XOR-fold algorithm for mapping IPv6 external addresses to underlay.
    ///
    /// IPv6 multicast addresses are 128 bits, but underlay uses ff04::/64 (64 host
    /// bits). The XOR-fold compresses 128→64 bits: `upper_64_bits ^ lower_64_bits`.
    ///
    /// This ensures external addresses with different scopes but identical lower
    /// bits (e.g., ff0e::1234 vs ff04::1234) map to different underlay addresses.
    #[test]
    fn test_prod_ipv6_xor_fold_math() {
        // ff04:0:0:0 XOR 0:0:0:1234 = ff04:0:0:1234
        let admin_simple = map_external_to_underlay_ip(
            IpAddr::V6("ff04::1234".parse().unwrap()),
            0,
        );
        assert_eq!(
            admin_simple,
            IpAddr::V6("ff04::ff04:0:0:1234".parse().unwrap()),
            "ff04::1234 XOR-fold: upper ff04:0:0:0 XOR lower 0:0:0:1234"
        );

        // Symmetric XOR: identical upper and lower halves → zero host bits
        let symmetric = map_external_to_underlay_ip(
            IpAddr::V6(
                "ff0e:1234:5678:9abc:ff0e:1234:5678:9abc".parse().unwrap(),
            ),
            0,
        );
        assert_eq!(
            symmetric,
            IpAddr::V6("ff04::".parse().unwrap()),
            "Symmetric address XOR-folds to zero host bits"
        );
    }

    /// Test salt behavior with IPv6 within production /64 prefix.
    ///
    /// Verifies salt produces unique outputs while staying in ff04::/64.
    #[test]
    fn test_prod_ipv6_salt_uniqueness() {
        let external: Ipv6Addr =
            "ff0e:abcd:1234:5678:9abc:def0:1122:3344".parse().unwrap();

        // Collect results for salts 0-15
        let results: Vec<IpAddr> = (0u8..16)
            .map(|salt| map_external_to_underlay_ip(IpAddr::V6(external), salt))
            .collect();

        // All should be unique
        let unique: std::collections::HashSet<_> = results.iter().collect();
        assert_eq!(
            unique.len(),
            16,
            "16 salts should produce 16 unique results"
        );

        // All should be in ff04::/64
        for (i, addr) in results.iter().enumerate() {
            if let IpAddr::V6(v6) = addr {
                assert_eq!(
                    v6.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    "Salt {i} result should be in ff04::/64"
                );
            }
        }
    }
}
