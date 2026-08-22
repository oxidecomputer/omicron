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
//!
//! ## Architecture: RPW and Sagas
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
//! - Switch zone coordination: MRIB route programming through MGD
//!   (mg-lower then derives underlay members from DDM peer subscriptions)
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
//!   those 256 scattered addresses, effectively impossible in 2^64 space
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
//! TODO: Egress (instance → external) is not yet supported. See RFD 488
//! (§sect-external-mcast) for the design.
//!
//! ## Reconciliation Components
//!
//! The reconciler handles:
//! - **Group lifecycle**: "Creating" → "Active" → "Deleting" → hard-deleted
//! - **Member lifecycle**: "Joining" → "Joined" → "Left" → soft-deleted → hard-deleted
//! - **Dataplane updates**: DPD API calls for P4 table updates
//! - **MRIB programming**: multicast routing entries written through
//!   MGD, diffed against a per-pass snapshot and withdrawn when no
//!   "Joined" members remain so DDM peers stop sending traffic
//! - **Sled propagation**: M2P mappings and forwarding entries pushed to sled-agents
//! - **OPTE subscriptions**: Per-instance multicast group subscriptions
//!   on target sleds (keyed at the sled by the active VMM's propolis-id)
//!
//! ## RPW Saga Coordination
//!
//! The reconciler launches sagas for transactional operations
//! (e.g. external+underlay group ensure). By default sagas retry
//! independently and the next reconciler tick observes the resulting
//! state.
//!
//! For group creation, the reconciler instead drains saga completion
//! within the same pass so [`reconcile_member_states`] and
//! [`reconcile_active_groups`] can converge in one tick. The motivation
//! is operator-visible latency: members see multicast settle within a
//! single reconciler interval of joining, rather than waiting an
//! additional tick for the saga's effects to be observed.
//!
//! This same-pass drain is bounded by the enclosing `buffer_unordered`
//! concurrency (one slot per in-flight saga), so multiple groups still
//! progress in parallel. A saga failure propagates to that group's per-iteration
//! result and is logged. The pass does not abort, and member / active
//! reconciliation still runs for the groups that succeeded.
//!
//! Saga completion is unbounded. Steno retries transient errors
//! indefinitely, so a wedged dependency holds a `buffer_unordered`
//! slot until the saga unwinds. The `group_concurrency_limit` (see
//! [`MulticastGroupReconcilerConfig`] for the default) caps concurrent
//! slots, but a slow saga in the creating phase delays the start of
//! [`reconcile_member_states`] and [`reconcile_active_groups`] for
//! this tick.
//!
//! This mirrors the `saga_run` + drain pattern used by
//! [`instance_reincarnation`] and [`instance_updater`], but interleaves
//! start-and-await per group inside `buffer_unordered` rather than
//! batch-starting then batch-draining like those tasks do.
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
//!   - RPW removes OPTE subscriptions and sled-agent multicast state
//!   - Cleanup task eventually hard-deletes the row
//!
//! [RFC 7346]: https://www.rfc-editor.org/rfc/rfc7346
//! [`UNDERLAY_MULTICAST_SUBNET`]: omicron_common::address::UNDERLAY_MULTICAST_SUBNET
//! [`reconcile_member_states`]: MulticastGroupReconciler::reconcile_member_states
//! [`reconcile_active_groups`]: MulticastGroupReconciler::reconcile_active_groups
//! [`instance_reincarnation`]: crate::app::background::tasks::instance_reincarnation
//! [`instance_updater`]: crate::app::background::tasks::instance_updater
//! [`MulticastGroupReconcilerConfig`]: nexus_config::MulticastGroupReconcilerConfig

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use serde_json::json;
use slog::{error, info};

use nexus_db_model::MulticastGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::MulticastGroupReconcilerStatus;
use omicron_uuid_kinds::MulticastGroupUuid;
use sled_agent_types::early_networking::SwitchSlot;

use crate::app::background::BackgroundTask;
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::multicast::sled::MulticastSledClient;
use crate::app::multicast::switch_zone::MulticastSwitchZoneClient;
use crate::app::saga::StartSaga;

pub(crate) mod groups;
pub(crate) mod members;
mod mrib;

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

/// Background task that reconciles multicast group state with Dendrite
/// configuration using the Saga + RPW hybrid pattern.
pub(crate) struct MulticastGroupReconciler {
    datastore: Arc<DataStore>,
    resolver: Resolver,
    sagas: Arc<dyn StartSaga>,
    /// Maximum number of members to process concurrently per group.
    member_concurrency_limit: NonZeroUsize,
    /// Maximum number of groups to process concurrently.
    group_concurrency_limit: NonZeroUsize,
    /// Grace period before an orphaned "Creating" group with no members is
    /// reaped by the emptiness sweep (`cleanup_empty_groups`).
    orphan_grace_period: chrono::TimeDelta,
    /// Whether multicast functionality is enabled.
    enabled: bool,
    /// Per-pass placement and election drift counters for active groups.
    /// Reset at the start of active-group reconciliation and folded into
    /// the task status afterward.
    drift_counters: groups::ActiveDriftCounters,
    /// The last observed external ingress owner per active group, kept across
    /// passes so a change in owner can be detected and counted as a
    /// re-election.
    ///
    /// This is mutated only between passes (`fold_owner_observations`) and
    /// is in-memory and per-Nexus: it resets on restart (the first pass
    /// re-records silently) and each Nexus counts slot moves independently.
    elected_owners: HashMap<MulticastGroupUuid, SwitchSlot>,
}

impl MulticastGroupReconciler {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        sagas: Arc<dyn StartSaga>,
        enabled: bool,
        group_concurrency_limit: NonZeroUsize,
        member_concurrency_limit: NonZeroUsize,
        orphan_grace_period: chrono::TimeDelta,
    ) -> Self {
        Self {
            datastore,
            resolver,
            sagas,
            member_concurrency_limit,
            group_concurrency_limit,
            orphan_grace_period,
            enabled,
            drift_counters: groups::ActiveDriftCounters::default(),
            elected_owners: HashMap::new(),
        }
    }

    /// Get tag for multicast groups.
    ///
    /// Returns the stored tag which uses the group's UUID to ensure uniqueness
    /// across the group's entire lifecycle. Format: `{uuid}:{multicast_ip}`.
    pub(crate) fn get_multicast_tag(group: &MulticastGroup) -> Option<&str> {
        group.tag.as_deref()
    }
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

        // Per-pass client construction policy:
        //
        // - DPD (dataplane): fail-closed. Required by every step. A
        //   pass without DPD has nothing useful to do.
        // - sled-agent: never fails. The wrapper builds per-sled
        //   clients on demand, so construction is infallible.
        // - MGD MRIB: fail-open. Only three steps are MRIB-coupled
        //   (member states, active reconciliation, deleting
        //   reconciliation). Creating-group reconciliation and the two
        //   cleanup steps run regardless. Subsequent passes retry the
        //   gated steps when MRIB returns.
        //
        // The non-gated cleanup steps never touch the dataplane.
        // `cleanup_empty_groups` only marks "Deleting", and the terminal
        // teardown and row removal live in the gated
        // `reconcile_deleting_groups`. A group therefore cannot vanish
        // from the reconciler's view while its MRIB route still exists.

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

        // Create sled-agent client for OPTE subscriptions and
        // M2P/forwarding propagation.
        let sled_client = MulticastSledClient::new(
            self.datastore.clone(),
            self.resolver.clone(),
        );

        // Create MGD MRIB client for multicast route distribution
        // via DDM. `mg-lower` syncs MRIB changes to DDM automatically.
        //
        // Construction failure (e.g., transient DNS resolution returning
        // no switch zones) skips MRIB-coupled work this pass but lets
        // creating-group and cleanup paths progress. Subsequent passes
        // will retry.
        let switch_zone_client = match MulticastSwitchZoneClient::new(
            self.resolver.clone(),
            opctx.log.clone(),
        )
        .await
        {
            Ok(client) => Some(client),
            Err(e) => {
                let msg =
                    format!("failed to create multicast MRIB client: {e:#}");
                status.errors.push(msg);
                None
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

        // Process member state changes. Underlay dataplane members are
        // programmed by ddmd from DDM peer subscriptions; the reconciler only
        // advances member DB state and manages OPTE subscriptions plus
        // M2P/forwarding propagation via sled-agent. The dataplane client is
        // read-only here, observing the external entry's owner so forwarding
        // stays co-located with it.
        match self
            .reconcile_member_states(opctx, &sled_client, &dataplane_client)
            .await
        {
            Ok(counts) => {
                status.members_processed += counts.processed;
            }
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

        // Reconcile active groups
        if let Some(switch_zone_client) = &switch_zone_client {
            self.drift_counters.reset();
            match self
                .reconcile_active_groups(
                    opctx,
                    &dataplane_client,
                    &sled_client,
                    switch_zone_client,
                )
                .await
            {
                Ok((count, observations)) => {
                    status.groups_verified += count;
                    // Detect owner slot moves against the map recorded on a
                    // previous pass, incrementing `groups_reelected`, before
                    // the drift counters are read into the status below.
                    self.fold_owner_observations(opctx, observations);
                }
                Err(e) => {
                    let msg =
                        format!("failed to reconcile active groups: {e:#}");
                    status.errors.push(msg);
                }
            }
            // Fold drift counters in even on error, since per-group handlers
            // may have observed drift before the pass-level failure.
            status.external_entries_misplaced = self
                .drift_counters
                .entries_misplaced
                .load(std::sync::atomic::Ordering::Relaxed);
            status.groups_reelected = self
                .drift_counters
                .groups_reelected
                .load(std::sync::atomic::Ordering::Relaxed);
        } else {
            status.skipped.push("reconcile_active_groups".to_string());
        }

        // Process deleting groups
        if let Some(switch_zone_client) = &switch_zone_client {
            match self
                .reconcile_deleting_groups(
                    opctx,
                    &dataplane_client,
                    &sled_client,
                    switch_zone_client,
                )
                .await
            {
                Ok(count) => status.groups_deleted += count,
                Err(e) => {
                    let msg =
                        format!("failed to reconcile deleting groups: {e:#}");
                    status.errors.push(msg);
                }
            }
        } else {
            status.skipped.push("reconcile_deleting_groups".to_string());
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
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use crate::app::multicast::{
        map_external_to_underlay_ip, map_external_to_underlay_ip_impl,
    };
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

    /// Test IPv4 multicast mapping to admin-local IPv6 using the default
    /// production prefix (ff04::/64). The IPv4 address fills the lower
    /// 32 bits.
    #[test]
    fn test_map_ipv4_to_underlay_ipv6() {
        let cases = [
            // 224=0xe0, 1=0x01, 2=0x02, 3=0x03
            (Ipv4Addr::new(224, 1, 2, 3), [0xe001, 0x0203]),
            // Minimum IPv4 multicast address
            (Ipv4Addr::new(224, 0, 0, 1), [0xe000, 0x0001]),
            // Maximum IPv4 multicast address
            (Ipv4Addr::new(239, 255, 255, 255), [0xefff, 0xffff]),
            // Canonical doc example: 224.1.1.1 = 0xE0010101 -> ff04::e001:101
            (Ipv4Addr::new(224, 1, 1, 1), [0xe001, 0x0101]),
        ];

        for (input, [seg6, seg7]) in cases {
            match map_external_to_underlay_ip(IpAddr::V4(input), 0) {
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
                            seg6,
                            seg7,
                        ],
                        "{input}"
                    );
                }
                _ => panic!("Expected IPv6 result for {input}"),
            }
        }
    }

    /// Test algorithm with wider /16 prefix (not used in production).
    ///
    /// With /16, the upper 112 bits XOR with the lower 112 bits. Covers
    /// site-local (ff05), global (ff0e), and already admin-local (ff04)
    /// inputs.
    #[test]
    fn test_xor_folding_16bit_prefix() {
        let prefix: Ipv6Net = "ff04::/16".parse().unwrap();
        let cases = [
            (
                "site-local",
                Ipv6Addr::new(
                    0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
                    0x9abc,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x1234,
                    0x5678,
                    0x9abc,
                    0xdef0,
                    0x1234,
                    0x5678,
                    0x65b9, // XOR folded last segment
                ],
            ),
            (
                "global",
                Ipv6Addr::new(
                    0xff0e, 0xabcd, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234,
                    0x5678,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0xabcd,
                    0x1234,
                    0x5678,
                    0x9abc,
                    0xdef0,
                    0x1234,
                    0xa976, // XOR folded last segment
                ],
            ),
            (
                "already admin-local",
                Ipv6Addr::new(
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x1111,
                    0x2222,
                    0x3333,
                    0x4444,
                    0x5555,
                    0x6666,
                    0x7777,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x1111,
                    0x2222,
                    0x3333,
                    0x4444,
                    0x5555,
                    0x6666,
                    0x8873, // XOR folded last segment
                ],
            ),
        ];

        for (name, input, expected) in cases {
            let res = map_external_to_underlay_ip_with_prefix(
                prefix,
                IpAddr::V6(input),
                0,
            )
            .unwrap();
            match res {
                IpAddr::V6(ipv6) => {
                    assert_eq!(ipv6.segments(), expected, "{name} ({input})");
                }
                _ => panic!("Expected IPv6 result for {name} ({input})"),
            }
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

    /// Test XOR folding with production /64 prefix: the upper and lower
    /// 64-bit halves XOR together into the host portion.
    ///
    /// Covers global (ff0e), site-local (ff05), and already admin-local
    /// (ff04) inputs.
    #[test]
    fn test_xor_folding_64bit_prefix() {
        let prefix: Ipv6Net = "ff04::/64".parse().unwrap();
        let cases = [
            // ff0e:1234:5678:9abc XOR 7ef0:1122:3344:5566
            // = 81fe:0316:653c:cfda
            (
                "global",
                Ipv6Addr::new(
                    0xff0e, 0x1234, 0x5678, 0x9abc, 0x7ef0, 0x1122, 0x3344,
                    0x5566,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0x81fe,
                    0x0316,
                    0x653c,
                    0xcfda,
                ],
            ),
            // ff05:1234:5678:9abc XOR def0:1234:5678:9abc = 21f5:0:0:0
            // (only the scope segment differs between the halves)
            (
                "site-local",
                Ipv6Addr::new(
                    0xff05, 0x1234, 0x5678, 0x9abc, 0xdef0, 0x1234, 0x5678,
                    0x9abc,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0x21f5,
                    0x0000,
                    0x0000,
                    0x0000,
                ],
            ),
            // ff04:1111:2222:3333 XOR 4444:5555:6666:7777
            // = bb40:4444:4444:4444
            (
                "already admin-local",
                Ipv6Addr::new(
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x1111,
                    0x2222,
                    0x3333,
                    0x4444,
                    0x5555,
                    0x6666,
                    0x7777,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0xbb40,
                    0x4444,
                    0x4444,
                    0x4444,
                ],
            ),
            // ff04:0:0:0 XOR 1234:5678:9abc:def0 = ed30:5678:9abc:def0
            (
                "admin-local, zero upper segments",
                Ipv6Addr::new(
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0,
                    0,
                    0,
                    0x1234,
                    0x5678,
                    0x9abc,
                    0xdef0,
                ),
                [
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    0x0000,
                    0x0000,
                    0x0000,
                    0xed30,
                    0x5678,
                    0x9abc,
                    0xdef0,
                ],
            ),
        ];

        for (name, input, expected) in cases {
            let res = map_external_to_underlay_ip_with_prefix(
                prefix,
                IpAddr::V6(input),
                0,
            )
            .unwrap();
            match res {
                IpAddr::V6(ipv6) => {
                    assert_eq!(ipv6.segments(), expected, "{name} ({input})");
                }
                _ => panic!("Expected IPv6 result for {name} ({input})"),
            }
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
                assert_eq!(
                    u.segments()[0],
                    IPV6_ADMIN_SCOPED_MULTICAST_PREFIX,
                    "salt {salt} should preserve ff04 prefix"
                );
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

        let results: Vec<IpAddr> = (0u8..16)
            .map(|salt| map_external_to_underlay_ip(IpAddr::V6(external), salt))
            .collect();

        let unique: std::collections::HashSet<_> = results.iter().collect();
        assert_eq!(
            unique.len(),
            16,
            "16 salts should produce 16 unique results"
        );

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
