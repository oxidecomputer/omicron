// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Member-specific multicast reconciler functions.
//!
//! This module handles multicast group member lifecycle operations. Members
//! represent endpoints that receive multicast traffic, typically instances
//! running on compute sleds, but potentially other resource types in the
//! future.
//!
//! # RPW Member Processing Model
//!
//! Member management is more complex than group management because members have
//! dynamic lifecycle tied to instance state (start/stop/migrate) and require
//! dataplane updates. The RPW maintains eventual consistency between intended
//! membership (database) and actual forwarding (dataplane configuration).
//!
//! ## 3-State Member Lifecycle
//!
//! - **Joining**: Member created but not yet receiving traffic
//!   - Created by instance lifecycle sagas (create/start)
//!   - Waiting for group activation and sled assignment
//!   - RPW transitions to "Joined" when ready
//!
//! - **Joined**: Member actively receiving multicast traffic
//!   - Dataplane configured via DPD client(s)
//!   - Instance is running and reachable on assigned sled
//!   - RPW responds to sled migrations
//!
//! - **Left**: Member not receiving traffic (temporary or permanent)
//!   - Instance stopping/stopped, failed, or explicitly detached
//!   - time_deleted=NULL: temporary (can rejoin)
//!   - time_deleted=SET: permanent deletion pending
//!
//! Migration note: migration is not treated as leaving. The reconciler removes
//! dataplane membership from the old sled and applies it on the new sled while
//! keeping the member in "Joined" (reconfigures in place).
//!
//! ## Operations Handled
//!
//! - **State transitions**: "Joining" → "Joined" → "Left" with reactivation
//! - **Dataplane updates**: Applying and removing configuration via DPD
//!   client(s) on switches
//! - **M2P/forwarding propagation**: After join, leave, or migration, M2P
//!   mappings and forwarding entries are propagated to all sleds via
//!   sled-agent inline (not deferred to the next reconciliation pass)
//! - **OPTE subscriptions**: Per-instance multicast group filters managed
//!   via sled-agent on the hosting sled (keyed by the active VMM's
//!   propolis ID)
//! - **Sled migration**: Detecting moves and updating dataplane configuration
//!   (no transition to "Left")
//! - **Cleanup**: Removing orphaned switch state for deleted members
//! - **Extensible processing**: Support for different member types (designed for
//!   future extension)
//!
//! ## Separation of Concerns: RPW +/- Sagas
//!
//! **Sagas:**
//! - Instance create/start → member "Joining" state
//! - Instance stop/delete → member "Left" state + time_deleted
//! - Sled assignment updates during instance operations
//! - Database state changes only (no switch operations)
//!
//! **RPW (background):**
//! - Determining switch ports and updating dataplane switches when members join
//! - Handling sled migrations
//! - Instance state monitoring and member state transitions
//! - Cleanup of deleted members from switch state
//!
//! # Member State Transition Matrix
//!
//! The RPW reconciler handles all possible state transitions for multicast group
//! members:
//!
//! ## Valid Instance States for Multicast
//! - **Valid**: Creating, Starting, Running, Rebooting, Migrating, Repairing
//! - **Invalid**: Stopping, Stopped, Failed, Destroyed, NotFound, Error
//!
//! ## State Transitions
//!
//! ### JOINING State Transitions
//! | # | Group State | Instance Valid | Has sled_id | Action | Next State |
//! |---|-------------|----------------|-------------|---------|------------|
//! | 1 | "Creating" | Any | Any | Wait for activation | "Joining" |
//! | 2 | "Active" | Invalid | Any | Clear sled_id → "Left" | "Left" |
//! | 3 | "Active" | Valid | No | Wait for sled assignment | "Joining" |
//! | 4 | "Active" | Valid | Yes | Add to DPD → "Joined" | "Joined" |
//!
//! ### JOINED State Transitions
//! | # | Instance Valid | Sled Changed | Has sled_id | Action | Next State |
//! |---|----------------|--------------|-------------|---------|------------|
//! | 1 | Invalid | Any | Any | Remove DPD + clear sled_id → "Left" | "Left" |
//! | 2 | Valid | Yes | Yes | Remove old + update sled_id + add new | "Joined" |
//! | 3 | Valid | No | Yes | Verify DPD config (idempotent) | "Joined" |
//! | 4 | Valid | N/A | No | Remove DPD → "Left" (edge case) | "Left" |
//!
//! ### LEFT State Transitions
//! | # | time_deleted | Instance Valid | Group State | Action | Next State |
//! |---|--------------|----------------|-------------|---------|------------|
//! | 1 | Set | Any | Any | Cleanup DPD config | NeedsCleanup |
//! | 2 | None | Invalid | Any | No action (stay stopped) | "Left" |
//! | 3 | None | Valid | "Creating" | Wait for activation | "Left" |
//! | 4 | None | Valid | "Active" | Reactivate member | "Joining" |

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::Ipv6Addr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use slog::{debug, info, trace, warn};
use uuid::Uuid;

use dpd_client::types::{BackplaneLink, Direction, LinkId, PortId, Rear};
use nexus_db_model::{
    DbTypedUuid, MulticastGroup, MulticastGroupMember,
    MulticastGroupMemberState, MulticastGroupState, Sled,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::multicast::ops::member_reconcile::{
    ReconcileAction, ReconcileJoiningResult,
};
use nexus_types::deployment::SledFilter;
use nexus_types::identity::{Asset, Resource};
use omicron_common::api::external::{DataPageParams, InstanceState};
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, PropolisUuid, SledKind,
    SledUuid,
};

use super::{MulticastGroupReconciler, StateTransition, SwitchBackplanePort};
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::multicast::sled::MulticastSledClient;
use crate::app::multicast::switch_zone::MulticastSwitchZoneClient;

/// Pre-fetched instance state for multicast reconciliation.
#[derive(Clone, Copy, Debug, Default)]
struct InstanceMulticastState {
    /// Whether the instance is in a state that can receive multicast traffic.
    valid: bool,
    /// Current sled hosting the VMM, if any.
    sled_id: Option<SledUuid>,
}

/// Context shared across member reconciliation operations.
struct MemberReconcileCtx<'a> {
    opctx: &'a OpContext,
    group: &'a MulticastGroup,
    member: &'a MulticastGroupMember,
    instance_states: &'a InstanceStateMap,
    dataplane_client: &'a MulticastDataplaneClient,
    sled_client: &'a MulticastSledClient,
    /// Sled-to-port mapping built once per reconciliation pass and shared
    /// across all members in that pass (sled lookups in this map are O(1)
    /// and never trigger I/O).
    sled_to_ports: &'a HashMap<SledUuid, Vec<SwitchBackplanePort>>,
}

/// Maps instance_id to pre-fetched multicast-relevant state.
type InstanceStateMap = HashMap<Uuid, InstanceMulticastState>;
type MemberPortKey = (PortId, LinkId);

/// Sled-to-port mapping for a single reconciliation pass.
///
/// `sled_to_ports` is the functional data we need. `ddm_inventory_drift` counts
/// sleds whose DDM port mapping diverged from inventory during a pass and is
/// reported for observability and (maybe) future signaling.
///
/// TODO: A future change could use sustained drift to signal an inventory
/// refresh.
struct SledPortMap {
    sled_to_ports: HashMap<SledUuid, Vec<SwitchBackplanePort>>,
    ddm_inventory_drift: usize,
}

impl SledPortMap {
    fn empty() -> Self {
        Self { sled_to_ports: HashMap::new(), ddm_inventory_drift: 0 }
    }
}

/// Outcome of a single [`MulticastGroupReconciler::reconcile_member_states`]
/// pass.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MemberReconcileCounts {
    /// Members whose state advanced this pass (e.g., "Joining" → "Joined",
    /// "Joining" → "Left").
    pub(super) processed: usize,
    /// Number of sleds whose DDM port mapping diverged from inventory.
    /// DDM wins (live state); a non-zero count surfaces inventory lag.
    pub(super) ddm_inventory_drift: usize,
}

/// Backplane port mapping from DPD-client.
/// Maps switch port ID to backplane link configuration.
type BackplaneMap = BTreeMap<PortId, BackplaneLink>;

/// Result of computing the union of member ports across a group.
///
/// Indicates whether all "Joined" members were successfully resolved when
/// computing the port union. Callers should only prune stale ports when
/// the union is `Complete` to avoid disrupting members that failed resolution.
enum MemberPortUnion {
    /// Union is complete: all "Joined" members were successfully resolved.
    Complete(HashSet<MemberPortKey>),
    /// Union is partial: some "Joined" members failed to resolve.
    /// The port set may be incomplete.
    Partial(HashSet<MemberPortKey>),
}

/// Check if a DPD member is a rear/underlay port (instance member).
fn is_rear_underlay_member(
    member: &dpd_client::types::MulticastGroupMember,
) -> bool {
    matches!(member.port_id, PortId::Rear(_))
        && member.direction == Direction::Underlay
}

/// Represents a sled_id update for a multicast group member.
#[derive(Debug, Clone, Copy)]
struct SledIdUpdate {
    old: Option<DbTypedUuid<SledKind>>,
    new: Option<DbTypedUuid<SledKind>>,
}

/// Trait for processing different types of multicast group members.
trait MemberStateProcessor {
    /// Process a member in "Joining" state.
    async fn process_joining(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Joined" state.
    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Left" state.
    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error>;
}

/// Processor for instance-based multicast group members.
struct InstanceMemberProcessor;

impl MemberStateProcessor for InstanceMemberProcessor {
    async fn process_joining(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler.handle_instance_joining(ctx).await
    }

    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler.handle_instance_joined(ctx).await
    }

    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler.handle_instance_left(ctx).await
    }
}

impl MulticastGroupReconciler {
    /// Group states that require member reconciliation processing.
    const RECONCILABLE_STATES: &'static [MulticastGroupState] = &[
        MulticastGroupState::Creating,
        MulticastGroupState::Active,
        MulticastGroupState::Deleting,
    ];

    /// Process member state changes ("Joining"→"Joined"→"Left").
    pub(super) async fn reconcile_member_states(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        switch_zone_client: Option<&MulticastSwitchZoneClient>,
    ) -> Result<MemberReconcileCounts, anyhow::Error> {
        trace!(opctx.log, "reconciling member state changes");

        let mut processed = 0;

        // Get all groups that need member state processing ("Creating" and "Active")
        let groups = self.get_reconcilable_groups(opctx).await?;

        // Build the reconciliation pass sled-to-port mapping once and share
        // it across all members in this pass. Avoids per-member DDM RPCs
        // and per-member inventory queries.
        //
        // A build failure (no DDM peers and no inventory yet) downgrades
        // to an empty map: "Joining" → "Left" for stopped instances is a
        // DB-only CAS that doesn't need a port lookup, so it still
        // converges. Members that do need a port lookup (e.g. "Joining"
        // → "Joined") fail their own processing this pass and retry on
        // the next.
        let SledPortMap { sled_to_ports, ddm_inventory_drift: drift_count } =
            match self
                .build_sled_port_map(
                    opctx,
                    dataplane_client,
                    switch_zone_client,
                )
                .await
            {
                Ok(map) => map,
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to build reconciliation pass sled-to-port \
                         mapping, continuing with empty map";
                        "error" => %e,
                    );
                    SledPortMap::empty()
                }
            };

        for group in groups {
            match self
                .process_group_member_states(
                    opctx,
                    &group,
                    dataplane_client,
                    sled_client,
                    &sled_to_ports,
                )
                .await
            {
                Ok(count) => {
                    processed += count;
                    if count > 0 {
                        debug!(
                            opctx.log,
                            "processed member state changes for group";
                            "group" => ?group,
                            "members_processed" => count
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to process member states for group";
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        debug!(
            opctx.log,
            "member state reconciliation completed";
            "members_processed" => processed,
            "ddm_inventory_drift" => drift_count,
        );

        Ok(MemberReconcileCounts {
            processed,
            ddm_inventory_drift: drift_count,
        })
    }

    /// Process member state changes for a single group.
    async fn process_group_member_states(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        sled_to_ports: &HashMap<SledUuid, Vec<SwitchBackplanePort>>,
    ) -> Result<usize, anyhow::Error> {
        let mut processed = 0;

        // Get members in various states that need processing
        let members = self.get_group_members(opctx, group.id()).await?;

        // Batch-fetch instance states for all members to avoid N+1 queries
        let instance_states =
            Arc::new(self.batch_fetch_instance_states(opctx, &members).await?);

        // Process members concurrently with configurable parallelism
        let member_outcomes = stream::iter(members)
            .map(|member| {
                let instance_states = Arc::clone(&instance_states);
                async move {
                    let ctx = MemberReconcileCtx {
                        opctx,
                        group,
                        member: &member,
                        instance_states: &instance_states,
                        dataplane_client,
                        sled_client,
                        sled_to_ports,
                    };

                    let res = self.process_member_state(&ctx).await;
                    (member, res)
                }
            })
            .buffer_unordered(self.member_concurrency_limit) // Configurable concurrency
            .collect::<Vec<_>>()
            .await;

        // Process results and update counters
        for (member, result) in member_outcomes {
            match result {
                Ok(transition) => match transition {
                    StateTransition::StateChanged
                    | StateTransition::NoChange => {
                        processed += 1;
                        trace!(
                            opctx.log,
                            "processed member state change";
                            "member" => ?member,
                            "group" => ?group,
                            "transition" => ?transition
                        );
                    }
                    StateTransition::NeedsCleanup => {
                        processed += 1;
                        trace!(
                            opctx.log,
                            "member marked for cleanup";
                            "member" => ?member,
                            "group" => ?group
                        );
                    }
                    StateTransition::EntityGone => {
                        trace!(
                            opctx.log,
                            "member deleted during processing";
                            "member" => ?member,
                            "group" => ?group
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to process member state change";
                        "member" => ?member,
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        Ok(processed)
    }

    /// Main dispatch function for processing member state changes.
    ///
    /// Routes to the appropriate handler based on member state.
    async fn process_member_state(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, .. } = *ctx;

        // Check if the parent group has been deleted or is being deleted.
        // If so, delete the member so cleanup can proceed.
        //
        // This should be impossible under normal operation because:
        // 1. Members can only be added to "Creating" or "Active" groups
        // 2. Groups only transition to "Deleting" when there are no active
        //    members (`mark_multicast_group_for_removal_if_no_members`)
        //
        // However, we provide a fallthrough case for robustness.
        if group.time_deleted().is_some()
            || group.state == MulticastGroupState::Deleting
        {
            warn!(
                opctx.log,
                "member found for deleted/deleting group (unexpected state)";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "group_state" => ?group.state,
                "group_time_deleted" => ?group.time_deleted()
            );
            return self
                .delete_member_for_deleted_group(opctx, group, member)
                .await;
        }

        // For now, all members are instance-based, but this is where we'd
        // dispatch to different processors for different member types
        let processor = InstanceMemberProcessor;

        match member.state {
            MulticastGroupMemberState::Joining => {
                processor.process_joining(self, ctx).await
            }
            MulticastGroupMemberState::Joined => {
                processor.process_joined(self, ctx).await
            }
            MulticastGroupMemberState::Left => {
                processor.process_left(self, ctx).await
            }
        }
    }

    /// Delete a member when its parent group has been deleted or is being deleted.
    /// Sets `time_deleted` and transitions to "Left" state for RPW cleanup.
    async fn delete_member_for_deleted_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
    ) -> Result<StateTransition, anyhow::Error> {
        // Skip if member is already deleted
        if member.time_deleted.is_some() {
            trace!(
                opctx.log,
                "member already deleted, no action needed";
                "member_id" => %member.id,
                "group_id" => %group.id()
            );
            return Ok(StateTransition::NoChange);
        }

        // Delete the member (sets `time_deleted`, `state`="Left", and clears `sled_id`)
        self.datastore
            .multicast_group_member_delete_by_id(
                opctx,
                member.id.into_untyped_uuid(),
            )
            .await
            .context("failed to delete member for deleted group")?;

        info!(
            opctx.log,
            "member deleted due to parent group deletion";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_state" => ?group.state,
            "group_time_deleted" => ?group.time_deleted()
        );

        Ok(StateTransition::StateChanged)
    }

    /// Instance-specific handler for members in "Joining" state.
    ///
    /// Validates instance state and attempts to transition the member to "Joined"
    /// when ready. Uses CAS operations for concurrent-safe state updates.
    async fn handle_instance_joining(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let instance_state =
            self.get_instance_state_from_cache(ctx.instance_states, ctx.member);

        let reconcile_res = self
            .execute_joining_reconciliation(
                ctx,
                instance_state.valid,
                instance_state.sled_id,
            )
            .await?;

        self.process_joining_reconcile_result(
            ctx,
            instance_state,
            reconcile_res,
        )
        .await
    }

    /// Extract instance state from pre-fetched cache.
    fn get_instance_state_from_cache(
        &self,
        instance_states: &InstanceStateMap,
        member: &MulticastGroupMember,
    ) -> InstanceMulticastState {
        instance_states.get(&member.parent_id).copied().unwrap_or_default()
    }

    /// Execute the reconciliation CAS operation for a member in "Joining" state.
    async fn execute_joining_reconciliation(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        instance_valid: bool,
        current_sled_id: Option<SledUuid>,
    ) -> Result<ReconcileJoiningResult, anyhow::Error> {
        let current_sled_id_db = current_sled_id.map(|id| id.into());

        self.datastore
            .multicast_group_member_reconcile_joining(
                ctx.opctx,
                MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                InstanceUuid::from_untyped_uuid(ctx.member.parent_id),
                instance_valid,
                current_sled_id_db,
            )
            .await
            .context("failed to reconcile member in 'Joining' state")
    }

    /// Process the result of a "Joining" state reconciliation operation.
    async fn process_joining_reconcile_result(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        instance_state: InstanceMulticastState,
        reconcile_result: ReconcileJoiningResult,
    ) -> Result<StateTransition, anyhow::Error> {
        match reconcile_result.action {
            ReconcileAction::TransitionedToLeft => {
                self.handle_transitioned_to_left(ctx).await
            }

            ReconcileAction::UpdatedSledId { old, new } => {
                self.handle_sled_id_updated(
                    ctx,
                    instance_state,
                    SledIdUpdate { old, new },
                )
                .await
            }

            ReconcileAction::NotFound | ReconcileAction::NoChange => {
                self.handle_no_change_or_not_found(ctx, instance_state).await
            }
        }
    }

    /// Handle the case where a member was transitioned to "Left" state.
    async fn handle_transitioned_to_left(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        info!(
            ctx.opctx.log,
            "multicast member lifecycle transition: 'Joining' → 'Left'";
            "member_id" => %ctx.member.id,
            "instance_id" => %ctx.member.parent_id,
            "group_id" => %ctx.group.id(),
            "group_name" => ctx.group.name().as_str(),
            "group_multicast_ip" => %ctx.group.multicast_ip,
            "reason" => "instance_not_valid_for_multicast_traffic"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Handle the case where a member's sled_id was updated.
    async fn handle_sled_id_updated(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        instance_state: InstanceMulticastState,
        sled_id_update: SledIdUpdate,
    ) -> Result<StateTransition, anyhow::Error> {
        trace!(
            ctx.opctx.log,
            "updated member sled_id, checking if ready to join";
            "member_id" => %ctx.member.id,
            "old_sled_id" => ?sled_id_update.old,
            "new_sled_id" => ?sled_id_update.new,
            "group_state" => ?ctx.group.state,
            "instance_valid" => instance_state.valid
        );

        self.try_complete_join_if_ready(ctx, instance_state).await
    }

    /// Handle the case where no changes were made or member was not found.
    async fn handle_no_change_or_not_found(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        instance_state: InstanceMulticastState,
    ) -> Result<StateTransition, anyhow::Error> {
        // Check if member is already in Joined state
        if ctx.member.state == MulticastGroupMemberState::Joined {
            trace!(
                ctx.opctx.log,
                "member already in 'Joined' state, no action needed";
                "member_id" => %ctx.member.id,
                "group_id" => %ctx.group.id(),
                "group_name" => ctx.group.name().as_str()
            );
            return Ok(StateTransition::NoChange);
        }

        // Try to complete the join if conditions are met
        self.try_complete_join_if_ready(ctx, instance_state).await
    }

    fn is_ready_to_join(
        &self,
        group: &MulticastGroup,
        instance_valid: bool,
    ) -> bool {
        group.state == MulticastGroupState::Active && instance_valid
    }

    async fn try_complete_join_if_ready(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        instance_state: InstanceMulticastState,
    ) -> Result<StateTransition, anyhow::Error> {
        if self.is_ready_to_join(ctx.group, instance_state.valid) {
            let joined = self.complete_instance_member_join(ctx, None).await?;
            if joined {
                Ok(StateTransition::StateChanged)
            } else {
                Ok(StateTransition::NoChange)
            }
        } else {
            trace!(
                ctx.opctx.log,
                "member not ready to join: waiting for next run";
                "member_id" => %ctx.member.id,
                "group_id" => %ctx.group.id(),
                "group_name" => ctx.group.name().as_str(),
                "instance_valid" => instance_state.valid,
                "group_state" => ?ctx.group.state
            );
            Ok(StateTransition::NoChange)
        }
    }

    /// Instance-specific handler for members in "Joined" state.
    async fn handle_instance_joined(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let instance_state = ctx
            .instance_states
            .get(&ctx.member.parent_id)
            .copied()
            .unwrap_or_default();

        match (instance_state.valid, instance_state.sled_id) {
            (false, _) => self.handle_invalid_instance(ctx).await,

            (true, Some(sled_id))
                if ctx.member.sled_id != Some(sled_id.into()) =>
            {
                self.handle_sled_migration(ctx, sled_id).await
            }

            (true, Some(_)) => {
                self.verify_members(ctx).await?;
                trace!(
                    ctx.opctx.log,
                    "member configuration verified, no changes needed";
                    "member_id" => %ctx.member.id,
                    "group_id" => %ctx.group.id()
                );
                Ok(StateTransition::NoChange)
            }

            (true, None) => self.handle_joined_without_sled(ctx).await,
        }
    }

    /// Handle a joined member whose instance became invalid.
    async fn handle_invalid_instance(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, sled_client, .. } = ctx;
        // Remove from dataplane first
        if let Err(e) = self.remove_member_from_dataplane(ctx).await {
            warn!(
                opctx.log,
                "failed to remove member from dataplane, will retry";
                "member_id" => %member.id,
                "error" => ?e
            );
            return Err(e);
        }

        // Unsubscribe the instance from the multicast group before the CAS
        // clears the sled ID. Best-effort since the VMM may already be torn
        // down.
        if let Some(sled_id) = member.sled_id {
            if let Err(e) = sled_client
                .unsubscribe_instance(opctx, group, member, sled_id.into())
                .await
            {
                warn!(
                    opctx.log,
                    "failed to unsubscribe instance during instance invalidation";
                    "member_id" => %member.id,
                    "sled_id" => %sled_id,
                    "error" => %e
                );
            }
        }

        // Update database state (atomically set "Left" and clear `sled_id`)
        let updated = self
            .datastore
            .multicast_group_member_to_left_if_current(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member.parent_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .context(
                "failed to conditionally transition member from 'Joined' to 'Left'",
            )?;

        if !updated {
            debug!(
                opctx.log,
                "skipping 'Joined' → 'Left' transition due to concurrent update";
                "member_id" => %member.id,
                "instance_id" => %member.parent_id,
                "group_id" => %group.id()
            );
            return Ok(StateTransition::NoChange);
        }

        // Propagate updated M2P/forwarding to all sleds so the
        // dataplane reflects the member's departure. Best-effort since
        // group reconciliation will converge if this fails.
        if let Err(e) =
            sled_client.propagate_m2p_and_forwarding(opctx, group).await
        {
            warn!(
                opctx.log,
                "failed to propagate M2P/forwarding after member leave";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "error" => %e
            );
        }

        info!(
            opctx.log,
            "multicast member lifecycle transition: 'Joined' → 'Left' (instance invalid)";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_multicast_ip" => %group.multicast_ip,
            "reason" => "instance_no_longer_valid_for_multicast_traffic"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Handle sled migration for a "Joined" member.
    async fn handle_sled_migration(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        new_sled_id: SledUuid,
    ) -> Result<StateTransition, anyhow::Error> {
        info!(
            ctx.opctx.log,
            "detected sled migration for 'Joined' member: re-applying configuration";
            "member_id" => %ctx.member.id,
            "instance_id" => %ctx.member.parent_id,
            "group_id" => %ctx.group.id(),
            "group_name" => ctx.group.name().as_str(),
            "group_multicast_ip" => %ctx.group.multicast_ip,
            "old_sled_id" => ?ctx.member.sled_id,
            "new_sled_id" => %new_sled_id
        );

        // Remove from old sled's dataplane first
        if let Err(e) = self.remove_member_from_dataplane(ctx).await {
            warn!(
                ctx.opctx.log,
                "failed to remove member from old sled, will retry";
                "member_id" => %ctx.member.id,
                "old_sled_id" => ?ctx.member.sled_id,
                "error" => ?e
            );
            return Err(e);
        }

        // Source-sled OPTE cleanup (M2P, forwarding, port subscription)
        // is handled by VMM teardown: remove_propolis_zone ->
        // release_opte_ports -> PortTicket::release_inner, which
        // clears multicast subscriptions along with V2P and firewall
        // rules.
        //
        // This is consistent with all other OPTE state. Nexus
        // never explicitly calls sled-agent for source-sled cleanup
        // after migration.

        // Update `sled_id` in database using CAS
        let updated = self
            .datastore
            .multicast_group_member_update_sled_id_if_current(
                ctx.opctx,
                InstanceUuid::from_untyped_uuid(ctx.member.parent_id),
                ctx.member.sled_id,
                Some(new_sled_id.into()),
            )
            .await
            .context(
                "failed to conditionally update member sled_id for migration",
            )?;

        if !updated {
            debug!(
                ctx.opctx.log,
                "skipping sled_id update after migration due to concurrent change";
                "member_id" => %ctx.member.id,
                "group_id" => %ctx.group.id(),
                "old_sled_id" => ?ctx.member.sled_id,
                "new_sled_id" => %new_sled_id
            );
            return Ok(StateTransition::NoChange);
        }

        // Re-apply configuration on new sled. Pass `new_sled_id` explicitly
        // because the in-memory member struct still has the old sled_id.
        match self.complete_instance_member_join(ctx, Some(new_sled_id)).await {
            Ok(joined) => {
                info!(
                    ctx.opctx.log,
                    "member configuration re-applied after sled migration";
                    "member_id" => %ctx.member.id,
                    "instance_id" => %ctx.member.parent_id,
                    "group_id" => %ctx.group.id(),
                    "group_name" => ctx.group.name().as_str(),
                    "group_multicast_ip" => %ctx.group.multicast_ip,
                    "new_sled_id" => %new_sled_id,
                    "action" => "re_add_member_to_underlay_multicast_group",
                    "joined" => joined
                );
                if joined {
                    Ok(StateTransition::StateChanged)
                } else {
                    Ok(StateTransition::NoChange)
                }
            }
            Err(e) => {
                // Failed to join on new sled. We transition to "Joining" and
                // retry next cycle/run.
                warn!(
                    ctx.opctx.log,
                    "failed to complete join on new sled after migration: transitioning to 'Joining' for retry";
                    "member_id" => %ctx.member.id,
                    "group_id" => %ctx.group.id(),
                    "new_sled_id" => %new_sled_id,
                    "error" => %e
                );

                // TODO: Use DDM as the primary source of truth for sled→port
                // mapping, with inventory as cross-validation.
                //
                // Currently we trust inventory (MGS/SP topology) for sled→port
                // mapping. DDM (maghemite/ddmd) on switches has authoritative
                // knowledge of which sleds are reachable on which ports.
                //
                // Future approach:
                //   1. Query DDM for operational sled→port mapping
                //      // TODO: Add GET /peers endpoint to ddm-admin-client
                //      // returning Map<peer_addr, PeerInfo> where PeerInfo
                //      // includes port/interface field (requires maghemite change)
                //   2. Use DDM mapping as primary source for multicast routing
                //   3. Cross-validate against inventory to detect mismatches
                //   4. On mismatch: invalidate cache, log warning, potentially
                //      trigger inventory reconciliation
                //
                // This catches cases where inventory is stale or a sled moved
                // but inventory hasn't updated yet.

                let updated = self
                    .datastore
                    .multicast_group_member_set_state_if_current(
                        ctx.opctx,
                        MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                        InstanceUuid::from_untyped_uuid(ctx.member.parent_id),
                        MulticastGroupMemberState::Joined,
                        MulticastGroupMemberState::Joining,
                    )
                    .await
                    .context(
                        "failed to transition member to 'Joining' after join failure",
                    )?;

                if updated {
                    info!(
                        ctx.opctx.log,
                        "member transitioned to 'Joining': will retry on next reconciliation run";
                        "member_id" => %ctx.member.id,
                        "group_id" => %ctx.group.id(),
                        "new_sled_id" => %new_sled_id
                    );
                    Ok(StateTransition::StateChanged)
                } else {
                    // Let the next cycle handle it
                    Ok(StateTransition::NoChange)
                }
            }
        }
    }

    /// Handle edge case where a "Joined" member has no sled_id.
    async fn handle_joined_without_sled(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, .. } = ctx;
        warn!(
            opctx.log,
            "'Joined' member has no sled_id: transitioning to 'Left'";
            "member_id" => %member.id,
            "parent_id" => %member.parent_id
        );

        // Remove from dataplane and transition to "Left"
        if let Err(e) = self.remove_member_from_dataplane(ctx).await {
            warn!(
                opctx.log,
                "failed to remove member with no sled_id from dataplane";
                "member_id" => %member.id,
                "error" => ?e
            );
            return Err(e);
        }

        let updated = self
            .datastore
            .multicast_group_member_set_state_if_current(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member.parent_id),
                MulticastGroupMemberState::Joined,
                MulticastGroupMemberState::Left,
            )
            .await
            .context(
                "failed to conditionally transition member with no sled_id to 'Left'",
            )?;

        if !updated {
            debug!(
                opctx.log,
                "skipping 'Joined'→'Left' transition (no sled_id) due to concurrent update";
                "member_id" => %member.id,
                "parent_id" => %member.parent_id,
                "group_id" => %group.id()
            );
            return Ok(StateTransition::NoChange);
        }

        info!(
            opctx.log,
            "multicast member forced to 'Left' state due to missing sled_id";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_multicast_ip" => %group.multicast_ip,
            "action" => "transition_to_left",
            "reason" => "inconsistent_state_sled_id_missing_in_joined_state"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Instance-specific handler for members in "Left" state.
    async fn handle_instance_left(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        let InstanceMulticastState {
            valid: instance_valid,
            sled_id: current_sled_id,
            ..
        } = ctx
            .instance_states
            .get(&ctx.member.parent_id)
            .copied()
            .unwrap_or_default();

        if ctx.member.time_deleted.is_some() {
            self.cleanup_deleted_member(ctx).await?;

            return Ok(StateTransition::NeedsCleanup);
        }

        // Always clean DPD for "Left" members before any other action.
        // This ensures stale DPD state is removed before reactivation attempts.
        // The cleanup is idempotent and handles cases where:
        // - sled_id is None (uses fallback path)
        // - member was already removed from DPD
        if let Err(e) = self.remove_member_from_dataplane(ctx).await {
            warn!(
                ctx.opctx.log,
                "failed to clean up DPD state for 'Left' member (will retry)";
                "member_id" => %ctx.member.id,
                "error" => ?e
            );
        }

        // Unsubscribe the instance's active VMM OPTE port from this multicast
        // group. Best-effort since if the VMM is already gone, there's
        // nothing to unsubscribe (the OPTE port was destroyed with the VMM).
        if let Some(sled_id) = ctx.member.sled_id {
            if let Err(e) = ctx
                .sled_client
                .unsubscribe_instance(
                    ctx.opctx,
                    ctx.group,
                    ctx.member,
                    sled_id.into(),
                )
                .await
            {
                warn!(
                    ctx.opctx.log,
                    "failed to unsubscribe instance from multicast group";
                    "member_id" => %ctx.member.id,
                    "sled_id" => %sled_id,
                    "error" => %e
                );
            }
        }

        if instance_valid && ctx.group.state == MulticastGroupState::Active {
            return self.reactivate_left_member(ctx, current_sled_id).await;
        }

        Ok(StateTransition::NoChange)
    }

    /// Reactivate a member in "Left" state when instance becomes valid again.
    /// Transitions the member back to "Joining" state so it can rejoin the group.
    async fn reactivate_left_member(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        current_sled_id: Option<SledUuid>,
    ) -> Result<StateTransition, anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, .. } = ctx;
        debug!(
            opctx.log,
            "transitioning member from 'Left' to 'Joining': instance became valid and group active";
            "member_id" => %member.id,
            "parent_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str()
        );

        let updated = if let Some(sled_id) = current_sled_id {
            self.datastore
                .multicast_group_member_left_to_joining_if_current(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                    InstanceUuid::from_untyped_uuid(member.parent_id),
                    sled_id.into(),
                )
                .await
                .context(
                    "failed to conditionally transition member from 'Left' to 'Joining' (with sled_id)",
                )?
        } else {
            self.datastore
                .multicast_group_member_set_state_if_current(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                    InstanceUuid::from_untyped_uuid(member.parent_id),
                    MulticastGroupMemberState::Left,
                    MulticastGroupMemberState::Joining,
                )
                .await
                .context(
                    "failed to conditionally transition member from 'Left' to 'Joining'",
                )?
        };

        if !updated {
            debug!(
                opctx.log,
                "skipping Left→Joining transition due to concurrent update";
                "member_id" => %member.id,
                "group_id" => %group.id()
            );
            return Ok(StateTransition::NoChange);
        }

        info!(
            opctx.log,
            "member transitioned to 'Joining' state";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str()
        );
        Ok(StateTransition::StateChanged)
    }

    /// Batch-fetch instance states for multiple members to avoid N+1 queries.
    /// Returns a map of instance_id -> (is_valid_for_multicast, current_sled_id).
    ///
    /// - Batch-fetching all instance records in one query via the datastore
    /// - Batch-fetching all VMM records in one query via the datastore
    /// - Building the result map from the fetched data
    async fn batch_fetch_instance_states(
        &self,
        opctx: &OpContext,
        members: &[MulticastGroupMember],
    ) -> Result<InstanceStateMap, anyhow::Error> {
        let mut state_map = HashMap::new();

        if members.is_empty() {
            return Ok(state_map);
        }

        // Extract unique instance IDs
        let instance_ids: Vec<InstanceUuid> = members
            .iter()
            .map(|m| InstanceUuid::from_untyped_uuid(m.parent_id))
            .collect();

        // Use datastore method to batch-fetch instance and VMM data
        let instance_vmm_data = self
            .datastore
            .instance_and_vmm_batch_fetch(opctx, &instance_ids)
            .await
            .context("failed to batch-fetch instance and VMM data")?;

        // Build the state map from the fetched data
        state_map.extend(members.iter().map(|member| {
            let state = if let Some((instance, vmm_opt)) =
                instance_vmm_data.get(&member.parent_id)
            {
                let valid = matches!(
                    instance.nexus_state.state(),
                    InstanceState::Creating
                        | InstanceState::Starting
                        | InstanceState::Running
                        | InstanceState::Rebooting
                        | InstanceState::Migrating
                        | InstanceState::Repairing
                );

                let sled_id = vmm_opt.as_ref().map(|vmm| {
                    SledUuid::from_untyped_uuid(vmm.sled_id.into_untyped_uuid())
                });

                InstanceMulticastState { valid, sled_id }
            } else {
                InstanceMulticastState::default()
            };

            (member.parent_id, state)
        }));

        debug!(
            opctx.log,
            "batch-fetched instance states for multicast reconciliation";
            "member_count" => members.len(),
            "instances_found" => instance_vmm_data.len()
        );

        Ok(state_map)
    }

    /// Look up an instance's current sled_id and update the member record if
    /// found.
    ///
    /// Returns `None` if the instance has no sled assignment or cannot be found.
    async fn lookup_and_update_member_sled_id(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<Option<DbTypedUuid<SledKind>>, anyhow::Error> {
        let MemberReconcileCtx { opctx, member, .. } = ctx;
        debug!(
            opctx.log,
            "member has no sled_id, attempting to look up instance sled";
            "member" => ?member
        );

        let instance_id = InstanceUuid::from_untyped_uuid(member.parent_id);

        // Try to get instance state
        let instance_state = match self
            .datastore
            .instance_get_state(opctx, &instance_id)
            .await
        {
            Ok(Some(state)) => state,
            Ok(None) => {
                debug!(
                    opctx.log,
                    "instance not found, cannot complete join";
                    "member" => ?member
                );
                return Ok(None);
            }
            Err(e) => {
                warn!(
                    opctx.log,
                    "failed to look up instance state";
                    "member" => ?member,
                    "error" => ?e
                );
                return Err(e.into());
            }
        };

        // Try to get sled_id from VMM
        let current_sled_id = match instance_state.propolis_id {
            Some(propolis_id) => {
                match self
                    .datastore
                    .vmm_fetch(
                        opctx,
                        &PropolisUuid::from_untyped_uuid(propolis_id),
                    )
                    .await
                {
                    Ok(vmm) => Some(SledUuid::from_untyped_uuid(
                        vmm.sled_id.into_untyped_uuid(),
                    )),
                    Err(_) => None,
                }
            }
            None => None,
        };

        match current_sled_id {
            Some(sled_id) => {
                debug!(
                    opctx.log,
                    "found instance sled, updating member record";
                    "member" => ?member,
                    "sled_id" => %sled_id
                );

                // Update the member record with the correct sled_id
                self.datastore
                    .multicast_group_member_update_sled_id(
                        opctx,
                        InstanceUuid::from_untyped_uuid(member.parent_id),
                        Some(sled_id.into()),
                    )
                    .await
                    .context("failed to update member sled_id")?;

                Ok(Some(sled_id.into()))
            }
            None => {
                debug!(
                    opctx.log,
                    "instance has no sled_id, cannot complete join";
                    "member" => ?member
                );
                Ok(None)
            }
        }
    }

    /// Complete a member join by configuring the dataplane and subscribing
    /// the VMM.
    ///
    /// When `sled_id_override` is provided (e.g., during migration), it
    /// is used instead of the potentially stale `member.sled_id`.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the join completed successfully. `Ok(false)` when no
    /// sled was available and the operation was a noop.
    async fn complete_instance_member_join(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        sled_id_override: Option<SledUuid>,
    ) -> Result<bool, anyhow::Error> {
        debug!(
            ctx.opctx.log,
            "completing member join";
            "member" => ?ctx.member,
            "group" => ?ctx.group
        );

        // Use the override if provided, then the member's cached sled_id,
        // then look it up from the instance as a last resort.
        let sled_id: SledUuid = if let Some(id) =
            sled_id_override.or(ctx.member.sled_id.map(Into::into))
        {
            id
        } else if let Some(id) =
            self.lookup_and_update_member_sled_id(ctx).await?
        {
            id.into()
        } else {
            return Ok(false);
        };

        self.add_member_to_dataplane(ctx, sled_id).await?;

        // If the member is already in a "Joined" state (migration path), skip
        // the state transition but still propagate and subscribe. During
        // migration the caller updates the sled ID without changing state,
        // so we must not gate propagation on this CAS.
        if ctx.member.state != MulticastGroupMemberState::Joined {
            let updated = self
                .datastore
                .multicast_group_member_set_state_if_current(
                    ctx.opctx,
                    MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                    InstanceUuid::from_untyped_uuid(ctx.member.parent_id),
                    MulticastGroupMemberState::Joining,
                    MulticastGroupMemberState::Joined,
                )
                .await
                .context(
                    "failed to conditionally transition member to 'Joined' state",
                )?;

            if !updated {
                debug!(
                    ctx.opctx.log,
                    "skipping Joining→Joined transition due to concurrent update";
                    "member_id" => %ctx.member.id,
                    "group_id" => %ctx.group.id()
                );
                // Concurrent update moved the member away from the "Joining"
                // state, so skip propagation and subscribe.
                return Ok(false);
            }
        }

        // Propagate M2P mappings and forwarding entries to all sleds.
        //
        // Athis point, the member is now "Joined" in the database, so propagate
        // includes this sled in forwarding next-hops. If propagation or
        // subscribe fails below, the member remains "Joined" with incomplete
        // sled state. The reconciler's next pass converges via
        // `handle_instance_joined` -> `verify_members`.
        //
        // Propagation failures are best-effort since the reconciler will
        // re-converge all sleds on the next cycle. Subscribe failures
        // below are treated as hard errors because the VMM cannot
        // receive traffic without an OPTE port subscription.
        if let Err(e) = ctx
            .sled_client
            .propagate_m2p_and_forwarding(ctx.opctx, ctx.group)
            .await
        {
            warn!(
                ctx.opctx.log,
                "failed to propagate M2P/forwarding after member join";
                "member_id" => %ctx.member.id,
                "group_id" => %ctx.group.id(),
                "error" => %e
            );
        }

        // Subscribe the instance's active VMM OPTE port last. Propagation
        // above is best-effort, and any sleds that failed will be converged
        // by the reconciler on the next cycle.
        if let Err(e) = ctx
            .sled_client
            .subscribe_instance(ctx.opctx, ctx.group, ctx.member, sled_id)
            .await
        {
            warn!(
                ctx.opctx.log,
                "failed to subscribe instance to multicast group via sled-agent \
                 (will retry next cycle)";
                "member_id" => %ctx.member.id,
                "group_id" => %ctx.group.id(),
                "sled_id" => %sled_id,
                "error" => %e
            );
            return Err(e);
        }

        info!(
            ctx.opctx.log,
            "member join completed";
            "member_id" => %ctx.member.id,
            "group_id" => %ctx.group.id(),
            "sled_id" => %sled_id
        );

        Ok(true)
    }

    /// Apply member dataplane configuration (via DPD-client).
    async fn add_member_to_dataplane(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        sled_id: SledUuid,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx {
            opctx,
            group,
            member,
            dataplane_client,
            sled_to_ports,
            ..
        } = ctx;
        let underlay_group_id = group.underlay_group_id.with_context(|| {
            format!("no underlay group for external group {}", group.id())
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context(
                "failed to fetch underlay group for member configuration",
            )?;

        // Resolve sled to switch port configurations
        let port_configs =
            Self::resolve_sled_to_switch_ports(sled_to_ports, sled_id)
                .context("failed to resolve sled to switch ports")?;

        for port_config in &port_configs {
            let dataplane_member = dpd_client::types::MulticastGroupMember {
                port_id: port_config.port_id.clone(),
                link_id: port_config.link_id,
                direction: port_config.direction,
            };

            dataplane_client
                .add_member(&underlay_group, dataplane_member)
                .await
                .context("failed to apply member configuration via DPD")?;

            debug!(
                opctx.log,
                "member added to DPD";
                "member_id" => %member.id,
                "sled_id" => %sled_id,
                "port_id" => %port_config.port_id
            );
        }

        // TODO: Add uplink (front port) members for egress traffic through to
        // Dendrite.
        //
        // When this is the first instance joining the group, we should also add
        // uplink members with `Direction::External` for multicast egress
        // traffic out of the rack.
        // These uplink members follow a different lifecycle:
        // - Added when first instance joins (check group member count)
        // - Removed when last instance leaves (would be handled in
        //   `remove_member_from_dataplane`)
        //
        // Uplink ports are probably going to be a group-level configuration
        // added by external params.

        info!(
            opctx.log,
            "multicast member configuration applied to switch forwarding tables";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "sled_id" => %sled_id,
            "port_count" => port_configs.len(),
            "dpd_operation" => "add_member_to_underlay_multicast_group"
        );

        Ok(())
    }

    /// Remove member from known port configurations.
    ///
    /// Multicast underlay membership is keyed by (port, link), not by
    /// member: the DPD member table tracks one entry per
    /// (group, port_id, link_id), so multiple members sharing a rear
    /// port collapse to one entry per group.
    ///
    /// Compute the union of active rear ports across other "Joined" members
    /// in the group and skip any port still in use, so that removing one
    /// member does not tear down forwarding for siblings on the same sled.
    async fn remove_from_known_ports(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        sled_id: DbTypedUuid<SledKind>,
        port_configs: &[SwitchBackplanePort],
        underlay_group: &nexus_db_model::UnderlayMulticastGroup,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx {
            opctx,
            member,
            dataplane_client,
            sled_to_ports,
            ..
        } = *ctx;

        let active_member_ports = match self
            .compute_active_member_ports(
                opctx,
                member.external_group_id,
                sled_to_ports,
                Some(member.id.into_untyped_uuid()),
            )
            .await
        {
            Ok(MemberPortUnion::Complete(ports)) => Some(ports),
            Ok(MemberPortUnion::Partial(_)) => {
                // Some other "Joined" members failed to resolve. Skip
                // pruning to avoid withdrawing ports that may still be in
                // use (reconciliation will retry).
                info!(
                    opctx.log,
                    "union incomplete: skipping known-port removal to avoid disrupting unresolved members";
                    "member_id" => %member.id,
                    "sled_id" => %sled_id,
                    "reason" => "some_joined_members_failed_port_resolution"
                );
                return Ok(());
            }
            Err(e) => {
                info!(
                    opctx.log,
                    "failed to compute active member ports: skipping known-port removal";
                    "member_id" => %member.id,
                    "sled_id" => %sled_id,
                    "error" => %e
                );
                return Ok(());
            }
        };

        let (to_retain, to_remove): (Vec<_>, Vec<_>) =
            port_configs.iter().partition(|pc| {
                active_member_ports.as_ref().is_some_and(|active| {
                    active.contains(&(pc.port_id.clone(), pc.link_id))
                })
            });

        for port_config in &to_retain {
            debug!(
                opctx.log,
                "retaining shared rear port still in use by other group members";
                "member_id" => %member.id,
                "port_id" => %port_config.port_id,
                "sled_id" => %sled_id,
            );
        }

        for port_config in &to_remove {
            let dataplane_member = dpd_client::types::MulticastGroupMember {
                port_id: port_config.port_id.clone(),
                link_id: port_config.link_id,
                direction: port_config.direction,
            };

            dataplane_client
                .remove_member(underlay_group, dataplane_member)
                .await
                .context("failed to remove member configuration via DPD")?;

            debug!(
                opctx.log,
                "member removed from DPD";
                "port_id" => %port_config.port_id,
                "sled_id" => %sled_id,
            );
        }

        let removed = to_remove.len();
        let retained = to_retain.len();

        info!(
            opctx.log,
            "multicast member configuration removed from switch forwarding tables";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "sled_id" => %sled_id,
            "port_count" => port_configs.len(),
            "ports_removed" => removed,
            "ports_retained_shared" => retained,
            "dpd_operation" => "remove_member_from_underlay_multicast_group",
            "reason" => "instance_state_change_or_migration"
        );
        Ok(())
    }

    /// Compute union of active rear/underlay port IDs across all "Joined"
    /// members in a group. Excludes a specific member ID if provided
    /// (useful when removing a member).
    ///
    /// Returns `MemberPortUnion::Complete` if all "Joined" members were
    /// successfully resolved, or `MemberPortUnion::Partial` if some members
    /// failed to resolve.
    async fn compute_active_member_ports(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
        sled_to_ports: &HashMap<SledUuid, Vec<SwitchBackplanePort>>,
        exclude_member_id: Option<Uuid>,
    ) -> Result<MemberPortUnion, anyhow::Error> {
        let group_members = self
            .get_group_members(opctx, group_id)
            .await
            .context("failed to fetch group members for expected port union")?;

        // Filter to joined members, excluding specified member if provided
        let joined_members = group_members
            .into_iter()
            .filter(|mem| {
                exclude_member_id
                    .map_or(true, |id| mem.id.into_untyped_uuid() != id)
            })
            .filter(|mem| mem.state == MulticastGroupMemberState::Joined)
            .collect::<Vec<_>>();

        // Resolve all members to ports, tracking successes and failures
        let member_ports = stream::iter(joined_members)
            .then(|mem| async move {
                // Check for missing sled_id
                let Some(mem_sled_id) = mem.sled_id else {
                    warn!(
                        opctx.log,
                        "joined member missing sled_id: marking union incomplete";
                        "member_id" => %mem.id,
                        "group_id" => %group_id
                    );
                    return None;
                };

                // Attempt to resolve sled to switch ports
                match Self::resolve_sled_to_switch_ports(
                    sled_to_ports,
                    mem_sled_id.into(),
                ) {
                    Ok(ports) => Some((mem, ports)),
                    Err(e) => {
                        warn!(
                            opctx.log,
                            "failed to resolve member ports for union computation";
                            "member_id" => %mem.id,
                            "sled_id" => %mem_sled_id,
                            "error" => %e
                        );
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        // Separate successful resolutions from failures
        let (resolved, failures): (Vec<_>, Vec<_>) =
            member_ports.into_iter().partition(Option::is_some);
        let resolved: Vec<_> = resolved.into_iter().flatten().collect();
        let failure_cnt = failures.len();

        // Extract rear/underlay ports from all successfully resolved members
        let active_member_ports = resolved
            .into_iter()
            .flat_map(|(_, ports)| ports)
            .filter_map(|cfg| {
                let member = dpd_client::types::MulticastGroupMember {
                    port_id: cfg.port_id.clone(),
                    link_id: cfg.link_id,
                    direction: cfg.direction,
                };
                is_rear_underlay_member(&member)
                    .then(|| (cfg.port_id, cfg.link_id))
            })
            .collect::<HashSet<_>>();

        // Return `Complete` or `Partial` based on whether all members resolved
        if failure_cnt == 0 {
            Ok(MemberPortUnion::Complete(active_member_ports))
        } else {
            Ok(MemberPortUnion::Partial(active_member_ports))
        }
    }

    /// Remove member by querying DPD directly when sled info is unavailable.
    /// (Used when `sled_id` unavailable or resolution fails).
    async fn remove_member_fallback(
        &self,
        opctx: &OpContext,
        member: &MulticastGroupMember,
        underlay_group: &nexus_db_model::UnderlayMulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
        sled_to_ports: &HashMap<SledUuid, Vec<SwitchBackplanePort>>,
    ) -> Result<(), anyhow::Error> {
        // Sled resolution failed or no sled_id available (e.g., removed
        // from inventory, or member.sled_id=NULL).
        //
        // We only remove rear/underlay ports to avoid interfering with
        // other member types (i.e., uplink/external members).
        info!(
            opctx.log,
            "using fallback path: querying DPD directly for member removal";
            "member_id" => %member.id,
            "member_sled_id" => ?member.sled_id,
            "reason" => "sled_id_unavailable_or_resolution_failed"
        );

        let current_members = dataplane_client
            .fetch_underlay_members(underlay_group.multicast_ip.ip())
            .await
            .context("failed to fetch DPD state for member removal")?;

        // Compute union of active member ports across all currently
        // "Joined" members for this group. We will only remove ports that are
        // not required by any active member.
        //
        // We exclude the current member from the union since we're removing it.
        let active_member_ports = match self
            .compute_active_member_ports(
                opctx,
                member.external_group_id,
                sled_to_ports,
                Some(member.id.into_untyped_uuid()),
            )
            .await
        {
            Ok(MemberPortUnion::Complete(ports)) => ports,
            Ok(MemberPortUnion::Partial(_ports)) => {
                // Union is partial (some members failed resolution)
                // Skip pruning to avoid removing ports that may still be needed
                info!(
                    opctx.log,
                    "union incomplete: skipping stale port removal to avoid disrupting unresolved members";
                    "member_id" => %member.id,
                    "reason" => "some_joined_members_failed_port_resolution"
                );
                return Ok(());
            }
            Err(e) => {
                // Failed to compute union (avoid removing anything)
                info!(
                    opctx.log,
                    "failed to compute active member ports for fallback removal: skipping cleanup";
                    "member_id" => %member.id,
                    "error" => %e
                );
                return Ok(());
            }
        };

        if let Some(members) = current_members {
            for current_member in &members {
                // Only consider rear/underlay ports (instance members)
                if !is_rear_underlay_member(current_member) {
                    continue;
                }

                // Remove only if not in union of active member ports
                let member_key: MemberPortKey =
                    (current_member.port_id.clone(), current_member.link_id);
                if !active_member_ports.contains(&member_key) {
                    dataplane_client
                        .remove_member(underlay_group, current_member.clone())
                        .await
                        .context(
                            "failed to remove member from DPD (fallback)",
                        )?;

                    info!(
                        opctx.log,
                        "removed stale rear/underlay member via fallback";
                        "member_id" => %member.id,
                        "port_id" => %current_member.port_id
                    );
                }
            }
        }
        Ok(())
    }

    /// Remove member dataplane configuration (via DPD-client).
    async fn remove_member_from_dataplane(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx {
            opctx,
            group,
            member,
            dataplane_client,
            sled_to_ports,
            ..
        } = ctx;

        let underlay_group_id = group.underlay_group_id.with_context(|| {
            format!(
                "no underlay group for external group {}",
                member.external_group_id
            )
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group for member removal")?;

        // Try to remove via known ports if we have a `sled_id` and can resolve it
        if let Some(sled_id) = member.sled_id {
            if let Ok(port_configs) = Self::resolve_sled_to_switch_ports(
                sled_to_ports,
                sled_id.into(),
            ) {
                self.remove_from_known_ports(
                    ctx,
                    sled_id,
                    &port_configs,
                    &underlay_group,
                )
                .await?;
                return Ok(());
            }
        }

        // Fallback: query DPD directly when `sled_id` unavailable or
        // resolution fails
        self.remove_member_fallback(
            opctx,
            member,
            &underlay_group,
            dataplane_client,
            sled_to_ports,
        )
        .await?;

        Ok(())
    }

    /// Clean up member dataplane configuration with strict error handling.
    /// Ensures dataplane consistency by failing if removal operations fail.
    async fn cleanup_member_from_dataplane(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, .. } = ctx;
        debug!(
            opctx.log,
            "cleaning up member from dataplane";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "parent_id" => %member.parent_id,
            "time_deleted" => ?member.time_deleted
        );

        // Strict removal from dataplane (fail on errors)
        self.remove_member_from_dataplane(ctx).await.context(
            "failed to remove member configuration via DPD during cleanup",
        )?;

        info!(
            opctx.log,
            "member cleaned up from dataplane";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str()
        );
        Ok(())
    }

    /// Verify that a "Joined" member is consistent with dataplane configuration.
    ///
    /// This function ensures the member is on the correct switch ports by:
    /// - Fetching current DPD state to see what ports the member is actually on
    /// - Computing expected ports from a refreshed cache
    /// - Removing the member from any unexpected/stale rear ports
    /// - Adding the member to expected ports
    ///
    /// If the sled cannot be resolved (e.g., decommissioned), the member
    /// is transitioned to "Left" and M2P/forwarding is propagated inline
    /// to remove stale entries.
    ///
    /// This handles cases like `sp_slot` changes where the sled's physical
    /// location changed but the `sled_id` stayed the same.
    async fn verify_members(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx {
            opctx,
            group,
            member,
            dataplane_client,
            sled_client,
            sled_to_ports,
            ..
        } = ctx;
        debug!(
            opctx.log,
            "verifying joined member consistency";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str()
        );

        // Get sled_id from member
        let sled_id = match member.sled_id {
            Some(id) => id,
            None => {
                debug!(opctx.log,
                    "member has no sled_id, skipping verification";
                    "member_id" => %member.id
                );
                return Ok(());
            }
        };

        // Get underlay group
        let underlay_group_id = group.underlay_group_id.with_context(|| {
            format!("no underlay group for external group {}", group.id())
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group")?;

        // Resolve expected member configurations from the reconciliation
        // pass map.
        let expected_port_configs = match Self::resolve_sled_to_switch_ports(
            sled_to_ports,
            sled_id.into(),
        ) {
            Ok(configs) => configs,
            Err(e) => {
                // If we can't resolve the sled anymore (e.g., removed from inventory),
                // remove from dataplane and transition to "Left"
                warn!(
                    opctx.log,
                    "failed to resolve sled to switch ports: removing from dataplane";
                    "member_id" => %member.id,
                    "sled_id" => %sled_id,
                    "error" => %e
                );

                // Best effort removal on verification
                let _ = self.remove_member_from_dataplane(ctx).await;

                // Unsubscribe the instance before the CAS clears sled_id;
                // otherwise, the OPTE subscription is stranded with no
                // way to identify the sled on later passes. Best-effort
                // since the VMM may already be torn down.
                if let Err(e) = sled_client
                    .unsubscribe_instance(opctx, group, member, sled_id.into())
                    .await
                {
                    warn!(
                        opctx.log,
                        "failed to unsubscribe instance during port resolution failure";
                        "member_id" => %member.id,
                        "sled_id" => %sled_id,
                        "error" => %e
                    );
                }

                let updated = self
                    .datastore
                    .multicast_group_member_to_left_if_current(
                        opctx,
                        MulticastGroupUuid::from_untyped_uuid(group.id()),
                        InstanceUuid::from_untyped_uuid(member.parent_id),
                        MulticastGroupMemberState::Joined,
                    )
                    .await
                    .context("failed to transition member to 'Left' after port resolution failure")?;

                if updated {
                    // Propagate updated M2P/forwarding to remove
                    // stale entries for this now-Left member.
                    if let Err(e) = sled_client
                        .propagate_m2p_and_forwarding(opctx, group)
                        .await
                    {
                        warn!(
                            opctx.log,
                            "failed to propagate M2P/forwarding after \
                             member left due to unresolvable sled";
                            "member_id" => %member.id,
                            "group_id" => %group.id(),
                            "error" => %e
                        );
                    }
                    info!(
                        opctx.log,
                        "member transitioned to 'Left': sled no longer resolvable";
                        "member_id" => %member.id,
                        "group_id" => %group.id()
                    );
                }
                return Ok(());
            }
        };

        // Fetch current DPD state to identify stale ports
        // We fetch from one switch since all should be consistent
        let current_dpd_members = dataplane_client
            .fetch_underlay_members(underlay_group.multicast_ip.ip())
            .await
            .context(
                "failed to fetch current underlay group members from DPD",
            )?;

        // Build union of active member ports across all currently
        // joined members for this group. This avoids removing ports needed by
        // other members while verifying a single member.
        let active_member_ports = match self
            .compute_active_member_ports(
                opctx,
                group.id(),
                sled_to_ports,
                None, // Don't exclude any member
            )
            .await
        {
            Ok(MemberPortUnion::Complete(ports)) => Some(ports),
            Ok(MemberPortUnion::Partial(_ports)) => {
                // Union is partial (skip stale port removal)
                info!(
                    opctx.log,
                    "union incomplete: skipping stale port removal to avoid disrupting unresolved members";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "reason" => "some_joined_members_failed_port_resolution"
                );
                None
            }
            Err(e) => {
                // Failed to compute union (skip stale port removal)
                info!(
                    opctx.log,
                    "failed to compute active member ports for verification: skipping stale port removal";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "error" => %e
                );
                None
            }
        };

        // Only prune stale ports if we successfully resolved All "Joined" members.
        // If we could not compute active member ports or if some members failed
        // to resolve, avoid removing anything to prevent disrupting other members.
        // We'll still proceed to ensure adding expected ports for this member.
        let mut stale_ports = Vec::new();
        if let Some(ref active_ports) = active_member_ports {
            if let Some(current_members) = &current_dpd_members {
                for current_member in current_members {
                    // Only consider rear ports with underlay direction
                    if !is_rear_underlay_member(current_member) {
                        continue;
                    }

                    // If this port is not in our active member set, it's stale
                    let member_key: MemberPortKey = (
                        current_member.port_id.clone(),
                        current_member.link_id,
                    );
                    if !active_ports.contains(&member_key) {
                        stale_ports.push(current_member.clone());
                    }
                }
            }
        }

        // Remove stale ports first
        if !stale_ports.is_empty() {
            info!(
                opctx.log,
                "detected member on stale ports: removing before verifying expected ports";
                "member_id" => %member.id,
                "sled_id" => %sled_id,
                "group_id" => %group.id(),
                "stale_port_count" => stale_ports.len(),
                "reason" => "sled_physical_location_changed_or_cache_refresh"
            );

            for stale_member in &stale_ports {
                match dataplane_client
                    .remove_member(&underlay_group, stale_member.clone())
                    .await
                {
                    Ok(()) => {
                        debug!(
                            opctx.log,
                            "removed member from stale port";
                            "member_id" => %member.id,
                            "old_port_id" => %stale_member.port_id,
                            "sled_id" => %sled_id
                        );
                    }
                    Err(e) => {
                        // Continue as the port might have been removed already
                        warn!(
                            opctx.log,
                            "failed to remove member from stale port (may already be gone)";
                            "member_id" => %member.id,
                            "port_id" => %stale_member.port_id,
                            "error" => %e
                        );
                    }
                }
            }
        }

        // Add member to all expected ports
        for port_config in &expected_port_configs {
            let expected_member = dpd_client::types::MulticastGroupMember {
                port_id: port_config.port_id.clone(),
                link_id: port_config.link_id,
                direction: port_config.direction,
            };

            match dataplane_client
                .add_member(&underlay_group, expected_member)
                .await
            {
                Ok(()) => {
                    debug!(
                        opctx.log,
                        "member verified/added to expected port";
                        "member_id" => %member.id,
                        "sled_id" => %sled_id,
                        "port_id" => %port_config.port_id
                    );
                }
                Err(e) => {
                    // Log as warning since we expect this to succeed
                    warn!(
                        opctx.log,
                        "failed to add member to expected port";
                        "member_id" => %member.id,
                        "port_id" => %port_config.port_id,
                        "error" => %e
                    );
                    return Err(e.into());
                }
            }
        }

        // Ensure the instance subscription is in place. Sled-agent resolves
        // the active VMM under its per-instance state lock, which keeps this
        // call correct across live-migration propolis_id changes when the
        // sled_id stays the same. The call is idempotent.
        if let Err(e) = sled_client
            .subscribe_instance(opctx, group, member, sled_id.into())
            .await
        {
            warn!(
                opctx.log,
                "failed to verify instance subscription during member verification";
                "member_id" => %member.id,
                "sled_id" => %sled_id,
                "error" => %e
            );
            return Err(e);
        }

        info!(
            opctx.log,
            "member verification completed";
            "member_id" => %member.id,
            "sled_id" => %sled_id,
            "expected_port_count" => expected_port_configs.len(),
            "stale_ports_removed" => stale_ports.len()
        );

        Ok(())
    }

    /// Cleanup members that are "Left" and time_deleted.
    /// This permanently removes member records that are no longer needed.
    pub async fn cleanup_deleted_members(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, anyhow::Error> {
        trace!(opctx.log, "cleaning up deleted multicast members");

        let deleted_count = self
            .datastore
            .multicast_group_members_complete_delete(opctx)
            .await
            .context("failed to cleanup deleted members")?;

        if deleted_count > 0 {
            info!(
                opctx.log,
                "cleaned up deleted multicast members";
                "members_deleted" => deleted_count
            );
        }

        Ok(deleted_count)
    }

    /// Check for and implicitly delete empty groups.
    ///
    /// With implicit deletion, all multicast groups are deleted when all members
    /// are removed. This function checks "Active" groups for any that have no
    /// active members and marks them for deletion.
    ///
    /// This handles the case where instance deletion causes members to be
    /// soft-deleted (via `multicast_group_members_mark_for_removal`), and after
    /// the member cleanup removes those records, the group becomes empty.
    ///
    /// The underlying datastore method uses an atomic NOT EXISTS guard to
    /// prevent race conditions where a concurrent join could create a member
    /// between the emptiness check and the mark-for-removal.
    pub async fn cleanup_empty_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, anyhow::Error> {
        trace!(
            opctx.log,
            "checking for empty multicast groups to implicitly delete"
        );

        // List all Active groups
        let active_groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Active,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list active groups")?;

        let mut groups_marked = 0;

        for group in active_groups {
            // Atomically mark for deletion only if no members exist.
            // This is race-safe: the NOT EXISTS guard in the datastore method
            // ensures we don't delete a group that just gained a member.
            let marked = self
                .datastore
                .mark_multicast_group_for_removal_if_no_members(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                )
                .await
                .context("failed to check/mark empty group for removal")?;

            if marked {
                info!(
                    opctx.log,
                    "auto-deleting empty multicast group";
                    "group_id" => %group.id(),
                    "group_name" => %group.name()
                );
                groups_marked += 1;
            }
        }

        if groups_marked > 0 {
            info!(
                opctx.log,
                "marked empty multicast groups for deletion";
                "groups_marked" => groups_marked
            );
        }

        Ok(groups_marked)
    }

    /// Get all members for a group.
    async fn get_group_members(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> Result<Vec<MulticastGroupMember>, anyhow::Error> {
        self.datastore
            .multicast_group_members_list_by_id(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id),
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list group members")
    }

    /// Fetch the backplane map from DPD-client with caching.
    ///
    /// The client responds with the entire mapping of all cubbies in a rack.
    ///
    /// The backplane map should remain consistent same across all switches,
    /// so we query one switch and cache the result.
    async fn fetch_backplane_map(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<BackplaneMap, anyhow::Error> {
        {
            let cache = self.backplane_map_cache.read().await;
            if let Some((cached_at, ref map)) = *cache {
                if cached_at.elapsed() < self.backplane_cache_ttl {
                    trace!(
                        opctx.log,
                        "backplane map cache hit";
                        "port_count" => map.len()
                    );
                    return Ok(map.clone());
                }
            }
        }

        debug!(
            opctx.log,
            "fetching backplane map from DPD (cache miss or stale)"
        );

        let backplane_map =
            dataplane_client.fetch_backplane_map().await.context(
                "failed to query backplane_map from DPD via dataplane client",
            )?;

        info!(
            opctx.log,
            "fetched backplane map from DPD";
            "port_count" => backplane_map.len()
        );

        let mut cache = self.backplane_map_cache.write().await;
        *cache = Some((Instant::now(), backplane_map.clone()));

        Ok(backplane_map)
    }

    /// Build the reconciliation pass sled-to-port mapping.
    ///
    /// Tries DDM peer topology first (live, authoritative for reachable
    /// sleds) when a switch-zone client is available. Falls back to
    /// inventory + DPD backplane validation when DDM is unavailable,
    /// returns an empty result, or no switch-zone client could be built
    /// this pass. The returned map is consumed by a single reconciler
    /// pass and dropped afterward, so peer-state churn between passes
    /// resolves on the next tick.
    async fn build_sled_port_map(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
        switch_zone_client: Option<&MulticastSwitchZoneClient>,
    ) -> Result<SledPortMap, anyhow::Error> {
        // Fetch DPD's backplane map once per reconciliation pass. It accounts
        // for the enumeration of valid PortId values (regardless of how
        // a peer's `if_name` ~ interface name ~ is shaped), so we use it to
        // cross-validate parsed DDM peers and to ground the inventory
        // fallback's slot lookups.
        let backplane_map =
            self.fetch_backplane_map(opctx, dataplane_client).await?;

        // List in-service sleds once per reconciliation pass and share with
        // both resolution paths, avoiding duplicate DB queries.
        let sleds = self
            .datastore
            .sled_list_all_batched(opctx, SledFilter::InService)
            .await
            .context("failed to list in-service sleds")?;

        // Prefer DDM: it reflects live peer status (link state, cable
        // up/down). Inventory is a periodic collection snapshot and can
        // lag actual topology. DDM may also be partial (a flapping link
        // can drop a sled out of peers temporarily, or test/sim
        // populates DDM from an earlier inventory snapshot); when it
        // is, fill gaps from inventory rather than treat the partial
        // result as authoritative.
        let mut mappings = match switch_zone_client {
            Some(switch_zone_client) => self
                .fetch_sled_mapping_from_ddm(
                    opctx,
                    switch_zone_client,
                    &backplane_map,
                    &sleds,
                )
                .await
                .unwrap_or_else(|e| {
                    debug!(
                        opctx.log,
                        "DDM peer resolution unavailable, relying on inventory";
                        "error" => %e,
                    );
                    HashMap::new()
                }),
            None => HashMap::new(),
        };
        let mut drift_count = 0usize;

        if mappings.len() < sleds.len() {
            debug!(
                opctx.log,
                "supplementing DDM-derived mapping with inventory fallback";
                "in_service_sleds" => sleds.len(),
                "ddm_mapped_sleds" => mappings.len(),
            );
            // If inventory itself fails, keep whatever DDM gave us.
            // Discarding the partial DDM map on inventory failure would
            // strand all members for this pass when DDM had useful data
            // we could have used. Next pass retries.
            match self
                .fetch_sled_mapping_from_inventory(
                    opctx,
                    dataplane_client,
                    backplane_map,
                    &sleds,
                )
                .await
            {
                Ok(inventory_map) => {
                    // Surface inventory-vs-DDM drift signals before
                    // merging. (a) DDM-only: DDM lists a sled missing
                    // from the latest inventory collection, typical
                    // when inventory hasn't caught up to a
                    // freshly-attached sled. (b) Disagreement: both
                    // have the sled but with different port info; DDM
                    // wins (live state), but the inventory lag is
                    // worth flagging.
                    //
                    // TODO: surface this drift as an observability
                    // signal rather than reconciliation pass logs.
                    for (sled_id, ddm_ports) in &mappings {
                        match inventory_map.get(sled_id) {
                            None => info!(
                                opctx.log,
                                "DDM is ahead of inventory, as sled in DDM peers but not in latest inventory";
                                "sled_id" => %sled_id,
                            ),
                            Some(inv_ports) if inv_ports != ddm_ports => {
                                warn!(
                                    opctx.log,
                                    "DDM and inventory disagree on sled port mapping, preferring DDM";
                                    "sled_id" => %sled_id,
                                );
                                drift_count += 1;
                            }
                            Some(_) => {}
                        }
                    }

                    for (sled_id, ports) in inventory_map {
                        mappings.entry(sled_id).or_insert(ports);
                    }
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "inventory fallback failed, proceeding with partial DDM map";
                        "ddm_mapped_sleds" => mappings.len(),
                        "in_service_sleds" => sleds.len(),
                        "error" => %e,
                    );
                }
            }
        }

        Ok(SledPortMap {
            sled_to_ports: mappings,
            ddm_inventory_drift: drift_count,
        })
    }

    /// Look up switch ports for a sled in the reconciliation pass mapping.
    fn resolve_sled_to_switch_ports(
        sled_to_ports: &HashMap<SledUuid, Vec<SwitchBackplanePort>>,
        sled_id: SledUuid,
    ) -> Result<Vec<SwitchBackplanePort>, anyhow::Error> {
        sled_to_ports.get(&sled_id).cloned().ok_or_else(|| {
            anyhow::Error::msg(format!(
                "sled {sled_id} not found in reconciliation pass sled \
                 mapping (not in DDM peers or inventory)"
            ))
        })
    }

    /// Find SP in inventory for a given sled's baseboard.
    /// Tries exact match (serial + part), then falls back to serial-only.
    fn find_sp_for_sled<'a>(
        &self,
        inventory: &'a nexus_types::inventory::Collection,
        sled: &Sled,
    ) -> Option<&'a nexus_types::inventory::ServiceProcessor> {
        // Try exact match first (serial + part)
        if let Some((_, sp)) = inventory.sps.iter().find(|(bb, _)| {
            bb.serial_number == sled.serial_number()
                && bb.part_number == sled.part_number()
        }) {
            return Some(sp);
        }

        // Fall back to serial-only match
        inventory
            .sps
            .iter()
            .find(|(bb, _)| bb.serial_number == sled.serial_number())
            .map(|(_, sp)| sp)
    }

    /// Map a single sled to switch port(s), validating against backplane map.
    /// Returns Ok(Some(ports)) on success, Ok(None) if validation failed.
    fn map_sled_to_ports(
        &self,
        opctx: &OpContext,
        sled: &Sled,
        sp_slot: u32,
        backplane_map: &BackplaneMap,
    ) -> Result<Option<Vec<SwitchBackplanePort>>, anyhow::Error> {
        let port_id = PortId::Rear(
            Rear::try_from(format!("rear{sp_slot}"))
                .context("invalid rear port number")?,
        );

        // Validate against hardware backplane map
        if !backplane_map.contains_key(&port_id) {
            warn!(
                opctx.log,
                "sled sp_slot validation failed (not in hardware backplane map)";
                "sled_id" => %sled.id(),
                "sp_slot" => sp_slot,
                "expected_port" => %format!("rear{}", sp_slot),
                "reason" => "inventory_sp_slot_out_of_range_for_platform",
                "action" => "skipped_sled_in_mapping_cache"
            );
            return Ok(None);
        }

        debug!(
            opctx.log,
            "mapped sled to rear port via inventory";
            "sled_id" => %sled.id(),
            "sp_slot" => sp_slot,
            "rear_port" => %format!("rear{}", sp_slot)
        );

        Ok(Some(vec![SwitchBackplanePort {
            port_id,
            link_id: LinkId(0),
            direction: Direction::Underlay,
        }]))
    }

    /// Build sled-to-port mappings for all sleds using inventory and backplane data.
    /// Returns (mappings, validation_failures).
    fn build_sled_mappings(
        &self,
        opctx: &OpContext,
        sleds: &[Sled],
        inventory: &nexus_types::inventory::Collection,
        backplane_map: &BackplaneMap,
    ) -> Result<
        (HashMap<SledUuid, Vec<SwitchBackplanePort>>, usize),
        anyhow::Error,
    > {
        sleds.iter().try_fold(
            (HashMap::new(), 0),
            |(mut mappings, mut validation_failures), sled| {
                let Some(sp) = self.find_sp_for_sled(inventory, sled) else {
                    debug!(
                        opctx.log,
                        "no SP data found for sled in current inventory collection";
                        "sled_id" => %sled.id(),
                        "serial_number" => sled.serial_number(),
                        "part_number" => sled.part_number()
                    );
                    return Ok((mappings, validation_failures));
                };

                match self.map_sled_to_ports(
                    opctx,
                    sled,
                    sp.sp_slot.into(),
                    backplane_map,
                )? {
                    Some(ports) => {
                        mappings.insert(sled.id(), ports);
                    }
                    None => {
                        validation_failures += 1;
                    }
                }

                Ok((mappings, validation_failures))
            },
        )
    }

    /// Refresh the sled-to-switch-port mapping cache using inventory data.
    ///
    /// Maps each sled to its physical rear (backplane) port on the switch by:
    /// 1. Getting sled's baseboard serial/part from the sled record
    /// 2. Looking up the service processor (SP) in inventory for that baseboard
    ///    (SP information is collected from MGS by the inventory collector)
    /// 3. Using `sp.sp_slot` (cubby number) to determine the rear port identifier
    /// 4. Creating `PortId::Rear(RearPort::try_from(format!("rear{sp_slot}")))`
    ///
    /// On the Dendrite side (switch's DPD daemon), a similar  mapping is performed:
    ///
    /// ```rust,ignore
    /// // From dendrite/dpd/src/port_map.rs rev_ab_port_map()
    /// for entry in SIDECAR_REV_AB_BACKPLANE_MAP.iter() {
    ///     let port = PortId::Rear(RearPort::try_from(entry.cubby).unwrap());
    ///     inner.insert(port, Connector::QSFP(entry.tofino_connector.into()));
    /// }
    /// ```
    ///
    /// Where `entry.cubby` is the physical cubby/slot number (same as our `sp_slot`),
    /// and this maps it to a `PortId::Rear` that DPD can program on the Tofino ASIC.
    /// Fetch the sled-to-port mapping from DDM peer topology.
    ///
    /// DDM peers provide live sled-to-port mapping via the `if_name`
    /// field (e.g., `"tfportrear0_0"`, `"tfportqsfp0_0"`). More current
    /// than inventory.
    ///
    /// Joins active DDM peers (by IPv6 address) against the in-service
    /// sled list and parses each peer's `tfport<port_id>_<link>`
    /// interface name into a [`SwitchBackplanePort`]. Any DPD port
    /// variant (rear, qsfp, ...) is supported; direction is derived
    /// from the port kind. Parsed `PortId`s are cross-validated against
    /// the DPD backplane map: peers whose port is unknown to DPD are
    /// dropped, so the prefix shape (`tfport`) is just a tokenizer and
    /// correctness rides on DPD's authoritative port enumeration.
    async fn fetch_sled_mapping_from_ddm(
        &self,
        opctx: &OpContext,
        switch_zone_client: &MulticastSwitchZoneClient,
        backplane_map: &BackplaneMap,
        sleds: &[Sled],
    ) -> Result<HashMap<SledUuid, Vec<SwitchBackplanePort>>, anyhow::Error>
    {
        let peers = switch_zone_client
            .get_ddm_peers()
            .await
            .context("failed to get DDM peers")?;

        let addr_to_sled: HashMap<Ipv6Addr, SledUuid> = sleds
            .iter()
            .map(|sled| (sled.ip(), SledUuid::from(sled.id())))
            .collect();

        let mappings: HashMap<SledUuid, Vec<SwitchBackplanePort>> = peers
            .iter()
            .filter(|p| {
                matches!(
                    p.status,
                    omicron_ddm_admin_client::types::PeerStatus::Active
                )
            })
            .filter_map(|p| {
                let if_name = p.if_name.as_ref()?;
                let sled_id = *addr_to_sled.get(&p.addr)?;
                let port = parse_ddm_if_name_to_port(if_name)?;
                if !backplane_map.contains_key(&port.port_id) {
                    debug!(
                        opctx.log,
                        "dropping DDM peer: port_id not in DPD backplane map";
                        "if_name" => %if_name,
                        "port_id" => %port.port_id,
                    );
                    return None;
                }
                Some((sled_id, port))
            })
            .fold(HashMap::new(), |mut acc, (sled_id, port)| {
                acc.entry(sled_id).or_default().push(port);
                acc
            });

        if mappings.is_empty() {
            return Err(anyhow::Error::msg(
                "no sled-to-port mappings resolved from DDM peers",
            ));
        }

        debug!(
            opctx.log,
            "fetched sled mapping from DDM peers";
            "mapped_sleds" => mappings.len(),
        );

        Ok(mappings)
    }

    /// Fetch the sled-to-port mapping from inventory (fallback).
    ///
    /// Used when DDM peer topology is unavailable. Joins the latest
    /// inventory collection's SP records against the in-service sled
    /// list, validating each `sp_slot` against the DPD backplane map
    /// passed in by [`Self::build_sled_port_map`].
    async fn fetch_sled_mapping_from_inventory(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
        mut backplane_map: BackplaneMap,
        sleds: &[Sled],
    ) -> Result<HashMap<SledUuid, Vec<SwitchBackplanePort>>, anyhow::Error>
    {
        let inventory = self
            .datastore
            .inventory_get_latest_collection(opctx)
            .await
            .context("failed to get latest inventory collection")?
            .ok_or_else(|| {
                anyhow::Error::msg("no inventory collection available")
            })?;

        let (mut mappings, mut validation_failures) =
            self.build_sled_mappings(opctx, sleds, &inventory, &backplane_map)?;

        // Validation failures may indicate stale backplane data, so we refresh
        // and retry once before reporting.
        if validation_failures > 0 {
            info!(
                opctx.log,
                "sled validation failures detected: invalidating backplane cache and retrying";
                "validation_failures" => validation_failures
            );

            self.invalidate_backplane_cache().await;

            backplane_map = self
                .fetch_backplane_map(opctx, dataplane_client)
                .await
                .context(
                    "failed to fetch fresh backplane map after invalidation",
                )?;

            (mappings, validation_failures) = self.build_sled_mappings(
                opctx,
                &sleds,
                &inventory,
                &backplane_map,
            )?;

            if validation_failures > 0 {
                warn!(
                    opctx.log,
                    "some sleds still fail validation with fresh backplane map";
                    "validation_failures" => validation_failures
                );
            }
        }

        let sled_count = mappings.len();
        if validation_failures > 0 {
            warn!(
                opctx.log,
                "fetched sled mapping from inventory with validation failures";
                "total_sleds" => sleds.len(),
                "mapped_sleds" => sled_count,
                "validation_failures" => validation_failures
            );
        } else {
            info!(
                opctx.log,
                "fetched sled mapping from inventory";
                "total_sleds" => sleds.len(),
                "mapped_sleds" => sled_count
            );
        }

        Ok(mappings)
    }

    /// Cleanup a member that is marked for deletion (time_deleted set).
    ///
    /// This includes unsubscribing a member from its VMM, removing
    /// it from the dataplane, and hard-deleting the DB row.
    async fn cleanup_deleted_member(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<(), anyhow::Error> {
        let MemberReconcileCtx { opctx, group, member, sled_client, .. } = ctx;
        // Unsubscribe from sled-agent (best-effort, VMM may be gone).
        if let Some(sled_id) = member.sled_id {
            if let Err(e) = sled_client
                .unsubscribe_instance(opctx, group, member, sled_id.into())
                .await
            {
                debug!(
                    opctx.log,
                    "failed to unsubscribe instance during member cleanup";
                    "member_id" => %member.id,
                    "sled_id" => %sled_id,
                    "error" => %e
                );
            }
        }

        // Use the consolidated cleanup helper with strict error handling
        self.cleanup_member_from_dataplane(ctx).await
    }

    /// Get all multicast groups that need member reconciliation.
    /// Returns "Creating", "Active", and "Deleting" groups.
    async fn get_reconcilable_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<MulticastGroup>, anyhow::Error> {
        self.datastore
            .multicast_groups_list_by_states(
                opctx,
                Self::RECONCILABLE_STATES,
                &DataPageParams::max_page(),
            )
            .await
            .context(
                "failed to list multicast groups for member reconciliation",
            )
    }
}

/// Parse a DDM peer interface name (e.g., `"tfportrear0_0"`) into a
/// `SwitchBackplanePort` for sled-bound multicast member programming.
///
/// The DDM peer `if_name` follows `tfport<port_id>_<link>`, where
/// `<port_id>` is a DPD-recognized port name. This parser deliberately
/// rejects any non-rear `PortId`. In production, a sled's only
/// physical path to a switch is the rack backplane.
///
/// TODO: Egress (uplink) members are not yet implemented. When they
/// land, they will come from group-level configuration applied
/// directly via DPD rather than from DDM peer discovery. See the
/// `TODO` in [`MulticastGroupReconciler::add_member_to_dataplane`].
fn parse_ddm_if_name_to_port(if_name: &str) -> Option<SwitchBackplanePort> {
    use std::str::FromStr;

    let stripped = if_name.strip_prefix("tfport")?;
    let (port_str, link_str) = stripped.rsplit_once('_')?;

    let port_id = PortId::from_str(port_str).ok()?;
    let PortId::Rear(_) = port_id else {
        return None;
    };
    let link_id = LinkId(link_str.parse::<u8>().ok()?);

    Some(SwitchBackplanePort {
        port_id,
        link_id,
        direction: Direction::Underlay,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_rear_port() {
        let port = parse_ddm_if_name_to_port("tfportrear0_0").unwrap();
        assert_eq!(
            port.port_id,
            PortId::Rear(Rear::try_from("rear0".to_string()).unwrap())
        );
        assert_eq!(port.link_id, LinkId(0));
        assert_eq!(port.direction, Direction::Underlay);
    }

    #[test]
    fn parse_higher_port_number() {
        let port = parse_ddm_if_name_to_port("tfportrear31_0").unwrap();
        assert_eq!(
            port.port_id,
            PortId::Rear(Rear::try_from("rear31".to_string()).unwrap())
        );
    }

    #[test]
    fn parse_nonzero_link() {
        let port = parse_ddm_if_name_to_port("tfportrear5_2").unwrap();
        assert_eq!(port.link_id, LinkId(2));
    }

    #[test]
    fn parse_non_rear_port_returns_none() {
        // Sleds only attach via rear ports; reject other variants.
        assert!(parse_ddm_if_name_to_port("tfportqsfp0_0").is_none());
    }

    #[test]
    fn parse_invalid_prefix_returns_none() {
        assert!(parse_ddm_if_name_to_port("eth0").is_none());
        assert!(parse_ddm_if_name_to_port("").is_none());
    }

    #[test]
    fn parse_missing_underscore_returns_none() {
        assert!(parse_ddm_if_name_to_port("tfportrear0").is_none());
    }

    #[test]
    fn parse_non_numeric_returns_none() {
        assert!(parse_ddm_if_name_to_port("tfportrearX_0").is_none());
        assert!(parse_ddm_if_name_to_port("tfportrear0_Y").is_none());
    }
}
