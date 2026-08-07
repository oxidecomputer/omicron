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
//! a dynamic lifecycle tied to instance state (start/stop/migrate). The RPW
//! maintains eventual consistency between intended membership (database) and the
//! per-sled state that lets a member's VMM receive traffic.
//!
//! Members do not program switch dataplane membership. Rear-port underlay
//! membership is owned by `ddmd`, which derives it from DDM peer subscriptions
//! and programs it into the switch dataplane via mg-lower/DDM. Group-level
//! switch effects (group creation, source filters) are applied elsewhere by
//! the group reconciler. This module's effects are limited to database state
//! and sled state reached through sled-agent.
//!
//! ## 3-State Member Lifecycle
//!
//! - **Joining**: Member created but not yet receiving traffic
//!   - Created by instance lifecycle sagas (create/start)
//!   - Waiting for group activation and sled assignment
//!   - RPW transitions to "Joined" when ready
//!
//! - **Joined**: Member actively receiving multicast traffic
//!   - VMM's OPTE port subscribed and sled forwarding state propagated
//!   - Instance is running and reachable on assigned sled
//!   - RPW responds to sled migrations
//!
//! - **Left**: Member not receiving traffic (temporary or permanent)
//!   - Instance stopping/stopped, failed, or explicitly detached
//!   - time_deleted=NULL: temporary (can rejoin)
//!   - time_deleted=SET: permanent deletion pending
//!
//! ## Operations Handled
//!
//! - **State transitions**: "Joining" → "Joined" → "Left" with reactivation
//! - **OPTE subscriptions**: The active VMM's OPTE port is subscribed and
//!   unsubscribed via sled-agent on the hosting sled (keyed by the active
//!   VMM's propolis ID)
//! - **M2P/forwarding propagation**: After join, leave, or migration, M2P
//!   mappings and forwarding entries are propagated to sleds via sled-agent
//!   inline (not deferred to the next reconciliation pass)
//! - **Sled migration**: Detecting moves and re-subscribing on the new sled
//!   (no transition to "Left")
//! - **Cleanup**: Unsubscribing deleted members from their VMM
//! - **Extensible processing**: Support for different member types (designed for
//!   future extension)
//!
//! ## Separation of Concerns: RPW and Sagas
//!
//! **Sagas:**
//! - Instance create/start → member "Joining" state
//! - Instance stop/delete → member "Left" state + time_deleted
//! - Sled assignment updates during instance operations
//! - Database state changes only (no sled or switch operations)
//!
//! **RPW (background):**
//! - Subscribing/unsubscribing the active VMM's OPTE port via sled-agent
//! - Propagating M2P/forwarding state to sleds
//! - Handling sled migrations
//! - Instance state monitoring and member state transitions
//! - Cleanup of deleted members
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
//! | 4 | "Active" | Valid | Yes | Subscribe VMM + propagate → "Joined" | "Joined" |
//!
//! ### JOINED State Transitions
//! | # | Instance Valid | Sled Changed | Has sled_id | Action | Next State |
//! |---|----------------|--------------|-------------|---------|------------|
//! | 1 | Invalid | Any | Any | Unsubscribe VMM + clear sled_id → "Left" | "Left" |
//! | 2 | Valid | Yes | Yes | Unsubscribe old + update sled_id + subscribe new | "Joined" |
//! | 3 | Valid | No | Yes | Verify config, no changes needed | "Joined" |
//! | 4 | Valid | N/A | No | DB state only → "Left" (edge case) | "Left" |
//!
//! ### LEFT State Transitions
//! | # | time_deleted | Instance Valid | Group State | Action | Next State |
//! |---|--------------|----------------|-------------|---------|------------|
//! | 1 | Set | Any | Any | Unsubscribe VMM | NeedsCleanup |
//! | 2 | None | Invalid | Any | No action (stay stopped) | "Left" |
//! | 3 | None | Valid | "Creating" | Wait for activation | "Left" |
//! | 4 | None | Valid | "Active" | Reactivate member | "Joining" |

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use slog::{debug, info, trace, warn};
use uuid::Uuid;

use nexus_db_model::{
    DbTypedUuid, MemberParentRef, MulticastGroup, MulticastGroupMember,
    MulticastGroupMemberState, MulticastGroupState,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::multicast::ops::member_reconcile::{
    ReconcileAction, ReconcileJoiningResult,
};
use nexus_types::external_api::instance::InstanceState;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, ProbeUuid, PropolisUuid,
    SledKind, SledUuid,
};

use super::{MulticastGroupReconciler, StateTransition};
use crate::app::multicast::sled::MulticastSledClient;

/// Whether at least `min_age` has elapsed since `since`.
///
/// Gates implicit group deletion behind a grace period. Orphaned "Creating"
/// groups are gated on their creation time so a group whose first member attach
/// is still in flight is not reaped before that member reaches "Joined".
fn grace_period_elapsed(
    since: chrono::DateTime<chrono::Utc>,
    min_age: chrono::TimeDelta,
) -> bool {
    chrono::Utc::now() - since >= min_age
}

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
    sled_client: &'a MulticastSledClient,
}

/// Maps instance_id to pre-fetched multicast-relevant state.
type InstanceStateMap = HashMap<Uuid, InstanceMulticastState>;

/// Outcome of a single [`MulticastGroupReconciler::reconcile_member_states`]
/// pass.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MemberReconcileCounts {
    /// Members whose state advanced this pass (e.g., "Joining" → "Joined",
    /// "Joining" → "Left").
    pub(super) processed: usize,
}

/// Represents a sled_id update for a multicast group member.
#[derive(Debug, Clone, Copy)]
struct SledIdUpdate {
    old: Option<DbTypedUuid<SledKind>>,
    new: Option<DbTypedUuid<SledKind>>,
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
        sled_client: &MulticastSledClient,
    ) -> Result<MemberReconcileCounts, anyhow::Error> {
        trace!(opctx.log, "reconciling member state changes");

        let mut processed = 0;

        // Get all groups that need member state processing ("Creating" and "Active")
        let groups = self.get_reconcilable_groups(opctx).await?;

        for group in groups {
            match self
                .process_group_member_states(opctx, &group, sled_client)
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
        );

        Ok(MemberReconcileCounts { processed })
    }

    /// Process member state changes for a single group.
    async fn process_group_member_states(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        sled_client: &MulticastSledClient,
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
                        sled_client,
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

        // Dispatch on (parent_kind, state). Probe OPTE state is set up at
        // probe-zone provisioning, so the probe path skips the sled-agent
        // RPCs the instance path needs and resolves its sled directly from
        // the probe row. Probes have no update endpoint, as is normal for
        // probes outside multicast, so membership is fixed at create and
        // never mutates after it. The probe path therefore only drives
        // initial setup and delete-time teardown, never a join or leave on
        // a live probe.
        use MulticastGroupMemberState::{Joined, Joining, Left};
        match (member.parent_ref(), member.state) {
            (MemberParentRef::Instance(_), Joining) => {
                self.handle_instance_joining(ctx).await
            }
            (MemberParentRef::Instance(_), Joined) => {
                self.handle_instance_joined(ctx).await
            }
            (MemberParentRef::Instance(_), Left) => {
                self.handle_instance_left(ctx).await
            }
            (MemberParentRef::Probe(id), Joining) => {
                self.handle_probe_joining(ctx, id).await
            }
            (MemberParentRef::Probe(id), Joined) => {
                self.handle_probe_joined(ctx, id).await
            }
            (MemberParentRef::Probe(_), Left) => {
                self.handle_probe_left(ctx).await
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
            .multicast_group_member_reconcile_joining_for_parent(
                ctx.opctx,
                MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    ctx.member.parent_id,
                )),
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
            .multicast_group_member_to_left_if_current_for_parent(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    member.parent_id,
                )),
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

                let updated = self
                    .datastore
                    .multicast_group_member_set_state_if_current_for_parent(
                        ctx.opctx,
                        MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                        MemberParentRef::Instance(
                            InstanceUuid::from_untyped_uuid(ctx.member.parent_id),
                        ),
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

        // Member has no `sled_id`, so there is no VMM OPTE port to unsubscribe
        // and no underlay member to remove (those are owned by `ddmd`). Only the
        // DB state moves to "Left".
        let updated = self
            .datastore
            .multicast_group_member_set_state_if_current_for_parent(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    member.parent_id,
                )),
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

    /// Probe-side "Joining" handler.
    ///
    /// Drives the polymorphic `_for_parent` reconcile-CAS with the probe's
    /// hosting sled read directly from `probe.sled`. The probe's OPTE
    /// multicast subscription is set up at zone provisioning, so there is no
    /// sled-agent RPC here, unlike the instance path.
    async fn handle_probe_joining(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        probe_id: ProbeUuid,
    ) -> Result<StateTransition, anyhow::Error> {
        let probe_state = self.lookup_probe_state(ctx.opctx, probe_id).await?;

        let reconcile_res = self
            .datastore
            .multicast_group_member_reconcile_joining_for_parent(
                ctx.opctx,
                MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                MemberParentRef::Probe(probe_id),
                probe_state.valid,
                probe_state.sled_id.map(Into::into),
            )
            .await
            .context("failed to reconcile probe member in 'Joining' state")?;

        match reconcile_res.action {
            ReconcileAction::TransitionedToLeft => {
                info!(
                    ctx.opctx.log,
                    "multicast probe member 'Joining' → 'Left'";
                    "member_id" => %ctx.member.id,
                    "probe_id" => %ctx.member.parent_id,
                    "group_id" => %ctx.group.id(),
                    "reason" => "probe_not_valid_for_multicast",
                );
                Ok(StateTransition::StateChanged)
            }
            ReconcileAction::UpdatedSledId { .. }
            | ReconcileAction::NotFound
            | ReconcileAction::NoChange => {
                if ctx.group.state == MulticastGroupState::Active
                    && probe_state.valid
                {
                    self.datastore
                        .multicast_group_member_set_state_if_current_for_parent(
                            ctx.opctx,
                            MulticastGroupUuid::from_untyped_uuid(
                                ctx.group.id(),
                            ),
                            MemberParentRef::Probe(probe_id),
                            MulticastGroupMemberState::Joining,
                            MulticastGroupMemberState::Joined,
                        )
                        .await?;
                    Ok(StateTransition::StateChanged)
                } else {
                    Ok(StateTransition::NoChange)
                }
            }
        }
    }

    /// Probe-side "Joined" handler.
    ///
    /// A missing or invalid probe transitions the member to "Left".
    /// Otherwise this is a noop (steady state).
    async fn handle_probe_joined(
        &self,
        ctx: &MemberReconcileCtx<'_>,
        probe_id: ProbeUuid,
    ) -> Result<StateTransition, anyhow::Error> {
        let probe_state = self.lookup_probe_state(ctx.opctx, probe_id).await?;

        if !probe_state.valid {
            self.datastore
                .multicast_group_member_set_state_if_current_for_parent(
                    ctx.opctx,
                    MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                    MemberParentRef::Probe(probe_id),
                    MulticastGroupMemberState::Joined,
                    MulticastGroupMemberState::Left,
                )
                .await?;
            return Ok(StateTransition::StateChanged);
        }

        Ok(StateTransition::NoChange)
    }

    /// Probe-side "Left" handler.
    ///
    /// If the membership is soft-deleted, finish cleanup. Otherwise noop:
    /// probes do not reactivate, since membership is set only at probe-create
    /// time. Rear-port underlay membership is owned by mg-lower, so there is
    /// no dataplane cleanup to do here.
    async fn handle_probe_left(
        &self,
        ctx: &MemberReconcileCtx<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        if ctx.member.time_deleted.is_some() {
            self.cleanup_deleted_member(ctx).await?;
            return Ok(StateTransition::NeedsCleanup);
        }
        Ok(StateTransition::NoChange)
    }

    /// Per-row probe lookup state. Probes are rare, so there is no pre-pass
    /// cache. The `valid` field reflects an active (not soft-deleted) probe
    /// row with a hosting sled.
    async fn lookup_probe_state(
        &self,
        opctx: &OpContext,
        probe_id: ProbeUuid,
    ) -> Result<InstanceMulticastState, anyhow::Error> {
        let sled_id = self
            .datastore
            .probe_get_sled_for_multicast(opctx, probe_id)
            .await?;
        Ok(InstanceMulticastState { valid: sled_id.is_some(), sled_id })
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
                .multicast_group_member_left_to_joining_if_current_for_parent(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                    MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                        member.parent_id,
                    )),
                    sled_id.into(),
                )
                .await
                .context(
                    "failed to conditionally transition member from 'Left' to 'Joining' (with sled_id)",
                )?
        } else {
            self.datastore
                .multicast_group_member_set_state_if_current_for_parent(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                    MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                        member.parent_id,
                    )),
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

    /// Complete a member join by propagating sled forwarding state and
    /// subscribing the VMM's OPTE port.
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

        // If the member is already in a "Joined" state (migration path), skip
        // the state transition but still propagate and subscribe. During
        // migration the caller updates the sled ID without changing state,
        // so we must not gate propagation on this CAS.
        if ctx.member.state != MulticastGroupMemberState::Joined {
            let updated = self
                .datastore
                .multicast_group_member_set_state_if_current_for_parent(
                    ctx.opctx,
                    MulticastGroupUuid::from_untyped_uuid(ctx.group.id()),
                    MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                        ctx.member.parent_id,
                    )),
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
        // At this point, the member is now "Joined" in the database, so propagate
        // includes this sled in forwarding next-hops. If propagation or
        // subscribe fails below, the member remains "Joined" with incomplete
        // sled state. The reconciler's next pass re-converges sled state. DPD
        // members are programmed by `ddmd` from DDM peer subscriptions.
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

    /// Cleanup members that are "Left" and time_deleted.
    /// This permanently removes member records that are no longer needed.
    pub(super) async fn cleanup_deleted_members(
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

    /// Implicitly delete empty multicast groups by marking them "Deleting".
    ///
    /// Group lifecycle is membership-driven, so a group is reaped once it has no
    /// member rows with `time_deleted IS NULL`. Emptiness is defined by member
    /// row absence rather than by "no Joined members", so a group whose members
    /// are all "Left" (e.g. instances stopped) but whose rows persist is not
    /// empty and stays alive. Membership origin does not matter here, as static
    /// API joins today and IGMP/MLD snooping in the future (RFD 0488) are
    /// treated alike.
    ///
    /// This sweep is the authority for the emptiness decision. The synchronous
    /// mark on explicit member leave is only a best-effort latency
    /// optimization; correctness does not depend on it firing.
    ///
    /// Both "Creating" and "Active" groups are swept:
    /// - "Active" groups go empty when their last member leaves or their
    ///   instances are deleted (member rows soft-deleted via
    ///   `multicast_group_members_mark_for_removal`, then hard-deleted by
    ///   `cleanup_deleted_members`, which must run before this sweep).
    /// - "Creating" groups go empty when implicit creation succeeded but the
    ///   first member attach failed (orphans). These are gated by the
    ///   configured `orphan_grace_secs` so an in-flight first attach is not
    ///   raced.
    ///
    /// The underlying datastore method uses an atomic NOT EXISTS guard so a
    /// concurrent join that adds a member between the emptiness check and the
    /// mark-for-removal cannot be lost.
    pub(super) async fn cleanup_empty_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, anyhow::Error> {
        trace!(
            opctx.log,
            "checking for empty multicast groups to implicitly delete"
        );

        let groups = self
            .datastore
            .multicast_groups_list_by_states(
                opctx,
                &[MulticastGroupState::Creating, MulticastGroupState::Active],
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list creating/active groups")?;

        let mut groups_marked = 0;

        for group in groups {
            // "Creating" orphans wait out the grace period; "Active" groups
            // are reaped as soon as they are observed empty.
            if group.state == MulticastGroupState::Creating
                && !grace_period_elapsed(
                    group.time_created(),
                    self.orphan_grace_period,
                )
            {
                continue;
            }

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
                    "group_name" => %group.name(),
                    "group_state" => ?group.state,
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

    /// Cleanup a member that is marked for deletion (time_deleted set).
    ///
    /// This unsubscribes a member from its VMM. DPD member removal is
    /// handled by `ddmd` based on DDM peer subscriptions.
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

        Ok(())
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
