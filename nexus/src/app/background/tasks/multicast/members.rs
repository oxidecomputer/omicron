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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use slog::{debug, info, trace, warn};
use uuid::Uuid;

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

/// Pre-fetched instance state data for batch processing.
/// Maps instance_id -> (is_valid_for_multicast, current_sled_id).
type InstanceStateMap = HashMap<Uuid, (bool, Option<SledUuid>)>;

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
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Joined" state.
    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Left" state.
    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;
}

/// Processor for instance-based multicast group members.
struct InstanceMemberProcessor;

impl MemberStateProcessor for InstanceMemberProcessor {
    async fn process_joining(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_joining(
                opctx,
                group,
                member,
                instance_states,
                dataplane_client,
            )
            .await
    }

    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_joined(
                opctx,
                group,
                member,
                instance_states,
                dataplane_client,
            )
            .await
    }

    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_left(
                opctx,
                group,
                member,
                instance_states,
                dataplane_client,
            )
            .await
    }
}

impl MulticastGroupReconciler {
    /// Process member state changes ("Joining"→"Joined"→"Left").
    pub async fn reconcile_member_states(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, anyhow::Error> {
        trace!(opctx.log, "reconciling member state changes");

        let mut processed = 0;

        // Get all groups that need member state processing ("Creating" and "Active")
        let groups = self.get_reconcilable_groups(opctx).await?;

        for group in groups {
            match self
                .process_group_member_states(opctx, &group, dataplane_client)
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
            "members_processed" => processed
        );

        Ok(processed)
    }

    /// Process member state changes for a single group.
    async fn process_group_member_states(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, anyhow::Error> {
        let mut processed = 0;

        // Get members in various states that need processing
        let members = self.get_group_members(opctx, group.id()).await?;

        // Batch-fetch instance states for all members to avoid N+1 queries
        let instance_states =
            Arc::new(self.batch_fetch_instance_states(opctx, &members).await?);

        // Process members concurrently with configurable parallelism
        let results = stream::iter(members)
            .map(|member| {
                let instance_states = Arc::clone(&instance_states);
                async move {
                    let result = self
                        .process_member_state(
                            opctx,
                            group,
                            &member,
                            &instance_states,
                            dataplane_client,
                        )
                        .await;
                    (member, result)
                }
            })
            .buffer_unordered(self.member_concurrency_limit) // Configurable concurrency
            .collect::<Vec<_>>()
            .await;

        // Process results and update counters
        for (member, result) in results {
            match result {
                Ok(transition) => match transition {
                    StateTransition::StateChanged
                    | StateTransition::NoChange => {
                        processed += 1;
                        debug!(
                            opctx.log,
                            "processed member state change";
                            "member" => ?member,
                            "group" => ?group,
                            "transition" => ?transition
                        );
                    }
                    StateTransition::NeedsCleanup => {
                        processed += 1;
                        debug!(
                            opctx.log,
                            "member marked for cleanup";
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
    /// Routes to appropriate node based on member type.
    async fn process_member_state(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // For now, all members are instance-based, but this is where we'd
        // dispatch to different processors for different member types
        let processor = InstanceMemberProcessor;

        match member.state {
            MulticastGroupMemberState::Joining => {
                processor
                    .process_joining(
                        self,
                        opctx,
                        group,
                        member,
                        instance_states,
                        dataplane_client,
                    )
                    .await
            }
            MulticastGroupMemberState::Joined => {
                processor
                    .process_joined(
                        self,
                        opctx,
                        group,
                        member,
                        instance_states,
                        dataplane_client,
                    )
                    .await
            }
            MulticastGroupMemberState::Left => {
                processor
                    .process_left(
                        self,
                        opctx,
                        group,
                        member,
                        instance_states,
                        dataplane_client,
                    )
                    .await
            }
        }
    }

    /// Instance-specific handler for members in "Joining" state.
    ///
    /// Validates instance state and attempts to transition the member to "Joined"
    /// when ready. Uses CAS operations for concurrent-safe state updates.
    async fn handle_instance_joining(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Extract pre-fetched instance state
        let (instance_valid, current_sled_id) =
            self.get_instance_state_from_cache(instance_states, member);

        // Execute reconciliation CAS operation
        let reconcile_result = self
            .execute_joining_reconciliation(
                opctx,
                group,
                member,
                instance_valid,
                current_sled_id,
            )
            .await?;

        // Process reconciliation result
        self.process_joining_reconcile_result(
            opctx,
            group,
            member,
            instance_valid,
            reconcile_result,
            dataplane_client,
        )
        .await
    }

    /// Extract instance state from pre-fetched cache.
    fn get_instance_state_from_cache(
        &self,
        instance_states: &InstanceStateMap,
        member: &MulticastGroupMember,
    ) -> (bool, Option<SledUuid>) {
        instance_states.get(&member.parent_id).copied().unwrap_or((false, None))
    }

    /// Execute the reconciliation CAS operation for a member in "Joining" state.
    async fn execute_joining_reconciliation(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_valid: bool,
        current_sled_id: Option<SledUuid>,
    ) -> Result<ReconcileJoiningResult, anyhow::Error> {
        let current_sled_id_db = current_sled_id.map(|id| id.into());

        self.datastore
            .multicast_group_member_reconcile_joining(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member.parent_id),
                instance_valid,
                current_sled_id_db,
            )
            .await
            .context("failed to reconcile member in 'Joining' state")
    }

    /// Process the result of a "Joining" state reconciliation operation.
    async fn process_joining_reconcile_result(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_valid: bool,
        reconcile_result: ReconcileJoiningResult,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        match reconcile_result.action {
            ReconcileAction::TransitionedToLeft => {
                self.handle_transitioned_to_left(opctx, group, member).await
            }

            ReconcileAction::UpdatedSledId { old, new } => {
                self.handle_sled_id_updated(
                    opctx,
                    group,
                    member,
                    instance_valid,
                    SledIdUpdate { old, new },
                    dataplane_client,
                )
                .await
            }

            ReconcileAction::NotFound | ReconcileAction::NoChange => {
                self.handle_no_change_or_not_found(
                    opctx,
                    group,
                    member,
                    instance_valid,
                    dataplane_client,
                )
                .await
            }
        }
    }

    /// Handle the case where a member was transitioned to "Left" state.
    async fn handle_transitioned_to_left(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
    ) -> Result<StateTransition, anyhow::Error> {
        info!(
            opctx.log,
            "multicast member lifecycle transition: 'Joining' → 'Left'";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "group_multicast_ip" => %group.multicast_ip,
            "reason" => "instance_not_valid_for_multicast_traffic"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Handle the case where a member's sled_id was updated.
    async fn handle_sled_id_updated(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_valid: bool,
        sled_id_update: SledIdUpdate,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        debug!(
            opctx.log,
            "updated member sled_id, checking if ready to join";
            "member_id" => %member.id,
            "old_sled_id" => ?sled_id_update.old,
            "new_sled_id" => ?sled_id_update.new,
            "group_state" => ?group.state,
            "instance_valid" => instance_valid
        );

        self.try_complete_join_if_ready(
            opctx,
            group,
            member,
            instance_valid,
            dataplane_client,
        )
        .await
    }

    /// Handle the case where no changes were made or member was not found.
    async fn handle_no_change_or_not_found(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_valid: bool,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Check if member is already in Joined state
        if member.state == MulticastGroupMemberState::Joined {
            debug!(
                opctx.log,
                "member already in 'Joined' state, no action needed";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "group_name" => group.name().as_str()
            );
            return Ok(StateTransition::NoChange);
        }

        // Try to complete the join if conditions are met
        self.try_complete_join_if_ready(
            opctx,
            group,
            member,
            instance_valid,
            dataplane_client,
        )
        .await
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
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_valid: bool,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        if self.is_ready_to_join(group, instance_valid) {
            self.complete_instance_member_join(
                opctx,
                group,
                member,
                dataplane_client,
            )
            .await?;
            Ok(StateTransition::StateChanged)
        } else {
            debug!(
                opctx.log,
                "member not ready to join - waiting for next cycle";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "group_name" => group.name().as_str(),
                "instance_valid" => instance_valid,
                "group_state" => ?group.state
            );
            Ok(StateTransition::NoChange)
        }
    }

    /// Instance-specific handler for members in "Joined" state.
    async fn handle_instance_joined(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Get pre-fetched instance state and sled_id
        let (instance_valid, current_sled_id) = instance_states
            .get(&member.parent_id)
            .copied()
            .unwrap_or((false, None));

        match (instance_valid, current_sled_id) {
            // Invalid instance -> remove from dataplane and transition to "Left"
            (false, _) => {
                self.handle_invalid_instance(
                    opctx,
                    group,
                    member,
                    dataplane_client,
                )
                .await
            }

            // Valid instance with sled, but sled changed (migration)
            (true, Some(sled_id)) if member.sled_id != Some(sled_id.into()) => {
                self.handle_sled_migration(
                    opctx,
                    group,
                    member,
                    sled_id,
                    dataplane_client,
                )
                .await
            }

            // Valid instance with sled, sled unchanged -> verify configuration
            (true, Some(_sled_id)) => {
                self.verify_members(opctx, group, member, dataplane_client)
                    .await?;
                trace!(
                    opctx.log,
                    "member configuration verified, no changes needed";
                    "member_id" => %member.id,
                    "group_id" => %group.id()
                );
                Ok(StateTransition::NoChange)
            }

            // Valid instance but no sled_id (shouldn't typically happen in "Joined" state)
            (true, None) => {
                self.handle_joined_without_sled(
                    opctx,
                    group,
                    member,
                    dataplane_client,
                )
                .await
            }
        }
    }

    /// Handle a joined member whose instance became invalid.
    async fn handle_invalid_instance(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Remove from dataplane first
        if let Err(e) = self
            .remove_member_from_dataplane(opctx, member, dataplane_client)
            .await
        {
            warn!(
                opctx.log,
                "failed to remove member from dataplane, will retry";
                "member_id" => %member.id,
                "error" => ?e
            );
            return Err(e);
        }

        // Update database state (atomically set "Left" and clear sled_id)
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
                "skipping Joined→Left transition due to concurrent update";
                "member_id" => %member.id,
                "instance_id" => %member.parent_id,
                "group_id" => %group.id()
            );
            return Ok(StateTransition::NoChange);
        }

        info!(
            opctx.log,
            "multicast member lifecycle transition: 'Joined' → 'Left' (instance invalid)";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_multicast_ip" => %group.multicast_ip,
            "dpd_operation" => "remove_member_from_underlay_group",
            "reason" => "instance_no_longer_valid_for_multicast_traffic"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Handle sled migration for a "Joined" member.
    async fn handle_sled_migration(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        new_sled_id: SledUuid,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        info!(
            opctx.log,
            "detected sled migration for 'Joined' member - re-applying configuration";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "group_multicast_ip" => %group.multicast_ip,
            "old_sled_id" => ?member.sled_id,
            "new_sled_id" => %new_sled_id
        );

        // Remove from old sled's dataplane first
        if let Err(e) = self
            .remove_member_from_dataplane(opctx, member, dataplane_client)
            .await
        {
            warn!(
                opctx.log,
                "failed to remove member from old sled, will retry";
                "member_id" => %member.id,
                "old_sled_id" => ?member.sled_id,
                "error" => ?e
            );
            return Err(e);
        }

        // Update sled_id in database using CAS
        let updated = self
            .datastore
            .multicast_group_member_update_sled_id_if_current(
                opctx,
                InstanceUuid::from_untyped_uuid(member.parent_id),
                member.sled_id,
                Some(new_sled_id.into()),
            )
            .await
            .context(
                "failed to conditionally update member sled_id for migration",
            )?;

        if !updated {
            debug!(
                opctx.log,
                "skipping sled_id update after migration due to concurrent change";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "old_sled_id" => ?member.sled_id,
                "new_sled_id" => %new_sled_id
            );
            return Ok(StateTransition::NoChange);
        }

        // Re-apply configuration on new sled
        // If this fails (e.g., sled not yet in inventory), transition to "Joining" for retry
        match self
            .complete_instance_member_join(
                opctx,
                group,
                member,
                dataplane_client,
            )
            .await
        {
            Ok(()) => {
                info!(
                    opctx.log,
                    "member configuration re-applied after sled migration";
                    "member_id" => %member.id,
                    "instance_id" => %member.parent_id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str(),
                    "group_multicast_ip" => %group.multicast_ip,
                    "new_sled_id" => %new_sled_id,
                    "dpd_operation" => "re_add_member_to_underlay_multicast_group"
                );
                Ok(StateTransition::StateChanged)
            }
            Err(e) => {
                // Failed to join on new sled - transition to "Joining" and retry next cycle
                // Example case: sled not yet in inventory (`sp_slot` mapping unavailable)
                warn!(
                    opctx.log,
                    "failed to complete join on new sled after migration - transitioning to 'Joining' for retry";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "new_sled_id" => %new_sled_id,
                    "error" => %e
                );

                // TODO: Cross-validate inventory sled→port mapping via DDM operational state
                //
                // We currently trust inventory (MGS/SP topology) for sled→port
                // mapping.
                //
                // We could add validation using DDM on switches to confirm
                // operational connectivity:
                //
                // Query DDM (underlay routing daemon on switches):
                //   - GET /peers → Map<peer_addr, PeerInfo>
                //   - **Needs API addition**: DDM's PeerInfo should include
                //     port/interface or similar field showing which rear port
                //     each underlay peer is reachable through
                //   - Cross-reference: Does sled's underlay address appear as
                //     an "Active" peer on the expected rear port?
                //
                // On mismatch: Could invalidate cache, transition member to
                // "Left", or trigger inventory reconciliation. Prevents wasted
                // retries on sleds with actual connectivity loss vs. inventory
                // mismatch.

                let updated = self
                    .datastore
                    .multicast_group_member_set_state_if_current(
                        opctx,
                        MulticastGroupUuid::from_untyped_uuid(group.id()),
                        InstanceUuid::from_untyped_uuid(member.parent_id),
                        MulticastGroupMemberState::Joined,
                        MulticastGroupMemberState::Joining,
                    )
                    .await
                    .context(
                        "failed to transition member to 'Joining' after join failure",
                    )?;

                if updated {
                    info!(
                        opctx.log,
                        "member transitioned to 'Joining' - will retry on next reconciliation cycle";
                        "member_id" => %member.id,
                        "group_id" => %group.id(),
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
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        warn!(
            opctx.log,
            "'Joined' member has no sled_id - transitioning to 'Left'";
            "member_id" => %member.id,
            "parent_id" => %member.parent_id
        );

        // Remove from dataplane and transition to "Left"
        if let Err(e) = self
            .remove_member_from_dataplane(opctx, member, dataplane_client)
            .await
        {
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
            "dpd_operation" => "remove_member_from_underlay_group",
            "reason" => "inconsistent_state_sled_id_missing_in_joined_state"
        );
        Ok(StateTransition::StateChanged)
    }

    /// Instance-specific handler for members in "Left" state.
    async fn handle_instance_left(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        instance_states: &InstanceStateMap,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Get pre-fetched instance state and sled_id
        let (instance_valid, current_sled_id) = instance_states
            .get(&member.parent_id)
            .copied()
            .unwrap_or((false, None));

        match (member.time_deleted.is_some(), instance_valid, &group.state) {
            // Member marked for deletion -> cleanup from dataplane
            (true, _, _) => {
                self.cleanup_deleted_member(
                    opctx,
                    group,
                    member,
                    dataplane_client,
                )
                .await?;
                Ok(StateTransition::NeedsCleanup)
            }

            // Instance valid and group active -> transition to "Joining"
            (false, true, MulticastGroupState::Active) => {
                debug!(
                    opctx.log,
                    "transitioning member from 'Left' to 'Joining' - instance became valid and group is active";
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
                            "failed to conditionally transition member from Left to Joining (with sled_id)",
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
                            "failed to conditionally transition member from Left to Joining",
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

            // Otherwise, we stay in the "Left" state
            _ => Ok(StateTransition::NoChange),
        }
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
        for member in members {
            if let Some((instance, vmm_opt)) =
                instance_vmm_data.get(&member.parent_id)
            {
                let is_valid = matches!(
                    instance.runtime_state.nexus_state.state(),
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

                state_map.insert(member.parent_id, (is_valid, sled_id));
            } else {
                // Instance not found - mark as invalid
                state_map.insert(member.parent_id, (false, None));
            }
        }

        debug!(
            opctx.log,
            "batch-fetched instance states for multicast reconciliation";
            "member_count" => members.len(),
            "instances_found" => instance_vmm_data.len()
        );

        Ok(state_map)
    }

    /// Look up an instance's current sled_id and update the member record if found.
    ///
    /// Returns `None` if the instance has no sled assignment or cannot be found.
    async fn lookup_and_update_member_sled_id(
        &self,
        opctx: &OpContext,
        member: &MulticastGroupMember,
    ) -> Result<Option<DbTypedUuid<SledKind>>, anyhow::Error> {
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
                debug!(
                    opctx.log,
                    "failed to look up instance state";
                    "member" => ?member,
                    "error" => ?e
                );
                return Ok(None);
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

    /// Complete a member join operation ("Joining" -> "Joined") for an instance.
    async fn complete_instance_member_join(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        debug!(
            opctx.log,
            "completing member join";
            "member" => ?member,
            "group" => ?group
        );

        // Get sled_id from member record, or look it up and update if missing
        let sled_id = match member.sled_id {
            Some(id) => id,
            None => {
                match self
                    .lookup_and_update_member_sled_id(opctx, member)
                    .await?
                {
                    Some(id) => id,
                    None => return Ok(()), // No sled available, cannot join
                }
            }
        };

        self.add_member_to_dataplane(
            opctx,
            group,
            member,
            sled_id.into(),
            dataplane_client,
        )
        .await?;

        // Transition to "Joined" state (only if still in "Joining")
        let updated = self
            .datastore
            .multicast_group_member_set_state_if_current(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member.parent_id),
                MulticastGroupMemberState::Joining,
                MulticastGroupMemberState::Joined,
            )
            .await
            .context(
                "failed to conditionally transition member to 'Joined' state",
            )?;
        if !updated {
            debug!(
                opctx.log,
                "skipping Joining→Joined transition due to concurrent update";
                "member_id" => %member.id,
                "group_id" => %group.id()
            );
        }

        info!(
            opctx.log,
            "member join completed";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "sled_id" => %sled_id
        );

        Ok(())
    }

    /// Apply member dataplane configuration (via DPD-client).
    async fn add_member_to_dataplane(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        let underlay_group_id = group.underlay_group_id.ok_or_else(|| {
            anyhow::Error::msg(format!(
                "no underlay group for external group {}",
                group.id()
            ))
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context(
                "failed to fetch underlay group for member configuration",
            )?;

        // Resolve sled to switch port configurations
        let port_configs = self
            .resolve_sled_to_switch_ports(opctx, sled_id, dataplane_client)
            .await
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

        info!(
            opctx.log,
            "multicast member configuration applied to switch forwarding tables";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "sled_id" => %sled_id,
            "switch_count" => port_configs.len(),
            "dpd_operation" => "add_member_to_underlay_multicast_group"
        );

        Ok(())
    }

    /// Remove member dataplane configuration (via DPD-client).
    async fn remove_member_from_dataplane(
        &self,
        opctx: &OpContext,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        let group = self
            .datastore
            .multicast_group_fetch(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(member.external_group_id),
            )
            .await
            .context("failed to fetch group for member removal")?;

        let underlay_group_id = group.underlay_group_id.ok_or_else(|| {
            anyhow::Error::msg(format!(
                "no underlay group for external group {}",
                member.external_group_id
            ))
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group for member removal")?;

        if let Some(sled_id) = member.sled_id {
            // Resolve sled to switch port configurations
            let port_configs = self
                .resolve_sled_to_switch_ports(
                    opctx,
                    sled_id.into(),
                    dataplane_client,
                )
                .await
                .context("failed to resolve sled to switch ports")?;

            // Remove member from DPD for each port on the sled
            for port_config in &port_configs {
                let dataplane_member =
                    dpd_client::types::MulticastGroupMember {
                        port_id: port_config.port_id.clone(),
                        link_id: port_config.link_id,
                        direction: port_config.direction,
                    };

                dataplane_client
                    .remove_member(&underlay_group, dataplane_member)
                    .await
                    .context("failed to remove member configuration via DPD")?;

                debug!(
                    opctx.log,
                    "member removed from DPD";
                    "port_id" => %port_config.port_id,
                    "sled_id" => %sled_id
                );
            }

            info!(
                opctx.log,
                "multicast member configuration removed from switch forwarding tables";
                "member_id" => %member.id,
                "instance_id" => %member.parent_id,
                "sled_id" => %sled_id,
                "switch_count" => port_configs.len(),
                "dpd_operation" => "remove_member_from_underlay_multicast_group",
                "cleanup_reason" => "instance_state_change_or_migration"
            );
        }

        Ok(())
    }

    /// Clean up member dataplane configuration with strict error handling.
    /// Ensures dataplane consistency by failing if removal operations fail.
    async fn cleanup_member_from_dataplane(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        debug!(
            opctx.log,
            "cleaning up member from dataplane";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "parent_id" => %member.parent_id,
            "time_deleted" => ?member.time_deleted
        );

        // Strict removal from dataplane - fail on errors for consistency
        self.remove_member_from_dataplane(opctx, member, dataplane_client)
            .await
            .context(
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

    /// Verify that a joined member is consistent with dataplane configuration.
    async fn verify_members(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
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
        let underlay_group_id = group.underlay_group_id.ok_or_else(|| {
            anyhow::Error::msg(format!(
                "no underlay group for external group {}",
                group.id()
            ))
        })?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group")?;

        // Resolve expected member configurations
        let expected_port_configs = self
            .resolve_sled_to_switch_ports(
                opctx,
                sled_id.into(),
                dataplane_client,
            )
            .await
            .context("failed to resolve sled to switch ports")?;

        // Verify/re-add member for each port on the sled
        for port_config in &expected_port_configs {
            let expected_member = dpd_client::types::MulticastGroupMember {
                port_id: port_config.port_id.clone(),
                link_id: port_config.link_id,
                direction: port_config.direction,
            };

            // Check if member needs to be re-added
            match dataplane_client
                .add_member(&underlay_group, expected_member)
                .await
            {
                Ok(()) => {
                    debug!(
                            opctx.log,
                        "member verified/re-added to dataplane";
                        "member_id" => %member.id,
                        "sled_id" => %sled_id
                    );
                }
                Err(e) => {
                    // Log but don't fail - member might already be present
                    debug!(
                        opctx.log,
                        "member verification add_member call failed (may already exist)";
                        "member_id" => %member.id,
                        "error" => %e
                    );
                }
            }
        }

        info!(
            opctx.log,
            "member verification completed for all ports";
            "member_id" => %member.id,
            "sled_id" => %sled_id,
            "port_count" => expected_port_configs.len()
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

    /// Check cache for a sled mapping.
    async fn check_sled_cache(
        &self,
        cache_key: SledUuid,
    ) -> Option<Vec<SwitchBackplanePort>> {
        let cache = self.sled_mapping_cache.read().await;
        let (cached_at, mappings) = &*cache;
        if cached_at.elapsed().unwrap_or(self.sled_cache_ttl)
            < self.sled_cache_ttl
        {
            return mappings.get(&cache_key).cloned();
        }
        None
    }

    /// Detect backplane topology change and invalidate sled cache if needed.
    async fn handle_backplane_topology_change(
        &self,
        opctx: &OpContext,
        previous_map: &Option<
            BTreeMap<
                dpd_client::types::PortId,
                dpd_client::types::BackplaneLink,
            >,
        >,
        new_map: &BTreeMap<
            dpd_client::types::PortId,
            dpd_client::types::BackplaneLink,
        >,
    ) {
        if let Some(prev_map) = previous_map {
            if prev_map.len() != new_map.len()
                || prev_map.keys().collect::<Vec<_>>()
                    != new_map.keys().collect::<Vec<_>>()
            {
                warn!(
                    opctx.log,
                    "backplane map topology change detected";
                    "previous_port_count" => prev_map.len(),
                    "new_port_count" => new_map.len()
                );
                info!(
                    opctx.log,
                    "invalidating sled mapping cache due to backplane topology change"
                );
                self.invalidate_sled_mapping_cache().await;
            }
        }
    }

    /// Fetch the backplane map from DPD-client with caching.
    ///
    /// The client respons with the entire mapping of all cubbies in a rack.
    ///
    /// The backplane map should remain consistent same across all switches,
    /// so we query one switch and cache the result.
    async fn fetch_backplane_map(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<
        BTreeMap<dpd_client::types::PortId, dpd_client::types::BackplaneLink>,
        anyhow::Error,
    > {
        // Check cache first
        let previous_map = {
            let cache = self.backplane_map_cache.read().await;
            if let Some((cached_at, ref map)) = *cache {
                if cached_at.elapsed().unwrap_or(self.backplane_cache_ttl)
                    < self.backplane_cache_ttl
                {
                    trace!(
                        opctx.log,
                        "backplane map cache hit";
                        "port_count" => map.len()
                    );
                    return Ok(map.clone());
                }
                // Cache expired but keep reference to previous map for comparison
                Some(map.clone())
            } else {
                None
            }
        };

        // Cache miss - fetch from DPD via dataplane client
        debug!(
            opctx.log,
            "fetching backplane map from DPD (cache miss or stale)"
        );

        let backplane_map =
            dataplane_client.fetch_backplane_map().await.context(
                "failed to query backplane_map from DPD via dataplane client",
            )?;

        // Detect topology change and invalidate sled cache if needed
        self.handle_backplane_topology_change(
            opctx,
            &previous_map,
            &backplane_map,
        )
        .await;

        info!(
            opctx.log,
            "fetched backplane map from DPD";
            "port_count" => backplane_map.len()
        );

        // Update cache
        let mut cache = self.backplane_map_cache.write().await;
        *cache = Some((SystemTime::now(), backplane_map.clone()));

        Ok(backplane_map)
    }

    /// Resolve a sled ID to switch ports for multicast traffic.
    pub async fn resolve_sled_to_switch_ports(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<Vec<SwitchBackplanePort>, anyhow::Error> {
        // Check cache first
        if let Some(port_configs) = self.check_sled_cache(sled_id).await {
            return Ok(port_configs); // Return even if empty - sled exists but may not be scrimlet
        }

        // Refresh cache if stale or missing entry
        if let Err(e) =
            self.refresh_sled_mapping_cache(opctx, dataplane_client).await
        {
            warn!(
                opctx.log,
                "failed to refresh sled mapping cache, using stale data";
                "sled_id" => %sled_id,
                "error" => %e
            );
            // Try cache again even with stale data
            if let Some(port_configs) = self.check_sled_cache(sled_id).await {
                return Ok(port_configs);
            }
            // If cache refresh failed and no stale data, propagate error
            return Err(e.context("failed to refresh sled mapping cache and no cached data available"));
        }

        // Try cache again after successful refresh
        if let Some(port_configs) = self.check_sled_cache(sled_id).await {
            return Ok(port_configs);
        }

        // Sled not found after successful cache refresh - treat as error so callers
        // can surface this condition rather than silently applying no changes.
        Err(anyhow::Error::msg(format!(
            "failed to resolve sled to switch ports: \
             sled {sled_id} not found in mapping cache (not a scrimlet or removed)"
        )))
    }

    /// Find SP in inventory for a given sled's baseboard.
    /// Tries exact match (serial + part), then falls back to serial-only.
    fn find_sp_for_sled<'a>(
        &self,
        inventory: &'a nexus_types::inventory::Collection,
        sled: &Sled,
    ) -> Option<&'a nexus_types::inventory::ServiceProcessor> {
        // Try exact match first (serial + part)
        if let Some((_bb, sp)) = inventory.sps.iter().find(|(bb, _sp)| {
            bb.serial_number == sled.serial_number()
                && bb.part_number == sled.part_number()
        }) {
            return Some(sp);
        }

        // Fall back to serial-only match
        inventory
            .sps
            .iter()
            .find(|(bb, _sp)| bb.serial_number == sled.serial_number())
            .map(|(_bb, sp)| sp)
    }

    /// Map a single sled to switch port(s), validating against backplane map.
    /// Returns Ok(Some(ports)) on success, Ok(None) if validation failed.
    fn map_sled_to_ports(
        &self,
        opctx: &OpContext,
        sled: &Sled,
        sp_slot: u32,
        backplane_map: &BTreeMap<
            dpd_client::types::PortId,
            dpd_client::types::BackplaneLink,
        >,
    ) -> Result<Option<Vec<SwitchBackplanePort>>, anyhow::Error> {
        let port_id = dpd_client::types::PortId::Rear(
            dpd_client::types::Rear::try_from(format!("rear{sp_slot}"))
                .context("invalid rear port number")?,
        );

        // Validate against hardware backplane map
        if !backplane_map.contains_key(&port_id) {
            warn!(
                opctx.log,
                "sled sp_slot validation failed - not in hardware backplane map";
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
            link_id: dpd_client::types::LinkId(0),
            direction: dpd_client::types::Direction::Underlay,
        }]))
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
    /// ```rust
    /// // From dendrite/dpd/src/port_map.rs rev_ab_port_map()
    /// for entry in SIDECAR_REV_AB_BACKPLANE_MAP.iter() {
    ///     let port = PortId::Rear(RearPort::try_from(entry.cubby).unwrap());
    ///     inner.insert(port, Connector::QSFP(entry.tofino_connector.into()));
    /// }
    /// ```
    ///
    /// Where `entry.cubby` is the physical cubby/slot number (same as our `sp_slot`),
    /// and this maps it to a `PortId::Rear` that DPD can program on the Tofino ASIC.
    async fn refresh_sled_mapping_cache(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        // Fetch required data
        let inventory = self
            .datastore
            .inventory_get_latest_collection(opctx)
            .await
            .context("failed to get latest inventory collection")?
            .ok_or_else(|| {
                anyhow::Error::msg("no inventory collection available")
            })?;

        // First attempt with current backplane map
        let mut backplane_map =
            self.fetch_backplane_map(opctx, dataplane_client).await?;

        let sleds = self
            .datastore
            .sled_list_all_batched(opctx, SledFilter::InService)
            .await
            .context("failed to list in-service sleds for inventory mapping")?;

        // Build sled → port mappings
        let mut mappings = HashMap::new();
        let mut validation_failures = 0;
        let mut retry_with_fresh_backplane = false;

        for sled in &sleds {
            let Some(sp) = self.find_sp_for_sled(&inventory, sled) else {
                debug!(
                    opctx.log,
                    "no SP data found for sled in current inventory collection";
                    "sled_id" => %sled.id(),
                    "serial_number" => sled.serial_number(),
                    "part_number" => sled.part_number()
                );
                continue;
            };

            match self.map_sled_to_ports(
                opctx,
                sled,
                sp.sp_slot.into(),
                &backplane_map,
            )? {
                Some(ports) => {
                    mappings.insert(sled.id(), ports);
                }
                None => {
                    validation_failures += 1;
                    // If we have validation failures, we should refresh backplane map
                    retry_with_fresh_backplane = true;
                }
            }
        }

        // If we had validation failures, invalidate backplane cache and retry once
        if retry_with_fresh_backplane && validation_failures > 0 {
            info!(
                opctx.log,
                "sled validation failures detected - invalidating backplane cache and retrying";
                "validation_failures" => validation_failures
            );

            // Invalidate the backplane cache
            self.invalidate_backplane_cache().await;

            // Fetch fresh backplane map
            backplane_map = self
                .fetch_backplane_map(opctx, dataplane_client)
                .await
                .context(
                    "failed to fetch fresh backplane map after invalidation",
                )?;

            // Retry mapping with fresh backplane data
            mappings.clear();
            validation_failures = 0;

            for sled in &sleds {
                let Some(sp) = self.find_sp_for_sled(&inventory, sled) else {
                    continue;
                };

                match self.map_sled_to_ports(
                    opctx,
                    sled,
                    sp.sp_slot.into(),
                    &backplane_map,
                )? {
                    Some(ports) => {
                        mappings.insert(sled.id(), ports);
                    }
                    None => {
                        // Even with fresh data, this sled doesn't validate
                        validation_failures += 1;
                        warn!(
                            opctx.log,
                            "sled still fails validation with fresh backplane map";
                            "sled_id" => %sled.id(),
                            "sp_slot" => sp.sp_slot
                        );
                    }
                }
            }
        }

        // Update cache
        let sled_count = mappings.len();
        let mut cache = self.sled_mapping_cache.write().await;
        *cache = (SystemTime::now(), mappings);

        // Log results
        if validation_failures > 0 {
            warn!(
                opctx.log,
                "sled mapping cache refreshed with validation failures";
                "total_sleds" => sleds.len(),
                "mapped_sleds" => sled_count,
                "validation_failures" => validation_failures
            );
        } else {
            info!(
                opctx.log,
                "sled mapping cache refreshed successfully";
                "total_sleds" => sleds.len(),
                "mapped_sleds" => sled_count
            );
        }

        Ok(())
    }

    /// Cleanup a member that is marked for deletion (time_deleted set).
    async fn cleanup_deleted_member(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        // Use the consolidated cleanup helper with strict error handling
        self.cleanup_member_from_dataplane(
            opctx,
            group,
            member,
            dataplane_client,
        )
        .await
    }

    /// Get all multicast groups that need member reconciliation.
    /// This combines "Creating" and "Active" groups in a single optimized query pattern.
    async fn get_reconcilable_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<MulticastGroup>, anyhow::Error> {
        // For now, we still make two queries but this is where we'd add
        // a single combined query method if/when the datastore supports it
        let mut groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Creating,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list 'Creating' multicast groups")?;

        let active_groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Active,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list 'Active' multicast groups")?;

        groups.extend(active_groups);

        debug!(
            opctx.log,
            "found groups for member reconciliation";
            "total_groups" => groups.len()
        );

        Ok(groups)
    }
}
