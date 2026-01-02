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

use std::collections::{BTreeMap, BTreeSet, HashMap};
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

/// Backplane port mapping from DPD-client.
/// Maps switch port ID to backplane link configuration.
type BackplaneMap =
    BTreeMap<dpd_client::types::PortId, dpd_client::types::BackplaneLink>;

/// Result of computing the union of member ports across a group.
///
/// Indicates whether all "Joined" members were successfully resolved when
/// computing the port union. Callers should only prune stale ports when
/// the union is `Complete` to avoid disrupting members that failed resolution.
enum MemberPortUnion {
    /// Union is complete: all "Joined" members were successfully resolved.
    Complete(BTreeSet<dpd_client::types::PortId>),
    /// Union is partial: some "Joined" members failed to resolve.
    /// The port set may be incomplete.
    Partial(BTreeSet<dpd_client::types::PortId>),
}

/// Check if a DPD member is a rear/underlay port (instance member).
fn is_rear_underlay_member(
    member: &dpd_client::types::MulticastGroupMember,
) -> bool {
    matches!(member.port_id, dpd_client::types::PortId::Rear(_))
        && member.direction == dpd_client::types::Direction::Underlay
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
                    let res = self
                        .process_member_state(
                            opctx,
                            group,
                            &member,
                            &instance_states,
                            dataplane_client,
                        )
                        .await;
                    (member, res)
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
        let reconcile_res = self
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
            reconcile_res,
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
                "member not ready to join: waiting for next run";
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
            (true, Some(_)) => {
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
            debug!(
                opctx.log,
                "failed to remove member from dataplane, will retry";
                "member_id" => %member.id,
                "error" => ?e
            );
            return Err(e);
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
            "detected sled migration for 'Joined' member: re-applying configuration";
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
            debug!(
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
                // Failed to join on new sled. We transition to "Joining" and
                // retry next cycle/run.
                warn!(
                    opctx.log,
                    "failed to complete join on new sled after migration: transitioning to 'Joining' for retry";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "new_sled_id" => %new_sled_id,
                    "error" => %e
                );

                // TODO: Cross-validate inventory sled→port mapping via DDM
                // operational state.
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
                        "member transitioned to 'Joining': will retry on next reconciliation run";
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
            "'Joined' member has no sled_id: transitioning to 'Left'";
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

        // Handle permanent deletion first
        if member.time_deleted.is_some() {
            self.cleanup_deleted_member(opctx, group, member, dataplane_client)
                .await?;

            return Ok(StateTransition::NeedsCleanup);
        }

        // Handle reactivation: instance valid and group active -> transition to "Joining"
        if instance_valid && group.state == MulticastGroupState::Active {
            return self
                .reactivate_left_member(opctx, group, member, current_sled_id)
                .await;
        }

        // Clean up DPD if needed (best-effort)
        if !instance_valid && member.sled_id.is_none() {
            // This handles the case where a saga transitioned to "Left" (e.g., instance stop)
            // but couldn't clean DPD because it doesn't have switch access.
            if let Err(e) = self
                .remove_member_from_dataplane(opctx, member, dataplane_client)
                .await
            {
                debug!(
                    opctx.log,
                    "failed to clean up stale DPD state for 'Left' member";
                    "member_id" => %member.id,
                    "error" => ?e
                );
            }
        }

        // Stay in "Left" state
        Ok(StateTransition::NoChange)
    }

    /// Reactivate a member in "Left" state when instance becomes valid again.
    /// Transitions the member back to "Joining" state so it can rejoin the group.
    async fn reactivate_left_member(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        current_sled_id: Option<SledUuid>,
    ) -> Result<StateTransition, anyhow::Error> {
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
            let (is_valid, sled_id) = if let Some((instance, vmm_opt)) =
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

                (is_valid, sled_id)
            } else {
                // Instance not found (mark as invalid)
                (false, None)
            };

            (member.parent_id, (is_valid, sled_id))
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
    async fn remove_from_known_ports(
        &self,
        opctx: &OpContext,
        member: &MulticastGroupMember,
        sled_id: DbTypedUuid<SledKind>,
        port_configs: &[SwitchBackplanePort],
        underlay_group: &nexus_db_model::UnderlayMulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        // Remove member from DPD for each port on the sled
        for port_config in port_configs {
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
                "sled_id" => %sled_id
            );
        }

        info!(
            opctx.log,
            "multicast member configuration removed from switch forwarding tables";
            "member_id" => %member.id,
            "instance_id" => %member.parent_id,
            "sled_id" => %sled_id,
            "port_count" => port_configs.len(),
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
        dataplane_client: &MulticastDataplaneClient,
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
                match self
                    .resolve_sled_to_switch_ports(
                        opctx,
                        mem_sled_id.into(),
                        dataplane_client,
                    )
                    .await
                {
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
                is_rear_underlay_member(&member).then(|| cfg.port_id)
            })
            .collect::<BTreeSet<_>>();

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
                dataplane_client,
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
                if !active_member_ports.contains(&current_member.port_id) {
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

        // Try to remove via known ports if we have a `sled_id` and can resolve it
        if let Some(sled_id) = member.sled_id {
            if let Ok(port_configs) = self
                .resolve_sled_to_switch_ports(
                    opctx,
                    sled_id.into(),
                    dataplane_client,
                )
                .await
            {
                self.remove_from_known_ports(
                    opctx,
                    member,
                    sled_id,
                    &port_configs,
                    &underlay_group,
                    dataplane_client,
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
        )
        .await?;

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

        // Strict removal from dataplane (fail on errors)
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

    /// Verify that a "Joined" member is consistent with dataplane configuration.
    ///
    /// This function ensures the member is on the correct switch ports by:
    /// - Fetching current DPD state to see what ports the member is actually on
    /// - Computing expected ports from a refreshed cache
    /// - Removing the member from any unexpected/stale rear ports
    /// - Adding the member to expected ports
    ///
    /// This handles cases like `sp_slot` changes where the sled's physical
    /// location changed but the `sled_id` stayed the same.
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

        // Resolve expected member configurations (may refresh cache if TTL expired)
        let expected_port_configs = match self
            .resolve_sled_to_switch_ports(
                opctx,
                sled_id.into(),
                dataplane_client,
            )
            .await
        {
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
                let _ = self
                    .remove_member_from_dataplane(
                        opctx,
                        member,
                        dataplane_client,
                    )
                    .await;

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
                dataplane_client,
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
                    if !active_ports.contains(&current_member.port_id) {
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

        // If we can't determine elapsed time, consider cache expired
        let elapsed = match cached_at.elapsed() {
            Ok(duration) => duration,
            Err(_) => return None,
        };

        if elapsed < self.sled_cache_ttl {
            mappings.get(&cache_key).cloned()
        } else {
            None
        }
    }

    /// Detect backplane topology change and invalidate sled cache if needed.
    ///
    /// Compares the full (PortId, BackplaneLink) pairs to detect changes in:
    /// - Port count (sleds added/removed)
    /// - Port IDs (different physical slots)
    /// - Link attributes (speed, lanes, connector type changes)
    async fn handle_backplane_topology_change(
        &self,
        opctx: &OpContext,
        previous_map: &Option<BackplaneMap>,
        new_map: &BackplaneMap,
    ) {
        if let Some(prev_map) = previous_map {
            // Compare full maps (keys + values) to detect any topology changes
            if prev_map != new_map {
                info!(
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
    /// The client responds with the entire mapping of all cubbies in a rack.
    ///
    /// The backplane map should remain consistent same across all switches,
    /// so we query one switch and cache the result.
    async fn fetch_backplane_map(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<BackplaneMap, anyhow::Error> {
        // Check cache first
        let previous_map = {
            let cache = self.backplane_map_cache.read().await;
            if let Some((cached_at, ref map)) = *cache {
                // If we can't determine elapsed time, consider cache expired
                let elapsed = match cached_at.elapsed() {
                    Ok(duration) => duration,
                    Err(_) => {
                        // If errored, we consider cache expired and return
                        // previous map for comparison
                        return Ok(map.clone());
                    }
                };

                if elapsed < self.backplane_cache_ttl {
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

        // Fetch from DPD via dataplane client on cache miss
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
            return Ok(port_configs);
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

        // Sled not found after successful cache refresh. We treat this as an error
        // so callers can surface this condition rather than silently applying
        // no changes.
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
        let port_id = dpd_client::types::PortId::Rear(
            dpd_client::types::Rear::try_from(format!("rear{sp_slot}"))
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
            link_id: dpd_client::types::LinkId(0),
            direction: dpd_client::types::Direction::Underlay,
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
        let (mut mappings, mut validation_failures) = self
            .build_sled_mappings(opctx, &sleds, &inventory, &backplane_map)?;

        // If we had validation failures, invalidate backplane cache and retry once
        if validation_failures > 0 {
            info!(
                opctx.log,
                "sled validation failures detected: invalidating backplane cache and retrying";
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
            (mappings, validation_failures) = self.build_sled_mappings(
                opctx,
                &sleds,
                &inventory,
                &backplane_map,
            )?;

            // Log sleds that still fail with fresh backplane data
            if validation_failures > 0 {
                warn!(
                    opctx.log,
                    "some sleds still fail validation with fresh backplane map";
                    "validation_failures" => validation_failures
                );
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
    /// Returns both "Creating" and "Active" groups.
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
