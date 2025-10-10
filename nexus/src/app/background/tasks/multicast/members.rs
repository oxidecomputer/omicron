// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Member-specific multicast reconciler functions.
//!
//! This module handles multicast group member lifecycle operations within an
//! RPW. Members represent endpoints that receive multicast traffic,
//! typically instances running on compute sleds, but potentially other
//! resource types in the future.
//!
//! # RPW Member Processing Model
//!
//! Member management is more complex than group management because members have
//! dynamic lifecycle tied to instance state (start/stop/migrate) and require
//! dataplane updates. The RPW ensures eventual consistency between
//! intended membership (database) and actual forwarding (dataplane configuration).
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
//!   - Instance stopped, failed, or migrating
//!   - time_deleted=NULL: temporary (can rejoin)
//!   - time_deleted=SET: permanent deletion pending
//!
//! ## Operations Handled
//!
//! - **State transitions**: "Joining" → "Joined" → "Left" with reactivation
//! - **Dataplane updates**: Applying and removing configuration via DPD client(s) on switches
//! - **Sled migration**: Detecting moves and updating dataplane configuration accordingly
//! - **Cleanup**: Removing orphaned switch state for deleted members
//! - **Extensible processing**: Support for different member types as we evolve
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
//! members. This comprehensive matrix ensures no edge cases are missed:
//!
//! ## Valid Instance States for Multicast
//! - **Valid**: Creating, Starting, Running, Rebooting, Migrating, Repairing
//! - **Invalid**: Stopping, Stopped, Failed, Destroyed, NotFound, Error
//!
//! ## State Transitions
//!
//! ### JOINING State Transitions
//! | Condition | Group State | Instance Valid | Has sled_id | Action | Next State |
//! |-----------|-------------|----------------|-------------|---------|------------|
//! | 1 | "Creating" | Any | Any | Wait | "Joining" (NoChange) |
//! | 2 | "Active" | Invalid | Any | Transition + clear sled_id | "Left" |
//! | 3 | "Active" | Valid | No | Wait/Skip | "Joining" (NoChange) |
//! | 4 | "Active" | Valid | Yes | DPD updates + transition | "Joined" |
//!
//! ### JOINED State Transitions
//! | Condition | Instance Valid | Action | Next State |
//! |-----------|----------------|---------|------------|
//! | 1 | Invalid | Remove from dataplane switch state + clear sled_id + transition | "Left" |
//! | 2 | Valid | No action | "Joined" (NoChange) |
//!
//! ### LEFT State Transitions
//! | Condition | time_deleted | Instance Valid | Group State | Action | Next State |
//! |-----------|-------------|----------------|-------------|---------|------------|
//! | 1 | Set | Any | Any | Cleanup via DPD clients | NeedsCleanup |
//! | 2 | None | Invalid | Any | No action | "Left" (NoChange) |
//! | 3 | None | Valid | "Creating" | No action | "Left" (NoChange) |
//! | 4 | None | Valid | "Active" | Transition | "Joining" |

use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use slog::{debug, info, trace, warn};
use uuid::Uuid;

use nexus_db_model::{
    MulticastGroup, MulticastGroupMember, MulticastGroupMemberState,
    MulticastGroupState,
};
use nexus_db_queries::context::OpContext;
use nexus_types::identity::{Asset, Resource};
use omicron_common::api::external::{DataPageParams, InstanceState};
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, PropolisUuid, SledUuid,
};

use super::{MulticastGroupReconciler, MulticastSwitchPort, StateTransition};
use crate::app::multicast::dataplane::MulticastDataplaneClient;

/// Trait for processing different types of multicast group members.
trait MemberStateProcessor {
    /// Process a member in "Joining" state.
    async fn process_joining(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Joined" state.
    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a member in "Left" state.
    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
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
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_joining(opctx, group, member, dataplane_client)
            .await
    }

    async fn process_joined(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_joined(opctx, group, member, dataplane_client)
            .await
    }

    async fn process_left(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_instance_left(opctx, group, member, dataplane_client)
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

        // Process members concurrently with configurable parallelism
        let results = stream::iter(members)
            .map(|member| async move {
                let result = self
                    .process_member_state(
                        opctx,
                        group,
                        &member,
                        dataplane_client,
                    )
                    .await;
                (member, result)
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
                        dataplane_client,
                    )
                    .await
            }
            MulticastGroupMemberState::Left => {
                processor
                    .process_left(self, opctx, group, member, dataplane_client)
                    .await
            }
        }
    }

    /// Instance-specific handler for members in "Joining" state.
    /// Handles sled_id updates and validates instance state before proceeding.
    async fn handle_instance_joining(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // First, ensure we have current instance state and sled_id
        let (instance_valid, current_sled_id) =
            self.get_instance_state_and_sled(opctx, member.parent_id).await;

        // Update member's sled_id if it changed
        if let Some(sled_id) = current_sled_id {
            if member.sled_id != Some(sled_id.into()) {
                debug!(
                    opctx.log,
                    "updating member sled_id";
                    "member" => ?member,
                    "new_sled_id" => %sled_id
                );
                self.datastore
                    .multicast_group_member_update_sled_id(
                        opctx,
                        member.parent_id,
                        Some(sled_id.into()),
                    )
                    .await
                    .context("failed to update member sled_id")?;
            }
        }

        if group.state == MulticastGroupState::Active {
            // Group is active - can process member state changes
            if !instance_valid {
                // Instance is invalid - transition to "Left"
                debug!(
                    opctx.log,
                    "multicast member lifecycle transition: Joining → Left (instance invalid)";
                    "member_id" => %member.id,
                    "instance_id" => %member.parent_id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str(),
                    "current_sled_id" => ?member.sled_id,
                    "reason" => "instance_not_valid_for_multicast_traffic",
                    "instance_states_valid" => "[Creating, Starting, Running, Rebooting, Migrating, Repairing]"
                );
                self.datastore
                    .multicast_group_member_set_state(
                        opctx,
                        group.id(),
                        member.parent_id,
                        MulticastGroupMemberState::Left,
                    )
                    .await
                    .context(
                        "failed to transition member from Joining to Left",
                    )?;

                // Also clear sled_id when transitioning to "Left"
                if member.sled_id.is_some() {
                    self.datastore
                        .multicast_group_member_update_sled_id(
                            opctx,
                            member.parent_id,
                            None,
                        )
                        .await
                        .context("failed to clear member sled_id")?;
                }

                info!(
                    opctx.log,
                    "multicast member excluded from forwarding (Left state)";
                    "member_id" => %member.id,
                    "instance_id" => %member.parent_id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str(),
                    "group_multicast_ip" => %group.multicast_ip,
                    "forwarding_status" => "EXCLUDED",
                    "dpd_cleanup" => "not_required_for_Joining_to_Left_transition"
                );
                Ok(StateTransition::StateChanged)
            } else {
                // Instance is valid and group is active - proceed with join
                self.complete_instance_member_join(
                    opctx,
                    group,
                    member,
                    dataplane_client,
                )
                .await?;
                Ok(StateTransition::StateChanged)
            }
        } else {
            // Group is still "Creating" - keep members in "Joining" state
            // regardless of instance validity
            debug!(
                opctx.log,
                "member staying in Joining state - group still Creating";
                "member_id" => %member.id,
                "group_id" => %group.id(),
                "group_name" => group.name().as_str(),
                "instance_valid" => instance_valid,
                "group_state" => ?group.state
            );
            Ok(StateTransition::NoChange) // No state change - wait for group to become "Active"
        }
    }

    /// Instance-specific handler for members in "Joined" state.
    async fn handle_instance_joined(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Check instance validity and get current sled_id
        let (instance_valid, current_sled_id) =
            self.get_instance_state_and_sled(opctx, member.parent_id).await;

        if !instance_valid {
            // Instance became invalid - remove from dataplane and transition to "Left"
            debug!(
                opctx.log,
                "multicast member lifecycle transition: Joined → Left (instance state change)";
                "member_id" => %member.id,
                "instance_id" => %member.parent_id,
                "group_id" => %group.id(),
                "group_name" => group.name().as_str(),
                "group_multicast_ip" => %group.multicast_ip,
                "previous_sled_id" => ?member.sled_id,
                "reason" => "instance_no_longer_valid_for_multicast_traffic",
                "dpd_cleanup_required" => true
            );

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

            // Update database state
            self.datastore
                .multicast_group_member_set_state(
                    opctx,
                    group.id(),
                    member.parent_id,
                    MulticastGroupMemberState::Left,
                )
                .await
                .context(
                    "failed to transition member from 'Joined' to 'Left'",
                )?;

            // Clear sled_id since instance is no longer valid
            self.datastore
                .multicast_group_member_update_sled_id(
                    opctx,
                    member.parent_id,
                    None,
                )
                .await
                .context("failed to clear member sled_id")?;

            info!(
                opctx.log,
                "multicast member removed from switch forwarding tables";
                "member_id" => %member.id,
                "instance_id" => %member.parent_id,
                "group_id" => %group.id(),
                "group_multicast_ip" => %group.multicast_ip,
                "forwarding_status" => "REMOVED",
                "dpd_operation" => "remove_member_from_underlay_group",
                "switch_cleanup" => "COMPLETED"
            );
            Ok(StateTransition::StateChanged)
        } else if let Some(sled_id) = current_sled_id {
            // Instance is valid - check for sled migration
            if member.sled_id != Some(sled_id.into()) {
                debug!(
                    opctx.log,
                    "detected sled migration for joined member - re-applying configuration";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str(),
                    "old_sled_id" => ?member.sled_id,
                    "new_sled_id" => %sled_id
                );

                // Remove from old sled's dataplane first
                if let Err(e) = self
                    .remove_member_from_dataplane(
                        opctx,
                        member,
                        dataplane_client,
                    )
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

                // Update sled_id in database
                self.datastore
                    .multicast_group_member_update_sled_id(
                        opctx,
                        member.parent_id,
                        Some(sled_id.into()),
                    )
                    .await
                    .context("failed to update member sled_id for migration")?;

                // Re-apply configuration on new sled
                self.complete_instance_member_join(
                    opctx,
                    group,
                    member,
                    dataplane_client,
                )
                .await?;

                info!(
                    opctx.log,
                    "member configuration re-applied after sled migration";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str(),
                    "new_sled_id" => %sled_id
                );
                Ok(StateTransition::StateChanged)
            } else {
                // Instance still valid and sled unchanged - verify member dataplane configuration
                self.verify_members(opctx, group, member, dataplane_client)
                    .await?;
                Ok(StateTransition::NoChange)
            }
        } else {
            // Instance is valid but has no sled_id (shouldn't happen in Joined state)
            warn!(
                opctx.log,
                "joined member has no sled_id - transitioning to Left";
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

            self.datastore
                .multicast_group_member_set_state(
                    opctx,
                    group.id(),
                    member.parent_id,
                    MulticastGroupMemberState::Left,
                )
                .await
                .context(
                    "failed to transition member with no sled_id to Left",
                )?;

            Ok(StateTransition::StateChanged)
        }
    }

    /// Instance-specific handler for members in "Left" state.
    async fn handle_instance_left(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        // Check if this member is marked for deletion (time_deleted set)
        if member.time_deleted.is_some() {
            // Member marked for removal - ensure it's cleaned up from dataplane
            self.cleanup_deleted_member(opctx, group, member, dataplane_client)
                .await?;
            Ok(StateTransition::NeedsCleanup)
        } else {
            // Check if instance became valid and group is active - if so, transition back to "Joining"
            let instance_valid = self
                .is_valid_instance_for_multicast(opctx, member.parent_id)
                .await;

            if instance_valid && group.state == MulticastGroupState::Active {
                debug!(
                    opctx.log,
                    "transitioning member from Left to Joining - instance became valid and group is active";
                    "member_id" => %member.id,
                    "parent_id" => %member.parent_id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str()
                );
                self.datastore
                    .multicast_group_member_set_state(
                        opctx,
                        group.id(),
                        member.parent_id,
                        MulticastGroupMemberState::Joining,
                    )
                    .await
                    .context(
                        "failed to transition member from Left to Joining",
                    )?;
                info!(
                    opctx.log,
                    "member transitioned to Joining state";
                    "member_id" => %member.id,
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str()
                );
                Ok(StateTransition::StateChanged)
            } else {
                // Stay in "Left" state
                Ok(StateTransition::NoChange)
            }
        }
    }

    /// Get instance state and current sled_id for multicast processing.
    /// Returns (is_valid_for_multicast, current_sled_id).
    async fn get_instance_state_and_sled(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> (bool, Option<SledUuid>) {
        let instance_uuid = InstanceUuid::from_untyped_uuid(instance_id);

        // We need to look up both instance and VMM to get sled_id
        match self.datastore.instance_get_state(opctx, &instance_uuid).await {
            Ok(Some(instance_state)) => {
                let is_valid = matches!(
                    instance_state.nexus_state.state(),
                    InstanceState::Creating
                        | InstanceState::Starting
                        | InstanceState::Running
                        | InstanceState::Rebooting
                        | InstanceState::Migrating
                        | InstanceState::Repairing
                );

                // Get sled_id from VMM if instance has one
                let sled_id =
                    if let Some(propolis_id) = instance_state.propolis_id {
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
                    } else {
                        None
                    };

                (is_valid, sled_id)
            }
            Ok(None) | Err(_) => (false, None), // Instance not found or error occurred
        }
    }

    /// Check if a given UUID is an instance ID in a valid state for multicast processing.
    /// Valid states are: Creating (initial state) and Vmm (has VMM/running).
    async fn is_valid_instance_for_multicast(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> bool {
        let instance_id = InstanceUuid::from_untyped_uuid(id);
        match self.datastore.instance_get_state(opctx, &instance_id).await {
            Ok(Some(instance_state)) => {
                match instance_state.nexus_state.state() {
                    InstanceState::Creating
                    | InstanceState::Starting
                    | InstanceState::Running => true,
                    InstanceState::Stopping
                    | InstanceState::Stopped
                    | InstanceState::Failed
                    | InstanceState::Destroyed => false,
                    InstanceState::Rebooting
                    | InstanceState::Migrating
                    | InstanceState::Repairing => true,
                }
            }
            Ok(None) | Err(_) => false, // Instance not found or error occurred
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

        // Get sled_id from member record, or look it up if missing
        let sled_id = match member.sled_id {
            Some(id) => id,
            None => {
                debug!(
                    opctx.log,
                    "member has no sled_id, attempting to look up instance sled";
                    "member" => ?member
                );

                // Try to find the instance's current sled
                let instance_id =
                    InstanceUuid::from_untyped_uuid(member.parent_id);
                match self
                    .datastore
                    .instance_get_state(opctx, &instance_id)
                    .await
                {
                    Ok(Some(instance_state)) => {
                        // Get sled_id from VMM if instance has one
                        let current_sled_id = if let Some(propolis_id) =
                            instance_state.propolis_id
                        {
                            match self
                                .datastore
                                .vmm_fetch(
                                    opctx,
                                    &PropolisUuid::from_untyped_uuid(
                                        propolis_id,
                                    ),
                                )
                                .await
                            {
                                Ok(vmm) => Some(SledUuid::from_untyped_uuid(
                                    vmm.sled_id.into_untyped_uuid(),
                                )),
                                Err(_) => None,
                            }
                        } else {
                            None
                        };

                        if let Some(current_sled_id) = current_sled_id {
                            debug!(
                                opctx.log,
                                "found instance sled, updating member record";
                                "member" => ?member,
                                "sled_id" => %current_sled_id
                            );

                            // Update the member record with the correct sled_id
                            self.datastore
                                .multicast_group_member_update_sled_id(
                                    opctx,
                                    member.parent_id,
                                    Some(current_sled_id.into()),
                                )
                                .await
                                .context("failed to update member sled_id")?;

                            current_sled_id.into()
                        } else {
                            debug!(
                                opctx.log,
                                "instance has no sled_id, cannot complete join";
                                "member" => ?member
                            );
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        debug!(
                            opctx.log,
                            "instance not found, cannot complete join";
                            "member" => ?member
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        debug!(
                            opctx.log,
                            "failed to look up instance state";
                            "member" => ?member,
                            "error" => ?e
                        );
                        return Ok(());
                    }
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

        // Transition to "Joined" state
        self.datastore
            .multicast_group_member_set_state(
                opctx,
                group.id(),
                member.parent_id,
                nexus_db_model::MulticastGroupMemberState::Joined,
            )
            .await
            .context("failed to transition member to Joined state")?;

        info!(
            opctx.log,
            "member join completed";
            "member_id" => %member.id,
            "group_id" => %group.id(),
            "sled_id" => %sled_id
        );

        Ok(())
    }

    /// Apply member dataplane configuration (via DPD).
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
            .resolve_sled_to_switch_ports(opctx, sled_id)
            .await
            .context("failed to resolve sled to switch ports")?;

        for port_config in &port_configs {
            let dataplane_member = dpd_client::types::MulticastGroupMember {
                port_id: port_config.port_id.clone(),
                link_id: port_config.link_id,
                direction: port_config.direction,
            };

            dataplane_client
                .add_member(opctx, &underlay_group, dataplane_member)
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
            "switch_ports_configured" => port_configs.len(),
            "dpd_operation" => "add_member_to_underlay_multicast_group",
            "forwarding_status" => "ACTIVE",
            "traffic_direction" => "Underlay"
        );

        Ok(())
    }

    /// Remove member dataplane configuration (via DPD).
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
                .resolve_sled_to_switch_ports(opctx, sled_id.into())
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
                    .remove_member(opctx, &underlay_group, dataplane_member)
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
                "switch_ports_cleaned" => port_configs.len(),
                "dpd_operation" => "remove_member_from_underlay_multicast_group",
                "forwarding_status" => "INACTIVE",
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
            .resolve_sled_to_switch_ports(opctx, sled_id.into())
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
                .add_member(opctx, &underlay_group, expected_member)
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
                group_id,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list group members")
    }

    /// Check cache for a sled mapping.
    async fn check_sled_cache(
        &self,
        cache_key: SledUuid,
    ) -> Option<Vec<MulticastSwitchPort>> {
        let cache = self.sled_mapping_cache.read().await;
        let (cached_at, mappings) = &*cache;
        if cached_at.elapsed().unwrap_or(self.cache_ttl) < self.cache_ttl {
            return mappings.get(&cache_key).cloned();
        }
        None
    }

    /// Resolve a sled ID to switch ports for multicast traffic.
    pub async fn resolve_sled_to_switch_ports(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<Vec<MulticastSwitchPort>, anyhow::Error> {
        // Check cache first
        if let Some(port_configs) = self.check_sled_cache(sled_id).await {
            return Ok(port_configs); // Return even if empty - sled exists but may not be scrimlet
        }

        // Refresh cache if stale or missing entry
        if let Err(e) = self.refresh_sled_mapping_cache(opctx).await {
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

    /// Refresh the sled-to-switch-port mapping cache.
    async fn refresh_sled_mapping_cache(
        &self,
        opctx: &OpContext,
    ) -> Result<(), anyhow::Error> {
        // Get all scrimlets (switch-connected sleds) from the database
        let sleds = self
            .datastore
            .sled_list_all_batched(
                opctx,
                nexus_types::deployment::SledFilter::Commissioned,
            )
            .await
            .context("failed to list sleds")?;

        // Filter to only scrimlets
        let scrimlets: Vec<_> =
            sleds.into_iter().filter(|sled| sled.is_scrimlet()).collect();

        trace!(
            opctx.log,
            "building sled mapping cache for scrimlets";
            "scrimlet_count" => scrimlets.len()
        );

        let mut mappings = HashMap::new();

        // For each scrimlet, determine its switch location from switch port data
        for sled in scrimlets {
            // Query switch ports to find which switch this sled is associated with
            // In the Oxide rack, each scrimlet has a co-located switch
            // We need to find switch ports that correspond to this sled's location
            let switch_ports = self
                .datastore
                .switch_port_list(opctx, &DataPageParams::max_page())
                .await
                .context("failed to list switch ports")?;

            // Find ports that map to this scrimlet
            let instance_switch_ports = match self
                .find_instance_switch_ports_for_sled(&sled, &switch_ports)
            {
                Some(ports) => ports,
                None => {
                    return Err(anyhow::Error::msg(format!(
                        "no instance switch ports found for sled {} - cannot create multicast mapping (sled rack_id: {})",
                        sled.id(),
                        sled.rack_id
                    )));
                }
            };

            // Create mappings for all available instance ports on this sled
            let mut sled_port_configs = Vec::new();
            for instance_switch_port in instance_switch_ports.iter() {
                // Set port and link IDs
                let port_id = instance_switch_port
                    .port_name
                    .as_str()
                    .parse()
                    .context("failed to parse port name")?;
                let link_id = dpd_client::types::LinkId(0);

                let config = MulticastSwitchPort {
                    port_id,
                    link_id,
                    direction: dpd_client::types::Direction::Underlay,
                };

                sled_port_configs.push(config);

                debug!(
                    opctx.log,
                    "mapped scrimlet to instance port";
                    "sled_id" => %sled.id(),
                    "switch_location" => %instance_switch_port.switch_location,
                    "port_name" => %instance_switch_port.port_name
                );
            }

            // Store all port configs for this sled
            mappings.insert(sled.id(), sled_port_configs);

            info!(
                opctx.log,
                "mapped scrimlet to all instance ports";
                "sled_id" => %sled.id(),
                "port_count" => instance_switch_ports.len()
            );
        }

        let mut cache = self.sled_mapping_cache.write().await;
        let mappings_len = mappings.len();
        *cache = (SystemTime::now(), mappings);

        info!(
            opctx.log,
            "sled mapping cache refreshed";
            "scrimlet_mappings" => mappings_len
        );

        Ok(())
    }

    /// Find switch ports on the same rack as the given sled.
    /// This is the general switch topology logic.
    fn find_rack_ports_for_sled<'a>(
        &self,
        sled: &nexus_db_model::Sled,
        switch_ports: &'a [nexus_db_model::SwitchPort],
    ) -> Vec<&'a nexus_db_model::SwitchPort> {
        switch_ports
            .iter()
            .filter(|port| port.rack_id == sled.rack_id)
            .collect()
    }

    /// Filter ports to only include instance ports (QSFP ports for instance traffic).
    /// This is the instance-specific port logic.
    fn filter_to_instance_switch_ports<'a>(
        &self,
        ports: &[&'a nexus_db_model::SwitchPort],
    ) -> Vec<&'a nexus_db_model::SwitchPort> {
        ports
            .iter()
            .filter(|port| {
                match port
                    .port_name
                    .as_str()
                    .parse::<dpd_client::types::PortId>()
                {
                    Ok(dpd_client::types::PortId::Qsfp(_)) => true,
                    _ => false,
                }
            })
            .copied()
            .collect()
    }

    /// Find the appropriate instance switch orts for a given sled.
    /// This combines general switch logic with instance-specific filtering.
    fn find_instance_switch_ports_for_sled<'a>(
        &self,
        sled: &nexus_db_model::Sled,
        switch_ports: &'a [nexus_db_model::SwitchPort],
    ) -> Option<Vec<&'a nexus_db_model::SwitchPort>> {
        // General switch logic: find ports on same rack
        let rack_ports = self.find_rack_ports_for_sled(sled, switch_ports);

        if rack_ports.is_empty() {
            return None;
        }

        // Instance-specific logic: filter to instance ports only
        let instance_switch_ports =
            self.filter_to_instance_switch_ports(&rack_ports);

        if !instance_switch_ports.is_empty() {
            Some(instance_switch_ports)
        } else {
            None
        }
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
            .context("failed to list Creating multicast groups")?;

        let active_groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Active,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list Active multicast groups")?;

        groups.extend(active_groups);

        debug!(
            opctx.log,
            "found groups for member reconciliation";
            "total_groups" => groups.len()
        );

        Ok(groups)
    }
}
