// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Group-specific multicast reconciler functions.
//!
//! This module handles multicast group lifecycle operations within an RPW
//! (Reliable Persistent Workflow). Groups represent the fundamental
//! multicast forwarding entities represented by dataplane configuration (via
//! DPD) applied on switches.
//!
//! # RPW Group Processing Model
//!
//! Unlike sagas that orchestrate targeted, synchronous changes, the RPW
//! reconciler ensures the dataplane (via DPD) reflects the intended state from
//! the database.
//! Group processing is idempotent and resilient to failures.
//!
//! ## Operations Handled
//! - **"Creating" state**: Initiate DPD "ensure" to apply configuration
//! - **"Active" state**: Verification and drift correction
//! - **"Deleting" state**: Switch cleanup and database removal
//! - **Extensible processing**: Support for different group types
//!
//! # Group State Transition Matrix
//!
//! The RPW reconciler handles all possible state transitions for multicast
//! groups:
//!
//! ## Group State Lifecycle
//! ```text
//! "Creating"                  → "Active" → "Deleting" → "Deleted" (removed from DB)
//!     ↓                            ↓           ↓
//!   (saga=external+underlay)    (verify)    (cleanup)
//! ```
//!
//! ## State Transition Permutations
//!
//! ### CREATING State Transitions
//! | Condition | Underlay Group | Saga Status | Action | Next State |
//! |-----------|---------------|-------------|--------|------------|
//! | 1 | Missing | N/A | Create underlay + start saga | "Creating" (saga handles →"Active") |
//! | 2 | Exists | N/A | Start DPD ensure | "Creating" (ensure handles →"Active") |
//! | 3 | Any | Failed | Log error, retry next pass | "Creating" (NoChange) |
//!
//! ### ACTIVE State Transitions
//! | Condition | DPD State | Action | Next State |
//! |-----------|-----------|---------|------------|
//! | 1 | Updated correctly | No action | "Active" (NoChange) |
//! | 2 | Missing/incorrect | Ensure dataplane reflects intended config (DPD) | "Active" (NoChange) |
//!
//! ### DELETING State Transitions
//! | Condition | DPD Cleanup | DB Cleanup | Action | Next State |
//! |-----------|------------|-----------|---------|------------|
//! | 1 | Success | Success | Remove from DB | Deleted (removed) |
//! | 2 | Failed | N/A | Log error, retry next pass | "Deleting" (NoChange) |
//! | 3 | Success | Failed | Log error, retry next pass | "Deleting" (NoChange) |
//!
//! ### DELETED State Transitions
//! | Condition | Action | Next State |
//! |-----------|---------|------------|
//! | 1 | Remove corresponding DPD configuration | Removed from DB |
//!
//! ## Triggering Events
//! - **"Creating"**: User API creates group → DB inserts with "Creating" state
//! - **"Active"**: DPD ensure completes successfully → state = "Active"
//! - **"Deleting"**: User API deletes group → DB sets state = "Deleting"
//! - **"Deleted"**: RPW reconciler completes cleanup → removes from DB
//!
//! ## Error Handling
//! - **Saga failures**: Group stays in "Creating", reconciler retries
//! - **DPD failures**: Group stays in current state, logged and retried
//! - **DB failures**: Operations retried in subsequent reconciler passes
//! - **Partial cleanup**: "Deleting" state preserved until complete cleanup

use anyhow::Context;
use futures::stream::{self, StreamExt};
use slog::{debug, info, trace, warn};

use nexus_db_model::{MulticastGroup, MulticastGroupState};
use nexus_db_queries::context::OpContext;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::{
    MulticastGroupReconciler, StateTransition, map_external_to_underlay_ip,
};
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::saga::create_saga_dag;
use crate::app::sagas;

/// Trait for processing different types of multicast groups
trait GroupStateProcessor {
    /// Process a group in "Creating" state.
    async fn process_creating(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a group in "Deleting" state.
    async fn process_deleting(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a group in "Active" state (verification).
    async fn process_active(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error>;
}

/// Processor for external multicast groups (customer/operator-facing).
struct ExternalGroupProcessor;

impl GroupStateProcessor for ExternalGroupProcessor {
    /// Handle groups in "Creating" state.
    async fn process_creating(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler.handle_creating_external_group(opctx, group).await
    }

    /// Handle groups in "Deleting" state.
    async fn process_deleting(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_deleting_external_group(opctx, group, dataplane_client)
            .await
    }

    /// Handle groups in "Active" state (verification).
    async fn process_active(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_active_external_group(opctx, group, dataplane_client)
            .await
    }
}

impl MulticastGroupReconciler {
    /// Process multicast groups that are in "Creating" state.
    pub async fn reconcile_creating_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, String> {
        trace!(opctx.log, "searching for creating multicast groups");

        let groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Creating,
                &DataPageParams::max_page(),
            )
            .await
            .map_err(|e| {
                error!(
                    opctx.log,
                    "failed to list creating multicast groups";
                    "error" => %e
                );
                "failed to list creating multicast groups".to_string()
            })?;

        trace!(opctx.log, "found creating multicast groups"; "count" => groups.len());

        // Process groups concurrently with configurable parallelism
        let results = stream::iter(groups)
            .map(|group| async move {
                let result =
                    self.process_group_state(opctx, &group, None).await;
                (group, result)
            })
            .buffer_unordered(self.group_concurrency_limit)
            .collect::<Vec<_>>()
            .await;

        let mut processed = 0;
        for (group, result) in results {
            match result {
                Ok(transition) => match transition {
                    StateTransition::StateChanged
                    | StateTransition::NoChange => {
                        processed += 1;
                        debug!(
                            opctx.log,
                            "processed creating multicast group";
                            "group" => ?group,
                            "transition" => ?transition
                        );
                    }
                    StateTransition::NeedsCleanup => {
                        debug!(
                            opctx.log,
                            "creating group marked for cleanup";
                            "group" => ?group
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to process creating multicast group";
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        Ok(processed)
    }

    /// Process multicast groups that are in "Deleting" state.
    pub async fn reconcile_deleting_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, String> {
        let groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Deleting,
                &DataPageParams::max_page(),
            )
            .await
            .map_err(|e| {
                error!(
                    opctx.log,
                    "failed to list deleting multicast groups";
                    "error" => %e
                );
                "failed to list deleting multicast groups".to_string()
            })?;

        // Process groups concurrently with configurable parallelism
        let results = stream::iter(groups)
            .map(|group| async move {
                let result = self
                    .process_group_state(opctx, &group, Some(dataplane_client))
                    .await;
                (group, result)
            })
            .buffer_unordered(self.group_concurrency_limit)
            .collect::<Vec<_>>()
            .await;

        let mut processed = 0;
        for (group, result) in results {
            match result {
                Ok(transition) => match transition {
                    StateTransition::StateChanged
                    | StateTransition::NeedsCleanup => {
                        processed += 1;
                        debug!(
                            opctx.log,
                            "processed deleting multicast group";
                            "group" => ?group,
                            "transition" => ?transition
                        );
                    }
                    StateTransition::NoChange => {
                        debug!(
                            opctx.log,
                            "deleting group no change needed";
                            "group" => ?group
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to process deleting multicast group";
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        Ok(processed)
    }

    /// Verify that active multicast groups are still properly configured.
    pub async fn reconcile_active_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, String> {
        trace!(opctx.log, "searching for active multicast groups");

        let groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                MulticastGroupState::Active,
                &DataPageParams::max_page(),
            )
            .await
            .map_err(|e| {
                error!(
                    opctx.log,
                    "failed to list active multicast groups";
                    "error" => %e
                );
                "failed to list active multicast groups".to_string()
            })?;

        trace!(opctx.log, "found active multicast groups"; "count" => groups.len());

        // Process groups concurrently with configurable parallelism
        let results = stream::iter(groups)
            .map(|group| async move {
                let result = self
                    .process_group_state(opctx, &group, Some(dataplane_client))
                    .await;
                (group, result)
            })
            .buffer_unordered(self.group_concurrency_limit)
            .collect::<Vec<_>>()
            .await;

        let mut verified = 0;
        let total_results = results.len();
        for (group, result) in results {
            match result {
                Ok(transition) => match transition {
                    StateTransition::StateChanged
                    | StateTransition::NoChange => {
                        verified += 1;
                        debug!(
                            opctx.log,
                            "processed active multicast group";
                            "group" => ?group,
                            "transition" => ?transition
                        );
                    }
                    StateTransition::NeedsCleanup => {
                        debug!(
                            opctx.log,
                            "active group marked for cleanup";
                            "group" => ?group
                        );
                    }
                },
                Err(e) => {
                    warn!(
                        opctx.log,
                        "active group verification/reconciliation failed";
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        debug!(
            opctx.log,
            "active group reconciliation completed";
            "verified" => verified,
            "total" => total_results
        );

        Ok(verified)
    }

    /// Main dispatch function for processing group state changes.
    /// Routes to appropriate processor based on group type and state.
    async fn process_group_state(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: Option<&MulticastDataplaneClient>,
    ) -> Result<StateTransition, anyhow::Error> {
        // Future: Match on group type to select different processors if
        // we add more nuanced group types
        let processor = ExternalGroupProcessor;

        match group.state {
            MulticastGroupState::Creating => {
                processor.process_creating(self, opctx, group).await
            }
            MulticastGroupState::Deleting => {
                let dataplane_client = dataplane_client.ok_or_else(|| {
                    anyhow::Error::msg(
                        "dataplane client required for deleting state",
                    )
                })?;
                processor
                    .process_deleting(self, opctx, group, dataplane_client)
                    .await
            }
            MulticastGroupState::Active => {
                let dataplane_client = dataplane_client.ok_or_else(|| {
                    anyhow::Error::msg(
                        "dataplane client required for active state",
                    )
                })?;
                processor
                    .process_active(self, opctx, group, dataplane_client)
                    .await
            }
            MulticastGroupState::Deleted => {
                debug!(
                    opctx.log,
                    "cleaning up deleted multicast group from local database";
                    "group_id" => %group.id(),
                    "group_name" => group.name().as_str()
                );

                // Try to delete underlay group record if it exists
                if let Some(underlay_group_id) = group.underlay_group_id {
                    self.datastore
                        .underlay_multicast_group_delete(
                            opctx,
                            underlay_group_id,
                        )
                        .await
                        .ok();
                }
                // Try to delete external group record
                self.datastore
                    .multicast_group_delete(
                        opctx,
                        MulticastGroupUuid::from_untyped_uuid(group.id()),
                    )
                    .await
                    .ok();

                Ok(StateTransition::StateChanged)
            }
        }
    }

    /// External group handler for groups in "Creating" state.
    async fn handle_creating_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<StateTransition, anyhow::Error> {
        debug!(
            opctx.log,
            "processing external multicast group transition: Creating → Active";
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "multicast_ip" => %group.multicast_ip,
            "multicast_scope" => if group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "vni" => ?group.vni,
            "underlay_linked" => group.underlay_group_id.is_some()
        );

        // Handle underlay group creation/linking (same logic as before)
        self.process_creating_group_inner(opctx, group).await?;

        // Successfully started saga - the saga will handle state transition to "Active".
        // We return NoChange because the reconciler shouldn't change the state;
        // the saga applies external + underlay configuration via DPD.
        Ok(StateTransition::NoChange)
    }

    /// External group handler for groups in "Deleting" state.
    async fn handle_deleting_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        debug!(
            opctx.log,
            "processing external multicast group transition: Deleting → Deleted (switch cleanup)";
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "multicast_ip" => %group.multicast_ip,
            "multicast_scope" => if group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "underlay_group_id" => ?group.underlay_group_id,
            "dpd_cleanup_required" => true
        );

        self.process_deleting_group_inner(opctx, group, dataplane_client)
            .await?;
        Ok(StateTransition::StateChanged)
    }

    /// External group handler for groups in "Active" state (verification).
    async fn handle_active_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        debug!(
            opctx.log,
            "verifying active external multicast group dataplane consistency";
            "group_id" => %group.id(),
            "multicast_ip" => %group.multicast_ip,
            "multicast_scope" => if group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "underlay_group_id" => ?group.underlay_group_id,
            "verification_type" => "switch_forwarding_table_sync"
        );

        self.verify_groups_inner(opctx, group, dataplane_client).await?;
        Ok(StateTransition::NoChange)
    }

    /// Process a single multicast group in "Creating" state.
    async fn process_creating_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<(), anyhow::Error> {
        debug!(
            opctx.log,
            "processing creating multicast group";
            "group" => ?group
        );

        // Handle underlay group creation/linking
        let underlay_group = match group.underlay_group_id {
            Some(underlay_id) => {
                let underlay = self
                    .datastore
                    .underlay_multicast_group_fetch(opctx, underlay_id)
                    .await
                    .with_context(|| {
                        format!("failed to fetch linked underlay group {underlay_id}")
                    })?;

                debug!(
                    opctx.log,
                    "found linked underlay group";
                    "group" => ?group,
                    "underlay_group" => ?underlay
                );
                underlay
            }
            None => {
                debug!(
                    opctx.log,
                    "creating new underlay group";
                    "group" => ?group
                );

                // Generate underlay multicast IP using IPv6 admin-local scope (RFC 7346)
                let underlay_ip =
                    map_external_to_underlay_ip(group.multicast_ip.ip())
                        .context(
                            "failed to map customer multicast IP to underlay",
                        )?;

                let vni = group.vni;

                let new_underlay = self
                    .datastore
                    .ensure_underlay_multicast_group(
                        opctx,
                        group.clone(),
                        underlay_ip.into(),
                        vni,
                    )
                    .await
                    .context("failed to create underlay multicast group")?;

                new_underlay
            }
        };

        // Launch DPD transaction saga for atomic dataplane configuration
        let saga_params = sagas::multicast_group_dpd_ensure::Params {
            serialized_authn:
                nexus_db_queries::authn::saga::Serialized::for_opctx(opctx),
            external_group_id: group.id(),
            underlay_group_id: underlay_group.id,
        };

        debug!(
            opctx.log,
            "initiating DPD transaction saga for multicast forwarding configuration";
            "external_group_id" => %group.id(),
            "external_multicast_ip" => %group.multicast_ip,
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => %underlay_group.multicast_ip,
            "vni" => ?underlay_group.vni,
            "saga_type" => "multicast_group_dpd_ensure",
            "dpd_operation" => "create_external_and_underlay_groups"
        );

        let dag = create_saga_dag::<
            sagas::multicast_group_dpd_ensure::SagaMulticastGroupDpdEnsure,
        >(saga_params)
        .context("failed to create multicast group transaction saga")?;

        let saga_id = self
            .sagas
            .saga_start(dag)
            .await
            .context("failed to start multicast group transaction saga")?;

        debug!(
            opctx.log,
            "DPD multicast forwarding configuration saga initiated";
            "external_group_id" => %group.id(),
            "underlay_group_id" => %underlay_group.id,
            "saga_id" => %saga_id,
            "pending_dpd_operations" => "[create_external_group, create_underlay_group, configure_nat_mapping]",
            "expected_outcome" => "Creating → Active"
        );

        Ok(())
    }

    /// Process a single multicast group in "Deleting" state.
    async fn process_deleting_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        let tag = Self::generate_multicast_tag(group);

        debug!(
            opctx.log,
            "executing DPD multicast group cleanup by tag";
            "group_id" => %group.id(),
            "multicast_ip" => %group.multicast_ip,
            "dpd_tag" => %tag,
            "cleanup_scope" => "all_switches_in_rack",
            "dpd_operation" => "multicast_reset_by_tag",
            "cleanup_includes" => "[external_group, underlay_group, forwarding_rules, member_ports]"
        );

        // Use dataplane client from reconciliation pass to cleanup switch(es)
        // state by tag
        dataplane_client
            .remove_groups(&tag)
            .await
            .context("failed to cleanup dataplane switch configuration")?;

        // Delete underlay group record
        if let Some(underlay_group_id) = group.underlay_group_id {
            self.datastore
                .underlay_multicast_group_delete(opctx, underlay_group_id)
                .await
                .context("failed to delete underlay group from database")?;
        }

        // Delete of external group record
        self.datastore
            .multicast_group_delete(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .context("failed to complete external group deletion")?;

        Ok(())
    }

    /// Verify and reconcile a group on all dataplane switches.
    async fn verify_groups_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        let tag = Self::generate_multicast_tag(group);

        // Use dataplane client from reconciliation pass to query switch state
        let switch_groups = dataplane_client
            .get_groups(&tag)
            .await
            .context("failed to get groups from switches")?;

        // Check if group exists on all switches
        let expected_switches = switch_groups.len();
        let mut switches_with_group = 0;
        let mut needs_reconciliation = false;

        for (location, groups) in &switch_groups {
            let has_groups = !groups.is_empty();
            if has_groups {
                switches_with_group += 1;
                debug!(
                    opctx.log,
                    "found multicast groups on switch";
                    "switch" => %location,
                    "tag" => %tag,
                    "count" => groups.len()
                );
            } else {
                debug!(
                    opctx.log,
                    "missing multicast groups on switch";
                    "switch" => %location,
                    "tag" => %tag
                );
                needs_reconciliation = true;
            }
        }

        // If group is missing from some switches, re-add it
        if needs_reconciliation {
            info!(
                opctx.log,
                "multicast group missing from switches - re-adding";
                "group" => ?group,
                "tag" => %tag,
                "switches_with_group" => switches_with_group,
                "total_switches" => expected_switches
            );

            // Get the external and underlay groups for recreation
            let external_group = self
                .datastore
                .multicast_group_fetch(
                    opctx,
                    MulticastGroupUuid::from_untyped_uuid(group.id()),
                )
                .await
                .context("failed to get external group for verification")?;

            let underlay_group_id = group
                .underlay_group_id
                .context("no underlay group for external group")?;

            let underlay_group = self
                .datastore
                .underlay_multicast_group_fetch(opctx, underlay_group_id)
                .await
                .context("failed to get underlay group for verification")?;

            // Re-create the groups on all switches
            match dataplane_client
                .create_groups(opctx, &external_group, &underlay_group)
                .await
            {
                Ok(_) => {
                    info!(
                        opctx.log,
                        "successfully re-added multicast groups to switches";
                        "group" => ?group,
                        "tag" => %tag
                    );
                }
                Err(
                    omicron_common::api::external::Error::ObjectAlreadyExists {
                        ..
                    },
                ) => {
                    debug!(
                        opctx.log,
                        "multicast groups already exist on some switches - this is expected";
                        "group" => ?group,
                        "tag" => %tag
                    );
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to re-add multicast groups to switches";
                        "group" => ?group,
                        "tag" => %tag,
                        "error" => %e
                    );
                    // Don't fail verification - just log the error and continue
                }
            }
        }

        Ok(())
    }
}
