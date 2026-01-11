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
//! - **"Active" state**: Detect DPD drift and sync directly
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
//!   (saga=external+underlay)  (check+sync)  (cleanup)
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
//! | 1 | Matches DB | No action | "Active" (NoChange) |
//! | 2 | Differs/missing | Direct dataplane call succeeds | "Active" (StateChanged) |
//! | 3 | Differs/missing | Direct dataplane call fails | "Active" (NoChange, retry) |
//!
//! ### DELETING State Transitions
//! | Condition | DPD cleanup (external+underlay) | DB cleanup (row) | Action | Next State |
//! |-----------|-------------------------------|-------------------|--------|------------|
//! | 1 | Success | Success | Delete DB row | "Deleted" (no row) |
//! | 2 | Failed | N/A | Log error, retry next pass | "Deleting" (NoChange) |
//! | 3 | Success | Failed | Log error, retry next pass | "Deleting" (NoChange) |
//!
//! Note: "Deleted" is a terminal outcome (the group row no longer exists). All
//! DPD cleanup happens while in "Deleting"; there are no transitions for
//! "Deleted" because the reconciler no longer sees the group.
//!
//! ## Triggering Events
//! - **"Creating"**: Instance joins group (implicitly creates if needed) → DB inserts with "Creating" state
//! - **"Active"**: DPD ensure saga completes successfully → state = "Active"
//! - **"Deleting"**: Last member leaves group → state = "Deleting" (implicit lifecycle)
//! - **"Deleted"**: RPW reconciler completes DPD cleanup → removes from DB
//!
//! ## Error Handling
//! - **Saga failures**: Group stays in "Creating", reconciler retries
//! - **DPD failures**: Group stays in current state, logged and retried
//! - **DB failures**: Operations retried in subsequent reconciler passes
//! - **Partial cleanup**: "Deleting" state preserved until complete cleanup

use anyhow::Context;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use slog::{debug, error, info, trace, warn};

use nexus_db_model::{MulticastGroup, MulticastGroupState, SqlU8};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::multicast::EnsureUnderlayResult;
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::address::is_ssm_address;
use omicron_common::api::external::{self, DataPageParams};
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::{
    MulticastGroupReconciler, StateTransition, map_external_to_underlay_ip,
};
use crate::app::multicast::dataplane::{
    GroupUpdateParams, MulticastDataplaneClient,
};
use crate::app::saga::create_saga_dag;
use crate::app::sagas;

/// Minimum age before an orphaned group in "Creating" state can be cleaned up.
///
/// This grace period avoids racing with in-progress member attachment operations
/// that occur immediately after group creation.
const ORPHAN_GROUP_MIN_AGE: chrono::Duration = chrono::Duration::seconds(10);

/// Check if DPD tag matches the database group's tag.
///
/// Tags use format `{uuid}:{ip}` to prevent collision when group names are reused.
fn dpd_state_matches_tag(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
) -> bool {
    match (&dpd_group.tag, &db_group.tag) {
        (Some(dpd_tag), Some(db_tag)) => dpd_tag == db_tag,
        _ => false,
    }
}

/// Check if DPD sources match the expected state based on source filter.
///
/// Source filtering logic per RFC 4607 (mirrors dataplane code):
/// - SSM (232/8, ff3x::/32): MUST have specific sources. `has_any_source_member`
///   is ignored because API validation prevents SSM joins without sources.
/// - ASM: Currently expects `None` (Dendrite doesn't support ASM filtering yet).
///
/// TODO: Once Dendrite accepts ASM source filtering, enable it for ASM groups
/// where `has_any_source_member=false`.
fn dpd_state_matches_sources(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    source_filter: &SourceFilterState,
    group: &MulticastGroup,
) -> bool {
    let dpd_sources = dpd_group.sources.clone();
    let group_ip = group.multicast_ip.ip();

    // Expected DPD state based on source filter logic (RFC 4607)
    let expected_sources = if is_ssm_address(group_ip) {
        // SSM: always expect specific sources
        Some(&source_filter.specific_sources)
    } else {
        // ASM: Dendrite doesn't support ASM filtering yet
        // TODO: check `has_any_source_member` to enable/disable filtering
        None
    };

    match (dpd_sources, expected_sources) {
        (None, None) => true,
        (Some(_), None) => false, // DPD has sources but shouldn't
        (None, Some(_)) => false, // DPD missing sources
        (Some(dpd_srcs), Some(expected)) => {
            // Extract exact IPs from DPD sources
            let mut dpd_ips: Vec<_> = dpd_srcs
                .into_iter()
                .filter_map(|src| match src {
                    dpd_client::types::IpSrc::Exact(ip) => Some(ip),
                    _ => None, // Subnet matching removed in follow-up Dendrite TODO
                })
                .collect();
            dpd_ips.sort();

            let mut expected_sorted: Vec<_> =
                expected.iter().copied().collect();
            expected_sorted.sort();

            dpd_ips == expected_sorted
        }
    }
}

/// Check if DPD vlan_id matches database mvlan.
fn dpd_state_matches_mvlan(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
) -> bool {
    let db_mvlan = db_group.mvlan.map(|v| v as u16);
    dpd_group.external_forwarding.vlan_id == db_mvlan
}

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

    /// Process a group in "Active" state (check DPD sync status).
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

    /// Handle groups in "Active" state (check DPD sync status).
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
    /// Ensure an underlay group exists for the given external group.
    ///
    /// Handles the XOR-fold mapping and collision retry with salt increment.
    /// Returns `Some(underlay)` on success, `None` if the group was deleted.
    ///
    /// Salt is a `u8`, so we can try up to 256 different values (0-255).
    async fn ensure_underlay_for_external(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> anyhow::Result<Option<nexus_db_model::UnderlayMulticastGroup>> {
        let initial_salt: u8 = group.underlay_salt.map_or(0, |s| *s);
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        for salt in initial_salt..=u8::MAX {
            let underlay_ip =
                map_external_to_underlay_ip(group.multicast_ip.ip(), salt);

            let result = self
                .datastore
                .ensure_underlay_multicast_group(
                    opctx,
                    group.clone(),
                    underlay_ip.into(),
                )
                .await;

            match result {
                Ok(
                    EnsureUnderlayResult::Created(underlay)
                    | EnsureUnderlayResult::Existing(underlay),
                ) => {
                    if salt != initial_salt {
                        // Persist the new salt. If the group was deleted during
                        // processing, return None; caller handles appropriately.
                        match self
                            .datastore
                            .multicast_group_set_underlay_salt(
                                opctx,
                                group_id,
                                SqlU8::new(salt),
                            )
                            .await
                        {
                            Ok(()) => {}
                            Err(external::Error::ObjectNotFound { .. }) => {
                                info!(
                                    opctx.log,
                                    "Group deleted during salt update";
                                    "group_id" => %group.id(),
                                );
                                return Ok(None);
                            }
                            Err(e) => {
                                return Err(e)
                                    .context("failed to update underlay salt");
                            }
                        }
                    }
                    return Ok(Some(underlay));
                }
                Ok(EnsureUnderlayResult::Collision) => {
                    info!(
                        opctx.log,
                        "Underlay IP collision at salt {salt}, will retry";
                        "group_id" => %group.id(),
                    );
                }
                Err(external::Error::ObjectNotFound { .. }) => {
                    info!(
                        opctx.log,
                        "Group deleted during underlay creation";
                        "group_id" => %group.id(),
                    );
                    return Ok(None);
                }
                Err(e) => {
                    return Err(e).context("failed to ensure underlay group");
                }
            }
        }

        // Exhausted all 256 possible salt values (0-255)
        anyhow::bail!(
            "failed to find non-colliding underlay IP after {} attempts \
             (salt range {initial_salt}..={})",
            u16::from(u8::MAX) - u16::from(initial_salt) + 1,
            u8::MAX
        )
    }

    /// Generic group reconciliation logic for any state.
    ///
    /// This consolidates the common pattern of:
    /// 1. List groups by state
    /// 2. Process concurrently
    /// 3. Collect and log results
    async fn reconcile_groups_by_state(
        &self,
        opctx: &OpContext,
        state: MulticastGroupState,
        dataplane_client: Option<&MulticastDataplaneClient>,
    ) -> Result<usize, String> {
        trace!(opctx.log, "searching for multicast groups"; "state" => %state);

        let groups = self
            .datastore
            .multicast_groups_list_by_state(
                opctx,
                state,
                &DataPageParams::max_page(),
            )
            .await
            .map_err(|e| {
                error!(
                    opctx.log,
                    "failed to list multicast groups";
                    "error" => %e,
                    "state" => %state
                );
                format!("failed to list {state} multicast groups")
            })?;

        trace!(opctx.log, "found multicast groups"; "count" => groups.len(), "state" => %state);

        // Process groups concurrently with configurable parallelism
        let results = stream::iter(groups)
            .map(|group| async move {
                let result = self
                    .process_group_state(opctx, &group, dataplane_client)
                    .await;
                (group, result)
            })
            .buffer_unordered(self.group_concurrency_limit)
            .collect::<Vec<_>>()
            .await;

        // Handle results with state-appropriate logging and counting
        let mut processed = 0;
        let total_results = results.len();
        for (group, result) in results {
            match result {
                Ok(transition) => {
                    // Count successful transitions based on state expectations
                    let should_count = match state {
                        // Creating: count StateChanged and NoChange
                        MulticastGroupState::Creating => matches!(
                            transition,
                            StateTransition::StateChanged
                                | StateTransition::NoChange
                        ),
                        // Deleting: count StateChanged and NeedsCleanup
                        MulticastGroupState::Deleting => matches!(
                            transition,
                            StateTransition::StateChanged
                                | StateTransition::NeedsCleanup
                        ),
                        // Active: count StateChanged and NoChange
                        MulticastGroupState::Active => matches!(
                            transition,
                            StateTransition::StateChanged
                                | StateTransition::NoChange
                        ),
                        MulticastGroupState::Deleted => true,
                    };

                    if should_count {
                        processed += 1;
                    }

                    debug!(
                        opctx.log,
                        "processed multicast group";
                        "state" => %state,
                        "group" => ?group,
                        "transition" => ?transition
                    );
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to process multicast group";
                        "state" => %state,
                        "group" => ?group,
                        "error" => %e
                    );
                }
            }
        }

        if total_results > 0 {
            debug!(
                opctx.log,
                "group reconciliation completed";
                "state" => %state,
                "processed" => processed,
                "total" => total_results
            );
        }

        Ok(processed)
    }

    /// Process multicast groups that are in "Creating" state.
    pub async fn reconcile_creating_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Creating,
            None,
        )
        .await
    }

    /// Process multicast groups that are in "Deleting" state.
    pub async fn reconcile_deleting_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Deleting,
            Some(dataplane_client),
        )
        .await
    }

    /// Reconcile active multicast groups with DPD (drift detection and correction).
    pub async fn reconcile_active_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Active,
            Some(dataplane_client),
        )
        .await
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
                let dataplane_client = dataplane_client
                    .context("dataplane client required for deleting state")?;
                processor
                    .process_deleting(self, opctx, group, dataplane_client)
                    .await
            }
            MulticastGroupState::Active => {
                let dataplane_client = dataplane_client
                    .context("dataplane client required for active state")?;
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
            "processing external multicast group transition: 'Creating' → 'Active'";
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "multicast_ip" => %group.multicast_ip,
            "multicast_scope" => if group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "vni" => ?group.vni,
            "underlay_linked" => group.underlay_group_id.is_some()
        );

        // Clean up orphaned groups stuck in "Creating" state with no members.
        // This handles cases where implicit group creation succeeded but member
        // attachment failed (e.g., SSM validation error, transient failures).
        //
        // We only clean up groups that have been in "Creating" for at least
        // ORPHAN_GROUP_MIN_AGE to avoid racing with in-progress member
        // attachment operations.
        let age = Utc::now() - group.time_created();
        if age > ORPHAN_GROUP_MIN_AGE {
            let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());
            match self
                .datastore
                .mark_multicast_group_for_removal_if_no_members(opctx, group_id)
                .await
            {
                Ok(true) => {
                    info!(
                        opctx.log,
                        "cleaned up orphaned multicast group in \"Creating\" state with no members";
                        "group_id" => %group.id(),
                        "group_name" => group.name().as_str(),
                        "age_seconds" => age.num_seconds(),
                    );
                    return Ok(StateTransition::NeedsCleanup);
                }
                Ok(false) => {
                    // Group has members, continue with normal processing
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to check/cleanup orphaned group";
                        "group_id" => %group.id(),
                        "error" => ?e,
                    );
                }
            }
        }

        // TODO: Add front port selection for egress traffic (instances →
        // external). When transitioning groups to Active, we need to identify
        // and validate front ports against DPD's QSFP topology (similar to
        // `backplane_map` validation for rear ports). These uplink members use
        // `Direction::External` and follow a different lifecycle - added when
        // first instance joins, removed when last instance leaves.
        // Should integrate with `switch_ports_with_uplinks()` or
        // equivalent front port discovery mechanism, which would be
        // configurable, and later learned (i.e., via `mcastd`/IGMP).

        // Handle underlay group creation/linking (same logic as before)
        if !self.process_creating_group_inner(opctx, group).await? {
            return Ok(StateTransition::EntityGone);
        }

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
            "processing external multicast group transition: 'Deleting' → 'Deleted' (switch cleanup)";
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

    /// External group handler for groups in "Active" state.
    ///
    /// Checks if the group's DPD state matches the database state. If not,
    /// we make dataplane calls to sync. This self-corrects any DPD drift.
    async fn handle_active_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        let underlay_group_id = group
            .underlay_group_id
            .context("active multicast group missing underlay_group_id")?;

        // Get source filter state for DPD comparison/update.
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());
        let filter_state_map = self
            .datastore
            .multicast_groups_source_filter_state(opctx, &[group_id])
            .await
            .context("failed to fetch source filter state")?;
        let source_filter =
            filter_state_map.get(&group.id()).cloned().unwrap_or_default();

        // Check if DPD state matches DB state (read-before-write for drift detection)
        let needs_update = match dataplane_client
            .fetch_external_group_for_drift_check(group.multicast_ip.ip())
            .await
        {
            Ok(Some(dpd_group)) => {
                let tag_matches = dpd_state_matches_tag(&dpd_group, group);
                let sources_match = dpd_state_matches_sources(
                    &dpd_group,
                    &source_filter,
                    group,
                );
                let mvlan_matches = dpd_state_matches_mvlan(&dpd_group, group);

                let needs_update =
                    !tag_matches || !sources_match || !mvlan_matches;

                if needs_update {
                    debug!(
                        opctx.log,
                        "detected DPD state mismatch for active group";
                        "group_id" => %group.id(),
                        "tag_matches" => tag_matches,
                        "sources_match" => sources_match,
                        "mvlan_matches" => mvlan_matches
                    );
                }

                needs_update
            }
            Ok(None) => {
                // Group not found in DPD
                debug!(
                    opctx.log,
                    "active group not found in DPD, will update";
                    "group_id" => %group.id()
                );
                true
            }
            Err(e) => {
                // Error fetching from DPD -> log and retry
                warn!(
                    opctx.log,
                    "error fetching active group from DPD, will retry update";
                    "group_id" => %group.id(),
                    "error" => %e
                );
                true
            }
        };

        if needs_update {
            debug!(
                opctx.log,
                "updating active multicast group in DPD";
                "group_id" => %group.id(),
                "multicast_ip" => %group.multicast_ip
            );

            // Fetch underlay group for the update
            let underlay_group = self
                .datastore
                .underlay_multicast_group_fetch(opctx, underlay_group_id)
                .await
                .context(
                    "failed to fetch underlay group for drift correction",
                )?;

            // Direct dataplane call for drift correction
            // If update fails, we leave existing state and retry on next RPW cycle.
            match dataplane_client
                .update_groups(GroupUpdateParams {
                    external_group: group,
                    underlay_group: &underlay_group,
                    new_name: group.name().as_str(),
                    source_filter: &source_filter,
                })
                .await
            {
                Ok(_) => {
                    info!(
                        opctx.log,
                        "drift correction completed for active group";
                        "group_id" => %group.id(),
                        "multicast_ip" => %group.multicast_ip
                    );
                    Ok(StateTransition::StateChanged)
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "drift correction failed, will retry on next cycle";
                        "group_id" => %group.id(),
                        "error" => %e
                    );
                    // Return NoChange so RPW retries on next activation
                    Ok(StateTransition::NoChange)
                }
            }
        } else {
            Ok(StateTransition::NoChange)
        }
    }

    /// Process a single multicast group in "Creating" state.
    /// Returns `false` if the group was deleted during processing.
    async fn process_creating_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<bool, anyhow::Error> {
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
                match self.ensure_underlay_for_external(opctx, &group).await? {
                    Some(underlay) => underlay,
                    None => return Ok(false), // Group deleted during processing
                }
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
            "vni" => ?group.vni,
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

        Ok(true)
    }

    /// Process a single multicast group in "Deleting" state.
    async fn process_deleting_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), anyhow::Error> {
        let tag = Self::get_multicast_tag(group)
            .context("multicast group missing tag")?;

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

        // Delete all membership records for this group
        self.datastore
            .multicast_group_members_delete_by_group(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .context("failed to delete group members from database")?;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;
    use std::net::IpAddr;

    use uuid::Uuid;

    use nexus_db_model::{
        ExternalMulticastGroup, ExternalMulticastGroupIdentity, Generation,
        MulticastGroupState, Vni,
    };
    use omicron_common::api::external::IdentityMetadataCreateParams;

    fn create_dpd_group(
        sources: Option<Vec<dpd_client::types::IpSrc>>,
    ) -> dpd_client::types::MulticastGroupExternalResponse {
        dpd_client::types::MulticastGroupExternalResponse {
            group_ip: "232.1.1.1".parse().unwrap(),
            sources,
            tag: Some("test-tag".to_string()),
            external_group_id: 1,
            external_forwarding: dpd_client::types::ExternalForwarding {
                vlan_id: None,
            },
            internal_forwarding: dpd_client::types::InternalForwarding {
                nat_target: None,
            },
        }
    }

    fn create_group(multicast_ip: &str) -> MulticastGroup {
        ExternalMulticastGroup {
            identity: ExternalMulticastGroupIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: "test-group".parse().unwrap(),
                    description: "test".to_string(),
                },
            ),
            ip_pool_id: Uuid::new_v4(),
            ip_pool_range_id: Uuid::new_v4(),
            vni: Vni(omicron_common::api::external::Vni::DEFAULT_MULTICAST_VNI),
            multicast_ip: multicast_ip.parse().unwrap(),
            mvlan: None,
            underlay_group_id: None,
            underlay_salt: None,
            tag: Some("test-tag".to_string()),
            state: MulticastGroupState::Active,
            version_added: Generation::new(),
            version_removed: None,
        }
    }

    #[test]
    fn test_dpd_state_matches_sources_ssm_with_sources() {
        // SSM address (232.x.x.x) with specific sources from all members
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::from([
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                "10.0.0.2".parse::<IpAddr>().unwrap(),
            ]),
            has_any_source_member: false,
        };

        let group = create_group("232.1.1.1"); // SSM address

        // DPD has matching sources
        let dpd_group = create_dpd_group(Some(vec![
            dpd_client::types::IpSrc::Exact("10.0.0.1".parse().unwrap()),
            dpd_client::types::IpSrc::Exact("10.0.0.2".parse().unwrap()),
        ]));
        assert!(dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD has sources in different order (should still match)
        let dpd_group = create_dpd_group(Some(vec![
            dpd_client::types::IpSrc::Exact("10.0.0.2".parse().unwrap()),
            dpd_client::types::IpSrc::Exact("10.0.0.1".parse().unwrap()),
        ]));
        assert!(dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD missing sources (mismatch)
        let dpd_group = create_dpd_group(None);
        assert!(!dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD has wrong sources (mismatch)
        let dpd_group = create_dpd_group(Some(vec![
            dpd_client::types::IpSrc::Exact("10.0.0.1".parse().unwrap()),
            dpd_client::types::IpSrc::Exact("10.0.0.3".parse().unwrap()), // wrong
        ]));
        assert!(!dpd_state_matches_sources(&dpd_group, &source_filter, &group));
    }

    #[test]
    fn test_dpd_state_matches_sources_ssm_ignores_has_any_source_member() {
        // SSM address with has_any_source_member=true should still use specific_sources
        // per RFC 4607: SSM MUST have source specification. The has_any_source_member
        // flag is ignored for SSM because API validation prevents SSM joins without
        // sources. This is defense-in-depth.
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::from([
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                "10.0.0.2".parse::<IpAddr>().unwrap(),
            ]),
            has_any_source_member: true, // Ignored for SSM
        };

        let group = create_group("232.1.1.1"); // SSM address

        // DPD should have specific sources (RFC 4607 compliance)
        let dpd_group = create_dpd_group(Some(vec![
            dpd_client::types::IpSrc::Exact("10.0.0.1".parse().unwrap()),
            dpd_client::types::IpSrc::Exact("10.0.0.2".parse().unwrap()),
        ]));
        assert!(dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD has None (mismatch: SSM must have sources)
        let dpd_group = create_dpd_group(None);
        assert!(!dpd_state_matches_sources(&dpd_group, &source_filter, &group));
    }

    #[test]
    fn test_dpd_state_matches_sources_asm_address() {
        // ASM address (not 232.x.x.x) - should always expect None from DPD
        // regardless of specific_sources (Dendrite limitation, see TODO)
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::from(["10.0.0.1"
                .parse::<IpAddr>()
                .unwrap()]),
            has_any_source_member: false,
        };

        let group = create_group("224.1.1.1"); // ASM address (not 232.x.x.x)

        // DPD has None (correct for ASM)
        let dpd_group = create_dpd_group(None);
        assert!(dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD has sources (mismatch: ASM should have none)
        let dpd_group =
            create_dpd_group(Some(vec![dpd_client::types::IpSrc::Exact(
                "10.0.0.1".parse().unwrap(),
            )]));
        assert!(!dpd_state_matches_sources(&dpd_group, &source_filter, &group));
    }

    #[test]
    fn test_dpd_state_matches_sources_asm_with_any_source_member() {
        // ASM address with has_any_source_member=true - expects None from DPD
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::new(),
            has_any_source_member: true,
        };

        let group = create_group("224.1.1.1"); // ASM address

        // DPD has None (correct for ASM with any-source members)
        let dpd_group = create_dpd_group(None);
        assert!(dpd_state_matches_sources(&dpd_group, &source_filter, &group));

        // DPD has sources (mismatch: should be none)
        let dpd_group =
            create_dpd_group(Some(vec![dpd_client::types::IpSrc::Exact(
                "10.0.0.1".parse().unwrap(),
            )]));
        assert!(!dpd_state_matches_sources(&dpd_group, &source_filter, &group));
    }
}
