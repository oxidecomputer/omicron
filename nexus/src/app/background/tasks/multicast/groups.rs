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
//! - **"Active" state**: Detect DPD drift and launch UPDATE saga when DB state differs
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
//! | 2 | Differs from DB | Launch UPDATE saga to fix drift | "Active" (StateChanged) |
//! | 3 | Missing/error | Launch UPDATE saga to fix drift | "Active" (StateChanged) |
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
use slog::{debug, error, trace, warn};

use nexus_db_model::{MulticastGroup, MulticastGroupState};
use nexus_db_queries::context::OpContext;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::{MulticastGroupReconciler, StateTransition};
use crate::app::multicast::dataplane::MulticastDataplaneClient;
use crate::app::saga::create_saga_dag;
use crate::app::sagas;

/// Check if DPD tag matches database name.
fn dpd_state_matches_name(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
) -> bool {
    dpd_group.tag.as_ref().map_or(false, |tag| tag == db_group.name().as_str())
}

/// Check if DPD sources match database sources.
fn dpd_state_matches_sources(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
) -> bool {
    let db_sources: Vec<_> =
        db_group.source_ips.iter().map(|ip| ip.ip()).collect();
    let dpd_sources = dpd_group.sources.clone().unwrap_or_default();

    // Extract exact IPs from DPD sources (filter out subnets)
    let mut dpd_ips: Vec<_> = dpd_sources
        .into_iter()
        .filter_map(|src| match src {
            dpd_client::types::IpSrc::Exact(ip) => Some(ip),
            dpd_client::types::IpSrc::Subnet(_) => None,
        })
        .collect();

    let mut db_sources_sorted = db_sources;
    dpd_ips.sort();
    db_sources_sorted.sort();

    dpd_ips == db_sources_sorted
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
            "processing external multicast group transition: 'Creating' → 'Active'";
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
    /// launches the UPDATE saga to sync. This handles updates triggered by
    /// the UPDATE API endpoint and self-corrects any DPD drift.
    async fn handle_active_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        let underlay_group_id = group.underlay_group_id.ok_or_else(|| {
            anyhow::Error::msg(
                "active multicast group missing underlay_group_id",
            )
        })?;

        // Check if DPD state matches DB state (read-before-write for drift detection)
        let needs_update = match dataplane_client
            .fetch_external_group_for_drift_check(
                opctx,
                group.multicast_ip.ip(),
            )
            .await
        {
            Ok(Some(dpd_group)) => {
                let name_matches = dpd_state_matches_name(&dpd_group, group);
                let sources_match =
                    dpd_state_matches_sources(&dpd_group, group);
                let mvlan_matches = dpd_state_matches_mvlan(&dpd_group, group);

                let needs_update =
                    !name_matches || !sources_match || !mvlan_matches;

                if needs_update {
                    debug!(
                        opctx.log,
                        "detected DPD state mismatch for active group";
                        "group_id" => %group.id(),
                        "name_matches" => name_matches,
                        "sources_match" => sources_match,
                        "mvlan_matches" => mvlan_matches
                    );
                }

                needs_update
            }
            Ok(None) => {
                // Group not found in DPD - need to create
                debug!(
                    opctx.log,
                    "active group not found in DPD, will update";
                    "group_id" => %group.id()
                );
                true
            }
            Err(e) => {
                // Error fetching from DPD - log and retry
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

            let saga_params = sagas::multicast_group_dpd_update::Params {
                serialized_authn:
                    nexus_db_queries::authn::saga::Serialized::for_opctx(opctx),
                external_group_id: group.id(),
                underlay_group_id,
            };

            let dag = create_saga_dag::<
                sagas::multicast_group_dpd_update::SagaMulticastGroupDpdUpdate,
            >(saga_params)
            .context("failed to create multicast group update saga")?;

            let saga_id = self
                .sagas
                .saga_start(dag)
                .await
                .context("failed to start multicast group update saga")?;

            debug!(
                opctx.log,
                "DPD update saga initiated for active group";
                "external_group_id" => %group.id(),
                "saga_id" => %saga_id,
            );

            Ok(StateTransition::StateChanged)
        } else {
            Ok(StateTransition::NoChange)
        }
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
                let underlay_ip = self
                    .map_external_to_underlay_ip(group.multicast_ip.ip())
                    .context(
                        "failed to map customer multicast IP to underlay",
                    )?;

                let new_underlay = self
                    .datastore
                    .ensure_underlay_multicast_group(
                        opctx,
                        group.clone(),
                        underlay_ip.into(),
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
}
