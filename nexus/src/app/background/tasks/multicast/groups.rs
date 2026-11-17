// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Group-specific multicast reconciler functions.
//!
//! # RPW Group Processing Model
//!
//! Unlike sagas that orchestrate targeted, synchronous changes, the RPW
//! reconciler ensures the dataplane (via DPD) reflects the intended state from
//! the database.
//! Group processing is idempotent.
//!
//! ## Operations Handled
//! - **"Creating" state**: Initiate DPD "ensure" to apply configuration
//! - **"Active" state**: Detect DPD drift and sync directly
//! - **MRIB programming**: For Active groups, reconcile switch MRIB
//!   routes against a per-pass snapshot (see [`super::mrib`])
//! - **"Deleting" state**: Switch cleanup, MRIB route withdrawal, and
//!   database removal
//! - **M2P/forwarding propagation**: Convergent per-sled propagation of
//!   M2P mappings and forwarding entries via sled-agent after member
//!   state changes
//!
//! # Group State Transition Matrix
//!
//! The RPW reconciler handles all possible state transitions for multicast
//! groups:
//!
//! ## Group State Lifecycle
//! ```text
//! "Creating"                  → "Active" → "Deleting" → (row removed)
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
//! | 1 | Success | Success | Delete DB row | N/A (row removed) |
//! | 2 | Failed | N/A | Log error, retry next pass | "Deleting" (NoChange) |
//! | 3 | Success | Failed | Log error, retry next pass | "Deleting" (NoChange) |
//!
//! Note: "Deleting" is the terminally stored state. DPD cleanup is attempted
//! on every reconciler pass while a group is in the "Deleting" state, and
//! once both the DPD and DB cleanup operations succeed, the row is deleted
//! for good.
//!
//! ## Triggering Events
//! - **"Creating"**: Instance joins group (implicitly creates if needed) → DB inserts with "Creating" state
//! - **"Active"**: DPD ensure saga completes successfully → state = "Active"
//! - **"Deleting"**: Last member leaves group → state = "Deleting" (implicit lifecycle)
//! - **Row removal**: RPW reconciler completes DPD cleanup → deletes the row
//!
//! ## Error Handling
//! - **Saga failures**: Group stays in "Creating", reconciler retries
//! - **DPD failures**: Group stays in current state, logged and retried
//! - **DB failures**: Operations retried in subsequent reconciler passes
//! - **Partial cleanup**: "Deleting" state preserved until complete cleanup

use std::collections::HashSet;
use std::net::IpAddr;

use anyhow::{Context, anyhow};
use futures::future::try_join_all;
use futures::stream::{self, StreamExt};
use mg_api_types::mrib::{MulticastAddr, MulticastRouteKey};
use oxnet::MulticastMac;
use slog::{debug, error, info, trace, warn};
use slog_error_chain::InlineErrorChain;

use dpd_client::types::{ExternalNatTarget, MacAddr, MulticastGroupResponse};
use dpd_types::mcast::{
    ExactSource, SourceEntry, SourceFilter, is_ssm_address,
};
use dpd_types::network::Vni;
use nexus_db_model::{
    MulticastGroup, MulticastGroupState, SqlU8, UnderlayMulticastGroup,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::multicast::EnsureUnderlayResult;
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::api::external::{self, DataPageParams};
use omicron_ddm_admin_client::DEFAULT_MULTICAST_VNI;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};
use sled_agent_types::early_networking::SwitchSlot;

use super::{MulticastGroupReconciler, StateTransition};
use crate::app::multicast::dataplane::{
    GroupUpdateParams, MulticastDataplaneClient,
};
use crate::app::multicast::map_external_to_underlay_ip;
use crate::app::multicast::sled::MulticastSledClient;
use crate::app::multicast::switch_zone::{
    MribRouteIndex, MulticastSwitchZoneClient,
};
use crate::app::saga::create_saga_dag;
use crate::app::sagas;

fn multicast_tag_group_id(tag: &str) -> Option<MulticastGroupUuid> {
    let (id, ip) = tag.split_once(':')?;
    let id = uuid::Uuid::parse_str(id).ok()?;
    let ip: IpAddr = ip.parse().ok()?;
    if !ip.is_multicast() || tag != format!("{id}:{ip}") {
        return None;
    }
    Some(MulticastGroupUuid::from_untyped_uuid(id))
}

/// Check if DPD tag matches the database group's tag.
///
/// Tags use format `{uuid}:{ip}` to prevent collision when group names are reused.
///
/// No Nexus-managed path produces a mismatch: every switch entry is written
/// with the database tag, and teardown removes the entries before the row is
/// deleted. A mismatch therefore implies direct DPD API mutation or a
/// Dendrite defect, and the reconciler cannot repair it in-band because DPD
/// authorizes updates and deletes against the tag already on the switch.
fn dpd_state_matches_tag(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
) -> bool {
    match &db_group.tag {
        Some(db_tag) => dpd_group.tag.as_str() == db_tag,
        None => false,
    }
}

/// Check if DPD sources match the expected state based on source filter.
///
/// Source filtering logic per RFC 4607 (mirrors dataplane code):
/// - SSM ([`dpd_types::mcast::is_ssm_address`]): always expect specific
///   sources. API validation prevents SSM joins without sources.
/// - ASM: expect specific sources when all members specify sources,
///   otherwise `None` to allow any source at the switch level.
fn dpd_state_matches_sources(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    source_filter: &SourceFilterState,
    group: &MulticastGroup,
) -> anyhow::Result<bool> {
    let group_ip = group
        .dpd_multicast_ip()
        .context("invalid stored external multicast address")?;
    let expected = source_filter.for_group(group_ip).map_err(|e| {
        anyhow!(
            "invalid stored multicast source filter for group {}: {e}",
            group.id(),
        )
    })?;
    let entries = dpd_group.sources.as_ref().map(|sources| {
        sources
            .iter()
            .map(|source| match source {
                dpd_client::types::IpSrc::Exact(ip) => {
                    ExactSource::new(*ip).map(SourceEntry::Exact)
                }
                dpd_client::types::IpSrc::Any => Ok(SourceEntry::Any),
            })
            .collect::<Result<Vec<_>, _>>()
    });
    let Ok(entries) = entries.transpose() else {
        return Ok(false);
    };
    Ok(SourceFilter::from_entries(group_ip, entries.as_deref())
        .is_ok_and(|actual| actual == expected))
}

/// Check if DPD forwarding configuration matches what Nexus programs.
///
/// The expected NAT target is rebuilt from the underlay group's IPv6
/// address, its derived multicast MAC, and the group VNI, matching the
/// dataplane create and update paths. Egress VLAN tagging is not yet
/// supported. `vlan_id` is expected to be `None`.
fn dpd_state_matches_forwarding(
    dpd_group: &dpd_client::types::MulticastGroupExternalResponse,
    db_group: &MulticastGroup,
    underlay_group: &UnderlayMulticastGroup,
) -> bool {
    let Ok(internal_ip) = underlay_group.dpd_multicast_ip() else {
        return false;
    };
    let underlay_ipv6 = internal_ip.ip();
    let Ok(vni) = Vni::new(u32::from(db_group.vni.0)) else {
        return false;
    };

    let expected = ExternalNatTarget {
        internal_ip,
        inner_mac: MacAddr { a: underlay_ipv6.derive_multicast_mac() },
        vni,
    };

    dpd_group.internal_forwarding.nat_target == expected
        && dpd_group.external_forwarding.vlan_id.is_none()
}

/// Switch-side clients threaded through group state processors.
struct GroupReconcileClients<'a> {
    dataplane: &'a MulticastDataplaneClient,
    sled: &'a MulticastSledClient,
    switch_zone: &'a MulticastSwitchZoneClient,
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
        clients: &GroupReconcileClients<'_>,
    ) -> Result<StateTransition, anyhow::Error>;

    /// Process a group in "Active" state (check DPD sync status).
    async fn process_active(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        clients: &GroupReconcileClients<'_>,
        mrib_route_index: Option<&MribRouteIndex>,
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
        clients: &GroupReconcileClients<'_>,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_deleting_external_group(
                opctx,
                group,
                clients.dataplane,
                clients.sled,
                clients.switch_zone,
            )
            .await
    }

    async fn process_active(
        &self,
        reconciler: &MulticastGroupReconciler,
        opctx: &OpContext,
        group: &MulticastGroup,
        clients: &GroupReconcileClients<'_>,
        mrib_route_index: Option<&MribRouteIndex>,
    ) -> Result<StateTransition, anyhow::Error> {
        reconciler
            .handle_active_external_group(
                opctx,
                group,
                clients.dataplane,
                clients.sled,
                clients.switch_zone,
                mrib_route_index,
            )
            .await
    }
}

impl MulticastGroupReconciler {
    pub(super) async fn sweep_stale_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
    ) -> Result<(), String> {
        async fn sweep_switch(
            datastore: &DataStore,
            opctx: &OpContext,
            slot: &SwitchSlot,
            client: &dpd_client::Client,
        ) -> anyhow::Result<()> {
            let mut tags = HashSet::new();
            let mut next_page = None;
            loop {
                let page = client
                    .multicast_groups_list(None, next_page.as_deref())
                    .await
                    .with_context(|| {
                        format!("failed to list DPD groups on {slot:?}")
                    })?
                    .into_inner();
                tags.extend(
                    page.items
                        .into_iter()
                        .map(|group| match group {
                            MulticastGroupResponse::External {
                                tag, ..
                            }
                            | MulticastGroupResponse::Underlay {
                                tag, ..
                            } => tag,
                        })
                        .filter_map(|tag| {
                            multicast_tag_group_id(&tag).map(|id| (tag, id))
                        }),
                );
                next_page = page.next_page;
                if next_page.is_none() {
                    break;
                }
            }

            let ids: Vec<_> = tags.iter().map(|(_, id)| *id).collect();
            let present = datastore
                .multicast_groups_present_ids(opctx, &ids)
                .await
                .context("failed to query multicast group IDs")?;
            let stale: Vec<_> = tags
                .into_iter()
                .filter(|(_, id)| !present.contains(id))
                .collect();
            let errors = stream::iter(stale)
                .then(|(tag, _)| async move {
                    let dpd_tag = tag.parse().with_context(|| {
                        format!("invalid multicast tag {tag}")
                    })?;
                    match client.multicast_reset_by_tag(&dpd_tag).await {
                        Ok(_) => {
                            info!(
                                opctx.log,
                                "removed stale DPD multicast groups";
                                "switch" => ?slot,
                                "tag" => %tag,
                            );
                            Ok(())
                        }
                        Err(dpd_client::Error::ErrorResponse(response))
                            if response.status()
                                == reqwest::StatusCode::NOT_FOUND =>
                        {
                            Ok(())
                        }
                        Err(err) => {
                            warn!(
                                opctx.log,
                                "failed to remove stale DPD multicast groups";
                                "switch" => ?slot,
                                "tag" => %tag,
                                "error" => %err,
                            );
                            Err(anyhow!("{slot:?}, tag {tag}: {err}"))
                        }
                    }
                })
                .filter_map(|result: anyhow::Result<()>| async move {
                    result.err().map(|err| format!("{err:#}"))
                })
                .collect::<Vec<_>>()
                .await;
            if errors.is_empty() {
                Ok(())
            } else {
                Err(anyhow!(errors.join("; ")))
            }
        }

        if dataplane_client.switch_clients().is_empty() {
            return Err("no DPD clients available for stale group sweep".into());
        }

        let sweeps: Vec<_> = dataplane_client
            .switch_clients()
            .iter()
            .map(|(slot, client)| {
                sweep_switch(&self.datastore, opctx, slot, client)
            })
            .collect();
        let errors = stream::iter(sweeps)
            .buffer_unordered(self.group_concurrency_limit.get())
            .filter_map(|result| async move {
                result.err().map(|err| format!("{err:#}"))
            })
            .collect::<Vec<_>>()
            .await;

        if errors.is_empty() { Ok(()) } else { Err(errors.join("; ")) }
    }

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
        let group_ip = group
            .dpd_multicast_ip()
            .context("invalid stored external multicast address")?;

        for salt in initial_salt..=u8::MAX {
            let underlay_ip = map_external_to_underlay_ip(group_ip.ip(), salt);

            let result = self
                .datastore
                .ensure_underlay_multicast_group(
                    opctx,
                    group.clone(),
                    underlay_ip,
                )
                .await;

            match result {
                Ok(
                    EnsureUnderlayResult::Created(underlay)
                    | EnsureUnderlayResult::Existing(underlay),
                ) => {
                    if salt != initial_salt {
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

        anyhow::bail!(
            "failed to find non-colliding underlay IP after {} attempts \
             (salt range {initial_salt}..={})",
            u16::from(u8::MAX) - u16::from(initial_salt) + 1,
            u8::MAX
        )
    }

    async fn reconcile_groups_by_state(
        &self,
        opctx: &OpContext,
        state: MulticastGroupState,
        dataplane_client: Option<&MulticastDataplaneClient>,
        sled_client: Option<&MulticastSledClient>,
        switch_zone_client: Option<&MulticastSwitchZoneClient>,
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

        let mrib_route_index = match (state, switch_zone_client) {
            (MulticastGroupState::Active, Some(client)) => client
                .list_routes_indexed()
                .await
                .inspect_err(|e| {
                    warn!(
                        opctx.log,
                        "failed to build per-pass MRIB route snapshot";
                        "error" => %e,
                    )
                })
                .ok(),
            _ => None,
        };
        let mrib_route_index = mrib_route_index.as_ref();

        // Process groups concurrently with configurable parallelism
        let total = groups.len();
        let mut group_outcomes = stream::iter(groups)
            .map(|group| async move {
                let result = self
                    .process_group_state(
                        opctx,
                        &group,
                        dataplane_client,
                        sled_client,
                        switch_zone_client,
                        mrib_route_index,
                    )
                    .await;
                (group, result)
            })
            .buffer_unordered(self.group_concurrency_limit.get());

        // Handle results with state-appropriate logging and counting
        let mut processed = 0;
        while let Some((group, result)) = group_outcomes.next().await {
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
                    };

                    if should_count {
                        processed += 1;
                    }

                    trace!(
                        opctx.log,
                        "processed multicast group";
                        "state" => %state,
                        "group" => ?group,
                        "transition" => ?transition
                    );
                }
                Err(err) => {
                    warn!(
                        opctx.log,
                        "failed to process multicast group";
                        "state" => %state,
                        "group_id" => %group.id(),
                        "multicast_ip" => %group.multicast_ip,
                        "error" => InlineErrorChain::new(&*err)
                    );
                }
            }
        }

        if total > 0 {
            debug!(
                opctx.log,
                "group reconciliation completed";
                "state" => %state,
                "processed" => processed,
                "total" => total
            );
        }

        Ok(processed)
    }

    /// Process multicast groups that are in "Creating" state.
    pub(super) async fn reconcile_creating_groups(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Creating,
            None,
            None,
            None,
        )
        .await
    }

    /// Process multicast groups that are in "Deleting" state.
    pub(super) async fn reconcile_deleting_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        switch_zone_client: &MulticastSwitchZoneClient,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Deleting,
            Some(dataplane_client),
            Some(sled_client),
            Some(switch_zone_client),
        )
        .await
    }

    /// Reconcile active multicast groups with DPD (drift detection and correction).
    pub(super) async fn reconcile_active_groups(
        &self,
        opctx: &OpContext,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        switch_zone_client: &MulticastSwitchZoneClient,
    ) -> Result<usize, String> {
        self.reconcile_groups_by_state(
            opctx,
            MulticastGroupState::Active,
            Some(dataplane_client),
            Some(sled_client),
            Some(switch_zone_client),
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
        sled_client: Option<&MulticastSledClient>,
        switch_zone_client: Option<&MulticastSwitchZoneClient>,
        mrib_route_index: Option<&MribRouteIndex>,
    ) -> Result<StateTransition, anyhow::Error> {
        // Future: Match on group type to select different processors if
        // we add more nuanced group types
        let processor = ExternalGroupProcessor;

        match group.state {
            MulticastGroupState::Creating => {
                processor.process_creating(self, opctx, group).await
            }
            MulticastGroupState::Deleting => {
                let clients = GroupReconcileClients {
                    dataplane: dataplane_client.context(
                        "dataplane client required for deleting state",
                    )?,
                    sled: sled_client
                        .context("sled client required for deleting state")?,
                    switch_zone: switch_zone_client.context(
                        "switch zone client required for deleting state",
                    )?,
                };
                processor.process_deleting(self, opctx, group, &clients).await
            }
            MulticastGroupState::Active => {
                let clients = GroupReconcileClients {
                    dataplane: dataplane_client.context(
                        "dataplane client required for active state",
                    )?,
                    sled: sled_client
                        .context("sled client required for active state")?,
                    switch_zone: switch_zone_client.context(
                        "switch zone client required for active state",
                    )?,
                };
                processor
                    .process_active(
                        self,
                        opctx,
                        group,
                        &clients,
                        mrib_route_index,
                    )
                    .await
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

        // A memberless group is never activated. An implicit creation whose
        // first attach has not landed, or whose attach failed, stays in
        // "Creating" until it gains a member or is reaped by the unified
        // emptiness sweep (`cleanup_empty_groups`). This keeps failed
        // creations out of the dataplane entirely.
        let members = self
            .get_group_members(opctx, group.id())
            .await
            .context("failed to list members for group in 'Creating' state")?;

        if members.is_empty() {
            return Ok(StateTransition::NoChange);
        }

        // TODO: Front port selection for egress (instance → external) is
        // not yet implemented. See RFD 488 (§sect-external-mcast) for
        // the design (mcastd / PIM-SM).

        // Handle underlay group creation/linking (same logic as before)
        if !self.process_creating_group_inner(opctx, group).await? {
            return Ok(StateTransition::EntityGone);
        }

        // The blocking await above ensures the saga has finished
        // applying external + underlay DPD configuration before we
        // return. The reconciler does not write "Active" itself; the
        // saga is the writer. `NoChange` here means this pass made no
        // further state change beyond what the saga already committed.
        Ok(StateTransition::NoChange)
    }

    /// External group handler for groups in "Deleting" state.
    async fn handle_deleting_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        switch_zone_client: &MulticastSwitchZoneClient,
    ) -> Result<StateTransition, anyhow::Error> {
        debug!(
            opctx.log,
            "processing external multicast group 'Deleting' cleanup (switch teardown)";
            "group_id" => %group.id(),
            "group_name" => group.name().as_str(),
            "multicast_ip" => %group.multicast_ip,
            "multicast_scope" => if group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "underlay_group_id" => ?group.underlay_group_id,
            "dpd_cleanup_required" => true
        );

        // Remove MRIB routes so `mg-lower` withdraws DDM advertisements
        // before cleaning up DPD and DB state. Return early on failure so
        // the next pass can retry. Proceeding would delete DB rows and
        // leave stale DDM advertisements.
        //
        // The switch snapshot, not the member source filter, drives which
        // routes are withdrawn. The member cleanup pass hard-deletes rows
        // before this handler runs in the same activation, so a group that
        // reaches "Deleting" may not contain the source list that
        // produced the installed (S,G) routes. Those routes would otherwise
        // remain in the MRIB with no group row left for any later pass to
        // reconcile.
        let group_ip = group.multicast_ip.ip();
        let group_addr = MulticastAddr::new(group_ip)
            .context("invalid stored multicast group for MRIB cleanup")?;
        let installed_routes =
            switch_zone_client.list_routes_indexed().await.context(
                "failed to list MRIB routes for 'Deleting' group cleanup",
            )?;

        // For an ASM group the (*,G) key is withdrawn whether or not the
        // listing shows it. A route the listing missed still gets removed.
        let mut route_keys = HashSet::new();
        if !group_addr.is_ssm() {
            route_keys.insert(MulticastRouteKey::any_source(
                group_addr,
                DEFAULT_MULTICAST_VNI,
            )?);
        }
        route_keys.extend(
            installed_routes
                .keys()
                .filter(|key| {
                    key.group() == group_addr
                        && key.vni() == DEFAULT_MULTICAST_VNI
                })
                .copied(),
        );

        // Each key is a distinct route. We fan out so a group with N routes
        // doesn't pay N round-trips serially.
        try_join_all(route_keys.into_iter().map(|key| async move {
            switch_zone_client.remove_route(key).await.with_context(
                || match key.source() {
                    Some(source) => format!(
                        "failed to remove MRIB (S,G) route for source {source}"
                    ),
                    None => {
                        "failed to remove MRIB (*,G) route for 'Deleting' group"
                            .to_string()
                    }
                },
            )
        }))
        .await?;

        self.process_deleting_group_inner(
            opctx,
            group,
            dataplane_client,
            sled_client,
        )
        .await?;
        Ok(StateTransition::StateChanged)
    }

    /// External group handler for groups in "Active" state.
    ///
    /// Checks if the group's DPD state matches the database state. If not,
    /// we make dataplane calls to sync. This self-corrects DPD drift, with
    /// the exception of tag drift (see [`dpd_state_matches_tag`]).
    async fn handle_active_external_group(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
        switch_zone_client: &MulticastSwitchZoneClient,
        mrib_route_index: Option<&MribRouteIndex>,
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

        if source_filter.specific_sources.is_empty()
            && !source_filter.has_any_source_member
            && self.get_group_members(opctx, group.id()).await?.is_empty()
        {
            super::mrib::reconcile_group(
                opctx,
                &self.datastore,
                switch_zone_client,
                mrib_route_index,
                group,
                &source_filter,
                underlay_group_id,
            )
            .await;
            return Ok(StateTransition::NoChange);
        }

        // Fetch the underlay group up front. The drift check verifies its
        // replication entry on every switch, and the update path below reuses
        // the same record.
        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group for drift check")?;

        // Check if DPD state matches DB state (read-before-write for drift
        // detection). The check expects the entry on every switch, and a
        // fetch failure skips this group's pass, retried on the next
        // activation.
        let group_ip = group
            .dpd_multicast_ip()
            .context("invalid stored external multicast address")?;
        let underlay_ip = underlay_group
            .dpd_multicast_ip()
            .context("invalid stored underlay multicast address")?;
        let check = dataplane_client
            .fetch_external_group_for_drift_check(group_ip, underlay_ip)
            .await
            .context("failed to fetch active group from DPD for drift check")?;

        let needs_update = match &check.external_config {
            Some(dpd_group) => {
                let tag_matches = dpd_state_matches_tag(dpd_group, group);
                let sources_match = dpd_state_matches_sources(
                    dpd_group,
                    &source_filter,
                    group,
                )?;
                let forwarding_matches = dpd_state_matches_forwarding(
                    dpd_group,
                    group,
                    &underlay_group,
                );

                let needs_update =
                    !tag_matches || !sources_match || !forwarding_matches;

                if needs_update {
                    debug!(
                        opctx.log,
                        "detected DPD state mismatch for active group";
                        "group_id" => %group.id(),
                        "tag_matches" => tag_matches,
                        "sources_match" => sources_match,
                        "forwarding_matches" => forwarding_matches
                    );
                }

                needs_update
            }
            None => {
                debug!(
                    opctx.log,
                    "active group missing on a switch in DPD, will update";
                    "group_id" => %group.id()
                );
                true
            }
        };

        // Track whether DPD is in the desired state for this group, either
        // because we just synced it or because it was already in sync. MRIB
        // advertisement below is gated on this. Advertising via MGD/DDM
        // while the switch dataplane state is stale would cause peer sleds
        // to forward traffic to a switch that cannot deliver it.
        let mut dpd_synced = false;

        let res = if needs_update {
            match self.datastore.multicast_group_fetch(opctx, group_id).await {
                Ok(current) if current.state == MulticastGroupState::Active => {
                }
                Ok(current) => {
                    debug!(
                        opctx.log,
                        "group left 'Active' during this pass, skipping \
                         drift correction";
                        "group_id" => %group.id(),
                        "state" => %current.state,
                    );
                    return Ok(StateTransition::NoChange);
                }
                Err(external::Error::ObjectNotFound { .. }) => {
                    debug!(
                        opctx.log,
                        "group deleted during this pass, skipping drift \
                         correction";
                        "group_id" => %group.id(),
                    );
                    return Ok(StateTransition::EntityGone);
                }
                Err(e) => {
                    return Err(anyhow!(e))
                        .context("failed to re-read group before update");
                }
            }

            debug!(
                opctx.log,
                "updating active multicast group in DPD";
                "group_id" => %group.id(),
                "multicast_ip" => %group.multicast_ip
            );

            // Direct dataplane call for drift correction
            // If update fails, we leave existing state and retry on next RPW cycle.
            match dataplane_client
                .update_groups(GroupUpdateParams {
                    external_group: group,
                    underlay_group: &underlay_group,
                    source_filter: &source_filter,
                })
                .await
            {
                Ok(()) => {
                    info!(
                        opctx.log,
                        "drift correction completed for active group";
                        "group_id" => %group.id(),
                        "multicast_ip" => %group.multicast_ip
                    );

                    dpd_synced = true;

                    // Propagate M2P/forwarding to member sleds after DPD
                    // sync to ensure OPTE state is also consistent.
                    if let Err(e) = sled_client
                        .propagate_m2p_and_forwarding(opctx, group)
                        .await
                    {
                        warn!(
                            opctx.log,
                            "failed to propagate M2P/forwarding after \
                             drift correction (will retry)";
                            "group_id" => %group.id(),
                            "error" => %e
                        );
                    }

                    Ok(StateTransition::StateChanged)
                }
                Err(e) => {
                    warn!(
                        opctx.log,
                        "drift correction failed, will retry on next cycle";
                        "group_id" => %group.id(),
                        "error" => %e
                    );
                    Ok(StateTransition::NoChange)
                }
            }
        } else {
            dpd_synced = true;

            // Even when DPD is in sync, propagate M2P/forwarding to
            // member sleds to correct any sled-level drift.
            if let Err(e) =
                sled_client.propagate_m2p_and_forwarding(opctx, group).await
            {
                warn!(
                    opctx.log,
                    "failed to propagate M2P/forwarding (will retry)";
                    "group_id" => %group.id(),
                    "error" => %e
                );
            }

            Ok(StateTransition::NoChange)
        };

        // Reconcile MRIB routes based on whether the group has active
        // ("Joined") members. If all members are "Left", withdraw the DDM
        // advertisement so peer sleds stop sending traffic.
        //
        // Gate on `dpd_synced` so we never advertise via MGD/DDM while
        // the switch DPD state is stale. Advertising peer sleds toward
        // a switch that cannot deliver the traffic would just drop
        // packets at the switch. We retry on the next reconciler pass
        // once DPD is back in sync.
        if dpd_synced {
            super::mrib::reconcile_group(
                opctx,
                &self.datastore,
                switch_zone_client,
                mrib_route_index,
                group,
                &source_filter,
                underlay_group_id,
            )
            .await;
        } else {
            debug!(
                opctx.log,
                "skipping MRIB reconcile: DPD update failed for this group";
                "group_id" => %group.id(),
                "multicast_ip" => %group.multicast_ip,
            );
        }

        res
    }

    /// Process a single multicast group in "Creating" state.
    /// Returns `false` if the group was deleted during processing.
    async fn process_creating_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<bool, anyhow::Error> {
        trace!(
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

                trace!(
                    opctx.log,
                    "found linked underlay group";
                    "group" => ?group,
                    "underlay_group" => ?underlay
                );
                underlay
            }
            None => {
                trace!(
                    opctx.log,
                    "creating new underlay group";
                    "group" => ?group
                );
                match self.ensure_underlay_for_external(opctx, group).await? {
                    Some(underlay) => underlay,
                    None => return Ok(false), // Group deleted during processing
                }
            }
        };

        let is_ssm = is_ssm_address(group.multicast_ip.ip());
        if is_ssm && self.get_group_members(opctx, group.id()).await?.is_empty()
        {
            debug!(
                opctx.log,
                "SSM group has no members yet, deferring DPD programming";
                "group_id" => %group.id(),
                "multicast_ip" => %group.multicast_ip,
            );
            return Ok(true);
        }

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

        let (saga_id, completion) = self
            .sagas
            .saga_run(dag)
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

        // Block this pass on saga completion so subsequent reconciler
        // steps observe "Active" within the same pass. See mod-level
        // "RPW Saga Coordination" for rationale.
        completion.await.context("multicast group transaction saga failed")?;

        Ok(true)
    }

    /// Process a single multicast group in "Deleting" state.
    async fn process_deleting_group_inner(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        dataplane_client: &MulticastDataplaneClient,
        sled_client: &MulticastSledClient,
    ) -> Result<(), anyhow::Error> {
        let tag = Self::get_multicast_tag(group)
            .context("multicast group missing tag")?;

        // Skip dataplane teardown when the group never reached it. The
        // reconciler writes `underlay_group_id` before any switch
        // programming and never clears it, so an absent link proves there
        // is no DPD state to remove.
        if group.underlay_group_id.is_some() {
            // A tag reset issued with no DPD clients returns success without
            // reaching a switch. The DB rows deleted below are what drive a
            // later reset for this tag, so the switch state would be left
            // behind with nothing to reconcile it. We fail here so the rows
            // survive and then a later pass tears the group down against
            // reachable switches.
            if dataplane_client.switch_count() == 0 {
                return Err(anyhow!(
                    "no DPD clients available for 'Deleting' group dataplane \
                     teardown, retrying on a later pass"
                ));
            }

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

            // Clear M2P/forwarding from all sleds before DPD cleanup.
            // This must succeed before deleting DB records, otherwise
            // stale OPTE state would persist on sleds where the clear
            // failed, with no DB record to drive a retry on a later pass.
            sled_client
                .clear_m2p_and_forwarding(opctx, group)
                .await
                .context("failed to clear M2P/forwarding from sleds")?;

            dataplane_client
                .remove_groups(&tag)
                .await
                .context("failed to cleanup dataplane switch configuration")?;
        }

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
    use dpd_client::types::IpSrc;
    use dpd_types::mcast::UnderlayMulticastIpv6;

    use std::collections::BTreeSet;

    use uuid::Uuid;

    use nexus_db_model::{
        ExternalMulticastGroup, ExternalMulticastGroupIdentity, Generation,
        MulticastGroupState, Vni,
    };
    use omicron_common::api::external::IdentityMetadataCreateParams;

    #[test]
    fn test_multicast_tag_group_id() {
        let id = MulticastGroupUuid::new_v4();
        for ip in ["239.1.1.1", "ff0e::123", "ff02::1"] {
            assert_eq!(multicast_tag_group_id(&format!("{id}:{ip}")), Some(id));
        }
        for tag in [
            "operator-tag".to_string(),
            format!("{id}:192.0.2.1"),
            format!("{id}:2001:db8::1"),
            format!("{id}:ff0e:0:0:0:0:0:0:123"),
            format!("{id}:239.1.1.1:extra"),
        ] {
            assert_eq!(multicast_tag_group_id(&tag), None);
        }
    }

    #[tokio::test]
    async fn test_stale_sweep_after_concurrent_recreation() {
        use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
        use nexus_test_utils::resource_helpers::{
            create_default_ip_pools, link_ip_pool, object_create,
        };
        use nexus_types::external_api::ip_pool::{
            IpPool, IpPoolAssignment, IpPoolCreate, IpPoolRange, IpPoolType,
        };
        use nexus_types::multicast::MulticastGroupCreate;
        use omicron_common::address::{IpRange, IpVersion, Ipv4Range};
        use std::num::NonZeroUsize;

        async fn assert_tag_count(
            dataplane: &MulticastDataplaneClient,
            tag: &str,
            count: usize,
        ) {
            assert_eq!(dataplane.switch_count(), 2);
            for (slot, client) in dataplane.switch_clients() {
                let mut groups = Vec::new();
                let mut next_page = None;
                loop {
                    let page = client
                        .multicast_groups_list(None, next_page.as_deref())
                        .await
                        .unwrap()
                        .into_inner();
                    if page.items.is_empty() {
                        break;
                    }
                    groups.extend(page.items);
                    match page.next_page {
                        Some(token) => next_page = Some(token),
                        None => break,
                    }
                }
                let matching = groups
                    .iter()
                    .filter(|group| match group {
                        MulticastGroupResponse::External {
                            tag: actual,
                            ..
                        }
                        | MulticastGroupResponse::Underlay {
                            tag: actual,
                            ..
                        } => actual == tag,
                    })
                    .count();
                assert_eq!(matching, count, "{slot:?}");
            }
        }

        let cptestctx = nexus_test_utils::ControlPlaneBuilder::new(
            "test_stale_sweep_after_concurrent_recreation",
        )
        .with_extra_sled_agents(1)
        .customize_nexus_config(&|config| {
            config.pkg.multicast.enabled = false;
        })
        .start::<crate::Server>()
        .await;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = crate::app::sagas::test_helpers::test_opctx(&cptestctx);
        let new_reconciler = || {
            MulticastGroupReconciler::new(
                datastore.clone(),
                nexus.resolver().clone(),
                nexus.sagas.clone(),
                true,
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                chrono::TimeDelta::seconds(60),
            )
        };
        let reconciler_a = new_reconciler();
        let reconciler_b = new_reconciler();
        let dataplane_a = MulticastDataplaneClient::new(
            nexus.resolver().clone(),
            opctx.log.clone(),
        )
        .await;
        let dataplane_b = MulticastDataplaneClient::new(
            nexus.resolver().clone(),
            opctx.log.clone(),
        )
        .await;
        let client = &cptestctx.external_client;
        create_default_ip_pools(client).await;
        let pool_name = "stale-sweep-pool";
        object_create::<_, IpPool>(
            client,
            "/v1/system/ip-pools",
            &IpPoolCreate {
                identity: IdentityMetadataCreateParams {
                    name: pool_name.parse().unwrap(),
                    description: String::new(),
                },
                ip_version: IpVersion::V4,
                pool_type: IpPoolType::Multicast,
                assignment: IpPoolAssignment::Silos,
            },
        )
        .await;
        object_create::<_, IpPoolRange>(
            client,
            &format!("/v1/system/ip-pools/{pool_name}/ranges/add"),
            &IpRange::V4(
                Ipv4Range::new(
                    "239.70.0.1".parse().unwrap(),
                    "239.70.0.255".parse().unwrap(),
                )
                .unwrap(),
            ),
        )
        .await;
        link_ip_pool(
            client,
            pool_name,
            &DEFAULT_SILO.id().into_untyped_uuid(),
            false,
        )
        .await;

        let group = datastore
            .multicast_group_create(
                &opctx,
                &MulticastGroupCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "stale-sweep-group".parse().unwrap(),
                        description: String::new(),
                    },
                    multicast_ip: Some("239.70.0.100".parse().unwrap()),
                    has_sources: false,
                    ip_version: None,
                },
            )
            .await
            .unwrap();
        let id = MulticastGroupUuid::from_untyped_uuid(group.id());
        let underlay = match datastore
            .ensure_underlay_multicast_group(
                &opctx,
                group.clone(),
                "ff04::ef46:64".parse().unwrap(),
            )
            .await
            .unwrap()
        {
            EnsureUnderlayResult::Created(group) => group,
            result => panic!("unexpected underlay result: {result:?}"),
        };
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::new(),
            has_any_source_member: true,
        };
        let tag = group.tag.as_ref().unwrap();
        dataplane_a
            .create_groups(&group, &underlay, &source_filter)
            .await
            .unwrap();
        reconciler_b.sweep_stale_groups(&opctx, &dataplane_b).await.unwrap();
        assert_tag_count(&dataplane_a, tag, 2).await;

        datastore.multicast_group_set_active(&opctx, id).await.unwrap();
        let stale_group =
            datastore.multicast_group_fetch(&opctx, id).await.unwrap();
        reconciler_b.sweep_stale_groups(&opctx, &dataplane_b).await.unwrap();
        assert_tag_count(&dataplane_a, tag, 2).await;

        assert!(
            datastore
                .multicast_group_mark_removal_if_empty(&opctx, id)
                .await
                .unwrap()
        );
        reconciler_b.sweep_stale_groups(&opctx, &dataplane_b).await.unwrap();
        assert_tag_count(&dataplane_a, tag, 2).await;

        let sled = MulticastSledClient::new(
            datastore.clone(),
            nexus.resolver().clone(),
        );
        let switch_zone = MulticastSwitchZoneClient::new(
            nexus.resolver().clone(),
            opctx.log.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            reconciler_b
                .reconcile_deleting_groups(
                    &opctx,
                    &dataplane_b,
                    &sled,
                    &switch_zone,
                )
                .await
                .unwrap(),
            1,
        );
        assert_tag_count(&dataplane_a, tag, 0).await;
        assert!(
            datastore
                .multicast_groups_present_ids(&opctx, &[id])
                .await
                .unwrap()
                .is_empty()
        );

        dataplane_a
            .update_groups(GroupUpdateParams {
                external_group: &stale_group,
                underlay_group: &underlay,
                source_filter: &source_filter,
            })
            .await
            .unwrap();
        assert_tag_count(&dataplane_a, tag, 2).await;

        reconciler_b.sweep_stale_groups(&opctx, &dataplane_b).await.unwrap();
        assert_tag_count(&dataplane_a, tag, 0).await;
        reconciler_a.sweep_stale_groups(&opctx, &dataplane_a).await.unwrap();
        assert_tag_count(&dataplane_b, tag, 0).await;
        cptestctx.teardown().await;
    }

    fn create_dpd_group(
        sources: Option<Vec<IpSrc>>,
    ) -> dpd_client::types::MulticastGroupExternalResponse {
        dpd_client::types::MulticastGroupExternalResponse {
            group_ip: "232.1.1.1".parse().unwrap(),
            sources,
            tag: "test-tag".to_string(),
            external_group_id: 1,
            external_forwarding: dpd_client::types::ExternalForwarding {
                vlan_id: None,
            },
            internal_forwarding:
                dpd_client::types::ExternalInternalForwarding {
                    nat_target: ExternalNatTarget {
                        internal_ip: UnderlayMulticastIpv6::new(
                            "ff04::1".parse().unwrap(),
                        )
                        .unwrap(),
                        inner_mac: MacAddr {
                            a: "ff04::1"
                                .parse::<std::net::Ipv6Addr>()
                                .unwrap()
                                .derive_multicast_mac(),
                        },
                        vni: omicron_ddm_admin_client::DEFAULT_MULTICAST_VNI,
                    },
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
                "10.0.0.1".parse().unwrap(),
                "10.0.0.2".parse().unwrap(),
            ]),
            has_any_source_member: false,
        };

        let group = create_group("232.1.1.1");

        // DPD has matching sources
        let dpd_group = create_dpd_group(Some(vec![
            IpSrc::Exact("10.0.0.1".parse().unwrap()),
            IpSrc::Exact("10.0.0.2".parse().unwrap()),
        ]));
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        // DPD has sources in different order (should still match)
        let dpd_group = create_dpd_group(Some(vec![
            IpSrc::Exact("10.0.0.2".parse().unwrap()),
            IpSrc::Exact("10.0.0.1".parse().unwrap()),
        ]));
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        // DPD missing sources (mismatch)
        let dpd_group = create_dpd_group(None);
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        // DPD has wrong sources (mismatch)
        let dpd_group = create_dpd_group(Some(vec![
            IpSrc::Exact("10.0.0.1".parse().unwrap()),
            IpSrc::Exact("10.0.0.3".parse().unwrap()), // wrong
        ]));
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );
    }

    #[test]
    fn test_dpd_state_matches_sources_ssm_ignores_has_any_source_member() {
        // SSM address with has_any_source_member=true should still use
        // specific_sources, since RFC 4607 requires a source specification for
        // SSM. The has_any_source_member flag is ignored for SSM because API
        // validation prevents SSM joins without sources.
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::from([
                "10.0.0.1".parse().unwrap(),
                "10.0.0.2".parse().unwrap(),
            ]),
            has_any_source_member: true, // Ignored for SSM
        };

        let group = create_group("232.1.1.1");

        // DPD should have specific sources (RFC 4607 compliance)
        let dpd_group = create_dpd_group(Some(vec![
            IpSrc::Exact("10.0.0.1".parse().unwrap()),
            IpSrc::Exact("10.0.0.2".parse().unwrap()),
        ]));
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        // DPD has None (mismatch: SSM must have sources)
        let dpd_group = create_dpd_group(None);
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );
    }

    #[test]
    fn test_dpd_state_matches_sources_asm_with_specific_sources() {
        // ASM address with specific sources only (no any-source members)
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::from(["10.0.0.1".parse().unwrap()]),
            has_any_source_member: false,
        };

        let group = create_group("224.1.1.1");

        let dpd_group = create_dpd_group(Some(vec![IpSrc::Exact(
            "10.0.0.1".parse().unwrap(),
        )]));
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        let dpd_group = create_dpd_group(None);
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        let dpd_group = create_dpd_group(Some(vec![IpSrc::Any]));
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );
    }

    #[test]
    fn test_dpd_state_matches_sources_asm_with_any_source_member() {
        // ASM address with has_any_source_member=true: we send None to DPD,
        // and DPD canonicalizes any-source representations to None.
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::new(),
            has_any_source_member: true,
        };

        let group = create_group("224.1.1.1");

        let dpd_group = create_dpd_group(None);
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        let dpd_group = create_dpd_group(Some(vec![IpSrc::Exact(
            "10.0.0.1".parse().unwrap(),
        )]));
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );
    }

    #[test]
    fn test_dpd_state_matches_sources_asm_no_sources() {
        // ASM with no source filters at all expects None
        let source_filter = SourceFilterState {
            specific_sources: BTreeSet::new(),
            has_any_source_member: false,
        };

        let group = create_group("224.1.1.1");

        let dpd_group = create_dpd_group(None);
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );

        let dpd_group = create_dpd_group(Some(vec![IpSrc::Exact(
            "10.0.0.1".parse().unwrap(),
        )]));
        assert!(
            !dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .unwrap()
        );
    }

    #[test]
    fn test_dpd_state_matches_sources_rejects_invalid_stored_state() {
        let dpd_group = create_dpd_group(None);
        let source_filter = SourceFilterState::default();
        let group = create_group("232.1.1.1");
        assert!(
            dpd_state_matches_sources(&dpd_group, &source_filter, &group)
                .is_err()
        );
    }
}
