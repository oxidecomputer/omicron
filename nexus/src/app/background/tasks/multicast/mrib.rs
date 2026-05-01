// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! MRIB route reconciliation for active multicast groups.
//!
//! This diffs the desired switch MRIB state, derived from group, member, and
//! source filter records, against a per-pass snapshot fetched by the
//! caller, then issues add/remove RPCs to converge. Best-effort:
//! failures are logged and retried on the next reconciler pass.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv6Addr};

use slog::{debug, warn};
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

use nexus_db_model::{MulticastGroup, MulticastGroupMemberState};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use crate::app::multicast::switch_zone::{
    MribRouteIndex, MulticastSwitchZoneClient,
};

/// Reconcile MRIB routes for a single active group against the per-pass
/// switch snapshot. Withdraws routes when no "Joined" members remain so
/// peer sleds stop sending traffic.
pub(super) async fn reconcile_group(
    opctx: &OpContext,
    datastore: &DataStore,
    switch_zone_client: &MulticastSwitchZoneClient,
    mrib_route_index: Option<&MribRouteIndex>,
    group: &MulticastGroup,
    source_filter: &SourceFilterState,
    underlay_group_id: Uuid,
) {
    let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

    let members = match datastore
        .multicast_group_members_list(
            opctx,
            group_id,
            &DataPageParams::max_page(),
        )
        .await
    {
        Ok(m) => m,
        Err(e) => {
            warn!(
                opctx.log,
                "failed to list members for MRIB reconcile, skipping";
                "group_id" => %group.id(),
                "error" => InlineErrorChain::new(&e),
            );
            return;
        }
    };
    let has_joined =
        members.iter().any(|m| m.state == MulticastGroupMemberState::Joined);

    let underlay_group = match datastore
        .underlay_multicast_group_fetch(opctx, underlay_group_id)
        .await
    {
        Ok(g) => g,
        Err(e) => {
            warn!(
                opctx.log,
                "failed to fetch underlay group for MRIB reconcile, skipping";
                "group_id" => %group.id(),
                "underlay_group_id" => %underlay_group_id,
                "error" => InlineErrorChain::new(&e),
            );
            return;
        }
    };

    let IpAddr::V6(underlay_ip) = underlay_group.multicast_ip.ip() else {
        warn!(
            opctx.log,
            "underlay multicast group has non-IPv6 address";
            "group_id" => %group.id(),
            "underlay_ip" => %underlay_group.multicast_ip.ip(),
        );
        return;
    };

    converge_routes(
        opctx,
        switch_zone_client,
        mrib_route_index,
        group,
        source_filter,
        underlay_ip,
        has_joined,
    )
    .await;
}

/// Diff the per-pass MRIB snapshot against the desired route set and
/// issue add/remove RPCs to converge.
async fn converge_routes(
    opctx: &OpContext,
    switch_zone_client: &MulticastSwitchZoneClient,
    mrib_route_index: Option<&MribRouteIndex>,
    group: &MulticastGroup,
    source_filter: &SourceFilterState,
    underlay_ip: Ipv6Addr,
    has_joined: bool,
) {
    let group_ip = group.multicast_ip.ip();
    let current = mrib_route_index
        .and_then(|index| index.get(&group_ip))
        .cloned()
        .unwrap_or_default();
    let current_sources = current.keys().copied().collect::<HashSet<_>>();
    let desired: HashSet<Option<IpAddr>> = if has_joined {
        source_filter
            .specific_sources
            .iter()
            .map(|s| Some(*s))
            .chain(source_filter.has_any_source_member.then_some(None))
            .collect()
    } else {
        HashSet::new()
    };

    // Ensure desired routes exist.
    for source in &desired {
        let current_switches = current.get(source).cloned().unwrap_or_default();
        if current_switches.len() == switch_zone_client.switch_count()
            && current_switches.values().all(|c| *c == underlay_ip)
        {
            continue;
        }
        if let Err(e) =
            switch_zone_client.add_route(group_ip, underlay_ip, *source).await
        {
            warn!(
                opctx.log,
                "failed to ensure MRIB route";
                "group_id" => %group.id(),
                "source" => ?source,
                "error" => %e,
            );
        }
    }

    // Remove routes no longer desired. The per-pass snapshot lets us
    // reconcile against current switch state without per-group RPCs.
    for source in current_sources.difference(&desired) {
        if let Err(e) = switch_zone_client.remove_route(group_ip, *source).await
        {
            warn!(
                opctx.log,
                "failed to remove stale MRIB route";
                "group_id" => %group.id(),
                "source" => ?source,
                "error" => %e,
            );
        }
    }

    // Surface RPF flux for diagnostics. The route lands in `mrib_in`
    // after `add_route` but only flows once promoted to `mrib_loc`.
    for source in &desired {
        if !switch_zone_client
            .route_active_on_all_switches(group_ip, *source)
            .await
        {
            debug!(
                opctx.log,
                "MRIB route not yet RPF-verified on all switches";
                "group_id" => %group.id(),
                "group_ip" => %group_ip,
                "source" => ?source,
            );
        }
    }
}
