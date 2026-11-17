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

use mg_api_types::mrib::{
    MulticastAddr, MulticastRouteKey, UnderlayMulticastIpv6,
};
use slog::{debug, warn};
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

use nexus_db_model::{MulticastGroup, MulticastGroupMemberState};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_ddm_admin_client::DEFAULT_MULTICAST_VNI;
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

    let ip = underlay_group.multicast_ip.ip();
    let underlay_ip = match underlay_group
        .dpd_multicast_ip()
        .map_err(anyhow::Error::from)
        .and_then(|ip| Ok(UnderlayMulticastIpv6::new(ip.ip())?))
    {
        Ok(underlay_ip) => underlay_ip,
        Err(e) => {
            warn!(
                opctx.log,
                "underlay multicast group address is invalid";
                "group_id" => %group.id(),
                "underlay_ip" => %ip,
                "error" => InlineErrorChain::new(&*e),
            );
            return;
        }
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
    underlay_ip: UnderlayMulticastIpv6,
    has_joined: bool,
) {
    let group_ip = group.multicast_ip.ip();
    let group_addr = match MulticastAddr::new(group_ip) {
        Ok(group_addr) => group_addr,
        Err(e) => {
            warn!(
                opctx.log,
                "invalid stored multicast group for MRIB reconcile";
                "group_id" => %group.id(),
                "group_ip" => %group_ip,
                "error" => InlineErrorChain::new(&e),
            );
            return;
        }
    };
    let current = mrib_route_index
        .into_iter()
        .flat_map(|index| index.iter())
        .filter(|(key, _)| {
            key.group() == group_addr && key.vni() == DEFAULT_MULTICAST_VNI
        })
        .map(|(&key, switches)| (key, switches))
        .collect::<std::collections::HashMap<_, _>>();
    let desired = if has_joined {
        let keys = source_filter
            .specific_sources
            .iter()
            .map(|&s| Some(s))
            .chain(
                (source_filter.has_any_source_member && !group_addr.is_ssm())
                    .then_some(None),
            )
            .map(|source| {
                MulticastRouteKey::new(
                    source,
                    group_addr,
                    DEFAULT_MULTICAST_VNI,
                )
            })
            .collect::<Result<HashSet<_>, _>>();
        match keys {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    opctx.log,
                    "invalid stored multicast source filter for MRIB reconcile";
                    "group_id" => %group.id(),
                    "error" => InlineErrorChain::new(&e),
                );
                return;
            }
        }
    } else {
        HashSet::new()
    };

    for key in &desired {
        if current.get(key).is_some_and(|current_switches| {
            current_switches.len() == switch_zone_client.switch_count()
                && current_switches.values().all(|c| *c == underlay_ip)
        }) {
            continue;
        }
        if let Err(e) = switch_zone_client.add_route(*key, underlay_ip).await {
            warn!(
                opctx.log,
                "failed to ensure MRIB route";
                "group_id" => %group.id(),
                "source" => ?key.source(),
                "error" => %e,
            );
        }
    }

    // Remove routes that are no longer desired. The per-pass snapshot lets us
    // reconcile against current switch state without per-group RPCs.
    for key in current.keys().filter(|key| !desired.contains(*key)) {
        if let Err(e) = switch_zone_client.remove_route(*key).await {
            warn!(
                opctx.log,
                "failed to remove stale MRIB route";
                "group_id" => %group.id(),
                "source" => ?key.source(),
                "error" => %e,
            );
        }
    }

    // Surface RPF flux for diagnostics. The route lands in `mrib_in`
    // after `add_route` but only flows once promoted to `mrib_loc`.
    for key in &desired {
        if !switch_zone_client.route_active_on_all_switches(*key).await {
            debug!(
                opctx.log,
                "MRIB route not yet RPF-verified on all switches";
                "group_id" => %group.id(),
                "group_ip" => %group_ip,
                "source" => ?key.source(),
            );
        }
    }
}
