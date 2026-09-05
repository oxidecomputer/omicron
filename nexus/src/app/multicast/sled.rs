// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled-agent multicast operations for OPTE subscriptions, M2P mappings,
//! and forwarding entries.
//!
//! Parallel to [`dataplane`] which handles DPD switch operations, this
//! module manages sled-local multicast state via sled-agent:
//!
//! - **OPTE subscriptions**: Per-VMM multicast group filters on the
//!   hosting sled
//! - **M2P mappings**: Overlay multicast IP to underlay IPv6 address
//!   translation, installed on all sleds
//! - **Forwarding entries**: Underlay multicast address to switch nexthop,
//!   installed on all sleds so OPTE forwards to the switch for replication
//!
//! [`dataplane`]: super::dataplane

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use anyhow::Context;
use futures::future::join_all;
use omicron_common::api::external;
use slog::{debug, info, warn};

use nexus_db_lookup::LookupPath;
use nexus_db_model::{
    MulticastGroup, MulticastGroupMember, MulticastGroupMemberState,
};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::{Asset, Resource};
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, PropolisUuid, SledUuid,
};
use sled_agent_client::types::{
    ClearMcast2Phys, ClearMcastForwarding, Mcast2PhysMapping, McastFilterMode,
    McastForwardingEntry, McastForwardingNextHop, McastReplication,
    McastSourceFilter,
};

/// Utility methods for sled-agent multicast operations used by the
/// background task reconciler.
///
/// Groups sled-agent HTTP calls (OPTE subscriptions, M2P mappings,
/// forwarding entries) behind a single type to keep the reconciler
/// logic focused on state transitions rather than client construction.
///
/// Unlike [`MulticastDataplaneClient`] which pre-builds per-switch
/// clients, sled clients are constructed on demand since the target
/// sled set varies per group.
///
/// [`MulticastDataplaneClient`]: super::dataplane::MulticastDataplaneClient
pub(crate) struct MulticastSledClient {
    datastore: Arc<DataStore>,
    resolver: internal_dns_resolver::Resolver,
}

impl MulticastSledClient {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        resolver: internal_dns_resolver::Resolver,
    ) -> Self {
        Self { datastore, resolver }
    }

    /// Create a sled-agent client for the given sled.
    ///
    /// Looks up the sled's address in the database and constructs an HTTP
    /// client. Follows the same pattern as V2P mapping propagation.
    async fn sled_client(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<sled_agent_client::Client, omicron_common::api::external::Error>
    {
        nexus_networking::sled_client(
            &self.datastore,
            opctx,
            sled_id,
            &opctx.log,
        )
        .await
    }

    /// Build the membership descriptor sent to sled-agent for
    /// subscribe/unsubscribe calls.
    fn membership_for(
        group: &MulticastGroup,
        member: &MulticastGroupMember,
    ) -> sled_agent_client::types::InstanceMulticastMembership {
        Self::membership_for_ip(group.multicast_ip.ip(), member)
    }

    fn membership_for_ip(
        group_ip: IpAddr,
        member: &MulticastGroupMember,
    ) -> sled_agent_client::types::InstanceMulticastMembership {
        sled_agent_client::types::InstanceMulticastMembership {
            group_ip,
            sources: member.source_ips.iter().map(|s| s.ip()).collect(),
        }
    }

    /// Subscribe an instance's active VMM OPTE port to a multicast group.
    ///
    /// Sled-agent resolves the active Propolis under its per-instance state
    /// lock and configures OPTE port-level multicast filters. The member's
    /// per-instance source IPs are passed for SSM filtering. If no active
    /// VMM is registered the call is a noop since the OPTE port is gone.
    pub(crate) async fn subscribe_instance(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
    ) -> Result<(), anyhow::Error> {
        let instance_id = InstanceUuid::from_untyped_uuid(member.parent_id);

        // Read-only dispatch lookup, not a mutation. Authorization for the
        // underlying state change is gated at member-create.
        let (.., authz_instance) = LookupPath::new(opctx, &self.datastore)
            .instance_id(member.parent_id)
            .lookup_for(authz::Action::Read)
            .await
            .context("failed to look up instance for multicast subscribe")?;
        let instance_and_vmm = self
            .datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await
            .context("failed to fetch instance with active VMM")?;
        let Some(vmm) = instance_and_vmm.vmm().as_ref() else {
            debug!(
                opctx.log,
                "instance has no active VMM; skipping multicast subscribe";
                "member_id" => %member.id,
                "instance_id" => %instance_id,
            );
            return Ok(());
        };
        // A racing live migration may invalidate this propolis_id. The
        // next reconciler pass converges against the new active VMM.
        let propolis_id = PropolisUuid::from_untyped_uuid(vmm.id);

        let client = self
            .sled_client(opctx, sled_id)
            .await
            .context("failed to create sled-agent client")?;

        let membership = Self::membership_for(group, member);

        client
            .vmm_join_multicast_group(&propolis_id, &membership)
            .await
            .context("sled-agent vmm_join_multicast_group call failed")?;

        debug!(
            opctx.log,
            "subscribed instance to multicast group via sled-agent";
            "member_id" => %member.id,
            "instance_id" => %instance_id,
            "propolis_id" => %propolis_id,
            "sled_id" => %sled_id,
            "group_ip" => %group.multicast_ip
        );

        Ok(())
    }

    /// Unsubscribe an instance's active VMM OPTE port from a multicast group.
    ///
    /// Best-effort since if the VMM or sled is already gone, the unsubscribe
    /// is effectively a noop because the OPTE port was destroyed.
    pub(crate) async fn unsubscribe_instance(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
    ) -> Result<(), anyhow::Error> {
        self.unsubscribe_vmm_by_ip(
            opctx,
            group.multicast_ip.ip(),
            member,
            sled_id,
        )
        .await
    }

    /// Unsubscribe a VMM when the external group row is unavailable.
    ///
    /// The member row stores the multicast IP, so cleanup can still withdraw
    /// the OPTE filter after the group has been soft- or hard-deleted.
    pub(crate) async fn unsubscribe_vmm_by_ip(
        &self,
        opctx: &OpContext,
        group_ip: IpAddr,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
    ) -> Result<(), anyhow::Error> {
        let instance_id = InstanceUuid::from_untyped_uuid(member.parent_id);

        let (.., authz_instance) = LookupPath::new(opctx, &self.datastore)
            .instance_id(member.parent_id)
            .lookup_for(authz::Action::Read)
            .await
            .context("failed to look up instance for multicast unsubscribe")?;
        let instance_and_vmm = self
            .datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await
            .context("failed to fetch instance with active VMM")?;
        let Some(vmm) = instance_and_vmm.vmm().as_ref() else {
            debug!(
                opctx.log,
                "instance has no active VMM; skipping multicast unsubscribe";
                "member_id" => %member.id,
                "instance_id" => %instance_id,
            );
            return Ok(());
        };
        let propolis_id = PropolisUuid::from_untyped_uuid(vmm.id);

        let client = self
            .sled_client(opctx, sled_id)
            .await
            .context("failed to create sled-agent client")?;

        let membership = Self::membership_for_ip(group_ip, member);

        client
            .vmm_leave_multicast_group(&propolis_id, &membership)
            .await
            .context("sled-agent vmm_leave_multicast_group call failed")?;

        debug!(
            opctx.log,
            "unsubscribed instance from multicast group via sled-agent";
            "member_id" => %member.id,
            "instance_id" => %instance_id,
            "propolis_id" => %propolis_id,
            "sled_id" => %sled_id,
            "group_ip" => %group_ip
        );

        Ok(())
    }

    /// Propagate M2P mappings and forwarding entries to all VPC-routing sleds.
    ///
    /// Performs convergent per-sled propagation: each sled's current state
    /// is queried and diffed against desired state. New entries are added
    /// and stale state is removed (member leaves, instance stops). When no
    /// joined members remain, every sled has stale state and it is cleared.
    ///
    /// # Scope
    ///
    /// M2P mappings and forwarding entries are pushed to all VPC-routing
    /// sleds, not just member sleds. Any instance on any sled may send to
    /// a multicast group address. Hence, without the M2P mapping, OPTE's
    /// overlay layer silently drops the packet. Forwarding entries point
    /// each sled at a switch, which replicates to member ports via DPD
    /// multicast group config. Subscriptions (per-port group membership) remain
    /// member-sled-only.
    pub(crate) async fn propagate_m2p_and_forwarding(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<(), anyhow::Error> {
        let underlay_ip = self
            .resolve_underlay_ip(opctx, group)
            .await
            .with_context(|| {
                format!(
                    "failed to resolve underlay multicast address for group {}",
                    group.id()
                )
            })?;

        let group_ip = group.multicast_ip.ip();

        // Compute desired state from DB, determining which sleds should have
        // M2P and forwarding entries for this group.
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());
        let members = self
            .datastore
            .multicast_group_members_list(
                opctx,
                group_id,
                &DataPageParams::max_page(),
            )
            .await
            .context("failed to list group members")?;

        let member_sled_ids: BTreeSet<SledUuid> = members
            .iter()
            .filter(|m| m.state == MulticastGroupMemberState::Joined)
            .filter_map(|m| m.sled_id.map(SledUuid::from))
            .collect();

        // Build desired M2P entry.
        let desired_m2p =
            Mcast2PhysMapping { group: group_ip, underlay: underlay_ip };

        // The group is active if any members are "Joined". M2P and
        // forwarding are pushed to all sleds when active, cleared
        // from all sleds when inactive.
        let group_is_active = !member_sled_ids.is_empty();

        // Query all VPC-routing sleds for current state and converge.
        let all_sleds = self
            .datastore
            .sled_list_all_batched(opctx, SledFilter::VpcRouting)
            .await
            .context("failed to enumerate sleds")?;

        // Program the switches as this group's forwarding nexthops.
        //
        // OPTE treats each nexthop as a duplication it performs itself, so
        // pointing at individual member sleds would cause O(n) copies over
        // cxgbe per sender.
        //
        // Every switch carries the group's external entry, so any of them
        // is a correct nexthop. We program every
        // reachable switch as a nexthop and OPTE deduplicates: its
        // per-target ECMP selection injects each packet toward exactly one
        // switch, so a copy is never sent down both uplinks. A switch
        // leaving the candidate set simply drops out of the list on the
        // next pass.
        //
        // The addresses come from the same switch discovery that defines
        // the candidate set (Dendrite zones in DNS, kept only when their
        // DPD reports a slot), so every programmed nexthop is addressable.
        // The MGS-derived switch-zone map is not used here: a second
        // resolver could name a switch this path cannot address,
        // black-holing guest egress.
        let switch_addrs =
            crate::app::dpd_switch_underlay_addrs(&self.resolver, &opctx.log)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .context("failed to resolve Dendrite switch addresses")?;

        // Sorted by slot so the nexthop list compares stably against the
        // sled's current entry across passes and Nexus instances.
        let mut slot_addrs: Vec<_> =
            switch_addrs.iter().map(|(slot, ip)| (*slot, *ip)).collect();
        slot_addrs.sort_by_key(|(slot, _)| *slot);
        let switch_ips: Vec<Ipv6Addr> =
            slot_addrs.into_iter().map(|(_, ip)| ip).collect();
        if switch_ips.is_empty() {
            anyhow::bail!("no reachable switch for forwarding nexthop");
        }

        let convergence_params = GroupConvergenceParams {
            group_ip,
            underlay_ip,
            group_is_active,
            desired_m2p: &desired_m2p,
            switch_ips: &switch_ips,
        };

        // Fan out per-sled convergence so a large rack doesn't pay
        // N sequential RPC round-trips. Each sled's RPC is independent, so
        // we accumulate per-sled failures rather than fail-fast.
        let convergence_params = &convergence_params;
        let results = join_all(all_sleds.iter().map(|sled| async move {
            let sled_id: SledUuid = sled.id();
            let client = match self.sled_client(opctx, sled_id).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        opctx.log,
                        "failed to create sled-agent client for \
                         M2P/forwarding convergence";
                        "sled_id" => %sled_id,
                        "error" => %e
                    );
                    return Err(());
                }
            };
            if let Err(e) =
                converge_sled_m2p_and_forwarding(&client, convergence_params)
                    .await
            {
                warn!(
                    opctx.log,
                    "failed to converge M2P/forwarding on sled";
                    "sled_id" => %sled_id,
                    "group_ip" => %group_ip,
                    "error" => %e
                );
                return Err(());
            }
            Ok(())
        }))
        .await;

        let failed_sleds = results.iter().filter(|r| r.is_err()).count();

        info!(
            opctx.log,
            "converged M2P and forwarding state";
            "group_id" => %group.id(),
            "group_ip" => %group_ip,
            "underlay_ip" => %underlay_ip,
            "member_sleds" => member_sled_ids.len(),
            "total_sleds_checked" => all_sleds.len(),
            "failed_sleds" => failed_sleds
        );

        if failed_sleds > 0 {
            anyhow::bail!(
                "failed to converge M2P/forwarding: \
                 {failed_sleds} sled convergence failures \
                 (out of {} sleds)",
                all_sleds.len()
            );
        }

        Ok(())
    }

    async fn resolve_underlay_ip(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<Ipv6Addr, anyhow::Error> {
        let underlay_group_id = group
            .underlay_group_id
            .context("group missing underlay_group_id")?;

        match self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
        {
            Ok(underlay_group) => match underlay_group.multicast_ip.ip() {
                IpAddr::V6(v6) => Ok(v6),
                other => anyhow::bail!(
                    "underlay multicast address for group {} is {other}, \
                     expected IPv6",
                    group.id()
                ),
            },
            Err(external::Error::ObjectNotFound { .. }) => {
                let salt = group.underlay_salt.map_or(0, |s| *s);
                match super::map_external_to_underlay_ip(
                    group.multicast_ip.ip(),
                    salt,
                ) {
                    IpAddr::V6(v6) => Ok(v6),
                    IpAddr::V4(_) => anyhow::bail!(
                        "computed IPv4 underlay address for group {}",
                        group.id()
                    ),
                }
            }
            Err(e) => Err(e).context("failed to fetch underlay group"),
        }
    }

    /// Clear M2P mappings and forwarding entries from all sleds for
    /// this group.
    ///
    /// Delegates to the convergent [`propagate_m2p_and_forwarding`] which
    /// will detect that no joined members remain and clear stale state
    /// from all sleds.
    ///
    /// [`propagate_m2p_and_forwarding`]: Self::propagate_m2p_and_forwarding
    pub(crate) async fn clear_m2p_and_forwarding(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
    ) -> Result<(), anyhow::Error> {
        self.propagate_m2p_and_forwarding(opctx, group).await
    }
}

/// Resolved group state used to converge M2P and forwarding on each sled.
struct GroupConvergenceParams<'a> {
    group_ip: IpAddr,
    underlay_ip: Ipv6Addr,
    group_is_active: bool,
    desired_m2p: &'a Mcast2PhysMapping,
    /// Switch zone underlay IPs programmed as forwarding nexthops, one per
    /// reachable switch (sorted by slot). OPTE's per-target ECMP selection
    /// picks one per packet, and each switch replicates to member sled
    /// ports via DPD config.
    switch_ips: &'a [Ipv6Addr],
}

/// Per-sled convergence of M2P and forwarding state.
///
/// # Errors
///
/// Returns an error when any sled-agent RPC fails (list, set, or clear).
/// The caller increments `failed_sleds` and continues to the next sled.
async fn converge_sled_m2p_and_forwarding(
    client: &sled_agent_client::Client,
    params: &GroupConvergenceParams<'_>,
) -> Result<(), anyhow::Error> {
    converge_m2p(client, params).await?;
    converge_forwarding(client, params).await?;
    Ok(())
}

/// Converge a single sled's M2P mapping for one group.
///
/// Active groups re-run the set unconditionally. The sled applies it
/// as an upsert and re-joins any missing underlay NIC memberships, so
/// a mapping stranded by a failed join rollback (xde entry present,
/// NIC join missing) is healed on a later pass instead of being
/// skipped as already converged. Inactive groups have a present
/// mapping cleared. Same-group mappings under any other underlay are
/// cleared in either case. These can only arise when a clear failed
/// during group teardown and the group IP was later reused with a
/// fresh underlay allocation.
async fn converge_m2p(
    client: &sled_agent_client::Client,
    params: &GroupConvergenceParams<'_>,
) -> Result<(), anyhow::Error> {
    let found = client
        .list_mcast_m2p()
        .await
        .context("failed to list M2P mappings on sled")?
        .into_inner();

    let mut has_m2p = false;
    for mapping in found.iter().filter(|m| m.group == params.group_ip) {
        if mapping.underlay == params.underlay_ip {
            has_m2p = true;
            continue;
        }
        let clear = ClearMcast2Phys {
            group: mapping.group,
            underlay: mapping.underlay,
        };
        client
            .clear_mcast_m2p(&clear)
            .await
            .context("failed to clear stale M2P from sled")?;
    }

    if params.group_is_active {
        // This is set even when the mapping is already listed. A listed
        // mapping does not prove the underlay NIC joins exist, since a failed
        // join rollback can leave the xde entry behind, and the upsert
        // is the retry path for those joins.
        client
            .set_mcast_m2p(params.desired_m2p)
            .await
            .context("failed to add M2P mapping to sled")?;
    } else if has_m2p {
        // Inactive group has stale M2P: remove it.
        let clear = ClearMcast2Phys {
            group: params.group_ip,
            underlay: params.underlay_ip,
        };
        client
            .clear_mcast_m2p(&clear)
            .await
            .context("failed to clear stale M2P from sled")?;
    }

    Ok(())
}

/// Converge a single sled's forwarding entries for one group.
///
/// When the group is active, this sets a single nexthop to the switch
/// zone. The switch replicates to member sled ports via its DPD
/// multicast group membership. When inactive, this clears any stale
/// entries.
async fn converge_forwarding(
    client: &sled_agent_client::Client,
    params: &GroupConvergenceParams<'_>,
) -> Result<(), anyhow::Error> {
    let found = client
        .list_mcast_fwd()
        .await
        .context("failed to list forwarding on sled")?
        .into_inner();

    let current_entry = found.iter().find(|f| f.underlay == params.underlay_ip);

    if !params.group_is_active {
        if current_entry.is_some() {
            let clear = ClearMcastForwarding { underlay: params.underlay_ip };
            client
                .clear_mcast_fwd(&clear)
                .await
                .context("failed to clear stale forwarding from sled")?;
        }
        return Ok(());
    }

    let desired_next_hops: Vec<McastForwardingNextHop> = params
        .switch_ips
        .iter()
        .map(|switch_ip| McastForwardingNextHop {
            next_hop: *switch_ip,
            replication: McastReplication::Underlay,
            filter: McastSourceFilter {
                mode: McastFilterMode::Exclude,
                sources: Vec::new(),
            },
        })
        .collect();

    let needs_update = match current_entry {
        Some(f) => f.next_hops != desired_next_hops,
        None => true,
    };

    if needs_update {
        // OPTE's set_mcast_fwd handler is additive: it inserts next
        // hops but never removes stale ones. Clear first so the
        // subsequent set produces an exact replacement.
        if current_entry.is_some() {
            let clear = ClearMcastForwarding { underlay: params.underlay_ip };
            client
                .clear_mcast_fwd(&clear)
                .await
                .context("failed to clear forwarding before update")?;
        }
        let desired_fwd = McastForwardingEntry {
            underlay: params.underlay_ip,
            next_hops: desired_next_hops,
        };
        client
            .set_mcast_fwd(&desired_fwd)
            .await
            .context("failed to set forwarding on sled")?;
    }

    Ok(())
}
