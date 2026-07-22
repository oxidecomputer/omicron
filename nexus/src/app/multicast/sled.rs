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
//! - **Forwarding entries**: Underlay multicast address to switch next-hop,
//!   installed on all sleds so OPTE forwards to the switch for replication
//!
//! [`dataplane`]: super::dataplane

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use anyhow::Context;
use futures::future::join_all;
use omicron_common::api::external;
use sled_agent_types::early_networking::SwitchSlot;
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
        incumbent_switch: Option<SwitchSlot>,
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

        // Select one of the available switches as the forwarding next hop.
        //
        // OPTE treats each next hop as a duplication it performs itself, so
        // pointing at individual member sleds would cause O(n) copies over
        // cxgbe per sender.
        //
        // A single switch next hop means one copy to the switch, which
        // replicates to member sled ports via DPD multicast group membership.
        // ECMP over both switches is the more correct longer-term answer,
        // but OPTE and mgd lack the tooling to express that today.
        //
        // The next hop should land on the switch that owns this group's
        // external entry in `dataplane`, since that single DPD object carries
        // both ingress NAT and egress forwarding. The reconciler observes that
        // owner during its drift check, and the member-churn paths observe it
        // directly from DPD, both threading it in as `incumbent_switch` so
        // this election prefers the same switch and the sites co-locate.
        // When no owner is supplied (teardown, or the observation saw zero or
        // split ownership), the election falls back to the shared hash, which
        // agrees with ingress in steady state and self-corrects on the next
        // group reconciler pass.
        //
        // The address comes from the same switch discovery that defines the
        // candidate set (Dendrite zones in DNS, kept only when their DPD
        // reports a slot), so an elected switch is always addressable. The
        // MGS-derived switch-zone map is deliberately not used here: a second
        // resolver could elect a switch this path cannot address, black-holing
        // guest egress.
        let switch_addrs =
            crate::app::dpd_switch_underlay_addrs(&self.resolver, &opctx.log)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .context("failed to resolve Dendrite switch addresses")?;

        let switch_ip = select_forwarding_switch_ip(
            group_id,
            &switch_addrs,
            incumbent_switch,
        )
        .context("no reachable switch for forwarding next hop")?;

        let convergence_params = GroupConvergenceParams {
            group_ip,
            underlay_ip,
            group_is_active,
            desired_m2p: &desired_m2p,
            switch_ip,
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
        // Clearing tears down forwarding on every sled, so there is no next
        // hop to elect and no owner to prefer.
        self.propagate_m2p_and_forwarding(opctx, group, None).await
    }
}

/// Resolved group state used to converge M2P and forwarding on each sled.
struct GroupConvergenceParams<'a> {
    group_ip: IpAddr,
    underlay_ip: Ipv6Addr,
    group_is_active: bool,
    desired_m2p: &'a Mcast2PhysMapping,
    /// Switch zone underlay IP chosen as the forwarding next hop.
    /// The switch replicates to member sled ports via DPD config.
    switch_ip: Ipv6Addr,
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
/// When the group is active, this sets a single next hop to the switch
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

    let desired_next_hops = vec![McastForwardingNextHop {
        next_hop: params.switch_ip,
        replication: McastReplication::Underlay,
        filter: McastSourceFilter {
            mode: McastFilterMode::Exclude,
            sources: Vec::new(),
        },
    }];

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

fn select_forwarding_switch_ip(
    group_id: MulticastGroupUuid,
    switch_addrs: &HashMap<SwitchSlot, Ipv6Addr>,
    incumbent_switch: Option<SwitchSlot>,
) -> Option<Ipv6Addr> {
    let slots: Vec<SwitchSlot> = switch_addrs.keys().copied().collect();
    super::select_switch_slot(group_id, &slots, incumbent_switch)
        .and_then(|slot| switch_addrs.get(&slot).copied())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::Ipv6Addr;

    use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};
    use sled_agent_types::early_networking::SwitchSlot;
    use uuid::Uuid;

    use super::select_forwarding_switch_ip;
    use crate::app::multicast::select_switch_slot;

    // Distinct random groups sampled per election test. Group UUIDs are
    // uniform, so a modest sample exercises the hash across inputs rather than
    // a single fixed outcome.
    const HASH_SAMPLES: usize = 32;

    #[test]
    fn select_forwarding_switch_ip_returns_none_when_empty() {
        let group_id = MulticastGroupUuid::from_untyped_uuid(Uuid::new_v4());
        let switch_zone_addrs = HashMap::new();

        assert_eq!(
            select_forwarding_switch_ip(group_id, &switch_zone_addrs, None),
            None
        );
    }

    #[test]
    fn select_forwarding_switch_ip_is_stable_across_map_ordering() {
        let group_id = MulticastGroupUuid::from_untyped_uuid(Uuid::new_v4());
        let switch0 = Ipv6Addr::LOCALHOST;
        let switch1 = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2);

        let mut first = HashMap::new();
        first.insert(SwitchSlot::Switch0, switch0);
        first.insert(SwitchSlot::Switch1, switch1);

        let mut second = HashMap::new();
        second.insert(SwitchSlot::Switch1, switch1);
        second.insert(SwitchSlot::Switch0, switch0);

        assert_eq!(
            select_forwarding_switch_ip(group_id, &first, None),
            select_forwarding_switch_ip(group_id, &second, None)
        );

        // A supplied incumbent is honored regardless of map ordering.
        assert_eq!(
            select_forwarding_switch_ip(
                group_id,
                &first,
                Some(SwitchSlot::Switch1)
            ),
            Some(switch1)
        );
    }

    /// Egress must resolve to the same switch the ingress election picks for a
    /// shared `group_id`, candidate set, and incumbent. This is the
    /// co-location invariant: the guest-sourced forwarding next hop lands on
    /// the switch that owns the group's external entry.
    #[test]
    fn select_forwarding_switch_ip_matches_ingress_election() {
        let switch0 = Ipv6Addr::LOCALHOST;
        let switch1 = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2);
        let mut switch_addrs = HashMap::new();
        switch_addrs.insert(SwitchSlot::Switch0, switch0);
        switch_addrs.insert(SwitchSlot::Switch1, switch1);
        let slots = [SwitchSlot::Switch0, SwitchSlot::Switch1];

        for _ in 0..HASH_SAMPLES {
            let group_id =
                MulticastGroupUuid::from_untyped_uuid(Uuid::new_v4());
            for incumbent in
                [None, Some(SwitchSlot::Switch0), Some(SwitchSlot::Switch1)]
            {
                let ingress =
                    select_switch_slot(group_id, &slots, incumbent).unwrap();
                let egress = select_forwarding_switch_ip(
                    group_id,
                    &switch_addrs,
                    incumbent,
                )
                .unwrap();
                assert_eq!(switch_addrs[&ingress], egress);
            }
        }
    }
}
