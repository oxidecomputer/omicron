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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use anyhow::Context;
use slog::{debug, info, warn};

use nexus_db_model::{
    MulticastGroup, MulticastGroupMember, MulticastGroupMemberState,
};
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

    /// Look up the current `propolis_id` for an instance.
    async fn lookup_propolis_id(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Result<Option<PropolisUuid>, anyhow::Error> {
        let instance_state = self
            .datastore
            .instance_get_state(opctx, &instance_id)
            .await
            .context("failed to look up instance state")?;

        Ok(instance_state
            .and_then(|s| s.propolis_id)
            .map(PropolisUuid::from_untyped_uuid))
    }

    /// Build the membership descriptor sent to sled-agent for
    /// subscribe/unsubscribe calls.
    fn membership_for(
        group: &MulticastGroup,
        member: &MulticastGroupMember,
    ) -> sled_agent_client::types::InstanceMulticastMembership {
        sled_agent_client::types::InstanceMulticastMembership {
            group_ip: group.multicast_ip.ip(),
            sources: member.source_ips.iter().map(|s| s.ip()).collect(),
        }
    }

    /// Subscribe a VMM to a multicast group via sled-agent.
    ///
    /// Looks up the instance's current `propolis_id` and calls the sled-agent
    /// endpoint to configure OPTE port-level multicast filters. The member's
    /// per-instance source IPs are passed for SSM filtering.
    pub(crate) async fn subscribe_vmm(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
        cached_propolis_id: Option<PropolisUuid>,
    ) -> Result<(), anyhow::Error> {
        let instance_id = InstanceUuid::from_untyped_uuid(member.parent_id);
        // If the instance has no propolis_id (already stopped/destroyed),
        // the OPTE port is gone and there's nothing to subscribe.
        let propolis_id = match cached_propolis_id {
            Some(id) => id,
            None => match self.lookup_propolis_id(opctx, instance_id).await? {
                Some(id) => id,
                None => {
                    debug!(
                        opctx.log,
                        "no propolis_id for instance, skipping subscribe";
                        "member_id" => %member.id,
                        "instance_id" => %instance_id
                    );
                    return Ok(());
                }
            },
        };

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
            "subscribed VMM to multicast group via sled-agent";
            "member_id" => %member.id,
            "propolis_id" => %propolis_id,
            "sled_id" => %sled_id,
            "group_ip" => %group.multicast_ip
        );

        Ok(())
    }

    /// Unsubscribe a VMM from a multicast group via sled-agent.
    ///
    /// Best-effort since if the VMM or sled is already gone, the unsubscribe
    /// is effectively a no-op since the OPTE port was destroyed.
    pub(crate) async fn unsubscribe_vmm(
        &self,
        opctx: &OpContext,
        group: &MulticastGroup,
        member: &MulticastGroupMember,
        sled_id: SledUuid,
        cached_propolis_id: Option<PropolisUuid>,
    ) -> Result<(), anyhow::Error> {
        let instance_id = InstanceUuid::from_untyped_uuid(member.parent_id);

        // If the instance has no propolis_id (already stopped/destroyed),
        // the OPTE port is gone and there's nothing to unsubscribe.
        let propolis_id = match cached_propolis_id {
            Some(id) => id,
            None => match self.lookup_propolis_id(opctx, instance_id).await? {
                Some(id) => id,
                None => {
                    debug!(
                        opctx.log,
                        "no propolis_id for instance, skipping unsubscribe";
                        "member_id" => %member.id,
                        "instance_id" => %instance_id
                    );
                    return Ok(());
                }
            },
        };

        let client = self
            .sled_client(opctx, sled_id)
            .await
            .context("failed to create sled-agent client")?;

        let membership = Self::membership_for(group, member);

        client
            .vmm_leave_multicast_group(&propolis_id, &membership)
            .await
            .context("sled-agent vmm_leave_multicast_group call failed")?;

        debug!(
            opctx.log,
            "unsubscribed VMM from multicast group via sled-agent";
            "member_id" => %member.id,
            "propolis_id" => %propolis_id,
            "sled_id" => %sled_id,
            "group_ip" => %group.multicast_ip
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
        let underlay_group_id = group
            .underlay_group_id
            .context("group missing underlay_group_id")?;

        let underlay_group = self
            .datastore
            .underlay_multicast_group_fetch(opctx, underlay_group_id)
            .await
            .context("failed to fetch underlay group")?;

        let underlay_ip = match underlay_group.multicast_ip.ip() {
            IpAddr::V6(v6) => v6,
            other => anyhow::bail!(
                "underlay multicast address for group {} is {other}, expected IPv6",
                group.id()
            ),
        };

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
        let switch_zone_addrs = crate::app::switch_zone_address_mappings(
            &self.resolver,
            &opctx.log,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to resolve switch zone addresses")?;

        // Hash the group UUID to distribute switch selection across both
        // switches. All Nexuses compute the same hash for a given group,
        // so they agree on the mapping without coordination.
        let mut hasher = DefaultHasher::new();
        group_id.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % switch_zone_addrs.len();
        let switch_ip = switch_zone_addrs
            .iter()
            .nth(idx)
            .map(|(_, ip)| *ip)
            .context("no switch zone found for forwarding next hop")?;

        let convergence_params = GroupConvergenceParams {
            group_ip,
            underlay_ip,
            group_is_active,
            desired_m2p: &desired_m2p,
            switch_ip,
        };

        let mut failed_sleds: usize = 0;

        for sled in &all_sleds {
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
                    failed_sleds += 1;
                    continue;
                }
            };

            if let Err(e) =
                converge_sled_m2p_and_forwarding(&client, &convergence_params)
                    .await
            {
                warn!(
                    opctx.log,
                    "failed to converge M2P/forwarding on sled";
                    "sled_id" => %sled_id,
                    "group_ip" => %group_ip,
                    "error" => %e
                );
                failed_sleds += 1;
            }
        }

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
/// Sets the mapping when the group is active and missing, clears it
/// when the group is inactive and present. Already-correct state
/// is left alone.
async fn converge_m2p(
    client: &sled_agent_client::Client,
    params: &GroupConvergenceParams<'_>,
) -> Result<(), anyhow::Error> {
    let found = client
        .list_mcast_m2p()
        .await
        .context("failed to list M2P mappings on sled")?
        .into_inner();

    let has_m2p = found.iter().any(|m| {
        m.group == params.group_ip && m.underlay == params.underlay_ip
    });

    match (params.group_is_active, has_m2p) {
        // Active group missing M2P: install it.
        (true, false) => {
            client
                .set_mcast_m2p(params.desired_m2p)
                .await
                .context("failed to add M2P mapping to sled")?;
        }
        // Inactive group has stale M2P: remove it.
        (false, true) => {
            let clear = ClearMcast2Phys {
                group: params.group_ip,
                underlay: params.underlay_ip,
            };
            client
                .clear_mcast_m2p(&clear)
                .await
                .context("failed to clear stale M2P from sled")?;
        }
        // Already converged.
        _ => {}
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
