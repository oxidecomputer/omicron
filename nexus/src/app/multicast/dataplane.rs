// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared multicast group dataplane operations for sagas and reconciler.
//!
//! Unified interface for multicast group operations in the dataplane
//! (DPD - Data Plane Daemon): creating, updating, and deleting external and
//! underlay groups, including source filters.
//!
//! This client does not program member state. Under RFD 488, `ddmd` derives
//! rear-port underlay members from DDM peer subscriptions and mg-lower programs
//! them into DPD; OPTE/front-port member subscriptions are handled by the sled
//! client.
//!
//! ## VNI and Forwarding Model
//!
//! All external multicast groups use `DEFAULT_MULTICAST_VNI` (77), a reserved
//! system VNI below `MIN_GUEST_VNI` (1024). The bifurcated architecture uses
//! NAT translation at switches:
//!
//! - External multicast packets arrive with VNI 77
//! - Switches perform NAT translation to underlay IPv6 multicast addresses
//! - Forwarding decisions happen at the underlay layer
//! - Security relies on underlay group membership validation
//!
//! The underlay IPv6 addresses live within the fixed admin-local prefix
//! [`UNDERLAY_MULTICAST_SUBNET`] (ff04::/64).
//!
//! This enables cross-project and cross-silo multicast while maintaining
//! security through API authorization and underlay membership control.
//!
//! External senders reach the rack through ordinary multicast routing on the
//! upstream network (PIM, IGMP snooping, or static routes toward the rack
//! uplinks). Only the elected switch carries the NAT ingress entry for a
//! group (see [`select_switch_slot`]). A copy arriving at the non-elected
//! switch is dropped because it has no matching entry. Test environments
//! without an upstream multicast router mirror frames to both switches and
//! rely on that same drop behavior.
//!
//! The election is one-way: nothing signals to the upstream network which
//! switch was elected, so deployments must deliver each group's stream
//! toward both switches' uplinks (static joins or equivalent) and rely on
//! the non-elected drop. Delivery reaching only the non-elected switch
//! drops until slot ownership moves.
//!
//! TODO(RFD 488 §sect-external-mcast, mcastd): couple NAT ownership to
//! upstream delivery. The elected switch originates IGMP/MLD membership
//! reports on its uplinks (host-side proxy reporting, RFC 4605), a new
//! owner reports and confirms delivery before the NAT ingress entry
//! moves, and the old owner leaves last.
//!
//! ## Source Filtering (IGMPv3/MLDv2)
//!
//! Source IPs are stored **per-member** in the control plane, allowing each
//! receiver to subscribe to different sources within a group. DPD enforces
//! source filtering at the **group level** using the union of all member
//! sources.
//!
//! - **SSM addresses** (232/8, ff3x::/32): Sources are **required** per-member.
//!   Each member must specify at least one source IP.
//! - **ASM addresses**: Sources are **optional** per-member. Empty sources
//!   means receive from any source; non-empty enables source filtering.
//!
//! [`UNDERLAY_MULTICAST_SUBNET`]: omicron_common::address::UNDERLAY_MULTICAST_SUBNET
//! [`select_switch_slot`]: super::select_switch_slot

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use futures::future::{join_all, try_join_all};
use oxnet::MulticastMac;
use slog::{Logger, debug, error, info, warn};

use dpd_client::Error as DpdError;
use dpd_client::types::{
    ExternalForwarding, InternalForwarding, IpSrc, MacAddr,
    MulticastGroupCreateExternalEntry, MulticastGroupCreateUnderlayEntry,
    MulticastGroupExternalResponse, MulticastGroupResponse,
    MulticastGroupUnderlayResponse, MulticastGroupUpdateExternalEntry,
    MulticastTag, NatTarget, UnderlayMulticastIpv6, Vni,
};
use internal_dns_resolver::Resolver;

use nexus_db_model::{ExternalMulticastGroup, UnderlayMulticastGroup};
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::address::is_ssm_address;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};
use sled_agent_types::early_networking::SwitchSlot;

use super::select_switch_slot;
use crate::app::dpd_clients;

/// Trait for extracting external responses from mixed DPD response types.
trait IntoExternalResponse {
    /// Extract external response, failing if the response is not external.
    fn into_external_response(
        self,
    ) -> Result<MulticastGroupExternalResponse, Error>;
}

impl IntoExternalResponse for MulticastGroupResponse {
    fn into_external_response(
        self,
    ) -> Result<MulticastGroupExternalResponse, Error> {
        match self {
            MulticastGroupResponse::External {
                group_ip,
                external_group_id,
                tag,
                internal_forwarding,
                external_forwarding,
                sources,
            } => Ok(MulticastGroupExternalResponse {
                group_ip,
                external_group_id,
                tag,
                internal_forwarding,
                external_forwarding,
                sources,
            }),
            _ => {
                Err(Error::internal_error("expected external group from get()"))
            }
        }
    }
}

/// Convert an [`IpAddr`] into a DPD [`UnderlayMulticastIpv6`],
/// rejecting IPv4.
///
/// Note: named without the `Ipv6` suffix because the input type is the general
/// `IpAddr`.
trait IntoUnderlayMulticast {
    /// Convert to [`UnderlayMulticastIpv6`], rejecting IPv4 addresses.
    fn into_underlay_multicast(self) -> Result<UnderlayMulticastIpv6, Error>;
}

impl IntoUnderlayMulticast for IpAddr {
    fn into_underlay_multicast(self) -> Result<UnderlayMulticastIpv6, Error> {
        match self {
            IpAddr::V6(ipv6) => UnderlayMulticastIpv6::try_from(ipv6)
                .map_err(|e| Error::invalid_request(e.to_string())),
            IpAddr::V4(_) => Err(Error::invalid_request(
                "underlay multicast groups must use IPv6 addresses",
            )),
        }
    }
}

/// Result type for multicast dataplane operations.
pub(crate) type MulticastDataplaneResult<T> = Result<T, Error>;

/// Client for multicast dataplane operations.
///
/// This handles multicast group and member operations across all switches
/// in the rack, with automatic error handling and rollback.
///
/// TODO: Egress (instance → external) is not yet supported. The current
/// implementation only handles ingress (external → switches → instances)
/// using rear ports with [`dpd_client::types::Direction::Underlay`]. See
/// RFD 488 (§sect-external-mcast) for the egress design, which will use
/// front-port uplink members with [`dpd_client::types::Direction::External`].
pub(crate) struct MulticastDataplaneClient {
    dpd_clients: HashMap<SwitchSlot, dpd_client::Client>,
    log: Logger,
}

/// Parameters for multicast group updates.
#[derive(Debug)]
pub(crate) struct GroupUpdateParams<'a> {
    pub external_group: &'a ExternalMulticastGroup,
    pub underlay_group: &'a UnderlayMulticastGroup,
    pub source_filter: &'a SourceFilterState,
    /// The switch currently holding the external entry, as observed by the
    /// preceding drift check. Used as an election hint so the update prefers
    /// the same owner without re-observing DPD. `None` falls back to the hash.
    pub incumbent_switch: Option<SwitchSlot>,
}

/// Outcome of [`MulticastDataplaneClient::fetch_external_group_for_drift_check`].
pub(crate) struct ExternalDriftCheck {
    /// The switch currently holding the external entry, if exactly one does.
    /// Threaded into [`GroupUpdateParams::incumbent_switch`] so a follow-up
    /// update elects the same switch the check observed.
    pub incumbent_switch: Option<SwitchSlot>,
    /// The elected switch's config when the entry is correctly placed there,
    /// `None` when the reconciler must re-issue an update.
    pub elected_config: Option<MulticastGroupExternalResponse>,
    /// Whether the entry was observed on a non-elected switch, a stale
    /// forwarder the follow-up update evicts. Surfaced so the reconciler can
    /// count placement drift in its task status.
    pub misplaced: bool,
}

/// The single switch holding the external entry, or `None` when zero or more
/// than one does. A transient duplicate across switches is not a stable owner,
/// so only an unambiguous single holder is treated as the incumbent.
fn single_external_owner(slots: &[SwitchSlot]) -> Option<SwitchSlot> {
    match slots {
        [owner] => Some(*owner),
        _ => None,
    }
}

/// Bound DPD client construction. On timeout (or DNS failure) we yield
/// an empty client map rather than failing the pass: group operations
/// skip with no switches, but DB-only member-state transitions
/// ("Joining" → "Left" when the instance is stopped) still proceed.
const DPD_CLIENT_BUILD_TIMEOUT: Duration =
    // Caps the internal-DNS retry budget for `_dendrite._tcp` so a DPD
    // outage doesn't starve the bg task's idle window.
    Duration::from_secs(5);

impl MulticastDataplaneClient {
    /// Create a new client - builds fresh DPD clients for current switch
    /// topology.
    pub(crate) async fn new(
        resolver: Resolver,
        log: Logger,
    ) -> MulticastDataplaneResult<Self> {
        let dpd_clients = match tokio::time::timeout(
            DPD_CLIENT_BUILD_TIMEOUT,
            dpd_clients(&resolver, &log),
        )
        .await
        {
            Ok(Ok(clients)) => clients,
            Ok(Err(e)) => {
                warn!(
                    log,
                    "failed to build DPD clients, continuing with empty \
                     client map";
                    "error" => %e,
                );
                HashMap::new()
            }
            Err(_) => {
                warn!(
                    log,
                    "timed out building DPD clients, continuing with empty \
                     client map";
                    "timeout" => ?DPD_CLIENT_BUILD_TIMEOUT,
                );
                HashMap::new()
            }
        };
        Ok(Self { dpd_clients, log })
    }

    /// Compute DPD source filter from aggregated member source state.
    ///
    /// For SSM addresses, always returns specific sources. For ASM addresses,
    /// returns `None` (any source) if any member omitted sources, otherwise
    /// returns the union of all member sources.
    fn compute_sources_for_dpd(
        external_group_ip: IpAddr,
        source_filter: &SourceFilterState,
    ) -> Option<Vec<IpSrc>> {
        if is_ssm_address(external_group_ip)
            || !source_filter.has_any_source_member
        {
            Some(
                source_filter
                    .specific_sources
                    .iter()
                    .map(|ip| dpd_client::types::IpSrc::Exact(*ip))
                    .collect(),
            )
        } else {
            None
        }
    }

    async fn dpd_ensure_underlay_created(
        &self,
        client: &dpd_client::Client,
        ip: UnderlayMulticastIpv6,
        tag: &str,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<MulticastGroupUnderlayResponse> {
        let create = MulticastGroupCreateUnderlayEntry {
            group_ip: ip.clone(),
            members: Vec::new(),
            tag: Some(tag.to_string()),
        };
        match client.multicast_group_create_underlay(&create).await {
            Ok(r) => Ok(r.into_inner()),
            Err(DpdError::ErrorResponse(resp))
                if resp.status() == reqwest::StatusCode::CONFLICT =>
            {
                debug!(
                    self.log,
                    "underlay exists; fetching";
                    "underlay_ip" => %ip,
                    "switch" => ?switch,
                    "dpd_operation" => "dpd_ensure_underlay_created"
                );
                Ok(client
                    .multicast_group_get_underlay(&ip)
                    .await
                    .map_err(|e| {
                        error!(
                            self.log,
                            "underlay fetch failed";
                            "underlay_ip" => %ip,
                            "switch" => ?switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_ensure_underlay_created"
                        );
                        Error::internal_error("underlay fetch failed")
                    })?
                    .into_inner())
            }
            Err(e) => {
                warn!(
                    self.log,
                    "underlay create failed";
                    "underlay_ip" => %ip,
                    "switch" => ?switch,
                    "error" => %e,
                    "dpd_operation" => "dpd_ensure_underlay_created"
                );
                Err(Error::internal_error("underlay create failed"))
            }
        }
    }

    async fn dpd_ensure_external_created(
        &self,
        client: &dpd_client::Client,
        create: &MulticastGroupCreateExternalEntry,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<MulticastGroupExternalResponse> {
        match client.multicast_group_create_external(create).await {
            Ok(r) => Ok(r.into_inner()),
            Err(DpdError::ErrorResponse(resp))
                if resp.status() == reqwest::StatusCode::CONFLICT =>
            {
                debug!(
                    self.log,
                    "external exists; fetching";
                    "external_ip" => %create.group_ip,
                    "switch" => ?switch,
                    "dpd_operation" => "dpd_ensure_external_created"
                );
                let response = client
                    .multicast_group_get(&create.group_ip)
                    .await
                    .map_err(|e| {
                        error!(
                            self.log,
                            "external fetch failed";
                            "external_ip" => %create.group_ip,
                            "switch" => ?switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_ensure_external_created"
                        );
                        Error::internal_error("external fetch failed")
                    })?;
                Ok(response.into_inner().into_external_response()?)
            }
            Err(e) => {
                warn!(
                    self.log,
                    "external create failed";
                    "external_ip" => %create.group_ip,
                    "switch" => ?switch,
                    "error" => %e,
                    "dpd_operation" => "dpd_ensure_external_created"
                );
                Err(Error::internal_error("external create failed"))
            }
        }
    }

    /// Ensure the external NAT-ingress entry for a group is deleted on a switch.
    ///
    /// Only the elected forwarder switch carries a group's external entry so
    /// that one switch ingresses and replicates the group. This evicts the
    /// entry from a non-elected switch, which also converges a stale forwarder
    /// after re-election. Tolerates a missing entry so the call is idempotent
    /// across reconciliation passes. The underlay group is left in place so the
    /// switch stays a warm failover candidate.
    async fn dpd_ensure_external_deleted(
        &self,
        client: &dpd_client::Client,
        group_ip: IpAddr,
        tag: &MulticastTag,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<()> {
        match client.multicast_group_delete(&group_ip, tag).await {
            Ok(_) => {
                debug!(
                    self.log,
                    "removed external group from non-elected switch";
                    "external_ip" => %group_ip,
                    "switch" => ?switch,
                    "dpd_operation" => "dpd_ensure_external_deleted"
                );
                Ok(())
            }
            Err(DpdError::ErrorResponse(resp))
                if resp.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                Ok(())
            }
            Err(e) => {
                warn!(
                    self.log,
                    "failed to remove external group from non-elected switch";
                    "external_ip" => %group_ip,
                    "switch" => ?switch,
                    "error" => %e,
                    "dpd_operation" => "dpd_ensure_external_deleted"
                );
                Err(Error::internal_error(
                    "failed to remove external group from non-elected switch",
                ))
            }
        }
    }

    async fn dpd_update_external_or_create(
        &self,
        client: &dpd_client::Client,
        group_ip: IpAddr,
        tag: &MulticastTag,
        update: &MulticastGroupUpdateExternalEntry,
        create: &MulticastGroupCreateExternalEntry,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<MulticastGroupExternalResponse> {
        match client
            .multicast_group_update_external(&group_ip, tag, update)
            .await
        {
            Ok(r) => Ok(r.into_inner()),
            Err(DpdError::ErrorResponse(resp))
                if resp.status() == reqwest::StatusCode::NOT_FOUND =>
            {
                // Create missing, then fetch-or-return
                match client.multicast_group_create_external(create).await {
                    Ok(r) => Ok(r.into_inner()),
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::CONFLICT =>
                    {
                        let response = client
                            .multicast_group_get(&group_ip)
                            .await
                            .map_err(|e| {
                                error!(
                                    self.log,
                                    "external fetch after conflict failed";
                                    "external_ip" => %group_ip,
                                    "switch" => ?switch,
                                    "error" => %e,
                                    "dpd_operation" => "dpd_update_external_or_create"
                                );
                                Error::internal_error(
                                    "external fetch after conflict failed",
                                )
                            })?;
                        Ok(response.into_inner().into_external_response()?)
                    }
                    Err(e) => {
                        error!(
                            self.log,
                            "external ensure failed";
                            "external_ip" => %group_ip,
                            "switch" => ?switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_update_external_or_create"
                        );
                        Err(Error::internal_error("external ensure failed"))
                    }
                }
            }
            Err(e) => {
                warn!(
                    self.log,
                    "external update failed";
                    "external_ip" => %group_ip,
                    "switch" => ?switch,
                    "error" => %e,
                    "dpd_operation" => "dpd_update_external_or_create"
                );
                Err(Error::internal_error("external update failed"))
            }
        }
    }

    /// Get the number of switches this client is managing.
    pub(crate) fn switch_count(&self) -> usize {
        self.dpd_clients.len()
    }

    /// Apply multicast group configuration across switches (via DPD).
    ///
    /// # Arguments
    ///
    /// * `external_group`: Customer-visible group (overlay IP, VNI, identity).
    /// * `underlay_group`: Underlay group derived from `external_group`
    ///   (ff04::/64 address that DPD programs into the dataplane).
    /// * `source_filter`: Aggregated source filtering state across all
    ///   members. If any member requests "any source", switch-level
    ///   filtering is disabled.
    pub(crate) async fn create_groups(
        &self,
        external_group: &ExternalMulticastGroup,
        underlay_group: &UnderlayMulticastGroup,
        source_filter: &SourceFilterState,
    ) -> MulticastDataplaneResult<(
        MulticastGroupUnderlayResponse,
        MulticastGroupExternalResponse,
    )> {
        debug!(
            self.log,
            "DPD multicast group creation initiated across rack switches";
            "external_group_id" => %external_group.id(),
            "external_multicast_ip" => %external_group.multicast_ip,
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => %underlay_group.multicast_ip,
            "vni" => ?external_group.vni,
            "switch_count" => self.switch_count(),
            "multicast_scope" => if external_group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
            "address_mode" => if is_ssm_address(external_group.multicast_ip.ip()) { "SSM" } else { "ASM" },
            "has_any_source_member" => source_filter.has_any_source_member,
            "specific_sources_count" => source_filter.specific_sources.len(),
            "dpd_operation" => "create_groups"
        );

        let dpd_clients = &self.dpd_clients;
        let tag = external_group.tag.as_ref().ok_or_else(|| {
            Error::internal_error("multicast group missing tag")
        })?;

        let underlay_ip_admin =
            underlay_group.multicast_ip.ip().into_underlay_multicast()?;
        let underlay_ipv6 = match underlay_group.multicast_ip.ip() {
            IpAddr::V6(ipv6) => ipv6,
            IpAddr::V4(_) => {
                return Err(Error::internal_error(
                    "underlay multicast groups must use IPv6 addresses",
                ));
            }
        };

        let nat_target = NatTarget {
            internal_ip: underlay_ipv6,
            inner_mac: MacAddr { a: underlay_ipv6.derive_multicast_mac() },
            vni: Vni::from(u32::from(external_group.vni.0)),
        };
        let external_group_ip = external_group.multicast_ip.ip();
        let sources_dpd =
            Self::compute_sources_for_dpd(external_group_ip, source_filter);

        // Elect one switch to own this group's external NAT ingress so a
        // single switch ingresses and replicates it. The underlay group is
        // created on every switch regardless, keeping each switch a warm
        // failover candidate whose OIL mg-lower keeps populated.
        //
        // The reconciler threads this owner into the sled-sourced egress next
        // hop in `sled::propagate_m2p_and_forwarding`, so guest egress and this
        // external entry stay co-located on one switch.
        let group_id =
            MulticastGroupUuid::from_untyped_uuid(external_group.id());
        let available: Vec<SwitchSlot> = dpd_clients.keys().copied().collect();
        // Initial creation, so nothing is programmed yet and there is no
        // current owner to prefer.
        let elected = select_switch_slot(group_id, &available, None);
        // One line per group lifetime, at info so operators can answer
        // "which switch owns this group's ingress" without debug logging.
        info!(
            self.log,
            "elected switch to own external multicast ingress";
            "external_group_id" => %external_group.id(),
            "multicast_ip" => %external_group_ip,
            "elected_switch" => ?elected,
            "dpd_operation" => "create_groups"
        );
        let dpd_tag: MulticastTag = tag.parse().map_err(|_| {
            Error::internal_error("invalid multicast tag for external removal")
        })?;

        let create_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let tag = tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                let underlay_ip_admin = underlay_ip_admin.clone();
                let dpd_tag = dpd_tag.clone();
                let is_elected = Some(*switch_slot) == elected;
                async move {
                    // Ensure underlay is present idempotently
                    let underlay_response = self
                        .dpd_ensure_underlay_created(
                            client,
                            underlay_ip_admin,
                            &tag,
                            switch_slot,
                        )
                        .await?;

                    let external_response = if is_elected {
                        // TODO: `vlan_id` is `None` because egress VLAN tagging
                        // is not yet supported. See RFD 488
                        // (§sect-external-mcast) for the egress design. When
                        // egress support lands, this should be populated from
                        // group configuration.
                        let external_entry =
                            MulticastGroupCreateExternalEntry {
                                group_ip: external_group_ip,
                                external_forwarding: ExternalForwarding {
                                    vlan_id: None,
                                },
                                internal_forwarding: InternalForwarding {
                                    nat_target: Some(nat_target),
                                },
                                tag: Some(tag.clone()),
                                sources,
                            };

                        Some(
                            self.dpd_ensure_external_created(
                                client,
                                &external_entry,
                                switch_slot,
                            )
                            .await?,
                        )
                    } else {
                        self.dpd_ensure_external_deleted(
                            client,
                            external_group_ip,
                            &dpd_tag,
                            switch_slot,
                        )
                        .await?;
                        None
                    };

                    Ok::<_, Error>((
                        switch_slot,
                        underlay_response,
                        external_response,
                    ))
                }
            });

        // Execute all switch operations in parallel
        let results = try_join_all(create_operations).await.map_err(|e| {
            error!(
                self.log,
                "DPD multicast forwarding configuration failed - dataplane inconsistency";
                "external_group_id" => %external_group.id(),
                "external_multicast_ip" => %external_group.multicast_ip.ip(),
                "underlay_multicast_ip" => %underlay_group.multicast_ip.ip(),
                "multicast_scope" => if external_group.multicast_ip.ip().is_ipv4() { "IPv4_External" } else { "IPv6_External" },
                "switch_count" => self.switch_count(),
                "dpd_error" => %e,
                "recovery" => "reconciler_retry_converges_partial_configuration",
                "dpd_operation" => "create_groups"
            );
            e
        })?;

        // Collect results. The underlay group is created on every switch, so
        // any response carries the same view and we keep the last. The external
        // entry lives only on the elected switch, so the single `Some` external
        // response is the one we return.
        let programmed_switches: Vec<SwitchSlot> =
            results.iter().map(|(loc, _, _)| **loc).collect();
        let (underlay, external) = results.into_iter().fold(
            (None, None),
            |(_, external), (_, underlay, ext)| {
                (Some(underlay), external.or(ext))
            },
        );
        let underlay_last = underlay.ok_or_else(|| {
            Error::internal_error("no switches were configured")
        })?;
        let external_last = external.ok_or_else(|| {
            Error::internal_error(
                "no switch was elected to own the external multicast group",
            )
        })?;

        debug!(
            self.log,
            "DPD multicast forwarding configuration completed - elected switch owns external ingress";
            "external_group_id" => %external_group.id(),
            "external_multicast_ip" => %external_group.multicast_ip,
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => ?underlay_last.group_ip,
            "elected_switch" => ?elected,
            "switch_count" => programmed_switches.len(),
            "dpd_operations_completed" => "[create_external_group, create_underlay_group, configure_nat_mapping]",
            "external_forwarding_vlan" => ?external_last.external_forwarding.vlan_id,
            "dpd_operation" => "create_groups"
        );

        Ok((underlay_last, external_last))
    }

    /// Update a multicast group's sources and forwarding in the dataplane.
    /// The dpd tag is immutable, so a rename never reaches the switches.
    ///
    /// Membership is left untouched here: underlay members are owned by
    /// mg-lower/DDM and are not rewritten by this client.
    pub(crate) async fn update_groups(
        &self,
        params: GroupUpdateParams<'_>,
    ) -> MulticastDataplaneResult<(
        MulticastGroupUnderlayResponse,
        MulticastGroupExternalResponse,
        SwitchSlot,
    )> {
        debug!(
            self.log,
            "updating multicast groups in dataplane";
            "external_group_id" => %params.external_group.id(),
            "underlay_group_id" => %params.underlay_group.id,
            "params" => ?params,
            "dpd_operation" => "update_groups"
        );

        let dpd_clients = &self.dpd_clients;

        // Pre-compute shared data once
        let underlay_ip_admin = params
            .underlay_group
            .multicast_ip
            .ip()
            .into_underlay_multicast()?;
        let underlay_ipv6 = match params.underlay_group.multicast_ip.ip() {
            IpAddr::V6(ipv6) => ipv6,
            IpAddr::V4(_) => {
                return Err(Error::internal_error(
                    "underlay multicast groups must use IPv6 addresses",
                ));
            }
        };

        let nat_target = NatTarget {
            internal_ip: underlay_ipv6,
            inner_mac: MacAddr { a: underlay_ipv6.derive_multicast_mac() },
            vni: Vni::from(u32::from(params.external_group.vni.0)),
        };
        // The DB tag (`{uuid}:{multicast_ip}`) is immutable for the group's
        // lifetime and shared by the external and underlay entries. Drift
        // repair below recreates a missing external entry with this tag,
        // matching `create_groups`. The user-facing name must not be used,
        // since dpd tags never change after creation.
        let db_tag = params.external_group.tag.clone().ok_or_else(|| {
            Error::internal_error("multicast group missing tag")
        })?;
        let external_group_ip = params.external_group.multicast_ip.ip();
        let sources_dpd = Self::compute_sources_for_dpd(
            external_group_ip,
            params.source_filter,
        );

        // Elect one switch to own this group's external NAT ingress, matching
        // `create_groups`. Non-elected switches have their external entry
        // evicted while the underlay group stays updated on every switch for
        // warm failover.
        let group_id =
            MulticastGroupUuid::from_untyped_uuid(params.external_group.id());
        let available: Vec<SwitchSlot> = dpd_clients.keys().copied().collect();
        // Prefer whichever switch already holds the entry so a benign rejoin
        // does not move ownership. The drift check that gates this update
        // already observed the owner, so it is threaded in as a hint rather
        // than re-queried, keeping both elections in agreement. An observed
        // incumbent switch is always among the candidates, so it is always
        // retained here. An ownership move only happens when no incumbent was
        // observed. The reconciler detects moves by comparing against the
        // owner it recorded on a previous pass.
        let elected =
            select_switch_slot(group_id, &available, params.incumbent_switch);

        let update_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let db_tag = db_tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                let underlay_ip_admin = underlay_ip_admin.clone();
                let is_elected = Some(*switch_slot) == elected;
                async move {
                    // Read the underlay group, creating it if absent.
                    //
                    // The member list is never written back from here.
                    // Underlay membership is owned by mg-lower/DDM, and a tag
                    // or source update does not change it. Writing the
                    // full-list `multicast_group_update_underlay` value would
                    // silently overwrite membership changes made since the
                    // read.
                    let underlay = match client
                        .multicast_group_get_underlay(&underlay_ip_admin)
                        .await
                    {
                        Ok(r) => r.into_inner(),
                        Err(DpdError::ErrorResponse(resp))
                            if resp.status()
                                == reqwest::StatusCode::NOT_FOUND =>
                        {
                            // Create missing underlay group with DB tag and empty members
                            let db_tag = params
                                .underlay_group
                                .tag
                                .as_deref()
                                .ok_or_else(|| {
                                Error::internal_error(
                                    "underlay multicast group missing tag",
                                )
                            })?;
                            self.dpd_ensure_underlay_created(
                                client,
                                underlay_ip_admin.clone(),
                                db_tag,
                                switch_slot,
                            )
                            .await?
                        }
                        Err(e) => {
                            error!(
                                self.log,
                                "failed to fetch underlay for update";
                                "underlay_ip" => %underlay_ip_admin,
                                "switch" => ?switch_slot,
                                "error" => %e
                            );
                            return Err(Error::internal_error(
                                "failed to fetch underlay for update",
                            ));
                        }
                    };

                    // The existing tag authorizes the external update below.
                    let tag: MulticastTag =
                        underlay.tag.clone().try_into().map_err(|e| {
                            Error::internal_error(&format!(
                                "invalid multicast tag: {e}"
                            ))
                        })?;

                    // Prepare external update/create entries with pre-computed data.
                    //
                    // TODO: `vlan_id` is `None` because egress VLAN tagging is not
                    // yet supported. See RFD 488 (§sect-external-mcast) for
                    // the egress design. When egress support lands, this
                    // should be populated from group configuration.
                    let external_forwarding =
                        ExternalForwarding { vlan_id: None };
                    let internal_forwarding =
                        InternalForwarding { nat_target: Some(nat_target) };

                    let update_entry = MulticastGroupUpdateExternalEntry {
                        external_forwarding: external_forwarding.clone(),
                        internal_forwarding: internal_forwarding.clone(),
                        sources: sources.clone(),
                    };
                    let create_entry = MulticastGroupCreateExternalEntry {
                        group_ip: external_group_ip,
                        external_forwarding,
                        internal_forwarding,
                        tag: Some(db_tag),
                        sources,
                    };

                    let external_response = if is_elected {
                        Some(
                            self.dpd_update_external_or_create(
                                client,
                                external_group_ip,
                                &tag,
                                &update_entry,
                                &create_entry,
                                switch_slot,
                            )
                            .await?,
                        )
                    } else {
                        // The external entry shares the underlay group's tag
                        // (both are created with the same tag in
                        // `create_groups`, and dpd tags are immutable), so the
                        // underlay tag read above authorizes this delete.
                        self.dpd_ensure_external_deleted(
                            client,
                            external_group_ip,
                            &tag,
                            switch_slot,
                        )
                        .await?;
                        None
                    };

                    Ok::<_, Error>((switch_slot, underlay, external_response))
                }
            });

        // Execute all switch operations in parallel
        let results = try_join_all(update_operations).await.map_err(|e| {
            error!(
                self.log,
                "DPD multicast group update failed - dataplane inconsistency";
                "external_group_id" => %params.external_group.id(),
                "external_multicast_ip" => %params.external_group.multicast_ip.ip(),
                "underlay_multicast_ip" => %params.underlay_group.multicast_ip.ip(),
                "update_operation" => "modify_tag_and_sources",
                "switch_count" => self.switch_count(),
                "dpd_error" => %e
            );
            e
        })?;

        // The underlay group is updated on every switch, so keep the last
        // response. The external entry lives only on the elected switch, so the
        // single `Some` external response is the one we return.
        let results_len = results.len();
        let (underlay, external) = results.into_iter().fold(
            (None, None),
            |(_, external), (_, underlay, ext)| {
                (Some(underlay), external.or(ext))
            },
        );
        let underlay_last = underlay
            .ok_or_else(|| Error::internal_error("no switches were updated"))?;
        let external_last = external.ok_or_else(|| {
            Error::internal_error(
                "no switch was elected to own the external multicast group",
            )
        })?;
        let elected_switch = elected.ok_or_else(|| {
            Error::internal_error(
                "no switch was elected to own the external multicast group",
            )
        })?;

        debug!(
            self.log,
            "successfully updated multicast groups - elected switch owns external ingress";
            "external_group_id" => %params.external_group.id(),
            "switches_updated" => results_len,
            "elected_switch" => ?elected,
            "dpd_operation" => "update_groups"
        );

        Ok((underlay_last, external_last, elected_switch))
    }

    /// Fetch external multicast group DPD state for RPW drift detection.
    ///
    /// A group's external NAT-ingress entry is owned by a single elected switch
    /// (see [`select_switch_slot`]), so this expects the entry on the elected
    /// switch and nowhere else. It queries all switches and classifies the
    /// result:
    ///
    /// - Present only on the elected switch: returns that config so the
    ///   reconciler can compare tag and sources against the database.
    /// - Present on a non-elected switch (a stale forwarder after re-election),
    ///   or absent on the elected switch: structural drift. Returns `None` so
    ///   the reconciler re-issues [`update_groups`], which re-gates the entry
    ///   onto the elected switch and evicts it from the others.
    /// - Absent everywhere (a new group): returns `None` so the reconciler
    ///   creates it.
    ///
    /// The paired underlay replication entry at `underlay_ip` is checked on
    /// every switch alongside the external entry. Unlike the external entry,
    /// it is expected on all switches, so a missing entry anywhere (a rebooted
    /// or wiped switch) is also structural drift that forces the update.
    ///
    /// Drift repair follows the RPW convergence model rather than an atomic
    /// cross-switch saga, so callers should expect *N*-pass convergence on
    /// partial failure.
    ///
    /// The returned [`ExternalDriftCheck`] carries `elected_config` as `Some`
    /// with the elected switch's config when the group is correctly placed
    /// there and `None` when the reconciler must re-issue an update, plus the
    /// observed `incumbent_switch` to thread into that update.
    ///
    /// [`select_switch_slot`]: super::select_switch_slot
    /// [`update_groups`]: Self::update_groups
    pub(crate) async fn fetch_external_group_for_drift_check(
        &self,
        group_id: MulticastGroupUuid,
        group_ip: IpAddr,
        underlay_ip: IpAddr,
    ) -> MulticastDataplaneResult<ExternalDriftCheck> {
        debug!(
            self.log,
            "fetching external group state from all switches for drift detection";
            "group_ip" => %group_ip,
            "underlay_ip" => %underlay_ip,
            "switch_count" => self.switch_count(),
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        let underlay_ip_admin = underlay_ip.into_underlay_multicast()?;

        let fetch_ops = self.dpd_clients.iter().map(|(switch_slot, client)| {
            let log = self.log.clone();
            let underlay_ip_admin = underlay_ip_admin.clone();
            async move {
                let external = match client.multicast_group_get(&group_ip).await
                {
                    Ok(response) => Some(response.into_inner()),
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::NOT_FOUND =>
                    {
                        debug!(
                            log,
                            "external group not found on switch";
                            "group_ip" => %group_ip,
                            "switch" => ?switch_slot,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        None
                    }
                    Err(e) => {
                        error!(
                            log,
                            "external group fetch failed";
                            "group_ip" => %group_ip,
                            "switch" => ?switch_slot,
                            "error" => %e,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        return Err(Error::internal_error(&format!(
                            "failed to fetch external group from DPD: {e}"
                        )));
                    }
                };

                let underlay_present = match client
                    .multicast_group_get_underlay(&underlay_ip_admin)
                    .await
                {
                    Ok(_) => true,
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::NOT_FOUND =>
                    {
                        debug!(
                            log,
                            "underlay group not found on switch";
                            "underlay_ip" => %underlay_ip_admin,
                            "switch" => ?switch_slot,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        false
                    }
                    Err(e) => {
                        error!(
                            log,
                            "underlay group fetch failed";
                            "underlay_ip" => %underlay_ip_admin,
                            "switch" => ?switch_slot,
                            "error" => %e,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        return Err(Error::internal_error(&format!(
                            "failed to fetch underlay group from DPD: {e}"
                        )));
                    }
                };

                Ok((switch_slot, external, underlay_present))
            }
        });

        let results = try_join_all(fetch_ops).await?;

        let available: Vec<SwitchSlot> =
            self.dpd_clients.keys().copied().collect();
        let holding_slots: Vec<SwitchSlot> = results
            .iter()
            .filter_map(|(slot, resp, _)| resp.as_ref().map(|_| **slot))
            .collect();
        let incumbent_switch = single_external_owner(&holding_slots);
        let elected =
            select_switch_slot(group_id, &available, incumbent_switch);

        // The underlay replication entry must exist on every switch. A switch
        // missing it cannot replicate the group's traffic, so re-issue the
        // update, which recreates the entry there.
        let underlay_missing: Vec<SwitchSlot> = results
            .iter()
            .filter(|(_, _, present)| !present)
            .map(|(slot, _, _)| **slot)
            .collect();
        if !underlay_missing.is_empty() {
            error!(
                self.log,
                "underlay multicast entry missing on switch(es)";
                "group_ip" => %group_ip,
                "underlay_ip" => %underlay_ip,
                "missing_switches" => ?underlay_missing,
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Ok(ExternalDriftCheck {
                incumbent_switch,
                elected_config: None,
                misplaced: false,
            });
        }

        // Classify by placement relative to the elected switch.
        let mut elected_config: Option<MulticastGroupResponse> = None;
        let mut misplaced: Vec<SwitchSlot> = Vec::new();
        for (switch_slot, resp, _) in &results {
            match resp {
                Some(cfg) if Some(**switch_slot) == elected => {
                    elected_config = Some(cfg.clone());
                }
                Some(_) => misplaced.push(**switch_slot),
                None => {}
            }
        }

        // A stale forwarder (external entry on a non-elected switch) is
        // structural drift: re-issue the update to evict it. This is the RX
        // dedup invariant, a single switch ingresses and replicates a group.
        if !misplaced.is_empty() {
            error!(
                self.log,
                "external multicast entry present on non-elected switch(es)";
                "group_ip" => %group_ip,
                "elected_switch" => ?elected,
                "misplaced_switches" => ?misplaced,
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Ok(ExternalDriftCheck {
                incumbent_switch,
                elected_config: None,
                misplaced: true,
            });
        }

        // Absent on the elected switch (whether new or drifted off it): the
        // reconciler should (re)create it there.
        let Some(config) = elected_config else {
            debug!(
                self.log,
                "external group absent on elected switch, reconciler will create";
                "group_ip" => %group_ip,
                "elected_switch" => ?elected,
                "switches_queried" => results.len(),
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Ok(ExternalDriftCheck {
                incumbent_switch,
                elected_config: None,
                misplaced: false,
            });
        };

        debug!(
            self.log,
            "external group correctly placed on elected switch";
            "group_ip" => %group_ip,
            "elected_switch" => ?elected,
            "switches_queried" => results.len(),
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        Ok(ExternalDriftCheck {
            incumbent_switch,
            elected_config: Some(config.into_external_response()?),
            misplaced: false,
        })
    }

    /// Observe which switch currently holds a group's external entry.
    ///
    /// Member attach and leave run between group reconciler passes and so
    /// lack the drift check's observed owner. Threading the result into
    /// `propagate_m2p_and_forwarding` as the election incumbent keeps the
    /// sled egress next hop co-located with the external entry instead of
    /// flip-flopping to the hash fallback until the next reconciler pass.
    ///
    /// Anything other than exactly one observed holder yields `None` (the
    /// hash fallback). A read error is treated as not holding, and if that
    /// hides the incumbent, the election degrades to the hash fallback and
    /// the next group reconciler pass restores the observed owner. The
    /// underlay group exists on every switch, so a next hop aimed at the
    /// non-owning switch still reaches member sleds. Only off-rack egress
    /// through the external entry is affected for that window.
    pub(crate) async fn observe_external_owner(
        &self,
        group_ip: IpAddr,
    ) -> Option<SwitchSlot> {
        let fetch_ops =
            self.dpd_clients.iter().map(|(switch_slot, client)| async move {
                (*switch_slot, client.multicast_group_get(&group_ip).await)
            });
        let mut holding: Vec<SwitchSlot> = Vec::new();
        for (switch_slot, outcome) in join_all(fetch_ops).await {
            match outcome {
                Ok(_) => holding.push(switch_slot),
                Err(DpdError::ErrorResponse(resp))
                    if resp.status() == reqwest::StatusCode::NOT_FOUND => {}
                Err(e) => {
                    warn!(
                        self.log,
                        "external owner observation failed on switch, \
                         treating as not holding";
                        "group_ip" => %group_ip,
                        "switch" => ?switch_slot,
                        "error" => %e,
                        "dpd_operation" => "observe_external_owner"
                    );
                }
            }
        }

        single_external_owner(&holding)
    }

    pub(crate) async fn remove_groups(
        &self,
        tag: &str,
    ) -> MulticastDataplaneResult<()> {
        debug!(
            self.log,
            "cleaning up multicast groups by tag";
            "tag" => tag
        );

        let dpd_clients = &self.dpd_clients;
        let dpd_tag: MulticastTag = tag
            .parse()
            .map_err(|_| Error::internal_error("invalid multicast tag"))?;

        // Execute cleanup operations on all switches in parallel
        let cleanup_ops = dpd_clients.iter().map(|(switch_slot, client)| {
            let log = self.log.clone();
            let dpd_tag = dpd_tag.clone();
            async move {
                match client.multicast_reset_by_tag(&dpd_tag).await {
                    Ok(_) => {
                        debug!(
                            log,
                            "cleaned up multicast groups";
                            "switch" => ?switch_slot,
                            "tag" => %tag
                        );
                        Ok::<(), Error>(())
                    }
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::NOT_FOUND =>
                    {
                        // Tag not found on this switch - this is fine, means nothing to clean up
                        debug!(
                            log,
                            "no multicast groups found with tag on switch (expected)";
                            "switch" => ?switch_slot,
                            "tag" => %tag
                        );
                        Ok::<(), Error>(())
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to clean up multicast groups by tag";
                            "switch" => ?switch_slot,
                            "tag" => %tag,
                            "error" => %e,
                            "dpd_operation" => "remove_groups"
                        );
                        Err(Error::internal_error(
                            "failed to clean up multicast groups by tag",
                        ))
                    }
                }
            }
        });

        // Wait for all cleanup operations to complete
        try_join_all(cleanup_ops).await?;

        info!(
            self.log,
            "successfully cleaned up multicast groups by tag";
            "tag" => tag
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};
    use sled_agent_types::early_networking::SwitchSlot;
    use uuid::Uuid;

    use super::single_external_owner;
    use crate::app::multicast::select_switch_slot;

    const AVAILABLE: [SwitchSlot; 2] =
        [SwitchSlot::Switch0, SwitchSlot::Switch1];

    // Distinct random groups sampled per case. Group UUIDs are uniform, so
    // each hashes to either switch with roughly equal probability; a modest
    // sample exercises the hash and lets the distribution check observe both
    // switches (P(missing one) is about 2 * 2^-HASH_SAMPLES). Matches the
    // election tests in the parent module.
    const HASH_SAMPLES: usize = 32;

    #[test]
    fn no_stable_owner_falls_back_to_hash() {
        // Zero holders and a split (more than one holder) both yield no stable
        // owner, so the election has no incumbent to honor and distributes
        // across the available switches via the hash rather than pinning to a
        // switch.
        let no_owner: [&[SwitchSlot]; 2] =
            [&[], &[SwitchSlot::Switch0, SwitchSlot::Switch1]];
        for holders in no_owner {
            let incumbent = single_external_owner(holders);
            assert_eq!(incumbent, None);

            let mut seen0 = false;
            let mut seen1 = false;
            for _ in 0..HASH_SAMPLES {
                let group_id =
                    MulticastGroupUuid::from_untyped_uuid(Uuid::new_v4());
                match select_switch_slot(group_id, &AVAILABLE, incumbent) {
                    Some(SwitchSlot::Switch0) => seen0 = true,
                    Some(SwitchSlot::Switch1) => seen1 = true,
                    other => panic!("unexpected election: {other:?}"),
                }
            }
            assert!(
                seen0 && seen1,
                "hash should distribute across both switches"
            );
        }
    }

    #[test]
    fn sole_holder_is_elected_over_hash() {
        // A single observed owner pins the election to that switch regardless
        // of the hash.
        for holder in AVAILABLE {
            let incumbent = single_external_owner(&[holder]);
            assert_eq!(incumbent, Some(holder));
            for _ in 0..HASH_SAMPLES {
                let group_id =
                    MulticastGroupUuid::from_untyped_uuid(Uuid::new_v4());
                assert_eq!(
                    select_switch_slot(group_id, &AVAILABLE, incumbent),
                    Some(holder)
                );
            }
        }
    }
}
