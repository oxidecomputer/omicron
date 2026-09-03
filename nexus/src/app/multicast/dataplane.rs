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
//! uplinks). Ingress is generally active-active: every switch carries every
//! group's NAT ingress entry, and whichever switch receives a packet rewrites
//! and replicates it accordingly. The rack does not delegate an ingress switch.
//! Uplinks carrying a group is up to the customer network. Delivering toward
//! one uplink gives each subscriber a single copy. Delivering toward both gives
//! it one copy per switch, since nothing downstream carries packet identity to
//! deduplicate on. Neither is wrong. A customer with two uplinks may
//! configure both, why not.
//!
//! TODO: advertise membership upstream (RFD 488 "External Multicast",
//! eventually mcastd) so a dynamic upstream builds its tree without
//! manual steering. Reports from both uplinks can signal the stream through
//! to both switches (one copy per switch). For single-copy delivery, we need mcastd
//! to pick a reporting uplink per group, which involves a switch selection in the
//! signaling plane rather than in NAT programming.
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

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use futures::future::try_join_all;
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
use sled_agent_types::early_networking::SwitchSlot;

use crate::app::dpd_switches;

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

/// Convert an `IpAddr` into a DPD `UnderlayMulticastIpv6`,
/// rejecting IPv4.
///
/// Note: named without the `Ipv6` suffix because the input type is the general
/// `IpAddr`.
trait IntoUnderlayMulticast {
    /// Convert to `UnderlayMulticastIpv6`, rejecting IPv4 addresses.
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
    /// Whether every Dendrite instance advertised in DNS produced a client.
    ///
    /// Non-destructive operations tolerate a partial map, but tag-based
    /// deletion refuses to run over one (see [`Self::remove_groups`]).
    discovery_complete: bool,
    log: Logger,
}

/// Parameters for multicast group updates.
#[derive(Debug)]
pub(crate) struct GroupUpdateParams<'a> {
    pub external_group: &'a ExternalMulticastGroup,
    pub underlay_group: &'a UnderlayMulticastGroup,
    pub source_filter: &'a SourceFilterState,
}

/// What [`MulticastDataplaneClient::fetch_external_group_for_drift_check`]
/// observed across the switches.
pub(crate) struct ExternalDriftCheck {
    /// One switch's config when the entry is present and consistent on
    /// every switch, `None` when the reconciler must re-issue an update.
    pub external_config: Option<MulticastGroupExternalResponse>,
}

/// Bound-by-timeout DPD client construction. On timeout or DNS failure we yield
/// an empty client map rather than failing the pass.
const DPD_CLIENT_BUILD_TIMEOUT: Duration =
    // Caps the internal-DNS retry budget for `_dendrite._tcp` so a DPD
    // outage doesn't starve the bg task's idle window.
    Duration::from_secs(5);

impl MulticastDataplaneClient {
    /// Create a new client - builds fresh DPD clients for current switch
    /// topology.
    ///
    /// The client map holds every switch whose Dendrite zone is advertised
    /// in internal DNS and whose DPD reports a slot. A missing switch (DNS
    /// gap or unreachable management plane) is tolerated rather than
    /// crashing out.
    ///
    /// Destructive tag-based cleanup is the exception and requires complete
    /// discovery (see [`Self::remove_groups`]). See
    /// [`DPD_CLIENT_BUILD_TIMEOUT`].
    pub(crate) async fn new(
        resolver: Resolver,
        log: Logger,
    ) -> MulticastDataplaneResult<Self> {
        let (dpd_clients, discovery_complete) = match tokio::time::timeout(
            DPD_CLIENT_BUILD_TIMEOUT,
            dpd_switches(&resolver, &log),
        )
        .await
        {
            Ok(Ok((switches, advertised))) => {
                let complete = switches.len() == advertised;
                let clients = switches
                    .into_iter()
                    .map(|(slot, (_addr, client))| (slot, client))
                    .collect();
                (clients, complete)
            }
            Ok(Err(e)) => {
                warn!(
                    log,
                    "failed to build DPD clients, continuing with empty \
                     client map";
                    "error" => %e,
                );
                (HashMap::new(), false)
            }
            Err(_) => {
                warn!(
                    log,
                    "timed out building DPD clients, continuing with empty \
                     client map";
                    "timeout" => ?DPD_CLIENT_BUILD_TIMEOUT,
                );
                (HashMap::new(), false)
            }
        };
        Ok(Self { dpd_clients, discovery_complete, log })
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
    ) -> MulticastDataplaneResult<()> {
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

        // Every switch carries the external entry. The underlay group is
        // created on every switch too, keeping each a warm failover
        // candidate whose member ports mg-lower populates.
        let create_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let tag = tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                let underlay_ip_admin = underlay_ip_admin.clone();
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

                    // TODO: `vlan_id` is `None` because egress VLAN tagging is
                    // not yet supported. See RFD 488 (§sect-external-mcast) for
                    // the egress design. Once egress is supported, populate
                    // this from group configuration.
                    let external_entry = MulticastGroupCreateExternalEntry {
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

                    let external_response = self
                        .dpd_ensure_external_created(
                            client,
                            &external_entry,
                            switch_slot,
                        )
                        .await?;

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

        let programmed_switches: Vec<SwitchSlot> =
            results.iter().map(|(loc, _, _)| **loc).collect();
        let (_, underlay_last, external_last) =
            results.into_iter().last().ok_or_else(|| {
                Error::internal_error("no switches were configured")
            })?;

        debug!(
            self.log,
            "DPD multicast forwarding configuration completed";
            "external_group_id" => %external_group.id(),
            "external_multicast_ip" => %external_group.multicast_ip,
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => ?underlay_last.group_ip,
            "switch_count" => programmed_switches.len(),
            "dpd_operations_completed" => "[create_external_group, create_underlay_group, configure_nat_mapping]",
            "external_forwarding_vlan" => ?external_last.external_forwarding.vlan_id,
            "dpd_operation" => "create_groups"
        );

        Ok(())
    }

    /// Update a multicast group's sources and forwarding in the dataplane.
    /// The DPD tag is immutable, so a rename never reaches the switches.
    ///
    /// Membership is left untouched here: underlay members are owned by
    /// mg-lower/DDM and are not rewritten by this client.
    pub(crate) async fn update_groups(
        &self,
        params: GroupUpdateParams<'_>,
    ) -> MulticastDataplaneResult<()> {
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
        // since DPD tags never change after creation.
        let db_tag = params.external_group.tag.clone().ok_or_else(|| {
            Error::internal_error("multicast group missing tag")
        })?;
        let external_group_ip = params.external_group.multicast_ip.ip();
        let sources_dpd = Self::compute_sources_for_dpd(
            external_group_ip,
            params.source_filter,
        );

        // Update the external entry on every switch, matching
        // `create_groups`. The underlay group stays updated on every switch
        // as before.
        let update_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let db_tag = db_tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                let underlay_ip_admin = underlay_ip_admin.clone();
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

                    // Prepare external entries with pre-computed data.
                    //
                    // TODO: `vlan_id` is `None` because egress VLAN tagging is
                    // not yet supported. See RFD 488 (§sect-external-mcast) for
                    // the egress design. Once egress is supported, populate
                    // this from group configuration.
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

                    let external_response = self
                        .dpd_update_external_or_create(
                            client,
                            external_group_ip,
                            &tag,
                            &update_entry,
                            &create_entry,
                            switch_slot,
                        )
                        .await?;

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

        // Every switch received the same update, but watch for the failure of
        // an empty switch set.
        let results_len = results.len();
        if results.is_empty() {
            return Err(Error::internal_error("no switches were updated"));
        }

        debug!(
            self.log,
            "successfully updated multicast groups";
            "external_group_id" => %params.external_group.id(),
            "switches_updated" => results_len,
            "dpd_operation" => "update_groups"
        );

        Ok(())
    }

    /// Fetch external multicast group DPD state for RPW drift detection.
    ///
    /// This expects the entry on every switch. It queries all switches
    /// and classifies the result as
    ///
    /// - present everywhere: returns one switch's config so that the
    ///   reconciler can compare tag and sources against the DB.
    /// - absent anywhere (external or underlay): returns `None` so
    ///   the reconciler can re-issue [`update_groups`], which recreates it
    ///   everywhere.
    ///
    /// The paired underlay replication entry at `underlay_ip` is checked on
    /// every switch alongside the external entry, with the same expecation.
    ///
    /// Drift repair follows the RPW convergence model rather than an atomic
    /// cross-switch saga.
    ///
    /// [`update_groups`]: Self::update_groups
    pub(crate) async fn fetch_external_group_for_drift_check(
        &self,
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

        // Discovery can yield an empty client map (see
        // [`DPD_CLIENT_BUILD_TIMEOUT`]), so warn and error out.
        if results.is_empty() {
            warn!(
                self.log,
                "no switches to query for drift detection";
                "group_ip" => %group_ip,
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Err(Error::internal_error("no switches were configured"));
        }

        // The underlay replication entry must exist on every switch. A switch
        // missing it cannot replicate the group's traffic: re-issue the
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
            return Ok(ExternalDriftCheck { external_config: None });
        }

        // Missing anywhere forces the update.
        let missing: Vec<SwitchSlot> = results
            .iter()
            .filter(|(_, resp, _)| resp.is_none())
            .map(|(slot, _, _)| **slot)
            .collect();
        if !missing.is_empty() {
            debug!(
                self.log,
                "external group absent on switch(es), reconciler will create";
                "group_ip" => %group_ip,
                "missing_switches" => ?missing,
                "switches_queried" => results.len(),
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Ok(ExternalDriftCheck { external_config: None });
        }

        // Present everywhere. Any copy will do for the tag and source
        // comparisons.
        let config = results
            .into_iter()
            .find_map(|(_, resp, _)| resp)
            .expect("missing is empty, so every result has a response");

        debug!(
            self.log,
            "external group present on every switch";
            "group_ip" => %group_ip,
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        Ok(ExternalDriftCheck {
            external_config: Some(config.into_external_response()?),
        })
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
        if dpd_clients.is_empty() {
            // An empty client map means switch state could not be reached
            // (see [`DPD_CLIENT_BUILD_TIMEOUT`]).
            return Err(Error::internal_error(
                "no switches were available for multicast group cleanup",
            ));
        }

        if !self.discovery_complete {
            return Err(Error::internal_error(
                "multicast group cleanup requires every switch advertised \
                 in DNS, but Dendrite discovery was partial",
            ));
        }

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
