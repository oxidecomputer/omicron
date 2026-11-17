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
//! Groups can be shared across projects and silos. Access is controlled by API
//! authorization and underlay membership.
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
//! - **SSM addresses** ([`is_ssm_address`]) cover IPv4 232/8 plus IPv6 ff3x
//!   and ffbx groups whose prefix-length octet is zero ([RFC 3306 §6],
//!   [RFC 7371 §4.1.2]). Sources are **required**: each member must specify
//!   at least one source IP.
//! - **ASM addresses**: Sources are **optional** per-member. Empty sources
//!   means receive from any source; non-empty enables source filtering.
//!
//! [`UNDERLAY_MULTICAST_SUBNET`]: dpd_types::mcast::UNDERLAY_MULTICAST_SUBNET
//! [RFC 3306 §6]: https://www.rfc-editor.org/rfc/rfc3306#section-6
//! [RFC 7371 §4.1.2]: https://www.rfc-editor.org/rfc/rfc7371#section-4.1.2

use futures::future::try_join_all;
use oxnet::MulticastMac;
use slog::{Logger, debug, error, info, warn};
use std::collections::HashMap;

use dpd_client::Error as DpdError;
use dpd_client::types::{
    ExternalForwarding, ExternalInternalForwarding, ExternalNatTarget, IpSrc,
    MacAddr, MulticastGroupCreateExternalEntry,
    MulticastGroupCreateUnderlayEntry, MulticastGroupExternalResponse,
    MulticastGroupResponse, MulticastGroupUnderlayResponse,
    MulticastGroupUpdateExternalEntry,
};
use dpd_types::mcast::{
    ExternalMulticastIp, UnderlayMulticastIpv6, is_ssm_address,
};
use dpd_types::network::Vni;
use internal_dns_resolver::Resolver;

use nexus_db_model::{ExternalMulticastGroup, UnderlayMulticastGroup};
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use sled_agent_types::early_networking::SwitchSlot;

use super::invalid_stored_address;
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

fn is_missing_nat_target(
    resp: &dpd_client::ResponseValue<dpd_client::types::Error>,
) -> bool {
    resp.status() == reqwest::StatusCode::CONFLICT
        && resp.error_code.as_deref()
            == Some(dpd_client::MISSING_NAT_TARGET_ERROR_CODE)
}

fn nat_target_vni(
    vni: omicron_common::api::external::Vni,
) -> Result<Vni, Error> {
    Vni::new(vni.as_u32()).map_err(|e| {
        Error::internal_error(&format!("invalid multicast VNI: {e}"))
    })
}

/// Result type for multicast dataplane operations.
pub(crate) type MulticastDataplaneResult<T> = Result<T, Error>;

/// Client for multicast dataplane operations.
///
/// TODO: Egress (instance → external) is not yet supported. The current
/// implementation only handles ingress (external → switches → instances)
/// using rear ports with [`dpd_client::types::Direction::Underlay`]. See
/// RFD 488 (§sect-external-mcast) for the egress design, which will use
/// front-port uplink members with [`dpd_client::types::Direction::External`].
pub(crate) struct MulticastDataplaneClient {
    dpd_clients: HashMap<SwitchSlot, dpd_client::Client>,
    /// Whether every expected switch produced a client.
    ///
    /// Set at construction when every Dendrite instance advertised in DNS
    /// answered, and cleared by [`Self::with_expected_switch_count`] when
    /// the map covers fewer switches than the caller expects. Non-destructive
    /// operations tolerate a partial map, but tag-based deletion refuses to
    /// run over one (see [`Self::remove_groups`]).
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
    /// One switch's config when the external and underlay entries are present
    /// on every queried switch; `None` when an entry is missing.
    pub external_config: Option<MulticastGroupExternalResponse>,
}

impl MulticastDataplaneClient {
    pub(crate) fn switch_clients(
        &self,
    ) -> &HashMap<SwitchSlot, dpd_client::Client> {
        &self.dpd_clients
    }

    /// Create a new client - builds fresh DPD clients for current switch
    /// topology.
    ///
    /// The client map holds every switch whose Dendrite zone is advertised
    /// in internal DNS and whose DPD reports a slot. A missing switch (DNS
    /// gap or unreachable management plane) is tolerated rather than
    /// crashing out.
    ///
    /// Destructive tag-based cleanup is the exception and requires complete
    /// discovery (see [`Self::remove_groups`] and
    /// [`Self::with_expected_switch_count`]).
    pub(crate) async fn new(resolver: Resolver, log: Logger) -> Self {
        let (dpd_clients, discovery_complete) =
            match dpd_switches(&resolver, &log).await {
                Ok((switches, advertised)) => {
                    let complete = switches.len() == advertised;
                    let clients = switches
                        .into_iter()
                        .map(|(slot, (_addr, client))| (slot, client))
                        .collect();
                    (clients, complete)
                }
                Err(e) => {
                    warn!(
                        log,
                        "failed to build DPD clients, continuing with empty \
                         client map";
                        "error" => %e,
                    );
                    (HashMap::new(), false)
                }
            };
        Self { dpd_clients, discovery_complete, log }
    }

    /// Compute DPD source filter from aggregated member source state.
    ///
    /// For SSM addresses, this always returns specific sources.
    ///
    /// For ASM addresses, this returns `[IpSrc::Any]` if any member has omitted
    /// sources. If sources exist, the return is a union of all member sources.
    ///
    /// Note: `Any` is sent explicitly and not as the negative constructor of
    /// `None` because DPD treats `None` on update as "leave sources unchanged".
    fn compute_sources_for_dpd(
        external_group_ip: ExternalMulticastIp,
        source_filter: &SourceFilterState,
    ) -> Result<Option<Vec<IpSrc>>, Error> {
        source_filter
            .for_group(external_group_ip)
            .map(|filter| {
                Some(filter.exact_sources().map_or_else(
                    || vec![IpSrc::Any],
                    |sources| {
                        sources.iter().map(|s| IpSrc::Exact(s.ip())).collect()
                    },
                ))
            })
            .map_err(|e| {
                Error::internal_error(&format!(
                    "invalid stored multicast source filter for \
                     group {external_group_ip}: {e}"
                ))
            })
    }

    fn ensure_own_tag(
        &self,
        existing: &str,
        expected: &str,
        group_ip: &str,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<()> {
        if existing == expected {
            return Ok(());
        }
        warn!(
            self.log,
            "multicast group already exists on switch with a foreign tag";
            "group_ip" => group_ip,
            "switch" => ?switch,
            "existing_tag" => existing,
            "expected_tag" => expected,
        );
        Err(Error::conflict(format!(
            "multicast group {group_ip} exists on switch {switch:?} with \
             another group's tag"
        )))
    }

    fn missing_nat_target(
        &self,
        create: &MulticastGroupCreateExternalEntry,
        switch: &SwitchSlot,
    ) -> Error {
        warn!(
            self.log,
            "external create references a missing underlay group";
            "external_ip" => %create.group_ip,
            "underlay_ip" => %create.internal_forwarding.nat_target.internal_ip,
            "switch" => ?switch,
        );
        Error::unavail(&format!(
            "underlay group for {} is missing on switch {switch:?}",
            create.group_ip,
        ))
    }

    async fn dpd_ensure_underlay_created(
        &self,
        client: &dpd_client::Client,
        ip: UnderlayMulticastIpv6,
        tag: &str,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<MulticastGroupUnderlayResponse> {
        let create = MulticastGroupCreateUnderlayEntry {
            group_ip: ip,
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

                let existing = client
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
                    .into_inner();
                self.ensure_own_tag(
                    existing.tag.as_str(),
                    tag,
                    &ip.to_string(),
                    switch,
                )?;
                Ok(existing)
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
                if is_missing_nat_target(&resp) =>
            {
                Err(self.missing_nat_target(create, switch))
            }
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
                    .multicast_group_get(&create.group_ip.ip())
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

                let existing =
                    response.into_inner().into_external_response()?;
                self.ensure_own_tag(
                    existing.tag.as_str(),
                    create.tag.as_deref().unwrap_or_default(),
                    &create.group_ip.to_string(),
                    switch,
                )?;
                Ok(existing)
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
        group_ip: ExternalMulticastIp,
        tag: String,
        update: &MulticastGroupUpdateExternalEntry,
        create: &MulticastGroupCreateExternalEntry,
        switch: &SwitchSlot,
    ) -> MulticastDataplaneResult<MulticastGroupExternalResponse> {
        let tag = tag.try_into().map_err(|e| {
            Error::internal_error(&format!("invalid multicast tag: {e}"))
        })?;
        match client
            .multicast_group_update_external(&create.group_ip, &tag, update)
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
                        if is_missing_nat_target(&resp) =>
                    {
                        Err(self.missing_nat_target(create, switch))
                    }
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::CONFLICT =>
                    {
                        let response = client
                            .multicast_group_get(&group_ip.ip())
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

                        let existing =
                            response.into_inner().into_external_response()?;
                        self.ensure_own_tag(
                            existing.tag.as_str(),
                            create.tag.as_deref().unwrap_or_default(),
                            &group_ip.to_string(),
                            switch,
                        )?;
                        Ok(existing)
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

    /// Mark discovery incomplete unless the client map covers `expected`
    /// switches. If `None` (count unknown), it's also marked as incomplete.
    pub(crate) fn with_expected_switch_count(
        mut self,
        expected: Option<usize>,
    ) -> Self {
        let covered = expected.is_some_and(|n| self.dpd_clients.len() >= n);
        if !covered {
            warn!(
                self.log,
                "DPD clients do not cover every expected switch";
                "expected_switches" => ?expected,
                "switch_count" => self.dpd_clients.len(),
            );
            self.discovery_complete = false;
        }
        self
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

        let underlay_ip_admin = underlay_group
            .dpd_multicast_ip()
            .map_err(invalid_stored_address)?;

        let nat_target = ExternalNatTarget {
            internal_ip: underlay_ip_admin,
            inner_mac: MacAddr {
                a: underlay_ip_admin.ip().derive_multicast_mac(),
            },
            vni: nat_target_vni(external_group.vni.0)?,
        };
        let external_group_dpd_ip = external_group
            .dpd_multicast_ip()
            .map_err(invalid_stored_address)?;
        let sources_dpd = Self::compute_sources_for_dpd(
            external_group_dpd_ip,
            source_filter,
        )?;

        // Every switch carries the external entry. The underlay group is
        // created on every switch too, keeping each a warm failover
        // candidate whose member ports mg-lower populates.
        let create_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let tag = tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
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
                        group_ip: external_group_dpd_ip,
                        external_forwarding: ExternalForwarding {
                            vlan_id: None,
                        },
                        internal_forwarding: ExternalInternalForwarding {
                            nat_target,
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
            .dpd_multicast_ip()
            .map_err(invalid_stored_address)?;

        let nat_target = ExternalNatTarget {
            internal_ip: underlay_ip_admin,
            inner_mac: MacAddr {
                a: underlay_ip_admin.ip().derive_multicast_mac(),
            },
            vni: nat_target_vni(params.external_group.vni.0)?,
        };
        // The DB tag (`{uuid}:{multicast_ip}`) is immutable for the group's
        // lifetime and shared by the external and underlay entries. Drift
        // repair below recreates a missing external entry with this tag,
        // matching `create_groups`. The user-facing name must not be used,
        // since DPD tags never change after creation.
        let db_tag = params.external_group.tag.clone().ok_or_else(|| {
            Error::internal_error("multicast group missing tag")
        })?;
        let external_group_dpd_ip = params
            .external_group
            .dpd_multicast_ip()
            .map_err(invalid_stored_address)?;
        let sources_dpd = Self::compute_sources_for_dpd(
            external_group_dpd_ip,
            params.source_filter,
        )?;

        let update_operations =
            dpd_clients.into_iter().map(|(switch_slot, client)| {
                let db_tag = db_tag.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                async move {
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
                                underlay_ip_admin,
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
                    let tag = underlay.tag.clone();

                    // TODO: `vlan_id` is `None` because egress VLAN tagging is
                    // not yet supported. See RFD 488 (§sect-external-mcast) for
                    // the egress design. Once egress is supported, populate
                    // this from group configuration.
                    let external_forwarding =
                        ExternalForwarding { vlan_id: None };
                    let internal_forwarding =
                        ExternalInternalForwarding { nat_target };

                    let update_entry = MulticastGroupUpdateExternalEntry {
                        external_forwarding: external_forwarding.clone(),
                        internal_forwarding: internal_forwarding.clone(),
                        sources: Some(sources.clone().unwrap_or_default()),
                    };
                    let create_entry = MulticastGroupCreateExternalEntry {
                        group_ip: external_group_dpd_ip,
                        external_forwarding,
                        internal_forwarding,
                        tag: Some(db_tag),
                        sources,
                    };

                    let external_response = self
                        .dpd_update_external_or_create(
                            client,
                            external_group_dpd_ip,
                            tag,
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
                "update_operation" => "modify_sources",
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
        group_ip: ExternalMulticastIp,
        underlay_ip: UnderlayMulticastIpv6,
    ) -> MulticastDataplaneResult<ExternalDriftCheck> {
        debug!(
            self.log,
            "fetching external group state from all switches for drift detection";
            "group_ip" => %group_ip,
            "underlay_ip" => %underlay_ip,
            "switch_count" => self.switch_count(),
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        let fetch_ops = self.dpd_clients.iter().map(|(switch_slot, client)| {
            let log = self.log.clone();

            async move {
                let external =
                    match client.multicast_group_get(&group_ip.ip()).await {
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
                    .multicast_group_get_underlay(&underlay_ip)
                    .await
                {
                    Ok(_) => true,
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::NOT_FOUND =>
                    {
                        debug!(
                            log,
                            "underlay group not found on switch";
                            "underlay_ip" => %underlay_ip,
                            "switch" => ?switch_slot,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        false
                    }
                    Err(e) => {
                        error!(
                            log,
                            "underlay group fetch failed";
                            "underlay_ip" => %underlay_ip,
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
        let mut configs = results
            .into_iter()
            .filter_map(|(slot, resp, _)| resp.map(|config| (slot, config)));
        let (first_slot, config) = configs
            .next()
            .expect("missing is empty, so every result has a response");
        let config = config.into_external_response()?;
        for (slot, other) in configs {
            let other = other.into_external_response()?;
            let sources_match = match (&config.sources, &other.sources) {
                (None, None) => true,
                (Some(first), Some(other)) => {
                    first.iter().all(|source| other.contains(source))
                        && other.iter().all(|source| first.contains(source))
                }
                _ => false,
            };
            if config.tag != other.tag
                || config.internal_forwarding != other.internal_forwarding
                || config.external_forwarding != other.external_forwarding
                || !sources_match
            {
                warn!(
                    self.log,
                    "external multicast configuration differs across switches";
                    "group_ip" => %group_ip,
                    "first_switch" => ?first_slot,
                    "mismatched_switch" => ?slot,
                    "dpd_operation" => "fetch_external_group_for_drift_check",
                );
                return Ok(ExternalDriftCheck { external_config: None });
            }
        }

        debug!(
            self.log,
            "external group present on every switch";
            "group_ip" => %group_ip,
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        Ok(ExternalDriftCheck { external_config: Some(config) })
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
            return Err(Error::internal_error(
                "no switches were available for multicast group cleanup",
            ));
        }

        if !self.discovery_complete {
            return Err(Error::internal_error(
                "multicast group cleanup requires every expected switch, \
                 but Dendrite discovery was partial",
            ));
        }

        let dpd_tag = tag
            .parse()
            .map_err(|_| Error::internal_error("invalid multicast tag"))?;

        // Execute cleanup operations on all switches in parallel
        let cleanup_ops = dpd_clients.iter().map(|(switch_slot, client)| {
            let log = self.log.clone();
            let dpd_tag = &dpd_tag;
            async move {
                match client.multicast_reset_by_tag(dpd_tag).await {
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
    use super::*;
    use httpmock::MockServer;
    use internal_dns_types::config::DnsConfigBuilder;
    use internal_dns_types::names::ServiceName;
    use omicron_test_utils::dev::tcp_proxy::{
        ProxyTarget, RetargetableTcpProxy,
    };
    use omicron_uuid_kinds::OmicronZoneUuid;
    use serde_json::json;
    use std::net::Ipv6Addr;
    use std::time::Duration;
    use transient_dns_server::TransientDnsServer;

    #[tokio::test]
    async fn test_discovery_retains_healthy_switch_when_peer_stalls() {
        let log = Logger::root(slog::Discard, slog::o!());
        let healthy = MockServer::start_async().await;
        let _identifiers = healthy
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/switch/identifiers");
                then.status(200).json_body_obj(&json!({
                    "sidecar_id": uuid::Uuid::nil(),
                    "asic_backend": "stub",
                    "model": "test",
                    "revision": 0,
                    "serial": "test",
                    "slot": 1,
                }));
            })
            .await;
        let proxy = RetargetableTcpProxy::bind(
            ProxyTarget::Forward(*healthy.address()),
            &log,
        )
        .await
        .unwrap();
        let stalled = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let dns = TransientDnsServer::new(&log).await.unwrap();
        let mut config = DnsConfigBuilder::new();
        for port in [stalled.local_addr().unwrap().port(), proxy.port()] {
            let zone = config
                .host_zone(OmicronZoneUuid::new_v4(), Ipv6Addr::LOCALHOST)
                .unwrap();
            config
                .service_backend_zone(ServiceName::Dendrite, &zone, port)
                .unwrap();
        }
        dns.initialize_with_config(
            &log,
            &config.build_full_config_for_initial_generation(),
        )
        .await
        .unwrap();
        let resolver = Resolver::new_from_addrs(
            log.clone(),
            &[dns.dns_server.sole_local_address().unwrap()],
        )
        .unwrap();
        let dataplane = tokio::time::timeout(
            Duration::from_secs(10),
            MulticastDataplaneClient::new(resolver, log),
        )
        .await
        .unwrap();
        assert_eq!(dataplane.switch_count(), 1);
        assert!(dataplane.dpd_clients.contains_key(&SwitchSlot::Switch1));
        assert!(!dataplane.discovery_complete);
        assert!(dataplane.remove_groups("test").await.is_err());
        proxy.shutdown().await;
    }

    #[tokio::test]
    async fn test_drift_checks_every_switch_config() {
        let log = Logger::root(slog::Discard, slog::o!());
        let servers =
            [MockServer::start_async().await, MockServer::start_async().await];
        let config = json!({
            "kind": "external",
            "group_ip": "239.1.1.1",
            "external_group_id": 1,
            "tag": "test",
            "internal_forwarding": {
                "nat_target": {
                    "internal_ip": "ff04::1",
                    "inner_mac": MacAddr { a: [0x33, 0x33, 0, 0, 0, 1] },
                    "vni": 77,
                },
            },
            "external_forwarding": { "vlan_id": null },
            "sources": [
                IpSrc::Exact("192.0.2.1".parse().unwrap()),
                IpSrc::Exact("192.0.2.2".parse().unwrap()),
            ],
        });
        let _first = servers[0]
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET)
                    .path("/multicast/groups/239.1.1.1");
                then.status(200).json_body_obj(&config);
            })
            .await;
        let _underlays =
            futures::future::join_all(servers.iter().map(|server| {
                server.mock_async(|when, then| {
                    when.method(httpmock::Method::GET)
                        .path("/multicast/underlay-groups/ff04::1");
                    then.status(200).json_body_obj(&json!({
                        "group_ip": "ff04::1",
                        "external_group_id": 1,
                        "underlay_group_id": 1,
                        "tag": "test",
                        "members": [],
                    }));
                })
            }))
            .await;
        let dpd_clients = [SwitchSlot::Switch0, SwitchSlot::Switch1]
            .into_iter()
            .zip(&servers)
            .map(|(slot, server)| {
                let client = dpd_client::Client::new(
                    &server.base_url(),
                    dpd_client::ClientState {
                        tag: "nexus-test".to_string(),
                        log: log.clone(),
                    },
                )
                .unwrap();
                (slot, client)
            })
            .collect();
        let dataplane = MulticastDataplaneClient {
            dpd_clients,
            discovery_complete: true,
            log,
        };
        let mut same = config.clone();
        same["external_group_id"] = json!(2);
        same["sources"].as_array_mut().unwrap().reverse();
        let mut sources = same.clone();
        sources["sources"] =
            json!([IpSrc::Exact("192.0.2.3".parse().unwrap())]);
        let mut tag = same.clone();
        tag["tag"] = json!("other");
        let mut forwarding = same.clone();
        forwarding["internal_forwarding"]["nat_target"]["vni"] = json!(78);
        let mut vlan = same.clone();
        vlan["external_forwarding"]["vlan_id"] = json!(100);
        for (config, needs_update) in [
            (same, false),
            (sources, true),
            (tag, true),
            (forwarding, true),
            (vlan, true),
        ] {
            let second = servers[1]
                .mock_async(|when, then| {
                    when.method(httpmock::Method::GET)
                        .path("/multicast/groups/239.1.1.1");
                    then.status(200).json_body_obj(&config);
                })
                .await;
            let result = dataplane
                .fetch_external_group_for_drift_check(
                    "239.1.1.1".parse().unwrap(),
                    "ff04::1".parse().unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                result.external_config.is_none(),
                needs_update,
                "switch config: {config}",
            );
            second.delete_async().await;
        }
    }
}
