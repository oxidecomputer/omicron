// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared multicast dataplane operations for sagas and reconciler.
//!
//! Unified interface for multicast group and member operations in the
//! dataplane (DPD - Data Plane Daemon).
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
//! This enables cross-project and cross-silo multicast while maintaining
//! security through API authorization and underlay membership control.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use futures::future::try_join_all;
use ipnetwork::IpNetwork;
use oxnet::MulticastMac;
use slog::{Logger, debug, error, info};

use dpd_client::Error as DpdError;
use dpd_client::types::{
    AdminScopedIpv6, ExternalForwarding, InternalForwarding, IpSrc, MacAddr,
    MulticastGroupCreateExternalEntry, MulticastGroupCreateUnderlayEntry,
    MulticastGroupExternalResponse, MulticastGroupMember,
    MulticastGroupResponse, MulticastGroupUnderlayResponse,
    MulticastGroupUpdateExternalEntry, MulticastGroupUpdateUnderlayEntry,
    NatTarget, Vni,
};
use internal_dns_resolver::Resolver;

use nexus_db_model::{ExternalMulticastGroup, UnderlayMulticastGroup};
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::api::external::{Error, SwitchLocation};
use omicron_common::vlan::VlanID;

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

/// Trait for converting database IPv6 types into DPD's
/// [`AdminScopedIpv6`] type.
trait IntoAdminScoped {
    /// Convert to [`AdminScopedIpv6`], rejecting IPv4 addresses.
    fn into_admin_scoped(self) -> Result<AdminScopedIpv6, Error>;
}

impl IntoAdminScoped for IpAddr {
    fn into_admin_scoped(self) -> Result<AdminScopedIpv6, Error> {
        match self {
            IpAddr::V6(ipv6) => Ok(AdminScopedIpv6(ipv6)),
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
/// TODO: Add `switch_port_uplinks` configuration to multicast groups to specify
/// which rack switch ports (e.g., `<switch>.<port>`) should carry multicast traffic
/// out of the rack to external groups.
pub(crate) struct MulticastDataplaneClient {
    // Will be used to fetch mvlan from multicast_group table in follow-up commit
    _datastore: Arc<DataStore>,
    dpd_clients: HashMap<SwitchLocation, dpd_client::Client>,
    log: Logger,
}

/// Parameters for multicast group updates.
#[derive(Debug)]
pub(crate) struct GroupUpdateParams<'a> {
    pub external_group: &'a ExternalMulticastGroup,
    pub underlay_group: &'a UnderlayMulticastGroup,
    pub new_name: &'a str,
    pub new_sources: &'a [IpNetwork],
}

impl MulticastDataplaneClient {
    /// Create a new client - builds fresh DPD clients for current switch
    /// topology.
    pub(crate) async fn new(
        datastore: Arc<DataStore>,
        resolver: Resolver,
        log: Logger,
    ) -> MulticastDataplaneResult<Self> {
        let dpd_clients = dpd_clients(&resolver, &log).await.map_err(|e| {
            error!(
                log,
                "failed to build DPD clients";
                "error" => %e
            );
            Error::internal_error("failed to build DPD clients")
        })?;
        Ok(Self { _datastore: datastore, dpd_clients, log })
    }

    async fn dpd_ensure_underlay_created(
        &self,
        client: &dpd_client::Client,
        ip: AdminScopedIpv6,
        tag: &str,
        switch: &SwitchLocation,
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
                    "switch" => %switch,
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
                            "switch" => %switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_ensure_underlay_created"
                        );
                        Error::internal_error("underlay fetch failed")
                    })?
                    .into_inner())
            }
            Err(e) => {
                error!(
                    self.log,
                    "underlay create failed";
                    "underlay_ip" => %ip,
                    "switch" => %switch,
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
        switch: &SwitchLocation,
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
                    "switch" => %switch,
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
                            "switch" => %switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_ensure_external_created"
                        );
                        Error::internal_error("external fetch failed")
                    })?;
                Ok(response.into_inner().into_external_response()?)
            }
            Err(e) => {
                error!(
                    self.log,
                    "external create failed";
                    "external_ip" => %create.group_ip,
                    "switch" => %switch,
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
        update: &MulticastGroupUpdateExternalEntry,
        create: &MulticastGroupCreateExternalEntry,
        switch: &SwitchLocation,
    ) -> MulticastDataplaneResult<MulticastGroupExternalResponse> {
        match client.multicast_group_update_external(&group_ip, update).await {
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
                                    "switch" => %switch,
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
                            "switch" => %switch,
                            "error" => %e,
                            "dpd_operation" => "dpd_update_external_or_create"
                        );
                        Err(Error::internal_error("external ensure failed"))
                    }
                }
            }
            Err(e) => {
                error!(
                    self.log,
                    "external update failed";
                    "external_ip" => %group_ip,
                    "switch" => %switch,
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
    pub(crate) async fn create_groups(
        &self,
        external_group: &ExternalMulticastGroup,
        underlay_group: &UnderlayMulticastGroup,
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
            "source_mode" => if external_group.source_ips.is_empty() { "ASM" } else { "SSM" },
            "dpd_operation" => "create_groups"
        );

        let dpd_clients = &self.dpd_clients;
        let tag = external_group.name().to_string();

        // Convert MVLAN to u16 for DPD, validating through VlanID
        let vlan_id = external_group
            .mvlan
            .map(|v| VlanID::new(v as u16))
            .transpose()
            .map_err(|e| {
                Error::internal_error(&format!("invalid VLAN ID: {e:#}"))
            })?
            .map(u16::from);
        let underlay_ip_admin =
            underlay_group.multicast_ip.ip().into_admin_scoped()?;
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

        let sources_dpd = external_group
            .source_ips
            .iter()
            .map(|ip| IpSrc::Exact(ip.ip()))
            .collect::<Vec<_>>();

        let external_group_ip = external_group.multicast_ip.ip();

        let create_operations =
            dpd_clients.into_iter().map(|(switch_location, client)| {
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
                            switch_location,
                        )
                        .await?;

                    let external_entry = MulticastGroupCreateExternalEntry {
                        group_ip: external_group_ip,
                        external_forwarding: ExternalForwarding { vlan_id },
                        internal_forwarding: InternalForwarding {
                            nat_target: Some(nat_target),
                        },
                        tag: Some(tag.clone()),
                        sources: Some(sources),
                    };

                    let external_response = self
                        .dpd_ensure_external_created(
                            client,
                            &external_entry,
                            switch_location,
                        )
                        .await?;

                    Ok::<_, Error>((
                        switch_location,
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
                "recovery" => "saga_will_rollback_partial_configuration",
                "dpd_operation" => "create_groups"
            );
            // Rollback handled by saga layer
            e
        })?;

        // Collect results
        let programmed_switches: Vec<SwitchLocation> =
            results.iter().map(|(loc, _, _)| **loc).collect();
        let (_loc, underlay_last, external_last) =
            results.into_iter().last().ok_or_else(|| {
                Error::internal_error("no switches were configured")
            })?;

        debug!(
            self.log,
            "DPD multicast forwarding configuration completed - all switches configured";
            "external_group_id" => %external_group.id(),
            "external_multicast_ip" => %external_group.multicast_ip,
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => ?underlay_last.group_ip,
            "switch_count" => programmed_switches.len(),
            "dpd_operations_completed" => "[create_external_group, create_underlay_group, configure_nat_mapping]",
            "external_forwarding_vlan" => ?external_last.external_forwarding.vlan_id,
            "dpd_operation" => "create_groups"
        );

        Ok((underlay_last, external_last))
    }

    /// Update a multicast group's tag (name) and/or sources in the dataplane.
    pub(crate) async fn update_groups(
        &self,
        params: GroupUpdateParams<'_>,
    ) -> MulticastDataplaneResult<(
        MulticastGroupUnderlayResponse,
        MulticastGroupExternalResponse,
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
        // Convert MVLAN to u16 for DPD, validating through VlanID
        let vlan_id = params
            .external_group
            .mvlan
            .map(|v| VlanID::new(v as u16))
            .transpose()
            .map_err(|e| {
                Error::internal_error(&format!("invalid VLAN ID: {e:#}"))
            })?
            .map(u16::from);
        let underlay_ip_admin =
            params.underlay_group.multicast_ip.ip().into_admin_scoped()?;
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

        let new_name_str = params.new_name.to_string();
        let external_group_ip = params.external_group.multicast_ip.ip();

        let sources_dpd = params
            .new_sources
            .iter()
            .map(|ip| IpSrc::Exact(ip.ip()))
            .collect::<Vec<_>>();

        let update_operations =
            dpd_clients.into_iter().map(|(switch_location, client)| {
                let new_name = new_name_str.clone();
                let nat_target = nat_target.clone();
                let sources = sources_dpd.clone();
                let underlay_ip_admin = underlay_ip_admin.clone();
                async move {
                    // Ensure/get underlay members, create if missing
                    let members = match client
                        .multicast_group_get_underlay(&underlay_ip_admin)
                        .await
                    {
                        Ok(r) => r.into_inner().members,
                        Err(DpdError::ErrorResponse(resp))
                            if resp.status()
                                == reqwest::StatusCode::NOT_FOUND =>
                        {
                            // Create missing underlay group with new tag and empty members
                            let created = self
                                .dpd_ensure_underlay_created(
                                    client,
                                    underlay_ip_admin.clone(),
                                    &new_name,
                                    switch_location,
                                )
                                .await?;
                            created.members
                        }
                        Err(e) => {
                            error!(
                                self.log,
                                "failed to fetch underlay for update";
                                "underlay_ip" => %underlay_ip_admin,
                                "switch" => %switch_location,
                                "error" => %e
                            );
                            return Err(Error::internal_error(
                                "failed to fetch underlay for update",
                            ));
                        }
                    };

                    // Update underlay tag preserving members
                    let underlay_entry = MulticastGroupUpdateUnderlayEntry {
                        members,
                        tag: Some(new_name.clone()),
                    };
                    let underlay_response = client
                        .multicast_group_update_underlay(
                            &underlay_ip_admin,
                            &underlay_entry,
                        )
                        .await
                        .map_err(|e| {
                            error!(
                                self.log,
                                "failed to update underlay";
                                "underlay_ip" => %underlay_ip_admin,
                                "switch" => %switch_location,
                                "error" => %e
                            );
                            Error::internal_error("failed to update underlay")
                        })?;

                    // Prepare external update/create entries with pre-computed data
                    let external_forwarding = ExternalForwarding { vlan_id };
                    let internal_forwarding =
                        InternalForwarding { nat_target: Some(nat_target) };

                    let update_entry = MulticastGroupUpdateExternalEntry {
                        external_forwarding: external_forwarding.clone(),
                        internal_forwarding: internal_forwarding.clone(),
                        tag: Some(new_name.clone()),
                        sources: Some(sources.clone()),
                    };
                    let create_entry = MulticastGroupCreateExternalEntry {
                        group_ip: external_group_ip,
                        external_forwarding,
                        internal_forwarding,
                        tag: Some(new_name.clone()),
                        sources: Some(sources),
                    };

                    let external_response = self
                        .dpd_update_external_or_create(
                            client,
                            external_group_ip,
                            &update_entry,
                            &create_entry,
                            switch_location,
                        )
                        .await?;

                    Ok::<_, Error>((
                        switch_location,
                        underlay_response.into_inner(),
                        external_response,
                    ))
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

        // Get the last response (all switches should return equivalent responses)
        let results_len = results.len();
        let (_loc, underlay_last, external_last) =
            results.into_iter().last().ok_or_else(|| {
                Error::internal_error("no switches were updated")
            })?;

        debug!(
            self.log,
            "successfully updated multicast groups on all switches";
            "external_group_id" => %params.external_group.id(),
            "switches_updated" => results_len,
            "new_name" => params.new_name,
            "dpd_operation" => "update_groups"
        );

        Ok((underlay_last, external_last))
    }

    /// Modify multicast group members across all switches in parallel.
    async fn modify_group_membership<F>(
        &self,
        underlay_group: &UnderlayMulticastGroup,
        member: MulticastGroupMember,
        operation_name: &str,
        modify_fn: F,
    ) -> MulticastDataplaneResult<()>
    where
        F: Fn(
                Vec<dpd_client::types::MulticastGroupMember>,
                MulticastGroupMember,
            ) -> Vec<dpd_client::types::MulticastGroupMember>
            + Clone
            + Send
            + 'static,
    {
        let dpd_clients = &self.dpd_clients;
        let operation_name = operation_name.to_string();

        let modify_ops = dpd_clients.iter().map(|(location, client)| {
            let underlay_ip = underlay_group.multicast_ip.ip();
            let member = member.clone();
            let log = self.log.clone();
            let modify_fn = modify_fn.clone();
            let operation_name = operation_name.clone();

            async move {
                // Get current underlay group state
                let current_group = client
                    .multicast_group_get_underlay(&underlay_ip.into_admin_scoped()?)
                    .await
                    .map_err(|e| {
                        error!(
                            log,
                            "underlay get failed";
                            "underlay_ip" => %underlay_ip,
                            "switch" => %location,
                            "error" => %e,
                            "dpd_operation" => "modify_group_membership_get"
                        );
                        Error::internal_error("underlay get failed")
                    })?;

                // Apply the modification function
                let current_group_inner = current_group.into_inner();
                let updated_members = modify_fn(current_group_inner.members, member.clone());

                let update_entry = MulticastGroupUpdateUnderlayEntry {
                    members: updated_members,
                    tag: current_group_inner.tag,
                };

                client
                    .multicast_group_update_underlay(&underlay_ip.into_admin_scoped()?, &update_entry)
                    .await
                    .map_err(|e| {
                        error!(
                            log,
                            "underlay member modify failed";
                            "operation_name" => operation_name.as_str(),
                            "underlay_ip" => %underlay_ip,
                            "switch" => %location,
                            "error" => %e,
                            "dpd_operation" => "modify_group_membership_update"
                        );
                        Error::internal_error("underlay member modify failed")
                    })?;

                info!(
                    log,
                    "DPD multicast member operation completed on switch";
                    "operation_name" => operation_name.as_str(),
                    "underlay_group_ip" => %underlay_ip,
                    "member_port_id" => %member.port_id,
                    "member_link_id" => %member.link_id,
                    "member_direction" => ?member.direction,
                    "switch_location" => %location,
                    "dpd_operation" => %format!("{}_member_in_underlay_group", operation_name.as_str())
                );

                Ok::<(), Error>(())
            }
        });

        try_join_all(modify_ops).await?;
        Ok(())
    }

    /// Add a member to a multicast group in the dataplane.
    pub(crate) async fn add_member(
        &self,
        underlay_group: &UnderlayMulticastGroup,
        member: MulticastGroupMember,
    ) -> MulticastDataplaneResult<()> {
        info!(
            self.log,
            "DPD multicast member addition initiated across rack switches";
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => %underlay_group.multicast_ip,
            "member_port_id" => %member.port_id,
            "member_link_id" => %member.link_id,
            "member_direction" => ?member.direction,
            "switch_count" => self.switch_count(),
            "dpd_operation" => "update_underlay_group_members"
        );

        self.modify_group_membership(
            underlay_group,
            member,
            "add",
            |mut existing_members, new_member| {
                // Add to existing members (avoiding duplicates)
                if !existing_members.iter().any(|m| {
                    m.port_id == new_member.port_id
                        && m.link_id == new_member.link_id
                        && m.direction == new_member.direction
                }) {
                    existing_members.push(new_member);
                }
                existing_members
            },
        )
        .await
    }

    /// Remove a member from a multicast group in the dataplane.
    pub(crate) async fn remove_member(
        &self,
        underlay_group: &UnderlayMulticastGroup,
        member: MulticastGroupMember,
    ) -> MulticastDataplaneResult<()> {
        info!(
            self.log,
            "DPD multicast member removal initiated across rack switches";
            "underlay_group_id" => %underlay_group.id,
            "underlay_multicast_ip" => %underlay_group.multicast_ip,
            "member_port_id" => %member.port_id,
            "member_link_id" => %member.link_id,
            "member_direction" => ?member.direction,
            "switch_count" => self.switch_count(),
            "dpd_operation" => "update_underlay_group_members"
        );

        self.modify_group_membership(
            underlay_group,
            member,
            "remove",
            |existing_members, target_member| {
                // Filter out the target member
                existing_members
                    .into_iter()
                    .filter(|m| {
                        !(m.port_id == target_member.port_id
                            && m.link_id == target_member.link_id
                            && m.direction == target_member.direction)
                    })
                    .collect()
            },
        )
        .await
    }

    /// Detect and log cross-switch drift for multicast groups.
    ///
    /// We logs errors if:
    /// - Group is present on some switches but missing on others (presence drift)
    /// - Group has different configurations across switches (config drift)
    fn log_drift_issues<'a>(
        &self,
        group_ip: IpAddr,
        first_location: &SwitchLocation,
        first_config: &MulticastGroupResponse,
        found_results: &[&'a (
            &'a SwitchLocation,
            Option<MulticastGroupResponse>,
        )],
        not_found_count: usize,
    ) {
        let total_switches = found_results.len() + not_found_count;

        // Check for cross-switch presence drift (group missing on some switches)
        if not_found_count > 0 {
            error!(
                self.log,
                "cross-switch drift detected: group missing on some switches";
                "group_ip" => %group_ip,
                "switches_with_group" => found_results.len(),
                "switches_without_group" => not_found_count,
                "total_switches" => total_switches,
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
        }

        // Check for config mismatches between switches (functional style)
        found_results
            .iter()
            .filter_map(|(loc, resp)| resp.as_ref().map(|r| (loc, r)))
            .filter(|(_, cfg)| *cfg != first_config)
            .for_each(|(location, _)| {
                error!(
                    self.log,
                    "cross-switch drift detected: different configs on switches";
                    "group_ip" => %group_ip,
                    "first_switch" => %first_location,
                    "mismatched_switch" => %location,
                    "dpd_operation" => "fetch_external_group_for_drift_check"
                );
            });
    }

    /// Fetch external multicast group DPD state for RPW drift detection.
    ///
    /// Queries all switches to detect configuration drift. If any switch has
    /// different state (missing group, different config), it will return the
    /// found state, so the reconciler can trigger an UPDATE
    /// saga that will fix all switches atomically.
    pub(crate) async fn fetch_external_group_for_drift_check(
        &self,
        group_ip: IpAddr,
    ) -> MulticastDataplaneResult<Option<MulticastGroupExternalResponse>> {
        debug!(
            self.log,
            "fetching external group state from all switches for drift detection";
            "group_ip" => %group_ip,
            "switch_count" => self.switch_count(),
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        let fetch_ops = self.dpd_clients.iter().map(|(location, client)| {
            let log = self.log.clone();
            async move {
                match client.multicast_group_get(&group_ip).await {
                    Ok(response) => {
                        Ok((location, Some(response.into_inner())))
                    }
                    Err(DpdError::ErrorResponse(resp))
                        if resp.status() == reqwest::StatusCode::NOT_FOUND =>
                    {
                        debug!(
                            log,
                            "external group not found on switch";
                            "group_ip" => %group_ip,
                            "switch" => %location,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        Ok((location, None))
                    }
                    Err(e) => {
                        error!(
                            log,
                            "external group fetch failed";
                            "group_ip" => %group_ip,
                            "switch" => %location,
                            "error" => %e,
                            "dpd_operation" => "fetch_external_group_for_drift_check"
                        );
                        Err(Error::internal_error(&format!(
                            "failed to fetch external group from DPD: {e}"
                        )))
                    }
                }
            }
        });

        let results = try_join_all(fetch_ops).await?;

        // Partition results into found/not-found for drift analysis
        let (found, not_found): (Vec<_>, Vec<_>) =
            results.iter().partition(|(_, resp)| resp.is_some());

        if found.is_empty() {
            // Group doesn't exist on any switch
            debug!(
                self.log,
                "external group not found on any switch (expected for new groups)";
                "group_ip" => %group_ip,
                "switches_queried" => results.len(),
                "dpd_operation" => "fetch_external_group_for_drift_check"
            );
            return Ok(None);
        }

        // Get first found config for comparison and return value
        let (first_location, first_config) = found
            .first()
            .and_then(|(loc, resp)| resp.as_ref().map(|r| (*loc, r)))
            .expect(
                "found_results non-empty check guarantees at least one element",
            );

        // Detect and log any cross-switch drift
        self.log_drift_issues(
            group_ip,
            first_location,
            first_config,
            &found,
            not_found.len(),
        );

        debug!(
            self.log,
            "external group state fetched from all switches";
            "group_ip" => %group_ip,
            "switches_queried" => results.len(),
            "switches_with_group" => found.len(),
            "dpd_operation" => "fetch_external_group_for_drift_check"
        );

        // Return first found config (reconciler will compare with DB and launch UPDATE if needed)
        Ok(Some(first_config.clone().into_external_response()?))
    }

    /// Fetch the hardware backplane map from DPD for topology validation.
    ///
    /// Queries a single switch to get the backplane topology map, which should
    /// be identical across all switches. Used by the reconciler to validate that
    /// inventory `sp_slot` values are within the valid range for
    /// the current hardware.
    pub(crate) async fn fetch_backplane_map(
        &self,
    ) -> MulticastDataplaneResult<
        std::collections::BTreeMap<
            dpd_client::types::PortId,
            dpd_client::types::BackplaneLink,
        >,
    > {
        let (switch_location, client) =
            self.dpd_clients.iter().next().ok_or_else(|| {
                Error::internal_error("no DPD clients available")
            })?;

        debug!(
            self.log,
            "fetching backplane map from DPD for topology validation";
            "switch" => %switch_location,
            "query_scope" => "single_switch",
            "dpd_operation" => "fetch_backplane_map"
        );

        match client.backplane_map().await {
            Ok(response) => {
                let backplane_map_raw = response.into_inner();

                // Convert HashMap<String, BackplaneLink> to BTreeMap<PortId, BackplaneLink>
                // DPD returns string keys like "rear0", "rear1" - parse them to PortId
                let backplane_map: std::collections::BTreeMap<_, _> = backplane_map_raw
                    .into_iter()
                    .filter_map(|(port_str, link)| {
                        match dpd_client::types::PortId::try_from(port_str.as_str()) {
                            Ok(port_id) => Some((port_id, link)),
                            Err(e) => {
                                error!(
                                    self.log,
                                    "failed to parse port ID from backplane map";
                                    "port_str" => %port_str,
                                    "error" => %e,
                                    "dpd_operation" => "fetch_backplane_map"
                                );
                                None
                            }
                        }
                    })
                    .collect();

                debug!(
                    self.log,
                    "backplane map fetched from DPD";
                    "switch" => %switch_location,
                    "port_count" => backplane_map.len(),
                    "dpd_operation" => "fetch_backplane_map"
                );
                Ok(backplane_map)
            }
            Err(e) => {
                error!(
                    self.log,
                    "backplane map fetch failed";
                    "switch" => %switch_location,
                    "error" => %e,
                    "dpd_operation" => "fetch_backplane_map"
                );
                Err(Error::internal_error(&format!(
                    "failed to fetch backplane map from DPD: {e}"
                )))
            }
        }
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

        // Execute cleanup operations on all switches in parallel
        let cleanup_ops = dpd_clients.iter().map(|(location, client)| {
            let tag = tag.to_string();
            let log = self.log.clone();
            async move {
                match client.multicast_reset_by_tag(&tag).await {
                    Ok(_) => {
                        debug!(
                            log,
                            "cleaned up multicast groups";
                            "switch" => %location,
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
                            "switch" => %location,
                            "tag" => %tag
                        );
                        Ok::<(), Error>(())
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to clean up multicast groups by tag";
                            "switch" => %location,
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
