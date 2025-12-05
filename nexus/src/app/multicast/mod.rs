// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management.
//!
//! See [RFD 488](https://rfd.shared.oxide.computer/rfd/488) for the bifurcated design.
//!
//! # Scoping
//!
//! Multicast groups are fleet-scoped for visibility: any authenticated user can
//! list and read groups, and instances from different projects or silos can
//! join the same group.
//!
//! # Access control
//!
//! Fleet administrators control multicast access by creating multicast IP pools
//! and linking them to silos. A silo can only use multicast pools that are
//! linked to it. Cross-silo multicast is enabled by linking the same pool to
//! multiple silos. This is the same model used for unicast IP pools.
//!
//! # Lifecycle
//!
//! Groups are created implicitly when the first member joins (via member-add)
//! and deleted when the last member leaves. The group's IP is allocated from
//! the multicast pool on creation and returned on deletion.
//!
//! Groups use their UUID as the dpd tag for switch configuration. This avoids
//! races when group names are reused after deletion.
//!
//! # Authorization
//!
//! - list/read groups: any authenticated user
//! - add/remove members: requires read on the group and modify on the instance
//!
//! # VNI
//!
//! All multicast groups use `DEFAULT_MULTICAST_VNI` (77), which is reserved for
//! multicast and below the guest VNI range.

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;

use ipnetwork::IpNetwork;
use ref_cast::RefCast;

use nexus_db_lookup::{LookupPath, lookup};
use nexus_db_model::Name;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::{authz, db};
use nexus_types::external_api::{params, views};
use nexus_types::identity::Resource;
use nexus_types::multicast::MulticastGroupCreate;
use omicron_common::address::{IPV4_SSM_SUBNET, IPV6_SSM_SUBNET};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult,
    IdentityMetadataCreateParams, ListResultVec, LookupResult,
    http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

pub(crate) mod dataplane;

impl super::Nexus {
    /// Look up a fleet-scoped multicast group by name, ID, or IP address.
    ///
    /// Returns a lookup builder for authorization and fetching. For IP lookups,
    /// the group is fetched first to resolve the ID, then a builder is created.
    pub(crate) async fn multicast_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        multicast_group_selector: &'a params::MulticastGroupSelector,
    ) -> LookupResult<lookup::MulticastGroup<'a>> {
        // Multicast groups are fleet-scoped (like IP pools)
        match &multicast_group_selector.multicast_group {
            params::MulticastGroupIdentifier::Id(id) => {
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_id(*id);
                Ok(multicast_group)
            }
            params::MulticastGroupIdentifier::Name(name) => {
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_name(Name::ref_cast(name));
                Ok(multicast_group)
            }
            params::MulticastGroupIdentifier::Ip(ip) => {
                // IP lookup requires fetching first to resolve the ID
                let group = self
                    .db_datastore
                    .multicast_group_lookup_by_ip(opctx, *ip)
                    .await?;
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_id(group.identity.id);
                Ok(multicast_group)
            }
        }
    }

    /// Create a multicast group (called during implicit creation from join).
    ///
    /// Access control is enforced by pool linking: the IP is allocated from a
    /// multicast pool linked to the caller's silo.
    pub(crate) async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        params: &MulticastGroupCreate,
    ) -> CreateResult<db::model::ExternalMulticastGroup> {
        // If an explicit multicast IP is provided, validate ASM/SSM semantics.
        //
        // Reserved ranges (ff00-ff02::/16) are validated at IP pool creation.
        // ff04::/16 addresses are allowed in pools, but the reconciler XOR-folds
        // them during underlay mapping to prevent collision with the fixed
        // underlay prefix (ff04::/64). See `map_external_to_underlay_ip_impl`.
        //
        // - ASM IPs should not specify sources
        // - SSM IPs require at least one source
        if let Some(mcast_ip) = params.multicast_ip {
            let empty: Vec<IpAddr> = Vec::new();
            let sources: &[IpAddr] =
                params.source_ips.as_deref().unwrap_or(&empty);
            validate_ssm_configuration(mcast_ip, sources)?;
        }

        // If multicast_ip is provided, discover the pool containing that IP.
        // Otherwise, pool resolution happens in the datastore layer.
        let authz_pool = match params.multicast_ip {
            Some(ip) => {
                Some(self.resolve_pool_for_multicast_ip(opctx, ip).await?)
            }
            None => None,
        };

        // Create multicast group (fleet-scoped, uses DEFAULT_MULTICAST_VNI)
        let group = self
            .db_datastore
            .multicast_group_create(opctx, params, authz_pool)
            .await?;

        // Activate reconciler to process the new group ("Creating" → "Active")
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(group)
    }

    /// View a multicast group by selector.
    ///
    /// For IP lookups, this avoids a double-fetch by fetching once to get
    /// the group, building the authz object, and authorizing. For Name/ID
    /// lookups, this uses the standard lookup + fetch path.
    pub(crate) async fn multicast_group_view(
        &self,
        opctx: &OpContext,
        selector: &params::MulticastGroupSelector,
    ) -> LookupResult<db::model::ExternalMulticastGroup> {
        match &selector.multicast_group {
            params::MulticastGroupIdentifier::Ip(ip) => {
                // IP lookup - fetch once and authorize
                let group = self
                    .db_datastore
                    .multicast_group_lookup_by_ip(opctx, *ip)
                    .await?;
                let authz_group = authz::MulticastGroup::new(
                    authz::FLEET,
                    group.identity.id,
                    external::LookupType::ById(group.identity.id),
                );
                opctx.authorize(authz::Action::Read, &authz_group).await?;
                Ok(group)
            }
            _ => {
                // Name/ID lookup - use lookup builder + fetch
                let group_lookup =
                    self.multicast_group_lookup(opctx, selector).await?;
                let (.., authz_group) =
                    group_lookup.lookup_for(authz::Action::Read).await?;
                self.db_datastore
                    .multicast_group_fetch(
                        opctx,
                        MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
                    )
                    .await
            }
        }
    }

    /// Resolve which multicast pool contains a given IP address.
    ///
    /// Used for join-by-IP functionality where the user specifies a multicast
    /// IP address directly. The system auto-discovers which pool contains the
    /// IP (pool ranges are globally unique, so lookup is unambiguous) and
    /// returns the authz pool for group creation.
    ///
    /// Note: only multicast pools linked to the caller's silo are
    /// considered. Pool linking controls access to multicast addresses.
    pub(crate) async fn resolve_pool_for_multicast_ip(
        &self,
        opctx: &OpContext,
        ip: IpAddr,
    ) -> Result<authz::IpPool, external::Error> {
        let pool = self
            .db_datastore
            .ip_pool_containing_multicast_ip(opctx, ip)
            .await?
            .ok_or_else(|| {
                external::Error::invalid_request(
                    "multicast IP not in any pool's address range",
                )
            })?;

        Ok(authz::IpPool::new(
            authz::FLEET,
            pool.id(),
            external::LookupType::ById(pool.id()),
        ))
    }

    /// List all multicast groups.
    pub(crate) async fn multicast_groups_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::ExternalMulticastGroup> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::MULTICAST_GROUP_LIST,
            )
            .await?;
        self.db_datastore.multicast_groups_list(opctx, pagparams).await
    }

    /// Join an instance to a multicast group by identifier (IP, name, or ID).
    ///
    /// # Authorization
    ///
    /// Requires `Modify` on the instance. Groups are fleet-scoped resources
    /// readable by any authenticated user; authorization is enforced on the
    /// instance being attached.
    ///
    /// # Behavior
    ///
    /// - **IP/name joins**: Creates the group implicitly if it doesn't exist
    /// - **ID joins**: The group must already exist (returns error otherwise)
    /// - **SSM validation**: If `source_ips` provided, validates SSM configuration
    pub(crate) async fn instance_join_multicast_group(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_identifier: &params::MulticastGroupIdentifier,
        instance_lookup: &lookup::Instance<'_>,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> CreateResult<db::model::MulticastGroupMember> {
        // Check if multicast is enabled
        if !self.multicast_enabled() {
            return Err(external::Error::invalid_request(
                "multicast functionality is currently disabled",
            ));
        }

        // Authorize instance modification upfront
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        // Find or create the group based on identifier type
        let group_id = match group_identifier {
            params::MulticastGroupIdentifier::Ip(ip) => {
                self.join_resolve_by_ip(opctx, *ip, source_ips).await?
            }
            params::MulticastGroupIdentifier::Name(name) => {
                self.join_resolve_by_name(
                    opctx,
                    name.clone().into(),
                    source_ips,
                )
                .await?
            }
            params::MulticastGroupIdentifier::Id(id) => {
                self.join_resolve_by_id(opctx, *id, source_ips).await?
            }
        };

        // Attach the member
        let member = self
            .db_datastore
            .multicast_group_member_add(
                opctx,
                group_id,
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?;

        // Activate reconciler to process the new member
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(member)
    }

    /// Resolve group by IP: find existing or create new.
    async fn join_resolve_by_ip(
        &self,
        opctx: &OpContext,
        ip: IpAddr,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> Result<MulticastGroupUuid, external::Error> {
        // Try to find existing group by IP
        match self.db_datastore.multicast_group_lookup_by_ip(opctx, ip).await {
            Ok(existing) => {
                // Authorize Read for audit trail symmetry with name/ID paths
                let authz_group = authz::MulticastGroup::new(
                    authz::FLEET,
                    existing.identity.id,
                    external::LookupType::ById(existing.identity.id),
                );
                opctx.authorize(authz::Action::Read, &authz_group).await?;
                validate_sources_match(source_ips, &existing.source_ips)?;
                return Ok(MulticastGroupUuid::from_untyped_uuid(
                    existing.identity.id,
                ));
            }
            Err(external::Error::ObjectNotFound { .. }) => {
                // Fall through to create
            }
            Err(e) => return Err(e),
        }

        // SSM addresses require at least one source IP
        if is_ssm_address(ip) && source_ips.is_none() {
            return Err(external::Error::invalid_request(
                "SSM multicast addresses require at least one source IP",
            ));
        }

        // Source IPs must match the multicast group's address family
        validate_source_address_family(ip, source_ips)?;

        let create_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: generate_group_name_from_ip(ip)?,
                description: format!(
                    "Implicitly created multicast group for {ip}"
                ),
            },
            multicast_ip: Some(ip),
            source_ips: source_ips.clone(),
            mvlan: None,
        };

        // Create the group; on conflict -> re-lookup
        match self.multicast_group_create(opctx, &create_params).await {
            Ok(created) => {
                Ok(MulticastGroupUuid::from_untyped_uuid(created.identity.id))
            }
            Err(external::Error::ObjectAlreadyExists { .. }) => {
                // Another request created it first, validate sources match
                let group = self
                    .db_datastore
                    .multicast_group_lookup_by_ip(opctx, ip)
                    .await?;
                // Authorize Read for audit trail symmetry
                let authz_group = authz::MulticastGroup::new(
                    authz::FLEET,
                    group.identity.id,
                    external::LookupType::ById(group.identity.id),
                );
                opctx.authorize(authz::Action::Read, &authz_group).await?;
                validate_sources_match(source_ips, &group.source_ips)?;
                Ok(MulticastGroupUuid::from_untyped_uuid(group.identity.id))
            }
            Err(e) => Err(e),
        }
    }

    /// Resolve group by name, either find existing or create a new group.
    async fn join_resolve_by_name(
        &self,
        opctx: &OpContext,
        name: Name,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> Result<MulticastGroupUuid, external::Error> {
        let selector = params::MulticastGroupSelector {
            multicast_group: params::MulticastGroupIdentifier::Name(
                name.clone().into(),
            ),
        };
        let group_lookup =
            self.multicast_group_lookup(opctx, &selector).await?;

        // Check if group exists (`lookup_for` does authz + returns ID)
        match group_lookup.lookup_for(authz::Action::Read).await {
            Ok((.., authz_group)) => {
                let group_id =
                    MulticastGroupUuid::from_untyped_uuid(authz_group.id());
                let group = self
                    .db_datastore
                    .multicast_group_fetch(opctx, group_id)
                    .await?;
                validate_sources_match(source_ips, &group.source_ips)?;
                return Ok(group_id);
            }
            Err(external::Error::ObjectNotFound { .. }) => {
                // Fall through to create
            }
            Err(e) => return Err(e),
        }

        let create_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: name.into(),
                description: "Implicitly created for instance attachment"
                    .to_string(),
            },
            multicast_ip: None,
            source_ips: source_ips.clone(),
            mvlan: None,
        };

        // Create the group; on conflict -> re-lookup
        match self.multicast_group_create(opctx, &create_params).await {
            Ok(created) => {
                Ok(MulticastGroupUuid::from_untyped_uuid(created.identity.id))
            }
            Err(external::Error::ObjectAlreadyExists { .. }) => {
                // Another request created it first, re-lookup and validate
                let (.., authz_group) =
                    group_lookup.lookup_for(authz::Action::Read).await?;
                let group_id =
                    MulticastGroupUuid::from_untyped_uuid(authz_group.id());
                let group = self
                    .db_datastore
                    .multicast_group_fetch(opctx, group_id)
                    .await?;
                validate_sources_match(source_ips, &group.source_ips)?;
                Ok(group_id)
            }
            Err(e) => Err(e),
        }
    }

    /// Resolve group by ID: must exist, no implicit creation.
    async fn join_resolve_by_id(
        &self,
        opctx: &OpContext,
        id: uuid::Uuid,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> Result<MulticastGroupUuid, external::Error> {
        let selector = params::MulticastGroupSelector {
            multicast_group: params::MulticastGroupIdentifier::Id(id),
        };
        let group_lookup =
            self.multicast_group_lookup(opctx, &selector).await?;

        // Authorize and fetch - group must exist
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Read).await?;
        let group_id = MulticastGroupUuid::from_untyped_uuid(authz_group.id());
        let group =
            self.db_datastore.multicast_group_fetch(opctx, group_id).await?;
        validate_sources_match(source_ips, &group.source_ips)?;

        Ok(group_id)
    }

    /// Remove an instance from a multicast group.
    ///
    /// # Authorization
    ///
    /// Requires `Read` on the group and `Modify` on the instance.
    ///
    /// # Behavior
    ///
    /// - **Idempotent**: Returns success if the member doesn't exist
    /// - **Implicit deletion**: If this was the last member, marks the group
    ///   for deletion (reconciler completes cleanup)
    pub(crate) async fn instance_leave_multicast_group(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> DeleteResult {
        // Check if multicast is enabled - if not, skip member removal
        if !self.multicast_enabled() {
            return Err(external::Error::invalid_request(
                "multicast functionality is currently disabled",
            ));
        }

        // Authorize: Modify on instance (checked first), Read on group
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Read).await?;

        // Idempotent: if member doesn't exist, return success
        let member = match self
            .db_datastore
            .multicast_group_member_get_by_group_and_instance(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?
        {
            Some(member) => member,
            None => {
                return Ok(());
            }
        };

        self.db_datastore
            .multicast_group_member_delete_by_id(opctx, member.id)
            .await?;

        // Atomically mark group for deletion if this was the last member.
        // The NOT EXISTS guard in the datastore method prevents race conditions
        // where a concurrent join could slip in between a "list members" check
        // and the mark-for-removal call.
        let _ = self
            .db_datastore
            .mark_multicast_group_for_removal_if_no_members(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
            )
            .await?;

        // Activate reconciler to process the member removal (and group deletion if triggered)
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(())
    }

    /// List members of a multicast group.
    ///
    /// # Authorization
    ///
    /// Requires `Read` on the multicast group (fleet-scoped). Does not check
    /// permissions on individual member instances.
    ///
    /// This asymmetry is intentional:
    /// - **Listing members**: Allows discovery of group membership across projects
    /// - **Adding/removing members**: Requires `Modify` on the specific instance
    ///   (project-scoped), so users can only manage their own instances
    ///
    /// Note: When unauthorized users attempt to add/remove instances they don't
    /// have access to, the instance lookup fails with 404 (not 403) to prevent
    /// information leakage about instances in inaccessible projects.
    pub(crate) async fn multicast_group_members_list(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        pagparams: &DataPageParams<'_, uuid::Uuid>,
    ) -> ListResultVec<db::model::MulticastGroupMember> {
        let (.., group_id) =
            group_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .multicast_group_members_list(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
                pagparams,
            )
            .await
    }

    /// List all multicast group memberships for an instance.
    ///
    /// Active-only: returns memberships that have not been soft-deleted
    /// (i.e., `time_deleted IS NULL`). For diagnostics that require
    /// historical memberships, query the datastore with
    /// `include_removed = true`.
    pub(crate) async fn instance_list_multicast_groups(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> ListResultVec<views::MulticastGroupMember> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;
        let members = self
            .db_datastore
            .multicast_group_members_list_by_instance(
                opctx,
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?;
        members
            .into_iter()
            .map(views::MulticastGroupMember::try_from)
            .collect::<Result<Vec<_>, _>>()
    }
}

/// Validate SSM configuration per [RFC 4607]: IPv4 232/8 or IPv6 ff30::/12.
///
/// [RFC 4607]: https://www.rfc-editor.org/rfc/rfc4607
fn validate_ssm_configuration(
    multicast_ip: IpAddr,
    source_ips: &[IpAddr],
) -> Result<(), omicron_common::api::external::Error> {
    let is_ssm_address = match multicast_ip {
        IpAddr::V4(addr) => IPV4_SSM_SUBNET.contains(addr),
        IpAddr::V6(addr) => IPV6_SSM_SUBNET.contains(addr),
    };

    let has_sources = !source_ips.is_empty();

    match (is_ssm_address, has_sources) {
        (true, false) => Err(external::Error::invalid_request(
            "SSM multicast addresses require at least one source IP",
        )),
        (false, true) => Err(external::Error::invalid_request(
            "ASM multicast addresses cannot have sources. \
             Use SSM range (232.x.x.x for IPv4, FF3x:: for IPv6) for source-specific multicast",
        )),
        _ => Ok(()), // (true, true) and (false, false) are valid
    }
}

// Private helpers for join logic

/// Check if an IP is in the SSM range.
fn is_ssm_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(addr) => IPV4_SSM_SUBNET.contains(addr),
        IpAddr::V6(addr) => IPV6_SSM_SUBNET.contains(addr),
    }
}

/// Validate that source IPs match the multicast group's address family.
fn validate_source_address_family(
    multicast_ip: IpAddr,
    source_ips: &Option<Vec<IpAddr>>,
) -> Result<(), external::Error> {
    let Some(sources) = source_ips else {
        return Ok(());
    };

    let is_v4_group = multicast_ip.is_ipv4();
    for source in sources {
        if source.is_ipv4() != is_v4_group {
            return Err(external::Error::invalid_request(&format!(
                "source IP {source} does not match multicast group address family ({})",
                if is_v4_group { "IPv4" } else { "IPv6" }
            )));
        }
    }
    Ok(())
}

/// Generate a group name from an IP address (e.g., "mcast-224-1-2-3").
fn generate_group_name_from_ip(
    ip: IpAddr,
) -> Result<omicron_common::api::external::Name, external::Error> {
    let name_str = match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, d] = v4.octets();
            format!("mcast-{a}-{b}-{c}-{d}")
        }
        IpAddr::V6(v6) => {
            // Use segments for consistent formatting (avoids :: compression issues)
            let segs = v6.segments();
            format!(
                "mcast-{:x}-{:x}-{:x}-{:x}-{:x}-{:x}-{:x}-{:x}",
                segs[0],
                segs[1],
                segs[2],
                segs[3],
                segs[4],
                segs[5],
                segs[6],
                segs[7]
            )
        }
    };
    name_str.parse().map_err(|_| {
        external::Error::internal_error(&format!(
            "IP should be valid as group name: {ip}"
        ))
    })
}

/// Validate that requested sources match existing group sources.
///
/// If `requested` is `None`, the join inherits the group's existing sources
/// (implicit acceptance). If `requested` is `Some`, the sources must exactly
/// match the group's existing sources - partial overlap is not allowed.
fn validate_sources_match(
    requested: &Option<Vec<IpAddr>>,
    existing: &[IpNetwork],
) -> Result<(), external::Error> {
    // None means "inherit existing sources" - always valid
    let Some(req_sources) = requested else {
        return Ok(());
    };

    let requested_set: HashSet<IpNetwork> =
        req_sources.iter().copied().map(IpNetwork::from).collect();
    let existing_set: HashSet<&IpNetwork> = existing.iter().collect();

    if requested_set.len() != existing_set.len()
        || !requested_set.iter().all(|ip| existing_set.contains(ip))
    {
        return Err(external::Error::invalid_request(
            "multicast group already exists with different source IPs",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_validate_ssm_configuration() {
        // Valid ASM - ASM address with no sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)),
                &[]
            )
            .is_ok()
        );

        // Valid SSM - SSM address with sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V4(Ipv4Addr::new(232, 1, 1, 1)),
                &[IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))]
            )
            .is_ok()
        );

        // Valid SSM IPv6 - FF3x::/32 range with sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V6(Ipv6Addr::new(0xff31, 0, 0, 0, 0, 0, 0, 1)),
                &[IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))]
            )
            .is_ok()
        );

        // Invalid - ASM address with sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1)),
                &[IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))]
            )
            .is_err()
        );

        // Invalid - SSM address without sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V4(Ipv4Addr::new(232, 1, 1, 1)),
                &[]
            )
            .is_err()
        );

        // Invalid - IPv6 ASM address with sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V6(Ipv6Addr::new(0xff0e, 0, 0, 0, 0, 0, 0, 1)),
                &[IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))]
            )
            .is_err()
        );

        // Invalid - IPv6 SSM address without sources
        assert!(
            validate_ssm_configuration(
                IpAddr::V6(Ipv6Addr::new(0xff31, 0, 0, 0, 0, 0, 0, 1)),
                &[]
            )
            .is_err()
        );
    }
}
