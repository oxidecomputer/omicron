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
//!
//! # Underlay Prefix
//!
//! Underlay multicast addresses live within the fixed admin-local (scoped)
//! prefix [`UNDERLAY_MULTICAST_SUBNET`] (ff04::/64). External addresses are
//! mapped deterministically into this /64 and persisted with a per-group salt
//! for collision avoidance.
//!
//! [`UNDERLAY_MULTICAST_SUBNET`]: omicron_common::address::UNDERLAY_MULTICAST_SUBNET

use std::net::IpAddr;
use std::sync::Arc;

use ipnetwork::IpNetwork;
use ref_cast::RefCast;
use slog::error;

use nexus_db_lookup::{LookupPath, lookup};
use nexus_db_model::Name;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::multicast::ExternalMulticastGroupWithSources;
use nexus_db_queries::{authz, db};
use nexus_types::external_api::{params, views};
use nexus_types::identity::Resource;
use nexus_types::multicast::MulticastGroupCreate;
use omicron_common::address::is_ssm_address;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult,
    IdentityMetadataCreateParams, ListResultVec, LookupResult,
    http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

pub(crate) mod dataplane;

/// Validate that SSM addresses have source IPs.
///
/// Source-Specific Multicast (SSM) addresses (232/8 for IPv4, ff3x::/32 for
/// IPv6) require at least one source IP. This is fundamental to SSM semantics:
///
/// - **SSM subscription model**: (S, G) - subscribe to traffic from specific
///   source(s) to a group. Without sources, the subscription doesn't make
///   much sense.
/// - **Per-member sources**: Each member specifies their own (S, G) subscriptions.
///   Different members can have different source lists for the same group.
///
/// Contrast with ASM (Any-Source Multicast):
/// - ASM subscription model: (*, G) - receive from ANY source to the group.
/// - Routing may still create (S, G) state internally, but receivers don't
///   need to specify sources upfront.
/// - Sources are optional for filtering (IGMPv3/MLDv2), not required.
///
/// This validation applies to all SSM joins (new or existing groups) because
/// every member must explicitly declare their source subscriptions.
///
/// # Arguments
/// - `group_ip`: The multicast group's IP address
/// - `source_ips`: The source IPs for the membership (None = no sources,
///   Some([]) = empty, both invalid for SSM)
///
/// # Returns
/// - `Ok(())` if validation passes (ASM address, or SSM with sources)
/// - `Err` if SSM address without sources
pub(crate) fn validate_ssm_sources(
    group_ip: std::net::IpAddr,
    source_ips: &Option<Vec<std::net::IpAddr>>,
) -> Result<(), external::Error> {
    if is_ssm_address(group_ip)
        && source_ips.as_ref().is_none_or(|s| s.is_empty())
    {
        return Err(external::Error::invalid_request(
            "SSM multicast addresses require at least one source IP",
        ));
    }
    Ok(())
}

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
    ///
    /// Note: SSM validation is done at member join time, not group creation.
    /// Groups don't store sources directly; sources are per-member. The group's
    /// `source_ips` view field shows the union of all active member sources.
    pub(crate) async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        params: &MulticastGroupCreate,
    ) -> CreateResult<db::model::ExternalMulticastGroup> {
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
    /// Returns the full API view with `source_ips` populated as the union of
    /// all member source IPs.
    ///
    /// For IP lookups, this avoids a double-fetch by fetching once to get the
    /// group, building the authz object, and authorizing.
    ///
    /// For Name/ID lookups, this uses the standard lookup + fetch path.
    pub(crate) async fn multicast_group_view(
        &self,
        opctx: &OpContext,
        selector: &params::MulticastGroupSelector,
    ) -> Result<views::MulticastGroup, external::Error> {
        let group = match &selector.multicast_group {
            params::MulticastGroupIdentifier::Ip(ip) => {
                // IP lookup -> fetch once and authorize
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
                group
            }
            _ => {
                // Name/ID lookup -> use lookup builder + fetch
                let group_lookup =
                    self.multicast_group_lookup(opctx, selector).await?;
                let (.., authz_group) =
                    group_lookup.lookup_for(authz::Action::Read).await?;
                self.db_datastore
                    .multicast_group_fetch(
                        opctx,
                        MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
                    )
                    .await?
            }
        };

        // Build the full view with source_ips (possibly) populated
        self.multicast_group_to_view(opctx, group).await
    }

    /// Convert a DB model to API view with source IPs (unioned over members) populated.
    async fn multicast_group_to_view(
        &self,
        opctx: &OpContext,
        group: db::model::ExternalMulticastGroup,
    ) -> Result<views::MulticastGroup, external::Error> {
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.identity.id);

        let source_ips_map = self
            .db_datastore
            .multicast_groups_source_ips_union(opctx, &[group_id])
            .await?;
        let source_ips =
            source_ips_map.get(&group.identity.id).cloned().unwrap_or_default();

        ExternalMulticastGroupWithSources { group, source_ips }.try_into()
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

    /// List all multicast groups with full view.
    pub(crate) async fn multicast_groups_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<views::MulticastGroup>, external::Error> {
        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::MULTICAST_GROUP_LIST,
            )
            .await?;
        let groups =
            self.db_datastore.multicast_groups_list(opctx, pagparams).await?;

        // Batch-fetch source_ips for all groups
        let group_ids: Vec<MulticastGroupUuid> = groups
            .iter()
            .map(|g| MulticastGroupUuid::from_untyped_uuid(g.identity.id))
            .collect();
        let source_ips_map = self
            .db_datastore
            .multicast_groups_source_ips_union(opctx, &group_ids)
            .await?;

        groups
            .into_iter()
            .map(|group| {
                let source_ips = source_ips_map
                    .get(&group.identity.id)
                    .cloned()
                    .unwrap_or_default();
                ExternalMulticastGroupWithSources { group, source_ips }
                    .try_into()
            })
            .collect()
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
    /// - **Source IPs**: Optional for ASM, required for SSM addresses (232/8, ff3x::/32)
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

        // Find or create the group based on identifier type.
        // SSM validation happens inside resolve functions.
        let group_id = match group_identifier {
            params::MulticastGroupIdentifier::Ip(ip) => {
                self.resolve_or_create_group_by_ip(opctx, *ip, source_ips)
                    .await?
            }
            params::MulticastGroupIdentifier::Name(name) => {
                self.resolve_or_create_group_by_name(
                    opctx,
                    name.clone().into(),
                    source_ips,
                )
                .await?
            }
            params::MulticastGroupIdentifier::Id(id) => {
                self.resolve_group_by_id(opctx, *id, source_ips).await?
            }
        };

        // Convert source IPs to IpNetwork for storage.
        let source_networks: Option<Vec<IpNetwork>> = source_ips
            .as_ref()
            .map(|ips| ips.iter().copied().map(IpNetwork::from).collect());

        // Attach the member with its source IPs
        let member = self
            .db_datastore
            .multicast_group_member_attach_to_instance(
                opctx,
                group_id,
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
                source_networks,
            )
            .await?;

        // Activate reconciler to process the new member
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(member)
    }

    /// Resolve group by IP, either by finding an existing group or creating a
    /// new one.
    ///
    /// Source IPs are per-member, not per-group. Each member can subscribe to
    /// different sources.
    ///
    /// Validates source IPs (address family match, SSM requirements) upfront.
    async fn resolve_or_create_group_by_ip(
        &self,
        opctx: &OpContext,
        ip: IpAddr,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> Result<MulticastGroupUuid, external::Error> {
        // Source IPs must match the multicast group's address family
        validate_source_address_family(ip, source_ips)?;

        // SSM groups always require sources when joining
        validate_ssm_sources(ip, source_ips)?;

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
                return Ok(MulticastGroupUuid::from_untyped_uuid(
                    existing.identity.id,
                ));
            }
            Err(external::Error::ObjectNotFound { .. }) => {
                // Fall through to creation
            }
            Err(e) => return Err(e),
        }

        let has_sources = source_ips.as_ref().is_some_and(|s| !s.is_empty());
        let create_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: generate_group_name_from_ip(ip)?,
                description: format!(
                    "Implicitly created multicast group for {ip}"
                ),
            },
            multicast_ip: Some(ip),
            mvlan: None,
            has_sources,
            // IP version is determined by the multicast IP address itself
            ip_version: None,
        };

        // Create the group; on conflict -> re-lookup
        match self.multicast_group_create(opctx, &create_params).await {
            Ok(created) => {
                Ok(MulticastGroupUuid::from_untyped_uuid(created.identity.id))
            }
            Err(external::Error::ObjectAlreadyExists { .. }) => {
                // Another request created it first -> re-lookup
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
                Ok(MulticastGroupUuid::from_untyped_uuid(group.identity.id))
            }
            Err(e) => Err(e),
        }
    }

    /// Resolve group by name, either find an existing one or create a new group.
    ///
    /// Source IPs are per-member, not per-group. This function only
    /// resolves the group identity and sources are stored with the member.
    /// The `source_ips` parameter is used to determine pool selection preference
    /// (SSM vs ASM) when creating a new group.
    ///
    /// # Validation
    ///
    /// - Existing group: Validates immediately (address family + SSM)
    /// - New group: Validates after pool allocation, since the IP is unknown
    ///   until then. If validation fails, the group is rolled back.
    async fn resolve_or_create_group_by_name(
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

        // Check if group exists; we use `fetch_for` to get multicast IP for validation
        match group_lookup.fetch_for(authz::Action::Read).await {
            Ok((.., db_group)) => {
                let group_ip = db_group.multicast_ip.ip();
                validate_source_address_family(group_ip, source_ips)?;
                validate_ssm_sources(group_ip, source_ips)?;
                return Ok(MulticastGroupUuid::from_untyped_uuid(
                    db_group.identity.id,
                ));
            }
            Err(external::Error::ObjectNotFound { .. }) => {
                // Fall through to create
            }
            Err(e) => return Err(e),
        }

        let has_sources = source_ips.as_ref().is_some_and(|s| !s.is_empty());
        let create_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: name.into(),
                description: "Implicitly created for instance attachment"
                    .to_string(),
            },
            multicast_ip: None,
            mvlan: None,
            has_sources,
            // No explicit IP, defaults to V4 when allocating from pool
            ip_version: None,
        };

        // Create the group; on conflict -> re-lookup
        match self.multicast_group_create(opctx, &create_params).await {
            Ok(created) => {
                let group_id =
                    MulticastGroupUuid::from_untyped_uuid(created.identity.id);
                let allocated_ip = created.multicast_ip.ip();

                // Post-allocation validation with rollback.
                // We couldn't validate earlier because the IP was unknown
                // until pool allocation. If validation fails, clean up
                // the orphaned group immediately.
                if let Err(e) =
                    validate_source_address_family(allocated_ip, source_ips)
                        .and_then(|_| {
                            validate_ssm_sources(allocated_ip, source_ips)
                        })
                {
                    if let Err(rollback_err) = self
                        .db_datastore
                        .mark_multicast_group_for_removal_if_no_members(
                            opctx, group_id,
                        )
                        .await
                    {
                        error!(
                            opctx.log,
                            "failed to rollback orphaned multicast group";
                            "group_id" => %group_id,
                            "allocated_ip" => %allocated_ip,
                            "error" => ?rollback_err,
                        );
                    }
                    return Err(e);
                }

                Ok(group_id)
            }
            Err(external::Error::ObjectAlreadyExists { .. }) => {
                // Another request created it first -> re-lookup
                let (.., db_group) =
                    group_lookup.fetch_for(authz::Action::Read).await?;
                let group_ip = db_group.multicast_ip.ip();
                validate_source_address_family(group_ip, source_ips)?;
                validate_ssm_sources(group_ip, source_ips)?;
                Ok(MulticastGroupUuid::from_untyped_uuid(db_group.identity.id))
            }
            Err(e) => Err(e),
        }
    }

    /// Resolve group by ID: must exist, no implicit creation.
    async fn resolve_group_by_id(
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

        // Authorize and fetch -> group must exist
        let (.., db_group) =
            group_lookup.fetch_for(authz::Action::Read).await?;

        // Validate source IPs: address family + SSM requirements
        let group_ip = db_group.multicast_ip.ip();
        validate_source_address_family(group_ip, source_ips)?;
        validate_ssm_sources(group_ip, source_ips)?;

        Ok(MulticastGroupUuid::from_untyped_uuid(db_group.identity.id))
    }

    /// Resolve a multicast group identifier to a UUID (lookup only).
    ///
    /// This is a lookup that does not create groups or perform validation.
    ///
    /// Use Case: when you need to check if a group exists before deciding
    /// whether to validate (address family, SSM requirements, etc.).
    ///
    /// Returns `ObjectNotFound` if the group doesn't exist.
    pub(crate) async fn resolve_multicast_group_identifier(
        &self,
        opctx: &OpContext,
        identifier: &params::MulticastGroupIdentifier,
    ) -> Result<MulticastGroupUuid, external::Error> {
        let selector = params::MulticastGroupSelector {
            multicast_group: identifier.clone(),
        };
        let group_lookup =
            self.multicast_group_lookup(opctx, &selector).await?;
        let (.., db_group) =
            group_lookup.fetch_for(authz::Action::Read).await?;
        Ok(MulticastGroupUuid::from_untyped_uuid(db_group.identity.id))
    }

    /// Resolve a multicast group identifier to a UUID, with source IP support.
    ///
    /// This is the preferred method for resolving multicast group identifiers
    /// when source IPs may be provided (from `MulticastGroupJoinSpec`).
    ///
    /// Validates internally so callers don't need to validate:
    /// - Address family match (IPv4 sources for IPv4 group, etc.)
    /// - SSM requirements (232/8, ff3x::/32 require sources)
    ///
    /// # Source IPs Semantics
    ///
    /// - `None` → no sources (only valid for ASM addresses)
    /// - `Some([])` → clear/no sources (only valid for ASM addresses)
    /// - `Some([a,b])` → set sources (required for SSM addresses)
    ///
    /// The address range determines SSM vs ASM mode, not the presence of sources.
    /// SSM addresses (232/8, ff3x::/32) always require sources.
    ///
    /// # Behavior
    ///
    /// - **IP identifier**: Auto-creates the group if it doesn't exist
    /// - **Name identifier**: Auto-creates if default pool exists, otherwise must exist
    /// - **ID identifier**: Group must already exist (UUID implies existing resource)
    ///
    /// # Errors
    ///
    /// - Address family mismatch between group and source IPs
    /// - SSM address without sources (any identifier type)
    pub(crate) async fn resolve_multicast_group_identifier_with_sources(
        &self,
        opctx: &OpContext,
        identifier: &params::MulticastGroupIdentifier,
        source_ips: &Option<Vec<IpAddr>>,
    ) -> Result<MulticastGroupUuid, external::Error> {
        match identifier {
            params::MulticastGroupIdentifier::Ip(ip) => {
                self.resolve_or_create_group_by_ip(opctx, *ip, source_ips).await
            }
            params::MulticastGroupIdentifier::Name(name) => {
                // Name-based: implicit auto-create if default pool exists.
                self.resolve_or_create_group_by_name(
                    opctx,
                    name.clone().into(),
                    source_ips,
                )
                .await
            }
            params::MulticastGroupIdentifier::Id(id) => {
                // ID-based: lookup only (UUID implies existing resource).
                self.resolve_group_by_id(opctx, *id, source_ips).await
            }
        }
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

// Private helpers for join logic

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_is_ssm_address() {
        // IPv4 SSM range: 232/8
        assert!(is_ssm_address(IpAddr::V4(Ipv4Addr::new(232, 1, 1, 1))));
        assert!(is_ssm_address(IpAddr::V4(Ipv4Addr::new(232, 255, 255, 255))));
        // ASM ranges
        assert!(!is_ssm_address(IpAddr::V4(Ipv4Addr::new(224, 1, 1, 1))));
        assert!(!is_ssm_address(IpAddr::V4(Ipv4Addr::new(239, 1, 1, 1))));

        // IPv6 SSM range: ff3x::/32
        assert!(is_ssm_address(IpAddr::V6(Ipv6Addr::new(
            0xff31, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(is_ssm_address(IpAddr::V6(Ipv6Addr::new(
            0xff3e, 0, 0, 0, 0, 0, 0, 1
        ))));
        // ASM ranges
        assert!(!is_ssm_address(IpAddr::V6(Ipv6Addr::new(
            0xff0e, 0, 0, 0, 0, 0, 0, 1
        ))));
        assert!(!is_ssm_address(IpAddr::V6(Ipv6Addr::new(
            0xff1e, 0, 0, 0, 0, 0, 0, 1
        ))));
    }
}
