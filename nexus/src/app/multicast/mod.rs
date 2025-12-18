// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management for network traffic distribution.
//!
//! Group creation, member management, and IP pool integration following
//! the bifurcated design from [RFD 488](https://rfd.shared.oxide.computer/rfd/488).
//!
//! ## Fleet-Scoped Authorization Model
//!
//! Multicast groups are **fleet-scoped resources** (authz parent = "Fleet"),
//! similar to IP pools. This enables:
//!
//! - **Cross-project multicast**: Instances from different projects can join
//!   the same group without IP waste
//! - **Cross-silo multicast**: Instances from different silos can join the
//!   same group (when pools are linked to multiple silos)
//!
//! ### Authorization Rules
//!
//! - **Creating/modifying/deleting groups**: Any authenticated user in the fleet (silo users)
//!   can create, modify, and delete multicast groups
//! - **Reading/listing groups**: Any authenticated user in the fleet can read and list groups
//!   (enables discovery of available groups for joining instances)
//! - **Listing group members**: Only requires Read permission on the group (fleet-scoped),
//!   not permissions on individual member instances
//! - **Adding/removing members**: Requires Read on group + Modify on the specific instance
//!   (project collaborators can attach only their own instances to any fleet-scoped group)
//!
//! ### VNI Assignment
//!
//! All fleet-scoped multicast groups use `DEFAULT_MULTICAST_VNI` (77), which is
//! reserved for fleet-wide multicast traffic and below the `MIN_GUEST_VNI` (1024)
//! threshold. This ensures consistent behavior across all multicast groups.

use std::net::IpAddr;
use std::sync::Arc;

use ref_cast::RefCast;

use nexus_config::DEFAULT_UNDERLAY_MULTICAST_NET;
use nexus_db_lookup::{LookupPath, lookup};
use nexus_db_model::Name;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::{authz, db};
use nexus_types::external_api::{params, views};
use omicron_common::address::{IPV4_SSM_SUBNET, IPV6_SSM_SUBNET};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, NameOrId, UpdateResult, http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

pub(crate) mod dataplane;

impl super::Nexus {
    /// Look up a fleet-scoped multicast group by name or ID.
    pub(crate) fn multicast_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        multicast_group_selector: &'a params::MulticastGroupSelector,
    ) -> LookupResult<lookup::MulticastGroup<'a>> {
        // Multicast groups are fleet-scoped (like IP pools)
        match &multicast_group_selector.multicast_group {
            NameOrId::Id(id) => {
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_id(*id);
                Ok(multicast_group)
            }
            NameOrId::Name(name) => {
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_name(Name::ref_cast(name));
                Ok(multicast_group)
            }
        }
    }

    /// Create a multicast group.
    pub(crate) async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        params: &params::MulticastGroupCreate,
    ) -> CreateResult<db::model::ExternalMulticastGroup> {
        // Authorization FIRST: check before validating parameters
        // This ensures 403 Forbidden is returned before 400 Bad Request
        opctx
            .authorize(authz::Action::CreateChild, &authz::MULTICAST_GROUP_LIST)
            .await?;

        // If an explicit multicast IP is provided, validate ASM/SSM semantics
        // and ensure it does not collide with the fixed underlay prefix.
        // - ASM IPs must not specify sources
        // - SSM IPs must specify at least one source
        if let Some(mcast_ip) = params.multicast_ip {
            let empty: Vec<IpAddr> = Vec::new();
            let sources: &[IpAddr] =
                params.source_ips.as_deref().unwrap_or(&empty);
            validate_ssm_configuration(mcast_ip, sources)?;

            // Block external IPv6 multicast addresses that fall within the
            // fixed underlay admin-local prefix (reserved for underlay).
            if let IpAddr::V6(ipv6) = mcast_ip {
                // Convert fixed underlay prefix to ipnet and compare
                let fixed_underlay: ipnet::Ipv6Net =
                    DEFAULT_UNDERLAY_MULTICAST_NET
                        .to_string()
                        .parse()
                        .expect("valid fixed underlay admin prefix");
                if fixed_underlay.contains(&ipv6) {
                    return Err(Error::invalid_request(&format!(
                        "IPv6 address {ipv6} is within the reserved underlay multicast prefix {}",
                        fixed_underlay
                    )));
                }
            }
        }

        let authz_pool = match &params.pool {
            Some(pool_selector) => {
                let authz_pool = self
                    .ip_pool_lookup(opctx, &pool_selector)?
                    .lookup_for(authz::Action::CreateChild)
                    .await?
                    .0;

                // Validate that the pool is of type Multicast
                Some(
                    self.db_datastore
                        .resolve_pool_for_allocation(
                            opctx,
                            Some(authz_pool),
                            nexus_db_model::IpPoolType::Multicast,
                        )
                        .await?,
                )
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

    /// Fetch a multicast group.
    pub(crate) async fn multicast_group_fetch(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
    ) -> LookupResult<db::model::ExternalMulticastGroup> {
        let (.., group_id) =
            group_lookup.lookup_for(authz::Action::Read).await?;
        self.db_datastore
            .multicast_group_fetch(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
            )
            .await
    }

    /// Look up multicast group by IP address.
    pub(crate) async fn multicast_group_lookup_by_ip(
        &self,
        opctx: &OpContext,
        ip_addr: std::net::IpAddr,
    ) -> LookupResult<db::model::ExternalMulticastGroup> {
        self.db_datastore.multicast_group_lookup_by_ip(opctx, ip_addr).await
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

    /// Update a multicast group.
    pub(crate) async fn multicast_group_update(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        params: &params::MulticastGroupUpdate,
    ) -> UpdateResult<db::model::ExternalMulticastGroup> {
        let (.., group_id) =
            group_lookup.lookup_for(authz::Action::Modify).await?;

        // Get the current group to check state and get underlay group ID
        let current_group = self
            .db_datastore
            .multicast_group_fetch(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
            )
            .await?;

        // Ensure group is in "Active" state (should have `underlay_group_id`)
        if current_group.state != db::model::MulticastGroupState::Active {
            return Err(Error::invalid_request(&format!(
                "cannot update multicast group in state: {state}. group must be in \"Active\" state.",
                state = current_group.state
            )));
        }

        // Ensure the group has an associated underlay group (required for updates)
        current_group.underlay_group_id.ok_or_else(|| {
            Error::internal_error(
                "active multicast group missing `underlay_group_id`",
            )
        })?;

        // Validate the new source configuration if provided
        if let Some(ref new_source_ips) = params.source_ips {
            validate_ssm_configuration(
                current_group.multicast_ip.ip(),
                new_source_ips,
            )?;
        }

        // Update the database
        let result = self
            .db_datastore
            .multicast_group_update(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
                params,
            )
            .await?;

        // Activate RPW to apply changes to DPD (eventually consistent)
        // The reconciler will detect drift and launch the UPDATE saga
        self.background_tasks.task_multicast_reconciler.activate();

        Ok(result)
    }

    /// Tag a multicast group for deletion.
    pub(crate) async fn multicast_group_delete(
        &self,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
    ) -> DeleteResult {
        let (.., group_id) =
            group_lookup.lookup_for(authz::Action::Delete).await?;

        // Mark for deletion via RPW: sets state="Deleting" (not soft-delete).
        // RPW cleanup ensures DPD configuration is removed before final deletion.
        self.db_datastore
            .mark_multicast_group_for_removal(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
            )
            .await?;

        // Activate reconciler to process the "Deleting" state
        self.background_tasks.task_multicast_reconciler.activate();

        Ok(())
    }

    /// Add an instance to a multicast group.
    pub(crate) async fn multicast_group_member_attach(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> CreateResult<db::model::MulticastGroupMember> {
        // Multicast groups are fleet-scoped - users only need Read permission on the group
        // and Modify permission on the instance to attach it
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Read).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let member = self
            .db_datastore
            .multicast_group_member_add(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?;

        // Activate reconciler to process the new member ("Joining" → "Joined")
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(member)
    }

    /// Remove an instance from a multicast group.
    pub(crate) async fn multicast_group_member_detach(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> DeleteResult {
        // Multicast groups are fleet-scoped - users only need Read permission on the group
        // and Modify permission on the instance to detach it
        let (.., authz_group) =
            group_lookup.lookup_for(authz::Action::Read).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        // First, get the member ID by group and instance
        // For idempotency, if the member doesn't exist, we consider the removal successful
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
                // Member doesn't exist - removal is idempotent, return success
                return Ok(());
            }
        };

        self.db_datastore
            .multicast_group_member_delete_by_id(opctx, member.id)
            .await?;

        // Activate reconciler to process the member removal
        self.background_tasks.task_multicast_reconciler.activate();
        Ok(())
    }

    /// List members of a multicast group.
    ///
    /// ##  Authorization
    ///
    /// This operation only requires "Read" permission on the multicast group
    /// itself (fleet-scoped). It does NOT check permissions on the individual
    /// instances that are members of the group.
    ///
    /// This asymmetry is intentional:
    /// - **Listing members**: Allows discovery of which instances are in a group
    ///   (useful for understanding multicast group membership across projects)
    /// - **Adding/removing members**: Requires Modify permission on the specific
    ///   instance (project-scoped), enforcing that users can only manage instances
    ///   they own
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
            group_lookup.lookup_for(authz::Action::Read).await?;
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
                false,
            )
            .await?;
        members
            .into_iter()
            .map(views::MulticastGroupMember::try_from)
            .collect::<Result<Vec<_>, _>>()
    }
}

/// Validate Source-Specific Multicast (SSM) configuration per RFC 4607:
/// <https://www.rfc-editor.org/rfc/rfc4607>
///
/// This function validates that:
/// 1. For IPv4 SSM: multicast address is in 232/8 range
/// 2. For IPv6 SSM: multicast address is in FF30::/12 range (covers all FF3x::/32 SSM scopes)
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
