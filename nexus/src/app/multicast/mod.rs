// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management for network traffic distribution
//!
//! This module provides multicast group management operations including
//! group creation, member management, and integration with IP pools
//! following the bifurcated design from [RFD 488](https://rfd.shared.oxide.computer/rfd/488).

use std::net::IpAddr;
use std::sync::Arc;

use nexus_db_lookup::{LookupPath, lookup};
use nexus_db_queries::authn::saga::Serialized;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::{authz, db};
use nexus_types::external_api::{params, views};
use nexus_types::identity::Resource;
use omicron_common::address::{IPV4_SSM_SUBNET, IPV6_SSM_FLAG_FIELD};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, NameOrId, UpdateResult, http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, MulticastGroupUuid};

use crate::app::sagas::multicast_group_dpd_update::{
    Params, SagaMulticastGroupDpdUpdate,
};

pub(crate) mod dataplane;

impl super::Nexus {
    /// Look up a multicast group by name or ID within a project.
    pub(crate) async fn multicast_group_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        multicast_group_selector: params::MulticastGroupSelector,
    ) -> LookupResult<lookup::MulticastGroup<'a>> {
        match multicast_group_selector {
            params::MulticastGroupSelector {
                multicast_group: NameOrId::Id(id),
                project: None,
            } => {
                let multicast_group =
                    LookupPath::new(opctx, &self.db_datastore)
                        .multicast_group_id(id);
                Ok(multicast_group)
            }
            params::MulticastGroupSelector {
                multicast_group: NameOrId::Name(name),
                project: Some(project),
            } => {
                let multicast_group = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .multicast_group_name_owned(name.into());
                Ok(multicast_group)
            }
            params::MulticastGroupSelector {
                multicast_group: NameOrId::Name(_),
                project: None,
            } => Err(Error::invalid_request(
                "project must be specified when looking up multicast group by name",
            )),
            params::MulticastGroupSelector {
                multicast_group: NameOrId::Id(_),
                ..
            } => Err(Error::invalid_request(
                "when providing a multicast group as an ID project should not be specified",
            )),
        }
    }

    /// Create a multicast group.
    pub(crate) async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::MulticastGroupCreate,
    ) -> CreateResult<db::model::ExternalMulticastGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // If an explicit multicast IP is provided, validate ASM/SSM semantics:
        // - ASM IPs must not specify sources
        // - SSM IPs must specify at least one source
        if let Some(mcast_ip) = params.multicast_ip {
            let empty: Vec<IpAddr> = Vec::new();
            let sources: &[IpAddr] =
                params.source_ips.as_deref().unwrap_or(&empty);
            validate_ssm_configuration(mcast_ip, sources)?;
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

        // Resolve VPC if provided
        let vpc_id = match &params.vpc {
            Some(vpc_selector) => {
                let vpc_lookup = self.vpc_lookup(
                    opctx,
                    params::VpcSelector {
                        vpc: vpc_selector.clone(),
                        project: Some(external::NameOrId::Id(
                            authz_project.id(),
                        )),
                    },
                )?;
                let (.., authz_vpc) =
                    vpc_lookup.lookup_for(authz::Action::Read).await?;
                Some(authz_vpc.id())
            }
            None => None,
        };

        // Create multicast group
        let group = self
            .db_datastore
            .multicast_group_create(
                opctx,
                authz_project.id(),
                self.rack_id(),
                params,
                authz_pool,
                vpc_id,
            )
            .await?;

        // Activate reconciler to process the new group ("Creating" → "Active")
        self.background_tasks.task_multicast_group_reconciler.activate();
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

    /// List multicast groups in a project.
    pub(crate) async fn multicast_groups_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::ExternalMulticastGroup> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .multicast_groups_list(opctx, &authz_project, pagparams)
            .await
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

        let underlay_group_id =
            current_group.underlay_group_id.ok_or_else(|| {
                Error::internal_error(
                    "active multicast group missing `underlay_group_id`",
                )
            })?;

        // Store old name for saga rollback
        let old_name = current_group.name().clone();
        // store the old sources
        let old_sources = current_group.source_ips.clone();

        // Validate the new source configuration if provided
        if let Some(ref new_source_ips) = params.source_ips {
            validate_ssm_configuration(
                current_group.multicast_ip.ip(),
                new_source_ips,
            )?;
        }

        // Update the database first
        let result = self
            .db_datastore
            .multicast_group_update(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(group_id.id()),
                params,
            )
            .await?;

        // If name or sources changed, execute DPD update saga to keep dataplane
        // configuration in sync with the database (including tag updates)
        if Self::needs_dataplane_update(
            old_name.as_str(),
            &params.identity.name,
            &params.source_ips,
        ) {
            let new_name = params
                .identity
                .name
                .as_ref()
                .map(|n| n.as_str())
                .unwrap_or(old_name.as_str());

            let saga_params = Params {
                serialized_authn: Serialized::for_opctx(opctx),
                external_group_id: current_group.id(),
                underlay_group_id,
                old_name: old_name.to_string(),
                new_name: new_name.to_string(),
                old_sources,
                new_sources: params
                    .source_ips
                    .as_ref()
                    .map(|ips| ips.iter().map(|ip| (*ip).into()).collect())
                    .unwrap_or_default(),
            };

            self.sagas.saga_execute::<SagaMulticastGroupDpdUpdate>(saga_params)
                .await
                .map_err(|e| Error::internal_error(&format!(
                    "failed to update multicast group DPD configuration: {}", e
                )))?;
        }

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

        // Prefer soft-delete + RPW cleanup to ensure DPD configuration is
        // removed before final deletion.
        self.db_datastore
            .mark_multicast_group_for_removal(opctx, group_id.id())
            .await?;

        // Activate reconciler to process the deletion (RPW pattern)
        self.background_tasks.task_multicast_group_reconciler.activate();

        Ok(())
    }

    /// Add an instance to a multicast group.
    pub(crate) async fn multicast_group_member_attach(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> CreateResult<db::model::MulticastGroupMember> {
        let (.., _authz_project, authz_group) =
            group_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;

        let member = self
            .db_datastore
            .multicast_group_member_add(
                opctx,
                MulticastGroupUuid::from_untyped_uuid(authz_group.id()),
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?;

        // Activate reconciler to process the new member ("Joining" → "Joined")
        self.background_tasks.task_multicast_group_reconciler.activate();
        Ok(member)
    }

    /// Remove an instance from a multicast group.
    pub(crate) async fn multicast_group_member_detach(
        self: &Arc<Self>,
        opctx: &OpContext,
        group_lookup: &lookup::MulticastGroup<'_>,
        instance_lookup: &lookup::Instance<'_>,
    ) -> DeleteResult {
        let (.., _authz_project, authz_group) =
            group_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;

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
        self.background_tasks.task_multicast_group_reconciler.activate();
        Ok(())
    }

    /// List members of a multicast group.
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
                authz_instance.id(),
                false,
            )
            .await?;
        members
            .into_iter()
            .map(views::MulticastGroupMember::try_from)
            .collect::<Result<Vec<_>, _>>()
    }

    fn needs_dataplane_update(
        old_name: &str,
        new_name: &Option<external::Name>,
        new_sources: &Option<Vec<IpAddr>>,
    ) -> bool {
        let name_changed =
            new_name.as_ref().map_or(false, |n| n.as_str() != old_name);
        let sources_changed = new_sources.is_some();
        name_changed || sources_changed
    }
}

/// Validate Source-Specific Multicast (SSM) configuration per RFC 4607:
/// <https://www.rfc-editor.org/rfc/rfc4607>
///
/// This function validates that:
/// 1. For IPv4 SSM: multicast address is in 232/8 range
/// 2. For IPv6 SSM: multicast address is in FF3x::/32 range
fn validate_ssm_configuration(
    multicast_ip: IpAddr,
    source_ips: &[IpAddr],
) -> Result<(), omicron_common::api::external::Error> {
    let is_ssm_address = match multicast_ip {
        IpAddr::V4(addr) => IPV4_SSM_SUBNET.contains(addr),
        IpAddr::V6(addr) => {
            // Check the flags nibble (high nibble of the second byte) for SSM
            let flags = (addr.octets()[1] & 0xF0) >> 4;
            flags == IPV6_SSM_FLAG_FIELD
        }
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
