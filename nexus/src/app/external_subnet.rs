// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! External Subnets, project-scoped resources allocated from subnet pools
//!
//! External subnets are similar to floating IPs but allocate entire subnets
//! rather than individual IP addresses. They can be attached to instances
//! to provide external connectivity.
//!
//! TODO(#9453): This module contains stub implementations that return
//! "not implemented" errors. Full implementation requires:
//! - Database schema and models (see nexus/db-model/)
//! - Datastore methods (see nexus/db-queries/src/db/datastore/)
//! - Authorization resources (see nexus/auth/src/authz/)
//! - Sagas for attach/detach operations
//! - Replacing these stubs with real implementations

use crate::app::Unimpl;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::{params, views};
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;

impl super::Nexus {
    /// Look up an external subnet by selector.
    ///
    /// TODO: This is a stub that always returns a not-found error. The real
    /// implementation should match other selector lookup functions (see
    /// `disk_lookup`, `vpc_lookup`, `floating_ip_lookup`, etc.):
    ///
    /// - Add lifetime parameter `'a` to the function
    /// - Add `opctx: &'a OpContext` parameter
    /// - Return `LookupResult<lookup::ExternalSubnet<'a>>`
    /// - Return `Ok(lookup_handle)` on success instead of `Err`
    fn external_subnet_lookup(
        &self,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<()> {
        let lookup_type = match selector {
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(id),
                project: None,
            } => LookupType::ById(id),
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Name(name),
                project: Some(_),
            } => LookupType::ByName(name.to_string()),
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(_),
                ..
            } => {
                return Err(Error::invalid_request(
                    "when providing external subnet as an ID \
                     project should not be specified",
                ));
            }
            _ => {
                return Err(Error::invalid_request(
                    "external subnet should either be a UUID or \
                     project should be specified",
                ));
            }
        };
        Err(lookup_type.into_not_found(ResourceType::ExternalSubnet))
    }

    pub(crate) async fn external_subnet_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::ExternalSubnet> {
        let _ = project_lookup.lookup_for(authz::Action::ListChildren).await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub(crate) async fn external_subnet_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        _params: params::ExternalSubnetCreate,
    ) -> Result<views::ExternalSubnet, Error> {
        let _ = project_lookup.lookup_for(authz::Action::CreateChild).await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub(crate) async fn external_subnet_view(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<views::ExternalSubnet> {
        let not_found = self.external_subnet_lookup(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn external_subnet_update(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        _params: params::ExternalSubnetUpdate,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found = self.external_subnet_lookup(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn external_subnet_delete(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> DeleteResult {
        let not_found = self.external_subnet_lookup(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn external_subnet_attach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        _attach: params::ExternalSubnetAttach,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found = self.external_subnet_lookup(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn external_subnet_detach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found = self.external_subnet_lookup(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn instance_list_external_subnets(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> ListResultVec<views::ExternalSubnet> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Read).await?;

        // External subnets are project-scoped, so check ListChildren on project
        opctx.authorize(authz::Action::ListChildren, &authz_project).await?;

        Ok(self
            .db_datastore
            .instance_lookup_external_subnets(opctx, &authz_instance)
            .await?
            .into_iter()
            .map(|subnet| subnet.into())
            .collect())
    }
}
