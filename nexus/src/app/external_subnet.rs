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
    // === Pattern 1: Project-scoped list/create (check project access first) ===

    // TODO(#9453): Implement using datastore external_subnet_list method
    pub(crate) async fn external_subnet_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        _pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::ExternalSubnet> {
        // Verify project access (will 404 if project doesn't exist or no access)
        let _ = project_lookup.lookup_for(authz::Action::ListChildren).await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    // TODO(#9453): Implement using datastore external_subnet_create method
    pub(crate) async fn external_subnet_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        _params: params::ExternalSubnetCreate,
    ) -> Result<views::ExternalSubnet, Error> {
        // Verify project access (uses Action::CreateChild)
        let _ = project_lookup.lookup_for(authz::Action::CreateChild).await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    // === Pattern 2: Selector-based lookup (follows floating_ip_lookup pattern) ===

    /// Look up an external subnet by selector.
    /// For stub: returns error after validating project access.
    /// Real impl: returns LookupResult<lookup::ExternalSubnet<'_>>
    ///
    /// TODO(#9453): Replace with proper lookup_resource! macro once DB models exist
    pub(crate) async fn external_subnet_lookup(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<()> {
        let lookup_type = match &selector {
            // ID alone - project context comes from the subnet itself
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(id),
                project: None,
            } => LookupType::ById(*id),

            // Name + project - verify project access first
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Name(name),
                project: Some(project),
            } => {
                let project_lookup = self.project_lookup(
                    opctx,
                    params::ProjectSelector { project: project.clone() },
                )?;
                let _ = project_lookup.lookup_for(authz::Action::Read).await?;
                LookupType::ByName(name.to_string())
            }

            // Invalid: ID with project specified
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(_),
                project: Some(_),
            } => {
                return Err(Error::invalid_request(
                    "when providing external subnet as an ID, \
                     project should not be specified",
                ));
            }

            // Invalid: Name without project
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Name(_),
                project: None,
            } => {
                return Err(Error::invalid_request(
                    "external subnet lookup by name requires \
                     project to be specified",
                ));
            }
        };
        Err(lookup_type.into_not_found(ResourceType::ExternalSubnet))
    }

    // === Pattern 3: Selector-based operations (use lookup, wrap in ProtectedLookup) ===

    // TODO(#9453): Implement using external_subnet_lookup and fetch
    pub(crate) async fn external_subnet_view(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup(opctx, selector).await.unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using external_subnet_lookup and datastore update
    pub(crate) async fn external_subnet_update(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        _params: params::ExternalSubnetUpdate,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup(opctx, selector).await.unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using external_subnet_lookup and datastore delete
    pub(crate) async fn external_subnet_delete(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> DeleteResult {
        let not_found =
            self.external_subnet_lookup(opctx, selector).await.unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // === Attach/Detach Operations ===

    // TODO(#9453): Implement using saga for async attach operation
    pub(crate) async fn external_subnet_attach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        _attach: params::ExternalSubnetAttach,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup(opctx, selector).await.unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    // TODO(#9453): Implement using saga for async detach operation
    pub(crate) async fn external_subnet_detach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup(opctx, selector).await.unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }
}
