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
use nexus_db_lookup::LookupPath;
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
use omicron_uuid_kinds::ExternalSubnetUuid;
use omicron_uuid_kinds::GenericUuid as _;

impl super::Nexus {
    /// Look up an external subnet by selector.
    fn external_subnet_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<lookup::ExternalSubnet<'a>> {
        match selector {
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(id),
                project: None,
            } => Ok(LookupPath::new(opctx, self.datastore())
                .external_subnet_id(ExternalSubnetUuid::from_untyped_uuid(id))),
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Name(name),
                project: Some(project),
            } => self
                .project_lookup(opctx, params::ProjectSelector { project })
                .map(|p| p.external_subnet_name_owned(name.into())),
            params::ExternalSubnetSelector {
                external_subnet: NameOrId::Id(_),
                project: Some(_),
            } => Err(Error::invalid_request(
                "when providing external subnet as an ID \
                     project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "external subnet should either be a UUID or \
                     project should be specified",
            )),
        }
    }

    pub(crate) async fn external_subnet_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<views::ExternalSubnet> {
        let (.., authz_project, _db_project) =
            project_lookup.fetch_for(authz::Action::ListChildren).await?;
        self.datastore()
            .list_external_subnets(opctx, &authz_project, pagparams)
            .await
            .map(|list| list.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn external_subnet_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: params::ExternalSubnetCreate,
    ) -> Result<views::ExternalSubnet, Error> {
        let (authz_silo, authz_project, _db_project) =
            project_lookup.fetch_for(authz::Action::CreateChild).await?;
        self.datastore()
            .create_external_subnet(
                opctx,
                &authz_silo.id(),
                &authz_project,
                params,
            )
            .await
            .map(Into::into)
    }

    pub(crate) async fn external_subnet_view(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> LookupResult<views::ExternalSubnet> {
        let (.., db_subnet) = self
            .external_subnet_lookup(opctx, selector)?
            .fetch_for(authz::Action::Read)
            .await?;
        Ok(db_subnet.into())
    }

    pub(crate) async fn external_subnet_update(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        params: params::ExternalSubnetUpdate,
    ) -> UpdateResult<views::ExternalSubnet> {
        let (.., authz_subnet, _db_subnet) = self
            .external_subnet_lookup(opctx, selector)?
            .fetch_for(authz::Action::Modify)
            .await?;
        self.datastore()
            .update_external_subnet(opctx, &authz_subnet, params.into())
            .await
            .map(Into::into)
    }

    pub(crate) async fn external_subnet_delete(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> DeleteResult {
        let (.., authz_subnet, _db_subnet) = self
            .external_subnet_lookup(opctx, selector)?
            .fetch_for(authz::Action::Delete)
            .await?;
        self.datastore().delete_external_subnet(opctx, &authz_subnet).await
    }

    // TODO-remove: This is a temporary method to ensure we continue to fail
    // reliably for the methods below that remain unimplemented.
    fn external_subnet_lookup_not_found(
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

    pub(crate) async fn external_subnet_attach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
        _attach: params::ExternalSubnetAttach,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup_not_found(selector).unwrap_err();
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(not_found))
            .await)
    }

    pub(crate) async fn external_subnet_detach(
        &self,
        opctx: &OpContext,
        selector: params::ExternalSubnetSelector,
    ) -> UpdateResult<views::ExternalSubnet> {
        let not_found =
            self.external_subnet_lookup_not_found(selector).unwrap_err();
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
