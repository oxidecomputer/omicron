// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Organizations, and roles contained within

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use crate::external_api::shared;
use anyhow::Context;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

impl super::Nexus {
    pub fn organization_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        organization_selector: &'a params::OrganizationSelector,
    ) -> LookupResult<lookup::Organization<'a>> {
        match organization_selector {
            params::OrganizationSelector(NameOrId::Id(id)) => {
                let organization = LookupPath::new(opctx, &self.db_datastore)
                    .organization_id(*id);
                Ok(organization)
            }
            params::OrganizationSelector(NameOrId::Name(name)) => {
                let organization = LookupPath::new(opctx, &self.db_datastore)
                    .organization_name(Name::ref_cast(name));
                Ok(organization)
            }
        }
    }
    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        new_organization: &params::OrganizationCreate,
    ) -> CreateResult<db::model::Organization> {
        self.db_datastore.organization_create(opctx, new_organization).await
    }

    pub async fn organizations_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Organization> {
        self.db_datastore.organizations_list_by_name(opctx, pagparams).await
    }

    pub async fn organizations_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Organization> {
        self.db_datastore.organizations_list_by_id(opctx, pagparams).await
    }

    pub async fn organization_delete(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
    ) -> DeleteResult {
        let (.., authz_org, db_org) = organization_lookup.fetch().await?;
        self.db_datastore.organization_delete(opctx, &authz_org, &db_org).await
    }

    pub async fn organization_update(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
        new_params: &params::OrganizationUpdate,
    ) -> UpdateResult<db::model::Organization> {
        let (.., authz_organization) =
            organization_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .organization_update(
                opctx,
                &authz_organization,
                new_params.clone().into(),
            )
            .await
    }

    // Role assignments

    pub async fn organization_fetch_policy(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
    ) -> LookupResult<shared::Policy<authz::OrganizationRole>> {
        let (.., authz_org) =
            organization_lookup.lookup_for(authz::Action::ReadPolicy).await?;
        let role_assignments = self
            .db_datastore
            .role_assignment_fetch_visible(opctx, &authz_org)
            .await?
            .into_iter()
            .map(|r| r.try_into().context("parsing database role assignment"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;
        Ok(shared::Policy { role_assignments })
    }

    pub async fn organization_update_policy(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
        policy: &shared::Policy<authz::OrganizationRole>,
    ) -> UpdateResult<shared::Policy<authz::OrganizationRole>> {
        let (.., authz_org) =
            organization_lookup.lookup_for(authz::Action::ModifyPolicy).await?;

        let role_assignments = self
            .db_datastore
            .role_assignment_replace_visible(
                opctx,
                &authz_org,
                &policy.role_assignments,
            )
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(shared::Policy { role_assignments })
    }
}
