// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        new_organization: &params::OrganizationCreate,
    ) -> CreateResult<db::model::Organization> {
        self.db_datastore.organization_create(opctx, new_organization).await
    }

    pub async fn organization_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
    ) -> LookupResult<db::model::Organization> {
        let (.., db_organization) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .fetch()
            .await?;
        Ok(db_organization)
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
        organization_name: &Name,
    ) -> DeleteResult {
        let (.., authz_org, db_org) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .fetch()
                .await?;
        self.db_datastore.organization_delete(opctx, &authz_org, &db_org).await
    }

    pub async fn organization_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_params: &params::OrganizationUpdate,
    ) -> UpdateResult<db::model::Organization> {
        let (.., authz_organization) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .lookup_for(authz::Action::Modify)
                .await?;
        self.db_datastore
            .organization_update(
                opctx,
                &authz_organization,
                new_params.clone().into(),
            )
            .await
    }
}
