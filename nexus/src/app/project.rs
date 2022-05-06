// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project APIs, contained within organizations

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::defaults;
use crate::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl super::Nexus {
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Create a project.
        let db_project =
            db::model::Project::new(authz_org.id(), new_project.clone());
        let db_project = self
            .db_datastore
            .project_create(opctx, &authz_org, db_project)
            .await?;

        // TODO: We probably want to have "project creation" and "default VPC
        // creation" co-located within a saga for atomicity.
        //
        // Until then, we just perform the operations sequentially.

        // Create a default VPC associated with the project.
        // TODO-correctness We need to be using the project_id we just created.
        // project_create() should return authz::Project and we should use that
        // here.
        let _ = self
            .project_create_vpc(
                opctx,
                &organization_name,
                &new_project.identity.name.clone().into(),
                &params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "default".parse().unwrap(),
                        description: "Default VPC".to_string(),
                    },
                    ipv6_prefix: Some(defaults::random_vpc_ipv6_prefix()?),
                    // TODO-robustness this will need to be None if we decide to
                    // handle the logic around name and dns_name by making
                    // dns_name optional
                    dns_name: "default".parse().unwrap(),
                },
            )
            .await?;

        Ok(db_project)
    }

    pub async fn project_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> LookupResult<db::model::Project> {
        let (.., db_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .fetch()
            .await?;
        Ok(db_project)
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_name(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_id(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn project_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        new_params: &params::ProjectUpdate,
    ) -> UpdateResult<db::model::Project> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update(opctx, &authz_project, new_params.clone().into())
            .await
    }

    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> DeleteResult {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.project_delete(opctx, &authz_project).await
    }
}
