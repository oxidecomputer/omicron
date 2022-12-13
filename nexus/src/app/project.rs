// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project APIs, contained within organizations

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use crate::external_api::shared;
use anyhow::Context;
use nexus_defaults as defaults;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use uuid::Uuid;

impl super::Nexus {
    pub fn project_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        project_selector: &'a params::ProjectSelector,
    ) -> LookupResult<lookup::Project<'a>> {
        match project_selector {
            params::ProjectSelector { project: NameOrId::Id(id), .. } => {
                // TODO: 400 if organization is present
                let project =
                    LookupPath::new(opctx, &self.db_datastore).project_id(*id);
                Ok(project)
            }
            params::ProjectSelector {
                project: NameOrId::Name(project_name),
                organization: Some(NameOrId::Id(organization_id)),
            } => {
                let project = LookupPath::new(opctx, &self.db_datastore)
                    .organization_id(*organization_id)
                    .project_name(Name::ref_cast(project_name));
                Ok(project)
            }
            params::ProjectSelector {
                project: NameOrId::Name(project_name),
                organization: Some(NameOrId::Name(organization_name)),
            } => {
                let project = LookupPath::new(opctx, &self.db_datastore)
                    .organization_name(Name::ref_cast(organization_name))
                    .project_name(Name::ref_cast(project_name));
                Ok(project)
            }
            _ => Err(Error::InvalidRequest {
                message: "
                    Unable to resolve project. Expected one of
                        - project: Uuid
                        - project: Name, organization: Uuid 
                        - project: Name, organization: Name
                    "
                .to_string(),
            }),
        }
    }
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

    pub async fn project_fetch_by_id(
        &self,
        opctx: &OpContext,
        project_id: &Uuid,
    ) -> LookupResult<db::model::Project> {
        let (.., db_project) = LookupPath::new(opctx, &self.db_datastore)
            .project_id(*project_id)
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
            .lookup_for(authz::Action::ListChildren)
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
            .lookup_for(authz::Action::ListChildren)
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
        let (.., authz_project, db_project) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .fetch_for(authz::Action::Delete)
                .await?;
        self.db_datastore
            .project_delete(opctx, &authz_project, &db_project)
            .await
    }

    // Role assignments

    pub async fn project_fetch_policy(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> LookupResult<shared::Policy<authz::ProjectRole>> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ReadPolicy)
            .await?;
        let role_assignments = self
            .db_datastore
            .role_assignment_fetch_visible(opctx, &authz_project)
            .await?
            .into_iter()
            .map(|r| r.try_into().context("parsing database role assignment"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;
        Ok(shared::Policy { role_assignments })
    }

    pub async fn project_update_policy(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        policy: &shared::Policy<authz::ProjectRole>,
    ) -> UpdateResult<shared::Policy<authz::ProjectRole>> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ModifyPolicy)
            .await?;

        let role_assignments = self
            .db_datastore
            .role_assignment_replace_visible(
                opctx,
                &authz_project,
                &policy.role_assignments,
            )
            .await?
            .into_iter()
            .map(|r| r.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(shared::Policy { role_assignments })
    }
}
