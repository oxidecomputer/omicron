// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project APIs, contained within organizations

use crate::app::sagas;
use crate::authn;
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
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub fn project_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        project_selector: &'a params::ProjectSelector,
    ) -> LookupResult<lookup::Project<'a>> {
        match project_selector {
            params::ProjectSelector {
                project: NameOrId::Id(id),
                organization_selector: None,
            } => {
                let project =
                    LookupPath::new(opctx, &self.db_datastore).project_id(*id);
                Ok(project)
            }
            params::ProjectSelector {
                project: NameOrId::Name(name),
                organization_selector: Some(organization_selector),
            } => {
                let project = self
                    .organization_lookup(opctx, organization_selector)?
                    .project_name(Name::ref_cast(name));
                Ok(project)
            }
            params::ProjectSelector {
                project: NameOrId::Id(_),
                organization_selector: Some(_)
            } => {
                Err(Error::invalid_request(
                    "when providing project as an ID, organization should not be specified",
                ))
            }
            _ => Err(Error::invalid_request(
                    "project should either be specified by id or organization should be specified"
            )),
        }
    }

    pub async fn project_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let (.., authz_org) =
            organization_lookup.lookup_for(authz::Action::CreateChild).await?;

        let saga_params = sagas::project_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_create: new_project.clone(),
            authz_org,
        };
        let saga_outputs = self
            .execute_saga::<sagas::project_create::SagaProjectCreate>(
                saga_params,
            )
            .await?;
        let (_authz_project, db_project) = saga_outputs
            .lookup_node_output::<(authz::Project, db::model::Project)>(
                "project",
            )
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from project create saga")?;
        Ok(db_project)
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) =
            organization_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .projects_list_by_name(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        organization_lookup: &lookup::Organization<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) =
            organization_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .projects_list_by_id(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn project_update(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        new_params: &params::ProjectUpdate,
    ) -> UpdateResult<db::model::Project> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .project_update(opctx, &authz_project, new_params.clone().into())
            .await
    }

    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
    ) -> DeleteResult {
        let (.., authz_project, db_project) =
            project_lookup.fetch_for(authz::Action::Delete).await?;
        self.db_datastore
            .project_delete(opctx, &authz_project, &db_project)
            .await
    }

    // Role assignments

    pub async fn project_fetch_policy(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
    ) -> LookupResult<shared::Policy<authz::ProjectRole>> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ReadPolicy).await?;
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
        project_lookup: &lookup::Project<'_>,
        policy: &shared::Policy<authz::ProjectRole>,
    ) -> UpdateResult<shared::Policy<authz::ProjectRole>> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ModifyPolicy).await?;

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
