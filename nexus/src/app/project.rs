// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project APIs

use crate::app::sagas;
use crate::authn;
use crate::authz;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::external_api::shared;
use anyhow::Context;
use nexus_db_model::Name;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;
use std::sync::Arc;

impl super::Nexus {
    pub fn project_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        project_selector: params::ProjectSelector,
    ) -> LookupResult<lookup::Project<'a>> {
        let lookup_path = LookupPath::new(opctx, &self.db_datastore);
        Ok(match project_selector {
            params::ProjectSelector { project: NameOrId::Id(id) } => {
                lookup_path.project_id(id)
            }
            params::ProjectSelector { project: NameOrId::Name(name) } => {
                lookup_path.project_name_owned(name.into())
            }
        })
    }

    pub async fn project_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Project")?;
        opctx.authorize(authz::Action::CreateChild, &authz_silo).await?;

        let saga_params = sagas::project_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_create: new_project.clone(),
            authz_silo,
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

    pub async fn project_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Project> {
        self.db_datastore.projects_list(opctx, pagparams).await
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
    ) -> LookupResult<shared::Policy<shared::ProjectRole>> {
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
        policy: &shared::Policy<shared::ProjectRole>,
    ) -> UpdateResult<shared::Policy<shared::ProjectRole>> {
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

    pub async fn project_ip_pools_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::IpPool> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;

        self.db_datastore
            .project_ip_pools_list(opctx, &authz_project, pagparams)
            .await
    }

    pub fn project_ip_pool_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
        _project_lookup: &Option<lookup::Project<'_>>,
    ) -> LookupResult<lookup::IpPool<'a>> {
        // TODO(2148, 2056): check that the given project has access (if one
        // is provided to the call) once that relation is implemented
        match pool {
            NameOrId::Name(name) => {
                let pool = LookupPath::new(opctx, &self.db_datastore)
                    .ip_pool_name(Name::ref_cast(name));
                Ok(pool)
            }
            NameOrId::Id(id) => {
                let pool =
                    LookupPath::new(opctx, &self.db_datastore).ip_pool_id(*id);
                Ok(pool)
            }
        }
    }
}
