// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Project APIs

use crate::app::sagas;
use crate::external_api::params;
use crate::external_api::shared;
use anyhow::Context;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
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

    pub(crate) async fn project_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Project")?;
        opctx.authorize(authz::Action::CreateChild, &authz_silo).await?;

        // Determine if we should create a default VPC.
        // Skip VPC creation if networking is restricted and user is not a Silo Admin.
        let create_default_vpc = if let Some(policy) = opctx.authn.silo_authn_policy() {
            if policy.restrict_network_actions() {
                // Networking is restricted - only create VPC if user is Silo Admin
                // (i.e., has Modify permission on the Silo)
                opctx.authorize(authz::Action::Modify, &authz_silo).await.is_ok()
            } else {
                // No networking restrictions, create VPC
                true
            }
        } else {
            // No policy, create VPC
            true
        };

        let saga_params = sagas::project_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_create: new_project.clone(),
            authz_silo,
            create_default_vpc,
        };
        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::project_create::SagaProjectCreate>(
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

    pub(crate) async fn project_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Project> {
        self.db_datastore.projects_list(opctx, pagparams).await
    }

    pub(crate) async fn project_update(
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

    pub(crate) async fn project_delete(
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

    pub(crate) async fn project_fetch_policy(
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

    pub(crate) async fn project_update_policy(
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
}
