// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{
    public_error_from_diesel, ErrorHandler, TransactionError,
};
use crate::db::model::{
    ComponentUpdate, SemverVersion, SystemUpdate, UpdateArtifact,
    UpdateDeployment, UpdateStatus, UpdateableComponent,
};
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::SystemUpdateComponentUpdate;
use nexus_types::identity::Asset;
use omicron_common::api::external::{
    CreateResult, DataPageParams, DeleteResult, InternalContext, ListResultVec,
    LookupResult, LookupType, ResourceType, UpdateResult,
};
use uuid::Uuid;

impl DataStore {
    pub async fn update_artifact_upsert(
        &self,
        opctx: &OpContext,
        artifact: UpdateArtifact,
    ) -> CreateResult<UpdateArtifact> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use db::schema::update_artifact::dsl;
        diesel::insert_into(dsl::update_artifact)
            .values(artifact.clone())
            .on_conflict((dsl::name, dsl::version, dsl::kind))
            .do_update()
            .set(artifact.clone())
            .returning(UpdateArtifact::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn update_artifact_hard_delete_outdated(
        &self,
        opctx: &OpContext,
        current_targets_role_version: i64,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // We use the `targets_role_version` column in the table to delete any
        // old rows, keeping the table in sync with the current copy of
        // artifacts.json.
        use db::schema::update_artifact::dsl;
        diesel::delete(dsl::update_artifact)
            .filter(dsl::targets_role_version.lt(current_targets_role_version))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .internal_context("deleting outdated available artifacts")
    }

    pub async fn upsert_system_update(
        &self,
        opctx: &OpContext,
        update: SystemUpdate,
    ) -> CreateResult<SystemUpdate> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        diesel::insert_into(system_update)
            .values(update.clone())
            .on_conflict(version)
            .do_update()
            // for now the only modifiable field is time_modified, but we intend
            // to add more metadata to this model
            .set(time_modified.eq(Utc::now()))
            .returning(SystemUpdate::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SystemUpdate,
                        &update.version.to_string(),
                    ),
                )
            })
    }

    // version is unique but not the primary key, so we can't use LookupPath to handle this for us
    pub async fn system_update_fetch_by_version(
        &self,
        opctx: &OpContext,
        target: SemverVersion,
    ) -> LookupResult<SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        let version_string = target.to_string();

        system_update
            .filter(version.eq(target))
            .select(SystemUpdate::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SystemUpdate,
                        LookupType::ByCompositeId(version_string),
                    ),
                )
            })
    }

    pub async fn create_component_update(
        &self,
        opctx: &OpContext,
        system_update_id: Uuid,
        update: ComponentUpdate,
    ) -> CreateResult<ComponentUpdate> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        // TODO: make sure system update with that ID exists first
        // let (.., db_system_update) = LookupPath::new(opctx, &self)

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        let version_string = update.version.to_string();

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let db_update = diesel::insert_into(component_update::table)
                    .values(update.clone())
                    .returning(ComponentUpdate::as_returning())
                    .get_result_async(&conn)
                    .await?;

                diesel::insert_into(join_table::table)
                    .values(SystemUpdateComponentUpdate {
                        system_update_id,
                        component_update_id: update.id(),
                    })
                    .returning(SystemUpdateComponentUpdate::as_returning())
                    .get_result_async(&conn)
                    .await?;

                Ok(db_update)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ComponentUpdate,
                        &version_string,
                    ),
                ),
            })
    }

    pub async fn system_updates_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        paginated(system_update, id, pagparams)
            .select(SystemUpdate::as_select())
            .order(version.desc())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn system_update_components_list(
        &self,
        opctx: &OpContext,
        system_update_id: Uuid,
    ) -> ListResultVec<ComponentUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        component_update::table
            .inner_join(join_table::table)
            .filter(join_table::columns::system_update_id.eq(system_update_id))
            .select(ComponentUpdate::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn create_updateable_component(
        &self,
        opctx: &OpContext,
        component: UpdateableComponent,
    ) -> CreateResult<UpdateableComponent> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        // make sure system version exists
        let sys_version = component.system_version.clone();
        self.system_update_fetch_by_version(opctx, sys_version).await?;

        use db::schema::updateable_component::dsl::*;

        diesel::insert_into(updateable_component)
            .values(component.clone())
            .returning(UpdateableComponent::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::UpdateableComponent,
                        &component.id().to_string(), // TODO: more informative identifier
                    ),
                )
            })
    }

    pub async fn updateable_components_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UpdateableComponent> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        paginated(updateable_component, id, pagparams)
            .select(UpdateableComponent::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn lowest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(system_version)
            .order(system_version.asc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn highest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(system_version)
            .order(system_version.desc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn create_update_deployment(
        &self,
        opctx: &OpContext,
        deployment: UpdateDeployment,
    ) -> CreateResult<UpdateDeployment> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        diesel::insert_into(update_deployment)
            .values(deployment.clone())
            .returning(UpdateDeployment::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::UpdateDeployment,
                        &deployment.id().to_string(),
                    ),
                )
            })
    }

    pub async fn steady_update_deployment(
        &self,
        opctx: &OpContext,
        deployment_id: Uuid,
    ) -> UpdateResult<UpdateDeployment> {
        // TODO: use authz::UpdateDeployment as the input so we can check Modify
        // on that instead
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        diesel::update(update_deployment)
            .filter(id.eq(deployment_id))
            .set((
                status.eq(UpdateStatus::Steady),
                time_modified.eq(diesel::dsl::now),
            ))
            .returning(UpdateDeployment::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::UpdateDeployment,
                        LookupType::ById(deployment_id),
                    ),
                )
            })
    }

    pub async fn update_deployments_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UpdateDeployment> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        paginated(update_deployment, id, pagparams)
            .select(UpdateDeployment::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn latest_update_deployment(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<UpdateDeployment> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        update_deployment
            .select(UpdateDeployment::as_returning())
            .order(time_created.desc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
