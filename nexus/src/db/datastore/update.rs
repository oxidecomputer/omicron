// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{
    public_error_from_diesel_pool, ErrorHandler, TransactionError,
};
use crate::db::model::{
    ComponentUpdate, SemverVersion, SystemUpdate, UpdateAvailableArtifact,
    UpdateDeployment, UpdateableComponent,
};
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use diesel::prelude::*;
use nexus_db_model::SystemUpdateComponentUpdate;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::{
    CreateResult, DeleteResult, InternalContext, ListResultVec,
};
use uuid::Uuid;

impl DataStore {
    pub async fn update_available_artifact_upsert(
        &self,
        opctx: &OpContext,
        artifact: UpdateAvailableArtifact,
    ) -> CreateResult<UpdateAvailableArtifact> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use db::schema::update_available_artifact::dsl;
        diesel::insert_into(dsl::update_available_artifact)
            .values(artifact.clone())
            .on_conflict((dsl::name, dsl::version, dsl::kind))
            .do_update()
            .set(artifact.clone())
            .returning(UpdateAvailableArtifact::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn update_available_artifact_hard_delete_outdated(
        &self,
        opctx: &OpContext,
        current_targets_role_version: i64,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // We use the `targets_role_version` column in the table to delete any
        // old rows, keeping the table in sync with the current copy of
        // artifacts.json.
        use db::schema::update_available_artifact::dsl;
        diesel::delete(dsl::update_available_artifact)
            .filter(dsl::targets_role_version.lt(current_targets_role_version))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
            .internal_context("deleting outdated available artifacts")
    }

    pub async fn create_system_update(
        &self,
        opctx: &OpContext,
        update: SystemUpdate,
    ) -> CreateResult<SystemUpdate> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        diesel::insert_into(system_update)
            .values(update.clone())
            .on_conflict(version)
            .do_nothing()
            .returning(SystemUpdate::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SystemUpdate,
                        &update.version.to_string(),
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

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        let version_string = update.version.to_string();

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let db_update = diesel::insert_into(component_update::table)
                    .values(update.clone())
                    .on_conflict(component_update::columns::id) // TODO: should also conflict on version
                    .do_nothing()
                    .returning(ComponentUpdate::as_returning())
                    .get_result_async(&conn)
                    .await?;

                diesel::insert_into(join_table::table)
                    .values(SystemUpdateComponentUpdate {
                        system_update_id,
                        component_update_id: update.id(),
                    })
                    .on_conflict(join_table::all_columns)
                    .do_nothing()
                    .returning(SystemUpdateComponentUpdate::as_returning())
                    .get_result_async(&conn)
                    .await?;

                Ok(db_update)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => public_error_from_diesel_pool(
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
            .order(version_sort.desc())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn system_update_components_list(
        &self,
        opctx: &OpContext,
        authz_update: &authz::SystemUpdate,
    ) -> ListResultVec<ComponentUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        component_update::table
            .inner_join(join_table::table)
            .filter(join_table::columns::system_update_id.eq(authz_update.id()))
            .select(ComponentUpdate::as_select())
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn create_updateable_component(
        &self,
        opctx: &OpContext,
        component: UpdateableComponent,
    ) -> CreateResult<UpdateableComponent> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        diesel::insert_into(updateable_component)
            .values(component.clone())
            .on_conflict(id) // TODO: should probably conflict on (component_type, device_id)
            .do_nothing()
            .returning(UpdateableComponent::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn lowest_component_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(version)
            .order(version_sort.asc())
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn highest_component_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(version)
            .order(version_sort.desc())
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
