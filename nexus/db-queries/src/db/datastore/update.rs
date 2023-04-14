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
    ComponentUpdate, SemverVersion, SystemUpdate, UpdateArtifact,
    UpdateDeployment, UpdateStatus, UpdateableComponent,
};
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::{KnownArtifactKind, SystemUpdateComponentUpdate};
use nexus_types::identity::Asset;
use omicron_common::api::external::{
    CreateResult, DataPageParams, ListResultVec, LookupResult, LookupType,
    ResourceType, UpdateResult,
};
use omicron_common::api::internal::nexus::KnownArtifactKind as KnownArtifactKind_;
use uuid::Uuid;

impl DataStore {
    pub async fn upsert_update_artifacts(
        &self,
        opctx: &OpContext,
        artifacts: Vec<UpdateArtifact>,
        system_version: SemverVersion,
    ) -> CreateResult<Vec<UpdateArtifact>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        if artifacts.is_empty() {
            return Ok(vec![]);
        }

        let current_role_version = artifacts[0].targets_role_version;

        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::update_artifact as ua;

                let _ = self
                    .upsert_system_update(
                        opctx,
                        SystemUpdate::new(system_version.0).unwrap(),
                    )
                    .await;

                // we have to loop here for two reasons: 1) `do_update().set()`
                // with a vec requires naming the columns one by one, and 2)
                // lifetime issues with the artifacts Vec
                let mut created = vec![];
                for artifact in &artifacts {
                    let created_artifact = diesel::insert_into(ua::table)
                        .values(artifact.clone())
                        .on_conflict((ua::name, ua::version, ua::kind))
                        .do_update()
                        .set(artifact.clone())
                        .returning(UpdateArtifact::as_returning())
                        .get_result_async(&conn)
                        .await?;
                    created.push(created_artifact);
                }

                // We use the `targets_role_version` column in the table to delete any
                // old rows, keeping the table in sync with the current copy of
                // artifacts.json.
                let _ = diesel::delete(ua::table)
                    .filter(ua::targets_role_version.lt(current_role_version))
                    .execute_async(&conn)
                    .await;

                // TODO: Also delete associations to system versions for all deleted
                // artifacts. Probably also delete the system version if there are no
                // more artifacts associated with it. Rather than doing this in here
                // it might make more sense to orchestrate these related actions in
                // updates_refresh_metadata.

                Ok(created)
            })
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn update_artifact_list(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<UpdateArtifact> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::update_artifact::dsl::*;
        // TODO: paginate? by PK? but that's a tuple. could do name, but it's
        // not unique. not sure if that's a problem. Need a stable order, at
        // least.
        update_artifact
            .select(UpdateArtifact::as_select())
            // TODO: get rid of this fake filter, which is needed to make CRDB
            // not complain about table scans, and instead paginate and/or list
            // artifacts for a single system version. Those could probably be
            // two different methods.
            .filter(
                kind.eq(KnownArtifactKind(KnownArtifactKind_::ControlPlane)),
            )
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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

        self.pool_authorized(opctx)
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
            .order(version.desc())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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

        // make sure system version exists
        let sys_version = component.system_version.clone();
        self.system_update_fetch_by_version(opctx, sys_version).await?;

        use db::schema::updateable_component::dsl::*;

        diesel::insert_into(updateable_component)
            .values(component.clone())
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

    pub async fn lowest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(system_version)
            .order(system_version.asc())
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .first_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
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
