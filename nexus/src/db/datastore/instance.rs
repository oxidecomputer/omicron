// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Instance`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_detach_many::DatastoreDetachManyTarget;
use crate::db::collection_detach_many::DetachManyError;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::Instance;
use crate::db::model::InstanceRuntimeState;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use uuid::Uuid;

impl DataStore {
    /// Idempotently insert a database record for an Instance
    ///
    /// This is intended to be used by a saga action.  When we say this is
    /// idempotent, we mean that if this function succeeds and the caller
    /// invokes it again with the same instance id, project id, creation
    /// parameters, and initial runtime, then this operation will succeed and
    /// return the current object in the database.  Because this is intended for
    /// use by sagas, we do assume that if the record exists, it should still be
    /// in the "Creating" state.  If it's in any other state, this function will
    /// return with an error on the assumption that we don't really know what's
    /// happened or how to proceed.
    ///
    /// ## Errors
    ///
    /// In addition to the usual database errors (e.g., no connections
    /// available), this function can fail if there is already a different
    /// instance (having a different id) with the same name in the same project.
    // TODO-design Given that this is really oriented towards the saga
    // interface, one wonders if it's even worth having an abstraction here, or
    // if sagas shouldn't directly work with the database here (i.e., just do
    // what this function does under the hood).
    pub async fn project_create_instance(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        instance: Instance,
    ) -> CreateResult<Instance> {
        use db::schema::instance::dsl;

        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let gen = instance.runtime().gen;
        let name = instance.name().clone();
        let project_id = instance.project_id;

        let instance: Instance = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::instance)
                .values(instance)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Instance,
                        name.as_str(),
                    ),
                )
            }
        })?;

        bail_unless!(
            instance.runtime().state.state()
                == &api::external::InstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.runtime().state
        );
        bail_unless!(
            instance.runtime().gen == gen,
            "newly-created Instance has unexpected generation: {:?}",
            instance.runtime().gen
        );
        Ok(instance)
    }

    pub async fn project_list_instances(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Instance> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::instance::dsl;
        paginated(dsl::instance, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::project_id.eq(authz_project.id()))
            .select(Instance::as_select())
            .load_async::<Instance>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Fetches information about an Instance that the caller has previously
    /// fetched
    ///
    /// See disk_refetch().
    pub async fn instance_refetch(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> LookupResult<Instance> {
        let (.., db_instance) = LookupPath::new(opctx, self)
            .instance_id(authz_instance.id())
            .fetch()
            .await
            .map_err(|e| match e {
                // Use the "not found" message of the authz object we were
                // given, which will reflect however the caller originally
                // looked it up.
                Error::ObjectNotFound { .. } => authz_instance.not_found(),
                e => e,
            })?;
        Ok(db_instance)
    }

    // TODO-design It's tempting to return the updated state of the Instance
    // here because it's convenient for consumers and by using a RETURNING
    // clause, we could ensure that the "update" and "fetch" are atomic.
    // But in the unusual case that we _don't_ update the row because our
    // update is older than the one in the database, we would have to fetch
    // the current state explicitly.  For now, we'll just require consumers
    // to explicitly fetch the state if they want that.
    pub async fn instance_update_runtime(
        &self,
        instance_id: &Uuid,
        new_runtime: &InstanceRuntimeState,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*instance_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .filter(
                dsl::migration_id
                    .is_null()
                    .or(dsl::target_propolis_id.eq(new_runtime.propolis_id)),
            )
            .set(new_runtime.clone())
            .check_if_exists::<Instance>(*instance_id)
            .execute_and_check(self.pool())
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(*instance_id),
                    ),
                )
            })?;

        Ok(updated)
    }

    pub async fn project_delete_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_instance).await?;

        // This is subject to change, but for now we're going to say that an
        // instance must be "stopped" or "failed" in order to delete it.  The
        // delete operation sets "time_deleted" (just like with other objects)
        // and also sets the state to "destroyed".
        use api::external::InstanceState as ApiInstanceState;
        use db::model::InstanceState as DbInstanceState;
        use db::schema::{disk, instance};

        let stopped = DbInstanceState::new(ApiInstanceState::Stopped);
        let failed = DbInstanceState::new(ApiInstanceState::Failed);
        let destroyed = DbInstanceState::new(ApiInstanceState::Destroyed);
        let ok_to_delete_instance_states = vec![stopped, failed];

        let detached_label = api::external::DiskState::Detached.label();
        let ok_to_detach_disk_states =
            vec![api::external::DiskState::Attached(authz_instance.id())];
        let ok_to_detach_disk_state_labels: Vec<_> =
            ok_to_detach_disk_states.iter().map(|s| s.label()).collect();

        let _instance = Instance::detach_resources(
            authz_instance.id(),
            instance::table.into_boxed().filter(
                instance::dsl::state.eq_any(ok_to_delete_instance_states),
            ),
            disk::table.into_boxed().filter(
                disk::dsl::disk_state.eq_any(ok_to_detach_disk_state_labels),
            ),
            diesel::update(instance::dsl::instance).set((
                instance::dsl::state.eq(destroyed),
                instance::dsl::time_deleted.eq(Utc::now()),
            )),
            diesel::update(disk::dsl::disk).set((
                disk::dsl::disk_state.eq(detached_label),
                disk::dsl::attach_instance_id.eq(Option::<Uuid>::None),
            )),
        )
        .detach_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            DetachManyError::CollectionNotFound => Error::not_found_by_id(
                ResourceType::Instance,
                &authz_instance.id(),
            ),
            DetachManyError::NoUpdate { collection } => {
                let instance_state = collection.runtime_state.state.state();
                match instance_state {
                    api::external::InstanceState::Stopped
                    | api::external::InstanceState::Failed => {
                        Error::internal_error("cannot delete instance")
                    }
                    _ => Error::invalid_request(&format!(
                        "instance cannot be deleted in state \"{}\"",
                        instance_state,
                    )),
                }
            }
            DetachManyError::DatabaseError(e) => {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }
        })?;

        Ok(())
    }
}
