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
use crate::db::collection_detach_many::DetachManyFromCollectionStatement;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::Instance;
use crate::db::model::InstanceRuntimeState;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::Sled;
use crate::db::model::Vmm;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::ApplySledFilterExt;
use nexus_db_model::Disk;
use nexus_db_model::VmmRuntimeState;
use nexus_types::deployment::SledFilter;
use omicron_common::api;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use ref_cast::RefCast;
use uuid::Uuid;

/// Wraps a record of an `Instance` along with its active `Vmm`, if it has one.
#[derive(Clone, Debug)]
pub struct InstanceAndActiveVmm {
    instance: Instance,
    vmm: Option<Vmm>,
}

impl InstanceAndActiveVmm {
    pub fn instance(&self) -> &Instance {
        &self.instance
    }

    pub fn vmm(&self) -> &Option<Vmm> {
        &self.vmm
    }

    pub fn sled_id(&self) -> Option<Uuid> {
        self.vmm.as_ref().map(|v| v.sled_id)
    }

    pub fn effective_state(
        &self,
    ) -> omicron_common::api::external::InstanceState {
        if let Some(vmm) = &self.vmm {
            vmm.runtime.state.0
        } else {
            self.instance.runtime().nexus_state.0
        }
    }
}

impl From<(Instance, Option<Vmm>)> for InstanceAndActiveVmm {
    fn from(value: (Instance, Option<Vmm>)) -> Self {
        Self { instance: value.0, vmm: value.1 }
    }
}

impl From<InstanceAndActiveVmm> for omicron_common::api::external::Instance {
    fn from(value: InstanceAndActiveVmm) -> Self {
        let (run_state, time_run_state_updated) = if let Some(vmm) = value.vmm {
            (vmm.runtime.state, vmm.runtime.time_state_updated)
        } else {
            (
                value.instance.runtime_state.nexus_state.clone(),
                value.instance.runtime_state.time_updated,
            )
        };

        Self {
            identity: value.instance.identity(),
            project_id: value.instance.project_id,
            ncpus: value.instance.ncpus.into(),
            memory: value.instance.memory.into(),
            hostname: value
                .instance
                .hostname
                .parse()
                .expect("found invalid hostname in the database"),
            runtime: omicron_common::api::external::InstanceRuntimeState {
                run_state: *run_state.state(),
                time_run_state_updated,
            },
        }
    }
}

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
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(ResourceType::Instance, name.as_str()),
            ),
        })?;

        bail_unless!(
            instance.runtime().nexus_state.state()
                == &api::external::InstanceState::Creating,
            "newly-created Instance has unexpected state: {:?}",
            instance.runtime().nexus_state
        );
        bail_unless!(
            instance.runtime().gen == gen,
            "newly-created Instance has unexpected generation: {:?}",
            instance.runtime().gen
        );
        Ok(instance)
    }

    pub async fn instance_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InstanceAndActiveVmm> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::instance::dsl;
        use db::schema::vmm::dsl as vmm_dsl;
        Ok(match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::instance, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::instance,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::project_id.eq(authz_project.id()))
        .filter(dsl::time_deleted.is_null())
        .left_join(
            vmm_dsl::vmm.on(vmm_dsl::id
                .nullable()
                .eq(dsl::active_propolis_id)
                .and(vmm_dsl::time_deleted.is_null())),
        )
        .select((Instance::as_select(), Option::<Vmm>::as_select()))
        .load_async::<(Instance, Option<Vmm>)>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
        .into_iter()
        .map(|(instance, vmm)| InstanceAndActiveVmm { instance, vmm })
        .collect())
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

    pub async fn instance_fetch_with_vmm(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> LookupResult<InstanceAndActiveVmm> {
        opctx.authorize(authz::Action::Read, authz_instance).await?;

        use db::schema::instance::dsl as instance_dsl;
        use db::schema::vmm::dsl as vmm_dsl;

        let (instance, vmm) = instance_dsl::instance
            .filter(instance_dsl::id.eq(authz_instance.id()))
            .filter(instance_dsl::time_deleted.is_null())
            .left_join(
                vmm_dsl::vmm.on(vmm_dsl::id
                    .nullable()
                    .eq(instance_dsl::active_propolis_id)
                    .and(vmm_dsl::time_deleted.is_null())),
            )
            .select((Instance::as_select(), Option::<Vmm>::as_select()))
            .get_result_async::<(Instance, Option<Vmm>)>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(authz_instance.id()),
                    ),
                )
            })?;

        Ok(InstanceAndActiveVmm { instance, vmm })
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
            // Runtime state updates are allowed if either:
            // - the active Propolis ID will not change, the state generation
            //   increased, and the Propolis generation will not change, or
            // - the Propolis generation increased.
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Instance>(*instance_id)
            .execute_and_check(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(*instance_id),
                    ),
                )
            })?;

        Ok(updated)
    }

    /// Updates an instance record and a VMM record with a single database
    /// command.
    ///
    /// This is intended to be used to apply updates from sled agent that
    /// may change a VMM's runtime state (e.g. moving an instance from Running
    /// to Stopped) and its corresponding instance's state (e.g. changing the
    /// active Propolis ID to reflect a completed migration) in a single
    /// transaction. The caller is responsible for ensuring the instance and
    /// VMM states are consistent with each other before calling this routine.
    ///
    /// # Arguments
    ///
    /// - instance_id: The ID of the instance to update.
    /// - new_instance: The new instance runtime state to try to write.
    /// - vmm_id: The ID of the VMM to update.
    /// - new_vmm: The new VMM runtime state to try to write.
    ///
    /// # Return value
    ///
    /// - `Ok((instance_updated, vmm_updated))` if the query was issued
    ///   successfully. `instance_updated` and `vmm_updated` are each true if
    ///   the relevant item was updated and false otherwise. Note that an update
    ///   can fail because it was inapplicable (i.e. the database has state with
    ///   a newer generation already) or because the relevant record was not
    ///   found.
    /// - `Err` if another error occurred while accessing the database.
    pub async fn instance_and_vmm_update_runtime(
        &self,
        instance_id: &Uuid,
        new_instance: &InstanceRuntimeState,
        vmm_id: &Uuid,
        new_vmm: &VmmRuntimeState,
    ) -> Result<(bool, bool), Error> {
        let query = crate::db::queries::instance::InstanceAndVmmUpdate::new(
            *instance_id,
            new_instance.clone(),
            *vmm_id,
            new_vmm.clone(),
        );

        // The InstanceAndVmmUpdate query handles and indicates failure to find
        // either the instance or the VMM, so a query failure here indicates
        // some kind of internal error and not a failed lookup.
        let result = query
            .execute_and_check(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let instance_updated = match result.instance_status {
            Some(UpdateStatus::Updated) => true,
            Some(UpdateStatus::NotUpdatedButExists) => false,
            None => false,
        };

        let vmm_updated = match result.vmm_status {
            Some(UpdateStatus::Updated) => true,
            Some(UpdateStatus::NotUpdatedButExists) => false,
            None => false,
        };

        Ok((instance_updated, vmm_updated))
    }

    /// Lists all instances on in-service sleds with active Propolis VMM
    /// processes, returning the instance along with the VMM on which it's
    /// running, the sled on which the VMM is running, and the project that owns
    /// the instance.
    ///
    /// The query performed by this function is paginated by the sled's UUID.
    pub async fn instance_and_vmm_list_by_sled_agent(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<(Sled, Instance, Vmm, Project)> {
        use crate::db::schema::{
            instance::dsl as instance_dsl, project::dsl as project_dsl,
            sled::dsl as sled_dsl, vmm::dsl as vmm_dsl,
        };
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let result = paginated(sled_dsl::sled, sled_dsl::id, pagparams)
            .filter(sled_dsl::time_deleted.is_null())
            .sled_filter(SledFilter::InService)
            .inner_join(
                vmm_dsl::vmm
                    .on(vmm_dsl::sled_id
                        .eq(sled_dsl::id)
                        .and(vmm_dsl::time_deleted.is_null()))
                    .inner_join(
                        instance_dsl::instance
                            .on(instance_dsl::id
                                .eq(vmm_dsl::instance_id)
                                .and(instance_dsl::time_deleted.is_null()))
                            .inner_join(
                                project_dsl::project.on(project_dsl::id
                                    .eq(instance_dsl::project_id)
                                    .and(project_dsl::time_deleted.is_null())),
                            ),
                    ),
            )
            .sled_filter(SledFilter::InService)
            .select((
                Sled::as_select(),
                Instance::as_select(),
                Vmm::as_select(),
                Project::as_select(),
            ))
            .load_async::<(Sled, Instance, Vmm, Project)>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(result)
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
            [api::external::DiskState::Attached(authz_instance.id())];
        let ok_to_detach_disk_state_labels: Vec<_> =
            ok_to_detach_disk_states.iter().map(|s| s.label()).collect();

        let stmt: DetachManyFromCollectionStatement<Disk, _, _, _> =
            Instance::detach_resources(
                authz_instance.id(),
                instance::table.into_boxed().filter(
                    instance::dsl::state
                        .eq_any(ok_to_delete_instance_states)
                        .and(instance::dsl::active_propolis_id.is_null()),
                ),
                disk::table.into_boxed().filter(
                    disk::dsl::disk_state
                        .eq_any(ok_to_detach_disk_state_labels),
                ),
                diesel::update(instance::dsl::instance).set((
                    instance::dsl::state.eq(destroyed),
                    instance::dsl::time_deleted.eq(Utc::now()),
                )),
                diesel::update(disk::dsl::disk).set((
                    disk::dsl::disk_state.eq(detached_label),
                    disk::dsl::attach_instance_id.eq(Option::<Uuid>::None),
                    disk::dsl::slot.eq(Option::<i16>::None),
                )),
            );

        let _instance = stmt
            .detach_and_get_result_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| match e {
                DetachManyError::CollectionNotFound => Error::not_found_by_id(
                    ResourceType::Instance,
                    &authz_instance.id(),
                ),
                DetachManyError::NoUpdate { collection } => {
                    if collection.runtime_state.propolis_id.is_some() {
                        return Error::invalid_request(
                        "cannot delete instance: instance is running or has \
                                not yet fully stopped",
                    );
                    }
                    let instance_state =
                        collection.runtime_state.nexus_state.state();
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
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        self.instance_ssh_keys_delete(opctx, authz_instance.id()).await?;

        Ok(())
    }
}
