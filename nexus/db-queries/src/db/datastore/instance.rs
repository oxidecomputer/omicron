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
use crate::db::model::Generation;
use crate::db::model::Instance;
use crate::db::model::InstanceRuntimeState;
use crate::db::model::Migration;
use crate::db::model::MigrationState;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::Sled;
use crate::db::model::Vmm;
use crate::db::model::VmmState;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::ApplySledFilterExt;
use nexus_db_model::Disk;
use nexus_types::deployment::SledFilter;
use omicron_common::api;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::MessagePair;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use ref_cast::RefCast;
use uuid::Uuid;

/// Wraps a record of an `Instance` along with its active `Vmm`, if it has one.
#[derive(Clone, Debug)]
pub struct InstanceAndActiveVmm {
    pub instance: Instance,
    pub vmm: Option<Vmm>,
}

impl InstanceAndActiveVmm {
    pub fn instance(&self) -> &Instance {
        &self.instance
    }

    pub fn vmm(&self) -> &Option<Vmm> {
        &self.vmm
    }

    pub fn sled_id(&self) -> Option<SledUuid> {
        self.vmm.as_ref().map(|v| SledUuid::from_untyped_uuid(v.sled_id))
    }

    /// Returns the operator-visible [external API
    /// `InstanceState`](external::InstanceState) for this instance and its
    /// active VMM.
    pub fn effective_state(&self) -> external::InstanceState {
        Self::determine_effective_state(&self.instance, self.vmm.as_ref())
    }

    /// Returns the operator-visible [external API
    /// `InstanceState`](external::InstanceState) for the provided [`Instance`]
    /// and its active [`Vmm`], if one exists.
    ///
    /// # Arguments
    ///
    /// - `instance`: the instance
    /// - `active_vmm`: the instance's active VMM, if one exists.
    ///
    /// # Notes
    ///
    /// Generally, the value of `active_vmm` should be
    /// the VMM pointed to by `instance.runtime_state.propolis_id`. However,
    /// this is not enforced by this function, as the `instance_migrate` saga
    /// must in some cases determine an effective instance state from the
    /// instance and *target* VMM states.
    pub fn determine_effective_state(
        instance: &Instance,
        active_vmm: Option<&Vmm>,
    ) -> external::InstanceState {
        use crate::db::model::InstanceState;
        use crate::db::model::VmmState;

        let instance_state = instance.runtime_state.nexus_state;
        let vmm_state = active_vmm.map(|vmm| vmm.runtime.state);

        // We want to only report that an instance is `Stopped` when a new
        // `instance-start` saga is able to proceed. That means that:
        match (instance_state, vmm_state) {
            // - If there's an active migration ID for the instance, *always*
            //   treat its state as "migration" regardless of the VMM's state.
            //
            //   This avoids an issue where an instance whose previous active
            //   VMM has been destroyed as a result of a successful migration
            //   out will appear to be "stopping" for the time between when that
            //   VMM was reported destroyed and when the instance record was
            //   updated to reflect the migration's completion.
            //
            //   Instead, we'll continue to report the instance's state as
            //   "migrating" until an instance-update saga has resolved the
            //   outcome of the migration, since only the instance-update saga
            //   can complete the migration and update the instance record to
            //   point at its new active VMM. No new instance-migrate,
            //   instance-stop, or instance-delete saga can be started
            //   until this occurs.
            //
            //   If the instance actually *has* stopped or failed before a
            //   successful migration out, this is fine, because an
            //   instance-update saga will come along and remove the active VMM
            //   and migration IDs.
            //
            (InstanceState::Vmm, Some(_))
                if instance.runtime_state.migration_id.is_some() =>
            {
                external::InstanceState::Migrating
            }
            // - An instance with a "stopped" or "destroyed" VMM needs to be
            //   recast as a "stopping" instance, as the virtual provisioning
            //   resources for that instance have not been deallocated until the
            //   active VMM ID has been unlinked by an update saga.
            (
                InstanceState::Vmm,
                Some(VmmState::Stopped | VmmState::Destroyed),
            ) => external::InstanceState::Stopping,
            // - An instance with a "saga unwound" VMM, on the other hand, can
            //   be treated as "stopped", since --- unlike "destroyed" --- a new
            //   start saga can run at any time by just clearing out the old VMM
            //   ID.
            (InstanceState::Vmm, Some(VmmState::SagaUnwound)) => {
                external::InstanceState::Stopped
            }
            // - An instance with no VMM is always "stopped" (as long as it's
            //   not "starting" etc.)
            (InstanceState::NoVmm, _vmm_state) => {
                debug_assert_eq!(_vmm_state, None);
                external::InstanceState::Stopped
            }
            // If there's a VMM state, and none of the above rules apply, use
            // that.
            (_instance_state, Some(vmm_state)) => {
                debug_assert_eq!(_instance_state, InstanceState::Vmm);
                vmm_state.into()
            }
            // If there's no VMM state, use the instance's state.
            (instance_state, None) => instance_state.into(),
        }
    }
}

impl From<(Instance, Option<Vmm>)> for InstanceAndActiveVmm {
    fn from(value: (Instance, Option<Vmm>)) -> Self {
        Self { instance: value.0, vmm: value.1 }
    }
}

impl From<InstanceAndActiveVmm> for external::Instance {
    fn from(value: InstanceAndActiveVmm) -> Self {
        let time_run_state_updated = value
            .vmm
            .as_ref()
            .map(|vmm| vmm.runtime.time_state_updated)
            .unwrap_or(value.instance.runtime_state.time_updated);

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
            runtime: external::InstanceRuntimeState {
                run_state: value.effective_state(),
                time_run_state_updated,
            },
        }
    }
}

/// The totality of database records describing the current state of
/// an instance: the [`Instance`] record itself, along with its active [`Vmm`],
/// target [`Vmm`], and current [`Migration`], if they exist.
///
/// This is returned by [`DataStore::instance_fetch_all`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct InstanceGestalt {
    /// The instance record.
    pub instance: Instance,
    /// The [`Vmm`] record pointed to by the instance's `active_propolis_id`, if
    /// it is set.
    pub active_vmm: Option<Vmm>,
    /// The [`Vmm`] record pointed to by the instance's `target_propolis_id`, if
    /// it is set.
    pub target_vmm: Option<Vmm>,
    /// The [`Migration`] record pointed to by the instance's `migration_id`, if
    /// it is set.
    pub migration: Option<Migration>,
}

/// A token which represents that a saga holds the instance-updater lock on a
/// particular instance.
///
/// This is returned by [`DataStore::instance_updater_lock`] if the lock is
/// successfully acquired, and passed to [`DataStore::instance_updater_unlock`]
/// when the lock is released.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UpdaterLock {
    pub updater_id: Uuid,
    locked_gen: Generation,
}

/// Errors returned by [`DataStore::instance_updater_lock`].
#[derive(
    Debug, thiserror::Error, PartialEq, serde::Serialize, serde::Deserialize,
)]
pub enum UpdaterLockError {
    /// The instance was already locked by another saga.
    #[error("instance already locked by another saga")]
    AlreadyLocked,
    /// An error occurred executing the query.
    #[error("error locking instance: {0}")]
    Query(#[from] Error),
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
            instance.runtime().nexus_state
                == nexus_db_model::InstanceState::Creating,
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

    /// List all instances with active VMMs in the `Destroyed` state that don't
    /// have currently-running instance-updater sagas.
    ///
    /// This is used by the `instance_updater` background task to ensure that
    /// update sagas are scheduled for these instances.
    pub async fn find_instances_with_destroyed_active_vmms(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<Instance> {
        use db::model::VmmState;
        use db::schema::instance::dsl;
        use db::schema::vmm::dsl as vmm_dsl;

        vmm_dsl::vmm
            .filter(vmm_dsl::state.eq(VmmState::Destroyed))
            // If the VMM record has already been deleted, we don't need to do
            // anything about it --- someone already has.
            .filter(vmm_dsl::time_deleted.is_null())
            .inner_join(
                dsl::instance.on(dsl::active_propolis_id
                    .eq(vmm_dsl::id.nullable())
                    .and(dsl::time_deleted.is_null())
                    .and(dsl::updater_id.is_null())),
            )
            .select(Instance::as_select())
            .load_async::<Instance>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all instances with active migrations that have terminated (either
    /// completed or failed) and don't have currently-running instance-updater
    /// sagas.
    ///
    /// This is used by the `instance_updater` background task to ensure that
    /// update sagas are scheduled for these instances.
    pub async fn find_instances_with_terminated_active_migrations(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<Instance> {
        use db::model::MigrationState;
        use db::schema::instance::dsl;
        use db::schema::migration::dsl as migration_dsl;

        dsl::instance
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::migration_id.is_not_null())
            .filter(dsl::updater_id.is_null())
            .inner_join(
                migration_dsl::migration.on(dsl::migration_id
                    .eq(migration_dsl::id.nullable())
                    .and(
                        migration_dsl::target_state
                            .eq_any(MigrationState::TERMINAL_STATES)
                            .or(migration_dsl::source_state
                                .eq_any(MigrationState::TERMINAL_STATES)),
                    )),
            )
            .select(Instance::as_select())
            .load_async::<Instance>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

    /// Fetches all database records describing the state of the provided
    /// instance in a single atomic query.
    ///
    /// If an instance with the provided UUID exists, this method returns an
    /// [`InstanceGestalt`], which contains the following:
    ///
    /// - The [`Instance`] record itself,
    /// - The instance's active [`Vmm`] record, if the `active_propolis_id`
    ///   column is not null,
    /// - The instance's target [`Vmm`] record, if the `target_propolis_id`
    ///   column is not null,
    /// - The instance's current active [`Migration`], if the `migration_id`
    ///   column is not null.
    pub async fn instance_fetch_all(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> LookupResult<InstanceGestalt> {
        opctx.authorize(authz::Action::Read, authz_instance).await?;

        use db::schema::instance::dsl as instance_dsl;
        use db::schema::migration::dsl as migration_dsl;
        use db::schema::vmm;

        // Create a Diesel alias to allow us to LEFT JOIN the `instance` table
        // with the `vmm` table twice; once on the `active_propolis_id` and once
        // on the `target_propolis_id`.
        let (active_vmm, target_vmm) =
            diesel::alias!(vmm as active_vmm, vmm as target_vmm);
        let vmm_selection =
            <Vmm as Selectable<diesel::pg::Pg>>::construct_selection();

        let query = instance_dsl::instance
            .filter(instance_dsl::id.eq(authz_instance.id()))
            .filter(instance_dsl::time_deleted.is_null())
            .left_join(
                active_vmm.on(active_vmm
                    .field(vmm::id)
                    .nullable()
                    .eq(instance_dsl::active_propolis_id)
                    .and(active_vmm.field(vmm::time_deleted).is_null())),
            )
            .left_join(
                target_vmm.on(target_vmm
                    .field(vmm::id)
                    .nullable()
                    .eq(instance_dsl::target_propolis_id)
                    .and(target_vmm.field(vmm::time_deleted).is_null())),
            )
            .left_join(
                migration_dsl::migration.on(migration_dsl::id
                    .nullable()
                    .eq(instance_dsl::migration_id)
                    .and(migration_dsl::time_deleted.is_null())),
            )
            .select((
                Instance::as_select(),
                active_vmm.fields(vmm_selection).nullable(),
                target_vmm.fields(vmm_selection).nullable(),
                Option::<Migration>::as_select(),
            ));

        let (instance, active_vmm, target_vmm, migration) =
            query
                .first_async::<(
                    Instance,
                    Option<Vmm>,
                    Option<Vmm>,
                    Option<Migration>,
                )>(
                    &*self.pool_connection_authorized(opctx).await?
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

        Ok(InstanceGestalt { instance, migration, active_vmm, target_vmm })
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
        instance_id: &InstanceUuid,
        new_runtime: &InstanceRuntimeState,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id.into_untyped_uuid()))
            // Runtime state updates are allowed if either:
            // - the active Propolis ID will not change, the state generation
            //   increased, and the Propolis generation will not change, or
            // - the Propolis generation increased.
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Instance>(instance_id.into_untyped_uuid())
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
                        LookupType::ById(instance_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(updated)
    }

    /// Updates an instance record by setting the instance's migration ID to the
    /// provided `migration_id` and the target VMM ID to the provided
    /// `target_propolis_id`, if the instance does not currently have an active
    /// migration, and the active VMM is in the [`VmmState::Running`] or
    /// [`VmmState::Rebooting`] states.
    ///
    /// Note that a non-NULL `target_propolis_id` will be overwritten, if (and
    /// only if) the target VMM record is in [`VmmState::SagaUnwound`],
    /// indicating that it was left behind by a failed `instance-migrate` saga
    /// unwinding.
    pub async fn instance_set_migration_ids(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        src_propolis_id: PropolisUuid,
        migration_id: Uuid,
        target_propolis_id: PropolisUuid,
    ) -> Result<Instance, Error> {
        use db::schema::instance::dsl;
        use db::schema::migration::dsl as migration_dsl;
        use db::schema::vmm::dsl as vmm_dsl;

        // Only allow migrating out if the active VMM is running or rebooting.
        const ALLOWED_ACTIVE_VMM_STATES: &[VmmState] =
            &[VmmState::Running, VmmState::Rebooting];

        let instance_id = instance_id.into_untyped_uuid();
        let target_propolis_id = target_propolis_id.into_untyped_uuid();
        let src_propolis_id = src_propolis_id.into_untyped_uuid();

        // Subquery for determining whether the active VMM is in a state where
        // it can be migrated out of. This returns the VMM row's instance ID, so
        // that we can use it in a `filter` on the update query.
        let vmm_ok = vmm_dsl::vmm
            .filter(vmm_dsl::id.eq(src_propolis_id))
            .filter(vmm_dsl::time_deleted.is_null())
            .filter(vmm_dsl::state.eq_any(ALLOWED_ACTIVE_VMM_STATES))
            .select(vmm_dsl::instance_id);
        // Subquery for checking if a present target VMM ID points at a VMM
        // that's in the saga-unwound state (in which it would be okay to clear
        // out that VMM).
        let target_vmm_unwound = vmm_dsl::vmm
            .filter(vmm_dsl::id.nullable().eq(dsl::target_propolis_id))
            // Don't filter out target VMMs with `time_deleted` set here --- we
            // *shouldn't* have deleted the VMM without unlinking it from the
            // instance record, but if something did, we should still allow the
            // ID to be clobbered.
            .filter(vmm_dsl::state.eq(VmmState::SagaUnwound))
            .select(vmm_dsl::instance_id);
        // Subquery for checking if an already present migration ID points at a
        // migration where both the source- and target-sides are marked as
        // failed. If both are failed, *and* the target VMM is `SagaUnwound` as
        // determined by the query above, then it's okay to clobber that
        // migration, as it was left behind by a previous migrate saga unwinding.
        let current_migration_failed = migration_dsl::migration
            .filter(migration_dsl::id.nullable().eq(dsl::migration_id))
            .filter(migration_dsl::target_state.eq(MigrationState::FAILED))
            .filter(migration_dsl::source_state.eq(MigrationState::FAILED))
            .select(migration_dsl::instance_id);

        diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(
                // Update the row if and only if one of the following is true:
                //
                // - The migration and target VMM IDs are not present
                (dsl::migration_id
                    .is_null()
                    .and(dsl::target_propolis_id.is_null()))
                // - The migration and target VMM IDs are set to the values
                //   we are trying to set.
                //
                //   This way, we can use a `RETURNING` clause to fetch the
                //   current state after the update, rather than
                //   `check_if_exists` which returns the prior state, and still
                //   fail to update the record if another migration/target VMM
                //   ID is already there.
                .or(dsl::migration_id
                    .eq(Some(migration_id))
                    .and(dsl::target_propolis_id.eq(Some(target_propolis_id))))
                // - The migration and target VMM IDs are set to another
                //   migration, but the target VMM state is `SagaUnwound` and
                //   the migration is `Failed` on both sides.
                //
                //   This would indicate that the migration/VMM IDs are left
                //   behind by another migrate saga failing, and are okay to get
                //   rid of.
                .or(
                    // Note that both of these queries return the instance ID
                    // from the VMM and migration records, so we check if one was
                    // found  by comparing it to the actual instance ID.
                    dsl::id
                        .eq_any(target_vmm_unwound)
                        .and(dsl::id.eq_any(current_migration_failed)),
                ),
            )
            .filter(dsl::active_propolis_id.eq(src_propolis_id))
            .filter(dsl::id.eq_any(vmm_ok))
            .set((
                dsl::migration_id.eq(Some(migration_id)),
                dsl::target_propolis_id.eq(Some(target_propolis_id)),
                // advance the generation
                dsl::state_generation.eq(dsl::state_generation + 1),
                dsl::time_state_updated.eq(Utc::now()),
            ))
            .returning(Instance::as_returning())
            .get_result_async::<Instance>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|error| Error::Conflict {
                message: MessagePair::new_full(
                    "another migration is already in progress".to_string(),
                    format!(
                        "cannot set migration ID {migration_id} for instance \
                         {instance_id} (perhaps another migration ID is \
                         already present): {error:#}"
                    ),
                ),
            })
    }

    /// Unsets the migration IDs set by
    /// [`DataStore::instance_set_migration_ids`].
    ///
    /// This method will only unset the instance's migration IDs if they match
    /// the provided ones.
    /// # Returns
    ///
    /// - `Ok(true)` if the migration IDs were unset,
    /// - `Ok(false)` if the instance IDs have *already* been unset (this method
    ///   is idempotent)
    /// - `Err` if the database query returned an error.
    pub async fn instance_unset_migration_ids(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        migration_id: Uuid,
        target_propolis_id: PropolisUuid,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let instance_id = instance_id.into_untyped_uuid();
        let target_propolis_id = target_propolis_id.into_untyped_uuid();
        let updated = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::migration_id.eq(migration_id))
            .filter(dsl::target_propolis_id.eq(target_propolis_id))
            .set((
                dsl::migration_id.eq(None::<Uuid>),
                dsl::target_propolis_id.eq(None::<Uuid>),
                // advance the generation
                dsl::state_generation.eq(dsl::state_generation + 1),
                dsl::time_state_updated.eq(Utc::now()),
            ))
            .check_if_exists::<Instance>(instance_id.into_untyped_uuid())
            .execute_and_check(&*self.pool_connection_authorized(&opctx).await?)
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
                        LookupType::ById(instance_id),
                    ),
                )
            })?;
        Ok(updated)
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
        use db::model::InstanceState as DbInstanceState;
        use db::schema::{disk, instance};

        let stopped = DbInstanceState::NoVmm;
        let failed = DbInstanceState::Failed;
        let destroyed = DbInstanceState::Destroyed;
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

        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());
        self.instance_ssh_keys_delete(opctx, instance_id).await?;
        self.instance_mark_migrations_deleted(opctx, instance_id).await?;

        Ok(())
    }

    /// Attempts to lock an instance's record to apply state updates in an
    /// instance-update saga, returning an [`UpdaterLock`] if the lock is
    /// successfully acquired.
    ///
    /// # Notes
    ///
    /// This method MUST only be called from the context of a saga! The
    /// calling saga must ensure that the reverse action for the action that
    /// acquires the lock must call [`DataStore::instance_updater_unlock`] to
    /// ensure that the lock is always released if the saga unwinds. If the saga
    /// locking the instance completes successfully, it must release the lock
    /// using [`DataStore::instance_updater_unlock`], or use
    /// [`DataStore::instance_commit_update`] to release the lock and write back
    /// a new [`InstanceRuntimeState`] in a single atomic query.
    ///
    /// This method is idempotent: if the instance is already locked by the same
    /// saga, it will succeed, as though the lock was acquired.
    ///
    /// # Arguments
    ///
    /// - `opctx`: the [`OpContext`] for this operation.
    /// - `authz_instance`: the instance to attempt to lock.
    /// - `updater_id`: the UUID of the saga that's attempting to lock this
    ///   instance.
    ///
    /// # Returns
    ///
    /// - [`Ok`]`(`[`UpdaterLock`]`)` if the lock was acquired.
    /// - [`Err`]`([`UpdaterLockError::AlreadyLocked`])` if the instance was
    ///   locked by another saga.
    /// - [`Err`]`([`UpdaterLockError::Query`]`(...))` if the query to fetch
    ///   the instance or lock it returned another error (such as if the
    ///   instance no longer exists, or if the database connection failed).
    pub async fn instance_updater_lock(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        updater_id: Uuid,
    ) -> Result<UpdaterLock, UpdaterLockError> {
        use db::schema::instance::dsl;

        let mut instance = self.instance_refetch(opctx, authz_instance).await?;
        let instance_id = instance.id();
        // `true` if the instance was locked by *this* call to
        // `instance_updater_lock`, *false* in the (rare) case that it was
        // previously locked by *this* saga's ID. This is used only for logging,
        // as this method is idempotent --- if the instance's current updater ID
        // matches the provided saga ID, this method completes successfully.
        //
        // XXX(eliza): I *think* this is the right behavior for sagas, since
        // saga actions are expected to be idempotent...but it also means that a
        // UUID collision would allow two sagas to lock the instance. But...(1)
        // a UUID collision is extremely unlikely, and (2), if a UUID collision
        // *did* occur, the odds are even lower that the same UUID would
        // assigned to two instance-update sagas which both try to update the
        // *same* instance at the same time. So, idempotency is probably more
        // important than handling that extremely unlikely edge case.
        let mut did_lock = false;
        let mut locked_gen = instance.updater_gen;
        loop {
            match instance.updater_id {
                // If the `updater_id` field is not null and the ID equals this
                // saga's ID, we already have the lock. We're done here!
                Some(lock_id) if lock_id == updater_id => {
                    slog::debug!(
                        &opctx.log,
                        "instance updater lock acquired!";
                        "instance_id" => %instance_id,
                        "updater_id" => %updater_id,
                        "locked_gen" => ?locked_gen,
                        "already_locked" => !did_lock,
                    );
                    return Ok(UpdaterLock { updater_id, locked_gen });
                }
                // The `updater_id` field is set, but it's not our ID. The instance
                // is locked by a different saga, so give up.
                Some(lock_id) => {
                    slog::info!(
                        &opctx.log,
                        "instance is locked by another saga";
                        "instance_id" => %instance_id,
                        "locked_by" => %lock_id,
                        "updater_id" => %updater_id,
                    );
                    return Err(UpdaterLockError::AlreadyLocked);
                }
                // No saga's ID is set as the instance's `updater_id`. We can
                // attempt to lock it.
                None => {}
            }

            // Okay, now attempt to acquire the lock
            let current_gen = instance.updater_gen;
            locked_gen = Generation(current_gen.0.next());
            slog::debug!(
                &opctx.log,
                "attempting to acquire instance updater lock";
                "instance_id" => %instance_id,
                "updater_id" => %updater_id,
                "current_gen" => ?current_gen,
            );

            (instance, did_lock) = diesel::update(dsl::instance)
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::id.eq(instance_id))
                // If the generation is the same as the captured generation when we
                // read the instance record to check if it was not locked, we can
                // lock this instance. This is because changing the `updater_id`
                // field always increments the generation number. Therefore, we
                // want the update query to succeed if and only if the
                // generation number remains the same as the generation when we
                // last fetched the instance. This query is used equivalently to
                // an atomic compare-and-swap instruction in the implementation
                // of a non-distributed, single-process mutex.
                .filter(dsl::updater_gen.eq(current_gen))
                .set((
                    dsl::updater_gen.eq(locked_gen),
                    dsl::updater_id.eq(Some(updater_id)),
                ))
                .check_if_exists::<Instance>(instance_id)
                .execute_and_check(
                    &*self.pool_connection_authorized(opctx).await?,
                )
                .await
                .map(|r| {
                    // If we successfully updated the instance record, we have
                    // acquired the lock; otherwise, we haven't --- either because
                    // our generation is stale, or because the instance is already locked.
                    let locked = match r.status {
                        UpdateStatus::Updated => true,
                        UpdateStatus::NotUpdatedButExists => false,
                    };
                    (r.found, locked)
                })
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Instance,
                            LookupType::ById(instance_id),
                        ),
                    )
                })?;
        }
    }

    /// Attempts to "inherit" the lock acquired by
    /// [`DataStore::instance_updater_lock`] by setting a new `child_lock_id` as
    /// the current updater, if (and only if) the lock is held by the provided
    /// `parent_lock`.
    ///
    /// This essentially performs the equivalent of a [compare-exchange]
    /// operation on the instance record's lock ID field, which succeeds if the
    /// current lock ID matches the parent. Using this method ensures that, if a
    /// parent saga starts multiple child sagas, only one of them can
    /// successfully acquire the lock.
    ///
    /// # Notes
    ///
    /// This method MUST only be called from the context of a saga! The
    /// calling saga must ensure that the reverse action for the action that
    /// acquires the lock must call [`DataStore::instance_updater_unlock`] to
    /// ensure that the lock is always released if the saga unwinds. If the saga
    /// locking the instance completes successfully, it must release the lock
    /// using [`DataStore::instance_updater_unlock`], or use
    /// [`DataStore::instance_commit_update`] to release the lock and write back
    /// a new [`InstanceRuntimeState`] in a single atomic query.

    ///
    /// This method is idempotent: if the instance is already locked by the same
    /// saga, it will succeed, as though the lock was acquired.
    ///
    /// # Arguments
    ///
    /// - `opctx`: the [`OpContext`] for this operation.
    /// - `authz_instance`: the instance to attempt to inherit the lock on.
    /// - `parent_lock`: the [`UpdaterLock`] to attempt to inherit the lock
    ///   from. If the current updater UUID and generation matches this, the
    ///   lock can be inherited by `child_id`.
    /// - `child_lock_id`: the UUID of the saga that's attempting to lock this
    ///   instance.
    ///
    /// # Returns
    ///
    /// - [`Ok`]`(`[`UpdaterLock`]`)` if the lock was successfully inherited.
    /// - [`Err`]`([`UpdaterLockError::AlreadyLocked`])` if the instance was
    ///   locked by a different saga, other than the provided `parent_lock`.
    /// - [`Err`]`([`UpdaterLockError::Query`]`(...))` if the query to fetch
    ///   the instance or lock it returned another error (such as if the
    ///   instance no longer exists, or if the database connection failed).
    pub async fn instance_updater_inherit_lock(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        parent_lock: UpdaterLock,
        child_lock_id: Uuid,
    ) -> Result<UpdaterLock, UpdaterLockError> {
        use db::schema::instance::dsl;
        let UpdaterLock { updater_id: parent_id, locked_gen } = parent_lock;
        let instance_id = authz_instance.id();
        let new_gen = Generation(locked_gen.0.next());

        let result = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::updater_gen.eq(locked_gen))
            .filter(dsl::updater_id.eq(parent_id))
            .set((
                dsl::updater_gen.eq(new_gen),
                dsl::updater_id.eq(Some(child_lock_id)),
            ))
            .check_if_exists::<Instance>(instance_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(instance_id),
                    ),
                )
            })?;

        match result {
            // If we updated the record, the lock has been successfully
            // inherited! Return `Ok(true)` to indicate that we have acquired
            // the lock successfully.
            UpdateAndQueryResult { status: UpdateStatus::Updated, .. } => {
                slog::debug!(
                    &opctx.log,
                    "inherited lock from {parent_id} to {child_lock_id}";
                    "instance_id" => %instance_id,
                    "updater_id" => %child_lock_id,
                    "locked_gen" => ?new_gen,
                    "parent_id" => %parent_id,
                    "parent_gen" => ?locked_gen,
                );
                Ok(UpdaterLock {
                    updater_id: child_lock_id,
                    locked_gen: new_gen,
                })
            }
            // The generation has advanced past the generation at which the
            // lock was held. This means that we have already inherited the
            // lock. Return `Ok(false)` here for idempotency.
            UpdateAndQueryResult {
                status: UpdateStatus::NotUpdatedButExists,
                ref found,
            } if found.updater_id == Some(child_lock_id) => {
                slog::debug!(
                    &opctx.log,
                    "previously inherited lock from {parent_id} to \
                     {child_lock_id}";
                    "instance_id" => %instance_id,
                    "updater_id" => %child_lock_id,
                    "locked_gen" => ?found.updater_gen,
                    "parent_id" => %parent_id,
                    "parent_gen" => ?locked_gen,
                );
                debug_assert_eq!(found.updater_gen, new_gen);
                Ok(UpdaterLock {
                    updater_id: child_lock_id,
                    locked_gen: new_gen,
                })
            }
            // The instance exists, but it's locked by a different saga than the
            // parent we were trying to inherit the lock from. We cannot acquire
            // the lock at this time.
            UpdateAndQueryResult { ref found, .. } => {
                slog::debug!(
                    &opctx.log,
                    "cannot inherit instance-updater lock from {parent_id} to \
                     {child_lock_id}: this instance is  not locked by the \
                     expected parent saga";
                    "instance_id" => %instance_id,
                    "updater_id" => %child_lock_id,
                    "parent_id" => %parent_id,
                    "actual_lock_id" => ?found.updater_id,
                );
                Err(UpdaterLockError::AlreadyLocked)
            }
        }
    }

    /// Release the instance-updater lock on this instance, if (and only if) the
    /// lock is currently held by the saga represented by the provided
    /// [`UpdaterLock`] token.
    pub async fn instance_updater_unlock(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        lock: &UpdaterLock,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let instance_id = authz_instance.id();
        let UpdaterLock { updater_id, locked_gen } = *lock;

        let result = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            // Only unlock the instance if:
            // - the provided updater ID matches that of the saga that has
            //   currently locked this instance.
            .filter(dsl::updater_id.eq(Some(updater_id)))
            // - the provided updater generation matches the current updater
            //   generation.
            .filter(dsl::updater_gen.eq(locked_gen))
            .set((
                dsl::updater_gen.eq(Generation(locked_gen.0.next())),
                dsl::updater_id.eq(None::<Uuid>),
            ))
            .check_if_exists::<Instance>(instance_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(instance_id),
                    ),
                )
            })?;

        match result {
            // If we updated the record, the lock has been released! Return
            // `Ok(true)` to indicate that we released the lock successfully.
            UpdateAndQueryResult { status: UpdateStatus::Updated, .. } => {
                return Ok(true);
            }

            // The instance exists, but we didn't unlock it. In almost all
            // cases, that's actually *fine*, since this suggests we didn't
            // actually have the lock to release, so we don't need to worry
            // about unlocking the instance. However, depending on the
            // particular reason we didn't actually unlock the instance, this
            // may be more or less likely to indicate a bug. Remember that saga
            // actions --- even unwind actions --- must be idempotent, so we
            // *may* just be trying to unlock an instance we already
            // successfully unlocked, which is fine.
            UpdateAndQueryResult { ref found, .. }
                if found.time_deleted().is_some() =>
            {
                debug!(
                    &opctx.log,
                    "attempted to unlock an instance that has been deleted";
                    "instance_id" => %instance_id,
                    "updater_id" => %updater_id,
                    "time_deleted" => ?found.time_deleted(),
                );
                return Ok(false);
            }

            // If the instance is no longer locked by this saga, that's probably fine.
            // We don't need to unlock it.
            UpdateAndQueryResult { ref found, .. }
                if found.updater_id != Some(updater_id) =>
            {
                if found.updater_gen > locked_gen {
                    // The generation has advanced past the generation where we
                    // acquired the lock. That's totally fine: a previous
                    // execution of the same saga action must have unlocked it,
                    // and now it is either unlocked, or locked by a different
                    // saga.
                    debug!(
                        &opctx.log,
                        "attempted to unlock an instance that is no longer \
                         locked by this saga";
                        "instance_id" => %instance_id,
                        "updater_id" => %updater_id,
                        "actual_id" => ?found.updater_id.as_ref(),
                        "found_gen" => ?found.updater_gen,
                        "locked_gen" => ?locked_gen,
                    );
                } else {
                    // On the other hand, if the generation is less than or
                    // equal to the generation at which we locked the instance,
                    // that eems kinda suspicious --- perhaps we believed we
                    // held the lock, but didn't actually, which could be
                    // programmer error.
                    //
                    // However, this *could* conceivably happen: the same saga
                    // node could have executed previously and released the
                    // lock, and then the generation counter advanced enough
                    // times to wrap around, and then the same action tried to
                    // release its lock again. 64-bit generation counters
                    // overflowing in an instance's lifetime seems unlikely, but
                    // nothing is impossible...
                    warn!(
                        &opctx.log,
                        "attempted to release a lock held by another saga \
                         at the same generation! this seems suspicious...";
                        "instance_id" => %instance_id,
                        "updater_id" => %updater_id,
                        "actual_id" => ?found.updater_id.as_ref(),
                        "found_gen" => ?found.updater_gen,
                        "locked_gen" => ?locked_gen,
                    );
                }

                Ok(false)
            }

            // If we *are* still holding the lock, we must be trying to
            // release it at the wrong generation. That seems quite
            // suspicious.
            UpdateAndQueryResult { ref found, .. } => {
                warn!(
                    &opctx.log,
                    "attempted to release a lock at the wrong generation";
                    "instance_id" => %instance_id,
                    "updater_id" => %updater_id,
                    "found_gen" => ?found.updater_gen,
                    "locked_gen" => ?locked_gen,
                );
                Err(Error::internal_error(
                    "instance is locked by this saga, but at a different \
                     generation",
                ))
            }
        }
    }

    /// Write the provided `new_runtime_state` for this instance, and release
    /// the provided `lock`.
    ///
    /// This method will unlock the instance if (and only if) the lock is
    /// currently held by the provided `updater_id`. If the lock is held by a
    /// different saga UUID, the instance will remain locked. If the instance
    /// has already been unlocked, this method will return `false`.
    ///
    /// # Arguments
    ///
    /// - `authz_instance`: the instance to attempt to unlock
    /// - `updater_lock`: an [`UpdaterLock`] token representing the acquired
    ///   lock to release.
    /// - `new_runtime`: an [`InstanceRuntimeState`] to write
    ///   back to the database when the lock is released. If this is [`None`],
    ///   the instance's runtime state will not be modified.
    pub async fn instance_commit_update(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        lock: &UpdaterLock,
        new_runtime: &InstanceRuntimeState,
    ) -> Result<bool, Error> {
        use db::schema::instance::dsl;

        let instance_id = authz_instance.id();
        let UpdaterLock { updater_id, locked_gen } = *lock;

        let result = diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            // Only unlock the instance if:
            // - the provided updater ID matches that of the saga that has
            //   currently locked this instance.
            .filter(dsl::updater_id.eq(Some(updater_id)))
            // - the provided updater generation matches the current updater
            //   generation.
            .filter(dsl::updater_gen.eq(locked_gen))
            .filter(dsl::state_generation.lt(new_runtime.r#gen))
            .set((
                dsl::updater_gen.eq(Generation(locked_gen.0.next())),
                dsl::updater_id.eq(None::<Uuid>),
                new_runtime.clone(),
            ))
            .check_if_exists::<Instance>(instance_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(instance_id),
                    ),
                )
            })?;

        // The expected state generation number of the instance record *before*
        // applying the update.
        let prev_state_gen = u64::from(new_runtime.r#gen.0).saturating_sub(1);
        match result {
            // If we updated the record, the lock has been released! Return
            // `Ok(true)` to indicate that we released the lock successfully.
            UpdateAndQueryResult { status: UpdateStatus::Updated, .. } => {
                Ok(true)
            }

            // The instance has been marked as deleted, so no updates were
            // committed!
            UpdateAndQueryResult { ref found, .. }
                if found.time_deleted().is_some() =>
            {
                warn!(
                    &opctx.log,
                    "cannot commit instance update, as the instance no longer \
                     exists";
                    "instance_id" => %instance_id,
                    "updater_id" => %updater_id,
                    "time_deleted" => ?found.time_deleted()
                );

                Err(LookupType::ById(instance_id)
                    .into_not_found(ResourceType::Instance))
            }

            // The instance exists, but both the lock generation *and* the state
            // generation no longer matches ours. That's fine --- presumably,
            // another execution of the same saga action has already updated the
            // instance record.
            UpdateAndQueryResult { ref found, .. }
                if u64::from(found.runtime().r#gen.0) != prev_state_gen
                    && found.updater_gen != locked_gen =>
            {
                debug_assert_ne!(found.updater_id, Some(updater_id));
                debug!(
                    &opctx.log,
                    "cannot commit instance updates, as the state generation \
                     and lock generation have advanced: the required updates \
                     have probably already been committed.";
                    "instance_id" => %instance_id,
                    "expected_state_gen" => ?new_runtime.r#gen,
                    "actual_state_gen" => ?found.runtime().r#gen,
                    "updater_id" => %updater_id,
                    "updater_gen" => ?locked_gen,
                    "actual_updater_gen" => ?found.updater_gen,
                );
                Ok(false)
            }

            // The state generation has advanced, but the instance is *still*
            // locked by this saga. That's bad --- this update saga may no
            // longer update the instance, as its state has changed, potentially
            // invalidating the updates. We need to unwind.
            UpdateAndQueryResult { ref found, .. }
                if u64::from(found.runtime().r#gen.0) != prev_state_gen
                    && found.updater_gen == locked_gen
                    && found.updater_id == Some(updater_id) =>
            {
                info!(
                    &opctx.log,
                    "cannot commit instance update, as the state generation \
                     has advanced, potentially invalidating the update";
                    "instance_id" => %instance_id,
                    "expected_state_gen" => ?new_runtime.r#gen,
                    "actual_state_gen" => ?found.runtime().r#gen,
                );
                Err(Error::conflict("instance state has changed"))
            }

            // The instance exists, but we could not update it because the lock
            // did not match.
            UpdateAndQueryResult { ref found, .. } => match found.updater_id {
                Some(actual_id) => {
                    const MSG: &'static str =
                        "cannot commit instance updates: the instance is \
                         locked by another saga!";
                    error!(
                        &opctx.log,
                        "{MSG}";
                        "instance_id" => %instance_id,
                        "updater_id" => %updater_id,
                        "actual_id" => %actual_id,
                        "found_gen" => ?found.updater_gen,
                        "locked_gen" => ?locked_gen,
                    );
                    Err(Error::internal_error(MSG))
                }
                None => {
                    const MSG: &'static str =
                        "cannot commit instance updates: the instance is \
                         not locked";
                    error!(
                        &opctx.log,
                        "{MSG}";
                        "instance_id" => %instance_id,
                        "updater_id" => %updater_id,
                        "found_gen" => ?found.updater_gen,
                        "locked_gen" => ?locked_gen,
                    );
                    Err(Error::internal_error(MSG))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::lookup::LookupPath;
    use nexus_db_model::InstanceState;
    use nexus_db_model::Project;
    use nexus_db_model::VmmRuntimeState;
    use nexus_db_model::VmmState;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params;
    use omicron_common::api::external;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;

    async fn create_test_instance(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> authz::Instance {
        let silo_id = *nexus_db_fixed_data::silo::DEFAULT_SILO_ID;
        let project_id = Uuid::new_v4();
        let instance_id = InstanceUuid::new_v4();

        let (authz_project, _project) = datastore
            .project_create(
                &opctx,
                Project::new_with_id(
                    project_id,
                    silo_id,
                    params::ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "stuff".parse().unwrap(),
                            description: "Where I keep my stuff".into(),
                        },
                    },
                ),
            )
            .await
            .expect("project must be created successfully");
        let _ = datastore
            .project_create_instance(
                &opctx,
                &authz_project,
                Instance::new(
                    instance_id,
                    project_id,
                    &params::InstanceCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "myinstance".parse().unwrap(),
                            description: "It's an instance".into(),
                        },
                        ncpus: 2i64.try_into().unwrap(),
                        memory: ByteCount::from_gibibytes_u32(16),
                        hostname: "myhostname".try_into().unwrap(),
                        user_data: Vec::new(),
                        network_interfaces:
                            params::InstanceNetworkInterfaceAttachment::None,
                        external_ips: Vec::new(),
                        disks: Vec::new(),
                        ssh_public_keys: None,
                        start: false,
                    },
                ),
            )
            .await
            .expect("instance must be created successfully");

        let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");
        authz_instance
    }

    #[tokio::test]
    async fn test_instance_updater_acquires_lock() {
        // Setup
        let logctx = dev::test_setup_log("test_instance_updater_acquires_lock");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let saga1 = Uuid::new_v4();
        let saga2 = Uuid::new_v4();
        let authz_instance = create_test_instance(&datastore, &opctx).await;

        macro_rules! assert_locked {
            ($id:expr) => {{
                let lock = dbg!(
                    datastore
                        .instance_updater_lock(&opctx, &authz_instance, $id)
                        .await
                )
                .expect(concat!(
                    "instance must be locked by ",
                    stringify!($id)
                ));
                assert_eq!(
                    lock.updater_id,
                    $id,
                    "instance's `updater_id` must be set to {}",
                    stringify!($id),
                );
                lock
            }};
        }

        macro_rules! assert_not_locked {
            ($id:expr) => {
                let err = dbg!(datastore
                    .instance_updater_lock(&opctx, &authz_instance, $id)
                    .await)
                    .expect_err("attempting to lock the instance while it is already locked must fail");
                assert_eq!(
                    err,
                    UpdaterLockError::AlreadyLocked,
                );
            };
        }

        // attempt to lock the instance from saga 1
        let lock1 = assert_locked!(saga1);

        // now, also attempt to lock the instance from saga 2. this must fail.
        assert_not_locked!(saga2);

        // unlock the instance from saga 1
        let unlocked = datastore
            .instance_updater_unlock(&opctx, &authz_instance, &lock1)
            .await
            .expect("instance must be unlocked by saga 1");
        assert!(unlocked, "instance must actually be unlocked");

        // now, locking the instance from saga 2 should succeed.
        let lock2 = assert_locked!(saga2);

        // trying to lock the instance again from saga 1 should fail
        assert_not_locked!(saga1);

        // unlock the instance from saga 2
        let unlocked = datastore
            .instance_updater_unlock(&opctx, &authz_instance, &lock2)
            .await
            .expect("instance must be unlocked by saga 2");
        assert!(unlocked, "instance must actually be unlocked");

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_updater_lock_is_idempotent() {
        // Setup
        let logctx =
            dev::test_setup_log("test_instance_updater_lock_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let saga1 = Uuid::new_v4();

        // attempt to lock the instance once.
        let lock1 = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect("instance should be locked");
        assert_eq!(lock1.updater_id, saga1);

        // doing it again should be fine.
        let lock2 = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect(
            "instance_updater_lock should succeed again with the same saga ID",
        );
        assert_eq!(lock2.updater_id, saga1);
        // the generation should not have changed as a result of the second
        // update.
        assert_eq!(lock1.locked_gen, lock2.locked_gen);

        // now, unlock the instance.
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(&opctx, &authz_instance, &lock1)
                .await
        )
        .expect("instance should unlock");
        assert!(unlocked, "instance should have unlocked");

        // unlocking it again should also succeed...
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(&opctx, &authz_instance, &lock2,)
                .await
        )
        .expect("instance should unlock again");
        // ...but the `locked` bool should now be false.
        assert!(!unlocked, "instance should already have been unlocked");

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_updater_cant_unlock_someone_elses_instance_() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_instance_updater_cant_unlock_someone_elses_instance_",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let saga1 = Uuid::new_v4();
        let saga2 = Uuid::new_v4();

        // lock the instance once.
        let lock1 = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect("instance should be locked");

        // attempting to unlock with a different saga ID shouldn't do anything.
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(
                    &opctx,
                    &authz_instance,
                    // N.B. that the `UpdaterLock` type's fields are private
                    // specifically to *prevent* callers from accidentally doing
                    // what we're doing here. But this simulates a case where
                    // an incorrect one is constructed, or a raw database query
                    // attempts an invalid unlock operation.
                    &UpdaterLock {
                        updater_id: saga2,
                        locked_gen: lock1.locked_gen,
                    },
                )
                .await
        )
        .unwrap();
        assert!(!unlocked);

        let instance =
            dbg!(datastore.instance_refetch(&opctx, &authz_instance).await)
                .expect("instance should exist");
        assert_eq!(instance.updater_id, Some(saga1));
        assert_eq!(instance.updater_gen, lock1.locked_gen);

        let next_gen = Generation(lock1.locked_gen.0.next());

        // unlocking with the correct ID should succeed.
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(&opctx, &authz_instance, &lock1)
                .await
        )
        .expect("instance should unlock");
        assert!(unlocked, "instance should have unlocked");

        let instance =
            dbg!(datastore.instance_refetch(&opctx, &authz_instance).await)
                .expect("instance should exist");
        assert_eq!(instance.updater_id, None);
        assert_eq!(instance.updater_gen, next_gen);

        // unlocking with the lock holder's ID *again* at a new generation
        // (where the lock is no longer held) shouldn't do anything
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(
                    &opctx,
                    &authz_instance,
                    // Again, these fields are private specifically to prevent
                    // you from doing this exact thing. But, we should  still
                    // test that we handle it gracefully.
                    &UpdaterLock { updater_id: saga1, locked_gen: next_gen },
                )
                .await
        )
        .unwrap();
        assert!(!unlocked);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unlocking_a_deleted_instance_is_okay() {
        // Setup
        let logctx =
            dev::test_setup_log("test_unlocking_a_deleted_instance_is_okay");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let saga1 = Uuid::new_v4();

        // put the instance in a state where it will be okay to delete later...
        datastore
            .instance_update_runtime(
                &InstanceUuid::from_untyped_uuid(authz_instance.id()),
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    r#gen: Generation(external::Generation::from_u32(2)),
                    propolis_id: None,
                    dst_propolis_id: None,
                    migration_id: None,
                    nexus_state: InstanceState::NoVmm,
                },
            )
            .await
            .expect("should update state successfully");

        // lock the instance once.
        let lock = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect("instance should be locked");

        // mark the instance as deleted
        dbg!(datastore.project_delete_instance(&opctx, &authz_instance).await)
            .expect("instance should be deleted");

        // unlocking should still succeed.
        dbg!(
            datastore
                .instance_updater_unlock(&opctx, &authz_instance, &lock)
                .await
        )
        .expect("instance should unlock");

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_commit_update_is_idempotent() {
        // Setup
        let logctx =
            dev::test_setup_log("test_instance_commit_update_is_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let saga1 = Uuid::new_v4();

        // lock the instance once.
        let lock = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect("instance should be locked");
        let new_runtime = &InstanceRuntimeState {
            time_updated: Utc::now(),
            r#gen: Generation(external::Generation::from_u32(2)),
            propolis_id: Some(Uuid::new_v4()),
            dst_propolis_id: None,
            migration_id: None,
            nexus_state: InstanceState::Vmm,
        };

        let updated = dbg!(
            datastore
                .instance_commit_update(
                    &opctx,
                    &authz_instance,
                    &lock,
                    &new_runtime
                )
                .await
        )
        .expect("instance_commit_update should succeed");
        assert!(updated, "it should be updated");

        // okay, let's do it again at the same generation.
        let updated = dbg!(
            datastore
                .instance_commit_update(
                    &opctx,
                    &authz_instance,
                    &lock,
                    &new_runtime
                )
                .await
        )
        .expect("instance_commit_update should succeed");
        assert!(!updated, "it was already updated");
        let instance =
            dbg!(datastore.instance_refetch(&opctx, &authz_instance).await)
                .expect("instance should exist");
        assert_eq!(instance.runtime().propolis_id, new_runtime.propolis_id);
        assert_eq!(instance.runtime().r#gen, new_runtime.r#gen);

        // Doing it again at the same generation with a *different* state
        // shouldn't change the instance at all.
        let updated = dbg!(
            datastore
                .instance_commit_update(
                    &opctx,
                    &authz_instance,
                    &lock,
                    &InstanceRuntimeState {
                        propolis_id: Some(Uuid::new_v4()),
                        migration_id: Some(Uuid::new_v4()),
                        dst_propolis_id: Some(Uuid::new_v4()),
                        ..new_runtime.clone()
                    }
                )
                .await
        )
        .expect("instance_commit_update should succeed");
        assert!(!updated, "it was already updated");
        let instance =
            dbg!(datastore.instance_refetch(&opctx, &authz_instance).await)
                .expect("instance should exist");
        assert_eq!(instance.runtime().propolis_id, new_runtime.propolis_id);
        assert_eq!(instance.runtime().dst_propolis_id, None);
        assert_eq!(instance.runtime().migration_id, None);
        assert_eq!(instance.runtime().r#gen, new_runtime.r#gen);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_update_invalidated_while_locked() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_instance_update_invalidated_while_locked",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let saga1 = Uuid::new_v4();

        // Lock the instance
        let lock = dbg!(
            datastore
                .instance_updater_lock(&opctx, &authz_instance, saga1)
                .await
        )
        .expect("instance should be locked");

        // Mutate the instance state, invalidating the state when the lock was
        // acquired.
        let new_runtime = &InstanceRuntimeState {
            time_updated: Utc::now(),
            r#gen: Generation(external::Generation::from_u32(2)),
            propolis_id: Some(Uuid::new_v4()),
            dst_propolis_id: Some(Uuid::new_v4()),
            migration_id: Some(Uuid::new_v4()),
            nexus_state: InstanceState::Vmm,
        };
        let updated = dbg!(
            datastore
                .instance_update_runtime(
                    &InstanceUuid::from_untyped_uuid(authz_instance.id()),
                    &new_runtime
                )
                .await
        )
        .expect("instance_update_runtime should succeed");
        assert!(updated, "it should be updated");

        // Okay, now try to commit the result of an update saga. This must fail,
        // because the state generation has changed while we had locked the
        // instance.
        let _err = dbg!(
            datastore
                .instance_commit_update(
                    &opctx,
                    &authz_instance,
                    &lock,
                    &InstanceRuntimeState {
                        time_updated: Utc::now(),
                        r#gen: Generation(external::Generation::from_u32(2)),
                        propolis_id: None,
                        dst_propolis_id: None,
                        migration_id: None,
                        nexus_state: InstanceState::NoVmm,
                    },
                )
                .await
        )
        .expect_err(
            "instance_commit_update should fail if the state generation is \
             stale",
        );

        let instance =
            dbg!(datastore.instance_refetch(&opctx, &authz_instance).await)
                .expect("instance should exist");
        assert_eq!(instance.runtime().propolis_id, new_runtime.propolis_id);
        assert_eq!(
            instance.runtime().dst_propolis_id,
            new_runtime.dst_propolis_id
        );
        assert_eq!(instance.runtime().migration_id, new_runtime.migration_id);
        assert_eq!(instance.runtime().nexus_state, new_runtime.nexus_state);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_fetch_all() {
        // Setup
        let logctx = dev::test_setup_log("test_instance_fetch_all");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let snapshot =
            dbg!(datastore.instance_fetch_all(&opctx, &authz_instance).await)
                .expect("instance fetch must succeed");

        assert_eq!(
            dbg!(snapshot.instance.id()),
            dbg!(authz_instance.id()),
            "must have fetched the correct instance"
        );
        assert_eq!(
            dbg!(snapshot.active_vmm),
            None,
            "instance does not have an active VMM"
        );
        assert_eq!(
            dbg!(snapshot.target_vmm),
            None,
            "instance does not have a target VMM"
        );
        assert_eq!(
            dbg!(snapshot.migration),
            None,
            "instance does not have a migration"
        );

        let active_vmm = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.32".parse().unwrap(),
                    propolis_port: 420.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("active VMM should be inserted successfully!");

        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());
        datastore
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    gen: Generation(
                        snapshot.instance.runtime_state.gen.0.next(),
                    ),
                    nexus_state: InstanceState::Vmm,
                    propolis_id: Some(active_vmm.id),
                    ..snapshot.instance.runtime_state.clone()
                },
            )
            .await
            .expect("instance update should work");
        let snapshot =
            dbg!(datastore.instance_fetch_all(&opctx, &authz_instance).await)
                .expect("instance fetch must succeed");

        assert_eq!(
            dbg!(snapshot.instance.id()),
            dbg!(authz_instance.id()),
            "must have fetched the correct instance"
        );
        assert_eq!(
            dbg!(snapshot.active_vmm.map(|vmm| vmm.id)),
            Some(dbg!(active_vmm.id)),
            "fetched active VMM must be the instance's active VMM"
        );
        assert_eq!(
            dbg!(snapshot.target_vmm),
            None,
            "instance does not have a target VMM"
        );
        assert_eq!(
            dbg!(snapshot.migration),
            None,
            "instance does not have a migration"
        );

        let target_vmm = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.42".parse().unwrap(),
                    propolis_port: 666.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("target VMM should be inserted successfully!");
        let migration = datastore
            .migration_insert(
                &opctx,
                Migration::new(
                    Uuid::new_v4(),
                    instance_id,
                    active_vmm.id,
                    target_vmm.id,
                ),
            )
            .await
            .expect("migration should be inserted successfully!");
        datastore
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    gen: Generation(
                        snapshot.instance.runtime_state.gen.0.next(),
                    ),
                    nexus_state: InstanceState::Vmm,
                    propolis_id: Some(active_vmm.id),
                    dst_propolis_id: Some(target_vmm.id),
                    migration_id: Some(migration.id),
                },
            )
            .await
            .expect("instance update should work");
        let snapshot =
            dbg!(datastore.instance_fetch_all(&opctx, &authz_instance).await)
                .expect("instance fetch must succeed");

        assert_eq!(
            dbg!(snapshot.instance.id()),
            dbg!(authz_instance.id()),
            "must have fetched the correct instance"
        );
        assert_eq!(
            dbg!(snapshot.active_vmm.map(|vmm| vmm.id)),
            Some(dbg!(active_vmm.id)),
            "fetched active VMM must be the instance's active VMM"
        );
        assert_eq!(
            dbg!(snapshot.target_vmm.map(|vmm| vmm.id)),
            Some(dbg!(target_vmm.id)),
            "fetched target VMM must be the instance's target VMM"
        );
        assert_eq!(
            dbg!(snapshot.migration.map(|m| m.id)),
            Some(dbg!(migration.id)),
            "fetched migration must be the instance's migration"
        );

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_set_migration_ids() {
        // Setup
        let logctx = dev::test_setup_log("test_instance_set_migration_ids");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;

        // Create the first VMM in a state where `set_migration_ids` should
        // *fail* (Stopped). We will assert that we cannot set the migration
        // IDs, and then advance it to Running, when we can start the migration.
        let vmm1 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.32".parse().unwrap(),
                    propolis_port: 420.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Stopped,
                    },
                },
            )
            .await
            .expect("active VMM should be inserted successfully!");

        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());
        let instance = datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .expect("instance should be there");
        datastore
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    r#gen: Generation(instance.runtime_state.gen.0.next()),
                    nexus_state: InstanceState::Vmm,
                    propolis_id: Some(vmm1.id),
                    ..instance.runtime_state.clone()
                },
            )
            .await
            .expect("instance update should work");

        let vmm2 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.42".parse().unwrap(),
                    propolis_port: 420.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("second VMM should insert");

        // make a migration...
        let migration = datastore
            .migration_insert(
                &opctx,
                Migration::new(Uuid::new_v4(), instance_id, vmm1.id, vmm2.id),
            )
            .await
            .expect("migration should be inserted successfully!");

        // Our first attempt to set migration IDs should fail, because the
        // active VMM is Stopped.
        let res = dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration.id,
                    PropolisUuid::from_untyped_uuid(vmm2.id),
                )
                .await
        );
        assert!(res.is_err());

        // Okay, now, advance the active VMM to Running, and try again.
        let updated = dbg!(
            datastore
                .vmm_update_runtime(
                    &PropolisUuid::from_untyped_uuid(vmm1.id),
                    &VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation(vmm2.runtime.r#gen.0.next()),
                        state: VmmState::Running,
                    },
                )
                .await
        )
        .expect("updating VMM state should be fine");
        assert!(updated);

        // Now, it should work!
        let instance = dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration.id,
                    PropolisUuid::from_untyped_uuid(vmm2.id),
                )
                .await
        )
        .expect("setting migration IDs should succeed");
        assert_eq!(instance.runtime().dst_propolis_id, Some(vmm2.id));
        assert_eq!(instance.runtime().migration_id, Some(migration.id));

        // Doing it again should be idempotent, and the instance record
        // shouldn't change.
        let instance2 = dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration.id,
                    PropolisUuid::from_untyped_uuid(vmm2.id),
                )
                .await
        )
        .expect("setting the same migration IDs a second time should succeed");
        assert_eq!(
            instance.runtime().dst_propolis_id,
            instance2.runtime().dst_propolis_id
        );
        assert_eq!(
            instance.runtime().migration_id,
            instance2.runtime().migration_id
        );

        // Trying to set a new migration should fail, as long as the prior stuff
        // is still in place.
        let vmm3 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.42".parse().unwrap(),
                    propolis_port: 420.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("third VMM should insert");
        let migration2 = datastore
            .migration_insert(
                &opctx,
                Migration::new(Uuid::new_v4(), instance_id, vmm1.id, vmm3.id),
            )
            .await
            .expect("migration should be inserted successfully!");
        dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration2.id,
                    PropolisUuid::from_untyped_uuid(vmm3.id),
                )
                .await
        )
        .expect_err(
            "trying to set migration IDs should fail when a previous \
             migration and VMM are still there",
        );

        // Pretend the previous migration saga has unwound the VMM
        let updated = dbg!(
            datastore
                .vmm_update_runtime(
                    &PropolisUuid::from_untyped_uuid(vmm2.id),
                    &VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation(vmm2.runtime.r#gen.0.next().next()),
                        state: VmmState::SagaUnwound,
                    },
                )
                .await
        )
        .expect("updating VMM state should be fine");
        assert!(updated);

        // It should still fail, since the migration is still in progress.
        dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration2.id,
                    PropolisUuid::from_untyped_uuid(vmm3.id),
                )
                .await
        )
        .expect_err(
            "trying to set migration IDs should fail when a previous \
             migration ID is present and not marked as failed",
        );

        // Now, mark the previous migration as Failed.
        let updated = dbg!(datastore
            .migration_mark_failed(&opctx, migration.id)
            .await
            .expect(
                "we should be able to mark the previous migration as failed"
            ));
        assert!(updated);

        // If the current migration is failed on both sides *and* the current
        // VMM is SagaUnwound, we should be able to clobber them with new IDs.
        let instance = dbg!(
            datastore
                .instance_set_migration_ids(
                    &opctx,
                    instance_id,
                    PropolisUuid::from_untyped_uuid(vmm1.id),
                    migration2.id,
                    PropolisUuid::from_untyped_uuid(vmm3.id),
                )
                .await
        )
        .expect("replacing SagaUnwound VMM should work");
        assert_eq!(instance.runtime().migration_id, Some(migration2.id));
        assert_eq!(instance.runtime().dst_propolis_id, Some(vmm3.id));

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
