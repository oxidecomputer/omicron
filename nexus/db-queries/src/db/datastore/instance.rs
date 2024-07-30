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

/// A complete snapshot of the database records describing the current state of
/// an instance: the [`Instance`] record itself, along with its active [`Vmm`],
/// target [`Vmm`], and current [`Migration`], if they exist.
///
/// This is returned by [`DataStore::instance_fetch_all`].
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct InstanceSnapshot {
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
    updater_id: Uuid,
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
    /// [`InstanceSnapshot`], which contains the following:
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
    ) -> LookupResult<InstanceSnapshot> {
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

        Ok(InstanceSnapshot { instance, migration, active_vmm, target_vmm })
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
    pub async fn instance_set_migration_ids(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        src_propolis_id: PropolisUuid,
        migration_id: Uuid,
        target_propolis_id: PropolisUuid,
    ) -> Result<Instance, Error> {
        use db::schema::instance::dsl;
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
            .filter(vmm_dsl::state.eq_any(ALLOWED_ACTIVE_VMM_STATES))
            .select(vmm_dsl::instance_id);
        // Subquery for checking if a present target VMM ID points at a VMM
        // that's in the saga-unwound state (in which it would be okay to clear
        // out that VMM).
        let target_vmm_unwound = vmm_dsl::vmm
            .filter(vmm_dsl::id.nullable().eq(dsl::target_propolis_id))
            .filter(vmm_dsl::state.eq(VmmState::SagaUnwound))
            .select(vmm_dsl::instance_id);

        diesel::update(dsl::instance)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(instance_id))
            .filter(
                // To ensure that saga actions that set migration IDs are
                // idempotent, we update the row if the migration and target
                // VMM IDs are not present *or* if they are already equal to the
                // desired values. This way, we can use a `RETURNING` clause to
                // fetch the current state after the update, rather than
                // `check_if_exists` which returns the prior state, and still
                // fail to update the record if another migration/target VMM ID
                // is already there.
                (dsl::migration_id.is_null().and(
                    dsl::target_propolis_id
                        .is_null()
                        // It's okay to clobber a previously-set target VMM ID
                        // if (and only if!) it's in the saga-unwound state.
                        .or(dsl::id.eq_any(target_vmm_unwound)),
                ))
                .or(dsl::migration_id
                    .eq(Some(migration_id))
                    .and(dsl::target_propolis_id.eq(Some(target_propolis_id)))),
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
            .map_err(|e| {
                // Turning all these errors into `NotFound` errors is a bit
                // unfortunate. The query will not find anything fail if the
                // instance ID actually doesn't exist, *or* if any of the "is
                // it valid to set migration IDs in the current state?" checks
                // fail, which should probably be `Error::Conflict`
                // instead...but, we can't really tell which is the case here.
                //
                // TODO(eliza): Perhaps these should all be mapped to `Conflict`
                // instead? It's arguably correct to say that trying to set
                // migration IDs for an instance that doesn't exist is sort of a
                // "conflict", for a significantly broad definition of "conflcit"...
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Instance,
                        LookupType::ById(instance_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Unsets the migration IDs set by
    /// [`DataStore::instance_set_migration_ids`].
    ///
    /// This method will only unset the instance's migration IDs if they match
    /// the provided ones.
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
    /// instance-update saga, returning the state of the instance when the lock
    /// was acquired.
    ///
    /// # Notes
    ///
    /// This method MUST only be called from the context of a saga! The
    /// calling saga must ensure that the reverse action for the action that
    /// acquires the lock must call [`DataStore::instance_updater_unlock`] to
    /// ensure that the lock is always released if the saga unwinds.
    ///
    /// This method is idempotent: if the instance is already locked by the same
    /// saga, it will succeed, as though the lock was acquired.
    ///
    /// # Arguments
    ///
    /// - `authz_instance`: the instance to attempt to lock to lock
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

    pub async fn instance_updater_inherit_lock(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        UpdaterLock { updater_id: parent_id, locked_gen }: UpdaterLock,
        child_lock_id: Uuid,
    ) -> Result<UpdaterLock, UpdaterLockError> {
        use db::schema::instance::dsl;

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
            // If we updated the record, the lock has been released! Return
            // `Ok(true)` to indicate that we released the lock successfully.
            UpdateAndQueryResult { status: UpdateStatus::Updated, .. } => {
                slog::info!(
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
                debug_assert_eq!(found.updater_gen, new_gen,);
                Ok(UpdaterLock {
                    updater_id: child_lock_id,
                    locked_gen: new_gen,
                })
            }
            // The instance exists, but the lock ID doesn't match our lock ID.
            // This means we were trying to release a lock we never held, whcih
            // is almost certainly a programmer error.
            UpdateAndQueryResult { .. } => Err(UpdaterLockError::AlreadyLocked),
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
            // N.B. that we intentionally *don't* filter out instances that have
            // been deleted. If the instance doesn't exist, whatever. It is, by
            // definition, "unlocked"... :)
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
                Ok(true)
            }
            // The generation has advanced past the generation at which the
            // lock was held. This means that we have already released the
            // lock. Return `Ok(false)` here for idempotency.
            UpdateAndQueryResult {
                status: UpdateStatus::NotUpdatedButExists,
                ref found,
            } if found.updater_gen > locked_gen => Ok(false),

            // The instance exists, but the lock ID doesn't match our lock ID.
            // This means we were trying to release a lock we never held, whcih
            // is almost certainly a programmer error.
            UpdateAndQueryResult { ref found, .. } => {
                match found.updater_id {
                    Some(actual_id) if actual_id != updater_id => {
                        slog::error!(
                            &opctx.log,
                            "attempted to release a lock held by another saga";
                            "instance_id" => %instance_id,
                            "updater_id" => %updater_id,
                            "actual_id" => %actual_id,
                            "found_gen" => ?found.updater_gen,
                            "locked_gen" => ?locked_gen,
                        );
                        Err(Error::internal_error(
                            "attempted to release a lock held by another saga! this is a bug!",
                        ))
                    },
                    Some(_) =>  Ok(false),
                    None => Err(Error::internal_error(
                        "attempted to release a lock on an instance that is not locked! this is a bug!",
                    )),
                }
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
            .filter(dsl::state_generation.lt(new_runtime.gen))
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

        match result {
            // If we updated the record, the lock has been released! Return
            // `Ok(true)` to indicate that we released the lock successfully.
            UpdateAndQueryResult { status: UpdateStatus::Updated, .. } => {
                Ok(true)
            }

            // The instance has been marked as deleted, so no updates were
            // committed!
            UpdateAndQueryResult {
                status: UpdateStatus::NotUpdatedButExists,
                ref found,
            } if found.time_deleted().is_some() => {
                warn!(
                    &opctx.log,
                    "cannot commit instance update, as the instance no longer exists";
                    "instance_id" => %instance_id,
                    "updater_id" => %updater_id,
                    "time_deleted" => ?found.time_deleted()
                );

                Err(LookupType::ById(instance_id).into_not_found(ResourceType::Instance))
            }

            // The generation has advanced past the generation at which the
            // lock was held. This means that we have already released the
            // lock. Return `Ok(false)` here for idempotency.
            UpdateAndQueryResult {
                status: UpdateStatus::NotUpdatedButExists,
                ref found,
            } if found.updater_gen > locked_gen => Ok(false),

            // The instance exists, but the lock ID doesn't match our lock ID.
            // This means we were trying to release a lock we never held, whcih
            // is almost certainly a programmer error.
            UpdateAndQueryResult { ref found, .. } => {
                match found.updater_id {
                    Some(actual_id) if actual_id != updater_id => {
                        slog::error!(
                            &opctx.log,
                            "attempted to release a lock held by another saga";
                            "instance_id" => %instance_id,
                            "updater_id" => %updater_id,
                            "actual_id" => %actual_id,
                            "found_gen" => ?found.updater_gen,
                            "locked_gen" => ?locked_gen,
                        );
                        Err(Error::internal_error(
                            "attempted to release a lock held by another saga! this is a bug!",
                        ))
                    },
                    Some(_) =>  Err(Error::conflict(
                        "attempted to commit an instance update, but the state generation has advanced!"
                    )),
                    None => Err(Error::internal_error(
                        "attempted to release a lock on an instance that is not locked! this is a bug!",
                    )),
                }
            }
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
    async fn test_instance_updater_unlocking_someone_elses_instance_errors() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_instance_updater_unlocking_someone_elses_instance_errors",
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

        // attempting to unlock with a different saga ID should be an error.
        let err = dbg!(
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
        .expect_err(
            "unlocking the instance with someone else's ID should fail",
        );
        assert_eq!(
            err,
            Error::internal_error(
                "attempted to release a lock held by another saga! \
                this is a bug!",
            ),
        );
        let next_gen = Generation(lock1.locked_gen.0.next());

        // unlocking with the correct ID should succeed.
        let unlocked = dbg!(
            datastore
                .instance_updater_unlock(&opctx, &authz_instance, &lock1)
                .await
        )
        .expect("instance should unlock");
        assert!(unlocked, "instance should have unlocked");

        // unlocking with the lock holder's ID *again* at a new generation
        // (where the lock is no longer held) should fail.
        let err = dbg!(
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
        .expect_err(
            "unlocking the instance with someone else's ID should fail",
        );
        assert_eq!(
            err,
            Error::internal_error(
                "attempted to release a lock on an instance \
                that is not locked! this is a bug!"
            ),
        );

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
        let instance = instance2;

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
        ).expect_err("trying to set migration IDs should fail when a previous migration and VMM are still there");

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

        // It should still fail due to the presence of the migration ID.
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
        ).expect_err("trying to set migration IDs should fail when a previous migration ID is still there");

        // Remove the migration ID.
        let updated = dbg!(datastore
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    r#gen: Generation(instance.runtime_state.gen.0.next()),
                    nexus_state: InstanceState::Vmm,
                    propolis_id: Some(vmm1.id),
                    migration_id: None,
                    ..instance.runtime_state.clone()
                },
            )
            .await
            .expect("instance update should work"));
        assert!(updated);

        // Now that the migration ID is gone, we should be able to clobber the
        // SagaUnwound VMM ID.
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
