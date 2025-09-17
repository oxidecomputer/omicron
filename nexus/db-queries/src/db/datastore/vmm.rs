// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] helpers for working with VMM records.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::Vmm;
use crate::db::model::VmmRuntimeState;
use crate::db::model::VmmState as DbVmmState;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::vmm::dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::Migrations;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use std::net::SocketAddr;
use uuid::Uuid;

/// The result of an [`DataStore::vmm_and_migration_update_runtime`] call,
/// indicating which records were updated.
#[derive(Clone, Debug)]
pub struct VmmStateUpdateResult {
    /// The VMM record that the update query found and possibly updated.
    ///
    /// NOTE: This is the record prior to the update!
    pub found_vmm: Vmm,

    /// `true` if the VMM record was updated, `false` otherwise.
    pub vmm_updated: bool,

    /// `true` if a migration record was updated for the migration in, false if
    /// no update was performed or no migration in was provided.
    pub migration_in_updated: bool,

    /// `true` if a migration record was updated for the migration out, false if
    /// no update was performed or no migration out was provided.
    pub migration_out_updated: bool,
}

impl DataStore {
    pub async fn vmm_insert(
        &self,
        opctx: &OpContext,
        vmm: Vmm,
    ) -> CreateResult<Vmm> {
        let vmm = diesel::insert_into(dsl::vmm)
            .values(vmm)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_state_updated.eq(dsl::time_state_updated))
            .returning(Vmm::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(vmm)
    }

    pub async fn vmm_mark_deleted(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
    ) -> UpdateResult<bool> {
        let updated = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::state.eq_any(DbVmmState::DESTROYABLE_STATES))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<Vmm>(vmm_id.into_untyped_uuid())
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(updated)
    }

    pub async fn vmm_fetch(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
    ) -> LookupResult<Vmm> {
        let vmm = dsl::vmm
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(Vmm::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(vmm)
    }

    pub async fn vmm_update_runtime(
        &self,
        vmm_id: &PropolisUuid,
        new_runtime: &VmmRuntimeState,
    ) -> Result<bool, Error> {
        self.vmm_update_runtime_on_connection(
            &*self.pool_connection_unauthorized().await?,
            vmm_id,
            new_runtime,
        )
        .await
        .map(|r| match r.status {
            UpdateStatus::Updated => true,
            UpdateStatus::NotUpdatedButExists => false,
        })
        .map_err(|e| {
            public_error_from_diesel(
                e,
                ErrorHandler::NotFoundByLookup(
                    ResourceType::Vmm,
                    LookupType::ById(vmm_id.into_untyped_uuid()),
                ),
            )
        })
    }

    async fn vmm_update_runtime_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        vmm_id: &PropolisUuid,
        new_runtime: &VmmRuntimeState,
    ) -> Result<UpdateAndQueryResult<Vmm>, diesel::result::Error> {
        diesel::update(dsl::vmm)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Vmm>(vmm_id.into_untyped_uuid())
            .execute_and_check(conn)
            .await
    }

    /// Updates a VMM record and associated migration record(s) with a single
    /// database command.
    ///
    /// This is intended to be used to apply updates from sled agent that
    /// may change a VMM's runtime state (e.g. moving an instance from Running
    /// to Stopped) and the state of its current active mgiration in a single
    /// transaction. The caller is responsible for ensuring the VMM and
    /// migration states are consistent with each other before calling this
    /// routine.
    ///
    /// # Arguments
    ///
    /// - `vmm_id`: The ID of the VMM to update.
    /// - `new_runtime`: The new VMM runtime state to try to write.
    /// - `migrations`: The (optional) migration-in and migration-out states to
    ///   try to write.
    ///
    /// # Return value
    ///
    /// - `Ok(`[`VmmStateUpdateResult`]`)` if the query was issued
    ///   successfully. The returned [`VmmStateUpdateResult`] indicates which
    ///   database record(s) were updated. Note that an update can fail because
    ///   it was inapplicable (i.e. the database has state with a newer
    ///   generation already) or because the relevant record was not found.
    /// - `Err` if another error occurred while accessing the database.
    pub async fn vmm_and_migration_update_runtime(
        &self,
        opctx: &OpContext,
        vmm_id: PropolisUuid,
        new_runtime: &VmmRuntimeState,
        Migrations { migration_in, migration_out }: Migrations<'_>,
    ) -> Result<VmmStateUpdateResult, Error> {
        fn migration_id(
            m: Option<&nexus::MigrationRuntimeState>,
        ) -> Option<Uuid> {
            m.as_ref().map(|m| m.migration_id)
        }

        // If both a migration-in and migration-out update was provided for this
        // VMM, they can't be from the same migration, since migrating from a
        // VMM to itself wouldn't make sense...
        let migration_out_id = migration_id(migration_out);
        if migration_out_id.is_some()
            && migration_out_id == migration_id(migration_in)
        {
            return Err(Error::conflict(
                "migrating from a VMM to itself is nonsensical",
            ))
            .internal_context(format!("migration_in: {migration_in:?}; migration_out: {migration_out:?}"));
        }

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("vmm_and_migration_update_runtime")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                let vmm_update_result = self
                    .vmm_update_runtime_on_connection(
                        &conn,
                        &vmm_id,
                        new_runtime,
                    )
                    .await?;


                let found_vmm = vmm_update_result.found;
                let vmm_updated = match vmm_update_result.status {
                     UpdateStatus::Updated => true,
                     UpdateStatus::NotUpdatedButExists => false
                };

                let migration_out_updated = match migration_out {
                    Some(migration) => {
                        let r = self.migration_update_source_on_connection(
                            &conn, &vmm_id, migration,
                        )
                        .await?;
                        match r.status {
                            UpdateStatus::Updated => true,
                            UpdateStatus::NotUpdatedButExists => match r.found {
                                m if m.time_deleted.is_some() => return Err(err.bail(Error::Gone)),
                                m if m.source_propolis_id != vmm_id.into_untyped_uuid() => {
                                    return Err(err.bail(Error::invalid_value(
                                        "source propolis UUID",
                                        format!("{vmm_id} is not the source VMM of this migration"),
                                    )));
                                }
                                // Not updated, generation has advanced.
                                _ => false
                            },
                        }
                    },
                    None => false,
                };
                let migration_in_updated = match migration_in {
                    Some(migration) => {
                        let r = self.migration_update_target_on_connection(
                            &conn, &vmm_id, migration,
                        )
                        .await?;
                        match r.status {
                            UpdateStatus::Updated => true,
                            UpdateStatus::NotUpdatedButExists => match r.found {
                                m if m.time_deleted.is_some() => return Err(err.bail(Error::Gone)),
                                m if m.target_propolis_id != vmm_id.into_untyped_uuid() => {
                                    return Err(err.bail(Error::invalid_value(
                                        "target propolis UUID",
                                        format!("{vmm_id} is not the target VMM of this migration"),
                                    )));
                                }
                                // Not updated, generation has advanced.
                                _ => false
                            },
                        }
                    },
                    None => false,
                };
                Ok(VmmStateUpdateResult {
                    found_vmm,
                    vmm_updated,
                    migration_in_updated,
                    migration_out_updated,
                })
            }})
            .await
            .map_err(|e| {
                err.take().unwrap_or_else(|| public_error_from_diesel(e, ErrorHandler::Server))
            })
    }

    /// Transitions a VMM to the `SagaUnwound` state.
    ///
    /// # Warning
    ///
    /// This may *only* be called by the saga that created a VMM record, as it
    /// unconditionally increments the generation number and advances the VMM to
    /// the `SagaUnwound` state.
    ///
    /// This is necessary as it is executed in compensating actions for
    /// unwinding saga nodes which cannot easily determine whether other
    /// actions, which advance the VMM's generation, have executed before the
    /// saga unwound.
    pub async fn vmm_mark_saga_unwound(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
    ) -> Result<bool, Error> {
        diesel::update(dsl::vmm)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .set((
                dsl::state.eq(DbVmmState::SagaUnwound),
                dsl::time_state_updated.eq(chrono::Utc::now()),
                dsl::state_generation.eq(dsl::state_generation + 1),
            ))
            .check_if_exists::<Vmm>(vmm_id.into_untyped_uuid())
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Forcibly overwrites the Propolis IP/Port in the supplied VMM's record with
    /// the supplied Propolis IP.
    ///
    /// This is used in tests to overwrite the IP for a VMM that is backed by a
    /// mock Propolis server that serves on localhost but has its Propolis IP
    /// allocated by the instance start procedure. (Unfortunately, this can't be
    /// marked #[cfg(test)] because the integration tests require this
    /// functionality.)
    pub async fn vmm_overwrite_addr_for_test(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
        new_addr: SocketAddr,
    ) -> UpdateResult<Vmm> {
        let new_ip = ipnetwork::IpNetwork::from(new_addr.ip());
        let new_port = new_addr.port();
        let vmm = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .set((
                dsl::propolis_ip.eq(new_ip),
                dsl::propolis_port.eq(i32::from(new_port)),
            ))
            .returning(Vmm::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(vmm)
    }

    /// Lists VMMs which have been abandoned by their instances after a
    /// migration and are in need of cleanup.
    ///
    /// A VMM is considered "abandoned" if (and only if):
    ///
    /// - It is in the `Destroyed` or `SagaUnwound` state.
    /// - It is not currently running an instance, and it is also not the
    ///   migration target of any instance (i.e. it is not pointed to by
    ///   any instance record's `active_propolis_id` and `target_propolis_id`
    ///   fields).
    /// - It has not been deleted yet.
    pub async fn vmm_list_abandoned(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Vmm> {
        use nexus_db_schema::schema::instance::dsl as instance_dsl;

        paginated(dsl::vmm, dsl::id, pagparams)
            // In order to be considered "abandoned", a VMM must be:
            // - in the `Destroyed`, `SagaUnwound`, or `Failed` states
            .filter(dsl::state.eq_any(DbVmmState::DESTROYABLE_STATES))
            // - not deleted yet
            .filter(dsl::time_deleted.is_null())
            // - not pointed to by any instance's `active_propolis_id` or
            //   `target_propolis_id`.
            .left_join(
                // Left join with the `instance` table on the VMM's instance ID, so
                // that we can check if the instance pointed to by this VMM (if
                // any exists) has this VMM pointed to by its
                // `active_propolis_id` or `target_propolis_id` fields.
                instance_dsl::instance
                    .on(instance_dsl::id.eq(dsl::instance_id)),
            )
            .filter(
                dsl::id
                    .nullable()
                    .ne(instance_dsl::active_propolis_id)
                    // In SQL, *all* comparisons with NULL are `false`, even `!=
                    // NULL`, so we have to explicitly check for nulls here, or
                    // else VMMs whose instances have no `active_propolis_id`
                    // will not be considered abandoned (incorrectly).
                    .or(instance_dsl::active_propolis_id.is_null()),
            )
            .filter(
                dsl::id
                    .nullable()
                    .ne(instance_dsl::target_propolis_id)
                    // As above, we must add this clause because SQL nulls have
                    // the most irritating behavior possible.
                    .or(instance_dsl::target_propolis_id.is_null()),
            )
            .select(Vmm::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::db::model::Generation;
    use crate::db::model::Migration;
    use crate::db::model::VmmRuntimeState;
    use crate::db::model::VmmState;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::VmmCpuPlatform;
    use omicron_common::api::internal::nexus;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::InstanceUuid;
    use omicron_uuid_kinds::SledUuid;

    #[tokio::test]
    async fn test_vmm_and_migration_update_runtime() {
        // Setup
        let logctx =
            dev::test_setup_log("test_vmm_and_migration_update_runtime");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let instance_id = InstanceUuid::from_untyped_uuid(Uuid::new_v4());
        let vmm1 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: instance_id.into_untyped_uuid(),
                    sled_id: SledUuid::new_v4().into(),
                    propolis_ip: "10.1.9.32".parse().unwrap(),
                    propolis_port: 420.into(),
                    cpu_platform: VmmCpuPlatform::SledDefault,
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("VMM 1 should be inserted successfully!");

        let vmm2 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: instance_id.into_untyped_uuid(),
                    sled_id: SledUuid::new_v4().into(),
                    propolis_ip: "10.1.9.42".parse().unwrap(),
                    propolis_port: 420.into(),
                    cpu_platform: VmmCpuPlatform::SledDefault,
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("VMM 2 should be inserted successfully!");

        let migration1 = datastore
            .migration_insert(
                &opctx,
                Migration::new(Uuid::new_v4(), instance_id, vmm1.id, vmm2.id),
            )
            .await
            .expect("migration should be inserted successfully!");

        info!(
            &logctx.log,
            "pretending to migrate from vmm1 to vmm2";
            "vmm1" => ?vmm1,
            "vmm2" => ?vmm2,
            "migration" => ?migration1,
        );

        let vmm1_migration_out = nexus::MigrationRuntimeState {
            migration_id: migration1.id,
            state: nexus::MigrationState::Completed,
            r#gen: Generation::new().0.next(),
            time_updated: Utc::now(),
        };
        datastore
            .vmm_and_migration_update_runtime(
                &opctx,
                PropolisUuid::from_untyped_uuid(vmm1.id),
                &VmmRuntimeState {
                    time_state_updated: Utc::now(),
                    r#gen: Generation(vmm1.runtime.r#gen.0.next()),
                    state: VmmState::Stopping,
                },
                Migrations {
                    migration_in: None,
                    migration_out: Some(&vmm1_migration_out),
                },
            )
            .await
            .expect("vmm1 state should update");
        let vmm2_migration_in = nexus::MigrationRuntimeState {
            migration_id: migration1.id,
            state: nexus::MigrationState::Completed,
            r#gen: Generation::new().0.next(),
            time_updated: Utc::now(),
        };
        datastore
            .vmm_and_migration_update_runtime(
                &opctx,
                PropolisUuid::from_untyped_uuid(vmm2.id),
                &VmmRuntimeState {
                    time_state_updated: Utc::now(),
                    r#gen: Generation(vmm2.runtime.r#gen.0.next()),
                    state: VmmState::Running,
                },
                Migrations {
                    migration_in: Some(&vmm2_migration_in),
                    migration_out: None,
                },
            )
            .await
            .expect("vmm1 state should update");

        let all_migrations = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_eq!(all_migrations.len(), 1);
        let db_migration1 = &all_migrations[0];
        assert_eq!(
            db_migration1.source_state,
            db::model::MigrationState::COMPLETED
        );
        assert_eq!(
            db_migration1.target_state,
            db::model::MigrationState::COMPLETED
        );
        assert_eq!(
            db_migration1.source_gen,
            Generation(Generation::new().0.next()),
        );
        assert_eq!(
            db_migration1.target_gen,
            Generation(Generation::new().0.next()),
        );

        // now, let's simulate a second migration, out of vmm2.
        let vmm3 = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: instance_id.into_untyped_uuid(),
                    sled_id: SledUuid::new_v4().into(),
                    propolis_ip: "10.1.9.69".parse().unwrap(),
                    propolis_port: 420.into(),
                    cpu_platform: VmmCpuPlatform::SledDefault,
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::Running,
                    },
                },
            )
            .await
            .expect("VMM 2 should be inserted successfully!");

        let migration2 = datastore
            .migration_insert(
                &opctx,
                Migration::new(Uuid::new_v4(), instance_id, vmm2.id, vmm3.id),
            )
            .await
            .expect("migration 2 should be inserted successfully!");
        info!(
            &logctx.log,
            "pretending to migrate from vmm2 to vmm3";
            "vmm2" => ?vmm2,
            "vmm3" => ?vmm3,
            "migration" => ?migration2,
        );

        let vmm2_migration_out = nexus::MigrationRuntimeState {
            migration_id: migration2.id,
            state: nexus::MigrationState::Completed,
            r#gen: Generation::new().0.next(),
            time_updated: Utc::now(),
        };
        datastore
            .vmm_and_migration_update_runtime(
                &opctx,
                PropolisUuid::from_untyped_uuid(vmm2.id),
                &VmmRuntimeState {
                    time_state_updated: Utc::now(),
                    r#gen: Generation(vmm2.runtime.r#gen.0.next()),
                    state: VmmState::Destroyed,
                },
                Migrations {
                    migration_in: Some(&vmm2_migration_in),
                    migration_out: Some(&vmm2_migration_out),
                },
            )
            .await
            .expect("vmm2 state should update");

        let vmm3_migration_in = nexus::MigrationRuntimeState {
            migration_id: migration2.id,
            // Let's make this fail, just for fun...
            state: nexus::MigrationState::Failed,
            r#gen: Generation::new().0.next(),
            time_updated: Utc::now(),
        };
        datastore
            .vmm_and_migration_update_runtime(
                &opctx,
                PropolisUuid::from_untyped_uuid(vmm3.id),
                &VmmRuntimeState {
                    time_state_updated: Utc::now(),
                    r#gen: Generation(vmm3.runtime.r#gen.0.next()),
                    state: VmmState::Destroyed,
                },
                Migrations {
                    migration_in: Some(&vmm3_migration_in),
                    migration_out: None,
                },
            )
            .await
            .expect("vmm3 state should update");

        let all_migrations = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_eq!(all_migrations.len(), 2);

        // the previous migration should not have closed.
        let new_db_migration1 = all_migrations
            .iter()
            .find(|m| m.id == migration1.id)
            .expect("query must include migration1");
        assert_eq!(new_db_migration1.source_state, db_migration1.source_state);
        assert_eq!(new_db_migration1.source_gen, db_migration1.source_gen);
        assert_eq!(
            db_migration1.time_source_updated,
            new_db_migration1.time_source_updated
        );
        assert_eq!(new_db_migration1.target_state, db_migration1.target_state);
        assert_eq!(new_db_migration1.target_gen, db_migration1.target_gen,);
        assert_eq!(
            new_db_migration1.time_target_updated,
            db_migration1.time_target_updated,
        );

        let db_migration2 = all_migrations
            .iter()
            .find(|m| m.id == migration2.id)
            .expect("query must include migration2");
        assert_eq!(
            db_migration2.source_state,
            db::model::MigrationState::COMPLETED
        );
        assert_eq!(
            db_migration2.target_state,
            db::model::MigrationState::FAILED
        );
        assert_eq!(
            db_migration2.source_gen,
            Generation(Generation::new().0.next()),
        );
        assert_eq!(
            db_migration2.target_gen,
            Generation(Generation::new().0.next()),
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
