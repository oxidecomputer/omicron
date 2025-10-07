// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Migration`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::Generation;
use crate::db::model::Migration;
use crate::db::model::MigrationState;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateAndQueryResult;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::migration::dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use uuid::Uuid;

impl DataStore {
    /// Insert a database record for a migration.
    pub async fn migration_insert(
        &self,
        opctx: &OpContext,
        migration: Migration,
    ) -> CreateResult<Migration> {
        diesel::insert_into(dsl::migration)
            .values(migration)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_created.eq(dsl::time_created))
            .returning(Migration::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all migrations associated with the provided instance ID.
    pub async fn instance_list_migrations(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Migration> {
        paginated(dsl::migration, dsl::id, pagparams)
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(Migration::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Mark *all* migrations for the provided instance as deleted.
    ///
    /// This should be called when deleting an instance.
    pub(crate) async fn instance_mark_migrations_deleted(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> UpdateResult<usize> {
        diesel::update(dsl::migration)
            .filter(dsl::instance_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Marks a migration record as failed.
    pub async fn migration_mark_failed(
        &self,
        opctx: &OpContext,
        migration_id: Uuid,
    ) -> UpdateResult<bool> {
        let failed = MigrationState(nexus::MigrationState::Failed);
        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration_id))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::source_state.eq(failed),
                dsl::source_gen.eq(dsl::source_gen + 1),
                dsl::time_source_updated.eq(Utc::now()),
                dsl::target_state.eq(failed),
                dsl::target_gen.eq(dsl::target_gen + 1),
                dsl::time_target_updated.eq(Utc::now()),
            ))
            .check_if_exists::<Migration>(migration_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Unconditionally mark a migration record as deleted.
    pub async fn migration_mark_deleted(
        &self,
        opctx: &OpContext,
        migration_id: Uuid,
    ) -> UpdateResult<bool> {
        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration_id))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<Migration>(migration_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub(crate) async fn migration_update_source_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        vmm_id: &PropolisUuid,
        migration: &nexus::MigrationRuntimeState,
    ) -> Result<UpdateAndQueryResult<Migration>, diesel::result::Error> {
        let generation = Generation(migration.r#gen);
        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration.migration_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::source_gen.lt(generation))
            .filter(dsl::source_propolis_id.eq(vmm_id.into_untyped_uuid()))
            .set((
                dsl::source_state.eq(MigrationState(migration.state)),
                dsl::source_gen.eq(generation),
                dsl::time_source_updated.eq(migration.time_updated),
            ))
            .check_if_exists::<Migration>(migration.migration_id)
            .execute_and_check(conn)
            .await
    }

    pub(crate) async fn migration_update_target_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        vmm_id: &PropolisUuid,
        migration: &nexus::MigrationRuntimeState,
    ) -> Result<UpdateAndQueryResult<Migration>, diesel::result::Error> {
        let generation = Generation(migration.r#gen);
        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration.migration_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::target_gen.lt(generation))
            .filter(dsl::target_propolis_id.eq(vmm_id.into_untyped_uuid()))
            .set((
                dsl::target_state.eq(MigrationState(migration.state)),
                dsl::target_gen.eq(generation),
                dsl::time_target_updated.eq(migration.time_updated),
            ))
            .check_if_exists::<Migration>(migration.migration_id)
            .execute_and_check(conn)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz;
    use crate::db::model::Instance;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::Project;
    use nexus_types::external_api::params;
    use nexus_types::silo::DEFAULT_SILO_ID;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;

    async fn create_test_instance(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> authz::Instance {
        let silo_id = DEFAULT_SILO_ID;
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
                        boot_disk: None,
                        cpu_platform: None,
                        ssh_public_keys: None,
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                    },
                ),
            )
            .await
            .expect("instance must be created successfully");

        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");
        authz_instance
    }

    async fn insert_migration(
        datastore: &DataStore,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Migration {
        let migration = Migration::new(
            Uuid::new_v4(),
            instance_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
        );

        datastore
            .migration_insert(&opctx, migration.clone())
            .await
            .expect("must insert migration successfully");

        migration
    }

    #[tokio::test]
    async fn test_migration_query_by_instance() {
        // Setup
        let logctx = dev::test_setup_log("test_migration_query_by_instance");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

        let migration1 =
            insert_migration(&datastore, &opctx, instance_id).await;
        let migration2 =
            insert_migration(&datastore, &opctx, instance_id).await;

        let list = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_all_migrations_found(&[&migration1, &migration2], &list[..]);

        let migration3 =
            insert_migration(&datastore, &opctx, instance_id).await;

        let list = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_all_migrations_found(
            &[&migration1, &migration2, &migration3],
            &list[..],
        );

        datastore
            .migration_mark_deleted(&opctx, migration3.id)
            .await
            .expect("must delete migration");
        let list = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_all_migrations_found(&[&migration1, &migration2], &list[..]);

        let deleted = datastore
            .instance_mark_migrations_deleted(&opctx, instance_id)
            .await
            .expect("must delete remaining migrations");
        assert_eq!(
            deleted, 2,
            "should not delete migration that was already marked as deleted"
        );

        let list = datastore
            .instance_list_migrations(
                &opctx,
                instance_id,
                &DataPageParams::max_page(),
            )
            .await
            .expect("list must succeed");
        assert!(list.is_empty(), "all migrations must be deleted");

        let deleted = datastore
            .instance_mark_migrations_deleted(&opctx, instance_id)
            .await
            .expect("must delete remaining migrations");
        assert_eq!(
            deleted, 0,
            "should not delete migration that was already marked as deleted"
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[track_caller]
    fn assert_all_migrations_found(
        expected: &[&Migration],
        actual: &[Migration],
    ) {
        assert_eq!(expected.len(), actual.len());
        for migration in expected {
            assert!(
                actual.iter().any(|m| m.id == migration.id),
                "couldn't find migration {:?} in {actual:#?}",
                migration.id,
            );
        }
    }
}
