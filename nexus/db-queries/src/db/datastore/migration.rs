// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Migration`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::{Migration, MigrationState};
use crate::db::pagination::paginated;
use crate::db::schema::migration::dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
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
    pub async fn migration_list_by_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Migration> {
        paginated(dsl::migration, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .select(Migration::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Marks a migration record as deleted if and only if both sides of the
    /// migration are in a terminal state.
    pub async fn migration_terminate(
        &self,
        opctx: &OpContext,
        migration_id: Uuid,
    ) -> UpdateResult<bool> {
        const TERMINAL_STATES: &[MigrationState] = &[
            MigrationState(nexus::MigrationState::Completed),
            MigrationState(nexus::MigrationState::Failed),
        ];

        diesel::update(dsl::migration)
            .filter(dsl::id.eq(migration_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::source_state.eq_any(TERMINAL_STATES))
            .filter(dsl::target_state.eq_any(TERMINAL_STATES))
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

    /// Unconditionally mark a migration record as deleted.
    ///
    /// This is distinct from [`DataStore::migration_terminate`], as it will
    /// mark a migration as deleted regardless of the states of the source and
    /// target VMMs.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::lookup::LookupPath;
    use crate::db::model::Instance;
    use nexus_db_model::Project;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;

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
    async fn test_migration_list_by_instance() {
        // Setup
        let logctx = dev::test_setup_log("test_instance_fetch_all");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let authz_instance = create_test_instance(&datastore, &opctx).await;
        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());
        let migration1 = Migration::new(
            Uuid::new_v4(),
            instance_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        datastore
            .migration_insert(&opctx, migration1.clone())
            .await
            .expect("must insert migration 1");
        let migration2 = Migration::new(
            Uuid::new_v4(),
            instance_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        datastore
            .migration_insert(&opctx, migration2.clone())
            .await
            .expect("must insert migration 2");

        let list = datastore
            .migration_list_by_instance(
                &opctx,
                &authz_instance,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_all_migrations_found(&[&migration1, &migration2], &list[..]);

        let migration3 = Migration::new(
            Uuid::new_v4(),
            instance_id,
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        datastore
            .migration_insert(&opctx, migration3.clone())
            .await
            .expect("must insert migration 3");

        let list = datastore
            .migration_list_by_instance(
                &opctx,
                &authz_instance,
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
            .migration_list_by_instance(
                &opctx,
                &authz_instance,
                &DataPageParams::max_page(),
            )
            .await
            .expect("must list migrations");
        assert_all_migrations_found(&[&migration1, &migration2], &list[..]);

        // Clean up.
        db.cleanup().await.unwrap();
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
