// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`VirtualProvisioningCollection`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::model::ByteCount;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::queries::virtual_provisioning_collection_update::VirtualProvisioningCollectionUpdate;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::{DeleteResult, Error};
use omicron_uuid_kinds::InstanceUuid;
use uuid::Uuid;

/// The types of resources which can consume storage space.
pub enum StorageType {
    Disk,
    Snapshot,
}

impl From<StorageType> for crate::db::model::ResourceTypeProvisioned {
    fn from(
        storage_type: StorageType,
    ) -> crate::db::model::ResourceTypeProvisioned {
        match storage_type {
            StorageType::Disk => {
                crate::db::model::ResourceTypeProvisioned::Disk
            }
            StorageType::Snapshot => {
                crate::db::model::ResourceTypeProvisioned::Snapshot
            }
        }
    }
}

impl DataStore {
    /// Create a [`VirtualProvisioningCollection`] object.
    pub async fn virtual_provisioning_collection_create(
        &self,
        opctx: &OpContext,
        virtual_provisioning_collection: VirtualProvisioningCollection,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.virtual_provisioning_collection_create_on_connection(
            &conn,
            virtual_provisioning_collection,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub(crate) async fn virtual_provisioning_collection_create_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        virtual_provisioning_collection: VirtualProvisioningCollection,
    ) -> Result<Vec<VirtualProvisioningCollection>, DieselError> {
        use nexus_db_schema::schema::virtual_provisioning_collection::dsl;

        let provisions: Vec<VirtualProvisioningCollection> =
            diesel::insert_into(dsl::virtual_provisioning_collection)
                .values(virtual_provisioning_collection)
                .on_conflict_do_nothing()
                .get_results_async(conn)
                .await?;
        let _ = self
            .virtual_provisioning_collection_producer
            .append_all_metrics(&provisions);
        Ok(provisions)
    }

    pub async fn virtual_provisioning_collection_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<VirtualProvisioningCollection, Error> {
        use nexus_db_schema::schema::virtual_provisioning_collection::dsl;

        let virtual_provisioning_collection =
            dsl::virtual_provisioning_collection
                .find(id)
                .select(VirtualProvisioningCollection::as_select())
                .get_result_async(
                    &*self.pool_connection_authorized(opctx).await?,
                )
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        Ok(virtual_provisioning_collection)
    }

    /// Delete a [`VirtualProvisioningCollection`] object.
    pub async fn virtual_provisioning_collection_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.virtual_provisioning_collection_delete_on_connection(
            &opctx.log, &conn, id,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete a [`VirtualProvisioningCollection`] object.
    pub(crate) async fn virtual_provisioning_collection_delete_on_connection(
        &self,
        log: &slog::Logger,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
    ) -> Result<(), DieselError> {
        use nexus_db_schema::schema::virtual_provisioning_collection::dsl;

        // NOTE: We don't really need to extract the value we're deleting from
        // the DB, but by doing so, we can validate that we haven't
        // miscalculated our usage accounting.
        let collection = diesel::delete(dsl::virtual_provisioning_collection)
            .filter(dsl::id.eq(id))
            .returning(VirtualProvisioningCollection::as_select())
            .get_result_async(conn)
            .await?;

        if !collection.is_empty() {
            warn!(log, "Collection deleted while non-empty: {collection:?}");
            return Err(DieselError::RollbackTransaction);
        }
        Ok(())
    }

    // TODO: These could 100% act on model types:
    // - Would help with identifying UUID
    // - Would help with project ID lookup
    // - Would help with calculating resource usage
    //
    // I think we just need to validate that the model exists when we make these
    // calls? Maybe it could be an optional helper?

    pub async fn virtual_provisioning_collection_insert_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_insert_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
            StorageType::Disk,
        )
        .await
    }

    pub async fn virtual_provisioning_collection_insert_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_insert_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
            StorageType::Snapshot,
        )
        .await
    }

    /// Transitively updates all provisioned disk provisions from project -> fleet.
    async fn virtual_provisioning_collection_insert_storage(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
        storage_type: StorageType,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_insert_storage(
                id,
                disk_byte_diff,
                project_id,
                storage_type,
            )
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                crate::db::queries::virtual_provisioning_collection_update::from_diesel(e)
            })?;
        self.virtual_provisioning_collection_producer
            .append_disk_metrics(&provisions)?;
        Ok(provisions)
    }

    pub async fn virtual_provisioning_collection_delete_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_delete_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
        )
        .await
    }

    pub async fn virtual_provisioning_collection_delete_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_delete_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
        )
        .await
    }

    // Transitively updates all provisioned disk provisions from project -> fleet.
    async fn virtual_provisioning_collection_delete_storage(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_delete_storage(
                id,
                disk_byte_diff,
                project_id,
            )
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| crate::db::queries::virtual_provisioning_collection_update::from_diesel(e))?;
        self.virtual_provisioning_collection_producer
            .append_disk_metrics(&provisions)?;
        Ok(provisions)
    }

    /// Transitively updates all CPU/RAM provisions from project -> fleet.
    pub async fn virtual_provisioning_collection_insert_instance(
        &self,
        opctx: &OpContext,
        id: InstanceUuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| crate::db::queries::virtual_provisioning_collection_update::from_diesel(e))?;
        self.virtual_provisioning_collection_producer
            .append_cpu_metrics(&provisions)?;
        Ok(provisions)
    }

    /// Transitively removes the CPU and memory charges for an instance from the
    /// instance's project, silo, and fleet.
    pub async fn virtual_provisioning_collection_delete_instance(
        &self,
        opctx: &OpContext,
        id: InstanceUuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_delete_instance(
                id,
                cpus_diff,
                ram_diff,
                project_id,
            )
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| crate::db::queries::virtual_provisioning_collection_update::from_diesel(e))?;
        self.virtual_provisioning_collection_producer
            .append_cpu_metrics(&provisions)?;
        Ok(provisions)
    }

    pub async fn load_builtin_fleet_virtual_provisioning_collection(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        let id = *nexus_db_fixed_data::FLEET_ID;
        self.virtual_provisioning_collection_create(
            opctx,
            db::model::VirtualProvisioningCollection::new(
                id,
                db::model::CollectionTypeProvisioned::Fleet,
            ),
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::Instance;
    use nexus_db_model::Project;
    use nexus_db_model::SiloQuotasUpdate;
    use nexus_types::external_api::params;
    use nexus_types::silo::DEFAULT_SILO_ID;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    async fn verify_collection_usage(
        datastore: &DataStore,
        opctx: &OpContext,
        id: Uuid,
        expected_cpus: i64,
        expected_memory: i64,
        expected_storage: i64,
    ) {
        let collection = datastore
            .virtual_provisioning_collection_get(opctx, id)
            .await
            .expect("Could not lookup collection");

        assert_eq!(collection.cpus_provisioned, expected_cpus);
        assert_eq!(
            collection.ram_provisioned.0.to_bytes(),
            expected_memory as u64
        );
        assert_eq!(
            collection.virtual_disk_bytes_provisioned.0.to_bytes(),
            expected_storage as u64
        );
    }

    struct TestData {
        project_id: Uuid,
        silo_id: Uuid,
        fleet_id: Uuid,
        authz_project: crate::authz::Project,
    }

    impl TestData {
        fn ids(&self) -> [Uuid; 3] {
            [self.project_id, self.silo_id, self.fleet_id]
        }
    }

    // Use the default fleet and silo, but create a new project.
    async fn setup_collections(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> TestData {
        let fleet_id = *nexus_db_fixed_data::FLEET_ID;
        let silo_id = DEFAULT_SILO_ID;
        let project_id = Uuid::new_v4();

        let (authz_project, _project) = datastore
            .project_create(
                &opctx,
                Project::new_with_id(
                    project_id,
                    silo_id,
                    params::ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: "myproject".parse().unwrap(),
                            description: "It's a project".into(),
                        },
                    },
                ),
            )
            .await
            .unwrap();

        // Ensure the silo has a quota that can fit our requested instance.
        //
        // This also acts as a guard against a change in the default silo quota
        // -- we overwrite it for the test unconditionally.

        let quotas_update = SiloQuotasUpdate {
            cpus: Some(24),
            memory: Some((1 << 40).try_into().unwrap()),
            storage: Some((1 << 50).try_into().unwrap()),
            time_modified: chrono::Utc::now(),
        };
        let authz_silo = LookupPath::new(&opctx, datastore)
            .silo_id(silo_id)
            .lookup_for(crate::authz::Action::Modify)
            .await
            .unwrap()
            .0;
        datastore
            .silo_update_quota(&opctx, &authz_silo, quotas_update)
            .await
            .unwrap();

        TestData { fleet_id, silo_id, project_id, authz_project }
    }

    async fn create_instance_record(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &crate::authz::Project,
        instance_id: InstanceUuid,
        project_id: Uuid,
        cpus: i64,
        memory: ByteCount,
    ) {
        datastore
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
                        ncpus: cpus.try_into().unwrap(),
                        memory: memory.try_into().unwrap(),
                        hostname: "myhostname".try_into().unwrap(),
                        user_data: Vec::new(),
                        network_interfaces:
                            params::InstanceNetworkInterfaceAttachment::None,
                        external_ips: Vec::new(),
                        disks: Vec::new(),
                        boot_disk: None,
                        ssh_public_keys: None,
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                    },
                ),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_instance_create_and_delete() {
        let logctx = dev::test_setup_log("test_instance_create_and_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = test_data.authz_project;

        // Actually provision the instance

        let instance_id = InstanceUuid::new_v4();
        let cpus = 12;
        let ram = ByteCount::try_from(1 << 30).unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        create_instance_record(
            &datastore,
            &opctx,
            &authz_project,
            instance_id,
            project_id,
            cpus,
            ram,
        )
        .await;

        datastore
            .virtual_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 12, 1 << 30, 0)
                .await;
        }

        // Delete the instance

        datastore
            .virtual_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_create_and_delete_twice() {
        let logctx =
            dev::test_setup_log("test_instance_create_and_delete_twice");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = test_data.authz_project;

        // Actually provision the instance

        let instance_id = InstanceUuid::new_v4();
        let cpus = 12;
        let ram = ByteCount::try_from(1 << 30).unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        create_instance_record(
            &datastore,
            &opctx,
            &authz_project,
            instance_id,
            project_id,
            cpus,
            ram,
        )
        .await;

        datastore
            .virtual_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 12, 1 << 30, 0)
                .await;
        }

        // Attempt to provision that same instance once more.
        //
        // The "virtual_provisioning_collection_insert" call should succeed for
        // idempotency reasons, but we should not be double-dipping on the
        // instance object's provisioning accounting.

        datastore
            .virtual_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        // Verify that the usage is the same as before the call
        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 12, 1 << 30, 0)
                .await;
        }

        // Delete the instance

        datastore
            .virtual_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        // Attempt to delete the same instance once more.
        //
        // Just like "double-adding", double deletion should be an idempotent
        // operation.

        datastore
            .virtual_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_storage_create_and_delete() {
        let logctx = dev::test_setup_log("test_storage_create_and_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        // Actually provision storage

        let disk_id = Uuid::new_v4();
        let disk_byte_diff = ByteCount::try_from(1 << 30).unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        datastore
            .virtual_provisioning_collection_insert_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
                StorageType::Disk,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 1 << 30)
                .await;
        }

        // Delete the disk

        datastore
            .virtual_provisioning_collection_delete_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_storage_create_and_delete_twice() {
        let logctx =
            dev::test_setup_log("test_storage_create_and_delete_twice");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        // Actually provision the disk

        let disk_id = Uuid::new_v4();
        let disk_byte_diff = ByteCount::try_from(1 << 30).unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        datastore
            .virtual_provisioning_collection_insert_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
                StorageType::Disk,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 1 << 30)
                .await;
        }

        // Attempt to provision that same disk once more.
        //
        // The "virtual_provisioning_collection_insert" call should succeed for
        // idempotency reasons, but we should not be double-dipping on the
        // disk object's provisioning accounting.

        datastore
            .virtual_provisioning_collection_insert_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
                StorageType::Disk,
            )
            .await
            .unwrap();

        // Verify that the usage is the same as before the call
        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 1 << 30)
                .await;
        }

        // Delete the disk

        datastore
            .virtual_provisioning_collection_delete_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        // Attempt to delete the same disk once more.
        //
        // Just like "double-adding", double deletion should be an idempotent
        // operation.

        datastore
            .virtual_provisioning_collection_delete_storage(
                &opctx,
                disk_id,
                project_id,
                disk_byte_diff,
            )
            .await
            .unwrap();

        for id in ids {
            verify_collection_usage(&datastore, &opctx, id, 0, 0, 0).await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
