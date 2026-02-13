// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`PhysicalProvisioningCollection`]s.

use super::DataStore;
use super::StorageType;
use crate::context::OpContext;
use crate::db;
use crate::db::model::ByteCount;
use crate::db::model::PhysicalProvisioningCollection;
use crate::db::model::PhysicalProvisioningCollectionNew;
use crate::db::queries::physical_provisioning_collection_update::DedupInfo;
use crate::db::queries::physical_provisioning_collection_update::PhysicalDiskBytes;
use crate::db::queries::physical_provisioning_collection_update::PhysicalProvisioningCollectionUpdate;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::{DeleteResult, Error};
use omicron_uuid_kinds::InstanceUuid;
use uuid::Uuid;

impl DataStore {
    /// Create a [`PhysicalProvisioningCollection`] object.
    pub async fn physical_provisioning_collection_create(
        &self,
        opctx: &OpContext,
        physical_provisioning_collection: PhysicalProvisioningCollectionNew,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.physical_provisioning_collection_create_on_connection(
            &conn,
            physical_provisioning_collection,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub(crate) async fn physical_provisioning_collection_create_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        physical_provisioning_collection: PhysicalProvisioningCollectionNew,
    ) -> Result<Vec<PhysicalProvisioningCollection>, DieselError> {
        use nexus_db_schema::schema::physical_provisioning_collection::dsl;

        diesel::insert_into(dsl::physical_provisioning_collection)
            .values(physical_provisioning_collection)
            .on_conflict_do_nothing()
            .get_results_async(conn)
            .await
    }

    pub async fn physical_provisioning_collection_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<PhysicalProvisioningCollection, Error> {
        use nexus_db_schema::schema::physical_provisioning_collection::dsl;

        let physical_provisioning_collection =
            dsl::physical_provisioning_collection
                .find(id)
                .select(PhysicalProvisioningCollection::as_select())
                .get_result_async(
                    &*self.pool_connection_authorized(opctx).await?,
                )
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        Ok(physical_provisioning_collection)
    }

    /// Delete a [`PhysicalProvisioningCollection`] object.
    pub async fn physical_provisioning_collection_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.physical_provisioning_collection_delete_on_connection(
            &opctx.log, &conn, id,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete a [`PhysicalProvisioningCollection`] object.
    pub(crate) async fn physical_provisioning_collection_delete_on_connection(
        &self,
        log: &slog::Logger,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
    ) -> Result<(), DieselError> {
        use nexus_db_schema::schema::physical_provisioning_collection::dsl;

        // NOTE: We don't really need to extract the value we're deleting from
        // the DB, but by doing so, we can validate that we haven't
        // miscalculated our usage accounting.
        let collection = diesel::delete(dsl::physical_provisioning_collection)
            .filter(dsl::id.eq(id))
            .returning(PhysicalProvisioningCollection::as_select())
            .get_result_async(conn)
            .await?;

        if !collection.is_empty() {
            warn!(
                log,
                "Physical collection deleted while non-empty: {collection:?}"
            );
            return Err(DieselError::RollbackTransaction);
        }
        Ok(())
    }

    /// Insert disk physical provisioning.
    ///
    /// When `dedup` is `Some`, the CTE atomically checks whether another
    /// disk in the same project/silo already references the same origin
    /// image/snapshot. If so, the read_only diff is zeroed out at the
    /// appropriate level.
    pub async fn physical_provisioning_collection_insert_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        writable_phys: ByteCount,
        read_only_phys: ByteCount,
        dedup: Option<DedupInfo>,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let bytes = PhysicalDiskBytes {
            writable: writable_phys,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_insert_storage(
                id,
                bytes,
                bytes,
                project_id,
                StorageType::Disk,
                dedup,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Delete disk physical provisioning.
    ///
    /// When `dedup` is `Some`, the CTE atomically checks whether the
    /// given origin is still referenced. If so, the read_only diff is
    /// zeroed out at the appropriate level.
    pub async fn physical_provisioning_collection_delete_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        writable_phys: ByteCount,
        read_only_phys: ByteCount,
        dedup: Option<DedupInfo>,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let bytes = PhysicalDiskBytes {
            writable: writable_phys,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_delete_storage(
                id,
                bytes,
                bytes,
                project_id,
                dedup,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    pub async fn physical_provisioning_collection_insert_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        project: PhysicalDiskBytes,
        silo: PhysicalDiskBytes,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_insert_storage(
                id,
                project,
                silo,
                project_id,
                StorageType::Snapshot,
                None,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Delete physical provisioning for a snapshot.
    ///
    /// When `dedup` is `Some`, the CTE atomically checks whether any
    /// surviving disk still references this snapshot. If so, the
    /// `read_only` charge is not subtracted.
    pub async fn physical_provisioning_collection_delete_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        project: PhysicalDiskBytes,
        silo: PhysicalDiskBytes,
        dedup: Option<DedupInfo>,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_delete_storage(
                id,
                project,
                silo,
                project_id,
                dedup,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Transitively updates all physical CPU/RAM provisions from project ->
    /// fleet.
    pub async fn physical_provisioning_collection_insert_instance(
        &self,
        opctx: &OpContext,
        id: InstanceUuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Transitively removes the CPU and memory charges for an instance from
    /// the instance's project, silo, and fleet.
    pub async fn physical_provisioning_collection_delete_instance(
        &self,
        opctx: &OpContext,
        id: InstanceUuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_delete_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Look up the `DiskTypeCrucible` record for a given disk.
    /// Returns `None` for non-Crucible (local storage) disks.
    pub async fn disk_type_crucible_lookup_optional(
        &self,
        opctx: &OpContext,
        disk_id: Uuid,
    ) -> Result<Option<db::model::DiskTypeCrucible>, Error> {
        use nexus_db_schema::schema::disk_type_crucible::dsl;

        dsl::disk_type_crucible
            .filter(dsl::disk_id.eq(disk_id))
            .select(db::model::DiskTypeCrucible::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert physical provisioning for a project-scoped image.
    ///
    /// Charges `(0, 0, read_only)` at project, silo, and fleet levels.
    pub async fn physical_provisioning_collection_insert_image(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        read_only_phys: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_insert_storage(
                id,
                bytes,
                bytes,
                project_id,
                StorageType::Image,
                None,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Insert physical provisioning for a silo-scoped image.
    ///
    /// Charges `(0, 0, read_only)` at silo and fleet levels only.
    pub async fn physical_provisioning_collection_insert_image_silo(
        &self,
        opctx: &OpContext,
        id: Uuid,
        silo_id: Uuid,
        read_only_phys: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_insert_storage_silo_level(
                id,
                bytes,
                silo_id,
                StorageType::Image,
                None,
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Delete physical provisioning for a project-scoped image, with
    /// dedup against surviving disks that reference this image.
    pub async fn physical_provisioning_collection_delete_image(
        &self,
        opctx: &OpContext,
        image_id: Uuid,
        project_id: Uuid,
        read_only_phys: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let dedup = DedupInfo::ImageDelete { image_id };
        let bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_delete_storage(
                image_id,
                bytes,
                bytes,
                project_id,
                Some(dedup),
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    /// Delete physical provisioning for a silo-scoped image, with
    /// dedup against surviving disks that reference this image.
    pub async fn physical_provisioning_collection_delete_image_silo(
        &self,
        opctx: &OpContext,
        image_id: Uuid,
        silo_id: Uuid,
        read_only_phys: ByteCount,
    ) -> Result<Vec<PhysicalProvisioningCollection>, Error> {
        let zero: ByteCount = 0.try_into().unwrap();
        let dedup = DedupInfo::ImageDelete { image_id };
        let bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: zero,
            read_only: read_only_phys,
        };
        let provisions =
            PhysicalProvisioningCollectionUpdate::new_delete_storage_silo_level(
                image_id,
                bytes,
                silo_id,
                Some(dedup),
            )
            .get_results_async(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| {
                crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
            })?;
        Ok(provisions)
    }

    pub async fn load_builtin_fleet_physical_provisioning_collection(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        let id = *nexus_db_fixed_data::FLEET_ID;
        self.physical_provisioning_collection_create(
            opctx,
            db::model::PhysicalProvisioningCollectionNew::new(
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

    async fn verify_physical_collection_usage(
        datastore: &DataStore,
        opctx: &OpContext,
        id: Uuid,
        expected_cpus: i64,
        expected_memory: i64,
        expected_writable: i64,
        expected_zfs_snapshot: i64,
        expected_read_only: i64,
    ) {
        let collection = datastore
            .physical_provisioning_collection_get(opctx, id)
            .await
            .expect("Could not lookup physical collection");

        assert_eq!(
            collection.cpus_provisioned, expected_cpus,
            "cpus_provisioned mismatch for collection {id}"
        );
        assert_eq!(
            collection.ram_provisioned.0.to_bytes(),
            expected_memory as u64,
            "ram_provisioned mismatch for collection {id}"
        );
        assert_eq!(
            collection.physical_writable_disk_bytes.0.to_bytes(),
            expected_writable as u64,
            "physical_writable_disk_bytes mismatch for collection {id}"
        );
        assert_eq!(
            collection.physical_zfs_snapshot_bytes.0.to_bytes(),
            expected_zfs_snapshot as u64,
            "physical_zfs_snapshot_bytes mismatch for collection {id}"
        );
        assert_eq!(
            collection.physical_read_only_disk_bytes.0.to_bytes(),
            expected_read_only as u64,
            "physical_read_only_disk_bytes mismatch for collection {id}"
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
                            name: "myphysproject".parse().unwrap(),
                            description: "It's a project".into(),
                        },
                    },
                ),
            )
            .await
            .unwrap();

        // Ensure the silo has a quota that can fit our requested instance.
        let quotas_update = SiloQuotasUpdate {
            cpus: Some(24),
            memory: Some((1 << 40).try_into().unwrap()),
            storage: Some((1 << 50).try_into().unwrap()),
            physical_storage_bytes: None,
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
                            name: "myphysinstance".parse().unwrap(),
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
                        cpu_platform: None,
                        ssh_public_keys: None,
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                        multicast_groups: Vec::new(),
                    },
                ),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_physical_instance_create_and_delete() {
        let logctx =
            dev::test_setup_log("test_physical_instance_create_and_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = test_data.authz_project;

        let instance_id = InstanceUuid::new_v4();
        let cpus = 12;
        let ram = ByteCount::try_from(1 << 30).unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
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
            .physical_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                12,
                1 << 30,
                0,
                0,
                0,
            )
            .await;
        }

        // Delete the instance
        datastore
            .physical_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_physical_instance_create_and_delete_twice() {
        let logctx = dev::test_setup_log(
            "test_physical_instance_create_and_delete_twice",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = test_data.authz_project;

        let instance_id = InstanceUuid::new_v4();
        let cpus = 12;
        let ram = ByteCount::try_from(1 << 30).unwrap();

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
            .physical_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                12,
                1 << 30,
                0,
                0,
                0,
            )
            .await;
        }

        // Attempt to provision that same instance once more (idempotent).
        datastore
            .physical_provisioning_collection_insert_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                12,
                1 << 30,
                0,
                0,
                0,
            )
            .await;
        }

        // Delete the instance
        datastore
            .physical_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Double delete (idempotent).
        datastore
            .physical_provisioning_collection_delete_instance(
                &opctx,
                instance_id,
                project_id,
                cpus,
                ram,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_physical_storage_create_and_delete() {
        let logctx =
            dev::test_setup_log("test_physical_storage_create_and_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        let disk_id = Uuid::new_v4();
        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Insert disk: writable at all levels, no snapshot/read-only
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                1 << 30,
                0,
                0,
            )
            .await;
        }

        // Delete the disk
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_physical_storage_create_and_delete_twice() {
        let logctx = dev::test_setup_log(
            "test_physical_storage_create_and_delete_twice",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        let disk_id = Uuid::new_v4();
        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();

        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                1 << 30,
                0,
                0,
            )
            .await;
        }

        // Double insert (idempotent)
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                1 << 30,
                0,
                0,
            )
            .await;
        }

        // Delete the disk
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Double delete (idempotent)
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_physical_snapshot_create_and_delete() {
        let logctx =
            dev::test_setup_log("test_physical_snapshot_create_and_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        let snapshot_id = Uuid::new_v4();
        // Snapshot charges both zfs_snapshot AND read_only (2x pre-accounting).
        let phys_bytes: ByteCount = (1 << 30).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Insert snapshot: zfs_snapshot + read_only at all levels
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        let p = phys_bytes.0.to_bytes() as i64;
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Delete the snapshot (no surviving disks, so no dedup needed —
        // but we use the deduped variant for correctness).
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn create_disk_with_origin_image(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &crate::authz::Project,
        disk_id: Uuid,
        project_id: Uuid,
        image_id: Uuid,
    ) {
        use crate::db;
        use nexus_db_model::DiskRuntimeState;
        use omicron_common::api::external::ByteCount as ExtByteCount;
        use omicron_uuid_kinds::VolumeUuid;

        let disk_source =
            params::DiskSource::Image { image_id, read_only: true };
        let create_params = params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: format!("disk-{disk_id}").parse().unwrap(),
                description: "test disk from image".into(),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: disk_source.clone(),
            },
            size: ExtByteCount::from(2147483648u32),
        };
        // Read-only disks from images begin life in `Detached` state
        // (since the volume already exists), not `Creating`.
        let runtime_initial = DiskRuntimeState::new().detach();
        let disk = db::model::Disk::new(
            disk_id,
            project_id,
            &create_params,
            db::model::BlockSize::Traditional,
            runtime_initial,
            db::model::DiskType::Crucible,
        );
        let dtc = db::model::DiskTypeCrucible::new(
            disk_id,
            VolumeUuid::new_v4(),
            &disk_source,
        );
        datastore
            .project_create_disk(
                opctx,
                authz_project,
                db::datastore::Disk::Crucible(db::datastore::CrucibleDisk {
                    disk,
                    disk_type_crucible: dtc,
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_physical_dedup_same_image_two_disks() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_dedup_same_image_two_disks");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let image_id = Uuid::new_v4();
        let disk1_id = Uuid::new_v4();
        let disk2_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();

        // Verify everything starts at zero.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Create disk1 record, then insert its provisioning.
        // Disk records must be created before provisioning so the dedup
        // CTE can find them in disk_type_crucible.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project,
            disk1_id,
            project_id,
            image_id,
        )
        .await;

        // Insert disk1 physical provisioning (deduped).
        // First reference to the image — read_only should be charged.
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk1_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk1_id,
                }),
            )
            .await
            .unwrap();

        let ro_bytes = read_only.0.to_bytes() as i64;
        let w_bytes = writable.0.to_bytes() as i64;

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w_bytes, 0, ro_bytes,
            )
            .await;
        }

        // Create disk2 record, then insert its provisioning.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project,
            disk2_id,
            project_id,
            image_id,
        )
        .await;

        // Insert disk2 physical provisioning (deduped).
        // Second reference to the same image — read_only should be deduped
        // (not doubled), writable still accumulates.
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk2_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk2_id,
                }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                w_bytes * 2, // writable accumulates
                0,
                ro_bytes, // read_only deduped — still just one charge
            )
            .await;
        }

        // Delete disk2 provisioning (deduped). disk1 still references
        // the image, so read_only should NOT be subtracted.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk2_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk2_id,
                }),
            )
            .await
            .unwrap();

        // Soft-delete disk2's record so the dedup CTE for disk1's
        // delete won't find it as a live reference.
        datastore
            .project_delete_disk_no_auth(
                &disk2_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w_bytes, 0, ro_bytes,
            )
            .await;
        }

        // Delete disk1 provisioning (deduped). Last reference removed
        // — read_only should now be subtracted.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk1_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk1_id,
                }),
            )
            .await
            .unwrap();

        // Soft-delete disk1's record too.
        datastore
            .project_delete_disk_no_auth(
                &disk1_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Setup a second project in the same silo. Returns the project ID and
    /// authz handle. The physical provisioning collection is created
    /// automatically by `project_create`.
    async fn setup_second_project(
        datastore: &DataStore,
        opctx: &OpContext,
        silo_id: Uuid,
        name: &str,
    ) -> (Uuid, crate::authz::Project) {
        let project_id = Uuid::new_v4();
        let (authz_project, _project) = datastore
            .project_create(
                opctx,
                Project::new_with_id(
                    project_id,
                    silo_id,
                    params::ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name.parse().unwrap(),
                            description: "second project".into(),
                        },
                    },
                ),
            )
            .await
            .unwrap();
        (project_id, authz_project)
    }

    /// Create an image table record with a known image_id.
    async fn create_image_record(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &crate::authz::Project,
        image_id: Uuid,
        silo_id: Uuid,
        project_id: Uuid,
    ) {
        use chrono::Utc;
        use nexus_db_model::BlockSize;
        use nexus_db_model::ProjectImage;
        use nexus_db_model::ProjectImageIdentity;
        use omicron_common::api::external;
        use omicron_uuid_kinds::VolumeUuid;

        datastore
            .project_image_create(
                opctx,
                authz_project,
                ProjectImage {
                    identity: ProjectImageIdentity {
                        id: image_id,
                        name: external::Name::try_from(format!(
                            "img-{image_id}"
                        ))
                        .unwrap()
                        .into(),
                        description: "test image".into(),
                        time_created: Utc::now(),
                        time_modified: Utc::now(),
                        time_deleted: None,
                    },
                    silo_id,
                    project_id,
                    volume_id: VolumeUuid::new_v4().into(),
                    url: None,
                    os: "debian".into(),
                    version: "12".into(),
                    digest: None,
                    block_size: BlockSize::Iso,
                    size: external::ByteCount::from_gibibytes_u32(1).into(),
                },
            )
            .await
            .unwrap();
    }

    // ---------------------------------------------------------------
    // Gap 1: Cross-project dedup within the same silo
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_dedup_cross_project_same_silo() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_dedup_cross_project_same_silo");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_a_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project_a = &test_data.authz_project;

        // Setup project B in the same silo.
        let (project_b_id, authz_project_b) =
            setup_second_project(&datastore, &opctx, silo_id, "projectb").await;

        let image_id = Uuid::new_v4();
        let disk_a_id = Uuid::new_v4();
        let disk_b_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Verify baseline is zero everywhere.
        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Step 1: Create disk A in project A from the image.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project_a,
            disk_a_id,
            project_a_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();

        // After disk A: all levels get writable + read_only.
        for id in [project_a_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }
        // Project B still zero.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;

        // Step 2: Create disk B in project B from the SAME image.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            &authz_project_b,
            disk_b_id,
            project_b_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();

        // Project A: unchanged (writable + read_only).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Project B: gets writable + read_only (no project-level dedup
        // because disk A is in a different project).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: writable accumulates (2×w), but read_only is deduped
        // (disk A already exists in this silo for the same image).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;

        // Step 3: Delete disk B provisioning. disk A still alive in the
        // silo, so silo/fleet read_only should NOT be subtracted.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_b_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project B: writable subtracted, and read_only subtracted because
        // no other disk in project B references the image.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        // Project A: unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: writable goes back to 1×w, read_only stays (disk A
        // still alive).
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, w, 0, ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, fleet_id, 0, 0, w, 0, ro,
        )
        .await;

        // Step 4: Delete disk A provisioning. Last reference removed.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_a_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Everything back to zero.
        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Gap 2: Image promote/demote accounting
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_image_promote_demote_accounting() {
        use crate::db::queries::physical_provisioning_collection_update::PhysicalProvisioningCollectionUpdate;

        let logctx = dev::test_setup_log(
            "test_physical_image_promote_demote_accounting",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;

        let image_id = Uuid::new_v4();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Insert project-level image PPR -> all three levels get
        // read_only.
        datastore
            .physical_provisioning_collection_insert_image(
                &opctx, image_id, project_id, read_only,
            )
            .await
            .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Step 2: Simulate promote step 1 — delete from project-level CTE.
        // This subtracts read_only from project, silo, fleet.
        PhysicalProvisioningCollectionUpdate::new_delete_storage(
            image_id,
            PhysicalDiskBytes { writable: zero, zfs_snapshot: zero, read_only },
            PhysicalDiskBytes { writable: zero, zfs_snapshot: zero, read_only },
            project_id,
            None,
        )
        .get_results_async::<PhysicalProvisioningCollection>(
            &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
        )
        .await
        .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Step 3: Simulate promote step 2 — insert at silo level.
        // This adds read_only to silo and fleet only; project stays 0.
        datastore
            .physical_provisioning_collection_insert_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        // After promote: project = 0, silo/fleet = read_only.
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, 0, 0, 0,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, 0, 0, ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, fleet_id, 0, 0, 0, 0, ro,
        )
        .await;

        // Step 4: Simulate demote step 1 — delete from silo-level CTE.
        // Uses the direct CTE call (same as image.rs:281-294).
        let image_bytes =
            PhysicalDiskBytes { writable: zero, zfs_snapshot: zero, read_only };
        PhysicalProvisioningCollectionUpdate::new_delete_storage_silo_level(
            image_id, image_bytes, silo_id, None,
        )
        .get_results_async::<PhysicalProvisioningCollection>(
            &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
        )
        .await
        .map_err(|e| {
            crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
        })
        .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Step 5: Simulate demote step 2 — re-insert at project level.
        datastore
            .physical_provisioning_collection_insert_image(
                &opctx, image_id, project_id, read_only,
            )
            .await
            .unwrap();

        // After demote: all three levels get read_only again.
        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Gap 3: Snapshot insertion fails when physical storage quota exceeded
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_snapshot_quota_failure() {
        let logctx =
            dev::test_setup_log("test_physical_snapshot_quota_failure");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;

        let snapshot_id = Uuid::new_v4();
        // 2x pre-accounting: both zfs_snapshot and read_only
        let phys_bytes: ByteCount = (256 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();

        // Set a tight physical storage quota (100 bytes).
        let authz_silo = LookupPath::new(&opctx, &*datastore)
            .silo_id(silo_id)
            .lookup_for(crate::authz::Action::Modify)
            .await
            .unwrap()
            .0;
        datastore
            .silo_update_quota(
                &opctx,
                &authz_silo,
                SiloQuotasUpdate {
                    cpus: None,
                    memory: None,
                    storage: None,
                    physical_storage_bytes: Some(Some(100)),
                    time_modified: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Attempt insert — should fail with InsufficientCapacity.
        // The 2x charge (zfs_snapshot + read_only) exceeds the quota.
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        let result = datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await;
        assert!(
            matches!(
                result,
                Err(omicron_common::api::external::Error::InsufficientCapacity {
                    ..
                })
            ),
            "Expected InsufficientCapacity, got: {:?}",
            result
        );

        // Collections unchanged — still zero.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Raise the quota to something generous.
        datastore
            .silo_update_quota(
                &opctx,
                &authz_silo,
                SiloQuotasUpdate {
                    cpus: None,
                    memory: None,
                    storage: None,
                    physical_storage_bytes: Some(Some(1 << 40)),
                    time_modified: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Retry — should succeed now.
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        let p = phys_bytes.0.to_bytes() as i64;
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Cleanup: delete the snapshot provisioning.
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Gap 4: Image PPR entry causes disk read_only dedup via OR EXISTS
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_dedup_image_ppr_entry() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx = dev::test_setup_log("test_physical_dedup_image_ppr_entry");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let image_id = Uuid::new_v4();
        let disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create image DB record (needed for CTE's OR EXISTS join).
        create_image_record(
            &datastore,
            &opctx,
            authz_project,
            image_id,
            test_data.silo_id,
            project_id,
        )
        .await;

        // Step 2: Insert image PPR -> read_only charged at all levels.
        datastore
            .physical_provisioning_collection_insert_image(
                &opctx, image_id, project_id, read_only,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Step 3: Create disk from that image -> writable accumulates,
        // read_only is deduped because the image's PPR entry exists (the
        // CTE's OR EXISTS clause detects it).
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project,
            disk_id,
            project_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();

        // read_only should still be ro (not 2*ro) — dedup via PPR entry.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 4: Delete disk provisioning. Image PPR still exists, so
        // read_only should NOT be subtracted from any level.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Step 5: Verify back to just the image's PPR charge.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Gap 5: Silo-level image insert/delete lifecycle
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_silo_image_lifecycle() {
        use crate::db::queries::physical_provisioning_collection_update::PhysicalProvisioningCollectionUpdate;

        let logctx = dev::test_setup_log("test_physical_silo_image_lifecycle");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;

        let image_id = Uuid::new_v4();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let ro = read_only.0.to_bytes() as i64;

        // Baseline: all zero.
        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Insert silo-level image -> project stays 0, silo/fleet get
        // read_only.
        datastore
            .physical_provisioning_collection_insert_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, 0, 0, 0,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, 0, 0, ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, fleet_id, 0, 0, 0, 0, ro,
        )
        .await;

        // Delete via silo-level CTE -> all back to zero.
        let image_bytes =
            PhysicalDiskBytes { writable: zero, zfs_snapshot: zero, read_only };
        PhysicalProvisioningCollectionUpdate::new_delete_storage_silo_level(
            image_id, image_bytes, silo_id, None,
        )
        .get_results_async::<PhysicalProvisioningCollection>(
            &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
        )
        .await
        .map_err(|e| {
            crate::db::queries::physical_provisioning_collection_update::from_diesel(e)
        })
        .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Gap 6: ImageDelete dedup across multiple projects
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_image_delete_dedup_multi_project() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx = dev::test_setup_log(
            "test_physical_image_delete_dedup_multi_project",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_a_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project_a = &test_data.authz_project;

        let (project_b_id, authz_project_b) =
            setup_second_project(&datastore, &opctx, silo_id, "projectb2")
                .await;

        let image_id = Uuid::new_v4();
        let disk_a_id = Uuid::new_v4();
        let disk_b_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create image record and insert image PPR in project A.
        create_image_record(
            &datastore,
            &opctx,
            authz_project_a,
            image_id,
            silo_id,
            project_a_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_image(
                &opctx,
                image_id,
                project_a_id,
                read_only,
            )
            .await
            .unwrap();

        // Step 2: Create disk A in project A from image (deduped — image
        // PPR exists, so read_only is deduped).
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project_a,
            disk_a_id,
            project_a_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();

        // Project A: writable + read_only (deduped via image PPR).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;

        // Step 3: Create disk B in project B from same image (deduped).
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            &authz_project_b,
            disk_b_id,
            project_b_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();

        // Project B: writable + read_only (no project-level dedup — image
        // PPR is in project A, not B, and no other disk in B).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: 2×w writable, but read_only stays at ro (deduped).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;

        // Step 4: Delete image PPR via ImageDelete dedup. Surviving disks
        // A and B should prevent read_only subtraction.
        datastore
            .physical_provisioning_collection_delete_image(
                &opctx,
                image_id,
                project_a_id,
                read_only,
            )
            .await
            .unwrap();

        // Project A: the image PPR read_only is NOT subtracted (disk A
        // survives with origin_image = image_id). Writable unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Project B unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: read_only still ro (surviving disks detected).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;

        // Step 5: Delete disk A. Project A goes to zero, silo/fleet
        // read_only stays (disk B in project B still alive).
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_a_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project A: writable subtracted, read_only subtracted (no other
        // disk or image PPR in project A references this image).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        // Project B: unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: writable = w, read_only stays (disk B alive).
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, w, 0, ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, fleet_id, 0, 0, w, 0, ro,
        )
        .await;

        // Step 6: Delete disk B. Everything goes to zero.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_b_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Snapshot helpers
    // ---------------------------------------------------------------

    /// Create a snapshot table record with a known snapshot_id.
    async fn create_snapshot_record(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &crate::authz::Project,
        snapshot_id: Uuid,
        project_id: Uuid,
        disk_id: Uuid,
    ) {
        use chrono::Utc;
        use nexus_db_model::BlockSize;
        use nexus_db_model::Snapshot;
        use nexus_db_model::SnapshotIdentity;
        use nexus_db_model::SnapshotState;
        use omicron_common::api::external;
        use omicron_uuid_kinds::VolumeUuid;

        // Must create in Creating state (project_ensure_snapshot asserts
        // this), then transition to Ready.
        let snapshot = datastore
            .project_ensure_snapshot(
                opctx,
                authz_project,
                Snapshot {
                    identity: SnapshotIdentity {
                        id: snapshot_id,
                        name: external::Name::try_from(format!(
                            "snap-{snapshot_id}"
                        ))
                        .unwrap()
                        .into(),
                        description: "test snapshot".into(),
                        time_created: Utc::now(),
                        time_modified: Utc::now(),
                        time_deleted: None,
                    },
                    project_id,
                    disk_id,
                    volume_id: VolumeUuid::new_v4().into(),
                    destination_volume_id: VolumeUuid::new_v4().into(),
                    generation: nexus_db_model::Generation::new(),
                    state: SnapshotState::Creating,
                    block_size: BlockSize::Traditional,
                    size: external::ByteCount::from_gibibytes_u32(1).into(),
                },
            )
            .await
            .unwrap();

        // Transition to Ready so it looks like a real completed snapshot.
        let (.., authz_snapshot) = LookupPath::new(opctx, datastore)
            .snapshot_id(snapshot_id)
            .lookup_for(crate::authz::Action::Modify)
            .await
            .unwrap();
        datastore
            .project_snapshot_update_state(
                opctx,
                &authz_snapshot,
                snapshot.generation,
                SnapshotState::Ready,
            )
            .await
            .unwrap();
    }

    /// Create a disk record whose origin is a snapshot.
    async fn create_disk_with_origin_snapshot(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &crate::authz::Project,
        disk_id: Uuid,
        project_id: Uuid,
        snapshot_id: Uuid,
    ) {
        use crate::db;
        use nexus_db_model::DiskRuntimeState;
        use omicron_common::api::external::ByteCount as ExtByteCount;
        use omicron_uuid_kinds::VolumeUuid;

        let disk_source =
            params::DiskSource::Snapshot { snapshot_id, read_only: true };
        let create_params = params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: format!("disk-{disk_id}").parse().unwrap(),
                description: "test disk from snapshot".into(),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: disk_source.clone(),
            },
            size: ExtByteCount::from(2147483648u32),
        };
        let runtime_initial = DiskRuntimeState::new().detach();
        let disk = db::model::Disk::new(
            disk_id,
            project_id,
            &create_params,
            db::model::BlockSize::Traditional,
            runtime_initial,
            db::model::DiskType::Crucible,
        );
        let dtc = db::model::DiskTypeCrucible::new(
            disk_id,
            VolumeUuid::new_v4(),
            &disk_source,
        );
        datastore
            .project_create_disk(
                opctx,
                authz_project,
                db::datastore::Disk::Crucible(db::datastore::CrucibleDisk {
                    disk,
                    disk_type_crucible: dtc,
                }),
            )
            .await
            .unwrap();
    }

    // ---------------------------------------------------------------
    // Snapshot PPR dedup: disk from snapshot with snapshot's PPR entry
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_dedup_snapshot_ppr_entry() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_dedup_snapshot_ppr_entry");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let snapshot_id = Uuid::new_v4();
        let source_disk_id = Uuid::new_v4();
        let disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let phys_bytes: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let p = phys_bytes.0.to_bytes() as i64;

        // Step 1: Create snapshot DB record (needed for CTE's OR EXISTS
        // join).
        create_snapshot_record(
            &datastore,
            &opctx,
            authz_project,
            snapshot_id,
            project_id,
            source_disk_id,
        )
        .await;

        // Step 2: Insert snapshot PPR -> zfs_snapshot + read_only charged
        // at all levels (2x pre-accounting).
        let snapshot_bytes = PhysicalDiskBytes {
            writable: 0.try_into().unwrap(),
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 3: Create disk from that snapshot -> writable accumulates,
        // read_only is deduped because the snapshot's PPR entry exists (the
        // CTE's OR EXISTS clause detects it).
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            disk_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();

        // read_only should still be p (not 2*p) — dedup via PPR entry.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, p, p,
            )
            .await;
        }

        // Step 4: Delete disk provisioning. Snapshot PPR still exists, so
        // read_only should NOT be subtracted from any level.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Step 5: Verify back to just the snapshot's PPR charge.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Snapshot 2x lifecycle: create snapshot, create disk, delete snapshot
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_snapshot_2x_lifecycle() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx = dev::test_setup_log("test_physical_snapshot_2x_lifecycle");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let snapshot_id = Uuid::new_v4();
        let source_disk_id = Uuid::new_v4();
        let disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let phys_bytes: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let p = phys_bytes.0.to_bytes() as i64;

        // Step 1: Create snapshot record.
        create_snapshot_record(
            &datastore,
            &opctx,
            authz_project,
            snapshot_id,
            project_id,
            source_disk_id,
        )
        .await;

        // Step 2: Insert snapshot PPR (2x pre-accounting).
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 3: Create disk from snapshot (deduped). Writable
        // accumulates, read_only deduped against snapshot PPR.
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            disk_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();

        // Total: writable from disk, zfs_snapshot + read_only from
        // snapshot.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, p, p,
            )
            .await;
        }

        // Step 4: Delete snapshot PPR (deduped). Surviving disk references
        // snapshot, so read_only should NOT be subtracted.
        // zfs_snapshot IS subtracted (it's not deduped).
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        // After snapshot delete: writable from disk, zfs_snapshot gone,
        // read_only stays (surviving disk dedup).
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, p,
            )
            .await;
        }

        // Step 5: Delete disk. Now read_only should be subtracted (no
        // other reference).
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Everything back to zero.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Two disks from same snapshot dedup
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_dedup_same_snapshot_two_disks() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_dedup_same_snapshot_two_disks");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let snapshot_id = Uuid::new_v4();
        let source_disk_id = Uuid::new_v4();
        let disk1_id = Uuid::new_v4();
        let disk2_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let phys_bytes: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let p = phys_bytes.0.to_bytes() as i64;

        // Step 1: Create snapshot record and insert snapshot PPR (2x).
        create_snapshot_record(
            &datastore,
            &opctx,
            authz_project,
            snapshot_id,
            project_id,
            source_disk_id,
        )
        .await;
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 2: Create disk1 from snapshot (deduped). read_only deduped
        // against snapshot PPR.
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            disk1_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk1_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk1_id,
                }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, p, p,
            )
            .await;
        }

        // Step 3: Create disk2 from snapshot (deduped). read_only deduped
        // (disk1 + snapshot PPR exist).
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            disk2_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk2_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk2_id,
                }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                2 * w, // writable accumulates
                p,
                p, // read_only deduped
            )
            .await;
        }

        // Step 4: Delete disk1. disk2 + snapshot PPR still exist, so
        // read_only NOT subtracted.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk1_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk1_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk1_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, p, p,
            )
            .await;
        }

        // Step 5: Delete disk2. Snapshot PPR still exists, so read_only
        // NOT subtracted.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk2_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk2_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk2_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 6: Delete snapshot (deduped). No surviving disks, so
        // read_only IS subtracted.
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Exercises read-only disks from snapshots, which charge
    // writable=0 (unlike writable disks). Verifies that the 2x
    // pre-accounting model correctly handles mixed writable +
    // read-only disks from the same snapshot.
    #[tokio::test]
    async fn test_physical_dedup_snapshot_read_only_disk() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_dedup_snapshot_read_only_disk");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let authz_project = &test_data.authz_project;

        let snapshot_id = Uuid::new_v4();
        let source_disk_id = Uuid::new_v4();
        let ro_disk_id = Uuid::new_v4();
        let rw_disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let phys_bytes: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let p = phys_bytes.0.to_bytes() as i64;

        // Step 1: Create snapshot record and insert snapshot PPR (2x).
        // collection = (0, 0, 0, phys, phys)
        create_snapshot_record(
            &datastore,
            &opctx,
            authz_project,
            snapshot_id,
            project_id,
            source_disk_id,
        )
        .await;
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 2: Create read-only disk from snapshot (deduped).
        // writable=0, read_only deduped against snapshot PPR.
        // collection = (0, 0, 0, phys, phys) — unchanged
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            ro_disk_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                ro_disk_id,
                project_id,
                zero,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: ro_disk_id,
                }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 3: Create writable disk from same snapshot (deduped).
        // writable accumulates, read_only deduped.
        // collection = (0, 0, writable, phys, phys)
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project,
            rw_disk_id,
            project_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                rw_disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: rw_disk_id,
                }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, p, p,
            )
            .await;
        }

        // Step 4: Delete snapshot (SnapshotDelete dedup). Both disks
        // survive, so read_only is NOT subtracted. zfs_snapshot IS
        // subtracted.
        // collection = (0, 0, writable, 0, phys)
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, p,
            )
            .await;
        }

        // Step 5: Delete writable disk. Read-only disk still survives,
        // so read_only NOT subtracted. writable IS subtracted.
        // collection = (0, 0, 0, 0, phys)
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                rw_disk_id,
                project_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: rw_disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &rw_disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, p,
            )
            .await;
        }

        // Step 6: Delete read-only disk — last reference. read_only IS
        // subtracted.
        // collection = (0, 0, 0, 0, 0)
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                ro_disk_id,
                project_id,
                zero,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: ro_disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &ro_disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Promote with active disk: validates the conditional subtract fix
    // ---------------------------------------------------------------

    /// Create a silo image record with a known image_id.
    async fn create_silo_image_record(
        datastore: &DataStore,
        opctx: &OpContext,
        image_id: Uuid,
        silo_id: Uuid,
    ) {
        use chrono::Utc;
        use nexus_db_model::BlockSize;
        use nexus_db_model::SiloImage;
        use nexus_db_model::SiloImageIdentity;
        use omicron_common::api::external;
        use omicron_uuid_kinds::VolumeUuid;

        let authz_silo = LookupPath::new(opctx, datastore)
            .silo_id(silo_id)
            .lookup_for(crate::authz::Action::CreateChild)
            .await
            .unwrap()
            .0;
        datastore
            .silo_image_create(
                opctx,
                &authz_silo,
                SiloImage {
                    identity: SiloImageIdentity {
                        id: image_id,
                        name: external::Name::try_from(format!(
                            "simg-{image_id}"
                        ))
                        .unwrap()
                        .into(),
                        description: "test silo image".into(),
                        time_created: Utc::now(),
                        time_modified: Utc::now(),
                        time_deleted: None,
                    },
                    silo_id,
                    volume_id: VolumeUuid::new_v4().into(),
                    url: None,
                    os: "debian".into(),
                    version: "12".into(),
                    digest: None,
                    block_size: BlockSize::Iso,
                    size: external::ByteCount::from_gibibytes_u32(1).into(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_physical_image_promote_with_active_disk() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_image_promote_with_active_disk");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project = &test_data.authz_project;

        let image_id = Uuid::new_v4();
        let disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create project image I1 with PPR at project level.
        create_image_record(
            &datastore,
            &opctx,
            authz_project,
            image_id,
            silo_id,
            project_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_image(
                &opctx, image_id, project_id, read_only,
            )
            .await
            .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Step 2: Create disk D1 from I1 in the same project (deduped —
        // image PPR exists, so read_only is deduped).
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project,
            disk_id,
            project_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();

        // After disk: writable at all levels, read_only stays at ro
        // (deduped via image PPR).
        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 3: Promote I1 to silo level. The conditional subtract
        // should see the active disk and NOT subtract read_only from the
        // project PPC.
        let (authz_silo, _, authz_project_image, project_image) =
            LookupPath::new(&opctx, &*datastore)
                .project_image_id(image_id)
                .fetch_for(crate::authz::Action::Modify)
                .await
                .unwrap();

        datastore
            .project_image_promote(
                &opctx,
                &authz_silo,
                &authz_project_image,
                &project_image,
            )
            .await
            .unwrap();

        // After promote: project keeps read_only (disk keeps it alive),
        // silo/fleet unchanged.
        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 4: Delete D1. After promote, the image's project_id is
        // NULL, so the dedup_in_project OR EXISTS clause (which checks
        // i.project_id = project_id) will NOT find the image PPR. This
        // means read_only IS subtracted from the project PPC. At silo
        // level, the image PPR is still found (via i.silo_id), so
        // read_only is NOT subtracted from silo/fleet.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project: all zeros (disk and image charge both gone).
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, 0, 0, 0,
        )
        .await;
        // Silo/fleet: read_only stays (image PPR still alive at silo
        // level).
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, 0, 0, ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore, &opctx, fleet_id, 0, 0, 0, 0, ro,
        )
        .await;

        // Cleanup: delete the silo image PPR (no surviving disks, so no
        // dedup).
        datastore
            .physical_provisioning_collection_delete_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Demote with active disk: validates the conditional add fix
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_image_demote_with_active_disk() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_image_demote_with_active_disk");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project = &test_data.authz_project;

        let image_id = Uuid::new_v4();
        let disk_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create silo image I1 with PPR at silo level.
        create_silo_image_record(&datastore, &opctx, image_id, silo_id).await;
        datastore
            .physical_provisioning_collection_insert_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        // Project: zero. Silo/fleet: read_only.
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, 0, 0, 0,
        )
        .await;
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Step 2: Create disk D1 from I1 in project A. Since the image is
        // silo-scoped, dedup_in_project doesn't find the image PPR
        // (i.project_id IS NULL), so read_only IS charged at project
        // level. dedup_in_silo DOES find the image PPR, so read_only is
        // deduped at silo/fleet level.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project,
            disk_id,
            project_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();

        // Project: writable + read_only (disk charged both).
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, w, 0, ro,
        )
        .await;
        // Silo/fleet: writable accumulates, read_only deduped (still ro).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 3: Demote I1 to project A. The conditional add should see
        // the active disk and NOT add read_only to the project PPC (would
        // double-count).
        let (.., authz_silo_image, silo_image) =
            LookupPath::new(&opctx, &*datastore)
                .silo_image_id(image_id)
                .fetch_for(crate::authz::Action::Modify)
                .await
                .unwrap();

        datastore
            .silo_image_demote(
                &opctx,
                &authz_silo_image,
                authz_project,
                &silo_image,
            )
            .await
            .unwrap();

        // After demote: project still at (w, 0, ro) — NOT (w, 0, 2*ro).
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, w, 0, ro,
        )
        .await;
        // Silo/fleet unchanged.
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 4: Delete D1. After demote, the image's project_id is set
        // to project_id, so dedup_in_project's OR EXISTS clause finds the
        // image PPR. read_only is NOT subtracted from project (image PPR
        // dedupes it).
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_id,
                project_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project: writable gone, read_only stays (image PPR dedupes).
        verify_physical_collection_usage(
            &datastore, &opctx, project_id, 0, 0, 0, 0, ro,
        )
        .await;
        // Silo/fleet: writable gone, read_only stays.
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Cleanup: delete the (now project-scoped) image PPR.
        datastore
            .physical_provisioning_collection_delete_image(
                &opctx, image_id, project_id, read_only,
            )
            .await
            .unwrap();

        for id in [project_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Silo image delete deduped with surviving disks
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_silo_image_delete_with_dedup() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_silo_image_delete_with_dedup");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_a_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project_a = &test_data.authz_project;

        let (project_b_id, authz_project_b) =
            setup_second_project(&datastore, &opctx, silo_id, "projectbsid")
                .await;

        let image_id = Uuid::new_v4();
        let disk_a_id = Uuid::new_v4();
        let disk_b_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create silo image I1 with PPR at silo level.
        create_silo_image_record(&datastore, &opctx, image_id, silo_id).await;
        datastore
            .physical_provisioning_collection_insert_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        // Step 2: Disk D1 in project A from I1.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project_a,
            disk_a_id,
            project_a_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();

        // Step 3: Disk D2 in project B from I1.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            &authz_project_b,
            disk_b_id,
            project_b_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();

        // Both projects: writable + read_only.
        for id in [project_a_id, project_b_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }
        // Silo/fleet: 2*writable, read_only deduped (still ro).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                2 * w,
                0,
                ro,
            )
            .await;
        }

        // Step 4: Delete silo image I1 (ImageDelete dedup). Surviving
        // disks D1+D2 should prevent read_only subtraction at silo/fleet.
        datastore
            .physical_provisioning_collection_delete_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        // Projects unchanged.
        for id in [project_a_id, project_b_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }
        // Silo/fleet: read_only still alive (surviving disks detected).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                2 * w,
                0,
                ro,
            )
            .await;
        }

        // Step 5: Delete D1. D2 still alive in silo.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_a_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project A: all zeros (no dedup refs in project A).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        // Project B unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: writable = w (D2), read_only still alive (D2).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 6: Delete D2. Last reference.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_b_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // All zeros everywhere.
        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Cross-project disks from a silo image
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_cross_project_disk_from_silo_image() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx = dev::test_setup_log(
            "test_physical_cross_project_disk_from_silo_image",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_a_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project_a = &test_data.authz_project;

        let (project_b_id, authz_project_b) =
            setup_second_project(&datastore, &opctx, silo_id, "projectbxp")
                .await;

        let image_id = Uuid::new_v4();
        let disk_a_id = Uuid::new_v4();
        let disk_b_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let read_only: ByteCount = (512 << 20).try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let ro = read_only.0.to_bytes() as i64;

        // Step 1: Create silo image I1 with PPR at silo level.
        create_silo_image_record(&datastore, &opctx, image_id, silo_id).await;
        datastore
            .physical_provisioning_collection_insert_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Step 2: Disk in project A from I1.
        // Project-level dedup: image is silo-scoped (project_id IS NULL),
        // so OR EXISTS fails → read_only IS charged at project level.
        // Silo-level dedup: image PPR found → read_only deduped.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            authz_project_a,
            disk_a_id,
            project_a_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();

        // Project A: writable + read_only.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo: writable accumulates, read_only deduped (still ro).
        verify_physical_collection_usage(
            &datastore, &opctx, silo_id, 0, 0, w, 0, ro,
        )
        .await;

        // Step 3: Disk in project B from same I1.
        create_disk_with_origin_image(
            &datastore,
            &opctx,
            &authz_project_b,
            disk_b_id,
            project_b_id,
            image_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();

        // Project B: writable + read_only (no project-level dedup — image
        // is silo-scoped).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            ro,
        )
        .await;
        // Silo/fleet: writable = 2*w, read_only still ro (silo deduped).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            0,
            ro,
        )
        .await;

        // Step 4: Delete disk A. Disk B still alive in silo.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_a_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project A: all zeros.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        // Silo/fleet: writable = w (D2), read_only still ro (D2 + image
        // PPR).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, ro,
            )
            .await;
        }

        // Step 5: Delete disk B. Image PPR still exists at silo level.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                read_only,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Image,
                    origin_id: image_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_b_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Both projects: all zeros.
        for id in [project_a_id, project_b_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }
        // Silo/fleet: writable gone, read_only still ro (image PPR).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, ro,
            )
            .await;
        }

        // Cleanup: delete silo image PPR.
        datastore
            .physical_provisioning_collection_delete_image_silo(
                &opctx, image_id, silo_id, read_only,
            )
            .await
            .unwrap();

        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Disk quota failure
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_disk_quota_failure() {
        let logctx = dev::test_setup_log("test_physical_disk_quota_failure");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;
        let silo_id = test_data.silo_id;

        let disk_id = Uuid::new_v4();
        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();

        // Set a tight physical storage quota (100 bytes).
        let authz_silo = LookupPath::new(&opctx, &*datastore)
            .silo_id(silo_id)
            .lookup_for(crate::authz::Action::Modify)
            .await
            .unwrap()
            .0;
        datastore
            .silo_update_quota(
                &opctx,
                &authz_silo,
                SiloQuotasUpdate {
                    cpus: None,
                    memory: None,
                    storage: None,
                    physical_storage_bytes: Some(Some(100)),
                    time_modified: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Attempt disk insert — should fail with InsufficientCapacity.
        let result = datastore
            .physical_provisioning_collection_insert_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await;
        assert!(
            matches!(
                result,
                Err(omicron_common::api::external::Error::InsufficientCapacity {
                    ..
                })
            ),
            "Expected InsufficientCapacity, got: {:?}",
            result
        );

        // Collections unchanged — no partial charge.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // ---------------------------------------------------------------
    // Snapshot cross-project dedup
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_physical_snapshot_cross_project_dedup() {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn,
        };

        let logctx =
            dev::test_setup_log("test_physical_snapshot_cross_project_dedup");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let project_a_id = test_data.project_id;
        let silo_id = test_data.silo_id;
        let fleet_id = test_data.fleet_id;
        let authz_project_a = &test_data.authz_project;

        let (project_b_id, authz_project_b) =
            setup_second_project(&datastore, &opctx, silo_id, "projectbsnap")
                .await;

        let snapshot_id = Uuid::new_v4();
        let source_disk_id = Uuid::new_v4();
        let disk_a_id = Uuid::new_v4();
        let disk_b_id = Uuid::new_v4();

        let writable: ByteCount = (1 << 30).try_into().unwrap();
        let phys_bytes: ByteCount = (512 << 20).try_into().unwrap();
        let zero: ByteCount = 0.try_into().unwrap();
        let w = writable.0.to_bytes() as i64;
        let p = phys_bytes.0.to_bytes() as i64;

        // Step 1: Create snapshot S1 in project A. Charges 2x
        // (zfs_snapshot + read_only) at all levels.
        create_snapshot_record(
            &datastore,
            &opctx,
            authz_project_a,
            snapshot_id,
            project_a_id,
            source_disk_id,
        )
        .await;
        let snapshot_bytes = PhysicalDiskBytes {
            writable: zero,
            zfs_snapshot: phys_bytes,
            read_only: phys_bytes,
        };
        datastore
            .physical_provisioning_collection_insert_snapshot(
                &opctx,
                snapshot_id,
                project_a_id,
                snapshot_bytes,
                snapshot_bytes,
            )
            .await
            .unwrap();

        for id in [project_a_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, p, p,
            )
            .await;
        }

        // Step 2: Disk D1 in project A from S1 (deduped against snapshot
        // PPR).
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            authz_project_a,
            disk_a_id,
            project_a_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();

        // Project A: writable + (deduped) read_only.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            p,
            p,
        )
        .await;

        // Step 3: Disk D2 in project B from S1. Project-level dedup: the
        // snapshot is in project A, not B, so OR EXISTS fails for project
        // B → read_only IS charged at project B level.
        // Silo-level: snapshot PPR found → deduped.
        create_disk_with_origin_snapshot(
            &datastore,
            &opctx,
            &authz_project_b,
            disk_b_id,
            project_b_id,
            snapshot_id,
        )
        .await;
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();

        // Project B: writable + read_only (no project-level dedup).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            p,
        )
        .await;
        // Silo/fleet: 2*writable, zfs_snapshot + read_only unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            p,
            p,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            p,
            p,
        )
        .await;

        // Step 4: Delete S1 (SnapshotDelete dedup). Surviving disks D1+D2
        // prevent read_only subtraction. zfs_snapshot IS subtracted.
        datastore
            .physical_provisioning_collection_delete_snapshot(
                &opctx,
                snapshot_id,
                project_a_id,
                snapshot_bytes,
                snapshot_bytes,
                Some(DedupInfo::SnapshotDelete { snapshot_id }),
            )
            .await
            .unwrap();

        // Project A: zfs_snapshot gone, read_only stays (D1 survives).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            w,
            0,
            p,
        )
        .await;
        // Project B unchanged.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_b_id,
            0,
            0,
            w,
            0,
            p,
        )
        .await;
        // Silo/fleet: zfs_snapshot subtracted, read_only stays.
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            silo_id,
            0,
            0,
            2 * w,
            0,
            p,
        )
        .await;
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            fleet_id,
            0,
            0,
            2 * w,
            0,
            p,
        )
        .await;

        // Step 5: Delete D1. D2 still alive in silo.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_a_id,
                project_a_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk_a_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_a_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Project A: all zeros (no dedup refs in project A — snapshot PPR
        // was deleted in step 4).
        verify_physical_collection_usage(
            &datastore,
            &opctx,
            project_a_id,
            0,
            0,
            0,
            0,
            0,
        )
        .await;
        // Silo/fleet: writable = w (D2), read_only stays (D2).
        for id in [silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, w, 0, p,
            )
            .await;
        }

        // Step 6: Delete D2. Last reference.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx,
                disk_b_id,
                project_b_id,
                writable,
                phys_bytes,
                Some(DedupInfo::Disk {
                    origin_column: DedupOriginColumn::Snapshot,
                    origin_id: snapshot_id,
                    disk_id: disk_b_id,
                }),
            )
            .await
            .unwrap();
        datastore
            .project_delete_disk_no_auth(
                &disk_b_id,
                &[omicron_common::api::external::DiskState::Detached],
            )
            .await
            .unwrap();

        // All zeros everywhere.
        for id in [project_a_id, project_b_id, silo_id, fleet_id] {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Verifies that local disks use a different physical byte formula
    // than distributed disks, and that the accounting works correctly
    // when only writable bytes are charged (no snapshot or read-only,
    // since local disks don't support snapshots/images).
    #[tokio::test]
    async fn test_physical_local_disk_accounting() {
        use nexus_db_model::{
            VirtualDiskBytes, distributed_disk_physical_bytes,
            local_disk_physical_bytes,
        };

        let logctx = dev::test_setup_log("test_physical_local_disk_accounting");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_data = setup_collections(&datastore, &opctx).await;
        let ids = test_data.ids();
        let project_id = test_data.project_id;

        let disk_id = Uuid::new_v4();
        let virtual_size = VirtualDiskBytes(
            omicron_common::api::external::ByteCount::from_gibibytes_u32(1),
        );

        let local_phys = local_disk_physical_bytes(virtual_size);
        let distributed_phys = distributed_disk_physical_bytes(virtual_size);

        // Sanity check: the two formulas produce different values for the
        // same virtual size.
        assert_ne!(
            local_phys.to_bytes(),
            distributed_phys.to_bytes(),
            "local and distributed formulas should differ"
        );

        let writable: ByteCount = local_phys.into_byte_count().into();
        let zero: ByteCount = 0.try_into().unwrap();

        // Initial state: all zeros.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        // Insert local disk: only writable bytes, no snapshot or read-only.
        datastore
            .physical_provisioning_collection_insert_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        let expected_writable = local_phys.to_bytes() as i64;
        for id in ids {
            verify_physical_collection_usage(
                &datastore,
                &opctx,
                id,
                0,
                0,
                expected_writable,
                0,
                0,
            )
            .await;
        }

        // Delete the disk.
        datastore
            .physical_provisioning_collection_delete_disk(
                &opctx, disk_id, project_id, writable, zero, None,
            )
            .await
            .unwrap();

        // All counters back to zero.
        for id in ids {
            verify_physical_collection_usage(
                &datastore, &opctx, id, 0, 0, 0, 0, 0,
            )
            .await;
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
