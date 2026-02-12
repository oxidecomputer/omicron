// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Disk`]s.

use super::DataStore;
use super::StorageType;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_attach::AttachError;
use crate::db::collection_attach::AttachToCollectionStatement;
use crate::db::collection_attach::DatastoreAttachTarget;
use crate::db::collection_detach::DatastoreDetachTarget;
use crate::db::collection_detach::DetachError;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::DbConnection;
use crate::db::identity::Resource;
use crate::db::model;
use crate::db::model::DiskRuntimeState;
use crate::db::model::DiskState;
use crate::db::model::DiskTypeCrucible;
use crate::db::model::DiskTypeCrucibleUpdate;
use crate::db::model::DiskTypeLocalStorage;
use crate::db::model::Instance;
use crate::db::model::LocalStorageDatasetAllocation;
use crate::db::model::LocalStorageUnencryptedDatasetAllocation;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::VirtualProvisioningResource;
use crate::db::model::Volume;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use crate::db::queries::disk::DiskSetClauseForAttach;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::dsl::exists;
use diesel::dsl::not;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_types::external_api::params;
use nexus_types::identity::Asset;
use omicron_common::api;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VolumeUuid;
use ref_cast::RefCast;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Disk {
    Crucible(CrucibleDisk),

    LocalStorage(LocalStorageDisk),
}

impl Disk {
    pub fn model(&self) -> &model::Disk {
        match &self {
            Disk::Crucible(disk) => disk.model(),
            Disk::LocalStorage(disk) => disk.model(),
        }
    }

    pub fn id(&self) -> Uuid {
        self.model().id()
    }

    pub fn name(&self) -> &api::external::Name {
        self.model().name()
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.model().time_deleted()
    }

    pub fn project_id(&self) -> Uuid {
        self.model().project_id
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.model().runtime()
    }

    pub fn state(&self) -> DiskState {
        self.model().state()
    }

    pub fn size(&self) -> model::ByteCount {
        self.model().size
    }

    pub fn slot(&self) -> Option<u8> {
        self.model().slot()
    }

    pub fn block_size(&self) -> model::BlockSize {
        self.model().block_size
    }

    pub fn is_read_only(&self) -> bool {
        match self {
            Self::Crucible(disk) => disk.is_read_only(),
            // local disks cannot currently be read-only
            Self::LocalStorage(_) => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrucibleDisk {
    pub disk: model::Disk,
    pub disk_type_crucible: DiskTypeCrucible,
}

impl CrucibleDisk {
    pub fn model(&self) -> &model::Disk {
        &self.disk
    }

    pub fn id(&self) -> Uuid {
        self.model().id()
    }

    pub fn name(&self) -> &api::external::Name {
        self.model().name()
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.model().time_deleted()
    }

    pub fn project_id(&self) -> Uuid {
        self.model().project_id
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.model().runtime()
    }

    pub fn state(&self) -> DiskState {
        self.model().state()
    }

    pub fn size(&self) -> model::ByteCount {
        self.model().size
    }

    pub fn slot(&self) -> Option<u8> {
        self.model().slot()
    }

    pub fn volume_id(&self) -> VolumeUuid {
        self.disk_type_crucible.volume_id()
    }

    pub fn pantry_address(&self) -> Option<SocketAddrV6> {
        self.disk_type_crucible.pantry_address()
    }

    pub fn is_read_only(&self) -> bool {
        self.disk_type_crucible.read_only
    }
}

/// A Disk backed by local storage can have an allocation in either the
/// associated unencrypted or encrypted dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LocalStorageAllocation {
    /// A portion of a `DatasetKind::LocalStorageUnencrypted` dataset has been
    /// allocated for a Disk backed by local storage.
    Unencrypted(LocalStorageUnencryptedDatasetAllocation),

    /// A portion of a `DatasetKind::LocalStorage` dataset has been allocated
    /// for a Disk backed by local storage.
    Encrypted(LocalStorageDatasetAllocation),
}

impl LocalStorageAllocation {
    pub fn id(&self) -> DatasetUuid {
        match &self {
            LocalStorageAllocation::Unencrypted(allocation) => allocation.id(),

            LocalStorageAllocation::Encrypted(allocation) => allocation.id(),
        }
    }

    pub fn sled_id(&self) -> SledUuid {
        match &self {
            LocalStorageAllocation::Unencrypted(allocation) => {
                allocation.sled_id()
            }

            LocalStorageAllocation::Encrypted(allocation) => {
                allocation.sled_id()
            }
        }
    }

    pub fn pool_id(&self) -> ExternalZpoolUuid {
        match &self {
            LocalStorageAllocation::Unencrypted(allocation) => {
                allocation.pool_id()
            }

            LocalStorageAllocation::Encrypted(allocation) => {
                allocation.pool_id()
            }
        }
    }

    pub fn encrypted_at_rest(&self) -> bool {
        match &self {
            LocalStorageAllocation::Unencrypted(_) => false,

            LocalStorageAllocation::Encrypted(_) => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStorageDisk {
    pub disk: model::Disk,
    pub disk_type_local_storage: DiskTypeLocalStorage,

    pub local_storage_dataset_allocation: Option<LocalStorageAllocation>,
}

impl LocalStorageDisk {
    /// Create a new unallocated LocalStorageDisk
    pub fn new(
        disk: model::Disk,
        disk_type_local_storage: DiskTypeLocalStorage,
    ) -> LocalStorageDisk {
        LocalStorageDisk {
            disk,
            disk_type_local_storage,
            local_storage_dataset_allocation: None,
        }
    }

    pub fn model(&self) -> &model::Disk {
        &self.disk
    }

    pub fn id(&self) -> Uuid {
        self.model().id()
    }

    pub fn name(&self) -> &api::external::Name {
        self.model().name()
    }

    pub fn time_deleted(&self) -> Option<DateTime<Utc>> {
        self.model().time_deleted()
    }

    pub fn project_id(&self) -> Uuid {
        self.model().project_id
    }

    pub fn runtime(&self) -> DiskRuntimeState {
        self.model().runtime()
    }

    pub fn state(&self) -> DiskState {
        self.model().state()
    }

    pub fn size(&self) -> model::ByteCount {
        self.model().size
    }

    pub fn slot(&self) -> Option<u8> {
        self.model().slot()
    }

    pub fn required_dataset_overhead(&self) -> external::ByteCount {
        self.disk_type_local_storage.required_dataset_overhead()
    }

    /// Return the full path to the local storage zvol's device
    pub fn zvol_path(&self) -> Result<String, Error> {
        let Some(allocation) = &self.local_storage_dataset_allocation else {
            return Err(Error::internal_error(&format!(
                "LocalStorageDisk {} not allocated!",
                self.id(),
            )));
        };

        match allocation {
            LocalStorageAllocation::Unencrypted(unencrypted_allocation) => {
                let pool_id = unencrypted_allocation.pool_id();
                let dataset_id = unencrypted_allocation.id();

                let zpool_name = ZpoolName::External(pool_id);

                let path = [
                    // Each zvol's path to the device starts with this
                    String::from("/dev/zvol/rdsk"),
                    // All local storage datasets have the same path template,
                    // and will all be called "vol" for now.
                    format!("{zpool_name}/local_storage_unencrypted"),
                    format!("{dataset_id}/vol"),
                ]
                .join("/");

                Ok(path)
            }

            LocalStorageAllocation::Encrypted(encrypted_allocation) => {
                let pool_id = encrypted_allocation.pool_id();
                let dataset_id = encrypted_allocation.id();

                let zpool_name = ZpoolName::External(pool_id);

                let path = [
                    // Each zvol's path to the device starts with this
                    String::from("/dev/zvol/rdsk"),
                    // All local storage datasets have the same path template,
                    // and will all be called "vol" for now.
                    format!("{zpool_name}/crypt/local_storage"),
                    format!("{dataset_id}/vol"),
                ]
                .join("/");

                Ok(path)
            }
        }
    }
}

/// Conversion to the external API type.
impl Into<api::external::Disk> for Disk {
    fn into(self) -> api::external::Disk {
        match self {
            Disk::Crucible(CrucibleDisk { disk, disk_type_crucible }) => {
                // XXX can we remove this?
                let device_path = format!("/mnt/{}", disk.name().as_str());
                api::external::Disk {
                    identity: disk.identity(),
                    project_id: disk.project_id,
                    snapshot_id: disk_type_crucible.create_snapshot_id,
                    image_id: disk_type_crucible.create_image_id,
                    size: disk.size.into(),
                    block_size: disk.block_size.into(),
                    state: disk.state().into(),
                    device_path,
                    disk_type: api::external::DiskType::Distributed,
                    read_only: disk_type_crucible.read_only,
                }
            }

            Disk::LocalStorage(LocalStorageDisk {
                disk,
                disk_type_local_storage: _,
                local_storage_dataset_allocation: _,
            }) => {
                // XXX can we remove this?
                let device_path = format!("/mnt/{}", disk.name().as_str());
                api::external::Disk {
                    identity: disk.identity(),
                    project_id: disk.project_id,
                    snapshot_id: None,
                    image_id: None,
                    size: disk.size.into(),
                    block_size: disk.block_size.into(),
                    state: disk.state().into(),
                    device_path,
                    disk_type: api::external::DiskType::Local,
                    // Local disks are (currently) never read-only
                    read_only: false,
                }
            }
        }
    }
}

enum ReadOnlyDiskSource {
    Image(authz::Image),
    Snapshot(authz::Snapshot),
}

impl DataStore {
    async fn fill_in_local_storage_disk(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        disk: model::Disk,
        disk_type_local_storage: DiskTypeLocalStorage,
    ) -> LookupResult<LocalStorageDisk> {
        let disk_id = disk.id();

        let encrypted_allocation = if let Some(allocation_id) =
            disk_type_local_storage.local_storage_dataset_allocation_id()
        {
            use nexus_db_schema::schema::local_storage_dataset_allocation::dsl;

            let allocation = dsl::local_storage_dataset_allocation
                .filter(dsl::id.eq(to_db_typed_uuid(allocation_id)))
                .select(LocalStorageDatasetAllocation::as_select())
                .first_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(format!(
                            "local storage disk {disk_id} missing encrypted \
                            allocation {allocation_id}"
                        ))
                })?;

            Some(allocation)
        } else {
            None
        };

        let unencrypted_allocation = if let Some(allocation_id) =
            disk_type_local_storage
                .local_storage_unencrypted_dataset_allocation_id()
        {
            use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl;

            let allocation = dsl::local_storage_unencrypted_dataset_allocation
                .filter(dsl::id.eq(to_db_typed_uuid(allocation_id)))
                .select(LocalStorageUnencryptedDatasetAllocation::as_select())
                .first_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context(format!(
                            "local storage disk {disk_id} missing unencrypted \
                            allocation {allocation_id}"
                        ))
                })?;

            Some(allocation)
        } else {
            None
        };

        let local_storage_dataset_allocation =
            match (encrypted_allocation, unencrypted_allocation) {
                (None, None) => None,

                (Some(encrypted_allocation), None) => Some(
                    LocalStorageAllocation::Encrypted(encrypted_allocation),
                ),

                (None, Some(unencrypted_allocation)) => Some(
                    LocalStorageAllocation::Unencrypted(unencrypted_allocation),
                ),

                (Some(_), Some(_)) => {
                    return Err(Error::internal_error(&format!(
                        "local storage disk {} has multiple dataset \
                        allocations!",
                        disk.id(),
                    )));
                }
            };

        Ok(LocalStorageDisk {
            disk,
            disk_type_local_storage,
            local_storage_dataset_allocation,
        })
    }

    /// Return a `datastore::Disk` given a disk's UUID. Will perform a
    /// LookupPath in order to retrive the `model::Disk`, then call
    /// `disk_get_with_model`.
    pub async fn disk_get(
        &self,
        opctx: &OpContext,
        disk_id: Uuid,
    ) -> LookupResult<Disk> {
        let (.., disk) =
            LookupPath::new(opctx, self).disk_id(disk_id).fetch().await?;

        self.disk_get_with_model(opctx, disk).await
    }

    /// Return a `datastore::Disk` given a `model::Disk`
    ///
    /// Note: basically all of Nexus should _not_ be using this, and should be
    /// using `disk_get` instead: this version of the function bypasses the
    /// LookupPath induced permissions check and should only called from omdb.
    pub async fn disk_get_with_model(
        &self,
        opctx: &OpContext,
        disk: model::Disk,
    ) -> LookupResult<Disk> {
        let disk_id = disk.id();

        let conn = self.pool_connection_authorized(opctx).await?;

        let disk = match disk.disk_type {
            db::model::DiskType::Crucible => {
                use nexus_db_schema::schema::disk_type_crucible::dsl;

                let disk_type_crucible = dsl::disk_type_crucible
                    .filter(dsl::disk_id.eq(disk_id))
                    .select(DiskTypeCrucible::as_select())
                    .first_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                            .internal_context(format!(
                                "{disk_id} missing disk_type_crucible record"
                            ))
                    })?;

                Disk::Crucible(CrucibleDisk { disk, disk_type_crucible })
            }

            db::model::DiskType::LocalStorage => {
                use nexus_db_schema::schema::disk_type_local_storage::dsl;

                let disk_type_local_storage = dsl::disk_type_local_storage
                    .filter(dsl::disk_id.eq(disk_id))
                    .select(DiskTypeLocalStorage::as_select())
                    .first_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                            .internal_context(format!(
                                "{disk_id} missing disk_type_local_storage \
                                record"
                            ))
                    })?;

                let local_storage_disk = Self::fill_in_local_storage_disk(
                    &conn,
                    disk,
                    disk_type_local_storage,
                )
                .await?;

                Disk::LocalStorage(local_storage_disk)
            }
        };

        Ok(disk)
    }

    /// Return all the Crucible Disks matching a list of volume IDs. Currently
    /// this is only used by omdb.
    pub async fn disks_get_matching_volumes(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume_ids: &HashSet<VolumeUuid>,
        include_deleted: bool,
        limit: i64,
    ) -> ListResultVec<CrucibleDisk> {
        use nexus_db_schema::schema::disk::dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl as disk_type_crucible_dsl;

        let mut query = dsl::disk.into_boxed();
        if !include_deleted {
            query = query.filter(dsl::time_deleted.is_null());
        }

        let volume_ids: Vec<Uuid> = volume_ids
            .iter()
            .map(|volume_id| volume_id.into_untyped_uuid())
            .collect();

        let result: Vec<CrucibleDisk> = query
            .inner_join(
                disk_type_crucible_dsl::disk_type_crucible
                    .on(disk_type_crucible_dsl::disk_id.eq(dsl::id)),
            )
            .filter(disk_type_crucible_dsl::volume_id.eq_any(volume_ids))
            .limit(limit)
            .select((
                db::model::Disk::as_select(),
                db::model::DiskTypeCrucible::as_select(),
            ))
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .into_iter()
            .map(|(disk, disk_type_crucible)| CrucibleDisk {
                disk,
                disk_type_crucible,
            })
            .collect();

        Ok(result)
    }

    /// List disks associated with a given instance by name.
    pub async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Disk> {
        let conn = self.pool_connection_authorized(opctx).await?;

        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        self.instance_list_disks_on_conn(&conn, authz_instance.id(), pagparams)
            .await
    }

    /// Consume a query result listing all parts of the higher level Disk type,
    /// and assemble it.
    async fn process_tuples_to_disk_list(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        results: Vec<(
            model::Disk,
            Option<DiskTypeCrucible>,
            Option<DiskTypeLocalStorage>,
        )>,
    ) -> ListResultVec<Disk> {
        let mut list = Vec::with_capacity(results.len());

        for result in results {
            match result {
                (disk, Some(disk_type_crucible), None) => {
                    list.push(Disk::Crucible(CrucibleDisk {
                        disk,
                        disk_type_crucible,
                    }));
                }

                (disk, None, Some(disk_type_local_storage)) => {
                    let local_storage_disk = Self::fill_in_local_storage_disk(
                        &conn,
                        disk,
                        disk_type_local_storage,
                    )
                    .await?;

                    list.push(Disk::LocalStorage(local_storage_disk));
                }

                (disk, _, _) => {
                    // The above paginated query attempts to get all types of
                    // disk in one query, instead of matching on the disk type
                    // of each returned disk row and doing additional queries.
                    //
                    // If we're in this branch then that query didn't return the
                    // type-specific information for a disk. It's possible that
                    // disk was constructed wrong, or that a new disk type
                    // hasn't been added to the above query and this match.
                    return Err(Error::internal_error(&format!(
                        "disk {} is type {:?}, but no type-specific row found!",
                        disk.id(),
                        disk.disk_type,
                    )));
                }
            }
        }

        Ok(list)
    }

    /// List disks associated with a given instance by name.
    pub async fn instance_list_disks_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        instance_id: Uuid,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Disk> {
        use nexus_db_schema::schema::disk::dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl as disk_type_crucible_dsl;
        use nexus_db_schema::schema::disk_type_local_storage::dsl as disk_type_local_storage_dsl;

        let results = match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::disk, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::disk,
                dsl::name,
                &pagparams.map_name(Name::ref_cast),
            ),
        }
        .left_join(
            disk_type_crucible_dsl::disk_type_crucible
                .on(dsl::id.eq(disk_type_crucible_dsl::disk_id)),
        )
        .left_join(
            disk_type_local_storage_dsl::disk_type_local_storage
                .on(dsl::id.eq(disk_type_local_storage_dsl::disk_id)),
        )
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::attach_instance_id.eq(instance_id))
        .select((
            model::Disk::as_select(),
            Option::<DiskTypeCrucible>::as_select(),
            Option::<DiskTypeLocalStorage>::as_select(),
        ))
        .get_results_async(conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Self::process_tuples_to_disk_list(conn, results).await
    }

    pub(super) async fn project_create_disk_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        authz_project: &authz::Project,
        disk: Disk,
    ) -> Result<Disk, diesel::result::Error> {
        use nexus_db_schema::schema::disk::dsl;

        // In what state do we expect the disk record to be created? This will
        // generally be `Creating`, save for read-only disks being created from
        // existing images or snapshots, which pop into existence already
        // `Detached` (as the read-only volume snapshot backing them already
        // exists). Thus, when we check for insert conflicts, we must compare
        // the inserted state with the requested initial state, rather than
        // assuming it will always be `Creating`.
        let expected_state = match disk {
            Disk::Crucible(CrucibleDisk {
                disk_type_crucible:
                    DiskTypeCrucible {
                        read_only: true,
                        create_snapshot_id,
                        create_image_id,
                        ..
                    },
                ..
            }) if create_snapshot_id.is_some() || create_image_id.is_some() => {
                external::DiskState::Detached
            }
            _ => external::DiskState::Creating,
        };

        let generation = disk.runtime().generation;
        let name = disk.name().clone();
        let project_id = disk.project_id();

        let disk_model: model::Disk = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::disk)
                .values(disk.model().clone())
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| {
            err.bail(match e {
                AsyncInsertError::CollectionNotFound => {
                    authz_project.not_found()
                }
                AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::Disk, name.as_str()),
                ),
            })
        })?;

        match &disk {
            Disk::Crucible(CrucibleDisk { disk: _, disk_type_crucible }) => {
                use nexus_db_schema::schema::disk_type_crucible::dsl;

                diesel::insert_into(dsl::disk_type_crucible)
                    .values(disk_type_crucible.clone())
                    .on_conflict(dsl::disk_id)
                    .do_nothing()
                    .execute_async(conn)
                    .await?;
            }

            Disk::LocalStorage(LocalStorageDisk {
                disk,
                disk_type_local_storage,
                local_storage_dataset_allocation,
            }) => {
                if local_storage_dataset_allocation.is_some() {
                    // This allocation is currently only performed during
                    // instance allocation, return an error here.
                    return Err(err.bail(Error::InternalError {
                        internal_message: format!(
                            "local storage dataset allocation is only \
                            performed during instance allocation, but {} is \
                            being created with an allocation when it should be \
                            None",
                            disk.id()
                        ),
                    }));
                }

                use nexus_db_schema::schema::disk_type_local_storage::dsl;

                diesel::insert_into(dsl::disk_type_local_storage)
                    .values(disk_type_local_storage.clone())
                    .on_conflict(dsl::disk_id)
                    .do_nothing()
                    .execute_async(conn)
                    .await?;
            }
        }

        // Perform a few checks in the transaction on the inserted Disk to
        // ensure that the newly created Disk is valid (even if there was an
        // insertion conflict).

        if disk_model.state().state() != &expected_state {
            return Err(err.bail(Error::internal_error(&format!(
                "newly-created Disk has unexpected state: {:?} (expected \
                 {expected_state:?})",
                disk_model.state(),
            ))));
        }

        let runtime = disk_model.runtime();

        if runtime.generation != generation {
            return Err(err.bail(Error::internal_error(&format!(
                "newly-created Disk has unexpected generation: {:?}",
                runtime.generation
            ))));
        }

        Ok(disk)
    }

    pub async fn project_create_disk(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        disk: Disk,
    ) -> CreateResult<Disk> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let disk = self
            .transaction_retry_wrapper("project_create_disk")
            .transaction(&conn, |conn| {
                let disk = disk.clone();
                let err = err.clone();
                async move {
                    Self::project_create_disk_in_txn(
                        &conn,
                        err,
                        authz_project,
                        disk,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(disk)
    }

    /// Atomically create a disk record AND insert virtual + physical
    /// provisioning in a single database transaction.
    ///
    /// This eliminates the race window where the dedup query could see
    /// the disk record without its provisioning entry (or vice versa).
    pub async fn project_create_disk_and_provision(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        disk: Disk,
        create_params: &params::DiskCreate,
    ) -> CreateResult<Disk> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let disk_id = disk.id();
        let project_id = disk.project_id();
        let create_params = create_params.clone();

        let disk = self
            .transaction_retry_wrapper("project_create_disk_and_provision")
            .transaction(&conn, |conn| {
                let disk = disk.clone();
                let err = err.clone();
                let create_params = create_params.clone();
                async move {
                    // Step 1: Create the disk record (and disk_type_*).
                    let created_disk = Self::project_create_disk_in_txn(
                        &conn,
                        err.clone(),
                        authz_project,
                        disk,
                    )
                    .await?;

                    // Step 2: Virtual provisioning.
                    use crate::db::queries::virtual_provisioning_collection_update::VirtualProvisioningCollectionUpdate;
                    VirtualProvisioningCollectionUpdate::new_insert_storage(
                        disk_id,
                        crate::db::model::ByteCount::from(create_params.size),
                        project_id,
                        StorageType::Disk,
                    )
                    .get_results_async::<crate::db::model::VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::virtual_provisioning_collection_update::from_diesel(e))
                    })?;

                    // Step 3: Physical provisioning.
                    Self::insert_physical_provisioning_in_txn(
                        &conn,
                        err.clone(),
                        disk_id,
                        project_id,
                        &create_params,
                    )
                    .await?;

                    Ok(created_disk)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(disk)
    }

    /// Helper: insert physical provisioning for a disk inside a transaction.
    async fn insert_physical_provisioning_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        disk_id: Uuid,
        project_id: Uuid,
        create_params: &params::DiskCreate,
    ) -> Result<(), diesel::result::Error> {
        use crate::db::queries::physical_provisioning_collection_update::{
            DedupInfo, DedupOriginColumn, PhysicalProvisioningCollectionUpdate,
        };

        let virtual_bytes =
            nexus_db_model::VirtualDiskBytes(create_params.size);
        let zero: crate::db::model::ByteCount = 0.try_into().unwrap();

        match &create_params.disk_backend {
            params::DiskBackend::Distributed {
                disk_source:
                    params::DiskSource::Image { image_id, read_only },
            } => {
                let physical =
                    nexus_db_model::distributed_disk_physical_bytes(
                        virtual_bytes,
                    );
                let phys_bytes = crate::db::model::ByteCount::from(
                    physical.into_byte_count(),
                );
                let writable = if *read_only { zero } else { phys_bytes };
                PhysicalProvisioningCollectionUpdate::new_insert_storage(
                    disk_id,
                    writable,
                    zero,
                    phys_bytes,
                    writable,
                    zero,
                    phys_bytes,
                    project_id,
                    StorageType::Disk,
                    Some(DedupInfo::Disk {
                        origin_column: DedupOriginColumn::Image,
                        origin_id: *image_id,
                        disk_id,
                    }),
                )
                .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(conn)
                .await
                .map_err(|e| {
                    err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                })?;
            }
            params::DiskBackend::Distributed {
                disk_source:
                    params::DiskSource::Snapshot { snapshot_id, read_only },
            } => {
                let physical =
                    nexus_db_model::distributed_disk_physical_bytes(
                        virtual_bytes,
                    );
                let phys_bytes = crate::db::model::ByteCount::from(
                    physical.into_byte_count(),
                );
                let writable = if *read_only { zero } else { phys_bytes };
                PhysicalProvisioningCollectionUpdate::new_insert_storage(
                    disk_id,
                    writable,
                    zero,
                    phys_bytes,
                    writable,
                    zero,
                    phys_bytes,
                    project_id,
                    StorageType::Disk,
                    Some(DedupInfo::Disk {
                        origin_column: DedupOriginColumn::Snapshot,
                        origin_id: *snapshot_id,
                        disk_id,
                    }),
                )
                .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(conn)
                .await
                .map_err(|e| {
                    err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                })?;
            }
            params::DiskBackend::Distributed { .. } => {
                // Blank or importing: writable only, no read-only
                let physical =
                    nexus_db_model::distributed_disk_physical_bytes(
                        virtual_bytes,
                    );
                let phys_bytes = crate::db::model::ByteCount::from(
                    physical.into_byte_count(),
                );
                PhysicalProvisioningCollectionUpdate::new_insert_storage(
                    disk_id,
                    phys_bytes,
                    zero,
                    zero,
                    phys_bytes,
                    zero,
                    zero,
                    project_id,
                    StorageType::Disk,
                    None,
                )
                .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(conn)
                .await
                .map_err(|e| {
                    err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                })?;
            }
            params::DiskBackend::Local {} => {
                let physical =
                    nexus_db_model::local_disk_physical_bytes(virtual_bytes);
                let phys_bytes = crate::db::model::ByteCount::from(
                    physical.into_byte_count(),
                );
                PhysicalProvisioningCollectionUpdate::new_insert_storage(
                    disk_id,
                    phys_bytes,
                    zero,
                    zero,
                    phys_bytes,
                    zero,
                    zero,
                    project_id,
                    StorageType::Disk,
                    None,
                )
                .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(conn)
                .await
                .map_err(|e| {
                    err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                })?;
            }
        }

        Ok(())
    }

    pub async fn disk_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Disk> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use nexus_db_schema::schema::disk::dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl as disk_type_crucible_dsl;
        use nexus_db_schema::schema::disk_type_local_storage::dsl as disk_type_local_storage_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let results = match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::disk, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::disk,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .left_join(
            disk_type_crucible_dsl::disk_type_crucible
                .on(dsl::id.eq(disk_type_crucible_dsl::disk_id)),
        )
        .left_join(
            disk_type_local_storage_dsl::disk_type_local_storage
                .on(dsl::id.eq(disk_type_local_storage_dsl::disk_id)),
        )
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::project_id.eq(authz_project.id()))
        .select((
            model::Disk::as_select(),
            Option::<DiskTypeCrucible>::as_select(),
            Option::<DiskTypeLocalStorage>::as_select(),
        ))
        .get_results_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Self::process_tuples_to_disk_list(&conn, results).await
    }

    /// Attaches a disk to an instance, if both objects:
    /// - Exist
    /// - Are in valid states
    /// - Are under the maximum "attach count" threshold
    pub async fn instance_attach_disk(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_disk: &authz::Disk,
        max_disks: u32,
    ) -> Result<(Instance, Disk), Error> {
        use nexus_db_schema::schema::disk;
        use nexus_db_schema::schema::instance;
        use nexus_db_schema::schema::sled_resource_vmm;

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_attach_disk_states = [
            api::external::DiskState::Creating,
            api::external::DiskState::Detached,
        ];
        let ok_to_attach_disk_state_labels: Vec<_> =
            ok_to_attach_disk_states.iter().map(|s| s.label()).collect();

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // This list of instance attach states is more restrictive than it
        // plausibly could be.
        //
        // We currently only permit attaching disks to stopped instances.
        let ok_to_attach_instance_states = vec![
            db::model::InstanceState::Creating,
            db::model::InstanceState::NoVmm,
        ];

        let attach_update = DiskSetClauseForAttach::new(authz_instance.id());

        let disk = self.disk_get(&opctx, authz_disk.id()).await?;
        let (resource_query, local_storage_disk) = match &disk {
            Disk::Crucible(_) => {
                let query = disk::table.into_boxed().filter(
                    disk::dsl::disk_state
                        .eq_any(ok_to_attach_disk_state_labels),
                );
                (query, false)
            }

            // Attaching a local storage disk to the instance has to be blocked
            // if sled reservation has occured for this instance: local storage
            // allocation records are only created during sled reservation, and
            // importantly a particular configuration of local storage
            // allocations for an instance are only _validated_ during the sled
            // reservation.
            //
            // The instance start saga performs sled reservation, creates the
            // corresponding VMM record, and changes the instance's runtime
            // state all in _separate saga nodes_. This means that there could
            // be an indeterminate amount of time between running the sled
            // reservation query and changing the instance's state.
            //
            // This separation is due to how the VMM state machine is defined:
            //
            // - we cannot create the `omicron.public.vmm` record until we have
            //   allocated sled resources to it (and allocated the Propolis IP),
            //   because that record includes what sled it resides on (as well
            //   as the IP)
            //
            // - we cannot move the `omicron.public.instance` record to the
            //   "starting" state until the `vmm` record exists, because
            //   "starting" is not a real instance state; instead, it is
            //   represented by the instance being in `InstanceState::Vmm` and
            //   having a non-`NULL` `propolis_id` UUID pointing to the `vmm`
            //   record. When an instance has an active VMM, its state is
            //   actually the state of the VMM record, so a "starting" instance
            //   is an instance with a non-`NULL` VMM ID and the corresponding
            //   VMM record is in the "starting" state
            //
            // - it would be _Considered Bad_ to populate the instance record's
            //   `propolis_id` before a corresponding vmm record exists, as
            //   anything looking at the instance's state would try to follow
            //   that foreign key and be unpleasantly surprised when it doesn't
            //   exist. so we can't just make up a UUID and put it in there; we
            //   must actually allocate the VMM before we can transition to
            //   "starting"
            //
            // Execution occurring in different saga nodes means that there
            // could be an indeterminate amount of time between running the sled
            // reservation query and changing the instance's state (see RFD 419,
            // section 3.4). Other client requests can race in those gaps.
            //
            // If a client attaches a local storage disk to an instance after
            // sled reservation occurs but before the instance's start saga
            // moves to starting, and we do not block it, there are several
            // problems that result:
            //
            // - if an allocation does not already exist for the local storage
            //   disk, the instance_start saga will fail (and unwind) when
            //   trying to ensure that the allocation's dataset and zvol exist,
            //   because the allocation_id column is None.
            //
            // - if an allocation does already exist for the local storage disk,
            //   _it may not be for the same sled the VMM is on_. the sled
            //   reservation query would prevent this, but this attach (if not
            //   blocked) happened afterwards. This would mean Nexus would
            //   construct a InstanceSledLocalConfig that contains DelegatedZvol
            //   entries that refer to different sleds, and send that request to
            //   a single sled. Sled-agent would either fail to construct a
            //   propolis zone due to the missing zvol device, or construct the
            //   zone anyway and the device would be missing.
            //
            // - if an allocation does exist already, and it's for the same sled
            //   the VMM is on, it may be colocated on a zpool with another
            //   local storage disk's allocation. again, the sled reservation
            //   query prevents this.
            //
            // - if an allocation does exist already, and it's for the same
            //   sled the VMM is on, and it's on a distinct zpool, then it's
            //   probably fine, but it's safer to let the sled reservation query
            //   validate everything, and it makes a much smaller query here to
            //   block this case as well.
            //
            // `reserve_on_random_sled` will create an entry in
            // `SledResourcesVmm` when the query is successful in finding a VMM
            // reservation, so use that here: if there is a `SledResourcesVmm`
            // record for this instance, then block attachment.
            //
            // Note that depending on our implementation, this may be the code
            // path responsible for attaching disks to already-running instances
            // when we support hot-plug. Local storage disks may never support
            // hot-plug because running zones cannot be reconfigured (aka a new
            // zvol rdsk device cannot be added to a running propolis zone).
            Disk::LocalStorage(_) => {
                let query = disk::table
                    .into_boxed()
                    .filter(
                        disk::dsl::disk_state
                            .eq_any(ok_to_attach_disk_state_labels),
                    )
                    .filter(not(exists(
                        sled_resource_vmm::table.filter(
                            sled_resource_vmm::dsl::instance_id
                                .eq(authz_instance.id()),
                        ),
                    )));

                (query, true)
            }
        };

        let query: AttachToCollectionStatement<model::Disk, _, _> =
            Instance::attach_resource(
                authz_instance.id(),
                authz_disk.id(),
                instance::table.into_boxed().filter(
                    instance::dsl::state
                        .eq_any(ok_to_attach_instance_states)
                        .and(instance::dsl::active_propolis_id.is_null()),
                ),
                resource_query,
                max_disks,
                diesel::update(disk::dsl::disk).set(attach_update),
            );

        let conn = self.pool_connection_authorized(opctx).await?;

        let instance = match query.attach_and_get_result_async(&conn).await {
            Ok((instance, _db_disk)) => {
                // We'll re-fetch the datastore::Disk later, so ignore the
                // model::Disk here
                instance
            }

            Err(e) => match e {
                AttachError::CollectionNotFound => {
                    return Err(Error::not_found_by_id(
                        ResourceType::Instance,
                        &authz_instance.id(),
                    ));
                }
                AttachError::ResourceNotFound => {
                    return Err(Error::not_found_by_id(
                        ResourceType::Disk,
                        &authz_disk.id(),
                    ));
                }
                AttachError::NoUpdate {
                    attached_count,
                    update_condition_satisfied: _,
                    resource,
                    collection,
                } => {
                    let disk_state = resource.state().into();
                    match disk_state {
                        // Idempotent errors: We did not perform an update,
                        // because we're already in the process of attaching.
                        api::external::DiskState::Attached(id)
                            if id == authz_instance.id() =>
                        {
                            collection
                        }
                        api::external::DiskState::Attaching(id)
                            if id == authz_instance.id() =>
                        {
                            collection
                        }
                        // Ok-to-attach disk states: Inspect the state to infer
                        // why we did not attach.
                        api::external::DiskState::Creating
                        | api::external::DiskState::Detached => {
                            if collection.runtime_state.propolis_id.is_some() {
                                return Err(Error::invalid_request(
                                    "cannot attach disk: instance is not \
                                        fully stopped",
                                ));
                            }
                            match collection.runtime_state.nexus_state.state() {
                                // Ok-to-be-attached instance states:
                                api::external::InstanceState::Creating
                                | api::external::InstanceState::Stopped => {
                                    // The disk is ready to be attached, and the
                                    // instance is ready to be attached. Perhaps
                                    // we are at attachment capacity?
                                    if attached_count == i64::from(max_disks) {
                                        return Err(Error::invalid_request(
                                            &format!(
                                                "cannot attach more than \
                                                {max_disks} disks to instance",
                                            ),
                                        ));
                                    }

                                    // Was this attach of a local storage disk
                                    // blocked due to an existing sled resource
                                    // record?
                                    if local_storage_disk {
                                        use sled_resource_vmm::dsl;

                                        let record = dsl::sled_resource_vmm
                                            .filter(
                                                dsl::instance_id
                                                    .eq(authz_instance.id()),
                                            )
                                            .select(dsl::instance_id)
                                            .execute_async(&*conn)
                                            .await
                                            .optional()
                                            .map_err(|e| {
                                                public_error_from_diesel(
                                                    e,
                                                    ErrorHandler::Server,
                                                )
                                            })?;

                                        if record.is_some() {
                                            let s = "cannot attach local \
                                                storage disk: instance is \
                                                starting";

                                            return Err(Error::conflict(s));
                                        }
                                    }

                                    // We can't attach, but the error hasn't
                                    // helped us infer why.
                                    return Err(Error::internal_error(
                                        "cannot attach disk",
                                    ));
                                }
                                // Not okay-to-be-attached instance states:
                                _ => {
                                    return Err(Error::invalid_request(
                                        &format!(
                                            "cannot attach disk to instance in \
                                            {} state",
                                            collection
                                                .runtime_state
                                                .nexus_state
                                                .state(),
                                        ),
                                    ));
                                }
                            }
                        }
                        // Not-okay-to-attach disk states: The disk is attached
                        // elsewhere.
                        api::external::DiskState::Attached(_)
                        | api::external::DiskState::Attaching(_)
                        | api::external::DiskState::Detaching(_) => {
                            return Err(Error::invalid_request(&format!(
                                "cannot attach disk \"{}\": disk is attached \
                                to another instance",
                                resource.name().as_str(),
                            )));
                        }
                        _ => {
                            return Err(Error::invalid_request(&format!(
                                "cannot attach disk \"{}\": invalid state {}",
                                resource.name().as_str(),
                                disk_state,
                            )));
                        }
                    }
                }
                AttachError::DatabaseError(e) => {
                    return Err(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ));
                }
            },
        };

        // Re-fetch the disk to get the updates
        let disk = self.disk_get(&opctx, authz_disk.id()).await?;
        Ok((instance, disk))
    }

    pub async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_disk: &authz::Disk,
    ) -> Result<Disk, Error> {
        use nexus_db_schema::schema::{disk, instance};

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_detach_disk_states =
            [api::external::DiskState::Attached(authz_instance.id())];
        let ok_to_detach_disk_state_labels: Vec<_> =
            ok_to_detach_disk_states.iter().map(|s| s.label()).collect();

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // This list of instance detach states is more restrictive than it
        // plausibly could be.
        //
        // We currently only permit detaching disks from stopped instances.
        let ok_to_detach_instance_states = vec![
            db::model::InstanceState::Creating,
            db::model::InstanceState::NoVmm,
            db::model::InstanceState::Failed,
        ];

        let detached_label = api::external::DiskState::Detached.label();

        let conn = self.pool_connection_authorized(opctx).await?;

        let _db_disk = Instance::detach_resource(
            authz_instance.id(),
            authz_disk.id(),
            instance::table
                .into_boxed()
                .filter(instance::dsl::state
                        .eq_any(ok_to_detach_instance_states)
                        .and(instance::dsl::active_propolis_id.is_null())
                        .and(
                            instance::dsl::boot_disk_id.ne(authz_disk.id())
                                .or(instance::dsl::boot_disk_id.is_null())
                        )),
            disk::table
                .into_boxed()
                .filter(disk::dsl::disk_state.eq_any(ok_to_detach_disk_state_labels)),
            diesel::update(disk::dsl::disk)
                .set((
                    disk::dsl::disk_state.eq(detached_label),
                    disk::dsl::attach_instance_id.eq(Option::<Uuid>::None),
                    disk::dsl::slot.eq(Option::<i16>::None)
                ))
        )
        .detach_and_get_result_async(&conn)
        .await
        .or_else(|e: DetachError<model::Disk, _, _>| {
            match e {
                DetachError::CollectionNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Instance,
                        &authz_instance.id(),
                    ))
                },
                DetachError::ResourceNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Disk,
                        &authz_disk.id(),
                    ))
                },
                DetachError::NoUpdate { resource, collection } => {
                    let disk_state = resource.state().into();
                    match disk_state {
                        // Idempotent errors: We did not perform an update,
                        // because we're already in the process of detaching.
                        api::external::DiskState::Detached => {
                            return Ok(resource);
                        }
                        api::external::DiskState::Detaching(id) if id == authz_instance.id() => {
                            return Ok(resource);
                        }
                        // Ok-to-detach disk states: Inspect the state to infer
                        // why we did not detach.
                        api::external::DiskState::Attached(id) if id == authz_instance.id() => {
                            if collection.runtime_state.propolis_id.is_some() {
                                return Err(
                                    Error::invalid_request(
                                        "cannot detach disk: instance is not \
                                        fully stopped"
                                    )
                                );
                            }
                            match collection.runtime_state.nexus_state.state() {
                                // Ok-to-be-detached instance states:
                                api::external::InstanceState::Creating |
                                api::external::InstanceState::Stopped => {
                                    if collection.boot_disk_id == Some(authz_disk.id()) {
                                        return Err(Error::conflict(
                                            "boot disk cannot be detached"
                                        ));
                                    }

                                    // We can't detach, but the error hasn't
                                    // helped us infer why.
                                    return Err(Error::internal_error(
                                        "cannot detach disk"
                                    ));
                                }
                                // Not okay-to-be-detached instance states:
                                _ => {
                                    Err(Error::invalid_request(&format!(
                                        "cannot detach disk from instance in {} state",
                                        collection.runtime_state.nexus_state.state(),
                                    )))
                                }
                            }
                        },
                        api::external::DiskState::Attaching(id) if id == authz_instance.id() => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": disk is currently being attached",
                                resource.name().as_str(),
                            )))
                        },
                        // Not-okay-to-detach disk states: The disk is attached elsewhere.
                        api::external::DiskState::Attached(_) |
                        api::external::DiskState::Attaching(_) |
                        api::external::DiskState::Detaching(_) => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": disk is attached to another instance",
                                resource.name().as_str(),
                            )))
                        }
                        _ => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": invalid state {}",
                                resource.name().as_str(),
                                disk_state,
                            )))
                        }
                    }
                },
                DetachError::DatabaseError(e) => {
                    Err(public_error_from_diesel(e, ErrorHandler::Server))
                },
            }
        })?;

        let disk = self.disk_get(&opctx, authz_disk.id()).await?;

        Ok(disk)
    }

    pub async fn disk_update_runtime(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        new_runtime: &DiskRuntimeState,
    ) -> Result<bool, Error> {
        // TODO-security This permission might be overloaded here.  The way disk
        // runtime updates work is that the caller in Nexus first updates the
        // Sled Agent to make a change, then updates to the database to reflect
        // that change.  So by the time we get here, we better have already done
        // an authz check, or we will have already made some unauthorized change
        // to the system!  At the same time, we don't want just anybody to be
        // able to modify the database state.  So we _do_ still want an authz
        // check here.  Arguably it's for a different kind of action, but it
        // doesn't seem that useful to split it out right now.
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();
        use nexus_db_schema::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::state_generation.lt(new_runtime.generation))
            .set(new_runtime.clone())
            .check_if_exists::<model::Disk>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_set_pantry(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        pantry_address: SocketAddrV6,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::disk::dsl as disk_dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl;

        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();

        let updated = diesel::update(dsl::disk_type_crucible)
            .filter(diesel::dsl::exists(
                disk_dsl::disk
                    .filter(disk_dsl::id.eq(disk_id))
                    .filter(disk_dsl::time_deleted.is_null()),
            ))
            .filter(dsl::disk_id.eq(disk_id))
            .set(dsl::pantry_address.eq(pantry_address.to_string()))
            .check_if_exists::<model::DiskTypeCrucible>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_clear_pantry(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::disk::dsl as disk_dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl;

        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();

        let updated = diesel::update(dsl::disk_type_crucible)
            .filter(diesel::dsl::exists(
                disk_dsl::disk
                    .filter(disk_dsl::id.eq(disk_id))
                    .filter(disk_dsl::time_deleted.is_null()),
            ))
            .filter(dsl::disk_id.eq(disk_id))
            .set(&DiskTypeCrucibleUpdate { pantry_address: None })
            .check_if_exists::<model::DiskTypeCrucible>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    /// Updates a disk record to indicate it has been deleted.
    ///
    /// Returns the disk before any modifications are made by this function.
    ///
    /// Does not attempt to modify any resources (e.g. regions) which may
    /// belong to the disk.
    // TODO: Delete me (this function, not the disk!), ensure all datastore
    // access is auth-checked.
    //
    // Here's the deal: We have auth checks on access to the database - at the
    // time of writing this comment, only a subset of access is protected, and
    // "Delete Disk" is actually one of the first targets of this auth check.
    //
    // However, there are contexts where we want to delete disks *outside* of
    // calling the HTTP API-layer "delete disk" endpoint. As one example, during
    // the "undo" part of the disk creation saga, we want to allow users to
    // delete the disk they (partially) created.
    //
    // This gets a little tricky mapping back to user permissions - a user
    // SHOULD be able to create a disk with the "create" permission, without the
    // "delete" permission. To still make the call internally, we'd basically
    // need to manufacture a token that identifies the ability to "create a
    // disk, or delete a very specific disk with ID = ...".
    pub async fn project_delete_disk_no_auth(
        &self,
        disk_id: &Uuid,
        ok_to_delete_states: &[api::external::DiskState],
    ) -> Result<model::Disk, Error> {
        use nexus_db_schema::schema::disk::dsl;
        let conn = self.pool_connection_unauthorized().await?;
        let now = Utc::now();

        let ok_to_delete_state_labels: Vec<_> =
            ok_to_delete_states.iter().map(|s| s.label()).collect();
        let destroyed = api::external::DiskState::Destroyed.label();

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::disk_state.eq_any(ok_to_delete_state_labels))
            .filter(dsl::attach_instance_id.is_null())
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<model::Disk>(*disk_id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Disk,
                        LookupType::ById(*disk_id),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(result.found),
            UpdateStatus::NotUpdatedButExists => {
                let disk = result.found;
                let disk_state = disk.state();
                if disk.time_deleted().is_some()
                    && disk_state.state()
                        == &api::external::DiskState::Destroyed
                {
                    // To maintain idempotency, if the disk has already been
                    // destroyed, don't throw an error.
                    return Ok(disk);
                } else if !ok_to_delete_states.contains(disk_state.state()) {
                    return Err(Error::invalid_request(format!(
                        "disk cannot be deleted in state \"{}\"",
                        disk.runtime_state.disk_state
                    )));
                } else if disk_state.is_attached() {
                    return Err(Error::invalid_request("disk is attached"));
                } else {
                    // NOTE: This is a "catch-all" error case, more specific
                    // errors should be preferred as they're more actionable.
                    return Err(Error::InternalError {
                        internal_message: String::from(
                            "disk exists, but cannot be deleted",
                        ),
                    });
                }
            }
        }
    }

    /// Set a disk to faulted and un-delete it
    ///
    /// If the disk delete saga unwinds, then the disk should _not_ remain
    /// deleted: disk delete saga should be triggered again in order to fully
    /// complete, and the only way to do that is to un-delete the disk. Set it
    /// to faulted to ensure that it won't be used. Use the disk's UUID as part
    /// of its new name to ensure that even if a user created another disk that
    /// shadows this "phantom" disk the original can still be un-deleted and
    /// faulted.
    ///
    /// It's worth pointing out that it's possible that the user created a disk,
    /// then used that disk's ID to make a new disk with the same name as this
    /// function would have picked when undeleting the original disk. In the
    /// event that the original disk's delete saga unwound, this would cause
    /// that unwind to fail at this step, and would cause a stuck saga that
    /// requires manual intervention. The fixes as part of addressing issue 3866
    /// should greatly reduce the number of disk delete sagas that unwind, but
    /// this possibility does exist. To any customer reading this: please don't
    /// name your disks `deleted-{another disk's id}` :)
    pub async fn project_undelete_disk_set_faulted_no_auth(
        &self,
        disk_id: &Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::disk::dsl;
        let conn = self.pool_connection_unauthorized().await?;

        let faulted = api::external::DiskState::Faulted.label();

        // If only the UUID is used, you will hit "name cannot be a UUID to
        // avoid ambiguity with IDs". Add a small prefix to avoid this, and use
        // "deleted" to be unambigious to the user about what they should do
        // with this disk.
        let new_name = format!("deleted-{disk_id}");

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_not_null())
            .filter(dsl::id.eq(*disk_id))
            .set((
                dsl::time_deleted.eq(None::<DateTime<Utc>>),
                dsl::disk_state.eq(faulted),
                dsl::name.eq(new_name),
            ))
            .check_if_exists::<model::Disk>(*disk_id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Disk,
                        LookupType::ById(*disk_id),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => {
                let disk = result.found;
                let disk_state = disk.state();

                if disk.time_deleted().is_none()
                    && disk_state.state() == &api::external::DiskState::Faulted
                {
                    // To maintain idempotency, if the disk has already been
                    // faulted, don't throw an error.
                    return Ok(());
                } else {
                    // NOTE: This is a "catch-all" error case, more specific
                    // errors should be preferred as they're more actionable.
                    return Err(Error::InternalError {
                        internal_message: String::from(
                            "disk exists, but cannot be faulted",
                        ),
                    });
                }
            }
        }
    }

    /// Find disks that have been deleted but still have a
    /// `virtual_provisioning_resource` record: this indicates that a disk
    /// delete saga partially succeeded, then unwound, which (before the fixes
    /// in customer-support#58) would mean the disk was deleted but the project
    /// it was in could not be deleted (due to an erroneous number of bytes
    /// "still provisioned").
    pub async fn find_phantom_disks(&self) -> ListResultVec<model::Disk> {
        use nexus_db_schema::schema::disk::dsl as disk_dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl;
        use nexus_db_schema::schema::virtual_provisioning_resource::dsl as resource_dsl;
        use nexus_db_schema::schema::volume::dsl as volume_dsl;

        let conn = self.pool_connection_unauthorized().await?;

        let potential_phantom_disks: Vec<(
            model::Disk,
            Option<VirtualProvisioningResource>,
            Option<Volume>,
        )> = disk_dsl::disk
            // only Crucible disks have volumes
            .inner_join(
                dsl::disk_type_crucible.on(dsl::disk_id.eq(disk_dsl::id)),
            )
            .left_join(
                resource_dsl::virtual_provisioning_resource
                    .on(resource_dsl::id.eq(disk_dsl::id)),
            )
            .left_join(volume_dsl::volume.on(dsl::volume_id.eq(volume_dsl::id)))
            .filter(disk_dsl::time_deleted.is_not_null())
            .select((
                model::Disk::as_select(),
                Option::<VirtualProvisioningResource>::as_select(),
                Option::<Volume>::as_select(),
            ))
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // The first forward steps of the disk delete saga (plus the volume
        // delete sub saga) are as follows:
        //
        // 1. soft-delete the disk
        // 2. call virtual_provisioning_collection_delete_disk
        // 3. soft-delete the disk's volume
        //
        // Before the fixes as part of customer-support#58, steps 1 and 3 did
        // not have undo steps, where step 2 did. In order to detect when the
        // disk delete saga unwound, find entries where
        //
        // 1. the disk and volume are soft-deleted
        // 2. the `virtual_provisioning_resource` exists
        //
        // It's important not to conflict with any currently running disk delete
        // saga.

        Ok(potential_phantom_disks
            .into_iter()
            .filter(|(disk, resource, volume)| {
                if let Some(volume) = volume {
                    // In this branch, the volume record exists. Because it was
                    // returned by the query above, if it is soft-deleted we
                    // then know the saga unwound before the volume record could
                    // be hard deleted. This won't conflict with a running disk
                    // delete saga, because the resource record should be None
                    // if the disk and volume were already soft deleted (if
                    // there is one, the saga will be at or past step 3).
                    disk.time_deleted().is_some()
                        && volume.time_deleted.is_some()
                        && resource.is_some()
                } else {
                    // In this branch, the volume record was hard-deleted. The
                    // saga could still have unwound after hard deleting the
                    // volume record, so proceed with filtering. This won't
                    // conflict with a running disk delete saga because the
                    // resource record should be None if the disk was soft
                    // deleted and the volume was hard deleted (if there is one,
                    // the saga should be almost finished as the volume hard
                    // delete is the last thing it does).
                    disk.time_deleted().is_some() && resource.is_some()
                }
            })
            .map(|(disk, _, _)| disk)
            .collect())
    }

    /// Returns a Some(disk) that has a matching volume ID, None if no disk
    /// matches that volume ID, or an error. Only disks of type `Crucible` have
    /// volumes, so that is the returned type.
    pub async fn disk_for_volume_id(
        &self,
        volume_id: VolumeUuid,
    ) -> LookupResult<Option<CrucibleDisk>> {
        let conn = self.pool_connection_unauthorized().await?;

        use nexus_db_schema::schema::disk::dsl as disk_dsl;
        use nexus_db_schema::schema::disk_type_crucible::dsl;

        let maybe_tuple = dsl::disk_type_crucible
            .inner_join(disk_dsl::disk.on(disk_dsl::id.eq(dsl::disk_id)))
            .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .select((model::Disk::as_select(), DiskTypeCrucible::as_select()))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(maybe_tuple.map(|(disk, disk_type_crucible)| CrucibleDisk {
            disk,
            disk_type_crucible,
        }))
    }

    /// Create a read-only disk from an existing snapshot or image.
    pub async fn project_create_read_only_disk(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        disk_id: &Uuid,
        params: &params::DiskCreate,
    ) -> CreateResult<db::datastore::Disk> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let params::DiskBackend::Distributed { ref disk_source } =
            params.disk_backend
        else {
            // This is an internal error rather than an invalid argument or
            // similar, because Nexus should not have called this function with
            // a local storage backend.
            return Err(Error::internal_error(
                "invalid disk_backend for `project_create_readonly_disk` \
                 (must be `Distributed`)",
            ));
        };
        // Check that `params` refers either to an image or snapshot as the
        // source, and look it up now to perform authz checks. We must pass the
        // authz resource into the transaction so that it can refresh the lookup
        // to insure the transaction still exists once the transaction executes.
        let src = match disk_source {
            &params::DiskSource::Image { image_id, read_only: true } => {
                let (_, authz_image, _) = LookupPath::new(opctx, self)
                    .image_id(image_id)
                    .fetch()
                    .await?;
                ReadOnlyDiskSource::Image(authz_image)
            }
            &params::DiskSource::Snapshot { snapshot_id, read_only: true } => {
                let (_, _, authz_snapshot, _) = LookupPath::new(opctx, self)
                    .snapshot_id(snapshot_id)
                    .fetch()
                    .await?;
                ReadOnlyDiskSource::Snapshot(authz_snapshot)
            }
            src => {
                // This is an internal error rather than an invalid argument or
                // similar, because Nexus should not have called this function with
                // a non-readonly disk source
                return Err(Error::InternalError {
                    internal_message: format!(
                        "invalid `DiskSource` for \
                         `project_create_readonly_disk`: {src:?}"
                    ),
                });
            }
        };

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let disk = self
            .transaction_retry_wrapper("project_create_read_only_disk")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let params = &params;
                let src = &src;
                async move {
                    Self::project_create_read_only_disk_in_txn(
                        &conn,
                        err,
                        authz_project,
                        disk_id,
                        params,
                        src,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(disk)
    }

    /// Atomically create a read-only disk record AND insert virtual +
    /// physical provisioning in a single database transaction.
    pub async fn project_create_read_only_disk_and_provision(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        disk_id: &Uuid,
        params: &params::DiskCreate,
    ) -> CreateResult<db::datastore::Disk> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let params::DiskBackend::Distributed { ref disk_source } =
            params.disk_backend
        else {
            return Err(Error::internal_error(
                "invalid disk_backend for \
                 `project_create_read_only_disk_and_provision` \
                 (must be `Distributed`)",
            ));
        };

        let src = match disk_source {
            &params::DiskSource::Image { image_id, read_only: true } => {
                let (_, authz_image, _) = LookupPath::new(opctx, self)
                    .image_id(image_id)
                    .fetch()
                    .await?;
                ReadOnlyDiskSource::Image(authz_image)
            }
            &params::DiskSource::Snapshot { snapshot_id, read_only: true } => {
                let (_, _, authz_snapshot, _) = LookupPath::new(opctx, self)
                    .snapshot_id(snapshot_id)
                    .fetch()
                    .await?;
                ReadOnlyDiskSource::Snapshot(authz_snapshot)
            }
            src => {
                return Err(Error::InternalError {
                    internal_message: format!(
                        "invalid `DiskSource` for \
                         `project_create_read_only_disk_and_provision`: \
                         {src:?}"
                    ),
                });
            }
        };

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let disk = self
            .transaction_retry_wrapper(
                "project_create_read_only_disk_and_provision",
            )
            .transaction(&conn, |conn| {
                let err = err.clone();
                let params = &params;
                let src = &src;
                async move {
                    // Step 1: Create the read-only disk record.
                    let created_disk =
                        Self::project_create_read_only_disk_in_txn(
                            &conn,
                            err.clone(),
                            authz_project,
                            disk_id,
                            params,
                            src,
                        )
                        .await?;

                    // Step 2: Virtual provisioning.
                    use crate::db::queries::virtual_provisioning_collection_update::VirtualProvisioningCollectionUpdate;
                    VirtualProvisioningCollectionUpdate::new_insert_storage(
                        *disk_id,
                        crate::db::model::ByteCount::from(params.size),
                        authz_project.id(),
                        StorageType::Disk,
                    )
                    .get_results_async::<crate::db::model::VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::virtual_provisioning_collection_update::from_diesel(e))
                    })?;

                    // Step 3: Physical provisioning.
                    Self::insert_physical_provisioning_in_txn(
                        &conn,
                        err.clone(),
                        *disk_id,
                        authz_project.id(),
                        params,
                    )
                    .await?;

                    Ok(created_disk)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(disk)
    }

    async fn project_create_read_only_disk_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        authz_project: &authz::Project,
        disk_id: &Uuid,
        params: &params::DiskCreate,
        src: &ReadOnlyDiskSource,
    ) -> Result<db::datastore::Disk, diesel::result::Error> {
        use crate::db::datastore::CrucibleTargets;
        use crate::db::datastore::VolumeCheckoutReason;
        use crate::db::datastore::read_only_resources_associated_with_volume;
        use sled_agent_client::VolumeConstructionRequest;

        // For idempotency, first check if the disk already exists, and if it
        // does, just return that.
        let maybe_disk = {
            use nexus_db_schema::schema::disk::dsl;
            dsl::disk
                .filter(dsl::id.eq(*disk_id))
                .select(db::model::Disk::as_select())
                .get_result_async(conn)
                .await
                .optional()?
        };
        if let Some(disk) = maybe_disk {
            if disk.disk_type != db::model::DiskType::Crucible {
                // this is *probably* a UUID collision. Seems bad.
                return Err(err.bail(Error::InternalError {
                    internal_message: format!(
                        "weird! disk {disk_id} already exists, \
                         but its type was {:?} (not Crucible)",
                        disk.disk_type,
                    ),
                }));
            }
            use nexus_db_schema::schema::disk_type_crucible::dsl;
            let disk_type_crucible = dsl::disk_type_crucible
                .filter(dsl::disk_id.eq(*disk_id))
                .select(db::model::DiskTypeCrucible::as_select())
                .get_result_async(conn)
                .await?;
            return Ok(db::datastore::Disk::Crucible(CrucibleDisk {
                disk,
                disk_type_crucible,
            }));
        }

        // Re-fetch the source image or snapshot again within the transaction to
        // ensure that it still exists (it could have been deleted since the
        // lookup in `project_create_read_only_disk`...)
        let (volume_id, block_size) = match src {
            ReadOnlyDiskSource::Snapshot(authz_snapshot) => {
                let maybe_snapshot: Option<db::model::Snapshot> = {
                    use nexus_db_schema::schema::snapshot::dsl;
                    dsl::snapshot
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_snapshot.id()))
                        .select(db::model::Snapshot::as_select())
                        .get_result_async(conn)
                        .await
                        .optional()?
                };

                let Some(snapshot) = maybe_snapshot else {
                    return Err(err.bail(authz_snapshot.not_found()));
                };

                (snapshot.volume_id(), snapshot.block_size)
            }
            ReadOnlyDiskSource::Image(authz_image) => {
                let maybe_image: Option<db::model::Image> = {
                    use nexus_db_schema::schema::image::dsl;
                    dsl::image
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(authz_image.id()))
                        .select(db::model::Image::as_select())
                        .get_result_async(conn)
                        .await
                        .optional()?
                };

                let Some(image) = maybe_image else {
                    return Err(err.bail(authz_image.not_found()));
                };
                (image.volume_id(), image.block_size)
            }
        };

        let sub_err = OptionalError::new();
        let copy_of_volume = Self::volume_checkout_in_txn(
            conn,
            sub_err.clone(),
            volume_id,
            VolumeCheckoutReason::ReadOnlyCopy,
        )
        .await
        .map_err(|e| {
            if let Some(sub_err) = sub_err.take() {
                err.bail(Error::from(sub_err).internal_context(
                    "failed to checkout base volume for readonly disk",
                ))
            } else {
                e
            }
        })?;

        let copy_of_vcr: VolumeConstructionRequest =
            serde_json::from_str(copy_of_volume.data()).map_err(|e| {
                err.bail(Error::InternalError {
                    internal_message: format!(
                        "failed to deserialize base volume VCR: {e}"
                    ),
                })
            })?;

        let crucible_targets = {
            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &copy_of_vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        let sub_err = OptionalError::new();
        let read_only_disk_volume =
            Self::volume_create_in_txn(
                conn,
                sub_err.clone(),
                VolumeUuid::new_v4(),
                copy_of_vcr,
                crucible_targets,
            )
            .await
            .map_err(|e| {
                if let Some(sub_err) = sub_err.take() {
                    err.bail(Error::from(sub_err).internal_context(
                        "failed to create read-only disk volume",
                    ))
                } else {
                    e
                }
            })?;

        // No additional work is required to create a read-only disk from a
        // snapshot or image, as the snapshot or image's volume already exists.
        // Thus, the new disk begins its life in the `Detached` state, rather
        // than `Creating`. As soon as the database records are created, the
        // disk is ready for use.
        let runtime_initial = db::model::DiskRuntimeState::new().detach();
        let disk = db::model::Disk::new(
            *disk_id,
            authz_project.id(),
            &params,
            block_size,
            runtime_initial,
            db::model::DiskType::Crucible,
        );
        let params::DiskBackend::Distributed { ref disk_source } =
            params.disk_backend
        else {
            return Err(err.bail(Error::internal_error(
                "`project_create_read_only_disk` called with \
                 non-Distributed backend",
            )));
        };
        let disk_type_crucible = db::model::DiskTypeCrucible::new(
            *disk_id,
            read_only_disk_volume.id(),
            disk_source,
        );

        let crucible_disk =
            db::datastore::CrucibleDisk { disk, disk_type_crucible };

        let disk = Self::project_create_disk_in_txn(
            conn,
            err,
            authz_project,
            db::datastore::Disk::Crucible(crucible_disk),
        )
        .await?;

        Ok(disk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::external_api::params;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_undelete_disk_set_faulted_idempotent() {
        let logctx =
            dev::test_setup_log("test_undelete_disk_set_faulted_idempotent");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, db_datastore) = (db.opctx(), db.datastore());

        let silo_id = opctx.authn.actor().unwrap().silo_id().unwrap();

        let (authz_project, _db_project) = db_datastore
            .project_create(
                &opctx,
                Project::new(
                    silo_id,
                    params::ProjectCreate {
                        identity: external::IdentityMetadataCreateParams {
                            name: "testpost".parse().unwrap(),
                            description: "please ignore".to_string(),
                        },
                    },
                ),
            )
            .await
            .unwrap();

        let disk_id = Uuid::new_v4();

        let disk_source = params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        };

        let create_params = params::DiskCreate {
            identity: external::IdentityMetadataCreateParams {
                name: "first-post".parse().unwrap(),
                description: "just trying things out".to_string(),
            },
            disk_backend: params::DiskBackend::Distributed {
                disk_source: disk_source.clone(),
            },
            size: external::ByteCount::from(2147483648),
        };

        let disk = db::model::Disk::new(
            disk_id,
            authz_project.id(),
            &create_params,
            db::model::BlockSize::Traditional,
            DiskRuntimeState::new(),
            db::model::DiskType::Crucible,
        );

        let disk_type_crucible = db::model::DiskTypeCrucible::new(
            disk_id,
            VolumeUuid::new_v4(),
            &disk_source,
        );

        let disk = db_datastore
            .project_create_disk(
                &opctx,
                &authz_project,
                db::datastore::Disk::Crucible(db::datastore::CrucibleDisk {
                    disk,
                    disk_type_crucible,
                }),
            )
            .await
            .unwrap();

        let (.., authz_disk, db_disk) = LookupPath::new(&opctx, db_datastore)
            .disk_id(disk.id())
            .fetch()
            .await
            .unwrap();

        db_datastore
            .disk_update_runtime(
                &opctx,
                &authz_disk,
                &db_disk.runtime().detach(),
            )
            .await
            .unwrap();

        db_datastore
            .project_delete_disk_no_auth(
                &authz_disk.id(),
                &[external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Assert initial state - deleting the Disk will make LookupPath::fetch
        // not work.
        {
            LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap_err();
        }

        // Function under test: call this twice to ensure it's idempotent

        db_datastore
            .project_undelete_disk_set_faulted_no_auth(&authz_disk.id())
            .await
            .unwrap();

        // Assert state change

        {
            let (.., db_disk) = LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap();

            assert!(db_disk.time_deleted().is_none());
            assert_eq!(
                db_disk.runtime().disk_state,
                external::DiskState::Faulted.label().to_string()
            );
        }

        db_datastore
            .project_undelete_disk_set_faulted_no_auth(&authz_disk.id())
            .await
            .unwrap();

        // Assert state is the same after the second call

        {
            let (.., db_disk) = LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap();

            assert!(db_disk.time_deleted().is_none());
            assert_eq!(
                db_disk.runtime().disk_state,
                external::DiskState::Faulted.label().to_string()
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
