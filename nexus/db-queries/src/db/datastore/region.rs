// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Region`]s.

use super::DataStore;
use super::RunnableQuery;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::CrucibleDataset;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::Region;
use crate::db::model::SqlU16;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::region_allocation;
use crate::db::queries::regions_hard_delete;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_config::RegionAllocationStrategy;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::VolumeUuid;
use slog::Logger;
use std::net::SocketAddrV6;
use uuid::Uuid;

pub enum RegionAllocationFor {
    /// Allocate region(s) for a disk volume
    DiskVolume { volume_id: VolumeUuid },

    /// Allocate region(s) for a snapshot volume, which may have read-only
    /// targets.
    SnapshotVolume { volume_id: VolumeUuid, snapshot_id: Uuid },
}

/// Describe the region(s) to be allocated
pub enum RegionAllocationParameters<'a> {
    FromDiskSource {
        disk_source: &'a params::DiskSource,
        size: external::ByteCount,
    },

    FromRaw {
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u64,
    },
}

impl DataStore {
    pub(super) fn get_allocated_regions_query(
        volume_id: VolumeUuid,
    ) -> impl RunnableQuery<(CrucibleDataset, Region)> {
        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;
        region_dsl::region
            .filter(region_dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .inner_join(
                dataset_dsl::crucible_dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .select((CrucibleDataset::as_select(), Region::as_select()))
    }

    /// Gets allocated regions for a disk, and the datasets to which those
    /// regions belong.
    ///
    /// Note that this function does not validate liveness of the Disk, so it
    /// may be used in a context where the disk is being deleted.
    pub async fn get_allocated_regions(
        &self,
        volume_id: VolumeUuid,
    ) -> Result<Vec<(CrucibleDataset, Region)>, Error> {
        Self::get_allocated_regions_query(volume_id)
            .get_results_async::<(CrucibleDataset, Region)>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_region(&self, region_id: Uuid) -> Result<Region, Error> {
        use nexus_db_schema::schema::region::dsl;
        dsl::region
            .filter(dsl::id.eq(region_id))
            .select(Region::as_select())
            .get_result_async::<Region>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_region_optional(
        &self,
        region_id: Uuid,
    ) -> Result<Option<Region>, Error> {
        use nexus_db_schema::schema::region::dsl;
        dsl::region
            .filter(dsl::id.eq(region_id))
            .select(Region::as_select())
            .get_result_async::<Region>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn get_block_size_from_disk_source(
        &self,
        opctx: &OpContext,
        disk_source: &params::DiskSource,
    ) -> Result<db::model::BlockSize, Error> {
        match &disk_source {
            params::DiskSource::Blank { block_size } => {
                Ok(db::model::BlockSize::try_from(*block_size)
                    .map_err(|e| Error::invalid_request(&e.to_string()))?)
            }
            params::DiskSource::Snapshot { snapshot_id } => {
                let (.., db_snapshot) = LookupPath::new(opctx, self)
                    .snapshot_id(*snapshot_id)
                    .fetch()
                    .await?;

                Ok(db_snapshot.block_size)
            }
            params::DiskSource::Image { image_id } => {
                let (.., db_image) = LookupPath::new(opctx, self)
                    .image_id(*image_id)
                    .fetch()
                    .await?;

                Ok(db_image.block_size)
            }
            params::DiskSource::ImportingBlocks { block_size } => {
                Ok(db::model::BlockSize::try_from(*block_size)
                    .map_err(|e| Error::invalid_request(&e.to_string()))?)
            }
        }
    }

    // TODO for now, extent size is fixed at 64 MiB. In the future, this may be
    // tunable at runtime.
    pub const EXTENT_SIZE: u64 = 64_u64 << 20;

    /// Given a block size and total disk size, get Crucible allocation values
    pub fn get_crucible_allocation(
        block_size: &db::model::BlockSize,
        size: external::ByteCount,
    ) -> (u64, u64) {
        let blocks_per_extent =
            Self::EXTENT_SIZE / u64::from(block_size.to_bytes());

        let size = size.to_bytes();

        // allocate enough extents to fit all the disk blocks, rounding up.
        let extent_count = size.div_ceil(Self::EXTENT_SIZE);

        (blocks_per_extent, extent_count)
    }

    /// Idempotently allocates enough regions to back a disk.
    ///
    /// Returns the allocated regions, as well as the datasets to which they
    /// belong.
    pub async fn disk_region_allocate(
        &self,
        opctx: &OpContext,
        volume_id: VolumeUuid,
        disk_source: &params::DiskSource,
        size: external::ByteCount,
        allocation_strategy: &RegionAllocationStrategy,
    ) -> Result<Vec<(CrucibleDataset, Region)>, Error> {
        self.arbitrary_region_allocate(
            opctx,
            RegionAllocationFor::DiskVolume { volume_id },
            RegionAllocationParameters::FromDiskSource { disk_source, size },
            allocation_strategy,
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await
    }

    /// Idempotently allocates an arbitrary number of regions for a volume.
    ///
    /// For regular disk creation, this will be REGION_REDUNDANCY_THRESHOLD.
    ///
    /// For region replacement, it's important to allocate the *new* region for
    /// a volume while respecting the current region allocation strategy.  This
    /// requires setting `num_regions_required` to one more than the current
    /// level for a volume. If a single region is allocated in isolation this
    /// could land on the same dataset as one of the existing volume's regions.
    ///
    /// For allocating for snapshot volumes, it's important to take into account
    /// `region_snapshot`s that may be used as some of the targets in the region
    /// set, representing read-only downstairs served out of a ZFS snapshot
    /// instead of a dataset.
    ///
    /// Returns the allocated regions, as well as the datasets to which they
    /// belong.
    pub async fn arbitrary_region_allocate(
        &self,
        opctx: &OpContext,
        region_for: RegionAllocationFor,
        region_parameters: RegionAllocationParameters<'_>,
        allocation_strategy: &RegionAllocationStrategy,
        num_regions_required: usize,
    ) -> Result<Vec<(CrucibleDataset, Region)>, Error> {
        let (volume_id, maybe_snapshot_id) = match region_for {
            RegionAllocationFor::DiskVolume { volume_id } => (volume_id, None),

            RegionAllocationFor::SnapshotVolume { volume_id, snapshot_id } => {
                (volume_id, Some(snapshot_id))
            }
        };

        let (block_size, blocks_per_extent, extent_count) =
            match region_parameters {
                RegionAllocationParameters::FromDiskSource {
                    disk_source,
                    size,
                } => {
                    let block_size = self
                        .get_block_size_from_disk_source(opctx, &disk_source)
                        .await?;

                    let (blocks_per_extent, extent_count) =
                        Self::get_crucible_allocation(&block_size, size);

                    (
                        u64::from(block_size.to_bytes()),
                        blocks_per_extent,
                        extent_count,
                    )
                }

                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                } => (block_size, blocks_per_extent, extent_count),
            };

        let query = region_allocation::allocation_query(
            volume_id,
            maybe_snapshot_id,
            region_allocation::RegionParameters {
                block_size,
                blocks_per_extent,
                extent_count,
                read_only: maybe_snapshot_id.is_some(),
            },
            allocation_strategy,
            num_regions_required,
        )?;

        let conn = self.pool_connection_authorized(&opctx).await?;

        let dataset_and_regions: Vec<(CrucibleDataset, Region)> = query
            .get_results_async(&*conn)
            .await
            .map_err(|e| region_allocation::from_diesel(e))?;

        info!(
            self.log,
            "Allocated regions for volume";
            "volume_id" => %volume_id,
            "maybe_snapshot_id" => ?maybe_snapshot_id,
            "datasets_and_regions" => ?dataset_and_regions,
        );

        Ok(dataset_and_regions)
    }

    /// Deletes a set of regions.
    ///
    /// Also updates the storage usage on their corresponding datasets.
    pub async fn regions_hard_delete(
        &self,
        _log: &Logger,
        region_ids: Vec<Uuid>,
    ) -> DeleteResult {
        if region_ids.is_empty() {
            return Ok(());
        }

        let conn = self.pool_connection_unauthorized().await?;

        self.transaction_retry_wrapper("regions_hard_delete")
            .transaction(&conn, |conn| {
                let region_ids = region_ids.clone();

                async move {
                    use nexus_db_schema::schema::region::dsl;

                    let dataset_ids: Vec<Uuid> = diesel::delete(dsl::region)
                        .filter(dsl::id.eq_any(region_ids))
                        .returning(dsl::dataset_id)
                        .get_results_async(&conn)
                        .await?;

                    let query =
                        regions_hard_delete::dataset_update_query(dataset_ids);
                    query.execute_async(&conn).await?;

                    // Whenever a region is hard-deleted, validate invariants
                    // for all volumes
                    #[cfg(any(test, feature = "testing"))]
                    Self::validate_volume_invariants(&conn).await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return the total reserved size for all the regions allocated to a
    /// dataset
    pub async fn regions_total_reserved_size(
        &self,
        dataset_id: DatasetUuid,
    ) -> Result<u64, Error> {
        use nexus_db_schema::schema::region::dsl;

        let dataset_regions: Vec<Region> = dsl::region
            .filter(dsl::dataset_id.eq(to_db_typed_uuid(dataset_id)))
            .select(Region::as_select())
            .load_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(dataset_regions.iter().map(|r| r.reserved_size()).sum())
    }

    /// Find read/write regions on expunged disks
    pub async fn find_read_write_regions_on_expunged_physical_disks(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Vec<Region>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        region_dsl::region
            .filter(region_dsl::dataset_id.eq_any(
                dataset_dsl::crucible_dataset
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::pool_id.eq_any(
                        zpool_dsl::zpool
                            .filter(zpool_dsl::time_deleted.is_null())
                            .filter(zpool_dsl::physical_disk_id.eq_any(
                                physical_disk_dsl::physical_disk
                                    .filter(physical_disk_dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
                                    .select(physical_disk_dsl::id)
                            ))
                            .select(zpool_dsl::id)
                    ))
                    .select(dataset_dsl::id)
            ))
            // only return read-write regions here
            .filter(region_dsl::read_only.eq(false))
            .select(Region::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Find read-only regions on expunged disks
    pub async fn find_read_only_regions_on_expunged_physical_disks(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Vec<Region>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        region_dsl::region
            .filter(region_dsl::dataset_id.eq_any(
                dataset_dsl::crucible_dataset
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::pool_id.eq_any(
                        zpool_dsl::zpool
                            .filter(zpool_dsl::time_deleted.is_null())
                            .filter(zpool_dsl::physical_disk_id.eq_any(
                                physical_disk_dsl::physical_disk
                                    .filter(physical_disk_dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
                                    .select(physical_disk_dsl::id)
                            ))
                            .select(zpool_dsl::id)
                    ))
                    .select(dataset_dsl::id)
            ))
            // only return read-only regions here
            .filter(region_dsl::read_only.eq(true))
            .select(Region::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn region_set_port(
        &self,
        region_id: Uuid,
        region_port: u16,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::region::dsl;

        let conn = self.pool_connection_unauthorized().await?;

        let updated = diesel::update(dsl::region)
            .filter(dsl::id.eq(region_id))
            .set(dsl::port.eq(Some::<SqlU16>(region_port.into())))
            .check_if_exists::<Region>(region_id)
            .execute_and_check(&conn)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.port() == Some(region_port) {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region {region_id} port set to {:?}",
                            record.port(),
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// If a region's port was recorded, return its associated address,
    /// otherwise return None.
    pub async fn region_addr(
        &self,
        region_id: Uuid,
    ) -> LookupResult<Option<SocketAddrV6>> {
        let Some(region) = self.get_region_optional(region_id).await? else {
            return Ok(None);
        };

        let Some(port) = region.port() else {
            return Ok(None);
        };

        let dataset = self.crucible_dataset_get(region.dataset_id()).await?;
        Ok(Some(dataset.address_with_port(port)))
    }

    pub async fn regions_missing_ports(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<Region> {
        opctx.check_complex_operations_allowed()?;

        let mut records = Vec::new();

        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::region::dsl;

            let batch = paginated(dsl::region, dsl::id, &p.current_pagparams())
                .filter(dsl::port.is_null())
                .select(Region::as_select())
                .load_async::<Region>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            paginator = p.found_batch(&batch, &|r| r.id());
            records.extend(batch);
        }

        Ok(records)
    }

    /// Find regions not on expunged disks that match a volume id
    pub async fn find_non_expunged_regions(
        &self,
        opctx: &OpContext,
        volume_id: VolumeUuid,
    ) -> LookupResult<Vec<Region>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        region_dsl::region
            .filter(region_dsl::dataset_id.eq_any(
                dataset_dsl::crucible_dataset
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::pool_id.eq_any(
                        zpool_dsl::zpool
                            .filter(zpool_dsl::time_deleted.is_null())
                            .filter(zpool_dsl::physical_disk_id.eq_any(
                                physical_disk_dsl::physical_disk
                                    .filter(physical_disk_dsl::disk_policy.eq(PhysicalDiskPolicy::InService))
                                    .select(physical_disk_dsl::id)
                            ))
                            .select(zpool_dsl::id)
                    ))
                    .select(dataset_dsl::id)
            ))
            .filter(region_dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .select(Region::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::BlockSize;
    use omicron_common::api::external::ByteCount;

    #[test]
    fn test_extent_count() {
        // Zero sized disks should get zero extents
        let (_, extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(0u64).unwrap(),
        );
        assert_eq!(0, extent_count);

        // Test 1 byte disk
        let (_, extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(1u64).unwrap(),
        );
        assert_eq!(1, extent_count);

        // Test 1 less than the (current) maximum extent size
        let (_, extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(DataStore::EXTENT_SIZE - 1).unwrap(),
        );
        assert_eq!(1, extent_count);

        // Test at than the (current) maximum extent size
        let (_, extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(DataStore::EXTENT_SIZE).unwrap(),
        );
        assert_eq!(1, extent_count);

        // Test at 1 byte more than the (current) maximum extent size
        let (_, extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(DataStore::EXTENT_SIZE + 1).unwrap(),
        );
        assert_eq!(2, extent_count);

        // Mostly just checking we don't blow up on an unwrap here.
        let (_, _extent_count) = DataStore::get_crucible_allocation(
            &BlockSize::Traditional,
            ByteCount::try_from(i64::MAX).unwrap(),
        );

        // Note that i64::MAX bytes is an invalid disk size as it's not
        // divisible by 4096. Create the maximum sized disk here.
        let max_disk_size = i64::MAX
            - (i64::MAX % i64::from(BlockSize::AdvancedFormat.to_bytes()));
        let (blocks_per_extent, extent_count) =
            DataStore::get_crucible_allocation(
                &BlockSize::AdvancedFormat,
                ByteCount::try_from(max_disk_size).unwrap(),
            );

        // We should still be rounding up to the nearest extent size.
        assert_eq!(
            u128::from(extent_count) * u128::from(DataStore::EXTENT_SIZE),
            i64::MAX as u128 + 1,
        );

        // Assert that the regions allocated will fit this disk
        assert!(
            max_disk_size as u128
                <= u128::from(extent_count)
                    * u128::from(blocks_per_extent)
                    * u128::from(DataStore::EXTENT_SIZE)
        );
    }
}
