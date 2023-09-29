// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Region`]s.

use super::DataStore;
use super::RegionAllocationStrategy;
use super::RunnableQuery;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::lookup::LookupPath;
use crate::db::model::Dataset;
use crate::db::model::Region;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use slog::Logger;
use uuid::Uuid;

impl DataStore {
    pub(super) fn get_allocated_regions_query(
        volume_id: Uuid,
    ) -> impl RunnableQuery<(Dataset, Region)> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;
        region_dsl::region
            .filter(region_dsl::volume_id.eq(volume_id))
            .inner_join(
                dataset_dsl::dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .select((Dataset::as_select(), Region::as_select()))
    }

    /// Gets allocated regions for a disk, and the datasets to which those
    /// regions belong.
    ///
    /// Note that this function does not validate liveness of the Disk, so it
    /// may be used in a context where the disk is being deleted.
    pub async fn get_allocated_regions(
        &self,
        volume_id: Uuid,
    ) -> Result<Vec<(Dataset, Region)>, Error> {
        Self::get_allocated_regions_query(volume_id)
            .get_results_async::<(Dataset, Region)>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
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
                let (.., db_snapshot) = LookupPath::new(opctx, &self)
                    .snapshot_id(*snapshot_id)
                    .fetch()
                    .await?;

                Ok(db_snapshot.block_size)
            }
            params::DiskSource::Image { image_id } => {
                let (.., db_image) = LookupPath::new(opctx, &self)
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
            Self::EXTENT_SIZE / block_size.to_bytes() as u64;

        let size = size.to_bytes();

        // allocate enough extents to fit all the disk blocks, rounding up.
        let extent_count = size / Self::EXTENT_SIZE
            + ((size % Self::EXTENT_SIZE) + Self::EXTENT_SIZE - 1)
                / Self::EXTENT_SIZE;

        (blocks_per_extent, extent_count)
    }

    /// Idempotently allocates enough regions to back a disk.
    ///
    /// Returns the allocated regions, as well as the datasets to which they
    /// belong.
    pub async fn region_allocate(
        &self,
        opctx: &OpContext,
        volume_id: Uuid,
        disk_source: &params::DiskSource,
        size: external::ByteCount,
        allocation_strategy: &RegionAllocationStrategy,
    ) -> Result<Vec<(Dataset, Region)>, Error> {
        let block_size =
            self.get_block_size_from_disk_source(opctx, &disk_source).await?;
        let (blocks_per_extent, extent_count) =
            Self::get_crucible_allocation(&block_size, size);

        let dataset_and_regions: Vec<(Dataset, Region)> =
            crate::db::queries::region_allocation::RegionAllocate::new(
                volume_id,
                block_size.to_bytes() as u64,
                blocks_per_extent,
                extent_count,
                allocation_strategy,
            )
            .get_results_async(&*self.pool_connection_authorized(&opctx).await?)
            .await
            .map_err(|e| {
                crate::db::queries::region_allocation::from_diesel(e)
            })?;

        Ok(dataset_and_regions)
    }

    /// Deletes a set of regions.
    ///
    /// Also updates the storage usage on their corresponding datasets.
    pub async fn regions_hard_delete(
        &self,
        log: &Logger,
        region_ids: Vec<Uuid>,
    ) -> DeleteResult {
        if region_ids.is_empty() {
            return Ok(());
        }

        #[derive(Debug, thiserror::Error)]
        enum RegionDeleteError {
            #[error("Numeric error: {0}")]
            NumericError(String),
        }
        type TxnError = TransactionError<RegionDeleteError>;

        // Retry this transaction until it succeeds. It's a little heavy in that
        // there's a for loop inside that iterates over the datasets the
        // argument regions belong to, and it often encounters the "retry
        // transaction" error.
        let transaction = {
            |region_ids: Vec<Uuid>| async {
                self.pool_connection_unauthorized()
                    .await?
                    .transaction_async(|conn| async move {
                        use db::schema::dataset::dsl as dataset_dsl;
                        use db::schema::region::dsl as region_dsl;

                        // Remove the regions, collecting datasets they're from.
                        let datasets = diesel::delete(region_dsl::region)
                            .filter(region_dsl::id.eq_any(region_ids))
                            .returning(region_dsl::dataset_id)
                            .get_results_async::<Uuid>(&conn).await?;

                        // Update datasets to which the regions belonged.
                        for dataset in datasets {
                            let dataset_total_occupied_size: Option<
                                diesel::pg::data_types::PgNumeric,
                            > = region_dsl::region
                                .filter(region_dsl::dataset_id.eq(dataset))
                                .select(diesel::dsl::sum(
                                    region_dsl::block_size
                                        * region_dsl::blocks_per_extent
                                        * region_dsl::extent_count,
                                ))
                                .nullable()
                                .get_result_async(&conn).await?;

                            let dataset_total_occupied_size: i64 = if let Some(
                                dataset_total_occupied_size,
                            ) =
                                dataset_total_occupied_size
                            {
                                let dataset_total_occupied_size: db::model::ByteCount =
                                    dataset_total_occupied_size.try_into().map_err(
                                        |e: anyhow::Error| {
                                            TxnError::CustomError(
                                                RegionDeleteError::NumericError(
                                                    e.to_string(),
                                                ),
                                            )
                                        },
                                    )?;

                                dataset_total_occupied_size.into()
                            } else {
                                0
                            };

                            diesel::update(dataset_dsl::dataset)
                                .filter(dataset_dsl::id.eq(dataset))
                                .set(
                                    dataset_dsl::size_used
                                        .eq(dataset_total_occupied_size),
                                )
                                .execute_async(&conn).await?;
                        }

                        Ok(())
                    })
                    .await
                    .map_err(|e: TxnError| {
                        if e.retry_transaction() {
                            BackoffError::transient(Error::internal_error(
                                &format!("Retryable transaction error {:?}", e)
                            ))
                        } else {
                            BackoffError::Permanent(Error::internal_error(
                                &format!("Transaction error: {}", e)
                            ))
                        }
                    })
            }
        };

        backoff::retry_notify(
            backoff::retry_policy_internal_service_aggressive(),
            || async {
                let region_ids = region_ids.clone();
                transaction(region_ids).await
            },
            |e: Error, delay| {
                info!(log, "{:?}, trying again in {:?}", e, delay,);
            },
        )
        .await
    }

    /// Return the total occupied size for a dataset
    pub async fn regions_total_occupied_size(
        &self,
        dataset_id: Uuid,
    ) -> Result<u64, Error> {
        use db::schema::region::dsl as region_dsl;

        let total_occupied_size: Option<diesel::pg::data_types::PgNumeric> =
            region_dsl::region
                .filter(region_dsl::dataset_id.eq(dataset_id))
                .select(diesel::dsl::sum(
                    region_dsl::block_size
                        * region_dsl::blocks_per_extent
                        * region_dsl::extent_count,
                ))
                .nullable()
                .get_result_async(&*self.pool_connection_unauthorized().await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

        if let Some(total_occupied_size) = total_occupied_size {
            let total_occupied_size: db::model::ByteCount =
                total_occupied_size.try_into().map_err(
                    |e: anyhow::Error| Error::internal_error(&e.to_string()),
                )?;

            Ok(total_occupied_size.to_bytes())
        } else {
            Ok(0)
        }
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
            - (i64::MAX % (BlockSize::AdvancedFormat.to_bytes() as i64));
        let (blocks_per_extent, extent_count) =
            DataStore::get_crucible_allocation(
                &BlockSize::AdvancedFormat,
                ByteCount::try_from(max_disk_size).unwrap(),
            );

        // We should still be rounding up to the nearest extent size.
        assert_eq!(
            extent_count as u128 * DataStore::EXTENT_SIZE as u128,
            i64::MAX as u128 + 1,
        );

        // Assert that the regions allocated will fit this disk
        assert!(
            max_disk_size as u128
                <= extent_count as u128
                    * blocks_per_extent as u128
                    * DataStore::EXTENT_SIZE as u128
        );
    }
}
