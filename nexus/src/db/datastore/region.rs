// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Region`]s.

use super::DataStore;
use super::RunnableQuery;
use super::REGION_REDUNDANCY_THRESHOLD;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::db::model::Dataset;
use crate::db::model::Region;
use crate::external_api::params;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
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
            .get_results_async::<(Dataset, Region)>(self.pool())
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    async fn get_block_size_from_disk_create(
        &self,
        opctx: &OpContext,
        disk_create: &params::DiskCreate,
    ) -> Result<db::model::BlockSize, Error> {
        match &disk_create.disk_source {
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
            params::DiskSource::Image { image_id: _ } => {
                // Until we implement project images, do not allow disks to be
                // created from a project image.
                return Err(Error::InvalidValue {
                    label: String::from("image"),
                    message: String::from(
                        "project image are not yet supported",
                    ),
                });
            }
            params::DiskSource::GlobalImage { image_id } => {
                let (.., db_global_image) = LookupPath::new(opctx, &self)
                    .global_image_id(*image_id)
                    .fetch()
                    .await?;

                Ok(db_global_image.block_size)
            }
        }
    }

    /// Idempotently allocates enough regions to back a disk.
    ///
    /// Returns the allocated regions, as well as the datasets to which they
    /// belong.
    pub async fn region_allocate(
        &self,
        opctx: &OpContext,
        volume_id: Uuid,
        params: &params::DiskCreate,
    ) -> Result<Vec<(Dataset, Region)>, Error> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;

        // ALLOCATION POLICY
        //
        // NOTE: This policy can - and should! - be changed.
        //
        // See https://rfd.shared.oxide.computer/rfd/0205 for a more
        // complete discussion.
        //
        // It is currently acting as a placeholder, showing a feasible
        // interaction between datasets and regions.
        //
        // This policy allocates regions to distinct Crucible datasets,
        // favoring datasets with the smallest existing (summed) region
        // sizes. Basically, "pick the datasets with the smallest load first".
        //
        // Longer-term, we should consider:
        // - Storage size + remaining free space
        // - Sled placement of datasets
        // - What sort of loads we'd like to create (even split across all disks
        // may not be preferable, especially if maintenance is expected)
        #[derive(Debug, thiserror::Error)]
        enum RegionAllocateError {
            #[error("Not enough datasets for replicated allocation: {0}")]
            NotEnoughDatasets(usize),
        }
        type TxnError = TransactionError<RegionAllocateError>;

        let params: params::DiskCreate = params.clone();
        let block_size =
            self.get_block_size_from_disk_create(opctx, &params).await?;
        let blocks_per_extent =
            params.extent_size() / block_size.to_bytes() as i64;

        self.pool()
            .transaction_async(|conn| async move {
                // First, for idempotency, check if regions are already
                // allocated to this disk.
                //
                // If they are, return those regions and the associated
                // datasets.
                let datasets_and_regions =
                    Self::get_allocated_regions_query(volume_id)
                        .get_results_async::<(Dataset, Region)>(&conn)
                        .await?;
                if !datasets_and_regions.is_empty() {
                    return Ok(datasets_and_regions);
                }

                let mut datasets: Vec<Dataset> =
                    Self::get_allocatable_datasets_query()
                        .get_results_async::<Dataset>(&conn)
                        .await?;

                if datasets.len() < REGION_REDUNDANCY_THRESHOLD {
                    return Err(TxnError::CustomError(
                        RegionAllocateError::NotEnoughDatasets(datasets.len()),
                    ));
                }

                // Create identical regions on each of the following datasets.
                let source_datasets =
                    &mut datasets[0..REGION_REDUNDANCY_THRESHOLD];
                let regions: Vec<Region> = source_datasets
                    .iter()
                    .map(|dataset| {
                        Region::new(
                            dataset.id(),
                            volume_id,
                            block_size.into(),
                            blocks_per_extent,
                            params.extent_count(),
                        )
                    })
                    .collect();
                let regions = diesel::insert_into(region_dsl::region)
                    .values(regions)
                    .returning(Region::as_returning())
                    .get_results_async(&conn)
                    .await?;

                // Update the tallied sizes in the source datasets containing
                // those regions.
                let region_size = i64::from(block_size.to_bytes())
                    * blocks_per_extent
                    * params.extent_count();
                for dataset in source_datasets.iter_mut() {
                    dataset.size_used =
                        dataset.size_used.map(|v| v + region_size);
                }

                let dataset_ids: Vec<Uuid> =
                    source_datasets.iter().map(|ds| ds.id()).collect();
                diesel::update(dataset_dsl::dataset)
                    .filter(dataset_dsl::id.eq_any(dataset_ids))
                    .set(
                        dataset_dsl::size_used
                            .eq(dataset_dsl::size_used + region_size),
                    )
                    .execute_async(&conn)
                    .await?;

                // Return the regions with the datasets to which they were allocated.
                Ok(source_datasets
                    .into_iter()
                    .map(|d| d.clone())
                    .zip(regions)
                    .collect())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    RegionAllocateError::NotEnoughDatasets(_),
                ) => Error::unavail("Not enough datasets to allocate disks"),
                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    /// Deletes all regions backing a disk.
    ///
    /// Also updates the storage usage on their corresponding datasets.
    pub async fn regions_hard_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;

        // Remove the regions, collecting datasets they're from.
        let (dataset_id, size) = diesel::delete(region_dsl::region)
            .filter(region_dsl::volume_id.eq(volume_id))
            .returning((
                region_dsl::dataset_id,
                region_dsl::block_size
                    * region_dsl::blocks_per_extent
                    * region_dsl::extent_count,
            ))
            .get_result_async::<(Uuid, i64)>(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting regions: {:?}",
                    e
                ))
            })?;

        // Update those datasets to which the regions belonged.
        diesel::update(dataset_dsl::dataset)
            .filter(dataset_dsl::id.eq(dataset_id))
            .set(dataset_dsl::size_used.eq(dataset_dsl::size_used - size))
            .execute_async(self.pool())
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error updating dataset space: {:?}",
                    e
                ))
            })?;

        Ok(())
    }
}
