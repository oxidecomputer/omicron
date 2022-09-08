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
use crate::db::model::Zpool;
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
        use db::schema::zpool::dsl as zpool_dsl;

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

            #[error("Not enough avaiable space for regions")]
            NotEnoughAvailableSpace,

            #[error("Numeric error: {0}")]
            NumericError(String),
        }
        type TxnError = TransactionError<RegionAllocateError>;

        let params: params::DiskCreate = params.clone();
        let block_size =
            self.get_block_size_from_disk_create(opctx, &params).await?;
        let blocks_per_extent =
            params.extent_size() / block_size.to_bytes() as i64;

        // TODO: as a CTE, this could be the following:
        //
        // WITH
        //   /*
        //    * Look up all regions referencing this volume.
        //    * This only returns results if the allocation previously
        //    * completed.
        //    */
        //   previously_allocated_regions AS (
        //     SELECT
        //       [region fields]
        //     FROM
        //       omicron.public.Region
        //     WHERE
        //       volume_id = <volume_id>
        //   ),
        //   /*
        //    * Find datasets provisioned for Crucible which have a
        //    * low storage usage.
        //    */
        //   candidate_datasets AS (
        //     SELECT
        //       [dataset fields]
        //     FROM
        //       omicron.public.Dataset
        //     WHERE
        //       size_used IS NOT NULL AND
        //       time_deleted IS NULL AND
        //       kind = 'crucible'
        //     ORDER BY size_used ASC
        //     LIMIT <REGION_REDUNDANCY_THRESHOLD>
        //   ),
        //   /* Get the zpools to which those candidate datasets belong. */
        //   candidate_zpools AS (
        //     SELECT
        //       [zpool fields]
        //     FROM
        //       omicron.public.Zpool
        //     JOIN
        //       candidate_datasets
        //     ON
        //       candidate_datasets.pool_id = omicron.public.Zpool.id
        //   ),
        //   /* Get the proposed new sizes for zpool capacity
        //   zpool_capacity AS (
        //     SELECT
        //       SUM(dataset.size_used) AS previous_size_used
        //       previous_size_used + <new_region_size> AS new_size_used
        //       new_size_used <= zpool.total_size AS insert_would_fit
        //     FROM
        //       dataset
        //     JOIN
        //       candidate_zpools
        //     ON
        //       dataset.pool_id = zpool.id
        //     WHERE
        //       dataset.size_used != NULL AND
        //       dataset.time_deleted = NULL
        //   ),
        //
        //   /* Make the decision on whether or not to perform the insert */
        //   do_insert AS (
        //     SELECT IF(
        //       zpool_capacity.insert_would_fit AND
        //       NOT(EXISTS(SELECT id from previously_allocated_regions)) AND
        //       COUNT(candidate_datasets) >= REGION_REDUNDANCY_THRESHOLD
        //       TRUE,
        //       FALSE
        //     ),
        //   ),
        //
        //   candidate_regions AS (
        //     SELECT
        //       gen_random_uuid() as id,
        //       now() as time_created,
        //       now() as time_modified,
        //       candidate_datasets.id as dataset_id,
        //       <volume_id> as volume_id,
        //       <block_size> as block_size,
        //       <blocks_per_extent> as blocks_per_extent,
        //       <extent_count> as extent_count,
        //     FROM
        //       candidate_datasets
        //   ),
        //
        //   inserted_regions AS (
        //     INSERT INTO omicron.public.Region
        //       candidate_regions
        //     WHERE
        //       (SELECT * FROM do_insert)
        //     RETURNING *
        //   ),
        //   updated_datasets AS (
        //     UPDATE dataset SET
        //       size_used = size_used + <new_region_size>
        //     WHERE
        //       id IN (SELECT id FROM candidate_datasets) AND
        //       (SELECT * FROM do_insert)
        //     RETURNING *
        //   ),
        // TODO: How hard is it to cope with the variable length here?
        // SELECT * FROM
        //   (SELECT * FROM previously_allocated_regions)
        //   LEFT JOIN (SELECT * FROM inserted_regions) ON TRUE;
        self.pool()
            .transaction(move |conn| {
                // First, for idempotency, check if regions are already
                // allocated to this disk.
                //
                // If they are, return those regions and the associated
                // datasets.
                let datasets_and_regions =
                    Self::get_allocated_regions_query(volume_id)
                        .get_results::<(Dataset, Region)>(conn)?;
                if !datasets_and_regions.is_empty() {
                    return Ok(datasets_and_regions);
                }

                // Return the REGION_REDUNDANCY_THRESHOLD datasets with the most
                // amount of available space.
                //
                // Note: it only returns REGION_REDUNDANCY_THRESHOLD datasets,
                // and as a result does not support allocating chunks of a disk
                // separately - this is all or nothing.
                let mut datasets: Vec<Dataset> =
                    Self::get_allocatable_datasets_query()
                        .get_results::<Dataset>(conn)?;

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

                // Insert regions into the DB
                let regions = diesel::insert_into(region_dsl::region)
                    .values(regions)
                    .returning(Region::as_returning())
                    .get_results(conn)?;

                // Update size_used in the source datasets containing those
                // regions.
                for dataset in source_datasets.iter_mut() {
                    let dataset_total_occupied_size: Option<
                        diesel::pg::data_types::PgNumeric,
                    > = region_dsl::region
                        .filter(region_dsl::dataset_id.eq(dataset.id()))
                        .select(diesel::dsl::sum(
                            region_dsl::block_size
                                * region_dsl::blocks_per_extent
                                * region_dsl::extent_count,
                        ))
                        .nullable()
                        .get_result(conn)?;

                    let dataset_total_occupied_size: i64 = if let Some(
                        dataset_total_occupied_size,
                    ) =
                        dataset_total_occupied_size
                    {
                        let dataset_total_occupied_size: db::model::ByteCount =
                            dataset_total_occupied_size.try_into().map_err(
                                |e: anyhow::Error| {
                                    TxnError::CustomError(
                                        RegionAllocateError::NumericError(
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
                        .filter(dataset_dsl::id.eq(dataset.id()))
                        .set(
                            dataset_dsl::size_used
                                .eq(dataset_total_occupied_size),
                        )
                        .execute(conn)?;

                    // Update the results we'll send the caller
                    dataset.size_used = Some(dataset_total_occupied_size);
                }

                // Validate that the total of each dataset's size_used isn't
                // larger the zpool's total_size
                for dataset in source_datasets.iter() {
                    let zpool_id = dataset.pool_id;

                    // Add up size used by all regions in all datasets in the
                    // zpool
                    let zpool_total_occupied_size: Option<
                        diesel::pg::data_types::PgNumeric,
                    > = dataset_dsl::dataset
                        .filter(dataset_dsl::pool_id.eq(zpool_id))
                        .filter(dataset_dsl::size_used.is_not_null())
                        .filter(dataset_dsl::time_deleted.is_null())
                        .select(diesel::dsl::sum(dataset_dsl::size_used))
                        .nullable()
                        .get_result(conn)?;

                    let zpool_total_occupied_size: u64 = if let Some(
                        zpool_total_occupied_size,
                    ) =
                        zpool_total_occupied_size
                    {
                        let zpool_total_occupied_size: db::model::ByteCount =
                            zpool_total_occupied_size.try_into().map_err(
                                |e: anyhow::Error| {
                                    TxnError::CustomError(
                                        RegionAllocateError::NumericError(
                                            e.to_string(),
                                        ),
                                    )
                                },
                            )?;

                        zpool_total_occupied_size.to_bytes()
                    } else {
                        0
                    };

                    let zpool = zpool_dsl::zpool
                        .filter(zpool_dsl::id.eq(zpool_id))
                        .select(Zpool::as_returning())
                        .get_result(conn)?;

                    // Does this go over the zpool's total size?
                    if zpool.total_size.to_bytes() < zpool_total_occupied_size {
                        return Err(TxnError::CustomError(
                            RegionAllocateError::NotEnoughAvailableSpace,
                        ));
                    }
                }

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
                TxnError::CustomError(
                    RegionAllocateError::NotEnoughAvailableSpace,
                ) => Error::unavail(
                    "Not enough available space to allocate disks",
                ),
                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    /// Deletes all regions backing a disk.
    ///
    /// Also updates the storage usage on their corresponding datasets.
    pub async fn regions_hard_delete(&self, volume_id: Uuid) -> DeleteResult {
        #[derive(Debug, thiserror::Error)]
        enum RegionDeleteError {
            #[error("Numeric error: {0}")]
            NumericError(String),
        }
        type TxnError = TransactionError<RegionDeleteError>;

        self.pool()
            .transaction(move |conn| {
                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region::dsl as region_dsl;

                // Remove the regions, collecting datasets they're from.
                let datasets = diesel::delete(region_dsl::region)
                    .filter(region_dsl::volume_id.eq(volume_id))
                    .returning(region_dsl::dataset_id)
                    .get_results::<Uuid>(conn)?;

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
                        .get_result(conn)?;

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
                        .execute(conn)?;
                }

                Ok(())
            })
            .await
            .map_err(|e: TxnError| {
                Error::internal_error(&format!("Transaction error: {}", e))
            })
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
                .get_result_async(self.pool())
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
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
