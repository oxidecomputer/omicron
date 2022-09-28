// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Region`]s.

use super::DataStore;
use super::RunnableQuery;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
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
        let params: params::DiskCreate = params.clone();
        let block_size =
            self.get_block_size_from_disk_create(opctx, &params).await?;
        let blocks_per_extent =
            params.extent_size() / block_size.to_bytes() as i64;

        let dataset_and_regions: Vec<(Dataset, Region)> =
            crate::db::queries::region_allocation::RegionAllocate::new(
                volume_id,
                block_size.into(),
                blocks_per_extent,
                params.extent_count(),
            )
            .get_results_async(self.pool())
            .await
            .map_err(|e| crate::db::queries::region_allocation::from_pool(e))?;

        Ok(dataset_and_regions)
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
