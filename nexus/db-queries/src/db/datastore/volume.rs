// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Volume`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::Region;
use crate::db::model::RegionSnapshot;
use crate::db::model::Volume;
use crate::db::queries::volume::DecreaseCrucibleResourceCountAndSoftDeleteVolume;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::OptionalExtension;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use sled_agent_client::types::VolumeConstructionRequest;
use uuid::Uuid;

impl DataStore {
    pub async fn volume_create(&self, volume: Volume) -> CreateResult<Volume> {
        use db::schema::volume::dsl;

        #[derive(Debug, thiserror::Error)]
        enum VolumeCreationError {
            #[error("Error from Volume creation: {0}")]
            Public(Error),

            #[error("Serde error during Volume creation: {0}")]
            SerdeError(#[from] serde_json::Error),
        }
        type TxnError = TransactionError<VolumeCreationError>;

        // Grab all the targets that the volume construction request references.
        // Do this outside the transaction, as the data inside volume doesn't
        // change and this would simply add to the transaction time.
        let crucible_targets = {
            let vcr: VolumeConstructionRequest =
                serde_json::from_str(&volume.data()).map_err(|e| {
                    Error::internal_error(&format!(
                        "serde_json::from_str error in volume_create: {}",
                        e
                    ))
                })?;

            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        self.pool_connection_unauthorized()
            .await?
            .transaction_async(|conn| async move {
                let maybe_volume: Option<Volume> = dsl::volume
                    .filter(dsl::id.eq(volume.id()))
                    .select(Volume::as_select())
                    .first_async(&conn)
                    .await
                    .optional()
                    .map_err(|e| {
                        TxnError::CustomError(VolumeCreationError::Public(
                            public_error_from_diesel(e, ErrorHandler::Server),
                        ))
                    })?;

                // If the volume existed already, return it and do not increase
                // usage counts.
                if let Some(volume) = maybe_volume {
                    return Ok(volume);
                }

                // TODO do we need on_conflict do_nothing here? if the transaction
                // model is read-committed, the SELECT above could return nothing,
                // and the INSERT here could still result in a conflict.
                //
                // See also https://github.com/oxidecomputer/omicron/issues/1168
                let volume: Volume = diesel::insert_into(dsl::volume)
                    .values(volume.clone())
                    .on_conflict(dsl::id)
                    .do_nothing()
                    .returning(Volume::as_returning())
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        TxnError::CustomError(VolumeCreationError::Public(
                            public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::Volume,
                                    volume.id().to_string().as_str(),
                                ),
                            ),
                        ))
                    })?;

                // Increase the usage count for Crucible resources according to the
                // contents of the volume.

                // Increase the number of uses for each referenced region snapshot.
                use db::schema::region_snapshot::dsl as rs_dsl;
                for read_only_target in &crucible_targets.read_only_targets {
                    diesel::update(rs_dsl::region_snapshot)
                        .filter(
                            rs_dsl::snapshot_addr.eq(read_only_target.clone()),
                        )
                        .filter(rs_dsl::deleting.eq(false))
                        .set(
                            rs_dsl::volume_references
                                .eq(rs_dsl::volume_references + 1),
                        )
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            TxnError::CustomError(VolumeCreationError::Public(
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                ),
                            ))
                        })?;
                }

                Ok(volume)
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(VolumeCreationError::Public(e)) => e,

                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    /// Return a `Option<Volume>` based on id, even if it's soft deleted.
    pub async fn volume_get(
        &self,
        volume_id: Uuid,
    ) -> LookupResult<Option<Volume>> {
        use db::schema::volume::dsl;
        dsl::volume
            .filter(dsl::id.eq(volume_id))
            .select(Volume::as_select())
            .first_async::<Volume>(&*self.pool_connection_unauthorized().await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete the volume if it exists. If it was already deleted, this is a
    /// no-op.
    pub async fn volume_hard_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::volume::dsl;

        diesel::delete(dsl::volume)
            .filter(dsl::id.eq(volume_id))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Checkout a copy of the Volume from the database.
    /// This action (getting a copy) will increase the generation number
    /// of Volumes of the VolumeConstructionRequest::Volume type that have
    /// sub_volumes of the VolumeConstructionRequest::Region type.
    /// This generation number increase is required for Crucible to support
    /// crash consistency.
    pub async fn volume_checkout(
        &self,
        volume_id: Uuid,
    ) -> LookupResult<Volume> {
        use db::schema::volume::dsl;

        #[derive(Debug, thiserror::Error)]
        enum VolumeGetError {
            #[error("Error during volume_checkout: {0}")]
            DieselError(#[from] diesel::result::Error),

            #[error("Serde error during volume_checkout: {0}")]
            SerdeError(#[from] serde_json::Error),

            #[error("Updated {0} database rows, expected {1}")]
            UnexpectedDatabaseUpdate(usize, usize),
        }
        type TxnError = TransactionError<VolumeGetError>;

        // We perform a transaction here, to be sure that on completion
        // of this, the database contains an updated version of the
        // volume with the generation number incremented (for the volume
        // types that require it).  The generation number (along with the
        // rest of the volume data) that was in the database is what is
        // returned to the caller.
        self.pool_connection_unauthorized()
            .await?
            .transaction_async(|conn| async move {
                // Grab the volume in question.
                let volume = dsl::volume
                    .filter(dsl::id.eq(volume_id))
                    .select(Volume::as_select())
                    .get_result_async(&conn)
                    .await?;

                // Turn the volume.data into the VolumeConstructionRequest
                let vcr: VolumeConstructionRequest =
                    serde_json::from_str(volume.data()).map_err(|e| {
                        TxnError::CustomError(VolumeGetError::SerdeError(e))
                    })?;

                // Look to see if the VCR is a Volume type, and if so, look at
                // its sub_volumes. If they are of type Region, then we need
                // to update their generation numbers and record that update
                // back to the database. We return to the caller whatever the
                // original volume data was we pulled from the database.
                match vcr {
                    VolumeConstructionRequest::Volume {
                        id,
                        block_size,
                        sub_volumes,
                        read_only_parent,
                    } => {
                        let mut update_needed = false;
                        let mut new_sv = Vec::new();
                        for sv in sub_volumes {
                            match sv {
                                VolumeConstructionRequest::Region {
                                    block_size,
                                    blocks_per_extent,
                                    extent_count,
                                    opts,
                                    gen,
                                } => {
                                    update_needed = true;
                                    new_sv.push(
                                        VolumeConstructionRequest::Region {
                                            block_size,
                                            blocks_per_extent,
                                            extent_count,
                                            opts,
                                            gen: gen + 1,
                                        },
                                    );
                                }
                                _ => {
                                    new_sv.push(sv);
                                }
                            }
                        }

                        // Only update the volume data if we found the type
                        // of volume that needed it.
                        if update_needed {
                            // Create a new VCR and fill in the contents
                            // from what the original volume had, but with our
                            // updated sub_volume records.
                            let new_vcr = VolumeConstructionRequest::Volume {
                                id,
                                block_size,
                                sub_volumes: new_sv,
                                read_only_parent,
                            };

                            let new_volume_data = serde_json::to_string(
                                &new_vcr,
                            )
                            .map_err(|e| {
                                TxnError::CustomError(
                                    VolumeGetError::SerdeError(e),
                                )
                            })?;

                            // Update the original volume_id with the new
                            // volume.data.
                            use db::schema::volume::dsl as volume_dsl;
                            let num_updated =
                                diesel::update(volume_dsl::volume)
                                    .filter(volume_dsl::id.eq(volume_id))
                                    .set(volume_dsl::data.eq(new_volume_data))
                                    .execute_async(&conn)
                                    .await?;

                            // This should update just one row.  If it does
                            // not, then something is terribly wrong in the
                            // database.
                            if num_updated != 1 {
                                return Err(TxnError::CustomError(
                                    VolumeGetError::UnexpectedDatabaseUpdate(
                                        num_updated,
                                        1,
                                    ),
                                ));
                            }
                        }
                    }
                    VolumeConstructionRequest::Region {
                        block_size: _,
                        blocks_per_extent: _,
                        extent_count: _,
                        opts: _,
                        gen: _,
                    } => {
                        // We don't support a pure Region VCR at the volume
                        // level in the database, so this choice should
                        // never be encountered, but I want to know if it is.
                        panic!("Region not supported as a top level volume");
                    }
                    VolumeConstructionRequest::File {
                        id: _,
                        block_size: _,
                        path: _,
                    }
                    | VolumeConstructionRequest::Url {
                        id: _,
                        block_size: _,
                        url: _,
                    } => {}
                }
                Ok(volume)
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(VolumeGetError::DieselError(e)) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }

                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }

    /// Create new UUIDs for the volume construction request layers
    pub fn randomize_ids(
        vcr: &VolumeConstructionRequest,
    ) -> anyhow::Result<VolumeConstructionRequest> {
        match vcr {
            VolumeConstructionRequest::Volume {
                id: _,
                block_size,
                sub_volumes,
                read_only_parent,
            } => Ok(VolumeConstructionRequest::Volume {
                id: Uuid::new_v4(),
                block_size: *block_size,
                sub_volumes: sub_volumes
                    .iter()
                    .map(
                        |subvol| -> anyhow::Result<VolumeConstructionRequest> {
                            Self::randomize_ids(&subvol)
                        },
                    )
                    .collect::<anyhow::Result<Vec<VolumeConstructionRequest>>>(
                    )?,
                read_only_parent: if let Some(read_only_parent) =
                    read_only_parent
                {
                    Some(Box::new(Self::randomize_ids(read_only_parent)?))
                } else {
                    None
                },
            }),

            VolumeConstructionRequest::Url { id: _, block_size, url } => {
                Ok(VolumeConstructionRequest::Url {
                    id: Uuid::new_v4(),
                    block_size: *block_size,
                    url: url.clone(),
                })
            }

            VolumeConstructionRequest::Region {
                block_size,
                blocks_per_extent,
                extent_count,
                opts,
                gen,
            } => {
                let mut opts = opts.clone();
                opts.id = Uuid::new_v4();

                Ok(VolumeConstructionRequest::Region {
                    block_size: *block_size,
                    blocks_per_extent: *blocks_per_extent,
                    extent_count: *extent_count,
                    opts,
                    gen: *gen,
                })
            }

            VolumeConstructionRequest::File { id: _, block_size, path } => {
                Ok(VolumeConstructionRequest::File {
                    id: Uuid::new_v4(),
                    block_size: *block_size,
                    path: path.clone(),
                })
            }
        }
    }

    /// Checkout a copy of the Volume from the database using `volume_checkout`,
    /// then randomize the UUIDs in the construction request. Because this is a
    /// new volume, it is immediately passed to `volume_create` so that the
    /// accounting for Crucible resources stays correct.
    pub async fn volume_checkout_randomize_ids(
        &self,
        volume_id: Uuid,
    ) -> CreateResult<Volume> {
        let volume = self.volume_checkout(volume_id).await?;

        let vcr: sled_agent_client::types::VolumeConstructionRequest =
            serde_json::from_str(volume.data())?;

        let randomized_vcr = serde_json::to_string(
            &Self::randomize_ids(&vcr)
                .map_err(|e| Error::internal_error(&e.to_string()))?,
        )?;

        self.volume_create(db::model::Volume::new(
            Uuid::new_v4(),
            randomized_vcr,
        ))
        .await
    }

    /// Find regions for deleted volumes that do not have associated region
    /// snapshots.
    pub async fn find_deleted_volume_regions(
        &self,
    ) -> ListResultVec<(Dataset, Region, Volume)> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;
        use db::schema::region_snapshot::dsl;
        use db::schema::volume::dsl as volume_dsl;

        // Find all regions and datasets
        region_dsl::region
            .inner_join(
                volume_dsl::volume.on(region_dsl::volume_id.eq(volume_dsl::id)),
            )
            .inner_join(
                dataset_dsl::dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            // where there either are no region snapshots, or the region
            // snapshot volume references have gone to zero
            .left_join(
                dsl::region_snapshot.on(dsl::region_id
                    .eq(region_dsl::id)
                    .and(dsl::dataset_id.eq(dataset_dsl::id))),
            )
            .filter(
                dsl::volume_references
                    .eq(0)
                    // Despite the SQL specifying that this column is NOT NULL,
                    // this null check is required for this function to work!
                    .or(dsl::volume_references.is_null()),
            )
            // where the volume has already been soft-deleted
            .filter(volume_dsl::time_deleted.is_not_null())
            // and return them (along with the volume so it can be hard deleted)
            .select((
                Dataset::as_select(),
                Region::as_select(),
                Volume::as_select(),
            ))
            .load_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn read_only_resources_associated_with_volume(
        &self,
        volume_id: Uuid,
    ) -> LookupResult<CrucibleTargets> {
        let volume = if let Some(volume) = self.volume_get(volume_id).await? {
            volume
        } else {
            // Volume has already been hard deleted (volume_get returns
            // soft deleted records), return that no cleanup is necessary.
            return Ok(CrucibleTargets::default());
        };

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data())?;

        let mut crucible_targets = CrucibleTargets::default();

        read_only_resources_associated_with_volume(&vcr, &mut crucible_targets);

        Ok(crucible_targets)
    }

    /// Decrease the usage count for Crucible resources according to the
    /// contents of the volume. Call this when deleting a volume (but before the
    /// volume record has been hard deleted).
    ///
    /// Returns a list of Crucible resources to clean up, and soft-deletes the
    /// volume. Note this function must be idempotent, it is called from a saga
    /// node.
    pub async fn decrease_crucible_resource_count_and_soft_delete_volume(
        &self,
        volume_id: Uuid,
    ) -> Result<CrucibleResources, Error> {
        // Grab all the targets that the volume construction request references.
        // Do this outside the transaction, as the data inside volume doesn't
        // change and this would simply add to the transaction time.
        let crucible_targets = {
            let volume =
                if let Some(volume) = self.volume_get(volume_id).await? {
                    volume
                } else {
                    // The volume was hard-deleted, return an empty
                    // CrucibleResources
                    return Ok(CrucibleResources::V1(
                        CrucibleResourcesV1::default(),
                    ));
                };

            let vcr: VolumeConstructionRequest =
                serde_json::from_str(&volume.data()).map_err(|e| {
                    Error::internal_error(&format!(
                        "serde_json::from_str error in volume_create: {}",
                        e
                    ))
                })?;

            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        // Call a CTE that will:
        //
        // 1. decrease the number of references for each region snapshot that
        //    this Volume references
        // 2. soft-delete the volume
        // 3. record the resources to clean up as a serialized CrucibleResources
        //    struct in volume's `resources_to_clean_up` column.
        //
        // Step 3 is important because this function is called from a saga node.
        // If saga execution crashes after steps 1 and 2, but before serializing
        // the resources to be cleaned up as part of the saga node context, then
        // that list of resources will be lost.
        //
        // We also have to guard against the case where this function is called
        // multiple times, and that is done by soft-deleting the volume during
        // the CTE, and returning the previously serialized list of resources to
        // clean up if a soft-delete has already occurred.

        let _old_volume: Vec<Volume> =
            DecreaseCrucibleResourceCountAndSoftDeleteVolume::new(
                volume_id,
                crucible_targets.read_only_targets.clone(),
            )
            .get_results_async::<Volume>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Get the updated Volume to get the resources to clean up
        let resources_to_clean_up: CrucibleResources = match self
            .volume_get(volume_id)
            .await?
        {
            Some(volume) => {
                match volume.resources_to_clean_up.as_ref() {
                    Some(v) => serde_json::from_str(v)?,

                    None => {
                        // Even volumes with nothing to clean up should have
                        // a serialized CrucibleResources that contains
                        // empty vectors instead of None. Instead of
                        // panicing here though, just return the default
                        // (nothing to clean up).
                        CrucibleResources::V1(CrucibleResourcesV1::default())
                    }
                }
            }

            None => {
                // If the volume was hard-deleted already, return the
                // default (nothing to clean up).
                CrucibleResources::V1(CrucibleResourcesV1::default())
            }
        };

        Ok(resources_to_clean_up)
    }

    // Here we remove the read only parent from volume_id, and attach it
    // to temp_volume_id.
    //
    // As this is part of a saga, it will be able to handle being replayed
    // If we call this twice, any work done the first time through should
    // not happen again, or be undone.
    pub async fn volume_remove_rop(
        &self,
        volume_id: Uuid,
        temp_volume_id: Uuid,
    ) -> Result<bool, Error> {
        #[derive(Debug, thiserror::Error)]
        enum RemoveReadOnlyParentError {
            #[error("Error removing read only parent: {0}")]
            DieselError(#[from] diesel::result::Error),

            #[error("Serde error removing read only parent: {0}")]
            SerdeError(#[from] serde_json::Error),

            #[error("Updated {0} database rows, expected {1}")]
            UnexpectedDatabaseUpdate(usize, usize),
        }
        type TxnError = TransactionError<RemoveReadOnlyParentError>;

        // In this single transaction:
        // - Get the given volume from the volume_id from the database
        // - Extract the volume.data into a VolumeConstructionRequest (VCR)
        // - Create a new VCR, copying over anything from the original VCR,
        //   but, replacing the read_only_parent with None.
        // - Put the new VCR into volume.data, then update the volume in the
        //   database.
        // - Get the given volume from temp_volume_id from the database
        // - Extract the temp volume.data into a VCR
        // - Create a new VCR, copying over anything from the original VCR,
        //   but, replacing the read_only_parent with the read_only_parent
        //   data from original volume_id.
        // - Put the new temp VCR into the temp volume.data, update the
        //   temp_volume in the database.
        self.pool_connection_unauthorized()
            .await?
            .transaction_async(|conn| async move {
                // Grab the volume in question. If the volume record was already
                // deleted then we can just return.
                let volume = {
                    use db::schema::volume::dsl;

                    let volume = dsl::volume
                        .filter(dsl::id.eq(volume_id))
                        .select(Volume::as_select())
                        .get_result_async(&conn)
                        .await
                        .optional()?;

                    let volume = if let Some(v) = volume {
                        v
                    } else {
                        // the volume does not exist, nothing to do.
                        return Ok(false);
                    };

                    if volume.time_deleted.is_some() {
                        // this volume is deleted, so let whatever is deleting
                        // it clean it up.
                        return Ok(false);
                    } else {
                        // A volume record exists, and was not deleted, we
                        // can attempt to remove its read_only_parent.
                        volume
                    }
                };

                // If a read_only_parent exists, remove it from volume_id, and
                // attach it to temp_volume_id.
                let vcr: VolumeConstructionRequest =
                    serde_json::from_str(
                        volume.data()
                    )
                    .map_err(|e| {
                        TxnError::CustomError(
                            RemoveReadOnlyParentError::SerdeError(
                                e,
                            ),
                        )
                    })?;

                match vcr {
                    VolumeConstructionRequest::Volume {
                        id,
                        block_size,
                        sub_volumes,
                        read_only_parent,
                    } => {
                        if read_only_parent.is_none() {
                            // This volume has no read_only_parent
                            Ok(false)
                        } else {
                            // Create a new VCR and fill in the contents
                            // from what the original volume had.
                            let new_vcr = VolumeConstructionRequest::Volume {
                                id,
                                block_size,
                                sub_volumes,
                                read_only_parent: None,
                            };

                            let new_volume_data =
                                serde_json::to_string(
                                    &new_vcr
                                )
                                .map_err(|e| {
                                    TxnError::CustomError(
                                        RemoveReadOnlyParentError::SerdeError(
                                            e,
                                        ),
                                    )
                                })?;

                            // Update the original volume_id with the new
                            // volume.data.
                            use db::schema::volume::dsl as volume_dsl;
                            let num_updated = diesel::update(volume_dsl::volume)
                                .filter(volume_dsl::id.eq(volume_id))
                                .set(volume_dsl::data.eq(new_volume_data))
                                .execute_async(&conn)
                                .await?;

                            // This should update just one row.  If it does
                            // not, then something is terribly wrong in the
                            // database.
                            if num_updated != 1 {
                                return Err(TxnError::CustomError(
                                    RemoveReadOnlyParentError::UnexpectedDatabaseUpdate(num_updated, 1),
                                ));
                            }

                            // Make a new VCR, with the information from
                            // our temp_volume_id, but the read_only_parent
                            // from the original volume.
                            let rop_vcr = VolumeConstructionRequest::Volume {
                                id: temp_volume_id,
                                block_size,
                                sub_volumes: vec![],
                                read_only_parent,
                            };
                            let rop_volume_data =
                                serde_json::to_string(
                                    &rop_vcr
                                )
                                .map_err(|e| {
                                    TxnError::CustomError(
                                        RemoveReadOnlyParentError::SerdeError(
                                            e,
                                        ),
                                    )
                                })?;
                            // Update the temp_volume_id with the volume
                            // data that contains the read_only_parent.
                            let num_updated =
                                diesel::update(volume_dsl::volume)
                                    .filter(volume_dsl::id.eq(temp_volume_id))
                                    .filter(volume_dsl::time_deleted.is_null())
                                    .set(volume_dsl::data.eq(rop_volume_data))
                                    .execute_async(&conn)
                                    .await?;
                            if num_updated != 1 {
                                return Err(TxnError::CustomError(
                                    RemoveReadOnlyParentError::UnexpectedDatabaseUpdate(num_updated, 1),
                                ));
                            }
                            Ok(true)
                        }
                    }
                    VolumeConstructionRequest::File { id: _, block_size: _, path: _ }
                    | VolumeConstructionRequest::Region {
                        block_size: _,
                        blocks_per_extent: _,
                        extent_count: _,
                        opts: _,
                        gen: _ }
                    | VolumeConstructionRequest::Url { id: _, block_size: _, url: _ } => {
                        // Volume has a format that does not contain ROPs
                        Ok(false)
                    }
                }
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(
                    RemoveReadOnlyParentError::DieselError(e),
                ) => public_error_from_diesel(
                    e,
                    ErrorHandler::Server,
                ),

                _ => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CrucibleTargets {
    pub read_only_targets: Vec<String>,
}

// Serialize this enum into the `resources_to_clean_up` column to handle
// different versions over time.
#[derive(Debug, Serialize, Deserialize)]
pub enum CrucibleResources {
    V1(CrucibleResourcesV1),
    V2(CrucibleResourcesV2),
    V3(CrucibleResourcesV3),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV1 {
    pub datasets_and_regions: Vec<(Dataset, Region)>,
    pub datasets_and_snapshots: Vec<(Dataset, RegionSnapshot)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV2 {
    pub datasets_and_regions: Vec<(Dataset, Region)>,
    pub snapshots_to_delete: Vec<RegionSnapshot>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RegionSnapshotV3 {
    dataset: Uuid,
    region: Uuid,
    snapshot: Uuid,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV3 {
    #[serde(deserialize_with = "null_to_empty_list")]
    pub regions: Vec<Uuid>,

    #[serde(deserialize_with = "null_to_empty_list")]
    pub region_snapshots: Vec<RegionSnapshotV3>,
}

// Cockroach's `json_agg` will emit a `null` instead of a `[]` if a SELECT
// returns zero rows. Handle that with this function when deserializing.
fn null_to_empty_list<'de, D, T>(de: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(match Option::<Vec<T>>::deserialize(de)? {
        Some(v) => v,
        None => vec![],
    })
}

impl DataStore {
    /// For a CrucibleResources object, return the Regions to delete, as well as
    /// the Dataset they belong to.
    pub async fn regions_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(Dataset, Region)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V3(crucible_resources) => {
                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region::dsl as region_dsl;

                region_dsl::region
                    .filter(
                        region_dsl::id
                            .eq_any(crucible_resources.regions.clone()),
                    )
                    .inner_join(
                        dataset_dsl::dataset
                            .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
                    )
                    .select((Dataset::as_select(), Region::as_select()))
                    .get_results_async::<(Dataset, Region)>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })
            }
        }
    }

    /// For a CrucibleResources object, return the RegionSnapshots to delete, as
    /// well as the Dataset they belong to.
    pub async fn snapshots_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(Dataset, RegionSnapshot)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_snapshots.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                use db::schema::dataset::dsl;

                let mut result: Vec<_> = Vec::with_capacity(
                    crucible_resources.snapshots_to_delete.len(),
                );

                for snapshots_to_delete in
                    &crucible_resources.snapshots_to_delete
                {
                    let maybe_dataset = dsl::dataset
                        .filter(dsl::id.eq(snapshots_to_delete.dataset_id))
                        .select(Dataset::as_select())
                        .first_async(&*conn)
                        .await
                        .optional()
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;

                    match maybe_dataset {
                        Some(dataset) => {
                            result.push((dataset, snapshots_to_delete.clone()));
                        }

                        None => {
                            return Err(Error::internal_error(&format!(
                                "could not find dataset {}!",
                                snapshots_to_delete.dataset_id,
                            )));
                        }
                    }
                }

                Ok(result)
            }

            CrucibleResources::V3(crucible_resources) => {
                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region_snapshot::dsl;

                let mut datasets_and_snapshots = Vec::with_capacity(
                    crucible_resources.region_snapshots.len(),
                );

                for region_snapshots in &crucible_resources.region_snapshots {
                    let maybe_tuple = dsl::region_snapshot
                        .filter(dsl::dataset_id.eq(region_snapshots.dataset))
                        .filter(dsl::region_id.eq(region_snapshots.region))
                        .filter(dsl::snapshot_id.eq(region_snapshots.snapshot))
                        .inner_join(
                            dataset_dsl::dataset
                                .on(dsl::dataset_id.eq(dataset_dsl::id)),
                        )
                        .select((
                            Dataset::as_select(),
                            RegionSnapshot::as_select(),
                        ))
                        .first_async::<(Dataset, RegionSnapshot)>(&*conn)
                        .await
                        .optional()
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;

                    match maybe_tuple {
                        Some(tuple) => {
                            datasets_and_snapshots.push(tuple);
                        }

                        None => {
                            // If something else is deleting the exact same
                            // CrucibleResources (for example from a duplicate
                            // resource-delete saga) then these region_snapshot
                            // entries could be gone (because they are hard
                            // deleted). Skip missing entries, return only what
                            // we can find.
                        }
                    }
                }

                Ok(datasets_and_snapshots)
            }
        }
    }
}

/// Return the targets from a VolumeConstructionRequest.
///
/// The targets of a volume construction request map to resources.
fn read_only_resources_associated_with_volume(
    vcr: &VolumeConstructionRequest,
    crucible_targets: &mut CrucibleTargets,
) {
    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes,
            read_only_parent,
        } => {
            for sub_volume in sub_volumes {
                read_only_resources_associated_with_volume(
                    sub_volume,
                    crucible_targets,
                );
            }

            if let Some(read_only_parent) = read_only_parent {
                read_only_resources_associated_with_volume(
                    read_only_parent,
                    crucible_targets,
                );
            }
        }

        VolumeConstructionRequest::Url { id: _, block_size: _, url: _ } => {
            // no action required
        }

        VolumeConstructionRequest::Region {
            block_size: _,
            blocks_per_extent: _,
            extent_count: _,
            opts,
            gen: _,
        } => {
            for target in &opts.target {
                if opts.read_only {
                    crucible_targets.read_only_targets.push(target.clone());
                }
            }
        }

        VolumeConstructionRequest::File { id: _, block_size: _, path: _ } => {
            // no action required
        }
    }
}
