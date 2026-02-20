// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::DataStore;
use crate::db::datastore::volume::read_only_target_in_vcr;
use crate::db::datastore::volume::region_in_vcr;
use crate::db::model;
use crate::db::model::VolumeResourceUsageRecord;
use crate::db::model::to_db_typed_uuid;
use anyhow::anyhow;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::OptionalExtension;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::VolumeConstructionRequest;
use std::collections::VecDeque;
use std::net::AddrParseError;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Clone)]
pub struct VolumeReplacementParams {
    pub volume_id: VolumeUuid,
    pub region_id: Uuid,
    pub region_addr: SocketAddrV6,
}

// types for volume_replace_snapshot and replace_read_only_target_in_vcr
// parameters

#[derive(Debug, Clone, Copy)]
pub struct VolumeWithTarget(pub VolumeUuid);

#[derive(Debug, Clone, Copy)]
pub struct ExistingTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct ReplacementTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct VolumeToDelete(pub VolumeUuid);

// The result type returned from both `volume_replace_region` and
// `volume_replace_snapshot`
#[must_use]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum VolumeReplaceResult {
    // based on the VCRs, seems like the replacement already happened
    AlreadyHappened,

    // this call performed the replacement
    Done,

    // the "existing" volume was soft deleted
    ExistingVolumeSoftDeleted,

    // the "existing" volume was hard deleted
    ExistingVolumeHardDeleted,
}

#[derive(Debug, thiserror::Error)]
enum ReplaceRegionError {
    #[error("Error from Volume region replacement: {0}")]
    Public(Error),

    #[error("Serde error during Volume region replacement: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Region replacement error: {0}")]
    RegionReplacementError(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
enum ReplaceSnapshotError {
    #[error("Error from Volume snapshot replacement: {0}")]
    Public(Error),

    #[error("Serde error during Volume snapshot replacement: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Snapshot replacement error: {0}")]
    SnapshotReplacementError(#[from] anyhow::Error),

    #[error("Replaced {0} targets, expected {1}")]
    UnexpectedReplacedTargets(usize, usize),

    #[error("Updated {0} database rows, expected {1}")]
    UnexpectedDatabaseUpdate(usize, usize),

    #[error(
        "Address parsing error during Volume snapshot \
    replacement: {0}"
    )]
    AddressParseError(#[from] AddrParseError),

    #[error("Could not match read-only resource to {0}")]
    CouldNotFindResource(String),

    #[error("Multiple volume resource usage records for {0}")]
    MultipleResourceUsageRecords(String),
}

impl DataStore {
    async fn volume_replace_region_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ReplaceRegionError>,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<VolumeReplaceResult, diesel::result::Error> {
        // In a single transaction:
        //
        // - set the existing region's volume id to the replacement's volume id
        // - set the replacement region's volume id to the existing's volume id
        // - update the existing volume's construction request to replace the
        // existing region's SocketAddrV6 with the replacement region's
        //
        // This function's effects can be undone by calling it with swapped
        // parameters.
        //
        // # Example #
        //
        // Imagine `volume_replace_region` is called with the following,
        // pretending that UUIDs are just eight uppercase letters:
        //
        //   let existing = VolumeReplacementParams {
        //     volume_id: TARGET_VOL,
        //     region_id: TARGET_REG,
        //     region_addr: "[fd00:1122:3344:145::10]:40001",
        //   }
        //
        //   let replace = VolumeReplacementParams {
        //     volume_id: NEW_VOL,
        //     region_id: NEW_REG,
        //     region_addr: "[fd00:1122:3344:322::4]:3956",
        //   }
        //
        // In the database, the relevant records (and columns) of the region
        // table look like this prior to the transaction:
        //
        //            id | volume_id
        //  -------------| ---------
        //    TARGET_REG | TARGET_VOL
        //       NEW_REG | NEW_VOL
        //
        // TARGET_VOL has a volume construction request where one of the targets
        // list will contain TARGET_REG's address:
        //
        //   {
        //     "type": "volume",
        //     "block_size": 512,
        //     "id": "TARGET_VOL",
        //     "read_only_parent": {
        //       ...
        //     },
        //     "sub_volumes": [
        //       {
        //         ...
        //         "opts": {
        //           ...
        //           "target": [
        //             "[fd00:1122:3344:103::3]:19004",
        //             "[fd00:1122:3344:79::12]:27015",
        //             "[fd00:1122:3344:145::10]:40001"  <-----
        //           ]
        //         }
        //       }
        //     ]
        //   }
        //
        // Note it is not required for the replacement volume to exist as a
        // database record for this transaction.
        //
        // The first part of the transaction will swap the volume IDs of the
        // existing and replacement region records:
        //
        //           id | volume_id
        //  ------------| ---------
        //   TARGET_REG | NEW_VOL
        //      NEW_REG | TARGET_VOL
        //
        // The second part of the transaction will update the volume
        // construction request of TARGET_VOL by finding and replacing
        // TARGET_REG's address (in the appropriate targets array) with
        // NEW_REG's address:
        //
        //   {
        //           ...
        //           "target": [
        //             "[fd00:1122:3344:103::3]:19004",
        //             "[fd00:1122:3344:79::12]:27015",
        //             "[fd00:1122:3344:322::4]:3956"  <-----
        //           ]
        //           ...
        //   }
        //
        // After the transaction, the caller should ensure that TARGET_REG is
        // referenced (via its socket address) in NEW_VOL. For an example, this
        // is done as part of the region replacement start saga.

        // Grab the old volume first
        let maybe_old_volume = {
            volume_dsl::volume
                .filter(volume_dsl::id.eq(to_db_typed_uuid(existing.volume_id)))
                .select(model::Volume::as_select())
                .first_async::<model::Volume>(conn)
                .await
                .optional()
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceRegionError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?
        };

        let old_volume = if let Some(old_volume) = maybe_old_volume {
            old_volume
        } else {
            // Existing volume was hard-deleted, so return here. We can't
            // perform the region replacement now, and this will short-circuit
            // the rest of the process.

            return Ok(VolumeReplaceResult::ExistingVolumeHardDeleted);
        };

        if old_volume.time_deleted.is_some() {
            // Existing volume was soft-deleted, so return here for the same
            // reason: the region replacement process should be short-circuited
            // now.
            return Ok(VolumeReplaceResult::ExistingVolumeSoftDeleted);
        }

        let old_vcr: VolumeConstructionRequest =
            match serde_json::from_str(&old_volume.data()) {
                Ok(vcr) => vcr,
                Err(e) => {
                    return Err(err.bail(ReplaceRegionError::SerdeError(e)));
                }
            };

        // Does it look like this replacement already happened?
        let old_region_in_vcr =
            match region_in_vcr(&old_vcr, &existing.region_addr) {
                Ok(v) => v,
                Err(e) => {
                    return Err(
                        err.bail(ReplaceRegionError::RegionReplacementError(e))
                    );
                }
            };
        let new_region_in_vcr =
            match region_in_vcr(&old_vcr, &replacement.region_addr) {
                Ok(v) => v,
                Err(e) => {
                    return Err(
                        err.bail(ReplaceRegionError::RegionReplacementError(e))
                    );
                }
            };

        if !old_region_in_vcr && new_region_in_vcr {
            // It does seem like the replacement happened - if this function is
            // called twice in a row then this can happen.
            return Ok(VolumeReplaceResult::AlreadyHappened);
        } else if old_region_in_vcr && !new_region_in_vcr {
            // The replacement hasn't happened yet, but can proceed
        } else if old_region_in_vcr && new_region_in_vcr {
            // Both the old region and new region exist in this VCR. Regions are
            // not reused, so this is an illegal state: if the replacement of
            // the old region occurred, then the new region would be present
            // multiple times in the volume. We have to bail out here.
            //
            // The guards against this happening are:
            //
            // - only one replacement can occur for a volume at a time (due to
            //   the volume repair lock), and
            //
            // - region replacement does not delete the old region until the
            //   "region replacement finish" saga, which happens at the very end
            //   of the process. If it eagerly deleted the region, the crucible
            //   agent would be free to reuse the port for another region
            //   allocation, and an identical target (read: ip and port) could
            //   be confusing. Most of the time, we assume that the dataset
            //   containing that agent has been expunged, so the agent is gone,
            //   so this port reuse cannot occur
            return Err(err.bail(ReplaceRegionError::RegionReplacementError(
                anyhow!("old_region_in_vcr && new_region_in_vcr"),
            )));
        } else if !old_region_in_vcr && !new_region_in_vcr {
            // Neither the region we've been asked to replace or the new region
            // is in the VCR. This is an illegal state, as this function would
            // be performing a no-op. We have to bail out here.
            //
            // The guard against this happening is again that only one
            // replacement can occur for a volume at a time: if it was possible
            // for multiple region replacements to occur, then both would be
            // attempting to swap out the same old region for different new
            // regions:
            //
            // region replacement one:
            //
            //   volume_replace_region_in_txn(
            //     ..,
            //     existing = [fd00:1122:3344:145::10]:40001,
            //     replacement = [fd00:1122:3344:322::4]:3956,
            //   )
            //
            // region replacement two:
            //
            //   volume_replace_region_in_txn(
            //     ..,
            //     existing = [fd00:1122:3344:145::10]:40001,
            //     replacement = [fd00:1122:3344:fd1::123]:27001,
            //   )
            //
            // The one that replaced second would always land in this branch.
            return Err(err.bail(ReplaceRegionError::RegionReplacementError(
                anyhow!("!old_region_in_vcr && !new_region_in_vcr"),
            )));
        }

        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::volume::dsl as volume_dsl;

        // Set the existing region's volume id to the replacement's volume id
        diesel::update(region_dsl::region)
            .filter(region_dsl::id.eq(existing.region_id))
            .set(
                region_dsl::volume_id
                    .eq(to_db_typed_uuid(replacement.volume_id)),
            )
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Set the replacement region's volume id to the existing's volume id
        diesel::update(region_dsl::region)
            .filter(region_dsl::id.eq(replacement.region_id))
            .set(region_dsl::volume_id.eq(to_db_typed_uuid(existing.volume_id)))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Update the existing volume's construction request to replace the
        // existing region's SocketAddrV6 with the replacement region's

        // Copy the old volume's VCR, changing out the old region for the new.
        let new_vcr = match replace_region_in_vcr(
            &old_vcr,
            existing.region_addr,
            replacement.region_addr,
        ) {
            Ok(new_vcr) => new_vcr,
            Err(e) => {
                return Err(
                    err.bail(ReplaceRegionError::RegionReplacementError(e))
                );
            }
        };

        let new_volume_data = serde_json::to_string(&new_vcr)
            .map_err(|e| err.bail(ReplaceRegionError::SerdeError(e)))?;

        // Update the existing volume's data
        diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(existing.volume_id)))
            .set(volume_dsl::data.eq(new_volume_data))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // After region replacement, validate invariants for all volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(conn).await?;

        Ok(VolumeReplaceResult::Done)
    }

    /// Replace a read-write region in a Volume with a new region.
    pub async fn volume_replace_region(
        &self,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<VolumeReplaceResult, Error> {
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_region")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let existing = existing.clone();
                let replacement = replacement.clone();
                async move {
                    Self::volume_replace_region_in_txn(
                        &conn,
                        err,
                        existing,
                        replacement,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        ReplaceRegionError::Public(e) => e,

                        ReplaceRegionError::SerdeError(_) => {
                            Error::internal_error(&err.to_string())
                        }

                        ReplaceRegionError::RegionReplacementError(_) => {
                            Error::internal_error(&err.to_string())
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    async fn volume_replace_snapshot_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ReplaceSnapshotError>,
        volume_id: VolumeWithTarget,
        existing: ExistingTarget,
        replacement: ReplacementTarget,
        volume_to_delete_id: VolumeToDelete,
    ) -> Result<VolumeReplaceResult, diesel::result::Error> {
        use nexus_db_schema::schema::volume::dsl as volume_dsl;
        use nexus_db_schema::schema::volume_resource_usage::dsl as ru_dsl;

        // Grab the old volume first
        let maybe_old_volume = {
            volume_dsl::volume
                .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id.0)))
                .select(model::Volume::as_select())
                .first_async::<model::Volume>(conn)
                .await
                .optional()
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceSnapshotError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?
        };

        let old_volume = if let Some(old_volume) = maybe_old_volume {
            old_volume
        } else {
            // Existing volume was hard-deleted, so return here. We can't
            // perform the region replacement now, and this will short-circuit
            // the rest of the process.

            return Ok(VolumeReplaceResult::ExistingVolumeHardDeleted);
        };

        if old_volume.time_deleted.is_some() {
            // Existing volume was soft-deleted, so return here for the same
            // reason: the region replacement process should be short-circuited
            // now.
            return Ok(VolumeReplaceResult::ExistingVolumeSoftDeleted);
        }

        let old_vcr: VolumeConstructionRequest =
            match serde_json::from_str(&old_volume.data()) {
                Ok(vcr) => vcr,
                Err(e) => {
                    return Err(err.bail(ReplaceSnapshotError::SerdeError(e)));
                }
            };

        // Does it look like this replacement already happened?
        let old_target_in_vcr =
            match read_only_target_in_vcr(&old_vcr, &existing.0) {
                Ok(v) => v,
                Err(e) => {
                    return Err(err.bail(
                        ReplaceSnapshotError::SnapshotReplacementError(e),
                    ));
                }
            };

        let new_target_in_vcr =
            match read_only_target_in_vcr(&old_vcr, &replacement.0) {
                Ok(v) => v,
                Err(e) => {
                    return Err(err.bail(
                        ReplaceSnapshotError::SnapshotReplacementError(e),
                    ));
                }
            };

        if !old_target_in_vcr && new_target_in_vcr {
            // It does seem like the replacement happened
            return Ok(VolumeReplaceResult::AlreadyHappened);
        }

        // Update the existing volume's construction request to replace the
        // existing target's SocketAddrV6 with the replacement target's

        // Copy the old volume's VCR, changing out the old target for the new.
        let (new_vcr, replacements) = match replace_read_only_target_in_vcr(
            &old_vcr,
            existing,
            replacement,
        ) {
            Ok(new_vcr) => new_vcr,
            Err(e) => {
                return Err(
                    err.bail(ReplaceSnapshotError::SnapshotReplacementError(e))
                );
            }
        };

        // Expect that this only happened once. If it happened multiple times,
        // question everything: how would a snapshot be used twice?!

        if replacements != 1 {
            return Err(err.bail(
                ReplaceSnapshotError::UnexpectedReplacedTargets(
                    replacements,
                    1,
                ),
            ));
        }

        let new_volume_data = serde_json::to_string(&new_vcr)
            .map_err(|e| err.bail(ReplaceSnapshotError::SerdeError(e)))?;

        // Update the existing volume's data
        diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id.0)))
            .set(volume_dsl::data.eq(new_volume_data))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Make a new VCR that will stash the target to delete. The values here
        // don't matter, just that it gets fed into the volume_delete machinery
        // later.
        let vcr = VolumeConstructionRequest::Volume {
            id: *volume_to_delete_id.0.as_untyped_uuid(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 1,
                extent_count: 1,
                generation: 1,
                opts: sled_agent_client::CrucibleOpts {
                    id: *volume_to_delete_id.0.as_untyped_uuid(),
                    target: vec![existing.0.into()],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: true,
                },
            }],
            read_only_parent: None,
        };

        let volume_data = serde_json::to_string(&vcr)
            .map_err(|e| err.bail(ReplaceSnapshotError::SerdeError(e)))?;

        // Update the volume to delete data
        let num_updated = diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_to_delete_id.0)))
            .filter(volume_dsl::time_deleted.is_null())
            .set(volume_dsl::data.eq(volume_data))
            .execute_async(conn)
            .await?;

        if num_updated != 1 {
            return Err(err.bail(
                ReplaceSnapshotError::UnexpectedDatabaseUpdate(num_updated, 1),
            ));
        }

        // Update the appropriate volume resource usage records - it could
        // either be a read-only region or a region snapshot, so determine what
        // it is first

        let maybe_existing_usage =
            Self::read_only_target_to_volume_resource_usage(conn, &existing.0)
                .await?;

        let Some(existing_usage) = maybe_existing_usage else {
            return Err(err.bail(ReplaceSnapshotError::CouldNotFindResource(
                format!("could not find resource for {}", existing.0,),
            )));
        };

        // The "existing" target moved into the volume to delete

        Self::swap_volume_usage_records_for_resources(
            conn,
            existing_usage,
            volume_id.0,
            volume_to_delete_id.0,
        )
        .await
        .map_err(|e| {
            err.bail_retryable_or_else(e, |e| {
                ReplaceSnapshotError::Public(public_error_from_diesel(
                    e,
                    ErrorHandler::Server,
                ))
            })
        })?;

        let maybe_replacement_usage =
            Self::read_only_target_to_volume_resource_usage(
                conn,
                &replacement.0,
            )
            .await?;

        let Some(replacement_usage) = maybe_replacement_usage else {
            return Err(err.bail(ReplaceSnapshotError::CouldNotFindResource(
                format!("could not find resource for {}", existing.0,),
            )));
        };

        // The intention leaving this transaction is that the correct volume
        // resource usage records exist, so:
        //
        // - if no usage record existed for the replacement usage, then create a
        //   new record that points to the volume id (this can happen if the
        //   volume to delete was blank when coming into this function)
        //
        // - if records exist for the "replacement" usage, then one of those
        //   will match the volume to delete id, so perform a swap instead to
        //   the volume id

        let existing_replacement_volume_usage_records =
            Self::volume_usage_records_for_resource_query(
                replacement_usage.clone(),
            )
            .load_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?
            // TODO be smart enough to .filter the above query
            .into_iter()
            .filter(|record| record.volume_id == volume_to_delete_id.0.into())
            .count();

        // The "replacement" target moved into the volume

        if existing_replacement_volume_usage_records == 0 {
            // No matching record
            let new_record =
                VolumeResourceUsageRecord::new(volume_id.0, replacement_usage);

            diesel::insert_into(ru_dsl::volume_resource_usage)
                .values(new_record)
                .execute_async(conn)
                .await
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceSnapshotError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?;
        } else if existing_replacement_volume_usage_records == 1 {
            // One matching record: perform swap
            Self::swap_volume_usage_records_for_resources(
                conn,
                replacement_usage,
                volume_to_delete_id.0,
                volume_id.0,
            )
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;
        } else {
            // More than one matching record!
            return Err(err.bail(
                ReplaceSnapshotError::MultipleResourceUsageRecords(format!(
                    "{replacement_usage:?}"
                )),
            ));
        }

        // After region snapshot replacement, validate invariants for all
        // volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(conn).await?;

        Ok(VolumeReplaceResult::Done)
    }

    /// Replace a read-only target in a Volume with a new region
    ///
    /// In a single transaction:
    ///
    /// - update a volume's serialized construction request by replacing a
    ///   single target.
    ///
    /// - stash the replaced target in a "volume to delete"'s serialized
    ///   construction request
    ///
    /// Note that this transaction does _not_ update a region snapshot's volume
    /// references table! This is legal because the existing target reference is
    /// written into the volume to delete's construction request.
    ///
    /// This function's effects can be undone by calling it with swapped
    /// `existing` and `replacement` parameters.
    pub async fn volume_replace_snapshot(
        &self,
        volume_id: VolumeWithTarget,
        existing: ExistingTarget,
        replacement: ReplacementTarget,
        volume_to_delete_id: VolumeToDelete,
    ) -> Result<VolumeReplaceResult, Error> {
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_snapshot")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    Self::volume_replace_snapshot_in_txn(
                        &conn,
                        err,
                        volume_id,
                        existing,
                        replacement,
                        volume_to_delete_id,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        ReplaceSnapshotError::Public(e) => e,

                        ReplaceSnapshotError::SerdeError(_)
                        | ReplaceSnapshotError::SnapshotReplacementError(_)
                        | ReplaceSnapshotError::UnexpectedReplacedTargets(
                            _,
                            _,
                        )
                        | ReplaceSnapshotError::UnexpectedDatabaseUpdate(
                            _,
                            _,
                        )
                        | ReplaceSnapshotError::AddressParseError(_)
                        | ReplaceSnapshotError::CouldNotFindResource(_)
                        | ReplaceSnapshotError::MultipleResourceUsageRecords(
                            _,
                        ) => Error::internal_error(&err.to_string()),
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }
}

/// Replace a Region in a VolumeConstructionRequest
///
/// Note that UUIDs are not randomized by this step: Crucible will reject a
/// `target_replace` call if the replacement VolumeConstructionRequest does not
/// exactly match the original, except for a single Region difference.
///
/// Note that the generation number _is_ bumped in this step, otherwise
/// `compare_vcr_for_update` will reject the update.
pub fn replace_region_in_vcr(
    vcr: &VolumeConstructionRequest,
    old_region: SocketAddrV6,
    new_region: SocketAddrV6,
) -> anyhow::Result<VolumeConstructionRequest> {
    let mut new_vcr = vcr.clone();

    let mut parts: VecDeque<&mut VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&mut new_vcr);

    let mut old_region_found = false;

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                // Skip looking at read-only parent, this function only replaces
                // R/W regions
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, generation, .. } => {
                for target in &mut opts.target {
                    if let SocketAddr::V6(target) = target {
                        if *target == old_region {
                            *target = new_region;
                            old_region_found = true;
                        }
                    }
                }

                // Bump generation number, otherwise update will be rejected
                *generation = *generation + 1;
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    if !old_region_found {
        bail!("old region {old_region} not found!");
    }

    Ok(new_vcr)
}

/// Replace a read-only target in a VolumeConstructionRequest
///
/// Note that UUIDs are not randomized by this step: Crucible will reject a
/// `target_replace` call if the replacement VolumeConstructionRequest does not
/// exactly match the original, except for a single Region difference.
///
/// Note that the generation number _is not_ bumped in this step.
pub fn replace_read_only_target_in_vcr(
    vcr: &VolumeConstructionRequest,
    old_target: ExistingTarget,
    new_target: ReplacementTarget,
) -> anyhow::Result<(VolumeConstructionRequest, usize)> {
    struct Work<'a> {
        vcr_part: &'a mut VolumeConstructionRequest,
        under_read_only_parent: bool,
    }
    let mut new_vcr = vcr.clone();

    let mut parts: VecDeque<Work> = VecDeque::new();
    parts.push_back(Work {
        vcr_part: &mut new_vcr,
        under_read_only_parent: false,
    });

    let mut replacements = 0;

    while let Some(work) = parts.pop_front() {
        match work.vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(Work {
                        vcr_part: sub_volume,
                        under_read_only_parent: work.under_read_only_parent,
                    });
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(Work {
                        vcr_part: read_only_parent,
                        under_read_only_parent: true,
                    });
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                if work.under_read_only_parent && !opts.read_only {
                    // This VCR isn't constructed properly, there's a read/write
                    // region under a read-only parent
                    bail!("read-write region under read-only parent");
                }

                for target in &mut opts.target {
                    if let SocketAddr::V6(target) = target {
                        if *target == old_target.0 && opts.read_only {
                            *target = new_target.0;
                            replacements += 1;
                        }
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    if replacements == 0 {
        bail!("target {old_target:?} not found!");
    }

    Ok((new_vcr, replacements))
}
