// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionSnapshotReplacement`] and
//! [`RegionSnapshotReplacementStep`] objects.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::lookup::LookupPath;
use crate::db::model::RegionSnapshot;
use crate::db::model::RegionSnapshotReplacement;
use crate::db::model::RegionSnapshotReplacementState;
use crate::db::model::RegionSnapshotReplacementStep;
use crate::db::model::RegionSnapshotReplacementStepState;
use crate::db::model::VolumeRepair;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use uuid::Uuid;

#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub enum InsertStepResult {
    /// A new region snapshot replacement step was inserted.
    Inserted { step_id: Uuid },

    /// A region snapshot replacement step exists already that references this
    /// volume id, so no new record is inserted.
    AlreadyHandled { existing_step_id: Uuid },
}

impl DataStore {
    /// Create and insert a region snapshot replacement request for a
    /// RegionSnapshot, returning the ID of the request.
    pub async fn create_region_snapshot_replacement_request(
        &self,
        opctx: &OpContext,
        region_snapshot: &RegionSnapshot,
    ) -> Result<Uuid, Error> {
        let request =
            RegionSnapshotReplacement::for_region_snapshot(region_snapshot);
        let request_id = request.id;

        self.insert_region_snapshot_replacement_request(opctx, request).await?;

        Ok(request_id)
    }

    /// Insert a region snapshot replacement request into the DB, also creating
    /// the VolumeRepair record.
    pub async fn insert_region_snapshot_replacement_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
    ) -> Result<(), Error> {
        let (.., db_snapshot) = LookupPath::new(opctx, &self)
            .snapshot_id(request.old_snapshot_id)
            .fetch()
            .await?;

        self.insert_region_snapshot_replacement_request_with_volume_id(
            opctx,
            request,
            db_snapshot.volume_id,
        )
        .await
    }

    /// Insert a region snapshot replacement request into the DB, also creating
    /// the VolumeRepair record.
    pub async fn insert_region_snapshot_replacement_request_with_volume_id(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
        volume_id: Uuid,
    ) -> Result<(), Error> {
        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::region_snapshot_replacement::dsl;
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                // An associated volume repair record isn't _strictly_ needed:
                // snapshot volumes should never be directly constructed, and
                // therefore won't ever have an associated Upstairs that
                // receives a volume replacement request. However it's being
                // done in an attempt to be overly cautious.

                diesel::insert_into(volume_repair_dsl::volume_repair)
                    .values(VolumeRepair { volume_id, repair_id: request.id })
                    .execute_async(&conn)
                    .await?;

                diesel::insert_into(dsl::region_snapshot_replacement)
                    .values(request)
                    .execute_async(&conn)
                    .await?;

                Ok(())
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_region_snapshot_replacement_request_by_id(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<RegionSnapshotReplacement, Error> {
        use db::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(dsl::id.eq(id))
            .get_result_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Find a region snapshot replacement request by region snapshot
    pub async fn lookup_region_snapshot_replacement_request(
        &self,
        opctx: &OpContext,
        region_snapshot: &RegionSnapshot,
    ) -> Result<Option<RegionSnapshotReplacement>, Error> {
        use db::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(dsl::old_dataset_id.eq(region_snapshot.dataset_id))
            .filter(dsl::old_region_id.eq(region_snapshot.region_id))
            .filter(dsl::old_snapshot_id.eq(region_snapshot.snapshot_id))
            .get_result_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return region snapshot replacement records in state `Requested` with no
    /// currently operating saga.
    pub async fn get_requested_region_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionSnapshotReplacement>, Error> {
        use db::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Requested),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return region snapshot replacement requests that are in state `Running`
    /// with no currently operating saga.
    pub async fn get_running_region_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionSnapshotReplacement>, Error> {
        use db::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Running),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return region snapshot replacement requests that are in state
    /// `ReplacementDone` with no currently operating saga.
    pub async fn get_replacement_done_region_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionSnapshotReplacement>, Error> {
        use db::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::ReplacementDone),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Transition a RegionSnapshotReplacement record from Requested to
    /// Allocating, setting a unique id at the same time.
    pub async fn set_region_snapshot_replacement_allocating(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Requested),
            )
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Allocating),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionSnapshotReplacementState::Allocating
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionSnapshotReplacement record from Allocating to
    /// Requested, clearing the operating saga id.
    pub async fn undo_set_region_snapshot_replacement_allocating(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Allocating),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionSnapshotReplacementState::Requested
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition from Allocating to ReplacementDone, and clear the operating
    /// saga id.
    pub async fn set_region_snapshot_replacement_replacement_done(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
        new_region_id: Uuid,
        old_snapshot_volume_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Allocating),
            )
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::ReplacementDone),
                dsl::old_snapshot_volume_id.eq(Some(old_snapshot_volume_id)),
                dsl::new_region_id.eq(Some(new_region_id)),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionSnapshotReplacementState::ReplacementDone
                        && record.new_region_id == Some(new_region_id)
                        && record.old_snapshot_volume_id
                            == Some(old_snapshot_volume_id)
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionSnapshotReplacement record from ReplacementDone to
    /// DeletingOldVolume, setting a unique id at the same time.
    pub async fn set_region_snapshot_replacement_deleting_old_volume(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::ReplacementDone),
            )
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::DeletingOldVolume),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionSnapshotReplacementState::DeletingOldVolume
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionSnapshotReplacement record from DeletingOldVolume to
    /// ReplacementDone, clearing the operating saga id.
    pub async fn undo_set_region_snapshot_replacement_deleting_old_volume(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::DeletingOldVolume),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::ReplacementDone),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionSnapshotReplacementState::ReplacementDone
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition from DeletingOldVolume to Running, and clear the operating
    /// saga id.
    pub async fn set_region_snapshot_replacement_running(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::DeletingOldVolume),
            )
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Running),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionSnapshotReplacement>(
                region_snapshot_replacement_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionSnapshotReplacementState::Running
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionSnapshotReplacement record from Running to Complete.
    /// Also removes the `volume_repair` record that is taking a "lock" on the
    /// Volume. Note this doesn't occur from a saga context, and therefore 1)
    /// doesn't accept an operating saga id parameter, and 2) checks that
    /// operating_saga_id is null for the corresponding record.
    pub async fn set_region_snapshot_replacement_complete(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair.filter(
                        volume_repair_dsl::repair_id
                            .eq(region_snapshot_replacement_id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use db::schema::region_snapshot_replacement::dsl;

                let result = diesel::update(dsl::region_snapshot_replacement)
                    .filter(dsl::id.eq(region_snapshot_replacement_id))
                    .filter(
                        dsl::replacement_state
                            .eq(RegionSnapshotReplacementState::Running),
                    )
                    .filter(dsl::operating_saga_id.is_null())
                    .set((dsl::replacement_state
                        .eq(RegionSnapshotReplacementState::Complete),))
                    .check_if_exists::<RegionSnapshotReplacement>(
                        region_snapshot_replacement_id,
                    )
                    .execute_and_check(&conn)
                    .await?;

                match result.status {
                    UpdateStatus::Updated => Ok(()),
                    UpdateStatus::NotUpdatedButExists => {
                        let record = result.found;

                        if record.replacement_state
                            == RegionSnapshotReplacementState::Complete
                        {
                            Ok(())
                        } else {
                            Err(TxnError::CustomError(Error::conflict(
                                format!(
                                "region snapshot replacement {} set to {:?} \
                                (operating saga id {:?})",
                                region_snapshot_replacement_id,
                                record.replacement_state,
                                record.operating_saga_id,
                            ),
                            )))
                        }
                    }
                }
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(error) => error,

                TxnError::Database(error) => {
                    public_error_from_diesel(error, ErrorHandler::Server)
                }
            })
    }

    pub async fn create_region_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request_id: Uuid,
        volume_id: Uuid,
    ) -> Result<InsertStepResult, Error> {
        let request = RegionSnapshotReplacementStep::new(request_id, volume_id);

        self.insert_region_snapshot_replacement_step(opctx, request).await
    }

    pub async fn insert_region_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacementStep,
    ) -> Result<InsertStepResult, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "insert_region_snapshot_replacement_step",
        )
        .transaction(&conn, |conn| {
            let request = request.clone();

            async move {
                use db::schema::region_snapshot_replacement_step::dsl;
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                // Skip inserting this new record if we found another region
                // snapshot replacement step with this volume in the step's
                // `old_snapshot_volume_id`, as that means we're duplicating
                // the replacement work: that volume will be garbage
                // collected later. There's a unique index that will prevent
                // the same step being inserted with the same volume id.

                let maybe_record = dsl::region_snapshot_replacement_step
                    .filter(dsl::old_snapshot_volume_id.eq(request.volume_id))
                    .get_result_async::<RegionSnapshotReplacementStep>(&conn)
                    .await
                    .optional()?;

                if let Some(found_record) = maybe_record {
                    return Ok(InsertStepResult::AlreadyHandled {
                        existing_step_id: found_record.id,
                    });
                }

                // Skip inserting this record if we found an existing region
                // snapshot replacement step for it in a non-complete state.

                let maybe_record = dsl::region_snapshot_replacement_step
                    .filter(dsl::volume_id.eq(request.volume_id))
                    .get_result_async::<RegionSnapshotReplacementStep>(&conn)
                    .await
                    .optional()?;

                if let Some(found_record) = maybe_record {
                    match found_record.replacement_state {
                        RegionSnapshotReplacementStepState::Complete
                        | RegionSnapshotReplacementStepState::VolumeDeleted => {
                            // Ok, we can insert another record with a matching
                            // volume ID because the volume_repair record would
                            // have been deleted during the transition to
                            // Complete.
                        }

                        RegionSnapshotReplacementStepState::Requested
                        | RegionSnapshotReplacementStepState::Running => {
                            return Ok(InsertStepResult::AlreadyHandled {
                                existing_step_id: found_record.id,
                            });
                        }
                    }
                }

                // The region snapshot replacement step saga could invoke a
                // volume replacement: create an associated volume repair
                // record.

                diesel::insert_into(volume_repair_dsl::volume_repair)
                    .values(VolumeRepair {
                        volume_id: request.volume_id,
                        repair_id: request.id,
                    })
                    .execute_async(&conn)
                    .await?;

                let request_id = request.id;

                diesel::insert_into(dsl::region_snapshot_replacement_step)
                    .values(request)
                    .execute_async(&conn)
                    .await?;

                Ok(InsertStepResult::Inserted { step_id: request_id })
            }
        })
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_region_snapshot_replacement_step_by_id(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
    ) -> Result<RegionSnapshotReplacementStep, Error> {
        use db::schema::region_snapshot_replacement_step::dsl;

        dsl::region_snapshot_replacement_step
            .filter(dsl::id.eq(region_snapshot_replacement_step_id))
            .get_result_async::<RegionSnapshotReplacementStep>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_requested_region_snapshot_replacement_steps(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionSnapshotReplacementStep>, Error> {
        opctx.check_complex_operations_allowed()?;

        let mut records = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use db::schema::region_snapshot_replacement_step::dsl;

            let batch = paginated(
                dsl::region_snapshot_replacement_step,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Requested),
            )
            .get_results_async::<RegionSnapshotReplacementStep>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|r| r.id);
            records.extend(batch);
        }

        Ok(records)
    }

    pub async fn set_region_snapshot_replacement_step_running(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement_step::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement_step)
            .filter(dsl::id.eq(region_snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Requested),
            )
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Running),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionSnapshotReplacementStep>(
                region_snapshot_replacement_step_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionSnapshotReplacementStepState::Running
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement step {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_step_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionSnapshotReplacementStep record from Running to
    /// Requested, clearing the operating saga id.
    pub async fn undo_set_region_snapshot_replacement_step_running(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement_step::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement_step)
            .filter(dsl::id.eq(region_snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Running),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionSnapshotReplacementStep>(
                region_snapshot_replacement_step_id,
            )
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionSnapshotReplacementStepState::Requested
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement step {} set to {:?} \
                            (operating saga id {:?})",
                            region_snapshot_replacement_step_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition from Running to Complete, clearing the operating saga id and
    /// removing the associated `volume_repair` record.
    pub async fn set_region_snapshot_replacement_step_complete(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
        operating_saga_id: Uuid,
        old_snapshot_volume_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair.filter(
                        volume_repair_dsl::repair_id
                            .eq(region_snapshot_replacement_step_id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use db::schema::region_snapshot_replacement_step::dsl;
                let result =
                    diesel::update(dsl::region_snapshot_replacement_step)
                        .filter(dsl::id.eq(region_snapshot_replacement_step_id))
                        .filter(dsl::operating_saga_id.eq(operating_saga_id))
                        .filter(dsl::old_snapshot_volume_id.is_null())
                        .filter(
                            dsl::replacement_state.eq(
                                RegionSnapshotReplacementStepState::Running,
                            ),
                        )
                        .set((
                            dsl::replacement_state.eq(
                                RegionSnapshotReplacementStepState::Complete,
                            ),
                            dsl::operating_saga_id.eq(Option::<Uuid>::None),
                            dsl::old_snapshot_volume_id
                                .eq(old_snapshot_volume_id),
                        ))
                        .check_if_exists::<RegionSnapshotReplacementStep>(
                            region_snapshot_replacement_step_id,
                        )
                        .execute_and_check(&conn)
                        .await?;

                match result.status {
                    UpdateStatus::Updated => Ok(()),
                    UpdateStatus::NotUpdatedButExists => {
                        let record = result.found;

                        if record.operating_saga_id == None
                            && record.replacement_state
                                == RegionSnapshotReplacementStepState::Complete
                        {
                            Ok(())
                        } else {
                            Err(TxnError::CustomError(Error::conflict(
                                format!(
                                    "region snapshot replacement step {} set \
                                    to {:?} (operating saga id {:?})",
                                    region_snapshot_replacement_step_id,
                                    record.replacement_state,
                                    record.operating_saga_id,
                                ),
                            )))
                        }
                    }
                }
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(error) => error,

                TxnError::Database(error) => {
                    public_error_from_diesel(error, ErrorHandler::Server)
                }
            })
    }

    /// Count all in-progress region snapshot replacement steps for a particular
    /// region snapshot replacement id.
    pub async fn in_progress_region_snapshot_replacement_steps(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
    ) -> Result<i64, Error> {
        use db::schema::region_snapshot_replacement_step::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let records = dsl::region_snapshot_replacement_step
            .filter(dsl::request_id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .ne(RegionSnapshotReplacementStepState::VolumeDeleted),
            )
            .count()
            .get_result_async::<i64>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(records)
    }

    /// Return all region snapshot replacement steps that are Complete
    pub async fn region_snapshot_replacement_steps_requiring_garbage_collection(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionSnapshotReplacementStep>, Error> {
        use db::schema::region_snapshot_replacement_step;

        let conn = self.pool_connection_authorized(opctx).await?;

        region_snapshot_replacement_step::table
            .filter(
                region_snapshot_replacement_step::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Complete),
            )
            .select(RegionSnapshotReplacementStep::as_select())
            .get_results_async::<RegionSnapshotReplacementStep>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Set a region snapshot replacement step's state to VolumeDeleted
    pub async fn set_region_snapshot_replacement_step_volume_deleted(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_snapshot_replacement_step::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let updated = diesel::update(dsl::region_snapshot_replacement_step)
            .filter(dsl::id.eq(region_snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::Complete),
            )
            .set(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementStepState::VolumeDeleted),
            )
            .check_if_exists::<RegionSnapshotReplacementStep>(
                region_snapshot_replacement_step_id,
            )
            .execute_and_check(&conn)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.replacement_state
                        == RegionSnapshotReplacementStepState::VolumeDeleted
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region snapshot replacement step {} set to {:?}",
                            region_snapshot_replacement_step_id,
                            record.replacement_state,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::model::RegionReplacement;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_one_replacement_per_volume() {
        let logctx = dev::test_setup_log("test_one_replacement_per_volume");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let dataset_1_id = Uuid::new_v4();
        let region_1_id = Uuid::new_v4();
        let snapshot_1_id = Uuid::new_v4();

        let dataset_2_id = Uuid::new_v4();
        let region_2_id = Uuid::new_v4();
        let snapshot_2_id = Uuid::new_v4();

        let volume_id = Uuid::new_v4();

        let request_1 = RegionSnapshotReplacement::new(
            dataset_1_id,
            region_1_id,
            snapshot_1_id,
        );

        let request_2 = RegionSnapshotReplacement::new(
            dataset_2_id,
            region_2_id,
            snapshot_2_id,
        );

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request_1, volume_id,
            )
            .await
            .unwrap();

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request_2, volume_id,
            )
            .await
            .unwrap_err();

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_one_replacement_per_volume_conflict_with_region() {
        let logctx = dev::test_setup_log(
            "test_one_replacement_per_volume_conflict_with_region",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let dataset_1_id = Uuid::new_v4();
        let region_1_id = Uuid::new_v4();
        let snapshot_1_id = Uuid::new_v4();

        let region_2_id = Uuid::new_v4();

        let volume_id = Uuid::new_v4();

        let request_1 = RegionSnapshotReplacement::new(
            dataset_1_id,
            region_1_id,
            snapshot_1_id,
        );

        let request_2 = RegionReplacement::new(region_2_id, volume_id);

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request_1, volume_id,
            )
            .await
            .unwrap();

        datastore
            .insert_region_replacement_request(&opctx, request_2)
            .await
            .unwrap_err();

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn count_replacement_steps() {
        let logctx = dev::test_setup_log("count_replacement_steps");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let dataset_id = Uuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let volume_id = Uuid::new_v4();

        let request =
            RegionSnapshotReplacement::new(dataset_id, region_id, snapshot_id);

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        // Make sure counts start at 0

        assert_eq!(
            datastore
                .in_progress_region_snapshot_replacement_steps(
                    &opctx, request_id
                )
                .await
                .unwrap(),
            0,
        );

        assert!(datastore
            .get_requested_region_snapshot_replacement_steps(&opctx)
            .await
            .unwrap()
            .is_empty());

        // Insert some replacement steps, and make sure counting works

        {
            let step = RegionSnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            let result = datastore
                .insert_region_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();

            assert!(matches!(result, InsertStepResult::Inserted { .. }));
        }

        assert_eq!(
            datastore
                .in_progress_region_snapshot_replacement_steps(
                    &opctx, request_id
                )
                .await
                .unwrap(),
            1,
        );

        assert_eq!(
            datastore
                .get_requested_region_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        {
            let mut step = RegionSnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            step.replacement_state =
                RegionSnapshotReplacementStepState::Running;

            let result = datastore
                .insert_region_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();

            assert!(matches!(result, InsertStepResult::Inserted { .. }));
        }

        assert_eq!(
            datastore
                .in_progress_region_snapshot_replacement_steps(
                    &opctx, request_id
                )
                .await
                .unwrap(),
            2,
        );

        assert_eq!(
            datastore
                .get_requested_region_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        {
            let mut step = RegionSnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            // VolumeDeleted does not count as "in-progress"
            step.replacement_state =
                RegionSnapshotReplacementStepState::VolumeDeleted;

            let result = datastore
                .insert_region_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();

            assert!(matches!(result, InsertStepResult::Inserted { .. }));
        }

        assert_eq!(
            datastore
                .in_progress_region_snapshot_replacement_steps(
                    &opctx, request_id
                )
                .await
                .unwrap(),
            2,
        );

        assert_eq!(
            datastore
                .get_requested_region_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn unique_region_snapshot_replacement_step_per_volume() {
        let logctx = dev::test_setup_log(
            "unique_region_snapshot_replacement_step_per_volume",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Ensure that only one non-complete replacement step can be inserted
        // per volume.

        let volume_id = Uuid::new_v4();

        let step =
            RegionSnapshotReplacementStep::new(Uuid::new_v4(), volume_id);
        let first_request_id = step.id;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert!(matches!(result, InsertStepResult::Inserted { .. }));

        let step =
            RegionSnapshotReplacementStep::new(Uuid::new_v4(), volume_id);

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap();

        let InsertStepResult::AlreadyHandled { existing_step_id } = result
        else {
            panic!("wrong return type: {result:?}");
        };
        assert_eq!(existing_step_id, first_request_id);

        // Ensure that transitioning the first step to running doesn't change
        // things.

        let saga_id = Uuid::new_v4();

        datastore
            .set_region_snapshot_replacement_step_running(
                &opctx,
                first_request_id,
                saga_id,
            )
            .await
            .unwrap();

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap();

        let InsertStepResult::AlreadyHandled { existing_step_id } = result
        else {
            panic!("wrong return type: {result:?}");
        };
        assert_eq!(existing_step_id, first_request_id);

        // Ensure that transitioning the first step to complete means another
        // can be added.

        datastore
            .set_region_snapshot_replacement_step_complete(
                &opctx,
                first_request_id,
                saga_id,
                Uuid::new_v4(), // old_snapshot_volume_id
            )
            .await
            .unwrap();

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap();

        let InsertStepResult::Inserted { step_id } = result else {
            panic!("wrong return type: {result:?}");
        };
        assert_eq!(step_id, step.id);

        // Ensure that transitioning the first step to volume deleted still
        // works.

        datastore
            .set_region_snapshot_replacement_step_volume_deleted(
                &opctx,
                first_request_id,
            )
            .await
            .unwrap();

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_gc() {
        let logctx = dev::test_setup_log("region_snapshot_replacement_step_gc");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut request = RegionSnapshotReplacement::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state = RegionSnapshotReplacementState::Complete;

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        assert!(datastore
            .region_snapshot_replacement_steps_requiring_garbage_collection(
                &opctx
            )
            .await
            .unwrap()
            .is_empty());

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step.replacement_state = RegionSnapshotReplacementStepState::Complete;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert!(matches!(result, InsertStepResult::Inserted { .. }));

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step.replacement_state = RegionSnapshotReplacementStepState::Complete;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert!(matches!(result, InsertStepResult::Inserted { .. }));

        assert_eq!(
            2,
            datastore
                .region_snapshot_replacement_steps_requiring_garbage_collection(
                    &opctx,
                )
                .await
                .unwrap()
                .len(),
        );

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_conflict() {
        let logctx =
            dev::test_setup_log("region_snapshot_replacement_step_conflict");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Assert that a region snapshot replacement step cannot be created for
        // a volume that is the "old snapshot volume" for another snapshot
        // replacement step.

        let request_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();
        let old_snapshot_volume_id = Uuid::new_v4();

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, volume_id);
        step.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step.old_snapshot_volume_id = Some(old_snapshot_volume_id);

        let first_step_id = step.id;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert!(matches!(result, InsertStepResult::Inserted { .. }));

        let step = RegionSnapshotReplacementStep::new(
            request_id,
            old_snapshot_volume_id,
        );

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert_eq!(
            result,
            InsertStepResult::AlreadyHandled {
                existing_step_id: first_step_id
            }
        );

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_conflict_with_region_replacement()
    {
        let logctx = dev::test_setup_log(
            "region_snapshot_replacement_step_conflict_with_region_replacement",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Assert that a region snapshot replacement step cannot be performed on
        // a volume if region replacement is occurring for that volume.

        let volume_id = Uuid::new_v4();

        let request = RegionReplacement::new(Uuid::new_v4(), volume_id);

        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        let request =
            RegionSnapshotReplacementStep::new(Uuid::new_v4(), volume_id);

        datastore
            .insert_region_snapshot_replacement_step(&opctx, request)
            .await
            .unwrap_err();

        datastore.terminate().await;
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
