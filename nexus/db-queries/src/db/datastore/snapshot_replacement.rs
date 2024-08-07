// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`SnapshotReplacement`] and
//! [`SnapshotReplacementStep`] objects.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::lookup::LookupPath;
use crate::db::model::RegionSnapshot;
use crate::db::model::SnapshotReplacement;
use crate::db::model::SnapshotReplacementState;
use crate::db::model::SnapshotReplacementStep;
use crate::db::model::SnapshotReplacementStepState;
use crate::db::model::VolumeRepair;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use crate::db::TransactionError;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use uuid::Uuid;

impl DataStore {
    /// Create and insert a snapshot replacement request for a RegionSnapshot
    /// and Snapshot, returning the ID of the request.
    pub async fn create_snapshot_replacement_request_for_region_snapshot(
        &self,
        opctx: &OpContext,
        region_snapshot: &RegionSnapshot,
    ) -> Result<Uuid, Error> {
        let request = SnapshotReplacement::for_region_snapshot(region_snapshot);
        let request_id = request.id;

        self.insert_snapshot_replacement_request(opctx, request).await?;

        Ok(request_id)
    }

    /// Insert a snapshot replacement request into the DB, also creating the
    /// VolumeRepair record.
    pub async fn insert_snapshot_replacement_request(
        &self,
        opctx: &OpContext,
        request: SnapshotReplacement,
    ) -> Result<(), Error> {
        let (.., db_snapshot) = LookupPath::new(opctx, &self)
            .snapshot_id(request.old_snapshot_id)
            .fetch()
            .await?;

        self.insert_snapshot_replacement_request_with_volume_id(
            opctx,
            request,
            db_snapshot.volume_id,
        )
        .await
    }

    /// Insert a snapshot replacement request into the DB, also creating the
    /// VolumeRepair record.
    pub async fn insert_snapshot_replacement_request_with_volume_id(
        &self,
        opctx: &OpContext,
        request: SnapshotReplacement,
        volume_id: Uuid,
    ) -> Result<(), Error> {
        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::snapshot_replacement::dsl;
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

                diesel::insert_into(dsl::snapshot_replacement)
                    .values(request)
                    .execute_async(&conn)
                    .await?;

                Ok(())
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_snapshot_replacement_request_by_id(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<SnapshotReplacement, Error> {
        use db::schema::snapshot_replacement::dsl;

        dsl::snapshot_replacement
            .filter(dsl::id.eq(id))
            .get_result_async::<SnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Find a snapshot replacement request by region snapshot
    pub async fn lookup_snapshot_replacement_request(
        &self,
        opctx: &OpContext,
        region_snapshot: &RegionSnapshot,
    ) -> Result<Option<SnapshotReplacement>, Error> {
        use db::schema::snapshot_replacement::dsl;

        dsl::snapshot_replacement
            .filter(dsl::old_region_id.eq(region_snapshot.region_id))
            .filter(dsl::old_snapshot_id.eq(region_snapshot.snapshot_id))
            .get_result_async::<SnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_requested_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<SnapshotReplacement>, Error> {
        use db::schema::snapshot_replacement::dsl;

        dsl::snapshot_replacement
            .filter(
                dsl::replacement_state.eq(SnapshotReplacementState::Requested),
            )
            .get_results_async::<SnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return snapshot replacement requests that are in state `Running` with no
    /// currently operating saga.
    pub async fn get_running_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<SnapshotReplacement>, Error> {
        use db::schema::snapshot_replacement::dsl;

        dsl::snapshot_replacement
            .filter(
                dsl::replacement_state.eq(SnapshotReplacementState::Running),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<SnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return snapshot replacement requests that are in state `ReplacementDone`
    /// with no currently operating saga.
    pub async fn get_replacement_done_snapshot_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<SnapshotReplacement>, Error> {
        use db::schema::snapshot_replacement::dsl;

        dsl::snapshot_replacement
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementState::ReplacementDone),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<SnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Transition a SnapshotReplacement record from Requested to Allocating,
    /// setting a unique id at the same time.
    pub async fn set_snapshot_replacement_allocating(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(
                dsl::replacement_state.eq(SnapshotReplacementState::Requested),
            )
            .set((
                dsl::replacement_state.eq(SnapshotReplacementState::Allocating),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == SnapshotReplacementState::Allocating
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a SnapshotReplacement record from Allocating to Requested,
    /// clearing the operating saga id.
    pub async fn undo_set_snapshot_replacement_allocating(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(
                dsl::replacement_state.eq(SnapshotReplacementState::Allocating),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state.eq(SnapshotReplacementState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == SnapshotReplacementState::Requested
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_id,
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
    pub async fn set_snapshot_replacement_replacement_done(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
        new_region_id: Uuid,
        old_snapshot_volume_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state.eq(SnapshotReplacementState::Allocating),
            )
            .set((
                dsl::replacement_state
                    .eq(SnapshotReplacementState::ReplacementDone),
                dsl::old_snapshot_volume_id.eq(Some(old_snapshot_volume_id)),
                dsl::new_region_id.eq(Some(new_region_id)),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == SnapshotReplacementState::ReplacementDone
                        && record.new_region_id == Some(new_region_id)
                        && record.old_snapshot_volume_id
                            == Some(old_snapshot_volume_id)
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} (operating \
                            saga id {:?})",
                            snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a SnapshotReplacement record from ReplacementDone to
    /// DeletingOldVolume, setting a unique id at the same time.
    pub async fn set_snapshot_replacement_deleting_old_volume(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementState::ReplacementDone),
            )
            .set((
                dsl::replacement_state
                    .eq(SnapshotReplacementState::DeletingOldVolume),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == SnapshotReplacementState::DeletingOldVolume
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a SnapshotReplacement record from DeletingOldVolume to
    /// ReplacementDone, clearing the operating saga id.
    pub async fn undo_set_snapshot_replacement_deleting_old_volume(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementState::DeletingOldVolume),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(SnapshotReplacementState::ReplacementDone),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == SnapshotReplacementState::ReplacementDone
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_id,
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
    pub async fn set_snapshot_replacement_running(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement::dsl;
        let updated = diesel::update(dsl::snapshot_replacement)
            .filter(dsl::id.eq(snapshot_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementState::DeletingOldVolume),
            )
            .set((
                dsl::replacement_state.eq(SnapshotReplacementState::Running),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == SnapshotReplacementState::Running
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement {} set to {:?} (operating \
                            saga id {:?})",
                            snapshot_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a SnapshotReplacement record from Running to Complete. Also
    /// removes the `volume_repair` record that is taking a "lock" on the
    /// Volume. Note this doesn't occur from a saga context, and therefore 1)
    /// doesn't accept an operating saga id parameter, and 2) checks that
    /// operating_saga_id is null for the corresponding record.
    pub async fn set_snapshot_replacement_complete(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair
                        .filter(volume_repair_dsl::repair_id.eq(snapshot_replacement_id))
                    )
                    .execute_async(&conn)
                    .await?;

                use db::schema::snapshot_replacement::dsl;

                let result = diesel::update(dsl::snapshot_replacement)
                    .filter(dsl::id.eq(snapshot_replacement_id))
                    .filter(
                        dsl::replacement_state.eq(SnapshotReplacementState::Running),
                    )
                    .filter(dsl::operating_saga_id.is_null())
                    .set((
                        dsl::replacement_state.eq(SnapshotReplacementState::Complete),
                    ))
                    .check_if_exists::<SnapshotReplacement>(snapshot_replacement_id)
                    .execute_and_check(&conn)
                    .await?;

                match result.status {
                    UpdateStatus::Updated => Ok(()),
                    UpdateStatus::NotUpdatedButExists => {
                        let record = result.found;

                        if record.replacement_state
                                == SnapshotReplacementState::Complete
                        {
                            Ok(())
                        } else {
                            Err(TxnError::CustomError(Error::conflict(format!(
                                "snapshot replacement {} set to {:?} (operating saga id {:?})",
                                snapshot_replacement_id,
                                record.replacement_state,
                                record.operating_saga_id,
                            ))))
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

    pub async fn create_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request_id: Uuid,
        volume_id: Uuid,
    ) -> Result<Uuid, Error> {
        let request = SnapshotReplacementStep::new(request_id, volume_id);
        let request_id = request.id;

        self.insert_snapshot_replacement_step(opctx, request).await?;

        Ok(request_id)
    }

    pub async fn insert_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request: SnapshotReplacementStep,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();
        self.transaction_retry_wrapper("insert_snapshot_replacement_step")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let request = request.clone();

                async move {
                    use db::schema::snapshot_replacement_step::dsl;
                    use db::schema::volume_repair::dsl as volume_repair_dsl;

                    // Skip inserting this new record if we found another
                    // snapshot replacement step with this volume in the step's
                    // `old_snapshot_volume_id`, as that means we're duplicating
                    // the replacement work: that volume will be garbage
                    // collected later. There's a unique index that will prevent
                    // the same step being inserted with the same volume id.

                    let maybe_record = dsl::snapshot_replacement_step
                        .filter(dsl::old_snapshot_volume_id.eq(request.volume_id))
                        .get_result_async::<SnapshotReplacementStep>(
                            &conn,
                        )
                        .await
                        .optional()?;

                    if let Some(found_record) = maybe_record {
                        return Err(err.bail(Error::conflict(format!(
                            "{:?} already referenced in old snapshot volume for \
                            request {:?}",
                            request.volume_id,
                            found_record.id,
                        ))));
                    }

                    // The snapshot replacement step saga could invoke a volume
                    // replacement: create an associated volume repair record.

                    diesel::insert_into(volume_repair_dsl::volume_repair)
                        .values(VolumeRepair { volume_id: request.volume_id, repair_id: request.id })
                        .execute_async(&conn)
                        .await?;

                    diesel::insert_into(dsl::snapshot_replacement_step)
                        .values(request)
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }

                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn get_snapshot_replacement_step_by_id(
        &self,
        opctx: &OpContext,
        snapshot_replacement_step_id: Uuid,
    ) -> Result<SnapshotReplacementStep, Error> {
        use db::schema::snapshot_replacement_step::dsl;

        dsl::snapshot_replacement_step
            .filter(dsl::id.eq(snapshot_replacement_step_id))
            .get_result_async::<SnapshotReplacementStep>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_requested_snapshot_replacement_steps(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<SnapshotReplacementStep>, Error> {
        opctx.check_complex_operations_allowed()?;

        let mut records = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use db::schema::snapshot_replacement_step::dsl;

            let batch = paginated(
                dsl::snapshot_replacement_step,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Requested),
            )
            .get_results_async::<SnapshotReplacementStep>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|r| r.id);
            records.extend(batch);
        }

        Ok(records)
    }

    pub async fn set_snapshot_replacement_step_running(
        &self,
        opctx: &OpContext,
        snapshot_replacement_step_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement_step::dsl;
        let updated = diesel::update(dsl::snapshot_replacement_step)
            .filter(dsl::id.eq(snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Requested),
            )
            .set((
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Running),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<SnapshotReplacementStep>(
                snapshot_replacement_step_id,
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
                            == SnapshotReplacementStepState::Running
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement step {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_step_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a SnapshotReplacementStep record from Running to Requested,
    /// clearing the operating saga id.
    pub async fn undo_set_snapshot_replacement_step_running(
        &self,
        opctx: &OpContext,
        snapshot_replacement_step_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement_step::dsl;
        let updated = diesel::update(dsl::snapshot_replacement_step)
            .filter(dsl::id.eq(snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Running),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<SnapshotReplacementStep>(
                snapshot_replacement_step_id,
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
                            == SnapshotReplacementStepState::Requested
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement step {} set to {:?} \
                            (operating saga id {:?})",
                            snapshot_replacement_step_id,
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
    pub async fn set_snapshot_replacement_step_complete(
        &self,
        opctx: &OpContext,
        snapshot_replacement_step_id: Uuid,
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
                            .eq(snapshot_replacement_step_id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use db::schema::snapshot_replacement_step::dsl;
                let result = diesel::update(dsl::snapshot_replacement_step)
                    .filter(dsl::id.eq(snapshot_replacement_step_id))
                    .filter(dsl::operating_saga_id.eq(operating_saga_id))
                    .filter(dsl::old_snapshot_volume_id.is_null())
                    .filter(
                        dsl::replacement_state
                            .eq(SnapshotReplacementStepState::Running),
                    )
                    .set((
                        dsl::replacement_state
                            .eq(SnapshotReplacementStepState::Complete),
                        dsl::operating_saga_id.eq(Option::<Uuid>::None),
                        dsl::old_snapshot_volume_id.eq(old_snapshot_volume_id),
                    ))
                    .check_if_exists::<SnapshotReplacementStep>(
                        snapshot_replacement_step_id,
                    )
                    .execute_and_check(
                        &*self.pool_connection_authorized(opctx).await?,
                    )
                    .await?;

                match result.status {
                    UpdateStatus::Updated => Ok(()),
                    UpdateStatus::NotUpdatedButExists => {
                        let record = result.found;

                        if record.operating_saga_id == None
                            && record.replacement_state
                                == SnapshotReplacementStepState::Complete
                        {
                            Ok(())
                        } else {
                            Err(TxnError::CustomError(Error::conflict(
                                format!(
                                    "snapshot replacement step {} set to {:?} \
                                (operating saga id {:?})",
                                    snapshot_replacement_step_id,
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

    /// Count all in-progress snapshot replacement steps for a particular
    /// snapshot replacement id.
    pub async fn in_progress_snapshot_replacement_steps(
        &self,
        opctx: &OpContext,
        snapshot_replacement_id: Uuid,
    ) -> Result<usize, Error> {
        opctx.check_complex_operations_allowed()?;

        let mut records: usize = 0;
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use db::schema::snapshot_replacement_step::dsl;

            let batch = paginated(
                dsl::snapshot_replacement_step,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(dsl::request_id.eq(snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .ne(SnapshotReplacementStepState::VolumeDeleted),
            )
            .get_results_async::<SnapshotReplacementStep>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|r| r.id);
            records += batch.len();
        }

        Ok(records)
    }

    /// Return all snapshot replacement steps that are Complete
    pub async fn snapshot_replacement_steps_requiring_garbage_collection(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<SnapshotReplacementStep>, Error> {
        use db::schema::snapshot_replacement_step;

        let conn = self.pool_connection_authorized(opctx).await?;

        snapshot_replacement_step::table
            .filter(
                snapshot_replacement_step::replacement_state
                    .eq(SnapshotReplacementStepState::Complete),
            )
            .select(SnapshotReplacementStep::as_select())
            .get_results_async::<SnapshotReplacementStep>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Set the snapshot replacement step's state to VolumeDeleted
    pub async fn set_snapshot_replacement_step_volume_deleted(
        &self,
        opctx: &OpContext,
        snapshot_replacement_step_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::snapshot_replacement_step::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let updated = diesel::update(dsl::snapshot_replacement_step)
            .filter(dsl::id.eq(snapshot_replacement_step_id))
            .filter(
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::Complete),
            )
            .set(
                dsl::replacement_state
                    .eq(SnapshotReplacementStepState::VolumeDeleted),
            )
            .check_if_exists::<SnapshotReplacementStep>(
                snapshot_replacement_step_id,
            )
            .execute_and_check(&conn)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.replacement_state
                        == SnapshotReplacementStepState::VolumeDeleted
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "snapshot replacement step {} set to {:?}",
                            snapshot_replacement_step_id,
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

        let request_1 =
            SnapshotReplacement::new(dataset_1_id, region_1_id, snapshot_1_id);

        let request_2 =
            SnapshotReplacement::new(dataset_2_id, region_2_id, snapshot_2_id);

        datastore
            .insert_snapshot_replacement_request_with_volume_id(
                &opctx, request_1, volume_id,
            )
            .await
            .unwrap();

        datastore
            .insert_snapshot_replacement_request_with_volume_id(
                &opctx, request_2, volume_id,
            )
            .await
            .unwrap_err();

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

        let request_1 =
            SnapshotReplacement::new(dataset_1_id, region_1_id, snapshot_1_id);

        let request_2 = RegionReplacement::new(region_2_id, volume_id);

        datastore
            .insert_snapshot_replacement_request_with_volume_id(
                &opctx, request_1, volume_id,
            )
            .await
            .unwrap();

        datastore
            .insert_region_replacement_request(&opctx, request_2)
            .await
            .unwrap_err();

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
            SnapshotReplacement::new(dataset_id, region_id, snapshot_id);

        let request_id = request.id;

        datastore
            .insert_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        // Make sure counts start at 0

        assert_eq!(
            datastore
                .in_progress_snapshot_replacement_steps(&opctx, request_id)
                .await
                .unwrap(),
            0,
        );

        assert!(datastore
            .get_requested_snapshot_replacement_steps(&opctx)
            .await
            .unwrap()
            .is_empty());

        // Insert some replacement steps, and make sure counting works

        {
            let step = SnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            datastore
                .insert_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();
        }

        assert_eq!(
            datastore
                .in_progress_snapshot_replacement_steps(&opctx, request_id,)
                .await
                .unwrap(),
            1,
        );

        assert_eq!(
            datastore
                .get_requested_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        {
            let mut step = SnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            step.replacement_state = SnapshotReplacementStepState::Running;

            datastore
                .insert_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();
        }

        assert_eq!(
            datastore
                .in_progress_snapshot_replacement_steps(&opctx, request_id)
                .await
                .unwrap(),
            2,
        );

        assert_eq!(
            datastore
                .get_requested_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        {
            let mut step = SnapshotReplacementStep::new(
                request_id,
                Uuid::new_v4(), // volume id
            );

            // VolumeDeleted does not count as "in-progress"
            step.replacement_state =
                SnapshotReplacementStepState::VolumeDeleted;

            datastore
                .insert_snapshot_replacement_step(&opctx, step)
                .await
                .unwrap();
        }

        assert_eq!(
            datastore
                .in_progress_snapshot_replacement_steps(&opctx, request_id)
                .await
                .unwrap(),
            2,
        );

        assert_eq!(
            datastore
                .get_requested_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .len(),
            1,
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn unique_snapshot_replacement_step_per_volume() {
        let logctx =
            dev::test_setup_log("unique_snapshot_replacement_step_per_volume");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Ensure that only one non-complete replacement step can be inserted
        // per volume.

        let volume_id = Uuid::new_v4();

        let step = SnapshotReplacementStep::new(Uuid::new_v4(), volume_id);
        let first_request_id = step.id;

        datastore.insert_snapshot_replacement_step(&opctx, step).await.unwrap();

        let step = SnapshotReplacementStep::new(Uuid::new_v4(), volume_id);

        datastore
            .insert_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap_err();

        // Ensure that transitioning the first step to running doesn't change
        // things.

        let saga_id = Uuid::new_v4();

        datastore
            .set_snapshot_replacement_step_running(
                &opctx,
                first_request_id,
                saga_id,
            )
            .await
            .unwrap();

        datastore
            .insert_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap_err();

        // Ensure that transitioning the first step to complete means another
        // can be added.

        datastore
            .set_snapshot_replacement_step_complete(
                &opctx,
                first_request_id,
                saga_id,
                Uuid::new_v4(), // old_snapshot_volume_id
            )
            .await
            .unwrap();

        datastore
            .insert_snapshot_replacement_step(&opctx, step.clone())
            .await
            .unwrap();

        // Ensure that transitioning the first step to volume deleted still
        // works.

        datastore
            .set_snapshot_replacement_step_volume_deleted(
                &opctx,
                first_request_id,
            )
            .await
            .unwrap();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn snapshot_replacement_step_gc() {
        let logctx = dev::test_setup_log("snapshot_replacement_step_gc");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut request = SnapshotReplacement::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state = SnapshotReplacementState::Complete;

        let request_id = request.id;

        datastore
            .insert_snapshot_replacement_request_with_volume_id(
                &opctx,
                request,
                Uuid::new_v4(),
            )
            .await
            .unwrap();

        assert!(datastore
            .snapshot_replacement_steps_requiring_garbage_collection(&opctx,)
            .await
            .unwrap()
            .is_empty());

        let mut step = SnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step.replacement_state = SnapshotReplacementStepState::Complete;
        datastore.insert_snapshot_replacement_step(&opctx, step).await.unwrap();

        let mut step = SnapshotReplacementStep::new(request_id, Uuid::new_v4());
        step.replacement_state = SnapshotReplacementStepState::Complete;
        datastore.insert_snapshot_replacement_step(&opctx, step).await.unwrap();

        assert_eq!(
            2,

            datastore.snapshot_replacement_steps_requiring_garbage_collection(
                &opctx,
            )
            .await
            .unwrap()
            .len(),
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn snapshot_replacement_step_conflict() {
        let logctx = dev::test_setup_log("snapshot_replacement_step_conflict");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Assert that a snapshot replacement step cannot be created for a
        // volume that is the "old snapshot volume" for another snapshot
        // replacement step.

        let request_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();
        let old_snapshot_volume_id = Uuid::new_v4();

        let mut step = SnapshotReplacementStep::new(request_id, volume_id);
        step.replacement_state = SnapshotReplacementStepState::Complete;
        step.old_snapshot_volume_id = Some(old_snapshot_volume_id);
        datastore.insert_snapshot_replacement_step(&opctx, step).await.unwrap();

        let step =
            SnapshotReplacementStep::new(request_id, old_snapshot_volume_id);
        datastore
            .insert_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap_err();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn snapshot_replacement_step_conflict_with_region_replacement() {
        let logctx = dev::test_setup_log(
            "snapshot_replacement_step_conflict_with_region_replacement",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Assert that a snapshot replacement step cannot be performed on a
        // volume if region replacement is occurring for that volume.

        let volume_id = Uuid::new_v4();

        let request = RegionReplacement::new(Uuid::new_v4(), volume_id);
        datastore
            .insert_region_replacement_request(&opctx, request)
            .await
            .unwrap();

        let request = SnapshotReplacementStep::new(Uuid::new_v4(), volume_id);
        datastore
            .insert_snapshot_replacement_step(&opctx, request)
            .await
            .unwrap_err();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
