// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionReplacement`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::Region;
use crate::db::model::RegionReplacement;
use crate::db::model::RegionReplacementState;
use crate::db::model::RegionReplacementStep;
use crate::db::model::UpstairsRepairNotification;
use crate::db::model::UpstairsRepairNotificationType;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::VolumeUuid;
use uuid::Uuid;

impl DataStore {
    /// Create and insert a region replacement request for a Region, returning
    /// the ID of the request.
    pub async fn create_region_replacement_request_for_region(
        &self,
        opctx: &OpContext,
        region: &Region,
    ) -> Result<Uuid, Error> {
        if region.read_only() {
            // You want `create_read_only_region_replacement_request`! :)
            return Err(Error::invalid_request(format!(
                "region {} is read-only",
                region.id(),
            )));
        }

        let request = RegionReplacement::for_region(region);
        let request_id = request.id;

        self.insert_region_replacement_request(opctx, request).await?;

        Ok(request_id)
    }

    /// Insert a region replacement request into the DB, also creating the
    /// VolumeRepair record.
    pub async fn insert_region_replacement_request(
        &self,
        opctx: &OpContext,
        request: RegionReplacement,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("insert_region_replacement_request")
            .transaction(&conn, |conn| {
                let request = request.clone();
                let err = err.clone();
                async move {
                    use nexus_db_schema::schema::region_replacement::dsl;

                    Self::volume_repair_insert_in_txn(
                        &conn,
                        err,
                        request.volume_id(),
                        request.id,
                    )
                    .await?;

                    diesel::insert_into(dsl::region_replacement)
                        .values(request)
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn get_region_replacement_request_by_id(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<RegionReplacement, Error> {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(dsl::id.eq(id))
            .get_result_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_requested_region_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionReplacement>, Error> {
        opctx.check_complex_operations_allowed()?;

        let mut replacements = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::region_replacement::dsl;

            let batch = paginated(
                dsl::region_replacement,
                dsl::id,
                &p.current_pagparams(),
            )
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Requested),
            )
            .get_results_async::<RegionReplacement>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|r| r.id);
            replacements.extend(batch);
        }

        Ok(replacements)
    }

    /// Return region replacement requests that are in state `Running` with no
    /// currently operating saga. These need to be checked on or driven forward.
    pub async fn get_running_region_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionReplacement>, Error> {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(dsl::replacement_state.eq(RegionReplacementState::Running))
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return region replacement requests that are in state `ReplacementDone`
    /// with no currently operating saga. These need to be completed.
    pub async fn get_done_region_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionReplacement>, Error> {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
            )
            .filter(dsl::operating_saga_id.is_null())
            .get_results_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Transition a RegionReplacement record from Requested to Allocating,
    /// setting a unique id at the same time.
    pub async fn set_region_replacement_allocating(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Requested),
            )
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Allocating),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionReplacementState::Allocating
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionReplacement record from Allocating to Requested,
    /// clearing the operating saga id.
    pub async fn undo_set_region_replacement_allocating(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Allocating),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::Requested
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition from Allocating to Running, and clear the operating saga id.
    pub async fn set_region_replacement_running(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
        new_region_id: Uuid,
        old_region_volume_id: VolumeUuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Allocating),
            )
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Running),
                dsl::old_region_volume_id
                    .eq(Some(to_db_typed_uuid(old_region_volume_id))),
                dsl::new_region_id.eq(Some(new_region_id)),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::Running
                        && record.new_region_id == Some(new_region_id)
                        && record.old_region_volume_id()
                            == Some(old_region_volume_id)
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Find an in-progress region replacement request by new region id
    pub async fn lookup_in_progress_region_replacement_request_by_new_region_id(
        &self,
        opctx: &OpContext,
        new_region_id: TypedUuid<DownstairsRegionKind>,
    ) -> Result<Option<RegionReplacement>, Error> {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(
                dsl::new_region_id
                    .eq(nexus_db_model::to_db_typed_uuid(new_region_id)),
            )
            .filter(dsl::replacement_state.ne(RegionReplacementState::Complete))
            .get_result_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Find a region replacement request by old region id
    pub async fn lookup_region_replacement_request_by_old_region_id(
        &self,
        opctx: &OpContext,
        old_region_id: TypedUuid<DownstairsRegionKind>,
    ) -> Result<Option<RegionReplacement>, Error> {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(
                dsl::old_region_id
                    .eq(nexus_db_model::to_db_typed_uuid(old_region_id)),
            )
            .get_result_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Transition a RegionReplacement record from Running to Driving,
    /// setting a unique id at the same time.
    pub async fn set_region_replacement_driving(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::replacement_state.eq(RegionReplacementState::Running))
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Driving),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionReplacementState::Driving
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionReplacement record from Driving to Running,
    /// clearing the operating saga id.
    pub async fn undo_set_region_replacement_driving(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::replacement_state.eq(RegionReplacementState::Driving))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Running),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::Running
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionReplacement record from Driving to ReplacementDone,
    /// clearing the operating saga id.
    pub async fn set_region_replacement_from_driving_to_done(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::replacement_state.eq(RegionReplacementState::Driving))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::ReplacementDone
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Return the most current step for a region replacement request
    pub async fn current_region_replacement_request_step(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<Option<RegionReplacementStep>, Error> {
        use nexus_db_schema::schema::region_replacement_step::dsl;

        dsl::region_replacement_step
            .filter(dsl::replacement_id.eq(id))
            .order_by(dsl::step_time.desc())
            .first_async::<RegionReplacementStep>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return all steps for a region replacement request
    pub async fn region_replacement_request_steps(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<Vec<RegionReplacementStep>, Error> {
        use nexus_db_schema::schema::region_replacement_step::dsl;

        dsl::region_replacement_step
            .filter(dsl::replacement_id.eq(id))
            .order_by(dsl::step_time.desc())
            .get_results_async::<RegionReplacementStep>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Record a step taken to drive a region replacement forward
    pub async fn add_region_replacement_request_step(
        &self,
        opctx: &OpContext,
        step: RegionReplacementStep,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement_step::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::insert_into(dsl::region_replacement_step)
            .values(step)
            .on_conflict((dsl::replacement_id, dsl::step_time, dsl::step_type))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Transition a RegionReplacement record from ReplacementDone to Completing,
    /// setting a unique id at the same time.
    pub async fn set_region_replacement_completing(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
            )
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Completing),
                dsl::operating_saga_id.eq(operating_saga_id),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == Some(operating_saga_id)
                        && record.replacement_state
                            == RegionReplacementState::Completing
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionReplacement record from Completing to ReplacementDone,
    /// clearing the operating saga id.
    pub async fn undo_set_region_replacement_completing(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Completing),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .set((
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),
                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::ReplacementDone
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Transition a RegionReplacement record from Completing to Complete,
    /// clearing the operating saga id. Also removes the `volume_repair` record
    /// that is taking a lock on the Volume.
    pub async fn set_region_replacement_complete(
        &self,
        opctx: &OpContext,
        request: RegionReplacement,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("set_region_replacement_complete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let request_volume_id = request.volume_id();
                async move {
                    Self::volume_repair_delete_query(
                        request_volume_id,
                        request.id,
                    )
                    .execute_async(&conn)
                    .await?;

                    use nexus_db_schema::schema::region_replacement::dsl;

                    let result = diesel::update(dsl::region_replacement)
                        .filter(dsl::id.eq(request.id))
                        .filter(
                            dsl::replacement_state.eq(RegionReplacementState::Completing),
                        )
                        .filter(dsl::operating_saga_id.eq(operating_saga_id))
                        .set((
                            dsl::replacement_state.eq(RegionReplacementState::Complete),
                            dsl::operating_saga_id.eq(Option::<Uuid>::None),
                        ))
                        .check_if_exists::<RegionReplacement>(request.id)
                        .execute_and_check(&conn)
                        .await?;

                    match result.status {
                        UpdateStatus::Updated => Ok(()),
                        UpdateStatus::NotUpdatedButExists => {
                            let record = result.found;

                            if record.operating_saga_id == None
                                && record.replacement_state
                                    == RegionReplacementState::Complete
                            {
                                Ok(())
                            } else {
                                Err(err.bail(TxnError::from(Error::conflict(format!(
                                    "region replacement {} set to {:?} (operating saga id {:?})",
                                    request.id,
                                    record.replacement_state,
                                    record.operating_saga_id,
                                )))))
                            }
                        }
                    }
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Transition a RegionReplacement record from Requested to Complete, which
    /// occurs when the associated volume is soft or hard deleted.  Also removes
    /// the `volume_repair` record that is taking a lock on the Volume.
    pub async fn set_region_replacement_complete_from_requested(
        &self,
        opctx: &OpContext,
        request: RegionReplacement,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        assert_eq!(
            request.replacement_state,
            RegionReplacementState::Requested,
        );

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("set_region_replacement_complete_from_requested")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let request_volume_id = request.volume_id();
                async move {
                    Self::volume_repair_delete_query(
                        request_volume_id,
                        request.id,
                    )
                    .execute_async(&conn)
                    .await?;

                    use nexus_db_schema::schema::region_replacement::dsl;

                    let result = diesel::update(dsl::region_replacement)
                        .filter(dsl::id.eq(request.id))
                        .filter(
                            dsl::replacement_state.eq(RegionReplacementState::Requested),
                        )
                        .filter(dsl::operating_saga_id.is_null())
                        .set((
                            dsl::replacement_state.eq(RegionReplacementState::Complete),
                        ))
                        .check_if_exists::<RegionReplacement>(request.id)
                        .execute_and_check(&conn)
                        .await?;

                    match result.status {
                        UpdateStatus::Updated => Ok(()),

                        UpdateStatus::NotUpdatedButExists => {
                            let record = result.found;

                            if record.replacement_state == RegionReplacementState::Complete {
                                Ok(())
                            } else {
                                Err(err.bail(TxnError::from(Error::conflict(format!(
                                    "region replacement {} set to {:?} (operating saga id {:?})",
                                    request.id,
                                    record.replacement_state,
                                    record.operating_saga_id,
                                )))))
                            }
                        }
                    }
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Nexus has been notified by an Upstairs (or has otherwised determined)
    /// that a region replacement is done, so update the record. Filter on the
    /// following:
    ///
    /// - operating saga id being None, as this happens outside of a saga and
    ///   should only transition the record if there isn't currently a lock.
    ///
    /// - the record being in the state "Running": this function is called when
    ///   a "finish" notification is seen, and that only happens after a region
    ///   replacement drive saga has invoked either a reconcilation or live
    ///   repair, and that has finished. The region replacement drive background
    ///   task will scan for these notifications and call this function if one
    ///   is seen.
    pub async fn mark_region_replacement_as_done(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::operating_saga_id.is_null())
            .filter(dsl::replacement_state.eq(RegionReplacementState::Running))
            .set(
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
            )
            .check_if_exists::<RegionReplacement>(region_replacement_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    if record.operating_saga_id == None
                        && record.replacement_state
                            == RegionReplacementState::ReplacementDone
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "region replacement {} set to {:?} (operating saga id {:?})",
                            region_replacement_id,
                            record.replacement_state,
                            record.operating_saga_id,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Check if a region replacement request has at least one matching
    /// successful "repair finished" notification.
    //
    // For the purposes of changing the state of a region replacement request to
    // `ReplacementDone`, check if Nexus has seen at least related one
    // successful "repair finished" notification.
    //
    // Note: after a region replacement request has transitioned to `Complete`,
    // there may be many future "repair finished" notifications for the "new"
    // region that are unrelated to the replacement request.
    pub async fn request_has_matching_successful_finish_notification(
        &self,
        opctx: &OpContext,
        region_replacement: &RegionReplacement,
    ) -> Result<bool, Error> {
        let Some(new_region_id) = region_replacement.new_region_id else {
            return Err(Error::invalid_request(format!(
                "region replacement {} has no new region id!",
                region_replacement.id,
            )));
        };

        use nexus_db_schema::schema::upstairs_repair_notification::dsl;

        let maybe_notification = dsl::upstairs_repair_notification
            .filter(dsl::region_id.eq(new_region_id))
            .filter(
                dsl::notification_type
                    .eq(UpstairsRepairNotificationType::Succeeded),
            )
            .first_async::<UpstairsRepairNotification>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(maybe_notification.is_some())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::VolumeConstructionRequest;

    #[tokio::test]
    async fn test_one_replacement_per_volume() {
        let logctx = dev::test_setup_log("test_one_replacement_per_volume");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let region_1_id = Uuid::new_v4();
        let region_2_id = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let request_1 = RegionReplacement::new(region_1_id, volume_id);
        let request_2 = RegionReplacement::new(region_2_id, volume_id);

        datastore
            .insert_region_replacement_request(&opctx, request_1)
            .await
            .unwrap();
        datastore
            .insert_region_replacement_request(&opctx, request_2)
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_replacement_done_in_middle_of_drive_saga() {
        // If Nexus receives a notification that a repair has finished in the
        // middle of a drive saga, then make sure the replacement request state
        // eventually ends up as `ReplacementDone`.

        let logctx = dev::test_setup_log(
            "test_replacement_done_in_middle_of_drive_saga",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let region_id = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let request = {
            let mut request = RegionReplacement::new(region_id, volume_id);
            request.replacement_state = RegionReplacementState::Running;
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // The drive saga will transition the record to Driving, locking it.

        let saga_id = Uuid::new_v4();

        datastore
            .set_region_replacement_driving(&opctx, request.id, saga_id)
            .await
            .unwrap();

        // Now, Nexus receives a notification that the repair has finished
        // successfully. A background task trying to mark as replacement done
        // should fail as the record was locked by the saga.

        datastore
            .mark_region_replacement_as_done(&opctx, request.id)
            .await
            .unwrap_err();

        // Ensure that the state is still Driving, and the operating saga id is
        // set.

        let actual_request = datastore
            .get_region_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            actual_request.replacement_state,
            RegionReplacementState::Driving
        );
        assert_eq!(actual_request.operating_saga_id, Some(saga_id));

        // The Drive saga will finish, but doesn't transition to replacement
        // done because it didn't detect that one of the repair operations had
        // finished ok.

        datastore
            .undo_set_region_replacement_driving(&opctx, request.id, saga_id)
            .await
            .unwrap();

        // Now the region replacement drive background task wakes up again, and
        // this time marks the record as replacement done successfully.

        datastore
            .mark_region_replacement_as_done(&opctx, request.id)
            .await
            .unwrap();

        // Ensure that the state is ReplacementDone, and the operating saga id
        // is cleared.

        let actual_request = datastore
            .get_region_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            actual_request.replacement_state,
            RegionReplacementState::ReplacementDone
        );
        assert_eq!(actual_request.operating_saga_id, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_replacement_done_in_middle_of_finish_saga() {
        // If multiple Nexus are racing, don't let one mark a record as
        // "ReplacementDone" if it's in the middle of the finish saga.

        let logctx = dev::test_setup_log(
            "test_replacement_done_in_middle_of_finish_saga",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let region_id = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let request = {
            let mut request = RegionReplacement::new(region_id, volume_id);
            request.replacement_state = RegionReplacementState::ReplacementDone;
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // The finish saga will transition to Completing, setting operating saga
        // id accordingly.

        let saga_id = Uuid::new_v4();

        datastore
            .set_region_replacement_completing(&opctx, request.id, saga_id)
            .await
            .unwrap();

        // Double check that another saga can't do this, because the first saga
        // took the lock.

        datastore
            .set_region_replacement_completing(
                &opctx,
                request.id,
                Uuid::new_v4(),
            )
            .await
            .unwrap_err();

        // mark_region_replacement_as_done is called due to a finish
        // notification scan by the region replacement drive background task.
        // This should fail as the saga took the lock on this record.

        datastore
            .mark_region_replacement_as_done(&opctx, request.id)
            .await
            .unwrap_err();

        // The first saga has finished and sets the record to Complete.

        datastore
            .set_region_replacement_complete(&opctx, request, saga_id)
            .await
            .unwrap();

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
