// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionReplacement`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Region;
use crate::db::model::RegionReplacement;
use crate::db::model::RegionReplacementState;
use crate::db::model::RegionReplacementStep;
use crate::db::model::UpstairsRepairNotification;
use crate::db::model::UpstairsRepairNotificationType;
use crate::db::model::VolumeRepair;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use uuid::Uuid;

impl DataStore {
    /// Create and insert a region replacement request for a Region, returning the ID of the
    /// request.
    pub async fn create_region_replacement_request_for_region(
        &self,
        opctx: &OpContext,
        region: &Region,
    ) -> Result<Uuid, Error> {
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
        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::region_replacement::dsl;
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::insert_into(volume_repair_dsl::volume_repair)
                    .values(VolumeRepair {
                        volume_id: request.volume_id,
                        repair_id: request.id,
                    })
                    .execute_async(&conn)
                    .await?;

                diesel::insert_into(dsl::region_replacement)
                    .values(request)
                    .execute_async(&conn)
                    .await?;

                Ok(())
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn get_region_replacement_request(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<RegionReplacement, Error> {
        use db::schema::region_replacement::dsl;

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
        use db::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Requested),
            )
            .get_results_async::<RegionReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return region replacement requests that are in state `Running` with no
    /// currently operating saga. These need to be checked on or driven forward.
    pub async fn get_running_region_replacements(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<RegionReplacement>, Error> {
        use db::schema::region_replacement::dsl;

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
        use db::schema::region_replacement::dsl;

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
        use db::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Requested),
            )
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
        use db::schema::region_replacement::dsl;
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
    ) -> Result<(), Error> {
        use db::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(
                dsl::replacement_state.eq(RegionReplacementState::Allocating),
            )
            .set((
                dsl::replacement_state.eq(RegionReplacementState::Running),
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
        use db::schema::region_replacement::dsl;

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
        use db::schema::region_replacement::dsl;

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
        use db::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(dsl::replacement_state.eq(RegionReplacementState::Running))
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
        use db::schema::region_replacement::dsl;
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

    /// Transition a RegionReplacement record from Running to ReplacementDone,
    /// clearing the operating saga id.
    pub async fn set_region_replacement_from_driving_to_done(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_replacement::dsl;
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
        use db::schema::region_replacement_step::dsl;

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

    /// Record a step taken to drive a region replacement forward
    pub async fn add_region_replacement_request_step(
        &self,
        opctx: &OpContext,
        step: RegionReplacementStep,
    ) -> Result<(), Error> {
        use db::schema::region_replacement_step::dsl;

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
        use db::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionReplacementState::ReplacementDone),
            )
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
        use db::schema::region_replacement::dsl;
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
    /// that is taking a "lock" on the Volume.
    pub async fn set_region_replacement_complete(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                use db::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair
                        .filter(volume_repair_dsl::repair_id.eq(region_replacement_id))
                    )
                    .execute_async(&conn)
                    .await?;

                use db::schema::region_replacement::dsl;

                let result = diesel::update(dsl::region_replacement)
                    .filter(dsl::id.eq(region_replacement_id))
                    .filter(
                        dsl::replacement_state.eq(RegionReplacementState::Completing),
                    )
                    .filter(dsl::operating_saga_id.eq(operating_saga_id))
                    .set((
                        dsl::replacement_state.eq(RegionReplacementState::Complete),
                        dsl::operating_saga_id.eq(Option::<Uuid>::None),
                    ))
                    .check_if_exists::<RegionReplacement>(region_replacement_id)
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
                            Err(TxnError::CustomError(Error::conflict(format!(
                                "region replacement {} set to {:?} (operating saga id {:?})",
                                region_replacement_id,
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

    /// Nexus has been notified by an Upstairs (or has otherwised determined)
    /// that a region replacement is done, so update the record. This may arrive
    /// in the middle of a drive saga invocation, so do not filter on state or
    /// operating saga id!
    pub async fn mark_region_replacement_as_done(
        &self,
        opctx: &OpContext,
        region_replacement_id: Uuid,
    ) -> Result<(), Error> {
        use db::schema::region_replacement::dsl;
        let updated = diesel::update(dsl::region_replacement)
            .filter(dsl::id.eq(region_replacement_id))
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

        use db::schema::upstairs_repair_notification::dsl;

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

    use crate::db::datastore::test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_one_replacement_per_volume() {
        let logctx = dev::test_setup_log("test_one_replacement_per_volume");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let region_1_id = Uuid::new_v4();
        let region_2_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();

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

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_replacement_done_in_middle_of_drive_saga() {
        // If Nexus receives a notification that a repair has finished in the
        // middle of a drive saga, then make sure the replacement request state
        // ends up as `ReplacementDone`.

        let logctx = dev::test_setup_log(
            "test_replacement_done_in_middle_of_drive_saga",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let region_id = Uuid::new_v4();
        let volume_id = Uuid::new_v4();

        let request = {
            let mut request = RegionReplacement::new(region_id, volume_id);
            request.replacement_state = RegionReplacementState::Running;
            request
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Transition to Driving

        let saga_id = Uuid::new_v4();

        datastore
            .set_region_replacement_driving(&opctx, request.id, saga_id)
            .await
            .unwrap();

        // Now, Nexus receives a notification that the repair has finished
        // successfully

        datastore
            .mark_region_replacement_as_done(&opctx, request.id)
            .await
            .unwrap();

        // Ensure that the state is ReplacementDone, and the operating saga id
        // is cleared.

        let actual_request = datastore
            .get_region_replacement_request(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            actual_request.replacement_state,
            RegionReplacementState::ReplacementDone
        );
        assert_eq!(actual_request.operating_saga_id, None);

        // The Drive saga will unwind when it tries to set the state back to
        // Running.

        datastore
            .undo_set_region_replacement_driving(&opctx, request.id, saga_id)
            .await
            .unwrap_err();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
