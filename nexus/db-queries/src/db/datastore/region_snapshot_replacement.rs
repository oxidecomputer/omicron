// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`RegionSnapshotReplacement`] and
//! [`RegionSnapshotReplacementStep`] objects.

use super::DataStore;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::ReadOnlyTargetReplacement;
use crate::db::model::ReadOnlyTargetReplacementType;
use crate::db::model::RegionSnapshot;
use crate::db::model::RegionSnapshotReplacement;
use crate::db::model::RegionSnapshotReplacementState;
use crate::db::model::RegionSnapshotReplacementStep;
use crate::db::model::RegionSnapshotReplacementStepState;
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
use omicron_uuid_kinds::VolumeUuid;
use std::net::SocketAddrV6;
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

pub struct NewRegionVolumeId(pub VolumeUuid);
pub struct OldSnapshotVolumeId(pub VolumeUuid);

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
        let ReadOnlyTargetReplacement::RegionSnapshot { snapshot_id, .. } =
            request.replacement_type()
        else {
            return Err(Error::internal_error(
                "wrong read-only target replacement type",
            ));
        };

        // Note: if `LookupPath` is used here, it will not be able to retrieve
        // deleted snapshots
        let db_snapshot = match self.snapshot_get(opctx, snapshot_id).await? {
            Some(db_snapshot) => db_snapshot,
            None => {
                return Err(Error::internal_error(
                    "cannot perform region snapshot replacement without snapshot volume",
                ));
            }
        };

        self.insert_region_snapshot_replacement_request_with_volume_id(
            opctx,
            request,
            db_snapshot.volume_id(),
        )
        .await
    }

    /// Insert a region snapshot replacement request into the DB, also creating
    /// the VolumeRepair record.
    pub async fn insert_region_snapshot_replacement_request_with_volume_id(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
        volume_id: VolumeUuid,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "insert_region_snapshot_replacement_request_with_volume_id",
        )
        .transaction(&conn, |conn| {
            let request = request.clone();
            let err = err.clone();
            async move {
                use nexus_db_schema::schema::region_snapshot_replacement::dsl;

                // An associated volume repair record isn't _strictly_
                // needed: snapshot volumes should never be directly
                // constructed, and therefore won't ever have an associated
                // Upstairs that receives a volume replacement request.
                //
                // However, per-volume serialization is still required in order
                // to serialize allocating an additional region for a particular
                // volume id.

                Self::volume_repair_insert_in_txn(
                    &conn, err, volume_id, request.id,
                )
                .await?;

                diesel::insert_into(dsl::region_snapshot_replacement)
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

    pub async fn get_region_snapshot_replacement_request_by_id(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<RegionSnapshotReplacement, Error> {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(dsl::old_dataset_id.eq(region_snapshot.dataset_id))
            .filter(dsl::old_region_id.eq(region_snapshot.region_id))
            .filter(dsl::old_snapshot_id.eq(region_snapshot.snapshot_id))
            .filter(
                dsl::replacement_type
                    .eq(ReadOnlyTargetReplacementType::RegionSnapshot),
            )
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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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
        new_region_volume_id: NewRegionVolumeId,
        old_snapshot_volume_id: OldSnapshotVolumeId,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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
                dsl::old_snapshot_volume_id
                    .eq(Some(to_db_typed_uuid(old_snapshot_volume_id.0))),
                dsl::new_region_id.eq(Some(new_region_id)),
                dsl::new_region_volume_id
                    .eq(Some(to_db_typed_uuid(new_region_volume_id.0))),
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
                        && record.new_region_volume_id()
                            == Some(new_region_volume_id.0)
                        && record.old_snapshot_volume_id()
                            == Some(old_snapshot_volume_id.0)
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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
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

    /// Transition a RegionSnapshotReplacement record from Running to
    /// Completing, setting a unique id at the same time.
    pub async fn set_region_snapshot_replacement_completing(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Running),
            )
            .filter(dsl::operating_saga_id.is_null())
            .set((
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Completing),
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
                            == RegionSnapshotReplacementState::Completing
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

    /// Transition a RegionReplacement record from Completing to Running,
    /// clearing the operating saga id.
    pub async fn undo_set_region_snapshot_replacement_completing(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;
        let updated = diesel::update(dsl::region_snapshot_replacement)
            .filter(dsl::id.eq(region_snapshot_replacement_id))
            .filter(
                dsl::replacement_state
                    .eq(RegionSnapshotReplacementState::Completing),
            )
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
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

    /// Transition a RegionSnapshotReplacement record from Completing to
    /// Complete. Also removes the `volume_repair` record that is taking a
    /// "lock" on the Volume.
    pub async fn set_region_snapshot_replacement_complete(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
        operating_saga_id: Uuid,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "set_region_snapshot_replacement_complete",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();
            async move {
                use nexus_db_schema::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair.filter(
                        volume_repair_dsl::repair_id
                            .eq(region_snapshot_replacement_id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use nexus_db_schema::schema::region_snapshot_replacement::dsl;

                let result = diesel::update(dsl::region_snapshot_replacement)
                    .filter(dsl::id.eq(region_snapshot_replacement_id))
                    .filter(
                        dsl::replacement_state
                            .eq(RegionSnapshotReplacementState::Completing),
                    )
                    .filter(dsl::operating_saga_id.eq(operating_saga_id))
                    .set((
                        dsl::replacement_state
                            .eq(RegionSnapshotReplacementState::Complete),
                        dsl::operating_saga_id.eq(Option::<Uuid>::None),
                    ))
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
                            Err(err.bail(Error::conflict(format!(
                                "region snapshot replacement {} set to {:?} \
                                    (operating saga id {:?})",
                                region_snapshot_replacement_id,
                                record.replacement_state,
                                record.operating_saga_id,
                            ))))
                        }
                    }
                }
            }
        })
        .await
        .map_err(|e| match err.take() {
            Some(error) => error,
            None => public_error_from_diesel(e, ErrorHandler::Server),
        })
    }

    /// Transition a RegionSnapshotReplacement record from Requested to Complete
    /// - this is required when the region snapshot is hard-deleted, which means
    /// that all volume references are gone and no replacement is required. Also
    /// removes the `volume_repair` record that is taking a "lock" on the
    /// Volume.
    pub async fn set_region_snapshot_replacement_complete_from_requested(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("set_region_snapshot_replacement_complete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use nexus_db_schema::schema::volume_repair::dsl as volume_repair_dsl;
                    use nexus_db_schema::schema::region_snapshot_replacement::dsl;

                    diesel::delete(
                        volume_repair_dsl::volume_repair.filter(
                            volume_repair_dsl::repair_id
                                .eq(region_snapshot_replacement_id),
                        ),
                    )
                    .execute_async(&conn)
                    .await?;

                    let result = diesel::update(dsl::region_snapshot_replacement)
                        .filter(dsl::id.eq(region_snapshot_replacement_id))
                        .filter(
                            dsl::replacement_state
                                .eq(RegionSnapshotReplacementState::Requested),
                        )
                        .filter(dsl::operating_saga_id.is_null())
                        .filter(dsl::new_region_volume_id.is_null())
                        .set(dsl::replacement_state
                            .eq(RegionSnapshotReplacementState::Complete))
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
                                Err(err.bail(TxnError::from(Error::conflict(
                                    format!(
                                    "region snapshot replacement {} set to {:?} \
                                    (operating saga id {:?})",
                                    region_snapshot_replacement_id,
                                    record.replacement_state,
                                    record.operating_saga_id,
                                )
                                ))))
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

    pub async fn create_region_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request_id: Uuid,
        volume_id: VolumeUuid,
    ) -> Result<InsertStepResult, Error> {
        let request = RegionSnapshotReplacementStep::new(request_id, volume_id);

        self.insert_region_snapshot_replacement_step(opctx, request).await
    }

    pub async fn insert_region_snapshot_replacement_step(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacementStep,
    ) -> Result<InsertStepResult, Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "insert_region_snapshot_replacement_step",
        )
        .transaction(&conn, |conn| {
            let request = request.clone();
            let err = err.clone();

            async move {
                use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

                // Skip inserting this new record if we found another region
                // snapshot replacement step with this volume in the step's
                // `old_snapshot_volume_id`, as that means we're duplicating
                // the replacement work: that volume will be garbage
                // collected later. There's a unique index that will prevent
                // the same step being inserted with the same volume id.

                let maybe_record = dsl::region_snapshot_replacement_step
                    .filter(
                        dsl::old_snapshot_volume_id
                            .eq(to_db_typed_uuid(request.volume_id())),
                    )
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
                    .filter(
                        dsl::volume_id
                            .eq(to_db_typed_uuid(request.volume_id())),
                    )
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

                Self::volume_repair_insert_in_txn(
                    &conn,
                    err,
                    request.volume_id(),
                    request.id,
                )
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
        .map_err(|e| {
            if let Some(err) = err.take() {
                err
            } else {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })
    }

    pub async fn get_region_snapshot_replacement_step_by_id(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step_id: Uuid,
    ) -> Result<RegionSnapshotReplacementStep, Error> {
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

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
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;
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
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;
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
        old_snapshot_volume_id: VolumeUuid,
    ) -> Result<(), Error> {
        type TxnError = TransactionError<Error>;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "set_region_snapshot_replacement_step_complete",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();
            async move {
                use nexus_db_schema::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair.filter(
                        volume_repair_dsl::repair_id
                            .eq(region_snapshot_replacement_step_id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;
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
                                .eq(to_db_typed_uuid(old_snapshot_volume_id)),
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
                            Err(err.bail(TxnError::from(Error::conflict(
                                format!(
                                    "region snapshot replacement step {} set \
                                        to {:?} (operating saga id {:?})",
                                    region_snapshot_replacement_step_id,
                                    record.replacement_state,
                                    record.operating_saga_id,
                                ),
                            ))))
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

    /// Count all in-progress region snapshot replacement steps for a particular
    /// region snapshot replacement id.
    pub async fn in_progress_region_snapshot_replacement_steps(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_id: Uuid,
    ) -> Result<i64, Error> {
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

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
        use nexus_db_schema::schema::region_snapshot_replacement_step;

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
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

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

    /// Transition from Requested to VolumeDeleted, and remove the associated
    /// `volume_repair` record. This occurs when the associated snapshot's
    /// volume is deleted. Note this doesn't occur from a saga context, and
    /// therefore 1) doesn't accept an operating saga id parameter, and 2)
    /// checks that operating_saga_id is null for the corresponding record.
    pub async fn set_region_snapshot_replacement_step_volume_deleted_from_requested(
        &self,
        opctx: &OpContext,
        region_snapshot_replacement_step: RegionSnapshotReplacementStep,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "set_region_snapshot_replacement_complete",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();

            async move {
                use nexus_db_schema::schema::volume_repair::dsl as volume_repair_dsl;

                diesel::delete(
                    volume_repair_dsl::volume_repair.filter(
                        volume_repair_dsl::repair_id
                            .eq(region_snapshot_replacement_step.id),
                    ),
                )
                .execute_async(&conn)
                .await?;

                use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;
                let result =
                    diesel::update(dsl::region_snapshot_replacement_step)
                        .filter(dsl::id.eq(region_snapshot_replacement_step.id))
                        .filter(dsl::operating_saga_id.is_null())
                        .filter(dsl::old_snapshot_volume_id.is_null())
                        .filter(
                            dsl::replacement_state.eq(
                                RegionSnapshotReplacementStepState::Requested,
                            ),
                        )
                        .set(dsl::replacement_state.eq(
                            RegionSnapshotReplacementStepState::VolumeDeleted,
                        ))
                        .check_if_exists::<RegionSnapshotReplacementStep>(
                            region_snapshot_replacement_step.id,
                        )
                        .execute_and_check(&conn)
                        .await?;

                match result.status {
                    UpdateStatus::Updated => Ok(()),

                    UpdateStatus::NotUpdatedButExists => {
                        let record = result.found;

                        if record.replacement_state
                            == RegionSnapshotReplacementStepState::VolumeDeleted
                        {
                            Ok(())
                        } else {
                            Err(err.bail(Error::conflict(format!(
                                "region snapshot replacement step {} set \
                                    to {:?} (operating saga id {:?})",
                                region_snapshot_replacement_step.id,
                                record.replacement_state,
                                record.operating_saga_id,
                            ))))
                        }
                    }
                }
            }
        })
        .await
        .map_err(|e| match err.take() {
            Some(error) => error,
            None => public_error_from_diesel(e, ErrorHandler::Server),
        })
    }

    pub async fn read_only_target_deleted(
        &self,
        request: &RegionSnapshotReplacement,
    ) -> Result<bool, Error> {
        let deleted = match request.replacement_type() {
            ReadOnlyTargetReplacement::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => self
                .region_snapshot_get(dataset_id.into(), region_id, snapshot_id)
                .await?
                .is_none(),

            ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
                self.get_region_optional(region_id).await?.is_none()
            }
        };

        Ok(deleted)
    }

    /// Returns Ok(Some(_)) if the read-only target exists and the target
    /// address can be determined, Ok(None) if the read-only target does not
    /// exist, or Err otherwise.
    pub async fn read_only_target_addr(
        &self,
        request: &RegionSnapshotReplacement,
    ) -> Result<Option<SocketAddrV6>, Error> {
        match request.replacement_type() {
            ReadOnlyTargetReplacement::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => {
                let region_snapshot = match self
                    .region_snapshot_get(
                        dataset_id.into(),
                        region_id,
                        snapshot_id,
                    )
                    .await?
                {
                    Some(region_snapshot) => region_snapshot,
                    None => return Ok(None),
                };

                match region_snapshot.snapshot_addr.parse() {
                    Ok(addr) => Ok(Some(addr)),
                    Err(e) => {
                        Err(Error::internal_error(&format!("parse error: {e}")))
                    }
                }
            }

            ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
                self.region_addr(region_id).await
            }
        }
    }

    /// Create and insert a read-only region replacement request, returning the
    /// ID of the request.
    pub async fn create_read_only_region_replacement_request(
        &self,
        opctx: &OpContext,
        region_id: Uuid,
    ) -> Result<Uuid, Error> {
        let request =
            RegionSnapshotReplacement::new_from_read_only_region(region_id);
        let request_id = request.id;

        self.insert_read_only_region_replacement_request(opctx, request)
            .await?;

        Ok(request_id)
    }

    /// Insert a read-only region replacement request into the DB, also creating
    /// the VolumeRepair record.
    pub async fn insert_read_only_region_replacement_request(
        &self,
        opctx: &OpContext,
        request: RegionSnapshotReplacement,
    ) -> Result<(), Error> {
        let ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } =
            request.replacement_type()
        else {
            return Err(Error::internal_error(
                "wrong read-only target replacement type",
            ));
        };

        let db_region = match self.get_region_optional(region_id).await? {
            Some(db_region) => db_region,
            None => {
                return Err(Error::internal_error(
                    "cannot perform read-only region replacement without \
                    getting volume id",
                ));
            }
        };

        if !db_region.read_only() {
            return Err(Error::internal_error(
                "read-only region replacement requires read-only region",
            ));
        }

        let maybe_snapshot = self
            .find_snapshot_by_volume_id(&opctx, db_region.volume_id())
            .await?;

        if maybe_snapshot.is_none() {
            return Err(Error::internal_error(
                "read-only region replacement requires snapshot volume",
            ));
        }

        self.insert_region_snapshot_replacement_request_with_volume_id(
            opctx,
            request,
            db_region.volume_id(),
        )
        .await
    }

    /// Find a read-only region replacement request
    pub async fn lookup_read_only_region_replacement_request(
        &self,
        opctx: &OpContext,
        region_id: Uuid,
    ) -> Result<Option<RegionSnapshotReplacement>, Error> {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(dsl::old_region_id.eq(region_id))
            .filter(
                dsl::replacement_type
                    .eq(ReadOnlyTargetReplacementType::ReadOnlyRegion),
            )
            .get_result_async::<RegionSnapshotReplacement>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::model::RegionReplacement;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::VolumeConstructionRequest;

    #[tokio::test]
    async fn test_one_replacement_per_volume() {
        let logctx = dev::test_setup_log("test_one_replacement_per_volume");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let dataset_1_id = DatasetUuid::new_v4();
        let region_1_id = Uuid::new_v4();
        let snapshot_1_id = Uuid::new_v4();

        let dataset_2_id = DatasetUuid::new_v4();
        let region_2_id = Uuid::new_v4();
        let snapshot_2_id = Uuid::new_v4();

        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let request_1 = RegionSnapshotReplacement::new_from_region_snapshot(
            dataset_1_id,
            region_1_id,
            snapshot_1_id,
        );

        let request_2 = RegionSnapshotReplacement::new_from_region_snapshot(
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_one_replacement_per_volume_conflict_with_region() {
        let logctx = dev::test_setup_log(
            "test_one_replacement_per_volume_conflict_with_region",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let dataset_1_id = DatasetUuid::new_v4();
        let region_1_id = Uuid::new_v4();
        let snapshot_1_id = Uuid::new_v4();

        let region_2_id = Uuid::new_v4();

        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let request_1 = RegionSnapshotReplacement::new_from_region_snapshot(
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn count_replacement_steps() {
        let logctx = dev::test_setup_log("count_replacement_steps");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let dataset_id = DatasetUuid::new_v4();
        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let volume_id = VolumeUuid::new_v4();

        let request = RegionSnapshotReplacement::new_from_region_snapshot(
            dataset_id,
            region_id,
            snapshot_id,
        );

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

        assert!(
            datastore
                .get_requested_region_snapshot_replacement_steps(&opctx)
                .await
                .unwrap()
                .is_empty()
        );

        // Insert some replacement steps, and make sure counting works

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        {
            let step =
                RegionSnapshotReplacementStep::new(request_id, step_volume_id);

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

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        {
            let mut step =
                RegionSnapshotReplacementStep::new(request_id, step_volume_id);

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

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        {
            let mut step =
                RegionSnapshotReplacementStep::new(request_id, step_volume_id);

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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn unique_region_snapshot_replacement_step_per_volume() {
        let logctx = dev::test_setup_log(
            "unique_region_snapshot_replacement_step_per_volume",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Ensure that only one non-complete replacement step can be inserted
        // per volume.

        let volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

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
                VolumeUuid::new_v4(), // old_snapshot_volume_id
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_gc() {
        let logctx = dev::test_setup_log("region_snapshot_replacement_step_gc");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let volume_id = VolumeUuid::new_v4();

        let mut request = RegionSnapshotReplacement::new_from_region_snapshot(
            DatasetUuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        request.replacement_state = RegionSnapshotReplacementState::Complete;

        let request_id = request.id;

        datastore
            .insert_region_snapshot_replacement_request_with_volume_id(
                &opctx, request, volume_id,
            )
            .await
            .unwrap();

        assert!(
            datastore
                .region_snapshot_replacement_steps_requiring_garbage_collection(
                    &opctx
                )
                .await
                .unwrap()
                .is_empty()
        );

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, step_volume_id);
        step.replacement_state = RegionSnapshotReplacementStepState::Complete;

        let result = datastore
            .insert_region_snapshot_replacement_step(&opctx, step)
            .await
            .unwrap();

        assert!(matches!(result, InsertStepResult::Inserted { .. }));

        let step_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                step_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, step_volume_id);
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_conflict() {
        let logctx =
            dev::test_setup_log("region_snapshot_replacement_step_conflict");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Assert that a region snapshot replacement step cannot be created for
        // a volume that is the "old snapshot volume" for another snapshot
        // replacement step.

        let request_id = Uuid::new_v4();
        let volume_id = VolumeUuid::new_v4();
        let old_snapshot_volume_id = VolumeUuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        datastore
            .volume_create(
                old_snapshot_volume_id,
                VolumeConstructionRequest::Volume {
                    id: Uuid::new_v4(), // not required to match!
                    block_size: 512,
                    sub_volumes: vec![], // nothing needed here
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        let mut step =
            RegionSnapshotReplacementStep::new(request_id, volume_id);
        step.replacement_state = RegionSnapshotReplacementStepState::Complete;
        step.old_snapshot_volume_id = Some(old_snapshot_volume_id.into());

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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn region_snapshot_replacement_step_conflict_with_region_replacement()
    {
        let logctx = dev::test_setup_log(
            "region_snapshot_replacement_step_conflict_with_region_replacement",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Assert that a region snapshot replacement step cannot be performed on
        // a volume if region replacement is occurring for that volume.

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

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
