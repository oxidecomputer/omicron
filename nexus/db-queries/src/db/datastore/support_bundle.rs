// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`SupportBundle`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::RendezvousDebugDataset;
use crate::db::model::SupportBundle;
use crate::db::model::SupportBundleState;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::raw_query_builder::TypedSqlQuery;
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use futures::FutureExt;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use nexus_types::deployment::BlueprintDatasetDisposition;
use nexus_types::deployment::BlueprintExpungedZoneAccessReason;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use uuid::Uuid;

const CANNOT_ALLOCATE_ERR_MSG: &'static str = "Current policy limits support bundle creation to 'one per external disk', and \
 no disks are available. You must delete old support bundles before new ones \
 can be created";

const FAILURE_REASON_NO_DATASET: &'static str =
    "Allocated dataset no longer exists";
const FAILURE_REASON_NO_NEXUS: &'static str =
    "Nexus managing this bundle no longer exists";

/// Provides a report on how many bundle were expunged, and why.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct SupportBundleExpungementReport {
    /// Bundles marked "failed" because the datasets storing them have been
    /// expunged.
    pub bundles_failed_missing_datasets: usize,
    /// Bundles already in the "destroying" state that have been deleted because
    /// the datasets storing them have been expunged.
    pub bundles_deleted_missing_datasets: usize,

    /// Bundles marked "destroying" because the nexuses managing them have been
    /// expunged.
    ///
    /// These bundles should be re-assigned to a different nexus for cleanup.
    pub bundles_failing_missing_nexus: usize,

    /// Bundles which had a new Nexus assigned to them.
    pub bundles_reassigned: usize,
}

/// Result of atomically deleting support bundles for capacity management.
#[derive(Debug, Clone)]
pub struct AutoDeletionResult {
    /// IDs of bundles that were transitioned to Destroying state.
    pub deleted_ids: Vec<SupportBundleUuid>,
    /// Total number of debug datasets available.
    pub total_datasets: usize,
    /// Number of active bundles (before any deletions).
    pub active_bundles: usize,
    /// Number of free debug datasets (before any deletions).
    pub free_datasets: usize,
}

impl DataStore {
    /// Creates a new support bundle.
    ///
    /// Requires that the UUID of the calling Nexus be supplied as input -
    /// this particular Zone is responsible for the collection process.
    ///
    /// Note that really any functioning Nexus would work as the "assignee",
    /// but it's clear that our instance will work, because we're currently
    /// running.
    pub async fn support_bundle_create(
        &self,
        opctx: &OpContext,
        reason_for_creation: &'static str,
        this_nexus_id: OmicronZoneUuid,
        user_comment: Option<String>,
    ) -> CreateResult<SupportBundle> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        #[derive(Debug)]
        enum SupportBundleError {
            TooManyBundles,
        }

        let err = OptionalError::new();
        self.transaction_retry_wrapper("support_bundle_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let user_comment = user_comment.clone();

                async move {
                    use nexus_db_schema::schema::rendezvous_debug_dataset::dsl as dataset_dsl;
                    use nexus_db_schema::schema::support_bundle::dsl as support_bundle_dsl;

                    // Observe all "non-deleted, debug datasets".
                    //
                    // Return the first one we find that doesn't already
                    // have a support bundle allocated to it.
                    let free_dataset = dataset_dsl::rendezvous_debug_dataset
                        .filter(dataset_dsl::time_tombstoned.is_null())
                        .left_join(support_bundle_dsl::support_bundle.on(
                            dataset_dsl::id.eq(support_bundle_dsl::dataset_id),
                        ))
                        .filter(support_bundle_dsl::dataset_id.is_null())
                        .select(RendezvousDebugDataset::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?;

                    let Some(dataset) = free_dataset else {
                        return Err(
                            err.bail(SupportBundleError::TooManyBundles)
                        );
                    };

                    // We could check that "this_nexus_id" is not expunged, but
                    // we have some evidence that it is valid: this Nexus is
                    // currently running!
                    //
                    // Besides, we COULD be expunged immediately after inserting
                    // the SupportBundle. In this case, we'd fall back to the
                    // case of "clean up a bundle which is managed by an
                    // expunged Nexus" anyway.

                    let bundle = SupportBundle::new(
                        reason_for_creation,
                        dataset.pool_id(),
                        dataset.id(),
                        this_nexus_id,
                        user_comment,
                    );

                    diesel::insert_into(support_bundle_dsl::support_bundle)
                        .values(bundle.clone())
                        .execute_async(&conn)
                        .await?;

                    Ok(bundle)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        SupportBundleError::TooManyBundles => {
                            return external::Error::insufficient_capacity(
                                CANNOT_ALLOCATE_ERR_MSG,
                                "Support Bundle storage exhausted",
                            );
                        }
                    }
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Looks up a single support bundle
    pub async fn support_bundle_get(
        &self,
        opctx: &OpContext,
        id: SupportBundleUuid,
    ) -> LookupResult<SupportBundle> {
        let (.., db_bundle) =
            LookupPath::new(opctx, self).support_bundle(id).fetch().await?;

        Ok(db_bundle)
    }

    /// Lists one page of support bundles ordered by creation time
    pub async fn support_bundle_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (chrono::DateTime<chrono::Utc>, Uuid)>,
    ) -> ListResultVec<SupportBundle> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::support_bundle::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        paginated_multicolumn(
            dsl::support_bundle,
            (dsl::time_created, dsl::id),
            pagparams,
        )
        .select(SupportBundle::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Lists one page of support bundles in a particular state, assigned to
    /// a particular Nexus.
    pub async fn support_bundle_list_assigned_to_nexus(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
        nexus_id: OmicronZoneUuid,
        states: Vec<SupportBundleState>,
    ) -> ListResultVec<SupportBundle> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::support_bundle::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(dsl::support_bundle, dsl::id, pagparams)
            .filter(dsl::assigned_nexus.eq(nexus_id.into_untyped_uuid()))
            .filter(dsl::state.eq_any(states))
            .order(dsl::time_created.asc())
            .select(SupportBundle::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Marks support bundles as failed if their assigned Nexus or backing
    /// dataset has been destroyed.
    pub async fn support_bundle_fail_expunged(
        &self,
        opctx: &OpContext,
        blueprint: &nexus_types::deployment::Blueprint,
        our_nexus_id: OmicronZoneUuid,
    ) -> Result<SupportBundleExpungementReport, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // For this blueprint: The set of all expunged Nexus zones that are
        // ready for cleanup
        let invalid_nexus_zones = blueprint
            .expunged_nexus_zones_ready_for_cleanup(
                BlueprintExpungedZoneAccessReason::NexusSupportBundleReassign,
            )
            .map(|(_sled, zone, _nexus_config)| zone.id.into_untyped_uuid())
            .collect::<Vec<Uuid>>();

        // For this blueprint: The set of expunged debug datasets
        let invalid_datasets = blueprint
            .all_omicron_datasets(BlueprintDatasetDisposition::is_expunged)
            .filter_map(|(_sled_id, dataset_config)| {
                if matches!(
                    dataset_config.kind,
                    omicron_common::api::internal::shared::DatasetKind::Debug
                ) {
                    Some(dataset_config.id.into_untyped_uuid())
                } else {
                    None
                }
            })
            .collect::<Vec<Uuid>>();

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_if_current_blueprint_is(
            &conn,
            "support_bundle_fail_expunged",
            opctx,
            blueprint.id,
            |conn| {
                let invalid_nexus_zones = invalid_nexus_zones.clone();
                let invalid_datasets = invalid_datasets.clone();
                async move {
                    use nexus_db_schema::schema::support_bundle::dsl;

                    // Find all bundles without backing storage.
                    let bundles_with_bad_datasets = dsl::support_bundle
                        .filter(dsl::dataset_id.eq_any(invalid_datasets))
                        .select(SupportBundle::as_select())
                        .load_async(conn)
                        .await?;

                    // Split these bundles into two categories:
                    // - Ones that are being destroyed anyway, and that can be
                    // fully deleted.
                    // - Ones that are NOT being destroyed, and should be marked
                    // failed so the end-user has visibility into their
                    // destruction.
                    let (bundles_to_delete, bundles_to_fail): (Vec<_>, Vec<_>) =
                        bundles_with_bad_datasets.into_iter().partition(
                            |bundle| {
                                bundle.state == SupportBundleState::Destroying
                            },
                        );
                    let bundles_to_delete = bundles_to_delete
                        .into_iter()
                        .map(|b| b.id)
                        .collect::<Vec<_>>();
                    let bundles_to_fail = bundles_to_fail
                        .into_iter()
                        .map(|b| b.id)
                        .collect::<Vec<_>>();

                    // Find all non-destroying bundles on datasets that no
                    // longer exist, and mark them "failed". They skip the
                    // "failing" state because there is no remaining storage to
                    // be cleaned up.
                    let state = SupportBundleState::Failed;
                    let bundles_failed_missing_datasets =
                        diesel::update(dsl::support_bundle)
                            .filter(dsl::state.eq_any(state.valid_old_states()))
                            .filter(dsl::id.eq_any(bundles_to_fail))
                            .set((
                                dsl::state.eq(state),
                                dsl::reason_for_failure
                                    .eq(FAILURE_REASON_NO_DATASET),
                            ))
                            .execute_async(conn)
                            .await?;
                    // For bundles that are in the process of being destroyed,
                    // the dataset expungement speeds up the process.
                    let bundles_deleted_missing_datasets =
                        diesel::delete(dsl::support_bundle)
                            .filter(dsl::id.eq_any(bundles_to_delete))
                            // This check should be redundant (we already
                            // partitioned above based on this state) but out of
                            // an abundance of caution we don't auto-delete a
                            // bundle in any other state.
                            .filter(
                                dsl::state.eq(SupportBundleState::Destroying),
                            )
                            .execute_async(conn)
                            .await?;

                    // Find all bundles managed by nexuses that no longer exist.
                    let bundles_with_bad_nexuses = dsl::support_bundle
                        .filter(dsl::assigned_nexus.eq_any(invalid_nexus_zones))
                        .select(SupportBundle::as_select())
                        .load_async(conn)
                        .await?;

                    let bundles_to_mark_failing = bundles_with_bad_nexuses
                        .iter()
                        .filter_map(|bundle| {
                            match bundle.state {
                                // If the Nexus died mid-collection, we'll mark
                                // this bundle failing. This should ensure that
                                // any storage it might have been using is
                                // cleaned up.
                                SupportBundleState::Collecting => {
                                    Some(bundle.id)
                                }
                                // If the bundle was marked "active", we need to
                                // re-assign the "owning Nexus" so it can later
                                // be deleted, but the bundle itself has already
                                // been stored on a sled.
                                //
                                // There's no need to fail it.
                                SupportBundleState::Active => None,
                                // If the bundle has already been marked
                                // "Destroying/Failing/Failed", it's already
                                // irreversibly marked for deletion.
                                SupportBundleState::Destroying
                                | SupportBundleState::Failing
                                | SupportBundleState::Failed => None,
                            }
                        })
                        .collect::<Vec<_>>();

                    let bundles_to_reassign = bundles_with_bad_nexuses
                        .iter()
                        .filter_map(|bundle| {
                            match bundle.state {
                                // If the bundle might be using any storage on
                                // the provisioned sled, it gets assigned to a
                                // new Nexus.
                                SupportBundleState::Collecting
                                | SupportBundleState::Active
                                | SupportBundleState::Destroying
                                | SupportBundleState::Failing => {
                                    Some(bundle.id)
                                }
                                SupportBundleState::Failed => None,
                            }
                        })
                        .collect::<Vec<_>>();

                    // Mark these support bundles as failing, and assign them
                    // to a new Nexus (ourselves).
                    //
                    // This should lead to their storage being freed, if it
                    // exists.
                    let state = SupportBundleState::Failing;
                    let bundles_failing_missing_nexus =
                        diesel::update(dsl::support_bundle)
                            .filter(dsl::state.eq_any(state.valid_old_states()))
                            .filter(dsl::id.eq_any(bundles_to_mark_failing))
                            .set((
                                dsl::state.eq(state),
                                dsl::reason_for_failure
                                    .eq(FAILURE_REASON_NO_NEXUS),
                            ))
                            .execute_async(conn)
                            .await?;

                    let mut report = SupportBundleExpungementReport {
                        bundles_failed_missing_datasets,
                        bundles_deleted_missing_datasets,
                        bundles_failing_missing_nexus,
                        bundles_reassigned: 0,
                    };

                    if !bundles_to_reassign.is_empty() {
                        // Reassign bundles that haven't been marked "fully failed"
                        // to ourselves, so we can free their storage if they have
                        // been provisioned on a sled.
                        report.bundles_reassigned =
                            diesel::update(dsl::support_bundle)
                                .filter(dsl::id.eq_any(bundles_to_reassign))
                                .set(
                                    dsl::assigned_nexus
                                        .eq(our_nexus_id.into_untyped_uuid()),
                                )
                                .execute_async(conn)
                                .await?;
                    }

                    Ok(report)
                }
                .boxed()
            },
        )
        .await
    }

    /// Updates the state of a support bundle.
    ///
    /// Returns:
    /// - "Ok" if the bundle was updated successfully.
    /// - "Err::InvalidRequest" if the bundle exists, but could not be updated
    /// because the state transition is invalid.
    pub async fn support_bundle_update(
        &self,
        opctx: &OpContext,
        authz_bundle: &authz::SupportBundle,
        state: SupportBundleState,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_bundle).await?;

        use nexus_db_schema::schema::support_bundle::dsl;

        let id = authz_bundle.id().into_untyped_uuid();
        let conn = self.pool_connection_authorized(opctx).await?;
        let result = diesel::update(dsl::support_bundle)
            .filter(dsl::id.eq(id))
            .filter(dsl::state.eq_any(state.valid_old_states()))
            .set(dsl::state.eq(state))
            .check_if_exists::<SupportBundle>(id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => {
                Err(Error::invalid_request(format!(
                    "Cannot update support bundle state from {:?} to {:?}",
                    result.found.state, state
                )))
            }
        }
    }

    /// Updates the user comment of a support bundle.
    ///
    /// Returns:
    /// - "Ok" if the bundle was updated successfully.
    /// - "Err::InvalidRequest" if the comment exceeds the maximum length.
    pub async fn support_bundle_update_user_comment(
        &self,
        opctx: &OpContext,
        authz_bundle: &authz::SupportBundle,
        user_comment: Option<String>,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_bundle).await?;

        if let Some(ref comment) = user_comment {
            if comment.len() > 4096 {
                return Err(Error::invalid_request(
                    "User comment cannot exceed 4096 bytes",
                ));
            }
        }

        use nexus_db_schema::schema::support_bundle::dsl;

        let id = authz_bundle.id().into_untyped_uuid();
        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::update(dsl::support_bundle)
            .filter(dsl::id.eq(id))
            .set(dsl::user_comment.eq(user_comment))
            .execute_async(&*conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Deletes a support bundle.
    ///
    /// This should only be invoked after all storage for the support bundle has
    /// been cleared.
    pub async fn support_bundle_delete(
        &self,
        opctx: &OpContext,
        authz_bundle: &authz::SupportBundle,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Delete, authz_bundle).await?;

        use nexus_db_schema::schema::support_bundle::dsl;

        let id = authz_bundle.id().into_untyped_uuid();
        let conn = self.pool_connection_authorized(opctx).await?;
        diesel::delete(dsl::support_bundle)
            .filter(
                dsl::state
                    .eq(SupportBundleState::Destroying)
                    .or(dsl::state.eq(SupportBundleState::Failed)),
            )
            .filter(dsl::id.eq(id))
            .execute_async(&*conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Atomically finds and deletes support bundles to maintain free dataset buffer.
    ///
    /// This method performs a single atomic operation that:
    /// 1. Reads config from the `support_bundle_config` table
    /// 2. Calculates thresholds based on percentage of total datasets
    /// 3. Calculates how many deletions are needed (free_datasets < target)
    /// 4. Applies min_keep constraint (never delete below minimum)
    /// 5. Finds the N oldest Active bundles
    /// 6. Transitions them to Destroying state atomically
    /// 7. Returns what was actually deleted
    ///
    /// This prevents over-deletion when multiple Nexuses run concurrently,
    /// because the calculation and state transitions happen in a single
    /// database operation.
    ///
    /// Configuration is read from the database, ensuring all Nexus replicas
    /// use consistent values.
    pub async fn support_bundle_auto_delete(
        &self,
        opctx: &OpContext,
    ) -> Result<AutoDeletionResult, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        let query = support_bundle_auto_delete_query();

        // Return type: (total_datasets, used_datasets, active_bundles, deleted_ids)
        let result: (i64, i64, i64, Vec<Uuid>) = query
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let total_datasets = result.0 as usize;
        let used_datasets = result.1 as usize;
        let active_bundles = result.2 as usize;
        let deleted_ids: Vec<SupportBundleUuid> = result
            .3
            .into_iter()
            .map(SupportBundleUuid::from_untyped_uuid)
            .collect();

        Ok(AutoDeletionResult {
            deleted_ids,
            total_datasets,
            active_bundles,
            free_datasets: total_datasets.saturating_sub(used_datasets),
        })
    }

    /// Get the current support bundle auto-deletion config.
    ///
    /// The singleton row always exists (created by schema migration).
    pub async fn support_bundle_config_get(
        &self,
        opctx: &OpContext,
    ) -> Result<crate::db::model::SupportBundleConfig, Error> {
        use nexus_db_schema::schema::support_bundle_config::dsl;

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::support_bundle_config
            .select(crate::db::model::SupportBundleConfig::as_select())
            .first_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Set support bundle auto-deletion config.
    ///
    /// Values are percentages (0-100). The singleton row always exists
    /// (created by schema migration), so this is an UPDATE operation.
    pub async fn support_bundle_config_set(
        &self,
        opctx: &OpContext,
        target_free_percent: u8,
        min_keep_percent: u8,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::support_bundle_config::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // Validate percentages are in range
        if target_free_percent > 100 {
            return Err(Error::invalid_request(
                "target_free_percent must be between 0 and 100",
            ));
        }
        if min_keep_percent > 100 {
            return Err(Error::invalid_request(
                "min_keep_percent must be between 0 and 100",
            ));
        }

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::update(dsl::support_bundle_config)
            .filter(dsl::singleton.eq(true))
            .set((
                dsl::target_free_percent.eq(i64::from(target_free_percent)),
                dsl::min_keep_percent.eq(i64::from(min_keep_percent)),
                dsl::time_modified.eq(chrono::Utc::now()),
            ))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

/// Builds the CTE query for atomic support bundle auto-deletion.
///
/// This query atomically:
/// 1. Reads config from support_bundle_config table
/// 2. Calculates thresholds based on percentage of total datasets using CEIL
/// 3. Counts datasets and bundles
/// 4. Calculates how many bundles to delete (respecting min_keep)
/// 5. Selects the oldest Active bundles
/// 6. Transitions them to Destroying state
/// 7. Returns the stats and deleted IDs
pub fn support_bundle_auto_delete_query() -> TypedSqlQuery<(
    diesel::sql_types::BigInt,
    diesel::sql_types::BigInt,
    diesel::sql_types::BigInt,
    diesel::sql_types::Array<diesel::sql_types::Uuid>,
)> {
    use crate::db::raw_query_builder::QueryBuilder;

    let mut query = QueryBuilder::new();

    // Build the CTE query
    query.sql(
        "
WITH
  -- Read config from database (singleton row always exists from migration)
  config AS (
    SELECT target_free_percent, min_keep_percent
    FROM support_bundle_config
    WHERE singleton = true
  ),
  -- Count non-tombstoned datasets (uses lookup_usable_rendezvous_debug_dataset index)
  dataset_count AS (
    SELECT COUNT(*) as total
    FROM rendezvous_debug_dataset
    WHERE time_tombstoned IS NULL
  ),
  -- Count bundles occupying datasets for deletion calculation purposes.
  -- We only count stable states: Collecting and Active.
  --
  -- Why exclude transitional states (Destroying, Failing)?
  -- These bundles are already in cleanup transitions and their datasets will
  -- be freed soon. Including them would cause concurrent auto-delete operations
  -- to each see free=0 and keep deleting more bundles than necessary.
  --
  -- Why exclude Failed?
  -- Failed bundles do NOT occupy datasets (their dataset was expunged).
  used_count AS (
    SELECT COUNT(*) as used
    FROM support_bundle
    WHERE state IN ('collecting', 'active')
  ),
  -- Count active bundles (for min_keep check)
  active_count AS (
    SELECT COUNT(*) as active
    FROM support_bundle
    WHERE state = 'active'
  ),
  -- Calculate absolute thresholds from percentages using CEIL.
  -- CEIL ensures we always round up, so 10% of 5 datasets = 1, not 0.
  thresholds AS (
    SELECT
      CEIL((SELECT total FROM dataset_count) * (SELECT target_free_percent FROM config) / 100.0)::INT8 as target_free,
      CEIL((SELECT total FROM dataset_count) * (SELECT min_keep_percent FROM config) / 100.0)::INT8 as min_keep
  ),
  -- Calculate how many bundles we need AND are allowed to delete.
  -- min_keep is respected first (max_deletable constraint).
  deletion_calc AS (
    SELECT
      (SELECT total FROM dataset_count) as total_datasets,
      (SELECT used FROM used_count) as used_datasets,
      (SELECT active FROM active_count) as active_bundles,
      GREATEST(0,
        (SELECT target_free FROM thresholds)
        - ((SELECT total FROM dataset_count) - (SELECT used FROM used_count))
      ) as bundles_needed,
      GREATEST(0,
        (SELECT active FROM active_count) - (SELECT min_keep FROM thresholds)
      ) as max_deletable
  ),
  -- Find the N oldest active bundles we're allowed to delete.
  -- Uses lookup_bundle_by_state_and_creation index for ordering.
  -- Secondary sort on id ensures deterministic selection when timestamps match,
  -- which is critical for preventing over-deletion under concurrent execution.
  candidates AS (
    SELECT id
    FROM support_bundle
    WHERE state = 'active'
    ORDER BY time_created ASC, id ASC
    LIMIT (SELECT LEAST(bundles_needed, max_deletable) FROM deletion_calc)
  ),
  -- Atomically transition to Destroying (only if still Active).
  -- The state='active' check handles concurrent user deletions.
  deleted AS (
    UPDATE support_bundle
    SET state = 'destroying'
    WHERE id IN (SELECT id FROM candidates)
      AND state = 'active'
    RETURNING id
  )
SELECT
  (SELECT total_datasets FROM deletion_calc),
  (SELECT used_datasets FROM deletion_calc),
  (SELECT active_bundles FROM deletion_calc),
  ARRAY(SELECT id FROM deleted) as deleted_ids
",
    );

    query.query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test::bp_insert_and_make_target;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::Generation;
    use nexus_db_model::SledBaseboard;
    use nexus_db_model::SledCpuFamily;
    use nexus_db_model::SledSystemHardware;
    use nexus_db_model::SledUpdate;
    use nexus_db_model::Zpool;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::example::SimRngState;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneType;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::LookupType;
    use omicron_common::api::internal::shared::DatasetKind::Debug as DebugDatasetKind;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use rand::Rng;

    fn authz_support_bundle_from_id(
        id: SupportBundleUuid,
    ) -> authz::SupportBundle {
        authz::SupportBundle::new(
            authz::FLEET,
            id,
            LookupType::ById(id.into_untyped_uuid()),
        )
    }

    // Pool/Dataset pairs, for debug datasets only.
    struct TestPool {
        pool: ZpoolUuid,
        dataset: DatasetUuid,
    }

    // Sleds and their pools, with a focus on debug datasets only.
    struct TestSled {
        sled: SledUuid,
        pools: Vec<TestPool>,
    }

    impl TestSled {
        fn new_with_pool_count(pool_count: usize) -> Self {
            Self {
                sled: SledUuid::new_v4(),
                pools: (0..pool_count)
                    .map(|_| TestPool {
                        pool: ZpoolUuid::new_v4(),
                        dataset: DatasetUuid::new_v4(),
                    })
                    .collect(),
            }
        }

        fn new_from_blueprint(blueprint: &Blueprint) -> Vec<Self> {
            let mut sleds = vec![];
            for (sled, config) in &blueprint.sleds {
                let pools = config
                    .datasets
                    .iter()
                    .filter_map(|dataset| {
                        if !matches!(dataset.kind, DebugDatasetKind)
                            || !dataset.disposition.is_in_service()
                        {
                            return None;
                        };

                        Some(TestPool {
                            pool: dataset.pool.id(),
                            dataset: dataset.id,
                        })
                    })
                    .collect();

                sleds.push(TestSled { sled: *sled, pools });
            }
            sleds
        }

        async fn create_database_records(
            &self,
            datastore: &DataStore,
            opctx: &OpContext,
        ) {
            let rack_id = Uuid::new_v4();
            let blueprint_id = BlueprintUuid::new_v4();
            let sled = SledUpdate::new(
                self.sled,
                "[::1]:0".parse().unwrap(),
                0,
                SledBaseboard {
                    serial_number: format!(
                        "test-{}",
                        rand::rng().random::<u64>()
                    ),
                    part_number: "test-pn".to_string(),
                    revision: 0,
                },
                SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 128,
                    usable_physical_ram: (64 << 30).try_into().unwrap(),
                    reservoir_size: (16 << 30).try_into().unwrap(),
                    cpu_family: SledCpuFamily::AmdMilan,
                },
                rack_id,
                Generation::new(),
            );
            datastore.sled_upsert(sled).await.expect("failed to upsert sled");

            // Create fake zpools that back our fake datasets.
            for pool in &self.pools {
                let zpool = Zpool::new(
                    pool.pool,
                    self.sled,
                    PhysicalDiskUuid::new_v4(),
                    ByteCount::from(0).into(),
                );
                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("inserted zpool");

                let dataset = RendezvousDebugDataset::new(
                    pool.dataset,
                    pool.pool,
                    blueprint_id,
                );
                datastore
                    .debug_dataset_insert_if_not_exists(opctx, dataset)
                    .await
                    .expect("inserted debug dataset");
            }
        }
    }

    // Creates a fake sled with `pool_count` zpools, and a debug dataset on each
    // zpool.
    async fn create_sled_and_zpools(
        datastore: &DataStore,
        opctx: &OpContext,
        pool_count: usize,
    ) -> TestSled {
        let sled = TestSled::new_with_pool_count(pool_count);
        sled.create_database_records(&datastore, &opctx).await;
        sled
    }

    async fn support_bundle_create_expect_no_capacity(
        datastore: &DataStore,
        opctx: &OpContext,
        this_nexus_id: OmicronZoneUuid,
    ) {
        let err = datastore
            .support_bundle_create(&opctx, "for tests", this_nexus_id, None)
            .await
            .expect_err("Shouldn't provision bundle without datasets");
        let Error::InsufficientCapacity { message } = err else {
            panic!(
                "Unexpected error: {err:?} - we expected 'InsufficientCapacity'"
            );
        };
        assert_eq!(
            CANNOT_ALLOCATE_ERR_MSG,
            message.external_message(),
            "Unexpected error: {message:?}"
        );
    }

    #[tokio::test]
    async fn test_bundle_list_filtering() {
        let logctx = dev::test_setup_log("test_bundle_create_capacity_limits");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let nexus_a = OmicronZoneUuid::new_v4();
        let nexus_b = OmicronZoneUuid::new_v4();

        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        let pagparams = DataPageParams::max_page();

        // No bundles exist yet, so the list should be empty

        assert_eq!(
            datastore
                .support_bundle_list_assigned_to_nexus(
                    &opctx,
                    &pagparams,
                    nexus_a,
                    vec![SupportBundleState::Collecting]
                )
                .await
                .expect("Should always be able to list bundles"),
            vec![]
        );

        // Create two bundles on "nexus A", one bundle on "nexus B"

        let bundle_a1 = datastore
            .support_bundle_create(&opctx, "for the test", nexus_a, None)
            .await
            .expect("Should be able to create bundle");
        let bundle_a2 = datastore
            .support_bundle_create(&opctx, "for the test", nexus_a, None)
            .await
            .expect("Should be able to create bundle");
        let bundle_b1 = datastore
            .support_bundle_create(&opctx, "for the test", nexus_b, None)
            .await
            .expect("Should be able to create bundle");

        assert_eq!(
            datastore
                .support_bundle_list_assigned_to_nexus(
                    &opctx,
                    &pagparams,
                    nexus_a,
                    vec![SupportBundleState::Collecting]
                )
                .await
                .expect("Should always be able to list bundles")
                .iter()
                .map(|b| b.id)
                .collect::<Vec<_>>(),
            vec![bundle_a1.id, bundle_a2.id,]
        );
        assert_eq!(
            datastore
                .support_bundle_list_assigned_to_nexus(
                    &opctx,
                    &pagparams,
                    nexus_b,
                    vec![SupportBundleState::Collecting]
                )
                .await
                .expect("Should always be able to list bundles")
                .iter()
                .map(|b| b.id)
                .collect::<Vec<_>>(),
            vec![bundle_b1.id,]
        );

        // When we update the state of the bundles, the list results
        // should also be filtered.
        let authz_bundle = authz_support_bundle_from_id(bundle_a1.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Active,
            )
            .await
            .expect("Should have been able to update state");

        // "bundle_a1" is no longer collecting, so it won't appear here.
        assert_eq!(
            datastore
                .support_bundle_list_assigned_to_nexus(
                    &opctx,
                    &pagparams,
                    nexus_a,
                    vec![SupportBundleState::Collecting]
                )
                .await
                .expect("Should always be able to list bundles")
                .iter()
                .map(|b| b.id)
                .collect::<Vec<_>>(),
            vec![bundle_a2.id,]
        );

        // ... but if we ask for enough states, it'll show up
        assert_eq!(
            datastore
                .support_bundle_list_assigned_to_nexus(
                    &opctx,
                    &pagparams,
                    nexus_a,
                    vec![
                        SupportBundleState::Active,
                        SupportBundleState::Collecting
                    ]
                )
                .await
                .expect("Should always be able to list bundles")
                .iter()
                .map(|b| b.id)
                .collect::<Vec<_>>(),
            vec![bundle_a1.id, bundle_a2.id,]
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bundle_create_capacity_limits() {
        let logctx = dev::test_setup_log("test_bundle_create_capacity_limits");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let this_nexus_id = OmicronZoneUuid::new_v4();

        // No sleds, no datasets. Allocation should fail.

        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // Create a sled with a couple pools. Allocation should succeed.

        const POOL_COUNT: usize = 2;
        let _test_sled =
            create_sled_and_zpools(&datastore, &opctx, POOL_COUNT).await;
        let mut bundles = vec![];
        for _ in 0..POOL_COUNT {
            bundles.push(
                datastore
                    .support_bundle_create(
                        &opctx,
                        "for the test",
                        this_nexus_id,
                        None,
                    )
                    .await
                    .expect("Should be able to create bundle"),
            );
        }

        // If we try to allocate any more bundles, we'll run out of capacity.

        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // If we destroy a bundle, it isn't deleted (yet).
        // This operation should signify that we can start to free up
        // storage on the dataset, but that needs to happen outside the
        // database.
        //
        // We should still expect to hit capacity limits.
        let authz_bundle = authz_support_bundle_from_id(bundles[0].id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .expect("Should be able to destroy this bundle");
        support_bundle_create_expect_no_capacity(
            &datastore,
            &opctx,
            this_nexus_id,
        )
        .await;

        // If we delete a bundle, it should be gone. This means we can
        // re-allocate from that dataset which was just freed up.

        let authz_bundle = authz_support_bundle_from_id(bundles[0].id.into());
        datastore
            .support_bundle_delete(&opctx, &authz_bundle)
            .await
            .expect("Should be able to destroy this bundle");
        datastore
            .support_bundle_create(&opctx, "for the test", this_nexus_id, None)
            .await
            .expect("Should be able to create bundle");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_crud_operations() {
        let logctx = dev::test_setup_log("test_crud_operations");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let test_sled = create_sled_and_zpools(&datastore, &opctx, 1).await;
        let reason = "Bundle for test";
        let this_nexus_id = OmicronZoneUuid::new_v4();

        // Create the bundle, then observe it through the "getter" APIs

        let mut bundle = datastore
            .support_bundle_create(&opctx, reason, this_nexus_id, None)
            .await
            .expect("Should be able to create bundle");
        assert_eq!(bundle.reason_for_creation, reason);
        assert_eq!(bundle.reason_for_failure, None);
        assert_eq!(bundle.assigned_nexus, Some(this_nexus_id.into()));
        assert_eq!(bundle.state, SupportBundleState::Collecting);
        assert_eq!(bundle.zpool_id, test_sled.pools[0].pool.into());
        assert_eq!(bundle.dataset_id, test_sled.pools[0].dataset.into());

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just created");
        // Overwrite this column; it is modified slightly upon database insertion.
        bundle.time_created = observed_bundle.time_created;
        assert_eq!(bundle, observed_bundle);

        let pagparams = DataPageParams::max_page();
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to get bundle we just created");
        assert_eq!(1, observed_bundles.len());
        assert_eq!(bundle, observed_bundles[0]);

        // Destroy the bundle, observe the new state
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .expect("Should be able to destroy our bundle");
        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just created");
        assert_eq!(SupportBundleState::Destroying, observed_bundle.state);

        // Delete the bundle, observe that it's gone

        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_delete(&opctx, &authz_bundle)
            .await
            .expect("Should be able to destroy our bundle");
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to query when no bundles exist");
        assert!(observed_bundles.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn get_in_service_nexuses_from_blueprint(
        bp: &Blueprint,
    ) -> Vec<OmicronZoneUuid> {
        bp.sleds
            .values()
            .flat_map(|sled_config| {
                let mut nexus_zones = vec![];
                for zone in &sled_config.zones {
                    if matches!(zone.zone_type, BlueprintZoneType::Nexus(_))
                        && zone.disposition.is_in_service()
                    {
                        nexus_zones.push(zone.id);
                    }
                }
                nexus_zones
            })
            .collect()
    }

    fn get_in_service_debug_datasets_from_blueprint(
        bp: &Blueprint,
    ) -> Vec<DatasetUuid> {
        bp.sleds
            .values()
            .flat_map(|sled_config| {
                let mut debug_datasets = vec![];
                for dataset in sled_config.datasets.iter() {
                    if matches!(dataset.kind, DebugDatasetKind)
                        && dataset.disposition.is_in_service()
                    {
                        debug_datasets.push(dataset.id);
                    }
                }
                debug_datasets
            })
            .collect()
    }

    fn expunge_dataset_for_bundle(bp: &mut Blueprint, bundle: &SupportBundle) {
        for sled in bp.sleds.values_mut() {
            for mut dataset in sled.datasets.iter_mut() {
                if dataset.id == bundle.dataset_id.into() {
                    dataset.disposition = BlueprintDatasetDisposition::Expunged;
                }
            }
        }
    }

    fn expunge_nexus_for_bundle(bp: &mut Blueprint, bundle: &SupportBundle) {
        for sled in bp.sleds.values_mut() {
            for mut zone in &mut sled.zones {
                if zone.id == bundle.assigned_nexus.unwrap().into() {
                    zone.disposition = BlueprintZoneDisposition::Expunged {
                        as_of_generation: *Generation::new(),
                        ready_for_cleanup: true,
                    };
                }
            }
        }
    }

    #[tokio::test]
    async fn test_bundle_failed_from_expunged_dataset() {
        static TEST_NAME: &str = "test_bundle_failed_from_expunged_dataset";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (_example, mut bp1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();

        // Weirdly, the "ExampleSystemBuilder" blueprint has a parent blueprint,
        // but which isn't exposed through the API. Since we're only able to see
        // the blueprint it emits, that means we can't actually make it the
        // target because "the parent blueprint is not the current target".
        //
        // Instead of dealing with that, we lie: claim this is the primordial
        // blueprint, with no parent.
        //
        // Regardless, make this starter blueprint our target.
        bp1.parent_blueprint_id = None;
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // Manually perform the equivalent of blueprint execution to populate
        // database records.
        let sleds = TestSled::new_from_blueprint(&bp1);
        for sled in &sleds {
            sled.create_database_records(&datastore, &opctx).await;
        }

        // Extract Nexus and Dataset information from the generated blueprint.
        let this_nexus_id = get_in_service_nexuses_from_blueprint(&bp1)
            .get(0)
            .map(|id| *id)
            .expect("There should be a Nexus in the example blueprint");
        let debug_datasets = get_in_service_debug_datasets_from_blueprint(&bp1);
        assert!(!debug_datasets.is_empty());

        // When we create a bundle, it should exist on a dataset provisioned by
        // the blueprint.
        let bundle = datastore
            .support_bundle_create(&opctx, "for the test", this_nexus_id, None)
            .await
            .expect("Should be able to create bundle");
        assert_eq!(bundle.assigned_nexus, Some(this_nexus_id.into()));
        assert!(
            debug_datasets.contains(&DatasetUuid::from(bundle.dataset_id)),
            "Bundle should have been allocated from a blueprint dataset"
        );

        // If we try to "fail support bundles" from expunged datasets/nexuses,
        // we should see a no-op. Nothing has been expunged yet!
        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp1, this_nexus_id)
            .await
            .expect(
                "Should have been able to perform no-op support bundle failure",
            );
        assert_eq!(SupportBundleExpungementReport::default(), report);

        // Expunge the bundle's dataset (manually)
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = BlueprintUuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            expunge_dataset_for_bundle(&mut bp2, &bundle);
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        datastore
            .support_bundle_fail_expunged(&opctx, &bp1, this_nexus_id)
            .await
            .expect_err("bp1 is no longer the target; this should fail");
        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp2, this_nexus_id)
            .await
            .expect("Should have been able to mark bundle state as failed");
        assert_eq!(
            SupportBundleExpungementReport {
                bundles_failed_missing_datasets: 1,
                ..Default::default()
            },
            report
        );

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just failed");
        assert_eq!(SupportBundleState::Failed, observed_bundle.state);
        assert!(
            observed_bundle
                .reason_for_failure
                .unwrap()
                .contains(FAILURE_REASON_NO_DATASET)
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bundle_deleted_from_expunged_dataset() {
        static TEST_NAME: &str = "test_bundle_deleted_from_expunged_dataset";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (_example, mut bp1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();

        // Weirdly, the "ExampleSystemBuilder" blueprint has a parent blueprint,
        // but which isn't exposed through the API. Since we're only able to see
        // the blueprint it emits, that means we can't actually make it the
        // target because "the parent blueprint is not the current target".
        //
        // Instead of dealing with that, we lie: claim this is the primordial
        // blueprint, with no parent.
        //
        // Regardless, make this starter blueprint our target.
        bp1.parent_blueprint_id = None;
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // Manually perform the equivalent of blueprint execution to populate
        // database records.
        let sleds = TestSled::new_from_blueprint(&bp1);
        for sled in &sleds {
            sled.create_database_records(&datastore, &opctx).await;
        }

        // Extract Nexus and Dataset information from the generated blueprint.
        let this_nexus_id = get_in_service_nexuses_from_blueprint(&bp1)
            .get(0)
            .map(|id| *id)
            .expect("There should be a Nexus in the example blueprint");
        let debug_datasets = get_in_service_debug_datasets_from_blueprint(&bp1);
        assert!(!debug_datasets.is_empty());

        // When we create a bundle, it should exist on a dataset provisioned by
        // the blueprint.
        let bundle = datastore
            .support_bundle_create(&opctx, "for the test", this_nexus_id, None)
            .await
            .expect("Should be able to create bundle");
        assert_eq!(bundle.assigned_nexus, Some(this_nexus_id.into()));
        assert!(
            debug_datasets.contains(&DatasetUuid::from(bundle.dataset_id)),
            "Bundle should have been allocated from a blueprint dataset"
        );

        // Start the deletion of this bundle
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Destroying,
            )
            .await
            .expect("Should have been able to update state");

        // If we try to "fail support bundles" from expunged datasets/nexuses,
        // we should see a no-op. Nothing has been expunged yet!
        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp1, this_nexus_id)
            .await
            .expect(
                "Should have been able to perform no-op support bundle failure",
            );
        assert_eq!(SupportBundleExpungementReport::default(), report);

        // Expunge the bundle's dataset (manually)
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = BlueprintUuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            expunge_dataset_for_bundle(&mut bp2, &bundle);
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        datastore
            .support_bundle_fail_expunged(&opctx, &bp1, this_nexus_id)
            .await
            .expect_err("bp1 is no longer the target; this should fail");
        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp2, this_nexus_id)
            .await
            .expect("Should have been able to mark bundle state as failed");
        assert_eq!(
            SupportBundleExpungementReport {
                bundles_deleted_missing_datasets: 1,
                ..Default::default()
            },
            report
        );

        // Should observe no bundles (it should have been deleted)
        let pagparams = DataPageParams::max_page();
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to query when no bundles exist");
        assert!(observed_bundles.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bundle_failed_from_expunged_nexus_no_reassign() {
        static TEST_NAME: &str =
            "test_bundle_failed_from_expunged_nexus_no_reassign";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (_example, mut bp1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();

        bp1.parent_blueprint_id = None;
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // Manually perform the equivalent of blueprint execution to populate
        // database records.
        let sleds = TestSled::new_from_blueprint(&bp1);
        for sled in &sleds {
            sled.create_database_records(&datastore, &opctx).await;
        }

        // Extract Nexus and Dataset information from the generated blueprint.
        let nexus_ids = get_in_service_nexuses_from_blueprint(&bp1);
        let debug_datasets = get_in_service_debug_datasets_from_blueprint(&bp1);
        assert!(!debug_datasets.is_empty());

        // When we create a bundle, it should exist on a dataset provisioned by
        // the blueprint.
        let bundle = datastore
            .support_bundle_create(&opctx, "for the test", nexus_ids[0], None)
            .await
            .expect("Should be able to create bundle");

        assert_eq!(bundle.state, SupportBundleState::Collecting);
        assert_eq!(bundle.assigned_nexus, Some(nexus_ids[0].into()));
        assert!(
            debug_datasets.contains(&DatasetUuid::from(bundle.dataset_id)),
            "Bundle should have been allocated from a blueprint dataset"
        );

        // Expunge the bundle's dataset. This marks it as "failed", and
        // is a prerequisite for the bundle not later being re-assigned.
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = BlueprintUuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            expunge_dataset_for_bundle(&mut bp2, &bundle);
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp2, nexus_ids[0])
            .await
            .expect("Should have been able to mark bundle state as failed");
        assert_eq!(
            SupportBundleExpungementReport {
                bundles_failed_missing_datasets: 1,
                ..Default::default()
            },
            report
        );

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just failed");
        assert_eq!(SupportBundleState::Failed, observed_bundle.state);
        assert!(
            observed_bundle
                .reason_for_failure
                .unwrap()
                .contains(FAILURE_REASON_NO_DATASET)
        );

        // Expunge the bundle's Nexus
        let bp3 = {
            let mut bp3 = bp2.clone();
            bp3.id = BlueprintUuid::new_v4();
            bp3.parent_blueprint_id = Some(bp2.id);
            expunge_nexus_for_bundle(&mut bp3, &bundle);
            bp3
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp3).await;

        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp3, nexus_ids[1])
            .await
            .expect("Should have been able to mark bundle state as failed");

        // Although the record for this bundle already exists, it is not
        // re-assigned, and the original reason for it failing (dataset loss) is
        // preserved.
        assert_eq!(SupportBundleExpungementReport::default(), report);

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we just failed");
        assert_eq!(SupportBundleState::Failed, observed_bundle.state);
        assert!(
            observed_bundle
                .reason_for_failure
                .unwrap()
                .contains(FAILURE_REASON_NO_DATASET)
        );

        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_delete(&opctx, &authz_bundle)
            .await
            .expect("Should have been able to delete support bundle");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bundle_failed_from_expunged_nexus_with_reassign() {
        static TEST_NAME: &str =
            "test_bundle_failed_from_expunged_nexus_with_reassign";
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut rng = SimRngState::from_seed(TEST_NAME);
        let (_example, mut bp1) = ExampleSystemBuilder::new_with_rng(
            &logctx.log,
            rng.next_system_rng(),
        )
        .build();

        bp1.parent_blueprint_id = None;
        bp_insert_and_make_target(&opctx, &datastore, &bp1).await;

        // Manually perform the equivalent of blueprint execution to populate
        // database records.
        let sleds = TestSled::new_from_blueprint(&bp1);
        for sled in &sleds {
            sled.create_database_records(&datastore, &opctx).await;
        }

        // Extract Nexus and Dataset information from the generated blueprint.
        let nexus_ids = get_in_service_nexuses_from_blueprint(&bp1);
        let debug_datasets = get_in_service_debug_datasets_from_blueprint(&bp1);
        assert!(!debug_datasets.is_empty());

        // When we create a bundle, it should exist on a dataset provisioned by
        // the blueprint.
        let bundle = datastore
            .support_bundle_create(&opctx, "for the test", nexus_ids[0], None)
            .await
            .expect("Should be able to create bundle");

        assert_eq!(bundle.state, SupportBundleState::Collecting);
        assert_eq!(bundle.assigned_nexus, Some(nexus_ids[0].into()));
        assert!(
            debug_datasets.contains(&DatasetUuid::from(bundle.dataset_id)),
            "Bundle should have been allocated from a blueprint dataset"
        );

        // Update the bundle's state.
        //
        // This is what we would do when we finish collecting, and
        // provisioned storage on a sled.
        let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Active,
            )
            .await
            .expect("Should have been able to update state");

        // Expunge the bundle's Nexus (manually)
        let bp2 = {
            let mut bp2 = bp1.clone();
            bp2.id = BlueprintUuid::new_v4();
            bp2.parent_blueprint_id = Some(bp1.id);
            expunge_nexus_for_bundle(&mut bp2, &bundle);
            bp2
        };
        bp_insert_and_make_target(&opctx, &datastore, &bp2).await;

        let report = datastore
            .support_bundle_fail_expunged(&opctx, &bp2, nexus_ids[1])
            .await
            .expect("Should have been able to mark bundle state as destroying");

        assert_eq!(
            SupportBundleExpungementReport {
                bundles_failing_missing_nexus: 0,
                bundles_reassigned: 1,
                ..Default::default()
            },
            report
        );

        let observed_bundle = datastore
            .support_bundle_get(&opctx, bundle.id.into())
            .await
            .expect("Should be able to get bundle we reassigned");
        assert_eq!(SupportBundleState::Active, observed_bundle.state);
        assert_eq!(
            observed_bundle.assigned_nexus.unwrap(),
            nexus_ids[1].into(),
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_bundle_list_time_ordering() {
        let logctx = dev::test_setup_log("test_bundle_list_time_ordering");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 3).await;
        let this_nexus_id = OmicronZoneUuid::new_v4();

        // Create multiple bundles with slight time delays to ensure different creation times
        let mut bundle_ids = Vec::new();
        let mut bundle_times = Vec::new();

        for _i in 0..3 {
            let bundle = datastore
                .support_bundle_create(
                    &opctx,
                    "Bundle for time ordering test",
                    this_nexus_id,
                    None,
                )
                .await
                .expect("Should be able to create bundle");
            bundle_ids.push(bundle.id);
            bundle_times.push(bundle.time_created);

            // Small delay to ensure different creation times
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // List bundles using time-based pagination
        let pagparams = DataPageParams::max_page();
        let observed_bundles = datastore
            .support_bundle_list(&opctx, &pagparams)
            .await
            .expect("Should be able to list bundles");

        assert_eq!(3, observed_bundles.len());

        // Verify bundles are ordered by creation time (ascending)
        for i in 0..observed_bundles.len() - 1 {
            assert!(
                observed_bundles[i].time_created
                    <= observed_bundles[i + 1].time_created,
                "Bundles should be ordered by creation time (ascending). Bundle at index {} has time {:?}, but bundle at index {} has time {:?}",
                i,
                observed_bundles[i].time_created,
                i + 1,
                observed_bundles[i + 1].time_created
            );
        }

        // Verify that the bundles are our created bundles
        let returned_ids: Vec<_> =
            observed_bundles.iter().map(|b| b.id).collect();
        for bundle_id in &bundle_ids {
            assert!(
                returned_ids.contains(bundle_id),
                "Bundle ID {:?} should be in the returned list",
                bundle_id
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests for support_bundle_auto_delete

    #[tokio::test]
    async fn test_auto_deletion_no_bundles() {
        let logctx = dev::test_setup_log("test_auto_deletion_no_bundles");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create 5 debug datasets (pools)
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        // Set config: 60% target_free (CEIL(5*60/100)=3), 0% min_keep
        // With 5 datasets, no bundles, free=5 >= 3, should delete no bundles
        datastore
            .support_bundle_config_set(&opctx, 60, 0)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 5);
        assert_eq!(result.active_bundles, 0);
        assert_eq!(result.free_datasets, 5);
        assert!(result.deleted_ids.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_enough_free_datasets() {
        let logctx =
            dev::test_setup_log("test_auto_deletion_enough_free_datasets");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 10 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 10).await;

        // Create 5 bundles, leaving 5 free
        for _ in 0..5 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            // Mark as active
            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");
        }

        // Set config: 30% target_free (CEIL(10*30/100)=3), 0% min_keep
        // With 10 datasets, 5 bundles, free=5 >= 3, should delete no bundles
        datastore
            .support_bundle_config_set(&opctx, 30, 0)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 10);
        assert_eq!(result.active_bundles, 5);
        assert_eq!(result.free_datasets, 5);
        assert!(result.deleted_ids.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_deletes_oldest_first() {
        let logctx =
            dev::test_setup_log("test_auto_deletion_deletes_oldest_first");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 5 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        // Create 5 bundles (all slots filled)
        let mut bundle_ids = Vec::new();
        for _ in 0..5 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            // Mark as active
            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");

            bundle_ids.push(bundle.id);

            // Small delay to ensure different creation times
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Set config: 40% target_free (CEIL(5*40/100)=2), 0% min_keep
        // With 5 datasets, 5 bundles, free=0 < 2, need to delete 2 bundles
        datastore
            .support_bundle_config_set(&opctx, 40, 0)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 5);
        assert_eq!(result.active_bundles, 5);
        assert_eq!(result.free_datasets, 0);
        assert_eq!(result.deleted_ids.len(), 2);

        // Verify the oldest bundles are selected (first two created)
        assert!(result.deleted_ids.contains(&bundle_ids[0].into()));
        assert!(result.deleted_ids.contains(&bundle_ids[1].into()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_respects_min_bundles_to_keep() {
        let logctx = dev::test_setup_log(
            "test_auto_deletion_respects_min_bundles_to_keep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 5 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        // Create 5 bundles (all slots filled)
        for _ in 0..5 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            // Mark as active
            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");
        }

        // Set config: 60% target_free (CEIL(5*60/100)=3), 80% min_keep (CEIL(5*80/100)=4)
        // With free=0, we'd want to delete 3 bundles
        // But min_keep=4 means we can only delete 1 (5-4=1)
        datastore
            .support_bundle_config_set(&opctx, 60, 80)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 5);
        assert_eq!(result.active_bundles, 5);
        assert_eq!(result.free_datasets, 0);
        // Can only delete 1 bundle due to min_bundles_to_keep constraint
        assert_eq!(result.deleted_ids.len(), 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_min_bundles_prevents_all_deletion() {
        let logctx = dev::test_setup_log(
            "test_auto_deletion_min_bundles_prevents_all_deletion",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 5 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        // Create 3 bundles
        for _ in 0..3 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            // Mark as active
            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");
        }

        // Set config: 100% target_free (CEIL(5*100/100)=5), 100% min_keep (CEIL(5*100/100)=5)
        // With free=2, we'd want to delete 3 bundles, but min_keep=5 > active=3
        // So we can't delete any
        datastore
            .support_bundle_config_set(&opctx, 100, 100)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 5);
        assert_eq!(result.active_bundles, 3);
        assert_eq!(result.free_datasets, 2);
        // min_keep (5) > active_bundles (3), so no deletion
        assert!(result.deleted_ids.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_only_selects_active_bundles() {
        let logctx = dev::test_setup_log(
            "test_auto_deletion_only_selects_active_bundles",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 5 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 5).await;

        // Create 3 bundles: 1 active, 1 collecting, 1 destroying
        let bundle1 = datastore
            .support_bundle_create(&opctx, "for tests", nexus_id, None)
            .await
            .expect("Should be able to create bundle");
        let authz_bundle1 = authz_support_bundle_from_id(bundle1.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle1,
                SupportBundleState::Active,
            )
            .await
            .expect("Should update state");

        // Second bundle stays in Collecting
        let _bundle2 = datastore
            .support_bundle_create(&opctx, "for tests", nexus_id, None)
            .await
            .expect("Should be able to create bundle");

        // Third bundle is Destroying
        let bundle3 = datastore
            .support_bundle_create(&opctx, "for tests", nexus_id, None)
            .await
            .expect("Should be able to create bundle");
        let authz_bundle3 = authz_support_bundle_from_id(bundle3.id.into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle3,
                SupportBundleState::Destroying,
            )
            .await
            .expect("Should update state");

        // Set config: 100% target_free (CEIL(5*100/100)=5), 0% min_keep
        // With 3 bundles (1 Active, 1 Collecting, 1 Destroying):
        // - used_count = 2 (Active + Collecting; Destroying not counted for deletion calc)
        // - free_datasets = 5 - 2 = 3
        // We should only delete Active bundles though
        datastore
            .support_bundle_config_set(&opctx, 100, 0)
            .await
            .expect("Should set config");

        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.total_datasets, 5);
        // Only 1 bundle is Active (candidates for deletion)
        assert_eq!(result.active_bundles, 1);
        // Free datasets: 5 total - 2 used (Active + Collecting) = 3
        // (Destroying bundles are not counted as they're already being freed)
        assert_eq!(result.free_datasets, 3);
        // We want 5 free but only have 3, so we want to delete 2
        // But we only have 1 Active bundle to delete
        assert_eq!(result.deleted_ids.len(), 1);
        assert_eq!(result.deleted_ids[0], bundle1.id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_verifies_state_transition() {
        let logctx =
            dev::test_setup_log("test_auto_deletion_verifies_state_transition");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 3 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 3).await;

        // Create 3 bundles
        let mut bundle_ids = Vec::new();
        for _ in 0..3 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            // Mark as active
            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");

            bundle_ids.push(bundle.id);
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Verify bundles start in Active state
        for id in &bundle_ids {
            let bundle = datastore
                .support_bundle_get(&opctx, (*id).into())
                .await
                .unwrap();
            assert_eq!(bundle.state, SupportBundleState::Active);
        }

        // Set config: 50% target_free → CEIL(3*50/100)=2, 0% min_keep
        datastore
            .support_bundle_config_set(&opctx, 50, 0)
            .await
            .expect("Should set config");

        // With target_free=2 (from 50% of 3 datasets) and free=0, delete 2 bundles
        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.deleted_ids.len(), 2);

        // Verify deleted bundles are now in Destroying state
        for id in &result.deleted_ids {
            let bundle =
                datastore.support_bundle_get(&opctx, *id).await.unwrap();
            assert_eq!(
                bundle.state,
                SupportBundleState::Destroying,
                "Bundle {} should be Destroying",
                id
            );
        }

        // Verify the remaining bundle is still Active
        let remaining_id = bundle_ids
            .iter()
            .find(|id| {
                !result.deleted_ids.contains(&SupportBundleUuid::from(**id))
            })
            .unwrap();
        let remaining_bundle = datastore
            .support_bundle_get(&opctx, (*remaining_id).into())
            .await
            .unwrap();
        assert_eq!(remaining_bundle.state, SupportBundleState::Active);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_auto_deletion_failed_bundles_dont_occupy_datasets() {
        let logctx = dev::test_setup_log(
            "test_auto_deletion_failed_bundles_dont_occupy_datasets",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 3 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 3).await;

        // Create 3 bundles, all Active
        let mut bundle_ids = Vec::new();
        for _ in 0..3 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");

            bundle_ids.push(bundle.id);
        }

        // Set config: 20% target_free → CEIL(3*20/100)=1, 0% min_keep
        datastore
            .support_bundle_config_set(&opctx, 20, 0)
            .await
            .expect("Should set config");

        // All 3 datasets are used, free=0
        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        assert_eq!(result.free_datasets, 0);
        assert_eq!(result.deleted_ids.len(), 1);

        // Manually mark one bundle as Failed (simulating dataset expungement)
        // Failed bundles don't occupy datasets
        let authz_bundle = authz_support_bundle_from_id(bundle_ids[1].into());
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failing,
            )
            .await
            .expect("Should update state");
        datastore
            .support_bundle_update(
                &opctx,
                &authz_bundle,
                SupportBundleState::Failed,
            )
            .await
            .expect("Should update state");

        // Now we have: 1 Destroying, 1 Failed, 1 Active
        // - Total datasets: 3
        // - Used for deletion calc: 1 (only Active; Destroying not counted)
        // - Free datasets: 3 - 1 = 2
        // Config still: 20% target_free (CEIL(3*20/100)=1), 0% min_keep
        let result = datastore
            .support_bundle_auto_delete(&opctx)
            .await
            .expect("Should succeed");

        // Free should be 2 now:
        // - Failed bundle doesn't count (dataset was expunged)
        // - Destroying bundle doesn't count (already being freed)
        assert_eq!(result.free_datasets, 2);
        // Since free=2 >= target=1, no deletion needed
        assert!(result.deleted_ids.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests for the auto_delete CTE query

    #[tokio::test]
    async fn test_auto_delete_query_explains() {
        use crate::db::explain::ExplainableAsync;
        use crate::db::pub_test_utils::TestDatabase;

        let logctx = dev::test_setup_log("test_auto_delete_query_explains");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let conn = db.pool().claim().await.unwrap();

        let query = support_bundle_auto_delete_query();

        let _ = query.explain_async(&conn).await.unwrap_or_else(|e| {
            panic!("Failed to explain query, is it valid SQL?\nerror: {e:#?}")
        });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_auto_delete_query() {
        use crate::db::raw_query_builder::expectorate_query_contents;

        let query = support_bundle_auto_delete_query();
        expectorate_query_contents(
            query,
            "tests/output/support_bundle_auto_delete.sql",
        )
        .await;
    }

    /// Test that concurrent auto-deletion operations don't over-delete bundles.
    ///
    /// This verifies the atomic CTE prevents the TOCTTOU issue where multiple
    /// Nexuses running concurrently could each decide to delete bundles based
    /// on stale state, resulting in more deletions than intended.
    #[tokio::test]
    async fn test_auto_deletion_concurrent_execution_prevents_over_deletion() {
        let logctx = dev::test_setup_log(
            "test_auto_deletion_concurrent_execution_prevents_over_deletion",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let nexus_id = OmicronZoneUuid::new_v4();

        // Create 10 debug datasets
        let _test_sled = create_sled_and_zpools(&datastore, &opctx, 10).await;

        // Create 10 bundles (all slots filled)
        for _ in 0..10 {
            let bundle = datastore
                .support_bundle_create(&opctx, "for tests", nexus_id, None)
                .await
                .expect("Should be able to create bundle");

            let authz_bundle = authz_support_bundle_from_id(bundle.id.into());
            datastore
                .support_bundle_update(
                    &opctx,
                    &authz_bundle,
                    SupportBundleState::Active,
                )
                .await
                .expect("Should update state");
        }

        // Set config: 20% target_free (CEIL(10*20/100)=2), 0% min_keep
        // With 10 datasets, 10 bundles, free=0 < 2, need to delete 2 bundles
        datastore
            .support_bundle_config_set(&opctx, 20, 0)
            .await
            .expect("Should set config");

        // Spawn multiple concurrent auto-delete operations.
        // Without the atomic CTE, each would see free=0 and try to delete 2,
        // potentially resulting in 10 deletions (5 tasks × 2 each).
        // With the atomic CTE, only the first operation(s) should delete,
        // and subsequent ones should see the updated state.
        let num_concurrent_tasks = 5;
        let mut handles = Vec::new();

        for _ in 0..num_concurrent_tasks {
            let datastore = datastore.clone();
            let opctx = opctx.child(std::collections::BTreeMap::new());
            handles.push(tokio::spawn(async move {
                datastore.support_bundle_auto_delete(&opctx).await
            }));
        }

        // Collect all results
        let mut total_deleted = 0;
        for handle in handles {
            let result = handle.await.expect("Task should complete");
            let result = result.expect("Auto-delete should succeed");
            total_deleted += result.deleted_ids.len();
        }

        // The key assertion: we should have deleted exactly 2 bundles total,
        // not 2 × num_concurrent_tasks. The atomic CTE ensures that once
        // bundles are transitioned to Destroying, they're no longer candidates
        // for other concurrent operations (the UPDATE's WHERE state='active'
        // clause filters them out).
        assert_eq!(
            total_deleted, 2,
            "Should delete exactly 2 bundles total across all concurrent \
             operations, not {} (which would indicate over-deletion)",
            total_deleted
        );

        // Verify the final state: 8 Active bundles remain
        use nexus_db_schema::schema::support_bundle::dsl;
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let active_count: i64 = dsl::support_bundle
            .filter(dsl::state.eq(SupportBundleState::Active))
            .count()
            .get_result_async(&*conn)
            .await
            .expect("Should count active bundles");

        assert_eq!(
            active_count, 8,
            "Should have 8 Active bundles remaining after concurrent deletion"
        );

        // Verify we now have 2 Destroying bundles
        let destroying_count: i64 = dsl::support_bundle
            .filter(dsl::state.eq(SupportBundleState::Destroying))
            .count()
            .get_result_async(&*conn)
            .await
            .expect("Should count destroying bundles");

        assert_eq!(
            destroying_count, 2,
            "Should have exactly 2 bundles in Destroying state"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_config_set_rejects_invalid_target_free_percent() {
        let logctx = dev::test_setup_log(
            "test_config_set_rejects_invalid_target_free_percent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // 101% should be rejected
        let result = datastore.support_bundle_config_set(&opctx, 101, 10).await;

        assert!(
            result.is_err(),
            "Setting target_free_percent > 100 should fail"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("target_free_percent"),
            "Error message should mention target_free_percent: {}",
            err
        );

        // Verify valid values still work
        datastore
            .support_bundle_config_set(&opctx, 100, 10)
            .await
            .expect("100% should be valid");

        datastore
            .support_bundle_config_set(&opctx, 0, 10)
            .await
            .expect("0% should be valid");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_config_set_rejects_invalid_min_keep_percent() {
        let logctx = dev::test_setup_log(
            "test_config_set_rejects_invalid_min_keep_percent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // 101% should be rejected
        let result = datastore.support_bundle_config_set(&opctx, 10, 101).await;

        assert!(result.is_err(), "Setting min_keep_percent > 100 should fail");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("min_keep_percent"),
            "Error message should mention min_keep_percent: {}",
            err
        );

        // Verify valid values still work
        datastore
            .support_bundle_config_set(&opctx, 10, 100)
            .await
            .expect("100% should be valid");

        datastore
            .support_bundle_config_set(&opctx, 10, 0)
            .await
            .expect("0% should be valid");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
