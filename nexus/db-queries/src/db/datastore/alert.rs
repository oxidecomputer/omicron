// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for alerts and alert delivery dispatching.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::Alert;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::alert::dsl as alert_dsl;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::{AlertUuid, GenericUuid};

impl DataStore {
    /// Insert an alert row, returning the inserted alert on success.
    ///
    /// If a row with this alert's id already exists, this returns
    /// [`Error::ObjectAlreadyExists`]. This isn't _really_ an error: in normal
    /// operation, multiple Nexus instances do race to insert the same alert,
    /// and -- in the case of FM-requested alerts -- as long as any sitrep
    /// requesting the given alert is still current, every periodic execution of
    /// FM rendezvous will continue to attempt to recreate the same alert.
    /// Omicron convention is to signal this idempotent "I didn't do anything"
    /// case to the caller as an error, and the caller can decide how to treat
    /// it.
    ///
    /// When `fm_sitrep_version` is `Some`, the insert is wrapped in
    /// [`SitrepVersionGuardedInsert`] so it only succeeds while the rendezvous
    /// progress tracker has not yet moved past `fm_sitrep_version`. If the
    /// tracker is ahead, returns [`Error::Conflict`] with a `"stale sitrep"`
    /// message, and the caller should treat the request as skipped (not
    /// retried).
    ///
    /// [`SitrepVersionGuardedInsert`]:
    ///     crate::db::sitrep_version_guard::SitrepVersionGuardedInsert
    pub async fn alert_create(
        &self,
        opctx: &OpContext,
        alert: Alert,
        fm_sitrep_version: Option<i64>,
    ) -> CreateResult<Alert> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let alert_id = alert.id();
        let insert = diesel::insert_into(alert_dsl::alert)
            .values(alert)
            .on_conflict_do_nothing()
            .returning(Alert::as_returning());

        let inserted = match fm_sitrep_version {
            None => insert.get_result_async(&*conn).await.optional().map_err(
                |e| public_error_from_diesel(e, ErrorHandler::Server),
            )?,
            Some(v) => {
                use crate::db::sitrep_version_guard::{
                    GuardedInsertError, SitrepVersionGuardedInsert,
                };
                SitrepVersionGuardedInsert::new(insert, v)
                    .insert_and_get_optional_result_async(&conn)
                    .await
                    .map_err(|err| match err {
                        GuardedInsertError::StaleSitrep => Error::conflict(
                            "cannot create alert for stale sitrep",
                        ),
                        GuardedInsertError::DatabaseError(e) => {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        }
                    })?
            }
        };

        inserted.ok_or_else(|| Error::ObjectAlreadyExists {
            type_name: ResourceType::Alert,
            object_name: alert_id.to_string(),
        })
    }

    pub async fn alert_delete(
        &self,
        opctx: &OpContext,
        id: AlertUuid,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let id = id.into_untyped_uuid();

        // First, try to soft-delete by setting `time_deleted` for a row
        // matching the given alert id with a non-null `case_id`, meaning it was
        // created by fault management. FM rendezvous will eventually
        // garbage-collect this when it's safe (see `actually_activate` and
        // `fm_tombstone_sweep`).
        let soft_rows = diesel::update(alert_dsl::alert)
            .filter(alert_dsl::id.eq(id))
            .filter(alert_dsl::case_id.is_not_null())
            .filter(alert_dsl::time_deleted.is_null())
            .set(alert_dsl::time_deleted.eq(diesel::dsl::now))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // If no rows were deleted above, we try hard-deleting instead, taking
        // care not to hard-delete any previously soft-deleted rows (or, more
        // subtly, any FM-created rows that were inserted after the above
        // statement). Note, we can't combine both attempts into a single CTE:
        // CRDB rejects that by default
        // (`sql.multiple_modifications_of_table.enabled = false`).
        if soft_rows == 0 {
            diesel::delete(alert_dsl::alert)
                .filter(alert_dsl::id.eq(id))
                .filter(alert_dsl::case_id.is_null())
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        Ok(())
    }

    pub async fn alert_select_next_for_dispatch(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<Alert>, Error> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        alert_dsl::alert
            .filter(alert_dsl::time_dispatched.is_null())
            .order_by(alert_dsl::time_created.asc())
            .select(Alert::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn alert_mark_dispatched(
        &self,
        opctx: &OpContext,
        alert_id: &AlertUuid,
        subscribed: usize,
    ) -> UpdateResult<usize> {
        let subscribed = i64::try_from(subscribed).map_err(|_| {
            // that is way too many alert receivers!
            Error::internal_error("alert subscribed count exceeds i64::MAX")
        })?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        diesel::update(alert_dsl::alert)
            .filter(alert_dsl::id.eq(alert_id.into_untyped_uuid()))
            .filter(
                // Update the alert record if one of the following is true:
                // - The `time_dispatched`` field has not already been set, or
                // - `time_dispatched` IS set, but `num_dispatched` is less than
                //   the number of deliveries we believe has been dispatched.
                //   This may be the case if a webhook receiver which is
                //   subscribed to this alert was added concurrently with
                //   another Nexus' dispatching the alert, and we dispatched the
                //   alert to that receiver but the other Nexus did not. In that
                //   case, we would like to update the record to indicate the
                //   correct number of subscribers.
                alert_dsl::time_dispatched
                    .is_null()
                    .or(alert_dsl::num_dispatched.le(subscribed)),
            )
            .set((
                alert_dsl::time_dispatched.eq(diesel::dsl::now),
                alert_dsl::num_dispatched.eq(subscribed),
            ))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use diesel::result::OptionalExtension;
    use nexus_db_model::Alert;
    use nexus_db_model::DbTypedUuid;
    use nexus_db_schema::schema::alert::dsl;
    use nexus_types::alert::AlertClass;
    use nexus_types::identity::Asset;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::CaseKind;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::GenericUuid;
    use serde_json::json;

    /// Fetch an alert row by id, ignoring `time_deleted`.
    ///
    /// Used by tests that need to inspect tombstoned rows that
    /// `alert_create`/normal selects would filter out.
    async fn test_alert_raw(
        datastore: &DataStore,
        id: AlertUuid,
    ) -> Option<Alert> {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        dsl::alert
            .filter(dsl::id.eq(id.into_untyped_uuid()))
            .select(Alert::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .unwrap()
    }

    #[tokio::test]
    async fn test_alert_time_deleted_roundtrip() {
        let logctx = dev::test_setup_log("test_alert_time_deleted_roundtrip");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let alert =
            Alert::new(AlertUuid::new_v4(), AlertClass::TestFoo, json!({}));
        let alert_id = alert.id().into_untyped_uuid();
        diesel::insert_into(dsl::alert)
            .values(alert)
            .execute_async(&*conn)
            .await
            .unwrap();

        diesel::update(dsl::alert)
            .filter(dsl::id.eq(alert_id))
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .unwrap();

        let fetched: Alert = dsl::alert
            .filter(dsl::id.eq(alert_id))
            .select(Alert::as_select())
            .first_async(&*conn)
            .await
            .unwrap();
        assert!(
            fetched.time_deleted.is_some(),
            "time_deleted should round-trip as Some"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_alert_create_non_fm_path() {
        let logctx = dev::test_setup_log("test_alert_create_non_fm_path");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let alert = nexus_db_model::Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            json!({}),
        );
        let _ =
            datastore.alert_create(opctx, alert.clone(), None).await.unwrap();

        let err = datastore
            .alert_create(opctx, alert, None)
            .await
            .expect_err("second insert should fail with ObjectAlreadyExists");
        assert!(
            matches!(err, Error::ObjectAlreadyExists { .. }),
            "expected ObjectAlreadyExists, got {err:?}",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_alert_create_stale_sitrep() {
        let logctx = dev::test_setup_log("test_alert_create_stale_sitrep");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        datastore.fm_rendezvous_progress_advance(opctx, 10).await.unwrap();

        let alert =
            Alert::new(AlertUuid::new_v4(), AlertClass::TestFoo, json!({}));
        let err = datastore
            .alert_create(opctx, alert, Some(5))
            .await
            .expect_err("stale sitrep should yield Err(Conflict)");
        match err {
            Error::Conflict { ref message }
                if message.external_message().contains("stale sitrep") => {}
            other => {
                panic!("expected Conflict with stale sitrep, got {other:?}")
            }
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_alert_delete_branches_on_case_id() {
        let logctx =
            dev::test_setup_log("test_alert_delete_branches_on_case_id");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Non-FM alert -> hard delete.
        let alert =
            Alert::new(AlertUuid::new_v4(), AlertClass::TestFoo, json!({}));
        datastore.alert_create(opctx, alert.clone(), None).await.unwrap();
        datastore.alert_delete(opctx, alert.id()).await.unwrap();
        assert!(test_alert_raw(datastore, alert.id()).await.is_none());

        // FM alert -> soft delete.
        let case_id: DbTypedUuid<CaseKind> = CaseUuid::new_v4().into();
        let mut fm_alert =
            Alert::new(AlertUuid::new_v4(), AlertClass::TestFoo, json!({}));
        fm_alert.case_id = Some(case_id);
        datastore.alert_create(opctx, fm_alert.clone(), None).await.unwrap();
        datastore.alert_delete(opctx, fm_alert.id()).await.unwrap();
        let row = test_alert_raw(datastore, fm_alert.id()).await.unwrap();
        let first_tombstone = row
            .time_deleted
            .expect("FM alert should be tombstoned after first delete");

        // Re-deleting an already-soft-deleted FM alert must be a no-op: the row
        // stays, and `time_deleted` stays at its original value.
        datastore.alert_delete(opctx, fm_alert.id()).await.unwrap();
        let row = test_alert_raw(datastore, fm_alert.id())
            .await
            .expect("soft-deleted FM alert must not be hard-deleted");
        assert_eq!(
            row.time_deleted,
            Some(first_tombstone),
            "second delete must not change time_deleted",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
