// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for alerts and alert delivery dispatching.

use super::DataStore;
use super::RunnableQuery;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::Alert;
use crate::db::sitrep_guard::SitrepGuardedInsert;
use crate::db::sitrep_guard::SitrepGuardedInsertOutcome;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::dsl::AsSelect;
use diesel::dsl::SqlTypeOf;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::Query;
use diesel::result::OptionalExtension;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::alert::dsl as alert_dsl;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::{AlertUuid, GenericUuid};
use std::collections::HashSet;
use uuid::Uuid;

impl DataStore {
    fn alert_insert_query(
        alert: Alert,
    ) -> impl RunnableQuery<Alert> + Query<SqlType = SqlTypeOf<AsSelect<Alert, Pg>>>
    {
        diesel::insert_into(alert_dsl::alert)
            .values(alert)
            .on_conflict_do_nothing()
            .returning(Alert::as_returning())
    }

    /// Insert an alert row, returning the inserted alert on success.
    ///
    /// If a row with this alert's id already exists, returns
    /// [`Error::ObjectAlreadyExists`]. This isn't really an error: multiple
    /// Nexus instances may race to insert the same alert. The caller decides
    /// how to treat it.
    pub async fn alert_create(
        &self,
        opctx: &OpContext,
        alert: Alert,
    ) -> CreateResult<Alert> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let alert_id = alert.id();

        let inserted: Option<Alert> = Self::alert_insert_query(alert)
            .get_result_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let alert = inserted.ok_or_else(|| Error::ObjectAlreadyExists {
            type_name: ResourceType::Alert,
            object_name: alert_id.to_string(),
        })?;

        slog::debug!(
            &opctx.log,
            "published alert";
            "alert_id" => ?alert.id(),
            "alert_class" => %alert.class,
            "alert_case_id" => ?alert.case_id,
            "time_created" => ?alert.identity.time_created,
        );

        Ok(alert)
    }

    /// Insert an alert requested by FM rendezvous, guarded against stale-sitrep
    /// execution.
    ///
    /// Like [`Self::alert_create`], but the insert is wrapped in a
    /// [`SitrepGuardedInsert`]: it writes a `rendezvous_alert_created` marker
    /// so a deleted alert is not resurrected by a later rendezvous pass, and
    /// it is gated on the executing sitrep still being current. If the latest
    /// sitrep's `alert_generation` has advanced past `expected_alert_generation`,
    /// the sitrep being executed is stale and this returns [`Error::Conflict`].
    pub async fn fm_rendezvous_alert_create(
        &self,
        opctx: &OpContext,
        alert: Alert,
        expected_alert_generation: Generation,
    ) -> CreateResult<Alert> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let alert_id = alert.id();

        let guarded = SitrepGuardedInsert::<Alert, _>::new(
            alert_id.into_untyped_uuid(),
            expected_alert_generation.into(),
            Self::alert_insert_query(alert),
        );

        let alert =
            match guarded.execute_async(&conn).await.map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
            })? {
                SitrepGuardedInsertOutcome::Created(row) => row,
                SitrepGuardedInsertOutcome::AlreadyExists => {
                    return Err(Error::ObjectAlreadyExists {
                        type_name: ResourceType::Alert,
                        object_name: alert_id.to_string(),
                    });
                }
                SitrepGuardedInsertOutcome::StaleSitrep => {
                    // We signal stale sitrep to the caller as a `Conflict` error.
                    // This is unambiguous; no other path produces `Conflict`.
                    return Err(Error::conflict(
                        "cannot create alert for stale sitrep",
                    ));
                }
            };

        slog::debug!(
            &opctx.log,
            "published alert for FM alert request";
            "alert_id" => ?alert.id(),
            "alert_class" => %alert.class,
            "alert_case_id" => ?alert.case_id,
            "time_created" => ?alert.identity.time_created,
        );

        Ok(alert)
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

    /// Given a set of alert ids, returns those that have a creation marker row
    /// in the FM `rendezvous_alert_created` table (i.e. the alerts that FM
    /// rendezvous has already created). Used by FM analysis to tell which alert
    /// requests on closed cases have been satisfied, to determine whether any
    /// cases can be dropped (see `nexus_fm::analysis_input::Builder::build`).
    pub async fn fm_rendezvous_existing_alert_markers(
        &self,
        opctx: &OpContext,
        candidates: &[AlertUuid],
    ) -> Result<HashSet<AlertUuid>, Error> {
        if candidates.is_empty() {
            return Ok(HashSet::new());
        }
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::rendezvous_alert_created::dsl as marker_dsl;

        let candidate_uuids: Vec<Uuid> =
            candidates.iter().map(|id| id.into_untyped_uuid()).collect();

        let rows: Vec<Uuid> = marker_dsl::rendezvous_alert_created
            .filter(marker_dsl::alert_id.eq_any(candidate_uuids))
            .select(marker_dsl::alert_id)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows.into_iter().map(AlertUuid::from_untyped_uuid).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use chrono::Utc;
    use nexus_db_model::fm::RendezvousAlertCreated;
    use nexus_db_schema::schema::rendezvous_alert_created::dsl as alert_marker_dsl;
    use nexus_types::alert::test_alerts;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use serde_json::json;

    fn make_sitrep(alert_generation: Generation) -> Sitrep {
        Sitrep {
            metadata: SitrepMetadata {
                id: SitrepUuid::new_v4(),
                inv_collection_id: CollectionUuid::new_v4(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: String::new(),
                time_created: Utc::now(),
                parent_sitrep_id: None,
                next_inv_min_time_started: Utc::now(),
                alert_generation,
                support_bundle_generation: Generation::new(),
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
        }
    }

    fn make_fm_alert(alert_id: AlertUuid, case_id: CaseUuid) -> Alert {
        let mut alert =
            Alert::new(alert_id, &test_alerts::Foo(json!({}))).unwrap();
        alert.case_id = Some(case_id.into());
        alert
    }

    #[tokio::test]
    async fn alert_create_then_already_exists() {
        let logctx = dev::test_setup_log("alert_create_then_already_exists");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let alert =
            Alert::new(AlertUuid::new_v4(), &test_alerts::Foo(json!({})))
                .unwrap();
        let alert_id = alert.id();

        let inserted =
            datastore.alert_create(opctx, alert.clone()).await.unwrap();
        assert_eq!(inserted.id(), alert_id);
        assert!(inserted.case_id.is_none());

        let err = datastore.alert_create(opctx, alert).await.unwrap_err();
        assert!(
            matches!(err, Error::ObjectAlreadyExists { .. }),
            "expected ObjectAlreadyExists, got {err:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_alert_create_writes_marker() {
        let logctx =
            dev::test_setup_log("fm_rendezvous_alert_create_writes_marker");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(1)))
            .await
            .unwrap();

        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let inserted = datastore
            .fm_rendezvous_alert_create(
                opctx,
                make_fm_alert(alert_id, case_id),
                Generation::from_u32(1),
            )
            .await
            .unwrap();
        assert_eq!(inserted.id(), alert_id);
        assert_eq!(inserted.case_id.map(Into::into), Some(case_id));

        let marker = alert_marker_dsl::rendezvous_alert_created
            .filter(alert_marker_dsl::alert_id.eq(alert_id.into_untyped_uuid()))
            .select(RendezvousAlertCreated::as_select())
            .first_async::<RendezvousAlertCreated>(&*conn)
            .await
            .unwrap();
        assert_eq!(
            marker.created_at_generation,
            nexus_db_model::Generation::new()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_alert_create_already_exists_via_marker() {
        let logctx = dev::test_setup_log(
            "fm_rendezvous_alert_create_already_exists_via_marker",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(2)))
            .await
            .unwrap();

        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();

        // Pre-write the marker at an earlier generation. The combinator
        // should short-circuit via `prior_marker_guard` and surface
        // ObjectAlreadyExists without inserting the alert row.
        diesel::insert_into(alert_marker_dsl::rendezvous_alert_created)
            .values(RendezvousAlertCreated::new(
                alert_id,
                nexus_db_model::Generation::new(),
            ))
            .execute_async(&*conn)
            .await
            .unwrap();

        let err = datastore
            .fm_rendezvous_alert_create(
                opctx,
                make_fm_alert(alert_id, case_id),
                Generation::from_u32(2),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::ObjectAlreadyExists { .. }),
            "expected ObjectAlreadyExists, got {err:?}"
        );

        // No alert row should have been written.
        let count: i64 = alert_dsl::alert
            .filter(alert_dsl::id.eq(alert_id.into_untyped_uuid()))
            .count()
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(count, 0);

        // The pre-existing marker must not have been overwritten with the
        // current `expected_alert_generation`. The combinator's marker
        // INSERT uses `ON CONFLICT DO NOTHING`; if a future change flipped
        // it to UPSERT (or dropped the conflict clause), the marker's
        // `created_at_generation` would silently advance from 1 to 2,
        // breaking the GC invariant that markers record the generation at
        // which the alert was originally created.
        let marker = alert_marker_dsl::rendezvous_alert_created
            .filter(alert_marker_dsl::alert_id.eq(alert_id.into_untyped_uuid()))
            .select(RendezvousAlertCreated::as_select())
            .first_async::<RendezvousAlertCreated>(&*conn)
            .await
            .unwrap();
        assert_eq!(
            marker.created_at_generation,
            nexus_db_model::Generation::new()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Regression test: an alert row exists without a corresponding marker
    // (pre-migration data, or marker GC outrunning alert deletion). The
    // guarded insert must surface `ObjectAlreadyExists` rather than letting
    // a bare `DieselError::NotFound` bubble out as HTTP 500, AND must NOT
    // fabricate a marker row for an alert this executor did not produce.
    #[tokio::test]
    async fn fm_rendezvous_alert_create_already_exists_without_marker() {
        let logctx = dev::test_setup_log(
            "fm_rendezvous_alert_create_already_exists_without_marker",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(1)))
            .await
            .unwrap();

        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();

        // Plant the alert row directly so no marker exists for it.
        diesel::insert_into(alert_dsl::alert)
            .values(make_fm_alert(alert_id, case_id))
            .execute_async(&*conn)
            .await
            .unwrap();

        let err = datastore
            .fm_rendezvous_alert_create(
                opctx,
                make_fm_alert(alert_id, case_id),
                Generation::from_u32(1),
            )
            .await
            .unwrap_err();
        assert_matches!(err, Error::ObjectAlreadyExists { .. });

        // No marker may have been written: the `new_marker` CTE is gated
        // by `WHERE EXISTS (SELECT 1 FROM new_resource)`.
        let marker_count: i64 = alert_marker_dsl::rendezvous_alert_created
            .filter(alert_marker_dsl::alert_id.eq(alert_id.into_untyped_uuid()))
            .count()
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(marker_count, 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_alert_create_stale_sitrep() {
        let logctx =
            dev::test_setup_log("fm_rendezvous_alert_create_stale_sitrep");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Latest sitrep is at generation 5; executor expects 1.
        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(5)))
            .await
            .unwrap();

        let err = datastore
            .fm_rendezvous_alert_create(
                opctx,
                make_fm_alert(AlertUuid::new_v4(), CaseUuid::new_v4()),
                Generation::new(),
            )
            .await
            .unwrap_err();
        assert_matches!(
            err,
            Error::Conflict { ref message }
                if message.external_message().contains("stale sitrep")
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_existing_alert_markers_returns_only_present_ids() {
        let logctx = dev::test_setup_log(
            "fm_rendezvous_existing_alert_markers_returns_only_present_ids",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let present_a = AlertUuid::new_v4();
        let present_b = AlertUuid::new_v4();
        let absent = AlertUuid::new_v4();

        // Insert markers for present_a and present_b only.
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::insert_into(alert_marker_dsl::rendezvous_alert_created)
                .values(vec![
                    RendezvousAlertCreated::new(
                        present_a,
                        nexus_db_model::Generation::new(),
                    ),
                    RendezvousAlertCreated::new(
                        present_b,
                        nexus_db_model::Generation::new(),
                    ),
                ])
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        let candidates = vec![present_a, absent, present_b];
        let existing = datastore
            .fm_rendezvous_existing_alert_markers(opctx, &candidates)
            .await
            .expect("query should succeed");

        assert_eq!(existing.len(), 2);
        assert!(existing.contains(&present_a));
        assert!(existing.contains(&present_b));
        assert!(!existing.contains(&absent));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_existing_alert_markers_empty_input_returns_empty() {
        let logctx = dev::test_setup_log(
            "fm_rendezvous_existing_alert_markers_empty_input_returns_empty",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let existing = datastore
            .fm_rendezvous_existing_alert_markers(opctx, &[])
            .await
            .expect("empty input must return Ok");

        assert!(existing.is_empty(), "empty input returns empty result");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fm_rendezvous_existing_alert_markers_explain_no_full_scan() {
        use crate::db::explain::ExplainableAsync;

        let logctx = dev::test_setup_log(
            "fm_rendezvous_existing_alert_markers_explain_no_full_scan",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let candidates: Vec<Uuid> =
            (0..3).map(|_| AlertUuid::new_v4().into_untyped_uuid()).collect();
        let query = alert_marker_dsl::rendezvous_alert_created
            .filter(alert_marker_dsl::alert_id.eq_any(candidates))
            .select(alert_marker_dsl::alert_id);

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("query should be valid SQL");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
