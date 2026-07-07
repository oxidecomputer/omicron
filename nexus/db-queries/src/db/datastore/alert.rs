// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for alerts and alert delivery dispatching.

use super::DataStore;
use super::RunnableQuery;
use crate::authz;
use crate::context::OpContext;
use crate::db::model;
use crate::db::model::Alert;
use crate::db::model::DbTypedUuid;
use crate::db::model::fm;
use crate::db::pagination::paginated_multicolumn;
use crate::db::sitrep_guard::SitrepGuardedInsert;
use crate::db::sitrep_guard::SitrepGuardedInsertOutcome;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::dsl::AsSelect;
use diesel::dsl::SqlTypeOf;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::Query;
use diesel::result::OptionalExtension;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::alert::dsl as alert_dsl;
use nexus_types::fm::case::AlertRequest;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::CaseKind;
use omicron_uuid_kinds::{AlertUuid, CaseUuid, GenericUuid};
use std::collections::HashSet;
use uuid::Uuid;

/// Error returned by [`DataStore::fm_rendezvous_alert_create`].
///
/// Note that the non-FM [`DataStore::alert_create`] just returns an external
/// [`Error`] as it's intended to be called from the API and doesn't have or
/// need any of FM's sitrep-guarded insert machinery. In contrast, this error
/// type signals what fm_rendezvous needs for its control flow and metrics.
#[derive(Debug, thiserror::Error)]
pub enum FmRendezvousAlertCreateError {
    /// An alert with this id was already created by an earlier rendezvous
    /// activation; the `rendezvous_alert_created` marker short-circuited the
    /// insert.
    #[error("alert was already created by a previous rendezvous activation")]
    AlreadyCreated,
    /// The sitrep being executed is stale: its `alert_generation` is behind the
    /// current sitrep in the database, so no alert was created.
    #[error("cannot create alert for a stale sitrep")]
    StaleSitrep,
    /// An error occurred while accessing the database.
    #[error(transparent)]
    Database(#[from] Error),
}

#[derive(Debug, Default)]
pub struct AlertFilters {
    /// Include only alerts created before this timestamp
    before: Option<DateTime<Utc>>,

    /// Include only alerts created after this timestamp
    after: Option<DateTime<Utc>>,

    /// Include only alerts fully dispatched before this timestamp
    dispatched_before: Option<DateTime<Utc>>,

    /// Include only alerts fully dispatched after this timestamp
    dispatched_after: Option<DateTime<Utc>>,

    /// Include only alerts requested by the fault management case(s) with the
    /// specified UUIDs.
    ///
    /// If multiple case IDs are provided, alerts requested by any of those
    /// cases will be included in the output.
    ///
    /// Note that not all alerts are requested by fault management cases.
    cases: Vec<DbTypedUuid<CaseKind>>,

    /// Include only alerts with the specified alert classes.
    classes: Vec<model::AlertClass>,

    /// If `true`, include only alerts that have been fully dispatched.
    /// If `false`, include only alerts that have not been fully dispatched.
    ///
    /// If this argument is not provided, both dispatched and un-dispatched
    /// events are included.
    dispatched: Option<bool>,
}

impl AlertFilters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Includes only alerts created at or after `time`
    ///
    /// Returns an error if `time` is after a previously set `before` time.
    pub fn after(mut self, time: DateTime<Utc>) -> Result<Self, Error> {
        if let Some(before) = self.before {
            if time > before {
                return Err(Error::invalid_request(
                    "`after` timestamp must be earlier than `before` \
                    timestamp",
                ));
            }
        }
        self.after = Some(time);
        Ok(self)
    }

    /// Includes only alerts created at or before `time`.
    ///
    /// Returns an error if `time` is before a previously set `after` time.
    pub fn before(mut self, time: DateTime<Utc>) -> Result<Self, Error> {
        if let Some(after) = self.after {
            if after > time {
                return Err(Error::invalid_request(
                    "`before` timestamp must be later than `after` \
                    timestamp",
                ));
            }
        }
        self.before = Some(time);
        Ok(self)
    }

    /// Includes only alerts dispatched at or after `time`
    ///
    /// Returns an error if `time` is after a previously set `dispatched_before`
    /// time.
    pub fn dispatched_after(
        mut self,
        time: DateTime<Utc>,
    ) -> Result<Self, Error> {
        if let Some(false) = self.dispatched {
            return Err(Error::invalid_request(
                "cannot filter by `dispatched_after` when querying \
                 for undispatched alerts (i.e. when `dispatched` is `false`)",
            ));
        }
        if let Some(before) = self.dispatched_before {
            if time > before {
                return Err(Error::invalid_request(
                    "`dispatched_after` timestamp must be earlier \
                    than `dispatched_before` timestamp",
                ));
            }
        }
        self.dispatched_after = Some(time);
        Ok(self)
    }

    /// Includes only alerts dispatched at or before `time`.
    ///
    /// Returns an error if `time` is before a previously set `dispatched_after`
    /// time.
    pub fn dispatched_before(
        mut self,
        time: DateTime<Utc>,
    ) -> Result<Self, Error> {
        if let Some(false) = self.dispatched {
            return Err(Error::invalid_request(
                "cannot filter by `dispatched_before` when querying \
                 for undispatched alerts (i.e. when `dispatched` is `false`)",
            ));
        }
        if let Some(after) = self.dispatched_after {
            if after > time {
                return Err(Error::invalid_request(
                    "`dispatched_before` timestamp must be later \
                    than  `dispatched_after` timestamp",
                ));
            }
        }
        self.dispatched_before = Some(time);
        Ok(self)
    }

    /// Add a set of fault management case IDs to filter by.
    ///
    /// Multiple calls to this method are additive: if this method is called
    /// multiple times on one instance of `AlertFilters`, the query will include
    /// alerts for any of the fault management case IDs passed to *any* of those
    /// calls.
    pub fn for_fm_cases<I>(mut self, cases: impl IntoIterator<Item = I>) -> Self
    where
        I: Into<DbTypedUuid<CaseKind>>,
    {
        self.cases.extend(cases.into_iter().map(Into::into));
        self
    }

    /// Add a set of alert classes to filter by.
    ///
    /// Multiple calls to this method are additive: if this method is called
    /// multiple times on one instance of `AlertFilters`, the query will include
    /// alerts with any of the alert classes passed to *any* of those calls.
    pub fn with_classes<I>(
        mut self,
        classes: impl IntoIterator<Item = I>,
    ) -> Self
    where
        I: Into<model::AlertClass>,
    {
        self.classes.extend(classes.into_iter().map(Into::into));
        self
    }

    /// If `true`, include only dispatched alerts; if `false`, include only
    /// undispatched alerts.
    pub fn dispatched(mut self, dispatched: bool) -> Result<Self, Error> {
        if !dispatched {
            if self.dispatched_after.is_some() {
                return Err(Error::invalid_request(
                    "cannot filter by `dispatched_after` when \
                     querying for undispatched alerts",
                ));
            }
            if self.dispatched_before.is_some() {
                return Err(Error::invalid_request(
                    "cannot filter by `dispatched_before` when \
                     querying for undispatched alerts",
                ));
            }
        }
        self.dispatched = Some(dispatched);
        Ok(self)
    }
}

impl DataStore {
    /// Insert an alert row, returning the inserted alert on success.
    ///
    /// If a row with this alert's id already exists, returns
    /// [`Error::ObjectAlreadyExists`]. This is expected: multiple Nexus
    /// instances may race to insert the same alert, so the operation must be
    /// idempotent. We surface this as distinct from a successful
    /// [`CreateResult`] so the caller can increment the "already existed"
    /// counter rather than the "created" counter.
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

    /// Insert an alert described by a fault-management [`AlertRequest`],
    /// guarded against stale-sitrep execution.
    ///
    /// Similar to [`Self::alert_create`], but the insert is wrapped in a
    /// [`SitrepGuardedInsert`]: it writes a `rendezvous_alert_created` marker
    /// so a deleted alert is not resurrected by a later rendezvous pass, and
    /// it is gated on the executing sitrep still being current. If the latest
    /// sitrep's `alert_generation` has advanced past `expected_alert_generation`,
    /// the sitrep being executed is stale and this returns
    /// [`FmRendezvousAlertCreateError::StaleSitrep`].
    pub async fn fm_rendezvous_alert_create(
        &self,
        opctx: &OpContext,
        request: &AlertRequest,
        case_id: CaseUuid,
        expected_alert_generation: Generation,
    ) -> Result<Alert, FmRendezvousAlertCreateError> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let alert = Alert::for_fm_alert_request(request, case_id);

        let guarded = SitrepGuardedInsert::<Alert, _>::new(
            alert.id().into_untyped_uuid(),
            expected_alert_generation.into(),
            Self::alert_insert_query(alert),
        );

        let alert =
            match guarded.execute_async(&conn).await.map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
            })? {
                SitrepGuardedInsertOutcome::Created(row) => row,
                SitrepGuardedInsertOutcome::AlreadyExists => {
                    return Err(FmRendezvousAlertCreateError::AlreadyCreated);
                }
                SitrepGuardedInsertOutcome::StaleSitrep => {
                    return Err(FmRendezvousAlertCreateError::StaleSitrep);
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

    fn alert_insert_query(
        alert: Alert,
    ) -> impl RunnableQuery<Alert> + Query<SqlType = SqlTypeOf<AsSelect<Alert, Pg>>>
    {
        diesel::insert_into(alert_dsl::alert)
            .values(alert)
            .on_conflict(alert_dsl::id)
            .do_nothing()
            .returning(Alert::as_returning())
    }

    pub async fn alert_select_next_for_dispatch(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<Alert>, Error> {
        let conn = self.pool_connection_authorized(&opctx).await?;
        Self::alert_select_next_for_dispatch_query()
            .get_result_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns the query used by [`Self::alert_select_next_for_dispatch`] to
    /// find the oldest not-yet-dispatched alert.
    ///
    /// The `LIMIT 1` is included here (rather than relying on `first_async`) so
    /// that the query's plan --- and therefore the `EXPLAIN` output exercised by
    /// tests --- matches what actually runs in production.
    fn alert_select_next_for_dispatch_query()
    -> impl RunnableQuery<Alert> + use<> {
        alert_dsl::alert
            .filter(alert_dsl::time_dispatched.is_null())
            .order_by(alert_dsl::time_created.asc())
            .select(Alert::as_select())
            .limit(1)
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
        use nexus_db_schema::schema::rendezvous_alert_created::dsl as marker_dsl;

        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        if candidates.is_empty() {
            return Ok(HashSet::new());
        }
        let conn = self.pool_connection_authorized(opctx).await?;

        // The candidate list is bounded by the number of alert requests on the
        // parent sitrep's closed cases, so the `eq_any` below shouldn't need to
        // be chunked into batches.
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

    pub async fn alert_fetch_fm_rendezvous_gen(
        &self,
        opctx: &OpContext,
        authz_alert: &authz::Alert,
    ) -> LookupResult<Option<fm::RendezvousAlertCreated>> {
        use nexus_db_schema::schema::rendezvous_alert_created::dsl as marker_dsl;

        marker_dsl::rendezvous_alert_created
            .filter(
                marker_dsl::alert_id.eq(authz_alert.id().into_untyped_uuid()),
            )
            .select(fm::RendezvousAlertCreated::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn alert_list_matching(
        &self,
        opctx: &OpContext,
        filters: &AlertFilters,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<(Alert, Option<fm::RendezvousAlertCreated>)> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        Self::alert_list_matching_query(filters, pagparams)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn alert_list_matching_query(
        filters: &AlertFilters,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> impl RunnableQuery<(Alert, Option<fm::RendezvousAlertCreated>)> + use<>
    {
        use nexus_db_schema::schema::rendezvous_alert_created::dsl as marker_dsl;

        let mut query = paginated_multicolumn(
            alert_dsl::alert,
            (alert_dsl::time_created, alert_dsl::id),
            &pagparams,
        )
        .left_join(marker_dsl::rendezvous_alert_created)
        .select((
            Alert::as_select(),
            Option::<fm::RendezvousAlertCreated>::as_select(),
        ));

        let &AlertFilters {
            before,
            after,
            dispatched_before,
            dispatched_after,
            dispatched,
            ref cases,
            ref classes,
        } = filters;
        if let Some(before) = before {
            query = query.filter(alert_dsl::time_created.le(before));
        }

        if let Some(after) = after {
            query = query.filter(alert_dsl::time_created.ge(after));
        }

        if let Some(before) = dispatched_before {
            query = query.filter(alert_dsl::time_dispatched.le(before));
        }

        if let Some(after) = dispatched_after {
            query = query.filter(alert_dsl::time_dispatched.ge(after));
        }

        if let Some(dispatched) = dispatched {
            if dispatched {
                query = query.filter(alert_dsl::time_dispatched.is_not_null());
            } else {
                query = query.filter(alert_dsl::time_dispatched.is_null());
            }
        }

        if !cases.is_empty() {
            query = query.filter(alert_dsl::case_id.eq_any(cases.clone()));
        }

        if !classes.is_empty() {
            query =
                query.filter(alert_dsl::alert_class.eq_any(classes.clone()));
        }

        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use assert_matches::assert_matches;
    use chrono::Utc;
    use dropshot::PaginationOrder;
    use nexus_db_model::fm::RendezvousAlertCreated;
    use nexus_db_schema::schema::rendezvous_alert_created::dsl as alert_marker_dsl;
    use nexus_types::alert::AlertClass;
    use nexus_types::alert::test_alerts;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CaseUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use serde_json::json;
    use std::num::NonZeroU32;

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

    fn make_alert_request(alert_id: AlertUuid) -> AlertRequest {
        AlertRequest {
            id: alert_id,
            class: nexus_types::alert::AlertClass::TestFoo,
            version: 0,
            payload: json!({}),
            requested_sitrep_id: SitrepUuid::new_v4(),
            comment: String::new(),
        }
    }

    #[tokio::test]
    async fn explain_alert_select_next_for_dispatch() {
        let logctx =
            dev::test_setup_log("explain_alert_select_next_for_dispatch");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::alert_select_next_for_dispatch_query();
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");

        // This query filters on `time_dispatched IS NULL` and orders by
        // `time_created`. It must be served by an index rather than a full
        // scan; in particular, dropping the old `lookup_undispatched_alerts`
        // partial index in favor of the single-column `time_dispatched` and
        // `time_created` indexes must not have regressed this query into a
        // full table scan.
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {explanation}",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_alert_list_matching_default() {
        explain_list_matching_query(
            "explain_alert_list_matching_default",
            AlertFilters::default(),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_created_time() {
        let now = Utc::now();
        explain_list_matching_query(
            "explain_alert_list_matching_created_time",
            AlertFilters::new()
                .after(now - chrono::Duration::hours(1))
                .expect("`after` is before `before`")
                .before(now)
                .expect("`before` is after `after`"),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_dispatched_time() {
        let now = Utc::now();
        explain_list_matching_query(
            "explain_alert_list_matching_dispatched_time",
            AlertFilters::new()
                .dispatched_after(now - chrono::Duration::hours(1))
                .expect("`dispatched_after` is before `dispatched_before`")
                .dispatched_before(now)
                .expect("`dispatched_before` is after `dispatched_after`"),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_dispatched_true() {
        explain_list_matching_query(
            "explain_alert_list_matching_dispatched_true",
            AlertFilters::new()
                .dispatched(true)
                .expect("no conflicting filters"),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_dispatched_false() {
        explain_list_matching_query(
            "explain_alert_list_matching_dispatched_false",
            AlertFilters::new()
                .dispatched(false)
                .expect("no conflicting filters"),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_only_cases() {
        explain_list_matching_query(
            "explain_alert_list_matching_only_cases",
            AlertFilters::new()
                .for_fm_cases([CaseUuid::new_v4(), CaseUuid::new_v4()]),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_only_classes() {
        explain_list_matching_query(
            "explain_alert_list_matching_only_classes",
            AlertFilters::new()
                .with_classes([AlertClass::TestFoo, AlertClass::TestFooBar]),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_cases_and_classes() {
        explain_list_matching_query(
            "explain_alert_list_matching_cases_and_classes",
            AlertFilters::new()
                .for_fm_cases([CaseUuid::new_v4(), CaseUuid::new_v4()])
                .with_classes([AlertClass::TestFoo, AlertClass::TestFooBar]),
        )
        .await
    }

    #[tokio::test]
    async fn explain_alert_list_matching_all_filters() {
        let now = Utc::now();
        explain_list_matching_query(
            "explain_alert_list_matching_all_filters",
            AlertFilters::new()
                .after(now - chrono::Duration::hours(2))
                .expect("`after` is before `before`")
                .before(now)
                .expect("`before` is after `after`")
                .dispatched_after(now - chrono::Duration::hours(1))
                .expect("`dispatched_after` is before `dispatched_before`")
                .dispatched_before(now)
                .expect("`dispatched_before` is after `dispatched_after`")
                .dispatched(true)
                .expect("true and dispatched before/after is valid")
                .for_fm_cases([CaseUuid::new_v4(), CaseUuid::new_v4()])
                .with_classes([AlertClass::TestFoo, AlertClass::TestFooBar]),
        )
        .await
    }

    async fn explain_list_matching_query(
        test_name: &str,
        filters: AlertFilters,
    ) {
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams::<'_, (DateTime<Utc>, Uuid)> {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        eprintln!("--- filters: {filters:#?}\n");

        let query = DataStore::alert_list_matching_query(&filters, &pagparams);

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("--- explanation: {explanation}");

        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {explanation}",
        );

        db.terminate().await;
        logctx.cleanup_successful();
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
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(1)), None)
            .await
            .unwrap();

        let alert_id = AlertUuid::new_v4();
        let case_id = CaseUuid::new_v4();
        let inserted = datastore
            .fm_rendezvous_alert_create(
                opctx,
                &make_alert_request(alert_id),
                case_id,
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
    async fn fm_rendezvous_alert_create_already_exists_without_marker() {
        let logctx = dev::test_setup_log(
            "fm_rendezvous_alert_create_already_exists_without_marker",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(1)), None)
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
                &make_alert_request(alert_id),
                case_id,
                Generation::from_u32(1),
            )
            .await
            .unwrap_err();
        assert_matches!(err, FmRendezvousAlertCreateError::AlreadyCreated);

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

        // Latest sitrep is at generation 5; the rendezvous task expects 1.
        datastore
            .fm_sitrep_insert(opctx, make_sitrep(Generation::from_u32(5)), None)
            .await
            .unwrap();

        let err = datastore
            .fm_rendezvous_alert_create(
                opctx,
                &make_alert_request(AlertUuid::new_v4()),
                CaseUuid::new_v4(),
                Generation::new(),
            )
            .await
            .unwrap_err();
        assert_matches!(err, FmRendezvousAlertCreateError::StaleSitrep);

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
}
