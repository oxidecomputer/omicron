// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A Diesel query wrapper that gates an `INSERT` on the FM rendezvous tracker.
//!
//! ## Problem
//!
//! FM rendezvous executes a sitrep by attempting to materialize the resources
//! (alerts and support bundles) that it requests. This process runs
//! periodically on every Nexus, so the same sitrep may be re-executed many
//! times, either by the same Nexus that first executed it, or by any other
//! Nexus running concurrently or lagging behind.
//!
//! Meanwhile, the same resources may be deleted out-of-band (e.g. by an
//! operator) at any time. This poses a problem: we don't want FM rendezvous
//! to resurrect deleted resources. We deal with this in part with tombstones
//! on the resources: a tombstoned resource can't be resurrected (we use
//! `ON CONFLICT DO NOTHING`). But that's not a complete solution: we need to
//! know when it's safe to hard-delete a tombstoned resource, such that a Nexus
//! (even one lagging arbitrarily far behind) can never resurrect it.
//!
//! ## Solution
//!
//! We achieve this using the `fm_rendezvous_progress` tracker, which records
//! the newest sitrep version any Nexus has fully processed. When FM rendezvous
//! attempts to insert a resource, it gates the insert on the caller's
//! (in-memory) sitrep version being at least as new as the tracker, using
//! [`SitrepVersionGuardedInsert`].
//!
//! With this in place, a tombstoned resource is safe to hard-delete once its
//! case no longer appears in any sitrep at least as new as the tracker: any
//! Nexus that could otherwise re-create it would be working from an older
//! sitrep, which [`SitrepVersionGuardedInsert`] rejects.

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::OptionalExtension;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::result::Error as DieselError;
use nexus_db_lookup::DbConnection;
use std::marker::PhantomData;

/// Outcome of a guarded INSERT that did not return a row.
#[derive(Debug)]
pub enum GuardedInsertError {
    /// The tracker has advanced past the caller's sitrep version.
    StaleSitrep,
    /// Any other database error.
    DatabaseError(DieselError),
}

/// Wraps a Diesel `INSERT ... RETURNING` to gate it on the FM sitrep tracker.
///
/// Type parameters:
/// - `R` is the row type the inner insert returns (the `RETURNING` shape).
/// - `ISR` is the inner insert query itself (typically an
///   `InsertStatement<...>`).
#[must_use = "Queries must be executed"]
pub struct SitrepVersionGuardedInsert<R, ISR> {
    /// The caller's `INSERT ... RETURNING` query
    insert_statement: ISR,
    /// The caller's sitrep version, for comparison against the tracker.
    sitrep_version: i64,
    _phantom: PhantomData<R>,
}

impl<R, ISR> SitrepVersionGuardedInsert<R, ISR> {
    pub fn new(insert_statement: ISR, sitrep_version: i64) -> Self {
        Self { insert_statement, sitrep_version, _phantom: PhantomData }
    }
}

/// Propagate the prepared-statement cache key from the inner insert: the
/// wrapper's CTE frame is structurally fixed, so if `ISR` has a static query
/// id, so does the wrapper.
impl<R, ISR> QueryId for SitrepVersionGuardedInsert<R, ISR>
where
    ISR: QueryId,
{
    /// Note that `R` is discarded here, following diesel's example doc:
    /// `Bound<SqlType, RustType>` uses `Bound<SqlType::QueryId, ()>`).
    type QueryId = SitrepVersionGuardedInsert<(), ISR::QueryId>;
    const HAS_STATIC_QUERY_ID: bool = ISR::HAS_STATIC_QUERY_ID;
}

/// The wrapper's SQL output shape is the inner insert's `RETURNING` shape:
/// the guard CTE only gates whether a row is produced, it never changes which
/// columns are projected.
impl<R, ISR> Query for SitrepVersionGuardedInsert<R, ISR>
where
    ISR: Query,
{
    type SqlType = ISR::SqlType;
}

/// Emit the guard CTE that wraps the caller's `INSERT ... RETURNING`.
impl<R, ISR> QueryFragment<Pg> for SitrepVersionGuardedInsert<R, ISR>
where
    ISR: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // The `guard` CTE reads the singleton tracker row and compares it to
        // the caller's sitrep version.
        //
        //   * If `tracker <= caller`, the `IF` returns `TRUE`.
        //   * Otherwise, evaluating `CAST(1/0 AS BOOL)` raises a non-retryable
        //     "division by zero" error, which we translate into
        //     [`GuardedInsertError::StaleSitrep`].
        //
        // `MATERIALIZED` on the guard tells CRDB to evaluate the CTE once
        // rather than inlining, or worse discarding it. We particularly don't
        // want the optimizer deciding the guard's result is unused and skipping
        // its evaluation.
        out.push_sql(
            "WITH guard AS MATERIALIZED (\
             SELECT IF(latest_processed_sitrep_version <= ",
        );
        out.push_bind_param::<diesel::sql_types::BigInt, _>(
            &self.sitrep_version,
        )?;
        out.push_sql(
            ", TRUE, CAST(1/0 AS BOOL)) AS proceed \
             FROM fm_rendezvous_progress WHERE singleton = true), ",
        );

        // The caller's `INSERT ... RETURNING`. Data-modifying CTEs are always
        // evaluated exactly once, so this one doesn't need (and wouldn't
        // benefit from) a `MATERIALIZED` hint.
        out.push_sql("inserted_row AS (");
        self.insert_statement.walk_ast(out.reborrow())?;
        out.push_sql(") SELECT inserted_row.* FROM inserted_row");
        Ok(())
    }
}

/// Marker impl that opts the wrapper into diesel's async extension methods
/// (`get_result_async`, `load_async`, etc.) for [`DbConnection`].
impl<R, ISR> RunQueryDsl<DbConnection> for SitrepVersionGuardedInsert<R, ISR> {}

impl<R, ISR> SitrepVersionGuardedInsert<R, ISR>
where
    R: Send + 'static,
    ISR: Send + 'static,
    Self: Send,
    Self: diesel::query_dsl::methods::LoadQuery<'static, DbConnection, R>,
{
    /// Run the guarded insert and return the (single) result row, if any.
    ///
    /// Outcomes:
    /// - `Ok(Some(r))`: the guard passed and the underlying INSERT produced
    ///   a row, meaning there was no conflict.
    /// - `Ok(None)`: the guard passed but the underlying INSERT produced no
    ///   row due to a conflict.
    /// - `Err(StaleSitrep)`: the guard fired (the tracker has advanced past
    ///   the caller's sitrep version), so the insert was rejected.
    /// - `Err(DatabaseError(_))`: any other database error.
    pub async fn insert_and_get_optional_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<R>, GuardedInsertError> {
        self.get_result_async::<R>(conn).await.optional().map_err(|e| {
            if error_is_division_by_zero(&e) {
                GuardedInsertError::StaleSitrep
            } else {
                GuardedInsertError::DatabaseError(e)
            }
        })
    }
}

/// True iff `err` is the runtime "division by zero" the guard CTE raises
/// when the tracker has advanced past the caller's sitrep version.
///
/// The match on the literal message is brittle by design — it's the receiver
/// end of the abort signal we deliberately emit in
/// [`SitrepVersionGuardedInsert::walk_ast`]. CRDB surfaces division-by-zero
/// as `DatabaseErrorKind::Unknown` rather than a more specific kind, which
/// is also why the `transaction_retry_wrapper` path doesn't retry it (only
/// `SerializationFailure` is retried). [`crate::db::collection_insert`]
/// uses the same shape for its `CollectionNotFound` signal.
fn error_is_division_by_zero(err: &DieselError) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(
            diesel::result::DatabaseErrorKind::Unknown,
            info,
        ) if info.message() == "division by zero"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use diesel::debug_query;
    use nexus_db_model::Alert;
    use nexus_db_schema::schema::alert;
    use nexus_test_utils::db::TestDatabase;
    use nexus_types::alert::AlertClass;
    use nexus_types::identity::Asset;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AlertUuid;
    use omicron_uuid_kinds::GenericUuid;

    #[test]
    fn emitted_sql_wraps_insert_in_guard_cte() {
        let alert = Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            serde_json::json!({}),
        );
        let insert = diesel::insert_into(alert::table)
            .values(alert)
            .on_conflict_do_nothing()
            .returning(Alert::as_returning());

        let guarded = SitrepVersionGuardedInsert::<Alert, _>::new(insert, 42);
        let sql = debug_query::<Pg, _>(&guarded).to_string();

        assert!(
            sql.contains("WITH guard AS MATERIALIZED"),
            "missing guard CTE: {sql}"
        );
        assert!(
            sql.contains("inserted_row AS ("),
            "missing inserted_row CTE: {sql}"
        );
        assert!(
            sql.contains("CAST(1/0 AS BOOL)"),
            "missing division-by-zero: {sql}"
        );
        assert!(
            sql.contains("fm_rendezvous_progress"),
            "guard must read tracker: {sql}"
        );
    }

    #[tokio::test]
    async fn guard_fires_when_tracker_ahead() {
        let logctx = dev::test_setup_log("guard_fires_when_tracker_ahead");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        datastore.fm_rendezvous_progress_advance(db.opctx(), 10).await.unwrap();

        let alert = Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            serde_json::json!({}),
        );
        let alert_id = alert.id();
        let insert = diesel::insert_into(alert::table)
            .values(alert)
            .on_conflict_do_nothing()
            .returning(Alert::as_returning());
        let guarded = SitrepVersionGuardedInsert::new(insert, 5);
        let result = guarded.insert_and_get_optional_result_async(&conn).await;
        assert!(
            matches!(result, Err(GuardedInsertError::StaleSitrep)),
            "got {result:?}",
        );

        let present: Option<Alert> = alert::table
            .filter(alert::id.eq(alert_id.into_untyped_uuid()))
            .select(Alert::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .unwrap();
        assert!(present.is_none(), "guard fired but row was written anyway");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn guard_passes_when_tracker_behind() {
        let logctx = dev::test_setup_log("guard_passes_when_tracker_behind");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let alert = Alert::new(
            AlertUuid::new_v4(),
            AlertClass::TestFoo,
            serde_json::json!({}),
        );
        let insert = diesel::insert_into(alert::table)
            .values(alert.clone())
            .on_conflict_do_nothing()
            .returning(Alert::as_returning());
        let result = SitrepVersionGuardedInsert::new(insert, 1)
            .insert_and_get_optional_result_async(&conn)
            .await;
        assert!(matches!(result, Ok(Some(_))), "got {result:?}");

        let insert = diesel::insert_into(alert::table)
            .values(alert)
            .on_conflict_do_nothing()
            .returning(Alert::as_returning());
        let result = SitrepVersionGuardedInsert::new(insert, 1)
            .insert_and_get_optional_result_async(&conn)
            .await;
        assert!(matches!(result, Ok(None)), "got {result:?}");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
