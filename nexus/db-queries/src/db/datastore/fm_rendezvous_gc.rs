// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Paginated GC sweep for the FM rendezvous resource creation marker tables
//! (`rendezvous_alert_created` and `rendezvous_support_bundle_created`).
//!
//! Each resource created by FM rendezvous (alert / support bundle) has a
//! corresponding creation marker, written by
//! [`crate::db::sitrep_guard::SitrepGuardedInsert`]. The purpose of the marker
//! is to prevent FM rendezvous from mistakenly resurrecting a resource that was
//! previously deleted by the user. The marker therefore needs to exist as long
//! as FM rendezvous *might* attempt to resurrect the resource. But we don't
//! want to keep these markers around forever -- we want to GC them
//! eventually -- so the question is, how long is long enough?
//!
//! Suppose we have a sitrep with an `alert_request` to create some alert A, and
//! the sitrep's `alert_generation` is 1. When FM rendezvous first encounters
//! this (that is, alert A doesn't exist yet), it writes two things:
//!
//!   - The `alert` row for A,
//!   - A corresponding `rendezvous_alert_created` row for A,
//!     with `created_at_generation = 1`.
//!
//! Then, suppose a user dismisses this alert, deleting it from the `alert`
//! table. Any subsequent FM rendezvous activation processing this same sitrep,
//! or any other sitrep with alert generation 1, could attempt to re-create
//! alert A as long as alert generation 1 is still current. So the
//! `rendezvous_alert_created` marker for A must exist for *at least* as long
//! as generation 1 is current, to prevent this.
//!
//! Now say we have a new sitrep in the database with `alert_generation` 2,
//! meaning the set of alerts it's requesting differs in some way from
//! generation 1, but it still contains the same `alert_request` for alert A,
//! carried forward. When FM rendezvous activates, there are now two cases:
//!
//!   1. It's processing the old sitrep (alert generation 1). In this case,
//!      `SitrepGuardedInsert` will prevent it from creating *any* alerts,
//!      simply because the sitrep's alert generation is stale. The
//!      `rendezvous_alert_created` markers aren't needed in this case.
//!
//!   2. It's processing the new sitrep (alert generation 2). In this case, it
//!      could still attempt to re-create alert A. So the
//!      `rendezvous_alert_created` marker for A must still exist here to
//!      prevent this! That is, even though the marker has
//!      `created_at_generation = 1`, we can't get away with deleting it yet.
//!
//! Finally, say another new sitrep with `alert_generation` 3 no longer
//! contains the `alert_request` for alert A. Alert UUIDs are ~never reused, so
//! no sitrep at generation 3 or later can request A -- and executors of stale
//! sitreps are still blocked wholesale, as above. Nothing can re-create alert
//! A, so its marker can be deleted safely.
//!
//! Let's formalize this! Given a sitrep S being executed by some activation of
//! FM rendezvous, and some resource creation marker row R, we can delete R if
//! all of the following hold:
//!
//!   * S *exists*; its `fm_sitrep` row is still present.
//!   * S *does not request* `R.<resource>_id`.
//!   * R *predates* S: `R.created_at_generation < S.<resource>_generation`.
//!
//! That is, if the sitrep we're executing doesn't request the resource, and the
//! resource was created by a sitrep older than the one we're executing, then no
//! future sitrep can request it either, and the marker can be deleted. Note
//! that this is true even if the sitrep we're executing isn't current anymore.

use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::fm_rendezvous_resources::FmRendezvousResource;
use crate::db::fm_rendezvous_resources::MarkerTable;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::Column;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::query_source::QuerySource;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use uuid::Uuid;

/// Totals from a completed creation-marker GC sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MarkerGcResult {
    /// Total marker rows deleted, across all pages.
    pub rows_deleted: usize,
    /// Number of page queries the sweep executed, counted the same way as
    /// `fm_sitrep_gc`'s batch counters.
    pub batches: usize,
}

impl DataStore {
    /// Garbage-collect `rendezvous_alert_created`, deleting every marker row
    /// whose referenced alert can no longer be requested by any current or
    /// future sitrep, as judged from the sitrep being executed.
    pub async fn fm_rendezvous_alert_marker_gc(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_alert_generation: Generation,
    ) -> Result<MarkerGcResult, Error> {
        self.fm_rendezvous_marker_gc::<nexus_db_model::Alert>(
            opctx,
            sitrep_id,
            sitrep_alert_generation,
        )
        .await
    }

    /// Garbage-collect `rendezvous_support_bundle_created`, deleting every
    /// marker row whose referenced support bundle can no longer be requested by
    /// any current or future sitrep, as judged from the sitrep being executed.
    pub async fn fm_rendezvous_support_bundle_marker_gc(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_support_bundle_generation: Generation,
    ) -> Result<MarkerGcResult, Error> {
        self.fm_rendezvous_marker_gc::<nexus_db_model::SupportBundle>(
            opctx,
            sitrep_id,
            sitrep_support_bundle_generation,
        )
        .await
    }

    /// Paginated GC sweep shared by both creation-marker tables, generic over
    /// the [`FmRendezvousResource`] resource `R`. See the [module docs](self)
    /// for the deletion predicate and its safety argument.
    async fn fm_rendezvous_marker_gc<R: FmRendezvousResource + 'static>(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_generation: Generation,
    ) -> Result<MarkerGcResult, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let mut result = MarkerGcResult::default();
        let mut cursor = Uuid::nil();
        loop {
            let (deleted, next_cursor) = RendezvousMarkerGcPage::<R>::new(
                cursor,
                i64::from(SQL_BATCH_SIZE.get()),
                sitrep_id,
                sitrep_generation,
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context(format!(
                        "sweeping rendezvous creation-marker rows keyed by {}",
                        <R::IdColumn as Column>::NAME,
                    ))
            })?;
            result.rows_deleted += deleted as usize;
            result.batches += 1;
            match next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        Ok(result)
    }
}

/// One page of a creation-marker GC sweep, emitted as raw SQL built from `R`'s
/// schema types (see [`FmRendezvousResource`]).
#[must_use = "Queries must be executed"]
struct RendezvousMarkerGcPage<R: FmRendezvousResource> {
    /// Exclusive lower bound on the marker id scanned this page (`Uuid::nil()`
    /// on the first page).
    cursor: Uuid,
    /// Maximum number of marker rows examined this page.
    page_size: i64,
    /// Id of the sitrep being executed. A marker row is deletable only if this
    /// sitrep still exists and the marker's resource is absent from its
    /// request set.
    sitrep_id: Uuid,
    /// Generation of the sitrep being executed. A marker row is deletable only
    /// if its `created_at_generation` is strictly less than this.
    sitrep_generation: nexus_db_model::Generation,
    /// `FROM` clause for the marker table, built once in [`Self::new`]. Stored
    /// as a field here, rather than reconstructed inside
    /// [`QueryFragment::walk_ast`], so it correctly shares `self`'s lifetime
    /// when walked.
    marker_from_clause: <MarkerTable<R> as QuerySource>::FromClause,
    /// `FROM` clause for the `fm_sitrep` table, stored for the same reason.
    sitrep_from_clause:
        <nexus_db_schema::schema::fm_sitrep::table as QuerySource>::FromClause,
    /// `FROM` clause for the request table, stored for the same reason.
    request_from_clause: <R::RequestTable as QuerySource>::FromClause,
}

impl<R: FmRendezvousResource> RendezvousMarkerGcPage<R> {
    fn new(
        cursor: Uuid,
        page_size: i64,
        sitrep_id: SitrepUuid,
        sitrep_generation: Generation,
    ) -> Self {
        Self {
            cursor,
            page_size,
            sitrep_id: sitrep_id.into_untyped_uuid(),
            sitrep_generation: nexus_db_model::Generation::from(
                sitrep_generation,
            ),
            marker_from_clause: <MarkerTable<R> as HasTable>::table()
                .from_clause(),
            sitrep_from_clause: nexus_db_schema::schema::fm_sitrep::table
                .from_clause(),
            request_from_clause: <R::RequestTable as HasTable>::table()
                .from_clause(),
        }
    }
}

impl<R: FmRendezvousResource> QueryId for RendezvousMarkerGcPage<R> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<R: FmRendezvousResource> Query for RendezvousMarkerGcPage<R> {
    type SqlType = (sql_types::BigInt, sql_types::Nullable<sql_types::Uuid>);
}

impl<R: FmRendezvousResource> RunQueryDsl<DbConnection>
    for RendezvousMarkerGcPage<R>
{
}

impl<R: FmRendezvousResource> QueryFragment<Pg> for RendezvousMarkerGcPage<R> {
    /// Executes a statement of the form:
    ///
    /// ```sql
    /// WITH
    ///   page AS (...),
    ///   deleted AS (...)
    /// SELECT ... AS rows_deleted, ... AS next_cursor
    /// ```
    ///
    /// with the marker id column and the marker/request table names spliced
    /// from `R`'s schema types. The individual CTEs here are:
    ///
    /// 1. `page`: one page of marker rows, at most `page_size` of them,
    ///    ordered by the marker id and starting strictly after `cursor`.
    /// 2. `deleted`: if the executed sitrep still exists, deletes the page rows
    ///    that it doesn't request and that predate it (see the module docs for
    ///    these conditions), returning their ids.
    ///
    /// The outer `SELECT` projects the page's outcome back to the sweep loop:
    /// the number of rows deleted, and the next cursor (or `NULL` when the
    /// table is exhausted).
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        let id_column = <R::IdColumn as Column>::NAME;

        // CTE 1: page -- one bounded page of marker rows.
        out.push_sql("WITH page AS (SELECT ");
        out.push_identifier(id_column)?;
        out.push_sql(", ");
        out.push_identifier("created_at_generation")?;
        out.push_sql(" FROM ");
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(id_column)?;
        out.push_sql(" > ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.cursor)?;
        out.push_sql(" ORDER BY ");
        out.push_identifier(id_column)?;
        out.push_sql(" LIMIT ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.page_size)?;

        // CTE 2: deleted -- if the executed sitrep still exists, delete the
        // page rows that it doesn't request and that predate it, returning
        // their ids so the outer SELECT can count them.
        out.push_sql("), deleted AS (DELETE FROM ");
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(id_column)?;
        out.push_sql(" IN (SELECT p.");
        out.push_identifier(id_column)?;
        // Sitrep deletion removes the `fm_sitrep` row either before or together
        // with any child request rows (see `fm_sitrep_gc_orphans` and
        // `fm_sitrep_delete_all`), so if the sitrep exists in our snapshot, we
        // know the request rows checked below are visible too.
        out.push_sql(" FROM page p WHERE EXISTS (SELECT 1 FROM ");
        self.sitrep_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier("id")?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.sitrep_id)?;
        out.push_sql(") AND NOT EXISTS (SELECT 1 FROM ");
        self.request_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" r WHERE r.");
        out.push_identifier("sitrep_id")?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.sitrep_id)?;
        out.push_sql(" AND r.");
        out.push_identifier("id")?;
        out.push_sql(" = p.");
        out.push_identifier(id_column)?;
        out.push_sql(") AND p.");
        out.push_identifier("created_at_generation")?;
        out.push_sql(" < ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.sitrep_generation)?;
        out.push_sql(") RETURNING ");
        out.push_identifier(id_column)?;

        // Outer SELECT: rows deleted, and the cursor advance.
        //
        // The next cursor is `MAX(id)` over the page, not over the rows
        // actually deleted, so the strict `>` advance in the page CTE
        // guarantees forward progress even on a page where nothing satisfied
        // the predicate. A partial page means the table is exhausted, so the
        // `CASE WHEN` returns NULL to end the sweep without an extra
        // empty-page query.
        out.push_sql(
            ") SELECT (SELECT COUNT(*) FROM deleted) AS rows_deleted, \
             CASE WHEN (SELECT COUNT(*) FROM page) >= ",
        );
        out.push_bind_param::<sql_types::BigInt, _>(&self.page_size)?;
        out.push_sql(" THEN (SELECT MAX(");
        out.push_identifier(id_column)?;
        out.push_sql(") FROM page) ELSE NULL END AS next_cursor");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::fm_rendezvous_resources::test_utils::DummyResource;
    use crate::db::fm_rendezvous_resources::test_utils::dummy_request;
    use crate::db::fm_rendezvous_resources::test_utils::insert_current_sitrep;
    use crate::db::fm_rendezvous_resources::test_utils::insert_dummy_marker;
    use crate::db::fm_rendezvous_resources::test_utils::marker_generation;
    use crate::db::fm_rendezvous_resources::test_utils::setup_dummy_schema;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use diesel::ExpressionMethods;
    use omicron_test_utils::dev;

    /// Put `resource_id` into `sitrep_id`'s request set via a dummy request row.
    async fn insert_dummy_request(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sitrep_id: SitrepUuid,
        resource_id: Uuid,
    ) {
        diesel::insert_into(dummy_request::table)
            .values((
                dummy_request::dsl::id.eq(resource_id),
                dummy_request::dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()),
            ))
            .execute_async(conn)
            .await
            .unwrap();
    }

    /// The predicate matrix in one sweep: given that the executed sitrep
    /// *exists*, a marker is deleted only when the sitrep *doesn't request* its
    /// resource and the marker *predates* the sitrep. Markers failing any
    /// condition are preserved.
    #[tokio::test]
    async fn gc_deletes_only_dropped_markers() {
        let logctx = dev::test_setup_log("gc_deletes_only_dropped_markers");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;

        // The executed sitrep must actually exist in `fm_sitrep`; sweeping
        // against a missing sitrep deletes nothing (see
        // `gc_deletes_nothing_when_sitrep_missing`).
        let sitrep_id =
            insert_current_sitrep(datastore, db.opctx(), &conn, None, 5).await;
        // 5 deletable markers: predate the executed sitrep, not requested by
        // any sitrep.
        let mut deletable: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        for &id in &deletable {
            insert_dummy_marker(&conn, id, 1).await;
        }
        // Also deletable: predates the executed sitrep, and requested only
        // by a *different* sitrep.
        let foreign_request = Uuid::new_v4();
        insert_dummy_marker(&conn, foreign_request, 1).await;
        insert_dummy_request(&conn, SitrepUuid::new_v4(), foreign_request)
            .await;
        deletable.push(foreign_request);
        // Preserved: predates the executed sitrep, but the sitrep still
        // requests the resource.
        let requested = Uuid::new_v4();
        insert_dummy_marker(&conn, requested, 1).await;
        insert_dummy_request(&conn, sitrep_id, requested).await;
        // Preserved: created at the executed sitrep's generation (the
        // predates condition is a strict `<`, so equality preserves).
        let at_generation = Uuid::new_v4();
        insert_dummy_marker(&conn, at_generation, 5).await;
        // Preserved: created above the executed sitrep's generation. This is
        // an executor lagging behind marker writers, which happens in normal
        // operation: while this sweep runs against a generation-5 sitrep,
        // another Nexus executing a newer sitrep can concurrently write
        // generation-6 markers.
        let above_generation = Uuid::new_v4();
        insert_dummy_marker(&conn, above_generation, 6).await;

        let result = datastore
            .fm_rendezvous_marker_gc::<DummyResource>(
                db.opctx(),
                sitrep_id,
                Generation::from_u32(5),
            )
            .await
            .unwrap();
        // All 9 markers fit in one partial page, which also ends the sweep.
        assert_eq!(result, MarkerGcResult { rows_deleted: 6, batches: 1 });

        // Deletables are gone; preserveds remain.
        for &id in &deletable {
            assert!(
                marker_generation(&conn, id).await.is_none(),
                "dropped marker {id:?} should have been swept",
            );
        }
        for (id, why) in [
            (requested, "requested by the executed sitrep"),
            (at_generation, "at the executed sitrep's generation"),
            (above_generation, "above the executed sitrep's generation"),
        ] {
            assert!(
                marker_generation(&conn, id).await.is_some(),
                "marker {id:?} ({why}) should have been preserved",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// The cursor a page reports comes from `MAX(id)` over the *examined* page,
    /// not over the deleted rows, so a full page in which nothing satisfies the
    /// predicate must still advance the sweep (see the `walk_ast` docs; getting
    /// this wrong is an infinite loop in the background task). Drive the page
    /// query by hand with a page size of 2 with the two lowest-sorting markers
    /// preserved, so the first page deletes nothing but must still advance the
    /// cursor. The sweep's two pages are issued explicitly rather than looping
    /// until the query reports completion, so a broken cursor advance fails an
    /// assertion instead of hanging.
    #[tokio::test]
    async fn gc_pages_advance_past_preserved_markers() {
        let logctx =
            dev::test_setup_log("gc_pages_advance_past_preserved_markers");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;

        let sitrep_id =
            insert_current_sitrep(datastore, db.opctx(), &conn, None, 5).await;
        // Fixed ids so the scan order is known: markers 1 and 2 sort first
        // and are preserved (their generation equals the sitrep's), while
        // marker 3 is deletable (predates the executed sitrep, never
        // requested).
        let preserved: Vec<Uuid> = (1..=2).map(Uuid::from_u128).collect();
        let deletable = Uuid::from_u128(3);
        for &id in &preserved {
            insert_dummy_marker(&conn, id, 5).await;
        }
        insert_dummy_marker(&conn, deletable, 1).await;

        let page = |cursor| {
            RendezvousMarkerGcPage::<DummyResource>::new(
                cursor,
                2,
                sitrep_id,
                Generation::from_u32(5),
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
        };

        // The first page examines the two preserved markers: it deletes
        // nothing, but must still advance the cursor past them.
        let (deleted, next_cursor) = page(Uuid::nil()).await.unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(next_cursor, Some(preserved[1]));

        // The second, partial page deletes marker 3 and ends the sweep.
        let (deleted, next_cursor) = page(preserved[1]).await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(next_cursor, None);
        for &id in &preserved {
            assert!(
                marker_generation(&conn, id).await.is_some(),
                "marker {id:?} should have been preserved",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A sweep against a sitrep with no `fm_sitrep` row must delete nothing.
    /// Whether a sitrep requests a resource is judged by querying its request
    /// rows, so without the existence check, a sweep against a hard-deleted
    /// sitrep would see every marker as unrequested and delete markers for
    /// resources still carried forward by the current sitreps.
    #[tokio::test]
    async fn gc_deletes_nothing_when_sitrep_missing() {
        let logctx =
            dev::test_setup_log("gc_deletes_nothing_when_sitrep_missing");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;

        // This marker predates the executed sitrep and is unrequested: it
        // satisfies every deletion condition except the sitrep's existence.
        let marker = Uuid::new_v4();
        insert_dummy_marker(&conn, marker, 1).await;

        let result = datastore
            .fm_rendezvous_marker_gc::<DummyResource>(
                db.opctx(),
                SitrepUuid::new_v4(),
                Generation::from_u32(5),
            )
            .await
            .unwrap();
        assert_eq!(result, MarkerGcResult { rows_deleted: 0, batches: 1 });
        assert!(
            marker_generation(&conn, marker).await.is_some(),
            "marker must be preserved when the executed sitrep does not exist",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Snapshots the SQL the page query generates, so accidental changes to
    /// the hand-assembled query are caught in review.
    #[tokio::test]
    async fn expectorate_rendezvous_marker_gc_page() {
        let query = RendezvousMarkerGcPage::<DummyResource>::new(
            Uuid::nil(),
            1000,
            SitrepUuid::nil(),
            Generation::from_u32(5),
        );
        expectorate_query_contents(
            query,
            "tests/output/rendezvous_marker_gc_page.sql",
        )
        .await;
    }
}
