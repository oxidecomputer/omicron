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
//! Finally, say we have another new sitrep in the database with
//! `alert_generation` 3, and now it no longer contains the `alert_request` for
//! alert A. This means that alert A can't be requested by any future sitrep,
//! since we ~never reuse alert UUIDs. At this point, any activation of FM
//! rendezvous can't possibly re-create alert A. Two cases, as before:
//!
//!   1. It's processing a stale sitrep (generation 1 or 2). In this case,
//!      `SitrepGuardedInsert` prevents it from creating alerts.
//!
//!   2. It's processing a current sitrep (generation 3 or later), which does
//!      not request alert A.
//!
//! Now, in both cases, we don't need to consult the `rendezvous_alert_created`
//! marker for A anymore; it can be deleted safely.
//!
//! Let's formalize this! Given a sitrep S being executed by some activation of
//! FM rendezvous, and some resource creation marker row R, we can delete R if:
//!
//!   * `R.created_at_generation < S.<resource>_generation`, AND
//!   * `R.<resource>_id` is not requested by S.
//!
//! That is, if the resource was created by a sitrep older than the one we're
//! currently executing, and the one we're executing doesn't request it, then
//! no future sitrep can request it either, and the marker can be deleted. Note
//! that this is true even if the sitrep we're executing isn't current.

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

impl DataStore {
    /// Garbage-collect `rendezvous_alert_created`, deleting every marker row
    /// whose referenced alert is provably no longer requested by any current
    /// or future sitrep at the generation currently being processed. Returns
    /// the number of marker rows deleted.
    pub async fn rendezvous_alert_created_gc(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_alert_generation: Generation,
    ) -> Result<usize, Error> {
        self.rendezvous_marker_gc::<nexus_db_model::Alert>(
            opctx,
            sitrep_id,
            sitrep_alert_generation,
        )
        .await
    }

    /// Garbage-collect `rendezvous_support_bundle_created`. Parallel to
    /// [`Self::rendezvous_alert_created_gc`].
    pub async fn rendezvous_support_bundle_created_gc(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_support_bundle_generation: Generation,
    ) -> Result<usize, Error> {
        self.rendezvous_marker_gc::<nexus_db_model::SupportBundle>(
            opctx,
            sitrep_id,
            sitrep_support_bundle_generation,
        )
        .await
    }

    /// Paginated GC sweep shared by both creation-marker tables, generic over
    /// the [`FmRendezvousResource`] resource `R`. Loops [`RendezvousMarkerGcPage`]
    /// with a fixed page size until a page reports no next cursor, accumulating
    /// the total number of marker rows deleted. See the [module docs](self) for
    /// the deletion predicate and its safety argument.
    async fn rendezvous_marker_gc<R: FmRendezvousResource + 'static>(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        sitrep_generation: Generation,
    ) -> Result<usize, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let mut rows_deleted = 0usize;
        let mut cursor = Uuid::nil();
        loop {
            let (deleted, next_cursor) = RendezvousMarkerGcPage::<R>::new(
                cursor,
                i64::from(SQL_BATCH_SIZE.get()),
                sitrep_generation,
                sitrep_id.into_untyped_uuid(),
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context(
                        "sweeping rendezvous creation-marker rows",
                    )
            })?;
            rows_deleted += deleted as usize;
            match next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        Ok(rows_deleted)
    }
}

/// One page of a creation-marker GC sweep, emitted as raw SQL built from `R`'s
/// schema types (see [`FmRendezvousResource`]). Executing it deletes up to
/// `page_size` marker rows satisfying the deletion predicate and returns
/// `(rows_deleted, next_cursor)`, where `next_cursor` is the greatest marker id
/// examined on this page (`NULL` once the table is exhausted).
///
/// Like [`SitrepGuardedInsert`], this is a hand-written [`QueryFragment`]
/// rather than a [`crate::db::raw_query_builder`] query: emitting Diesel schema
/// identifiers (via [`AstPass::push_identifier`] and the tables' `FROM`
/// clauses) requires Diesel's `AstPass`, which the raw builder does not expose.
///
/// [`SitrepGuardedInsert`]: crate::db::sitrep_guard::SitrepGuardedInsert
#[must_use = "Queries must be executed"]
struct RendezvousMarkerGcPage<R: FmRendezvousResource> {
    /// Exclusive lower bound on the marker id scanned this page (`Uuid::nil()`
    /// on the first page). The page CTE selects rows with a strictly greater id.
    cursor: Uuid,
    /// Maximum number of marker rows examined this page.
    page_size: i64,
    /// Generation of the sitrep being executed. A marker row is deletable only
    /// if its `created_at_generation` is strictly less than this.
    sitrep_generation: nexus_db_model::Generation,
    /// Id of the sitrep being executed, used to test the request set.
    sitrep_id: Uuid,
    /// `FROM` clause for the marker table, built once in [`Self::new`] and
    /// stored so it shares `self`'s lifetime when walked -- the same pattern
    /// (and reason) as [`SitrepGuardedInsert`]'s stored from-clause.
    ///
    /// [`SitrepGuardedInsert`]: crate::db::sitrep_guard::SitrepGuardedInsert
    marker_from_clause: <MarkerTable<R> as QuerySource>::FromClause,
    /// `FROM` clause for the request table, stored for the same reason.
    request_from_clause: <R::RequestTable as QuerySource>::FromClause,
}

impl<R: FmRendezvousResource> RendezvousMarkerGcPage<R> {
    fn new(
        cursor: Uuid,
        page_size: i64,
        sitrep_generation: Generation,
        sitrep_id: Uuid,
    ) -> Self {
        Self {
            cursor,
            page_size,
            sitrep_generation: nexus_db_model::Generation::from(
                sitrep_generation,
            ),
            sitrep_id,
            marker_from_clause: <MarkerTable<R> as HasTable>::table()
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

/// The page query returns a single row: `(rows_deleted, next_cursor)`.
impl<R: FmRendezvousResource> Query for RendezvousMarkerGcPage<R> {
    type SqlType = (sql_types::Int8, sql_types::Nullable<sql_types::Uuid>);
}

impl<R: FmRendezvousResource> RunQueryDsl<DbConnection>
    for RendezvousMarkerGcPage<R>
{
}

impl<R: FmRendezvousResource> QueryFragment<Pg> for RendezvousMarkerGcPage<R> {
    /// Emits, with `<id>` = the marker id column and the marker/request table
    /// names spliced from `R`'s schema types:
    ///
    /// ```sql
    /// WITH page AS MATERIALIZED (          -- one bounded page of marker rows
    ///        SELECT <id>, created_at_generation FROM <marker_table>
    ///        WHERE <id> > $cursor ORDER BY <id> LIMIT $page_size),
    ///      deleted AS (                    -- delete page rows meeting BOTH:
    ///        DELETE FROM <marker_table> WHERE <id> IN (
    ///          SELECT p.<id> FROM page p
    ///          WHERE p.created_at_generation < $sitrep_generation  -- (1)
    ///            AND NOT EXISTS (                                   -- (2)
    ///              SELECT 1 FROM <request_table> r
    ///              WHERE r.sitrep_id = $sitrep_id AND r.id = p.<id>))
    ///        RETURNING <id>)
    /// SELECT (SELECT COUNT(*) FROM deleted) AS rows_deleted,
    ///        (SELECT MAX(<id>)  FROM page)  AS next_cursor;
    /// ```
    ///
    /// The `page` CTE is `MATERIALIZED` on purpose. Without it, CRDB will
    /// happily push the outer `created_at_generation < $gen` and `NOT EXISTS`
    /// predicates down into the page scan, which silently inflates the page and
    /// breaks the `page_size` bound. Materializing fixes the page to exactly
    /// `<= page_size` rows ordered by the marker id, no matter how many
    /// ultimately satisfy the predicate.
    ///
    /// The next cursor is `MAX(<id>)` over the (materialized) page, not over
    /// the rows actually deleted, so the strict `>` advance guarantees forward
    /// progress even on a page where nothing satisfied the predicate.
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        let id_column = <R::IdColumn as Column>::NAME;

        // CTE 1: page -- one bounded, materialized page of marker rows.
        out.push_sql("WITH page AS MATERIALIZED (SELECT ");
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

        // CTE 2: deleted -- delete the page rows meeting both conjuncts of the
        // predicate, returning their ids so the outer SELECT can count them.
        out.push_sql("), deleted AS (DELETE FROM ");
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(id_column)?;
        out.push_sql(" IN (SELECT p.");
        out.push_identifier(id_column)?;
        out.push_sql(" FROM page p WHERE p.");
        out.push_identifier("created_at_generation")?;
        out.push_sql(" < ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.sitrep_generation)?;
        out.push_sql(" AND NOT EXISTS (SELECT 1 FROM ");
        self.request_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" r WHERE r.");
        out.push_identifier("sitrep_id")?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.sitrep_id)?;
        out.push_sql(" AND r.");
        out.push_identifier("id")?;
        out.push_sql(" = p.");
        out.push_identifier(id_column)?;
        out.push_sql(")) RETURNING ");
        out.push_identifier(id_column)?;

        // Outer SELECT: page stats -- rows deleted, and the cursor advance.
        out.push_sql(
            ") SELECT (SELECT COUNT(*) FROM deleted) AS rows_deleted, \
             (SELECT MAX(",
        );
        out.push_identifier(id_column)?;
        out.push_sql(") FROM page) AS next_cursor");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::fm_rendezvous_resources::test_utils::DummyResource;
    use crate::db::fm_rendezvous_resources::test_utils::dummy_marker;
    use crate::db::fm_rendezvous_resources::test_utils::dummy_request;
    use crate::db::fm_rendezvous_resources::test_utils::setup_dummy_schema;
    use crate::db::pub_test_utils::TestDatabase;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use omicron_test_utils::dev;

    // ---- dummy-resource helpers ----
    //
    // The GC predicate, pagination, and cursor advance are generic over
    // `FmRendezvousResource`; nothing about them is specific to alerts or support
    // bundles. So the matrix below drives the sweep against `DummyResource` and
    // its throwaway schema, decoupled from any real resource. The real
    // `rendezvous_*_created` tables are still exercised for index coverage by the
    // `*_explain_no_full_scan` tests, and end-to-end by the `fm_rendezvous`
    // integration tests.

    /// Create the dummy schema on a throwaway connection.
    async fn setup_schema(datastore: &DataStore) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
    }

    /// Insert a creation-marker row into the dummy marker table at the given
    /// `created_at_generation`.
    async fn insert_dummy_marker(
        datastore: &DataStore,
        id: Uuid,
        created_at_generation: i64,
    ) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::insert_into(dummy_marker::table)
            .values((
                dummy_marker::dsl::dummy_id.eq(id),
                dummy_marker::dsl::created_at_generation
                    .eq(created_at_generation),
            ))
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    /// Put `resource_id` into `sitrep_id`'s request set via a dummy request row.
    async fn insert_dummy_request(
        datastore: &DataStore,
        sitrep_id: SitrepUuid,
        resource_id: Uuid,
    ) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::insert_into(dummy_request::table)
            .values((
                dummy_request::dsl::id.eq(resource_id),
                dummy_request::dsl::sitrep_id.eq(sitrep_id.into_untyped_uuid()),
            ))
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    async fn dummy_marker_exists(datastore: &DataStore, id: Uuid) -> bool {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let count: i64 = dummy_marker::table
            .filter(dummy_marker::dsl::dummy_id.eq(id))
            .count()
            .get_result_async(&*conn)
            .await
            .unwrap();
        count > 0
    }

    /// Drain a marker GC sweep to completion by driving
    /// [`RendezvousMarkerGcPage`] directly with an arbitrary `page_size`
    /// (rather than the public methods, whose page size is fixed at
    /// `SQL_BATCH_SIZE`), so tests can exercise pagination. Returns total rows
    /// deleted.
    async fn run_marker_gc_sweep<R: FmRendezvousResource + 'static>(
        datastore: &DataStore,
        sitrep_id: SitrepUuid,
        sitrep_generation: Generation,
        page_size: i64,
    ) -> usize {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let mut total = 0usize;
        let mut cursor = Uuid::nil();
        loop {
            let (deleted, next) = RendezvousMarkerGcPage::<R>::new(
                cursor,
                page_size,
                sitrep_generation,
                sitrep_id.into_untyped_uuid(),
            )
            .get_result_async::<(i64, Option<Uuid>)>(&*conn)
            .await
            .unwrap();
            total += deleted as usize;
            match next {
                Some(c) => cursor = c,
                None => break,
            }
        }
        total
    }

    // ---- GC predicate + pagination matrix (against DummyResource) ----

    /// A marker whose resource is still in the current sitrep's request set
    /// must be preserved, even if its `created_at_generation` is strictly less
    /// than the sitrep's generation (both conjuncts must hold).
    #[tokio::test]
    async fn gc_preserves_marker_when_resource_in_current_sitrep() {
        let logctx = dev::test_setup_log(
            "gc_preserves_marker_when_resource_in_current_sitrep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        setup_schema(datastore).await;

        let sitrep_id = SitrepUuid::new_v4();
        let id = Uuid::new_v4();
        insert_dummy_marker(datastore, id, 1).await;
        insert_dummy_request(datastore, sitrep_id, id).await;

        let deleted = run_marker_gc_sweep::<DummyResource>(
            datastore,
            sitrep_id,
            Generation::from_u32(5), // current sitrep gen > marker generation
            1000,
        )
        .await;
        assert_eq!(deleted, 0);
        assert!(dummy_marker_exists(datastore, id).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// A marker whose resource is *not* in the current sitrep's request set and
    /// whose `created_at_generation` is strictly less than the sitrep's
    /// generation is deletable.
    #[tokio::test]
    async fn gc_deletes_marker_when_resource_dropped() {
        let logctx =
            dev::test_setup_log("gc_deletes_marker_when_resource_dropped");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        setup_schema(datastore).await;

        let sitrep_id = SitrepUuid::new_v4();
        let id = Uuid::new_v4();
        // Marker created at gen 1; current sitrep is at gen 2 and does not
        // request this resource.
        insert_dummy_marker(datastore, id, 1).await;

        let deleted = run_marker_gc_sweep::<DummyResource>(
            datastore,
            sitrep_id,
            Generation::from_u32(2),
            1000,
        )
        .await;
        assert_eq!(deleted, 1);
        assert!(!dummy_marker_exists(datastore, id).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Conjunct 2 (`NOT EXISTS`) is scoped to the *current* sitrep's request set
    /// (`r.sitrep_id = <sitrep_id>`), not to request rows globally. A marker
    /// whose resource is requested only by some *other* sitrep -- and whose
    /// `created_at_generation` is strictly less than the current sitrep's
    /// generation -- is therefore still deletable. This pins the sitrep-scoping
    /// of the join: dropping the `sitrep_id` predicate would wrongly preserve
    /// this marker, and this test would catch it.
    #[tokio::test]
    async fn gc_deletes_when_request_belongs_to_other_sitrep() {
        let logctx = dev::test_setup_log(
            "gc_deletes_when_request_belongs_to_other_sitrep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        setup_schema(datastore).await;

        let sitrep_id = SitrepUuid::new_v4();
        let other_sitrep_id = SitrepUuid::new_v4();
        let id = Uuid::new_v4();
        // Marker at gen 1; the only request row for this resource belongs to a
        // *different* sitrep than the one we sweep against.
        insert_dummy_marker(datastore, id, 1).await;
        insert_dummy_request(datastore, other_sitrep_id, id).await;

        let deleted = run_marker_gc_sweep::<DummyResource>(
            datastore,
            sitrep_id,
            Generation::from_u32(5),
            1000,
        )
        .await;
        assert_eq!(deleted, 1);
        assert!(!dummy_marker_exists(datastore, id).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// The predicate uses strict `<`, so a marker whose `created_at_generation`
    /// equals (or exceeds) the current sitrep's generation must be preserved --
    /// that resource was created at this generation or later and could still be
    /// requested by it.
    #[tokio::test]
    async fn gc_preserves_marker_when_generation_at_or_above_current_sitrep() {
        let logctx = dev::test_setup_log(
            "gc_preserves_marker_when_generation_at_or_above_current_sitrep",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        setup_schema(datastore).await;

        let sitrep_id = SitrepUuid::new_v4();
        // Marker created at gen 5; current sitrep at gen 5 (equal): not
        // deletable.
        let equal_id = Uuid::new_v4();
        insert_dummy_marker(datastore, equal_id, 5).await;
        // Marker created at gen 6; current sitrep at gen 5 (executor lagging,
        // shouldn't really happen but the predicate must still preserve).
        let ahead_id = Uuid::new_v4();
        insert_dummy_marker(datastore, ahead_id, 6).await;

        let deleted = run_marker_gc_sweep::<DummyResource>(
            datastore,
            sitrep_id,
            Generation::from_u32(5),
            1000,
        )
        .await;
        assert_eq!(deleted, 0);
        assert!(dummy_marker_exists(datastore, equal_id).await);
        assert!(dummy_marker_exists(datastore, ahead_id).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// With a page size of 1 and several rows, the sweep advances through all
    /// rows and terminates, deleting exactly the deletable markers.
    #[tokio::test]
    async fn gc_pagination_terminates() {
        let logctx = dev::test_setup_log("gc_pagination_terminates");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        setup_schema(datastore).await;

        let sitrep_id = SitrepUuid::new_v4();
        // 5 deletable markers (gen 1, no request rows).
        let deletable: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
        for &id in &deletable {
            insert_dummy_marker(datastore, id, 1).await;
        }
        // 3 preserved markers (gen 1, with request rows in the swept sitrep).
        let preserved: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        for &id in &preserved {
            insert_dummy_marker(datastore, id, 1).await;
            insert_dummy_request(datastore, sitrep_id, id).await;
        }

        // page_size = 1 forces many pages.
        let deleted = run_marker_gc_sweep::<DummyResource>(
            datastore,
            sitrep_id,
            Generation::from_u32(5),
            1,
        )
        .await;
        assert_eq!(deleted, 5);

        // Deletables are gone; preserveds remain. (We check by id rather than by
        // COUNT(*) because CRDB rejects unindexed full scans.)
        for &id in &deletable {
            assert!(
                !dummy_marker_exists(datastore, id).await,
                "marker {id:?} should have been swept",
            );
        }
        for &id in &preserved {
            assert!(
                dummy_marker_exists(datastore, id).await,
                "marker {id:?} should have been preserved",
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// The page query must not full-scan either of the two tables it
    /// reads from. (Both have indexes on the columns we filter by.)
    #[tokio::test]
    async fn gc_alert_explain_no_full_scan() {
        let logctx = dev::test_setup_log("gc_alert_explain_no_full_scan");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = RendezvousMarkerGcPage::<nexus_db_model::Alert>::new(
            Uuid::nil(),
            1000,
            Generation::from_u32(5),
            SitrepUuid::nil().into_untyped_uuid(),
        );
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn gc_support_bundle_explain_no_full_scan() {
        let logctx =
            dev::test_setup_log("gc_support_bundle_explain_no_full_scan");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query =
            RendezvousMarkerGcPage::<nexus_db_model::SupportBundle>::new(
                Uuid::nil(),
                1000,
                Generation::from_u32(5),
                SitrepUuid::nil().into_untyped_uuid(),
            );
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
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
