// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic stale-execution-guarded inserts for resources created by fault
//! management.
//!
//! The caller is expected to be a subtask of fm_rendezvous, creating some
//! resource requested by the sitrep currently being executed. A "resource" here
//! is any object requested by a sitrep (e.g. [`AlertRequest`],
//! [`SupportBundleRequest`]). Given some unconditional Diesel `INSERT`
//! statement for the requested resource, the goal here is to wrap the statement
//! such that it's conditional on a couple things:
//!
//!   - The generation number in `GENERATION_COLUMN` on the sitrep being
//!     executed must be equal to the generation of the latest sitrep in
//!     the database. This guards against stale execution.
//!
//!   - There is no entry in `MARKER_TABLE` for the requested resource id
//!     indicating that the resource was previously created. This guards
//!     against resurrection of resources that were already deleted.
//!
//! See detailed comments on trait [`FmRendezvousResource`] and struct
//! [`SitrepGuardedInsert`] for how this policy is implemented.
//!
//! [`AlertRequest`]: nexus_types::fm::case::AlertRequest
//! [`SupportBundleRequest`]: nexus_types::fm::case::SupportBundleRequest

use crate::db::fm_rendezvous_resources::FmRendezvousResource;
use crate::db::fm_rendezvous_resources::MarkerTable;
use crate::db::true_or_cast_error::matches_sentinel;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::Column;
use diesel::QueryResult;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::query_source::QuerySource;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use nexus_db_model::Generation;
use uuid::Uuid;

/// CTE-wrapped stale-execution-guarded INSERT.
///
/// Type parameters:
/// - `R` is the type of the resource being inserted, which must implement
///   [`FmRendezvousResource`] (see [`Self::walk_ast`] below).
/// - `ISR` is the wrapped `INSERT` -- typically a Diesel-built
///   `InsertStatement<...>`, inferred at the call site. It must include a
///   `RETURNING` clause projecting to `R`.
#[must_use = "Queries must be executed"]
pub struct SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource,
{
    /// UUID of the resource being inserted. Used as the lookup key in the
    /// `prior_marker_guard` CTE and as the row value written by the
    /// `new_marker` CTE.
    resource_id: Uuid,

    /// Resource generation in the sitrep currently being executed by
    /// fm_rendezvous. The `stale_guard` CTE requires this to equal the latest
    /// sitrep's value of [`FmRendezvousResource::GenerationColumn`].
    expected_generation: Generation,

    /// Caller-built INSERT for the resource row itself, nested into the
    /// `new_resource` CTE via [`QueryFragment::walk_ast`].
    resource_insert: ISR,

    /// The marker table's `FROM` clause, built once in [`Self::new`] from
    /// [`MarkerTable<R>`]. Stored as a field here, rather than reconstructed
    /// inside [`Self::walk_ast`] so it correctly shares `self`'s lifetime when
    /// walked.
    marker_from_clause: <MarkerTable<R> as QuerySource>::FromClause,
}

impl<R, ISR> SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource,
{
    /// Build a guarded insert for `resource_id` at `expected_generation`. The
    /// wrapped `resource_insert` statement must satisfy the following:
    ///
    /// 1. It has a `RETURNING` clause that projects to `R`.
    /// 2. It is a single-row INSERT.
    /// 3. The id it writes matches the given `resource_id`.
    /// 4. It is built with `ON CONFLICT DO NOTHING`. This combinator
    ///    distinguishes "freshly created" from "already exists" purely by
    ///    whether the inner INSERT's `RETURNING` produced a row:
    ///    [`Self::execute_async`] maps an empty result to
    ///    [`SitrepGuardedInsertOutcome::AlreadyExists`]. An inner INSERT that
    ///    errored (rather than silently no-op'd) on a primary-key conflict
    ///    would turn the "row already exists" outcome into a hard error.
    ///
    /// Ideally we'd enforce most of these constraints in the type system, but
    /// the Diesel traits we'd need to reference (`IntoConflictValueClause`,
    /// `OnConflictValues`, `DoNothing`) are not exposed in its public API.
    ///
    pub fn new(
        resource_id: Uuid,
        expected_generation: Generation,
        resource_insert: ISR,
    ) -> Self {
        let marker_from_clause =
            <MarkerTable<R> as HasTable>::table().from_clause();
        Self {
            resource_id,
            expected_generation,
            resource_insert,
            marker_from_clause,
        }
    }
}

impl<R, ISR> QueryId for SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// The combinator's projection mirrors the inner `INSERT`'s `RETURNING` clause.
/// See [`Self::walk_ast`]'s outer `SELECT nr.* FROM new_resource nr`.
impl<R, ISR> Query for SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource,
    ISR: Query,
{
    type SqlType = ISR::SqlType;
}

/// Sentinel string emitted by the `stale_guard` CTE on generation mismatch.
/// Decoded back to [`SitrepGuardedInsertOutcome::StaleSitrep`].
const STALE_GENERATION_SENTINEL: &str = "stale generation";

/// Sentinel string emitted by the `prior_marker_guard` CTE when a creation
/// marker already exists for this resource id. Decoded back to
/// [`SitrepGuardedInsertOutcome::AlreadyExists`].
const ALREADY_EXISTS_SENTINEL: &str = "marker already exists";

impl<R, ISR> QueryFragment<Pg> for SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource,
    ISR: QueryFragment<Pg>,
{
    /// Executes a statement of the form:
    ///
    /// ```sql
    /// WITH
    ///   latest_history AS MATERIALIZED (...),
    ///   stale_guard AS MATERIALIZED (...),
    ///   prior_marker_guard AS MATERIALIZED (...),
    ///   new_resource AS (...),
    ///   new_marker AS (...)
    /// SELECT nr.* FROM new_resource nr
    /// ```
    ///
    /// The individual CTEs here are:
    ///
    /// 1. `latest_history`: the most-recent `fm_sitrep_history` row.
    /// 2. `stale_guard`: compare `expected_generation` against the generation
    ///    on the latest sitrep. If it doesn't match, it aborts with a
    ///    non-retryable DB error that maps to
    ///    [`SitrepGuardedInsertOutcome::StaleSitrep`].
    /// 3. `prior_marker_guard`: checks whether a creation marker already exists
    ///    for this resource id. If it does, aborts with a non-retryable DB
    ///    error that maps to [`SitrepGuardedInsertOutcome::AlreadyExists`].
    /// 4. `new_resource`: the caller's resource INSERT.
    /// 5. `new_marker`: the marker INSERT, emitted inline by the
    ///    combinator using R's schema types. This only runs when
    ///    `new_resource` actually produced a row.
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // Column names injected into the CTEs below.
        let id_column = <R::IdColumn as Column>::NAME;
        let generation_column = <R::GenerationColumn as Column>::NAME;

        // CTE 1: latest_history.
        out.push_sql(
            "WITH latest_history AS MATERIALIZED (\
                SELECT sitrep_id FROM omicron.public.fm_sitrep_history \
                 ORDER BY version DESC LIMIT 1\
             )",
        );

        // CTE 2: stale_guard.
        //
        // A note on why Diesel has made us sad today: We'd have preferred to
        // use `TrueOrCastError` here in `stale_guard` and `prior_marker_guard`
        // below, rather than copy-pasting the `CAST(IF(...))` shape. We
        // couldn't make it work.
        //
        // The crux of the problem was the signature of Diesel's
        // `QueryFragment::walk_ast`, which ties the lifetimes of its `&'b self`
        // and `out: AstPass<'_, 'b, Pg>` parameters to a single `'b`. If we try
        // to write something here like:
        //
        // ```rust
        // let guard = TrueOrCastError::new(...);
        // guard.walk_ast(out.reborrow());
        // ```
        //
        // ... the borrow checker would complain that `guard` doesn't have the
        // same lifetime as `self`. We could certainly do this, lifting the
        // guard clauses into fields of `SitrepGuardedInsert` to give them the
        // correct lifetimes, but that would be way less readable than just
        // writing the guards inline here.
        //
        // Also note that an empty `fm_sitrep_history` will also yield a "stale
        // generation" sentinel here. Assuming `SitrepGuardedInsert` is used in
        // the intended context of `FmRendezvous::actually_activate`, where we
        // first check for the presence of a current sitrep, this won't happen.
        out.push_sql(
            ", stale_guard AS MATERIALIZED (SELECT CAST(IF(\
                ((SELECT s.",
        );
        out.push_identifier(generation_column)?;
        out.push_sql(
            " FROM omicron.public.fm_sitrep s \
                  JOIN latest_history lh ON lh.sitrep_id = s.id) = ",
        );
        out.push_bind_param::<sql_types::BigInt, _>(&self.expected_generation)?;
        out.push_sql("), 'TRUE', '");
        out.push_sql(STALE_GENERATION_SENTINEL);
        out.push_sql("') AS BOOL) AS ok)");

        // CTE 3: prior_marker_guard.
        out.push_sql(
            ", prior_marker_guard AS MATERIALIZED (SELECT CAST(IF(\
                NOT EXISTS (SELECT 1 FROM ",
        );
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(id_column)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.resource_id)?;
        out.push_sql("), 'TRUE', '");
        out.push_sql(ALREADY_EXISTS_SENTINEL);
        out.push_sql("') AS BOOL) AS ok)");

        // CTE 4: new_resource, the caller's INSERT statement.
        //
        // Note that unlike the guard CTEs, we're able to call walk_ast without
        // lifetime issues here because resource_insert is a field of self.
        out.push_sql(", new_resource AS (");
        self.resource_insert.walk_ast(out.reborrow())?;
        out.push_sql(")");

        // CTE 5: new_marker.
        //
        // The `WHERE EXISTS (SELECT 1 FROM new_resource)` predicate gates the
        // marker INSERT on the inner resource INSERT having produced a row.
        out.push_sql(", new_marker AS (INSERT INTO ");
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" (");
        out.push_identifier(id_column)?;
        out.push_sql(", ");
        // The generation column name is hardcoded here rather than carried as a
        // `FmRendezvousResource` associated type. We expect all marker tables
        // to use the same column name for consistency.
        out.push_identifier("created_at_generation")?;
        out.push_sql(") SELECT ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.resource_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.expected_generation)?;
        out.push_sql(
            " WHERE EXISTS (SELECT 1 FROM new_resource) ON CONFLICT (",
        );
        out.push_identifier(id_column)?;
        out.push_sql(") DO NOTHING RETURNING ");
        out.push_identifier(id_column)?;
        out.push_sql(")");

        // Outer SELECT projects the inner INSERT's `RETURNING` columns through,
        // so callers receive the inserted row.
        out.push_sql(" SELECT nr.* FROM new_resource nr");

        Ok(())
    }
}

impl<R, ISR> diesel::RunQueryDsl<DbConnection> for SitrepGuardedInsert<R, ISR> where
    R: FmRendezvousResource
{
}

/// Outcome of a [`SitrepGuardedInsert`] attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SitrepGuardedInsertOutcome<R> {
    /// Both guards passed and the resource `INSERT` produced a row; the inner
    /// statement's `RETURNING` clause is projected through and surfaced here.
    /// The marker `INSERT` also wrote its row atomically.
    Created(R),
    /// The resource already exists. Either `prior_marker_guard`
    /// short-circuited because a marker for this resource id was present, or
    /// both guards passed but the inner INSERT `ON CONFLICT DO NOTHING` matched
    /// a pre-existing row.
    AlreadyExists,
    /// The rendezvous task's expected generation does not match the current
    /// generation on the latest sitrep; nothing was inserted.
    StaleSitrep,
}

impl<R, ISR> SitrepGuardedInsert<R, ISR>
where
    R: FmRendezvousResource + Send + 'static,
    ISR: 'static,
    Self:
        Send + diesel::query_dsl::methods::LoadQuery<'static, DbConnection, R>,
{
    pub async fn execute_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<SitrepGuardedInsertOutcome<R>, DieselError> {
        match self.get_result_async::<R>(conn).await {
            Ok(row) => Ok(SitrepGuardedInsertOutcome::Created(row)),
            // Both guards passed, but the outer SELECT returned zero rows. This
            // is unlikely: the only way to reach this state is if the guards
            // both passed but the wrapped `INSERT`'s `ON CONFLICT DO NOTHING`
            // did nothing because there was a preexisting row with the same
            // UUID.
            Err(DieselError::NotFound) => {
                Ok(SitrepGuardedInsertOutcome::AlreadyExists)
            }
            Err(e)
                if matches_sentinel(&e, &[STALE_GENERATION_SENTINEL])
                    .is_some() =>
            {
                Ok(SitrepGuardedInsertOutcome::StaleSitrep)
            }
            Err(e)
                if matches_sentinel(&e, &[ALREADY_EXISTS_SENTINEL])
                    .is_some() =>
            {
                Ok(SitrepGuardedInsertOutcome::AlreadyExists)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::OpContext;
    use crate::db::DataStore;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use chrono::Utc;
    use diesel::prelude::*;
    use iddqd::IdOrdMap;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use uuid::uuid;

    // The synthetic resource and dummy schema are shared with the GC test suite;
    // see `crate::db::fm_rendezvous_resources::test_utils`.
    use crate::db::fm_rendezvous_resources::test_utils::DummyResource;
    use crate::db::fm_rendezvous_resources::test_utils::dummy_marker;
    use crate::db::fm_rendezvous_resources::test_utils::dummy_resource;
    use crate::db::fm_rendezvous_resources::test_utils::insert_dummy_marker;
    use crate::db::fm_rendezvous_resources::test_utils::setup_dummy_schema;

    // Snapshots the SQL the combinator generates, so accidental changes to the
    // hand-assembled query are caught in review.
    #[tokio::test]
    async fn expectorate_sitrep_guarded_insert() {
        let resource_id = uuid!("11111111-1111-1111-1111-111111111111");
        let insert = diesel::insert_into(dummy_resource::table)
            .values((
                dummy_resource::dsl::id.eq(resource_id),
                dummy_resource::dsl::name.eq("test"),
            ))
            .on_conflict(dummy_resource::dsl::id)
            .do_nothing()
            .returning(DummyResource::as_returning());
        let query = SitrepGuardedInsert::<DummyResource, _>::new(
            resource_id,
            Generation::try_from(3).unwrap(),
            insert,
        );
        expectorate_query_contents(
            &query,
            "tests/output/sitrep_guarded_insert.sql",
        )
        .await;
    }

    // Inserts a current sitrep with a given `dummy_generation`, returning its
    // ID so further sitreps can be chained onto it via `parent`.
    async fn insert_current_sitrep(
        datastore: &DataStore,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        parent: Option<SitrepUuid>,
        generation: i64,
    ) -> SitrepUuid {
        let sitrep_id = SitrepUuid::new_v4();
        let sitrep = Sitrep {
            metadata: SitrepMetadata {
                id: sitrep_id,
                parent_sitrep_id: parent,
                inv_collection_id: CollectionUuid::new_v4(),
                next_inv_min_time_started: Utc::now(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "sitrep_guard test sitrep".to_string(),
                time_created: Utc::now(),
                alert_generation: external::Generation::new(),
                support_bundle_generation: external::Generation::new(),
            },
            cases: IdOrdMap::new(),
            ereports_by_id: IdOrdMap::new(),
        };
        datastore.fm_sitrep_insert(opctx, sitrep, None).await.unwrap();

        // `SitrepMetadata` doesn't have a `dummy_generation` field, so we have
        // to update it manually.
        conn.batch_execute_async(&format!(
            "UPDATE omicron.public.fm_sitrep \
                 SET dummy_generation = {generation} WHERE id = '{sitrep_id}'"
        ))
        .await
        .unwrap();

        sitrep_id
    }

    // Builds and runs a guarded insert for `resource_id` at
    // `expected_generation`.
    async fn run_guarded_insert(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        resource_id: Uuid,
        expected_generation: i64,
    ) -> SitrepGuardedInsertOutcome<DummyResource> {
        let insert = diesel::insert_into(dummy_resource::table)
            .values((
                dummy_resource::dsl::id.eq(resource_id),
                dummy_resource::dsl::name.eq("test"),
            ))
            .on_conflict(dummy_resource::dsl::id)
            .do_nothing()
            .returning(DummyResource::as_returning());
        SitrepGuardedInsert::<DummyResource, _>::new(
            resource_id,
            Generation::try_from(expected_generation).unwrap(),
            insert,
        )
        .execute_async(conn)
        .await
        .unwrap()
    }

    // Whether a dummy resource row exists for `id`.
    async fn resource_exists(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
    ) -> bool {
        diesel::select(diesel::dsl::exists(
            dummy_resource::table.filter(dummy_resource::dsl::id.eq(id)),
        ))
        .get_result_async::<bool>(conn)
        .await
        .unwrap()
    }

    // The generation recorded in the marker row for `id`, if any.
    async fn marker_generation(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
    ) -> Option<i64> {
        dummy_marker::table
            .filter(dummy_marker::dsl::dummy_id.eq(id))
            .select(dummy_marker::dsl::created_at_generation)
            .first_async::<i64>(conn)
            .await
            .optional()
            .unwrap()
    }

    // Both guards pass: the resource row is inserted and a marker is written
    // with the executed generation.
    //
    // The history deliberately contains *two* sitreps at the same generation
    // (the request set did not change between them), and the rendezvous task is
    // working from the older one. Along a sitrep chain, an unchanged generation
    // means an unchanged request set, so the executor here is creating exactly
    // the resources a "fresh" one would, rather than a stale set.
    #[tokio::test]
    async fn sitrep_guarded_insert_created_writes_marker() {
        let logctx =
            dev::test_setup_log("sitrep_guarded_insert_created_writes_marker");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
        let parent =
            insert_current_sitrep(datastore, opctx, &conn, None, 2).await;
        insert_current_sitrep(datastore, opctx, &conn, Some(parent), 2).await;

        let resource_id = uuid!("22222222-2222-2222-2222-222222222222");
        let outcome = run_guarded_insert(&conn, resource_id, 2).await;

        assert_matches!(
            outcome,
            SitrepGuardedInsertOutcome::Created(row)
                if row.id == resource_id && row.name == "test"
        );
        // The marker was written with the executed generation.
        assert_eq!(marker_generation(&conn, resource_id).await, Some(2));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // A pre-existing marker short-circuits `prior_marker_guard`: the insert
    // reports `AlreadyExists` and the resource row is not written.
    #[tokio::test]
    async fn sitrep_guarded_insert_already_exists_via_marker() {
        let logctx = dev::test_setup_log(
            "sitrep_guarded_insert_already_exists_via_marker",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
        insert_current_sitrep(datastore, opctx, &conn, None, 2).await;

        let resource_id = uuid!("33333333-3333-3333-3333-333333333333");
        // Seed a marker for this resource id, at a generation *different* from
        // the one we will execute at, so the final assertion can distinguish
        // "marker preserved" from "marker overwritten with the executed
        // generation".
        insert_dummy_marker(&conn, resource_id, 1).await;

        let outcome = run_guarded_insert(&conn, resource_id, 2).await;

        assert_matches!(outcome, SitrepGuardedInsertOutcome::AlreadyExists);
        // The resource row was not inserted; the seeded marker is unchanged.
        // (The sentinel from `prior_marker_guard` aborts the whole statement
        // atomically, so `new_marker` never runs here; the generation could
        // only change if the guard itself regressed. Asserting it pins the
        // invariant that markers record the generation at which the resource
        // was originally created.)
        assert!(!resource_exists(&conn, resource_id).await);
        assert_eq!(marker_generation(&conn, resource_id).await, Some(1));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Both guards pass, but the inner `ON CONFLICT DO NOTHING` matches a
    // pre-existing resource row, so no row is returned and no marker is
    // written. This is also surfaced as `AlreadyExists`.
    #[tokio::test]
    async fn sitrep_guarded_insert_already_exists_via_conflict() {
        let logctx = dev::test_setup_log(
            "sitrep_guarded_insert_already_exists_via_conflict",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
        insert_current_sitrep(datastore, opctx, &conn, None, 2).await;

        let resource_id = uuid!("44444444-4444-4444-4444-444444444444");
        // Seed the resource row, but no marker.
        diesel::insert_into(dummy_resource::table)
            .values((
                dummy_resource::dsl::id.eq(resource_id),
                dummy_resource::dsl::name.eq("preexisting"),
            ))
            .execute_async(&*conn)
            .await
            .unwrap();

        let outcome = run_guarded_insert(&conn, resource_id, 2).await;

        assert_matches!(outcome, SitrepGuardedInsertOutcome::AlreadyExists);
        // The pre-existing resource row is still there, and no marker was
        // written (the marker INSERT is gated on the resource INSERT producing
        // a row).
        assert!(resource_exists(&conn, resource_id).await);
        assert_eq!(marker_generation(&conn, resource_id).await, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // The executed generation does not match the latest sitrep's generation:
    // `stale_guard` aborts and nothing is written.
    //
    // The executed generation (1) deliberately matches an ancestor sitrep in
    // the history: this is exactly the stale-sitrep case the guard exists
    // for, and a `stale_guard` that matched any sitrep in `fm_sitrep_history`
    // (rather than only the current one) would incorrectly let the insert
    // through.
    #[tokio::test]
    async fn sitrep_guarded_insert_stale_sitrep() {
        let logctx = dev::test_setup_log("sitrep_guarded_insert_stale_sitrep");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
        // History: ancestor at generation 1, current sitrep at generation 2...
        let parent =
            insert_current_sitrep(datastore, opctx, &conn, None, 1).await;
        insert_current_sitrep(datastore, opctx, &conn, Some(parent), 2).await;

        let resource_id = uuid!("55555555-5555-5555-5555-555555555555");
        // ... and we execute expecting generation 1.
        let outcome = run_guarded_insert(&conn, resource_id, 1).await;

        assert_matches!(outcome, SitrepGuardedInsertOutcome::StaleSitrep);
        assert!(!resource_exists(&conn, resource_id).await);
        assert_eq!(marker_generation(&conn, resource_id).await, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // With no sitrep in history at all, there is no current generation to
    // match, so the guarded insert must fail closed: no resource row and no
    // marker row may be written. We deliberately do not assert *which*
    // outcome is reported -- this situation is unreachable via fm_rendezvous,
    // which only runs with a loaded sitrep, so the reported outcome is not a
    // behavior we want to guarantee -- only that the guard does not fail
    // open.
    #[tokio::test]
    async fn sitrep_guarded_insert_no_sitrep_writes_nothing() {
        let logctx = dev::test_setup_log(
            "sitrep_guarded_insert_no_sitrep_writes_nothing",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (_opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        setup_dummy_schema(&conn).await;
        // Deliberately no sitrep inserted.

        let resource_id = uuid!("88888888-8888-8888-8888-888888888888");
        let _ = run_guarded_insert(&conn, resource_id, 1).await;

        assert!(!resource_exists(&conn, resource_id).await);
        assert_eq!(marker_generation(&conn, resource_id).await, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
